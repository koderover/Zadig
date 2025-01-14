/*
Copyright 2023 The KodeRover Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kube

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	versionedclient "istio.io/client-go/pkg/clientset/versioned"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/helm/pkg/releaseutil"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type SharedEnvHandler func(context.Context, *commonmodels.Product, string, client.Client, versionedclient.Interface) error
type IstioGrayscaleEnvHandler func(context.Context, *commonmodels.Product, string, client.Client, versionedclient.Interface) error

type ResourceApplyParam struct {
	ProductInfo         *commonmodels.Product
	ServiceName         string
	ServiceList         []string // used for batch operations
	CurrentResourceYaml string
	UpdateResourceYaml  string

	// used for helm services
	Images                []string // all images need to be updated, used for helm services
	VariableYaml          string   // variables
	Timeout               int      // timeout for helm services
	UpdateServiceRevision bool

	Informer                 informers.SharedInformerFactory
	KubeClient               client.Client
	IstioClient              versionedclient.Interface
	AddZadigLabel            bool
	InjectSecrets            bool
	SharedEnvHandler         SharedEnvHandler
	IstioGrayscaleEnvHandler IstioGrayscaleEnvHandler
	Uninstall                bool
	WaitForUninstall         bool
}

func DeploymentSelectorLabelExists(resourceName, namespace string, informer informers.SharedInformerFactory, log *zap.SugaredLogger) bool {
	deployment, err := informer.Apps().V1().Deployments().Lister().Deployments(namespace).Get(resourceName)
	// default we assume the deployment is new so we don't need to add selector labels
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("Failed to find deployment in the namespace: %s, the error is: %s", namespace, err)
		}
		return false
	}
	// since the 2 predefined labels are always together, we just check for only one
	// if the match label exists, we return true. otherwise we return false
	if _, ok := deployment.Spec.Selector.MatchLabels["s-product"]; ok {
		return true
	}
	return false
}

func StatefulsetSelectorLabelExists(resourceName, namespace string, informer informers.SharedInformerFactory, log *zap.SugaredLogger) bool {
	sts, err := informer.Apps().V1().StatefulSets().Lister().StatefulSets(namespace).Get(resourceName)
	// default we assume the deployment is new so we don't need to add selector labels
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("Failed to find deployment in the namespace: %s, the error is: %s", namespace, err)
		}
		return false
	}
	// since the 2 predefined labels are always together, we just check for only one
	// if the match label exists, we return true. otherwise we return false
	if _, ok := sts.Spec.Selector.MatchLabels["s-product"]; ok {
		return true
	}
	return false
}

func GetPredefinedLabels(product, service string) map[string]string {
	ls := make(map[string]string)
	ls["s-product"] = product
	ls["s-service"] = service
	return ls
}

func GetPredefinedClusterLabels(product, service, envName string) map[string]string {
	labels := GetPredefinedLabels(product, service)
	labels[setting.EnvNameLabel] = envName
	return labels
}

func ApplyUpdatedAnnotations(annotations map[string]string) map[string]string {
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[setting.UpdatedByLabel] = fmt.Sprintf("%d", time.Now().Unix())
	return annotations
}

func ApplySystemImagePullSecrets(podSpec *corev1.PodSpec) {
	for _, secret := range podSpec.ImagePullSecrets {
		if secret.Name == setting.DefaultImagePullSecret {
			return
		}
	}
	podSpec.ImagePullSecrets = append(podSpec.ImagePullSecrets,
		corev1.LocalObjectReference{
			Name: setting.DefaultImagePullSecret,
		})
}

func SetFieldValueIsNotExist(obj map[string]interface{}, value interface{}, fields ...string) map[string]interface{} {
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if val, ok := m[field]; ok {
			if valMap, ok := val.(map[string]interface{}); ok {
				m = valMap
			} else {
				newVal := make(map[string]interface{})
				m[field] = newVal
				m = newVal
			}
		}
	}
	m[fields[len(fields)-1]] = value
	return obj
}

// in kubernetes 1.21+, CronJobV1BetaGVK is deprecated, so we should use CronJobGVK instead
func GetValidGVK(gvk schema.GroupVersionKind, version *version.Info) schema.GroupVersionKind {
	if gvk == getter.CronJobV1BetaGVK && !kubeclient.VersionLessThan121(version) {
		return getter.CronJobGVK
	}
	return gvk
}

// removeResources removes resources currently deployed in k8s that are not in the new resource list
func removeResources(currentItems, newItems []*unstructured.Unstructured, namespace string, waitForDelete bool, kubeClient client.Client, clientSet *kubernetes.Clientset, version *version.Info, log *zap.SugaredLogger) error {
	itemsMap := make(map[string]*unstructured.Unstructured)
	errList := &multierror.Error{}
	for _, u := range newItems {
		itemsMap[fmt.Sprintf("%s/%s", u.GetKind(), u.GetName())] = u
	}

	oldItemsMap := make(map[string]*unstructured.Unstructured)
	for _, u := range currentItems {
		oldItemsMap[fmt.Sprintf("%s/%s", u.GetKind(), u.GetName())] = u
	}

	for name, item := range oldItemsMap {
		_, exists := itemsMap[name]
		item.SetNamespace(namespace)
		if exists {
			continue
		}
		if err := updater.DeleteUnstructured(item, kubeClient); err != nil {
			item.SetGroupVersionKind(GetValidGVK(item.GroupVersionKind(), version))
			errList = multierror.Append(errList, errors.Wrapf(err, "failed to remove old item %s/%s from %s", item.GetName(), item.GetKind(), namespace))
			continue
		}

		if !waitForDelete {
			continue
		}

		labelSelector := fmt.Sprintf("app=%s", item.GetName())
		watchOpts := metav1.ListOptions{
			TypeMeta:      metav1.TypeMeta{},
			LabelSelector: labelSelector,
			FieldSelector: "",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()

		var watcher watch.Interface
		var err error
		switch item.GetKind() {
		case setting.Deployment:
			watcher, err = clientSet.AppsV1().Deployments(namespace).Watch(ctx, watchOpts)
		case setting.Service:
			watcher, err = clientSet.CoreV1().Services(namespace).Watch(ctx, watchOpts)
		case setting.StatefulSet:
			watcher, err = clientSet.AppsV1().StatefulSets(namespace).Watch(ctx, watchOpts)
		case setting.Secret:
			watcher, err = clientSet.CoreV1().Secrets(namespace).Watch(ctx, watchOpts)
		case setting.ConfigMap:
			watcher, err = clientSet.CoreV1().ConfigMaps(namespace).Watch(ctx, watchOpts)
		case setting.Ingress:
			watcher, err = clientSet.ExtensionsV1beta1().Ingresses(namespace).Watch(ctx, watchOpts)
		case setting.PersistentVolumeClaim:
			watcher, err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Watch(ctx, watchOpts)
		case setting.Pod:
			watcher, err = clientSet.CoreV1().Pods(namespace).Watch(ctx, watchOpts)
		case setting.ReplicaSet:
			watcher, err = clientSet.AppsV1().ReplicaSets(namespace).Watch(ctx, watchOpts)
		case setting.Job:
			watcher, err = clientSet.BatchV1().Jobs(namespace).Watch(ctx, watchOpts)
		case setting.CronJob:
			watcher, err = clientSet.BatchV1beta1().CronJobs(namespace).Watch(ctx, watchOpts)
		case setting.ClusterRoleBinding:
			watcher, err = clientSet.RbacV1().ClusterRoleBindings().Watch(ctx, watchOpts)
		case setting.ServiceAccount:
			watcher, err = clientSet.CoreV1().ServiceAccounts(namespace).Watch(ctx, watchOpts)
		case setting.ClusterRole:
			watcher, err = clientSet.RbacV1().ClusterRoles().Watch(ctx, watchOpts)
		case setting.Role:
			watcher, err = clientSet.RbacV1().Roles(namespace).Watch(ctx, watchOpts)
		case setting.RoleBinding:
			watcher, err = clientSet.RbacV1().RoleBindings(namespace).Watch(ctx, watchOpts)
		default:
			err = fmt.Errorf("unknown kind %s to watch", item.GetKind())
			log.Error(err)
		}
		if err != nil {
			log.Errorf("failed to watch %s in namespace %s, error: %v", item.GetKind(), namespace, err)
			continue
		}
		defer watcher.Stop()

	FOR:
		for {
			select {
			case event := <-watcher.ResultChan():
				if event.Type == watch.Deleted {
					log.Infof("succeed to remove old item %s/%v from %s", item.GetName(), item.GroupVersionKind(), namespace)
					break FOR
				}
			case <-ctx.Done():
				log.Error("Context timeout for delete %s/%s", item.GetKind(), item.GetName())
				break FOR
			}
		}
	}

	return errList.ErrorOrNil()
}

func ManifestToResource(manifest string) ([]*commonmodels.ServiceResource, error) {
	unstructuredList, _, err := ManifestToUnstructured(manifest)
	if err != nil {
		return nil, err
	}
	return UnstructuredToResources(unstructuredList), nil
}

func UnstructuredToResources(unstructured []*unstructured.Unstructured) []*commonmodels.ServiceResource {
	ret := make([]*commonmodels.ServiceResource, 0)
	for _, res := range unstructured {
		ret = append(ret, &commonmodels.ServiceResource{
			GroupVersionKind: res.GroupVersionKind(),
			Name:             res.GetName(),
		})
	}
	return ret
}

type Resource struct {
	mainfest     string
	unstructured *unstructured.Unstructured
}

func ManifestToUnstructured(manifest string) ([]*unstructured.Unstructured, map[string]*Resource, error) {
	if len(manifest) == 0 {
		return nil, nil, nil
	}
	manifests := releaseutil.SplitManifests(manifest)
	errList := &multierror.Error{}
	resources := []*unstructured.Unstructured{}
	resourceMap := make(map[string]*Resource)
	for _, item := range manifests {
		u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
		if err != nil {
			errList = multierror.Append(errList, err)
			continue
		}
		GVKN := fmt.Sprintf("%s-%s", u.GetObjectKind().GroupVersionKind(), u.GetName())
		resourceMap[GVKN] = &Resource{
			mainfest:     item,
			unstructured: u,
		}
		resources = append(resources, u)
	}
	return resources, resourceMap, errList.ErrorOrNil()
}

func CheckReleaseInstalledByOtherEnv(releaseNames sets.String, productInfo *commonmodels.Product) error {
	sharedNSEnvList := make(map[string]*commonmodels.Product)
	insertEnvData := func(release string, env *commonmodels.Product) {
		sharedNSEnvList[release] = env
	}
	envs, err := commonrepo.NewProductColl().ListEnvByNamespace(productInfo.ClusterID, productInfo.Namespace)
	if err != nil {
		log.Errorf("Failed to list existed namespace from the env List, error: %s", err)
		return err
	}

	for _, env := range envs {
		if env.ProductName == productInfo.ProductName && env.EnvName == productInfo.EnvName {
			continue
		}
		for _, svc := range env.GetSvcList() {
			if releaseNames.Has(svc.ReleaseName) {
				insertEnvData(svc.ReleaseName, env)
				break
			}
		}
	}

	if len(sharedNSEnvList) == 0 {
		return nil
	}
	usedEnvStr := make([]string, 0)
	for releasename, env := range sharedNSEnvList {
		usedEnvStr = append(usedEnvStr, fmt.Sprintf("%s: %s/%s", releasename, env.ProductName, env.EnvName))
	}
	return fmt.Errorf("release is installed by other envs: %v", strings.Join(usedEnvStr, ","))
}

func CheckResourceAppliedByOtherEnv(serviceYaml string, productInfo *commonmodels.Product, serviceName string) error {
	unstructuredRes, _, err := ManifestToUnstructured(serviceYaml)
	if err != nil {
		return fmt.Errorf("failed to convert manifest to resource, error: %v", err)
	}

	sharedNSEnvList := make(map[string]*commonmodels.Product)
	insertEnvData := func(resource string, env *commonmodels.Product) {
		sharedNSEnvList[resource] = env
	}

	resSet := sets.NewString()
	resources := UnstructuredToResources(unstructuredRes)

	for _, res := range resources {
		resSet.Insert(res.String())
	}
	log.Infof("checkResourceAppliedByOtherEnv %s/%s, clusterID: %s, namespace: %s, resource: %v ", productInfo.ProductName, productInfo.EnvName, productInfo.ClusterID, productInfo.Namespace, resSet.List())

	envs, err := commonrepo.NewProductColl().ListEnvByNamespace(productInfo.ClusterID, productInfo.Namespace)
	if err != nil {
		log.Errorf("Failed to list existed namespace from the env List, error: %s", err)
		return err
	}

	for _, env := range envs {
		for _, svc := range env.GetServiceMap() {
			if env.ProductName == productInfo.ProductName && env.EnvName == productInfo.EnvName && svc.ServiceName == serviceName {
				continue
			}
			for _, res := range svc.Resources {
				if resSet.Has(res.String()) {
					insertEnvData(res.String(), env)
					break
				}
			}
		}
	}

	if len(sharedNSEnvList) == 0 {
		return nil
	}

	usedEnvStr := make([]string, 0)
	for resource, env := range sharedNSEnvList {
		usedEnvStr = append(usedEnvStr, fmt.Sprintf("%s: %s/%s", resource, env.ProductName, env.EnvName))
	}
	return fmt.Errorf("resource is applied by other envs: %v", strings.Join(usedEnvStr, ","))
}

// CreateOrPatchResource create or patch resources defined in UpdateResourceYaml
// `CurrentResourceYaml` will be used to determine if some resources will be deleted
func CreateOrPatchResource(applyParam *ResourceApplyParam, log *zap.SugaredLogger) ([]*unstructured.Unstructured, error) {
	var err error
	productInfo := applyParam.ProductInfo
	namespace, productName, envName := productInfo.Namespace, productInfo.ProductName, productInfo.EnvName
	informer := applyParam.Informer
	kubeClient := applyParam.KubeClient
	istioClient := applyParam.IstioClient

	clientSet, errGetClientSet := clientmanager.NewKubeClientManager().GetKubernetesClientSet(productInfo.ClusterID)
	if errGetClientSet != nil {
		err = errors.WithMessagef(errGetClientSet, "failed to init k8s clientset")
		return nil, err
	}
	versionInfo, errGetVersion := clientSet.ServerVersion()
	if errGetVersion != nil {
		err = errors.WithMessagef(errGetVersion, "failed to get k8s server version")
		return nil, err
	}

	curResources, curResourceMap, err := ManifestToUnstructured(applyParam.CurrentResourceYaml)
	if err != nil {
		log.Errorf("Failed to convert currently deplyed resource yaml to Unstructured, manifest is\n%s\n, error: %v", applyParam.CurrentResourceYaml, err)
		return nil, err
	}

	updateResources, updateResourceMap, err := ManifestToUnstructured(applyParam.UpdateResourceYaml)
	if err != nil {
		log.Errorf("Failed to convert yaml to Unstructured, manifest is\n%s\n, error: %v", applyParam.UpdateResourceYaml, err)
		return nil, err
	}

	if applyParam.Uninstall {
		if !commonutil.ServiceDeployed(applyParam.ServiceName, productInfo.ServiceDeployStrategy) {
			return nil, nil
		}

		err = removeResources(curResources, updateResources, namespace, applyParam.WaitForUninstall, applyParam.KubeClient, clientSet, versionInfo, log)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to remove old resources")
		}
		return nil, nil
	}

	// calc resources to be removed and resources to be created or updated
	removeRes := []*unstructured.Unstructured{}
	unchangedResources := []*unstructured.Unstructured{}
	for GVKN, cr := range curResourceMap {
		r, ok := updateResourceMap[GVKN]
		if !ok {
			removeRes = append(removeRes, cr.unstructured)
		} else {
			if r.mainfest == cr.mainfest {
				unchangedResources = append(unchangedResources, cr.unstructured)
				delete(updateResourceMap, GVKN)
			}
		}
	}
	updateResources = []*unstructured.Unstructured{}
	for _, r := range updateResourceMap {
		updateResources = append(updateResources, r.unstructured)
	}

	err = removeResources(removeRes, nil, namespace, applyParam.WaitForUninstall, applyParam.KubeClient, clientSet, versionInfo, log)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to remove old resources")
	}

	labels := GetPredefinedLabels(productName, applyParam.ServiceName)
	clusterLabels := GetPredefinedClusterLabels(productName, applyParam.ServiceName, envName)
	if !applyParam.AddZadigLabel {
		labels = map[string]string{}
		clusterLabels = map[string]string{}
	}

	var res []*unstructured.Unstructured
	errList := &multierror.Error{}

	for _, u := range unchangedResources {
		res = append(res, u)
	}
	for _, u := range updateResources {
		switch u.GetKind() {
		case setting.Ingress:
			ls := MergeLabels(labels, u.GetLabels())

			u.SetNamespace(namespace)
			u.SetLabels(ls)

			err = updater.CreateOrPatchUnstructured(u, kubeClient)
			if err != nil {
				log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), u, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}

		case setting.Service:
			u.SetNamespace(namespace)
			u.SetLabels(MergeLabels(labels, u.GetLabels()))

			err = updater.CreateOrPatchUnstructured(u, kubeClient)
			if err != nil {
				log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), u, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}

			if istioClient != nil && applyParam.SharedEnvHandler != nil {
				err = applyParam.SharedEnvHandler(context.TODO(), productInfo, u.GetName(), kubeClient, istioClient)
				if err != nil {
					log.Errorf("Failed to update Zadig service %s for env %s of product %s: %s", u.GetName(), productInfo.EnvName, productInfo.ProductName, err)
					errList = multierror.Append(errList, err)
					continue
				}
			}
			if istioClient != nil && applyParam.IstioGrayscaleEnvHandler != nil {
				err = applyParam.IstioGrayscaleEnvHandler(context.TODO(), productInfo, u.GetName(), kubeClient, istioClient)
				if err != nil {
					log.Errorf("Failed to update grayscale service %s for env %s of product %s: %s", u.GetName(), productInfo.EnvName, productInfo.ProductName, err)
					errList = multierror.Append(errList, err)
					continue
				}
			}
		case setting.Deployment, setting.StatefulSet:
			// compatibility flag, We add a match label in spec.selector field pre 1.10.
			needSelectorLabel := false

			u.SetNamespace(namespace)
			u.SetLabels(MergeLabels(labels, u.GetLabels()))

			switch u.GetKind() {
			case setting.Deployment:
				needSelectorLabel = DeploymentSelectorLabelExists(u.GetName(), namespace, informer, log)
			case setting.StatefulSet:
				needSelectorLabel = StatefulsetSelectorLabelExists(u.GetName(), namespace, informer, log)
			}

			podLabels, _, err := unstructured.NestedStringMap(u.Object, "spec", "template", "metadata", "labels")
			if err != nil {
				log.Errorf("get pod labels failed err: %v", err)
				podLabels = nil
			}
			err = unstructured.SetNestedStringMap(u.Object, MergeLabels(labels, podLabels), "spec", "template", "metadata", "labels")
			if err != nil {
				log.Errorf("merge label failed err:%s", err)
				u.Object = SetFieldValueIsNotExist(u.Object, MergeLabels(labels, podLabels), "spec", "template", "metadata", "labels")
			}

			podAnnotations, _, err := unstructured.NestedStringMap(u.Object, "spec", "template", "metadata", "annotations")
			if err != nil {
				log.Errorf("get pod annotations failed err: %v", err)
				podAnnotations = nil
			}
			err = unstructured.SetNestedStringMap(u.Object, ApplyUpdatedAnnotations(podAnnotations), "spec", "template", "metadata", "annotations")
			if err != nil {
				log.Errorf("merge annotation failed err:%s", err)
				u.Object = SetFieldValueIsNotExist(u.Object, ApplyUpdatedAnnotations(podAnnotations), "spec", "template", "metadata", "annotations")
			}

			if needSelectorLabel {
				// Inject selector: s-product and s-service
				selector, _, err := unstructured.NestedStringMap(u.Object, "spec", "selector", "matchLabels")
				if err != nil {
					selector = nil
				}

				err = unstructured.SetNestedStringMap(u.Object, MergeLabels(labels, selector), "spec", "selector", "matchLabels")
				if err != nil {
					log.Errorf("merge selector failed err:%s", err)
					u.Object = SetFieldValueIsNotExist(u.Object, MergeLabels(labels, selector), "spec", "selector", "matchLabels")
				}
			}

			jsonData, err := u.MarshalJSON()
			if err != nil {
				log.Errorf("Failed to marshal JSON, manifest is\n%v\n, error: %v", u, err)
				errList = multierror.Append(errList, err)
				continue
			}
			obj, err := serializer.NewDecoder().JSONToRuntimeObject(jsonData)
			if err != nil {
				log.Errorf("Failed to convert JSON to Object, manifest is\n%v\n, error: %v", u, err)
				errList = multierror.Append(errList, err)
				continue
			}

			switch res := obj.(type) {
			case *appsv1.Deployment:
				// Inject imagePullSecrets if qn-registry-secret is not set
				if applyParam.InjectSecrets {
					ApplySystemImagePullSecrets(&res.Spec.Template.Spec)
				}

				err = updater.CreateOrPatchDeployment(res, kubeClient)
				if err != nil {
					log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), res, err)
					errList = multierror.Append(errList, err)
					continue
				}
			case *appsv1.StatefulSet:
				// Inject imagePullSecrets if qn-registry-secret is not set
				if applyParam.InjectSecrets {
					ApplySystemImagePullSecrets(&res.Spec.Template.Spec)
				}

				err = updater.CreateOrPatchStatefulSet(res, kubeClient)
				if err != nil {
					log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), res, err)
					errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
					continue
				}
			default:
				errList = multierror.Append(errList, fmt.Errorf("object is not a appsv1.Deployment or appsv1.StatefulSet"))
				continue
			}

		case setting.Job:
			jsonData, err := u.MarshalJSON()
			if err != nil {
				log.Errorf("Failed to marshal JSON, manifest is\n%v\n, error: %v", u, err)
				errList = multierror.Append(errList, err)
				continue
			}
			obj, err := serializer.NewDecoder().JSONToJob(jsonData)
			if err != nil {
				log.Errorf("Failed to convert JSON to Job, manifest is\n%v\n, error: %v", u, err)
				errList = multierror.Append(errList, err)
				continue
			}

			obj.Namespace = namespace
			obj.ObjectMeta.Labels = MergeLabels(labels, obj.ObjectMeta.Labels)
			obj.Spec.Template.ObjectMeta.Labels = MergeLabels(labels, obj.Spec.Template.ObjectMeta.Labels)

			// Inject imagePullSecrets if qn-registry-secret is not set
			if applyParam.InjectSecrets {
				ApplySystemImagePullSecrets(&obj.Spec.Template.Spec)
			}

			if err := updater.DeleteJobAndWait(namespace, obj.Name, kubeClient); err != nil {
				log.Errorf("Failed to delete Job, error: %v", err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}

			if err := updater.CreateJob(obj, kubeClient); err != nil {
				log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), obj, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}

		case setting.CronJob:
			jsonData, err := u.MarshalJSON()
			if err != nil {
				log.Errorf("Failed to marshal JSON, manifest is\n%v\n, error: %v", u, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}
			if u.GetAPIVersion() == batchv1.SchemeGroupVersion.String() {
				obj, err := serializer.NewDecoder().JSONToCronJob(jsonData)
				if err != nil {
					log.Errorf("Failed to convert JSON to CronJob, manifest is\n%v\n, error: %v", u, err)
					errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
					continue
				}

				obj.Namespace = namespace
				obj.ObjectMeta.Labels = MergeLabels(labels, obj.ObjectMeta.Labels)
				obj.Spec.JobTemplate.ObjectMeta.Labels = MergeLabels(labels, obj.Spec.JobTemplate.ObjectMeta.Labels)
				obj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels = MergeLabels(labels, obj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels)

				// Inject imagePullSecrets if qn-registry-secret is not set
				if applyParam.InjectSecrets {
					ApplySystemImagePullSecrets(&obj.Spec.JobTemplate.Spec.Template.Spec)
				}

				err = updater.CreateOrPatchCronJob(obj, kubeClient)
				if err != nil {
					log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), obj, err)
					errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
					continue
				}
			} else {
				obj, err := serializer.NewDecoder().JSONToCronJobBeta(jsonData)
				if err != nil {
					log.Errorf("Failed to convert JSON to CronJobBeta, manifest is\n%v\n, error: %v", u, err)
					errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
					continue
				}

				obj.Namespace = namespace
				obj.ObjectMeta.Labels = MergeLabels(labels, obj.ObjectMeta.Labels)
				obj.Spec.JobTemplate.ObjectMeta.Labels = MergeLabels(labels, obj.Spec.JobTemplate.ObjectMeta.Labels)
				obj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels = MergeLabels(labels, obj.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels)

				// Inject imagePullSecrets if qn-registry-secret is not set
				if applyParam.InjectSecrets {
					ApplySystemImagePullSecrets(&obj.Spec.JobTemplate.Spec.Template.Spec)
				}

				err = updater.CreateOrPatchCronJob(obj, kubeClient)
				if err != nil {
					log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), obj, err)
					errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
					continue
				}
			}

		case setting.ClusterRole, setting.ClusterRoleBinding:
			u.SetLabels(MergeLabels(clusterLabels, u.GetLabels()))

			err = updater.CreateOrPatchUnstructured(u, kubeClient)
			if err != nil {
				log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), u, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}
		default:
			u.SetNamespace(namespace)
			u.SetLabels(MergeLabels(labels, u.GetLabels()))

			err = updater.CreateOrPatchUnstructured(u, kubeClient)
			if err != nil {
				log.Errorf("Failed to create or update %s, manifest is\n%v\n, error: %v", u.GetKind(), u, err)
				errList = multierror.Append(errList, errors.Wrapf(err, "failed to create or update %s/%s", u.GetKind(), u.GetName()))
				continue
			}
		}

		res = append(res, u)
	}

	if errList.ErrorOrNil() != nil {
		return res, errList.ErrorOrNil()
	}

	return res, nil
}

// func PrepareHelmServiceData(applyParam *ResourceApplyParam) (*commonmodels.ProductService, *commonmodels.Service, error) {
// 	productInfo := applyParam.ProductInfo
// 	productService := applyParam.ProductInfo.GetServiceMap()[applyParam.ServiceName]
// 	if productService == nil {
// 		if !applyParam.UpdateServiceRevision {
// 			return nil, nil, fmt.Errorf("first time online service %s needs to check the update service configuration", applyParam.ServiceName)
// 		}

// 		productService = &commonmodels.ProductService{
// 			ProductName: applyParam.ProductInfo.ProductName,
// 			ServiceName: applyParam.ServiceName,
// 			Type:        setting.HelmDeployType,
// 		}
// 		if len(productInfo.Services) == 0 {
// 			productInfo.Services = [][]*commonmodels.ProductService{{}}
// 		}
// 		productInfo.Services[0] = append(productInfo.Services[0], productService)
// 	}

// 	svcFindOption := &commonrepo.ServiceFindOption{
// 		ProductName: applyParam.ProductInfo.ProductName,
// 		ServiceName: applyParam.ServiceName,
// 		Revision:    productService.Revision,
// 	}
// 	// use latest svc template if option 'UpdateServiceRevision' is true
// 	if applyParam.UpdateServiceRevision {
// 		svcFindOption.Revision = 0
// 	}

// 	svcTemplate, err := repository.QueryTemplateService(svcFindOption, productInfo.Production)
// 	if err != nil {
// 		return nil, nil, errors.Wrapf(err, "failed to find service %s/%d in product %s", applyParam.ServiceName, svcFindOption.Revision, productInfo.ProductName)
// 	}

// 	targetChart := productInfo.GetChartRenderMap()[applyParam.ServiceName]
// 	if targetChart == nil {
// 		targetChart = &template.ServiceRender{
// 			ServiceName:  applyParam.ServiceName,
// 			ValuesYaml:   svcTemplate.HelmChart.ValuesYaml,
// 			ChartVersion: svcTemplate.HelmChart.Version,
// 			OverrideYaml: &template.CustomYaml{},
// 		}
// 		productService.Render = targetChart
// 	} else {
// 		productService.GetServiceRender().ValuesYaml = svcTemplate.HelmChart.ValuesYaml
// 		productService.GetServiceRender().ChartVersion = svcTemplate.HelmChart.Version
// 	}

// 	if applyParam.UpdateServiceRevision && productService.Revision != svcTemplate.Revision {
// 		// reuse the images in the product service if it is not empty
// 		imageMap := make(map[string]string)
// 		for _, container := range productService.Containers {
// 			imageMap[container.ImageName] = container.Image
// 		}
// 		productService.Containers = svcTemplate.Containers
// 		for _, container := range productService.Containers {
// 			if image, ok := imageMap[container.ImageName]; ok {
// 				container.Image = image
// 			}
// 		}

// 		imageValuesMaps := make([]map[string]interface{}, 0)
// 		for _, targetContainer := range productService.Containers {
// 			// prepare image replace info
// 			replaceValuesMap, err := commonutil.AssignImageData(targetContainer.Image, commonutil.GetValidMatchData(targetContainer.ImagePath))
// 			if err != nil {
// 				return nil, nil, fmt.Errorf("failed to pase image uri %s/%s, err %s", productInfo.ProductName, applyParam.ServiceName, err.Error())
// 			}
// 			imageValuesMaps = append(imageValuesMaps, replaceValuesMap)
// 		}

// 		// replace image into service's values.yaml
// 		replacedValuesYaml, err := commonutil.ReplaceImage(svcTemplate.HelmChart.ValuesYaml, imageValuesMaps...)
// 		if err != nil {
// 			return nil, nil, fmt.Errorf("failed to replace image uri %s/%s, err %s", productInfo.ProductName, applyParam.ServiceName, err.Error())

// 		}
// 		if replacedValuesYaml == "" {
// 			return nil, nil, fmt.Errorf("failed to set new image uri into service's values.yaml %s/%s", productInfo.ProductName, applyParam.ServiceName)
// 		}
// 		targetChart.ValuesYaml = replacedValuesYaml
// 	}

// 	productService.Revision = svcTemplate.Revision

// 	return productService, svcTemplate, nil
// }

// RemoveHelmResource create or patch helm services
// if service is not deployed ever, it will be added into target environment
// database will also be updated
func RemoveHelmResource(applyParam *ResourceApplyParam, log *zap.SugaredLogger) error {
	productInfo := applyParam.ProductInfo
	// uninstall release
	if applyParam.Uninstall {
		// we need to find the product info from db again to ensure product latest
		productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
			Name:    applyParam.ProductInfo.ProductName,
			EnvName: productInfo.EnvName,
		})
		if err != nil {
			return fmt.Errorf("failed to find product: %s/%s, err: %v", applyParam.ProductInfo.ProductName, productInfo.EnvName, err)
		}
		targetServices := applyParam.ServiceList
		if len(targetServices) == 0 {
			targetServices = []string{applyParam.ServiceName}
		}
		return DeleteHelmReleaseFromEnv("workflow", "", productInfo, targetServices, log)
	}
	return nil
}
