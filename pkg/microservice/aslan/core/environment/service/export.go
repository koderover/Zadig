/*
Copyright 2021 The KodeRover Authors.

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

package service

import (
	"context"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/openkruise/kruise-api/client/clientset/versioned"
	"go.uber.org/zap"
	"helm.sh/helm/v3/pkg/releaseutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
)

// ExportYaml 查询使用到服务模板的服务组模板
// source determines where the request comes from, can be "wd" or "nil"
func ExportYaml(envName, productName, serviceName, source string, production bool, log *zap.SugaredLogger) []string {
	var yamls [][]byte
	res := make([]string, 0)

	env, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       productName,
		Production: &production,
	})
	if err != nil {
		log.Errorf("failed to find env [%s][%s] %v", envName, productName, err)
		return res
	}

	namespace := env.Namespace
	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(env.ClusterID)
	if err != nil {
		log.Errorf("cluster is not connected [%s][%s][%s]", env.EnvName, env.ProductName, env.ClusterID)
		return res
	}

	clientSet, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(env.ClusterID)
	if err != nil {
		log.Errorf("failed to get clientset for cluster %s", env.ClusterID)
		return res
	}
	clusterVersion, err := clientSet.Discovery().ServerVersion()
	if err != nil {
		log.Errorf("failed to get cluster version for cluster %s", env.ClusterID)
		return res
	}

	kruise, err := clientmanager.NewKubeClientManager().GetKruiseClient(env.ClusterID)
	if err != nil {
		log.Errorf("failed to get kruise for cluster %s", env.ClusterID)
		return res
	}

	// needFetchByRenderedManifest happens when service is not deployed by zadig, or is not connected to zadig (when request comes from wd)
	needFetchByRenderedManifest := false

	if commonutil.ServiceDeployed(serviceName, env.ServiceDeployStrategy) {
		selector := labels.Set{setting.ProductLabel: productName, setting.ServiceLabel: serviceName}.AsSelector()
		yamls = append(yamls, getConfigMapYaml(kubeClient, namespace, selector, log)...)
		yamls = append(yamls, getIngressYaml(kubeClient, namespace, selector, log)...)
		yamls = append(yamls, getServiceYaml(kubeClient, namespace, selector, log)...)
		deploys := getDeploymentYaml(kubeClient, namespace, selector, log)
		yamls = append(yamls, deploys...)
		stss := getStatefulSetYaml(kubeClient, namespace, selector, log)
		yamls = append(yamls, stss...)
		cronJobs := getCronJobYaml(kubeClient, namespace, selector, VersionLessThan121(clusterVersion), log)
		yamls = append(yamls, cronJobs...)
		cloneSets := getKruiseYaml(kruise, namespace, selector, log)
		yamls = append(yamls, cloneSets...)
		if len(deploys) == 0 && len(stss) == 0 && len(cronJobs) == 0 {
			if source == "wd" {
				needFetchByRenderedManifest = true
			}
		}
	} else {
		needFetchByRenderedManifest = true
	}

	if needFetchByRenderedManifest {
		yamls = make([][]byte, 0)
		// for services just import not deployed, workloads can't be queried by labels
		productService, ok := env.GetServiceMap()[serviceName]
		if !ok {
			log.Errorf("failed to find product service: %s", serviceName)
			return res
		}
		rederedYaml, err := kube.RenderEnvService(env, productService.GetServiceRender(), productService)
		if err != nil {
			log.Errorf("failed to render service yaml, err: %s", err)
			return res
		}

		manifests := releaseutil.SplitManifests(rederedYaml)
		for _, item := range manifests {
			u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
			if err != nil {
				log.Errorf("failed to convert yaml to Unstructured when check resources, manifest is\n%s\n, error: %v", item, err)
				continue
			}
			switch u.GetKind() {
			case setting.Deployment, setting.StatefulSet, setting.ConfigMap, setting.Service, setting.Ingress, setting.CronJob:
				resource, exists, err := getter.GetResourceYamlInCache(namespace, u.GetName(), u.GroupVersionKind(), kubeClient)
				if err != nil {
					log.Errorf("failed to get resource yaml, err: %s", err)
					continue
				}
				if !exists {
					continue
				}
				yamls = append(yamls, resource)
			}
		}
	}

	for _, y := range yamls {
		res = append(res, string(y))
	}
	return res
}

func getConfigMapYaml(kubeClient client.Client, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListConfigMapsYaml(namespace, selector, kubeClient)
	if err != nil {
		log.Errorf("ListConfigMaps error: %v", err)
		return nil
	}

	return resources
}

func getIngressYaml(kubeClient client.Client, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListIngressesYaml(namespace, selector, kubeClient)
	if err != nil {
		log.Errorf("ListIngresses error: %v", err)
		return nil
	}

	return resources
}

func getServiceYaml(kubeClient client.Client, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListServicesYaml(namespace, selector, kubeClient)
	if err != nil {
		log.Errorf("ListServices error: %v", err)
		return nil
	}
	return resources
}

func getDeploymentYaml(kubeClient client.Client, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListDeploymentsYaml(namespace, selector, kubeClient)
	if err != nil {
		log.Errorf("ListDeployments error: %v", err)
		return nil
	}
	return resources
}

func getKruiseYaml(kubeClient versioned.Interface, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	listOptions := metav1.ListOptions{
		LabelSelector: selector.String(),
	}

	resources, err := kubeClient.AppsV1alpha1().CloneSets(namespace).List(context.Background(), listOptions)
	if err != nil {
		log.Errorf("List CloneSet error: %v", err)
		return nil
	}

	var yamlBytes [][]byte
	for _, item := range resources.Items {
		jsonData, err := json.Marshal(item)
		if err != nil {
			log.Errorf("Failed to marshal CloneSet %s to JSON: %v", item.Name, err)
			continue
		}

		yamlData, err := yaml.JSONToYAML(jsonData)
		if err != nil {
			log.Errorf("Failed to convert CloneSet %s JSON to YAML: %v", item.Name, err)
			continue
		}

		yamlBytes = append(yamlBytes, yamlData)
	}
	return yamlBytes
}

func getStatefulSetYaml(kubeClient client.Client, namespace string, selector labels.Selector, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListStatefulSetsYaml(namespace, selector, kubeClient)
	if err != nil {
		log.Errorf("ListStatefulSets error: %v", err)
		return nil
	}
	return resources
}

func getCronJobYaml(kubeClient client.Client, namespace string, selector labels.Selector, lessThanVersion121 bool, log *zap.SugaredLogger) [][]byte {
	resources, err := getter.ListCronJobsYaml(namespace, selector, kubeClient, lessThanVersion121)
	if err != nil {
		log.Errorf("ListCronJobs error: %v", err)
		return nil
	}
	return resources
}
