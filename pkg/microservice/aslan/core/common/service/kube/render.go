/*
Copyright 2022 The KodeRover Authors.

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
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/helm/pkg/releaseutil"
	yaml "sigs.k8s.io/yaml/goyaml.v3"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/koderover/zadig/v2/pkg/util/converter"
)

type GeneSvcYamlOption struct {
	ProductName           string
	EnvName               string
	ServiceName           string
	UpdateServiceRevision bool
	VariableYaml          string
	VariableKVs           []*commontypes.RenderVariableKV
	UnInstall             bool
	Containers            []*models.Container
}

type WorkloadResource struct {
	Type string
	Name string
}

func GeneKVFromYaml(yamlContent string) ([]*commonmodels.VariableKV, error) {
	if len(yamlContent) == 0 {
		return nil, nil
	}
	flatMap, err := converter.YamlToFlatMap([]byte(yamlContent))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert yaml to flat map")
	} else {
		kvs := make([]*commonmodels.VariableKV, 0)
		for k, v := range flatMap {
			if len(k) == 0 {
				continue
			}
			kvs = append(kvs, &models.VariableKV{
				Key:   k,
				Value: v,
			})
		}
		return kvs, nil
	}
}

func GenerateYamlFromKV(kvs []*commonmodels.VariableKV) (string, error) {
	if len(kvs) == 0 {
		return "", nil
	}
	flatMap := make(map[string]interface{})
	for _, kv := range kvs {
		flatMap[kv.Key] = kv.Value
	}

	validKvMap, err := converter.Expand(flatMap)
	if err != nil {
		return "", errors.Wrapf(err, "failed to expand flat map")
	}

	bs, err := yaml.Marshal(validKvMap)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal map to yaml")
	}
	return string(bs), nil
}

func resourceToYaml(obj runtime.Object) (string, error) {
	y := printers.YAMLPrinter{}
	writer := bytes.NewBuffer(nil)
	err := y.PrintObj(obj, writer)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal object to yaml")
	}
	return writer.String(), nil
}

// ReplaceWorkloadImages  replace images in yaml with new images
func ReplaceWorkloadImages(rawYaml string, images []*commonmodels.Container) (string, []*WorkloadResource, error) {
	imageMap := make(map[string]*commonmodels.Container)
	for _, image := range images {
		imageMap[image.Name] = image
	}

	customKVRegExp := regexp.MustCompile(`{{\.(\w+(\.\w+)*)}}`)
	restoreRegExp := regexp.MustCompile(`TEMP_PLACEHOLDER_(\w+(\.\w+)*)`)

	splitYams := util.SplitYaml(rawYaml)
	yamlStrs := make([]string, 0)
	workloadRes := make([]*WorkloadResource, 0)
	for _, yamlStr := range splitYams {
		modifiedYamlStr := customKVRegExp.ReplaceAll([]byte(yamlStr), []byte("TEMP_PLACEHOLDER_$1"))

		var rawData map[string]interface{}
		err := yaml.Unmarshal(modifiedYamlStr, &rawData)
		if err != nil {
			return "", nil, fmt.Errorf("decode yaml error: %s", err)
		}

		cleanedData := util.ConvertIntsToInt64(rawData).(map[string]interface{})

		var obj unstructured.Unstructured
		obj.Object = cleanedData

		resourceName := obj.GetName()
		resourceKind := obj.GetKind()

		workloadRes = append(workloadRes, &WorkloadResource{
			Name: resourceName,
			Type: resourceKind,
		})

		switch obj.GetKind() {
		case setting.Deployment, setting.StatefulSet, setting.Job:
			containers, _, err := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to find containers in deployment, sts or job, error: %s", err)
			}

			for i, c := range containers {
				container := c.(map[string]interface{})
				containerName := container["name"].(string)
				if image, ok := imageMap[containerName]; ok {
					container["image"] = image.Image
					containers[i] = container
				}
			}

			err = unstructured.SetNestedSlice(obj.Object, containers, "spec", "template", "spec", "containers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to set containers in deployment, sts or job, error: %s", err)
			}

			initContainers, _, err := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "initContainers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to find init containers in deployment, sts or job, error: %s", err)
			}

			for i, c := range initContainers {
				container := c.(map[interface{}]interface{})
				containerName := container["name"].(string)
				if image, ok := imageMap[containerName]; ok {
					container["image"] = image.Image
					initContainers[i] = container
				}
			}

			err = unstructured.SetNestedSlice(obj.Object, initContainers, "spec", "template", "spec", "initContainers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to set init containers in deployment, sts or job, error: %s", err)
			}

		case setting.CronJob:
			containers, _, err := unstructured.NestedSlice(obj.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to find containers in cronjob, error: %s", err)
			}

			for i, c := range containers {
				container := c.(map[interface{}]interface{})
				containerName := container["name"].(string)
				if image, ok := imageMap[containerName]; ok {
					container["image"] = image.Image
					containers[i] = container
				}
			}

			err = unstructured.SetNestedSlice(obj.Object, containers, "spec", "jobTemplate", "spec", "template", "spec", "containers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to set containers in cronjob, error: %s", err)
			}

			initContainers, _, err := unstructured.NestedSlice(obj.Object, "spec", "jobTemplate", "spec", "template", "spec", "initContainers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to find init containers in cronjob, error: %s", err)
			}

			for i, c := range initContainers {
				container := c.(map[interface{}]interface{})
				containerName := container["name"].(string)
				if image, ok := imageMap[containerName]; ok {
					container["image"] = image.Image
					initContainers[i] = container
				}
			}

			err = unstructured.SetNestedSlice(obj.Object, initContainers, "spec", "jobTemplate", "spec", "template", "spec", "initContainers")
			if err != nil {
				return "", nil, fmt.Errorf("failed to set init containers in cronjob, error: %s", err)
			}
		}

		updatedYaml, err := yaml.Marshal(obj.Object)
		if err != nil {
			return "", nil, fmt.Errorf("updated resource cannot be marshaled into a YAML, error: %s", err)
		}

		finalYaml := restoreRegExp.ReplaceAll(updatedYaml, []byte("{{.$1}}"))
		yamlStrs = append(yamlStrs, string(finalYaml))
	}

	return util.JoinYamls(yamlStrs), workloadRes, nil
}

func buildContainerMap(cs []*models.Container) map[string]*models.Container {
	containerMap := make(map[string]*models.Container)
	for _, c := range cs {
		containerMap[c.Name] = c
	}
	return containerMap
}

// CalculateContainer calculates containers to be applied into environments for helm and k8s projects
// if image has no change since last deploy, containers in latest service will be used
// if image hse been change since lase deploy (eg. workflow), current values will be remained
func CalculateContainer(productSvc *commonmodels.ProductService, curUsedSvc *commonmodels.Service, latestContainers []*models.Container, productInfo *commonmodels.Product) []*models.Container {
	resp := make([]*models.Container, 0)

	if productInfo == nil {
		return latestContainers
	}
	if curUsedSvc == nil {
		return productSvc.Containers
	}

	prodSvcContainers := buildContainerMap(productSvc.Containers)
	prodTmpContainers := buildContainerMap(curUsedSvc.Containers)

	for _, container := range latestContainers {
		prodSvcContainer, _ := prodSvcContainers[container.Name]
		prodTmpContainer, _ := prodTmpContainers[container.Name]
		// image has changed in zadig since last deploy
		if prodSvcContainer != nil && prodTmpContainer != nil && prodSvcContainer.Image != prodTmpContainer.Image {
			container.Image = prodSvcContainer.Image
			container.ImageName = prodSvcContainer.ImageName
		}
		resp = append(resp, container)
	}

	return resp
}

func mergeContainers(curContainers []*commonmodels.Container, newContainers ...[]*commonmodels.Container) []*commonmodels.Container {
	curContainerMap := make(map[string]*commonmodels.Container)
	for _, container := range curContainers {
		curContainerMap[container.Name] = container
	}
	for _, containers := range newContainers {
		for _, container := range containers {
			curContainerMap[container.Name] = container
		}
	}
	var containers []*commonmodels.Container
	for _, container := range curContainerMap {
		containers = append(containers, container)
	}
	return containers
}

func MergeImages(curContainers []*models.Container, images []string) []string {
	ret := make([]string, 0)
	containerMap := make(map[string]string)
	for _, container := range curContainers {
		containerMap[container.ImageName] = container.Image
	}

	imageMap := make(map[string]string)
	for _, image := range images {
		imageMap[util.ExtractImageName(image)] = image
	}

	for imageName, image := range imageMap {
		if len(imageName) == 0 {
			continue
		}
		containerMap[imageName] = image
	}

	for _, image := range containerMap {
		ret = append(ret, image)
	}
	return ret
}

// FetchCurrentAppliedYaml generates full yaml of some service currently applied in Zadig
// and returns the service yaml, currently used service revision
func FetchCurrentAppliedYaml(option *GeneSvcYamlOption) (string, int, error) {
	_, err := templaterepo.NewProductColl().Find(option.ProductName)
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to find template product %s", option.ProductName)
	}

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName: option.EnvName,
		Name:    option.ProductName,
	})
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to find product %s", option.ProductName)
	}

	curProductSvc := productInfo.GetServiceMap()[option.ServiceName]

	// service not installed, nothing to return
	if curProductSvc == nil {
		return "", 0, nil
	}

	prodSvcTemplate, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ProductName: option.ProductName,
		ServiceName: option.ServiceName,
		Revision:    curProductSvc.Revision,
	}, productInfo.Production)
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to find service %s with revision %d", option.ServiceName, curProductSvc.Revision)
	}

	fullRenderedYaml, err := RenderServiceYaml(prodSvcTemplate.Yaml, option.ProductName, option.ServiceName, curProductSvc.GetServiceRender())
	if err != nil {
		return "", 0, err
	}
	fullRenderedYaml = ParseSysKeys(productInfo.Namespace, productInfo.EnvName, option.ProductName, option.ServiceName, fullRenderedYaml)
	mergedContainers := mergeContainers(prodSvcTemplate.Containers, curProductSvc.Containers)
	fullRenderedYaml, _, err = ReplaceWorkloadImages(fullRenderedYaml, mergedContainers)
	return fullRenderedYaml, 0, nil
}

func fetchImportedManifests(option *GeneSvcYamlOption, productInfo *models.Product, serviceTmp *models.Service, svcRender *template.ServiceRender) (string, []*WorkloadResource, error) {
	fullRenderedYaml, err := RenderServiceYaml(serviceTmp.Yaml, option.ProductName, option.ServiceName, svcRender)
	if err != nil {
		return "", nil, err
	}
	fullRenderedYaml = ParseSysKeys(productInfo.Namespace, productInfo.EnvName, option.ProductName, option.ServiceName, fullRenderedYaml)

	manifests := releaseutil.SplitManifests(fullRenderedYaml)

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(productInfo.ClusterID)
	if err != nil {
		log.Errorf("cluster is not connected [%s]", productInfo.ClusterID)
		return "", nil, errors.Wrapf(err, "cluster is not connected [%s]", productInfo.ClusterID)
	}

	clientset, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(productInfo.ClusterID)
	if err != nil {
		log.Errorf("get client set error: %v", err)
		return "", nil, err
	}
	versionInfo, err := clientset.Discovery().ServerVersion()
	if err != nil {
		log.Errorf("get server version error: %v", err)
		return "", nil, err
	}
	manifestArr := make([]string, 0)

	for _, item := range manifests {
		u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
		if err != nil {
			return "", nil, errors.Wrapf(err, "failed to decode yaml %s", item)
		}
		switch u.GetKind() {
		case setting.Deployment:
			workloadBs, exist, err := getter.GetDeploymentYamlFormat(productInfo.Namespace, u.GetName(), kubeClient)
			if err != nil {
				return "", nil, errors.Wrapf(err, "failed to get deployment %s", u.GetName())
			}
			if !exist {
				return "", nil, errors.Errorf("deployment %s not found", u.GetName())
			}
			manifestArr = append(manifestArr, string(workloadBs))
		case setting.StatefulSet:
			workloadBs, exist, err := getter.GetStatefulSetYamlFormat(productInfo.Namespace, u.GetName(), kubeClient)
			if err != nil {
				return "", nil, errors.Wrapf(err, "failed to get statefulset %s", u.GetName())
			}
			if !exist {
				return "", nil, errors.Errorf("statefulset %s not found", u.GetName())
			}
			manifestArr = append(manifestArr, string(workloadBs))
		case setting.CronJob:
			workloadBs, exist, err := getter.GetCronJobYamlFormat(productInfo.Namespace, u.GetName(), kubeClient, kubeclient.VersionLessThan121(versionInfo))
			if err != nil {
				return "", nil, errors.Wrapf(err, "failed to get cronjob %s", u.GetName())
			}
			if !exist {
				return "", nil, errors.Errorf("cronjob %s not found", u.GetName())
			}
			manifestArr = append(manifestArr, string(workloadBs))
		}
	}
	return ReplaceWorkloadImages(util.JoinYamls(manifestArr), option.Containers)
}

func variableYamlNil(variableYaml string) bool {
	if len(variableYaml) == 0 {
		return true
	}
	kvMap, _ := converter.YamlToFlatMap([]byte(variableYaml))
	return len(kvMap) == 0
}

// GenerateRenderedYaml generates full yaml of some service defined in Zadig (images not included)
// and returns the service yaml, used service revision
func GenerateRenderedYaml(option *GeneSvcYamlOption) (string, int, []*WorkloadResource, error) {
	_, err := templaterepo.NewProductColl().Find(option.ProductName)
	if err != nil {
		return "", 0, nil, errors.Wrapf(err, "failed to find template product %s", option.ProductName)
	}

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName: option.EnvName,
		Name:    option.ProductName,
	})
	if err != nil {
		return "", 0, nil, errors.Wrapf(err, "failed to find product %s", option.ProductName)
	}

	curProductSvc := productInfo.GetServiceMap()[option.ServiceName]

	// nothing to render when trying to uninstall a service which is not deployed
	if option.UnInstall && curProductSvc == nil {
		return "", 0, nil, nil
	}

	var prodSvcTemplate, latestSvcTemplate *commonmodels.Service

	latestSvcTemplate, err = repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ProductName:         option.ProductName,
		ServiceName:         option.ServiceName,
		ExcludeStatus:       setting.ProductStatusDeleting,
		Revision:            0,
		IgnoreNoDocumentErr: true,
	}, productInfo.Production)
	if err != nil {
		return "", 0, nil, errors.Wrapf(err, "failed to find latest service template %s", option.ServiceName)
	}

	var svcContainersInProduct []*models.Container
	if curProductSvc != nil {
		svcContainersInProduct = curProductSvc.Containers
		prodSvcTemplate, err = repository.QueryTemplateService(&commonrepo.ServiceFindOption{
			ProductName: option.ProductName,
			ServiceName: option.ServiceName,
			Revision:    curProductSvc.Revision,
		}, productInfo.Production)
		if err != nil {
			return "", 0, nil, errors.Wrapf(err, "failed to find service %s with revision %d", option.ServiceName, curProductSvc.Revision)
		}
	} else {
		prodSvcTemplate = latestSvcTemplate
	}

	// use latest service revision
	if latestSvcTemplate == nil {
		latestSvcTemplate = prodSvcTemplate
	}
	if !option.UpdateServiceRevision {
		latestSvcTemplate = prodSvcTemplate
	}

	if latestSvcTemplate == nil {
		return "", 0, nil, fmt.Errorf("failed to find service template for service %s, isProduction %v", option.ServiceName, productInfo.Production)
	}

	serviceRender := productInfo.GetSvcRender(option.ServiceName)

	// service not deployed by zadig, should only be updated with images
	if !option.UnInstall && !option.UpdateServiceRevision && variableYamlNil(option.VariableYaml) && curProductSvc != nil && !commonutil.ServiceDeployed(option.ServiceName, productInfo.ServiceDeployStrategy) {
		manifest, workloads, err := fetchImportedManifests(option, productInfo, prodSvcTemplate, serviceRender)
		return manifest, int(curProductSvc.Revision), workloads, err
	}

	curContainers := latestSvcTemplate.Containers
	if curProductSvc != nil {
		curContainers = curProductSvc.Containers
		svcContainersInProduct = CalculateContainer(curProductSvc, prodSvcTemplate, latestSvcTemplate.Containers, productInfo)
	}

	renderVariableKVs := serviceRender.OverrideYaml.RenderVariableKVs

	// merge service template, renderset and option variables
	templVariableKV := commontypes.ServiceToRenderVariableKVs(latestSvcTemplate.ServiceVariableKVs)
	mergedYaml, _, err := commontypes.MergeRenderVariableKVs(templVariableKV, renderVariableKVs, option.VariableKVs)
	if err != nil {
		return "", 0, nil, errors.Wrapf(err, "failed to merge service variable yaml")
	}

	serviceRender.OverrideYaml.YamlContent = mergedYaml

	fullRenderedYaml, err := RenderServiceYaml(latestSvcTemplate.Yaml, option.ProductName, option.ServiceName, serviceRender)
	if err != nil {
		return "", 0, nil, err
	}
	fullRenderedYaml = ParseSysKeys(productInfo.Namespace, productInfo.EnvName, option.ProductName, option.ServiceName, fullRenderedYaml)

	// service may not be deployed in environment, we need to extract containers again, since image related variables may be changed
	latestSvcTemplate.KubeYamls = util.SplitYaml(fullRenderedYaml)
	commonutil.SetCurrentContainerImages(latestSvcTemplate)

	mergedContainers := mergeContainers(curContainers, latestSvcTemplate.Containers, svcContainersInProduct, option.Containers)
	fullRenderedYaml, workloadResource, err := ReplaceWorkloadImages(fullRenderedYaml, mergedContainers)
	return fullRenderedYaml, int(latestSvcTemplate.Revision), workloadResource, err
}

func RenderServiceYaml(originYaml, productName, serviceName string, svcRender *template.ServiceRender) (string, error) {
	if svcRender == nil {
		originYaml = strings.ReplaceAll(originYaml, setting.TemplateVariableProduct, productName)
		originYaml = strings.ReplaceAll(originYaml, setting.TemplateVariableService, serviceName)
		return originYaml, nil
	}
	variableYaml := svcRender.GetSafeVariable()
	return commonutil.RenderK8sSvcYamlStrict(originYaml, productName, serviceName, variableYaml)
}

// RenderEnvService renders service with particular revision and service vars in environment
func RenderEnvService(prod *commonmodels.Product, serviceRender *template.ServiceRender, service *commonmodels.ProductService) (yaml string, err error) {
	opt := &commonrepo.ServiceFindOption{
		ServiceName: service.ServiceName,
		ProductName: service.ProductName,
		Type:        service.Type,
		Revision:    service.Revision,
	}
	svcTmpl, err := repository.QueryTemplateService(opt, prod.Production)
	if err != nil {
		return "", err
	}

	return RenderEnvServiceWithTempl(prod, serviceRender, service, svcTmpl)
}

func RenderEnvServiceWithTempl(prod *commonmodels.Product, serviceRender *template.ServiceRender, service *commonmodels.ProductService, svcTmpl *commonmodels.Service) (yaml string, err error) {
	// Note only the keys in TemplateService.ServiceVar can work
	parsedYaml, err := RenderServiceYaml(svcTmpl.Yaml, prod.ProductName, svcTmpl.ServiceName, serviceRender)
	if err != nil {
		log.Errorf("failed to render service yaml, err: %s", err)
		return "", err
	}
	parsedYaml = ParseSysKeys(prod.Namespace, prod.EnvName, prod.ProductName, service.ServiceName, parsedYaml)
	parsedYaml, _, err = ReplaceWorkloadImages(parsedYaml, service.Containers)
	return parsedYaml, err
}
