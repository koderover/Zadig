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

package util

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/koderover/zadig/v2/pkg/util/converter"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

var (
	presetPatterns = []map[string]string{
		{setting.PathSearchComponentImage: "image.repository", setting.PathSearchComponentTag: "image.tag"},
		{setting.PathSearchComponentImage: "image"},
	}
)

func GetServiceDeployStrategy(serviceName string, strategyMap map[string]string) string {
	if strategyMap == nil {
		return setting.ServiceDeployStrategyDeploy
	}
	if value, ok := strategyMap[serviceName]; !ok || value == "" {
		return setting.ServiceDeployStrategyDeploy
	} else {
		return value
	}
}

func GetReleaseDeployStrategyKey(releaseName string) string {
	return fmt.Sprintf("%s%s", releaseName, setting.HelmChartDeployStrategySuffix)
}

func GetReleaseDeployStrategy(releaseName string, strategyMap map[string]string) string {
	if strategyMap == nil {
		return setting.ServiceDeployStrategyDeploy
	}
	if value, ok := strategyMap[GetReleaseDeployStrategyKey(releaseName)]; !ok || value == "" {
		return setting.ServiceDeployStrategyDeploy
	} else {
		return value
	}
}

func ChartDeployed(render *templatemodels.ServiceRender, strategyMap map[string]string) bool {
	if render.DeployedFromZadig() {
		return ServiceDeployed(render.ServiceName, strategyMap)
	} else {
		return ReleaseDeployed(render.ReleaseName, strategyMap)
	}
}

func SetChartDeployed(render *templatemodels.ServiceRender, strategyMap map[string]string) {
	if render.DeployedFromZadig() {
		strategyMap[render.ServiceName] = setting.ServiceDeployStrategyDeploy
	} else {
		strategyMap[GetReleaseDeployStrategyKey(render.ReleaseName)] = setting.ServiceDeployStrategyDeploy
	}
}

func ServiceDeployed(serviceName string, strategyMap map[string]string) bool {
	return GetServiceDeployStrategy(serviceName, strategyMap) == setting.ServiceDeployStrategyDeploy
}

func ReleaseDeployed(releaseName string, strategyMap map[string]string) bool {
	return GetReleaseDeployStrategy(releaseName, strategyMap) == setting.ServiceDeployStrategyDeploy
}

func DeployStrategyChanged(serviceName string, strategyMapOld map[string]string, strategyMapNew map[string]string) bool {
	return ServiceDeployed(serviceName, strategyMapOld) != ServiceDeployed(serviceName, strategyMapNew)
}

func SetServiceDeployStrategyDepoly(strategyMap map[string]string, serviceName string) map[string]string {
	if strategyMap == nil {
		strategyMap = make(map[string]string)
	}
	strategyMap[serviceName] = setting.ServiceDeployStrategyDeploy
	return strategyMap
}

func SetServiceDeployStrategyImport(strategyMap map[string]string, serviceName string) map[string]string {
	if strategyMap == nil {
		strategyMap = make(map[string]string)
	}
	strategyMap[serviceName] = setting.ServiceDeployStrategyImport
	return strategyMap
}

func SetChartServiceDeployStrategyDepoly(strategyMap map[string]string, releaseName string) map[string]string {
	if strategyMap == nil {
		strategyMap = make(map[string]string)
	}
	strategyMap[GetReleaseDeployStrategyKey(releaseName)] = setting.ServiceDeployStrategyDeploy
	return strategyMap
}

func SetChartServiceDeployStrategyImport(strategyMap map[string]string, releaseName string) map[string]string {
	if strategyMap == nil {
		strategyMap = make(map[string]string)
	}
	strategyMap[GetReleaseDeployStrategyKey(releaseName)] = setting.ServiceDeployStrategyImport
	return strategyMap
}

func SetCurrentContainerImages(args *commonmodels.Service) error {
	var srvContainers []*commonmodels.Container
	for _, data := range args.KubeYamls {
		yamlDataArray := util.SplitYaml(data)
		for index, yamlData := range yamlDataArray {
			resKind := new(types.KubeResourceKind)
			// replace render variable {{.}} before Unmarshal
			yamlData = config.RenderTemplateAlias.ReplaceAllLiteralString(yamlData, "ssssssss")
			// replace $Service$ with service name
			yamlData = config.ServiceNameAlias.ReplaceAllLiteralString(yamlData, args.ServiceName)
			// replace $Product$ with product name
			yamlData = config.ProductNameAlias.ReplaceAllLiteralString(yamlData, args.ProductName)

			if err := yaml.Unmarshal([]byte(yamlData), &resKind); err != nil {
				return fmt.Errorf("unmarshal ResourceKind error: %v", err)
			}

			if resKind == nil {
				if index == 0 {
					continue
				}
				return errors.New("nil Resource Kind")
			}

			if resKind.Kind == setting.Deployment || resKind.Kind == setting.StatefulSet || resKind.Kind == setting.Job {
				containers, err := getContainers(yamlData)
				if err != nil {
					return fmt.Errorf("GetContainers error: %v", err)
				}

				srvContainers = append(srvContainers, containers...)
			} else if resKind.Kind == setting.CronJob {
				containers, err := getCronJobContainers(yamlData)
				if err != nil {
					return fmt.Errorf("GetCronjobContainers error: %v", err)
				}
				srvContainers = append(srvContainers, containers...)
			}
		}
	}

	args.Containers = uniqueSlice(srvContainers)
	return nil
}

func getContainers(data string) ([]*commonmodels.Container, error) {
	containers := make([]*commonmodels.Container, 0)

	res := new(types.KubeResource)
	if err := yaml.Unmarshal([]byte(data), &res); err != nil {
		return containers, fmt.Errorf("unmarshal yaml data error: %v", err)
	}

	for _, val := range res.Spec.Template.Spec.InitContainers {
		container, err := parseContainer(val, setting.ContainerTypeInit)
		if err != nil {
			return containers, err
		}

		containers = append(containers, container)
	}

	for _, val := range res.Spec.Template.Spec.Containers {
		container, err := parseContainer(val, setting.ContainerTypeNormal)
		if err != nil {
			return containers, err
		}

		containers = append(containers, container)
	}

	return containers, nil
}

func getCronJobContainers(data string) ([]*commonmodels.Container, error) {
	containers := make([]*commonmodels.Container, 0)

	res := new(types.CronjobResource)
	if err := yaml.Unmarshal([]byte(data), &res); err != nil {
		return containers, fmt.Errorf("unmarshal yaml data error: %v", err)
	}

	for _, val := range res.Spec.Template.Spec.Template.Spec.InitContainers {
		container, err := parseContainer(val, setting.ContainerTypeInit)
		if err != nil {
			return containers, err
		}

		containers = append(containers, container)
	}

	for _, val := range res.Spec.Template.Spec.Template.Spec.Containers {
		container, err := parseContainer(val, setting.ContainerTypeNormal)
		if err != nil {
			return containers, err
		}

		containers = append(containers, container)
	}

	return containers, nil
}

func uniqueSlice(elements []*commonmodels.Container) []*commonmodels.Container {
	existing := make(map[string]bool)
	ret := make([]*commonmodels.Container, 0)
	for _, elem := range elements {
		key := elem.Name
		if _, ok := existing[key]; !ok {
			ret = append(ret, elem)
			existing[key] = true
		}
	}

	return ret
}

func KVs2Set(kvs []*commonmodels.ServiceKeyVal) sets.String {
	keySet := sets.NewString()
	for _, kv := range kvs {
		splitStrs := strings.Split(kv.Key, ".")
		key := strings.Split(splitStrs[0], "[")[0]
		keySet.Insert(key)
	}

	return keySet
}

func FilterKV(varKV *commonmodels.VariableKV, keySet sets.String) bool {
	if varKV == nil {
		return false
	}

	prefixKey := strings.Split(varKV.Key, ".")[0]
	prefixKey = strings.Split(prefixKey, "[")[0]
	if keySet.Has(prefixKey) {
		return true
	}
	return false
}

func MergeServiceVariableKVsAndKVInput(serviceKVs []*commontypes.ServiceVariableKV, kvinput util.KVInput) (string, []*commontypes.ServiceVariableKV, error) {
	svcVarMap := map[string]*commontypes.ServiceVariableKV{}
	for _, kv := range serviceKVs {
		svcVarMap[kv.Key] = kv
	}

	mergedKVs := make([]*commontypes.ServiceVariableKV, 0)
	for _, kv := range kvinput {
		templKV, ok := svcVarMap[kv.Key]
		if !ok {
			mergedKVs = append(mergedKVs, &commontypes.ServiceVariableKV{
				Key:   kv.Key,
				Type:  commontypes.ServiceVariableKVTypeString,
				Value: kv.Value,
			})
		} else {
			templKV.Value = kv.Value
			mergedKVs = append(mergedKVs, templKV)
		}
	}

	mergedYaml, err := commontypes.ServiceVariableKVToYaml(mergedKVs, true)
	if err != nil {
		return "", nil, fmt.Errorf("failed to convert service variable kv to yaml, err: %w", err)
	}

	return mergedYaml, mergedKVs, nil
}

// GenerateServiceNextRevision is used to generate the next revision of the service
func GenerateServiceNextRevision(isProductionService bool, serviceName, projectName string) (int64, error) {
	var counterName string
	if isProductionService {
		counterName = fmt.Sprintf(setting.ProductionServiceTemplateCounterName, serviceName, projectName)
	} else {
		counterName = fmt.Sprintf(setting.ServiceTemplateCounterName, serviceName, projectName)
	}
	return commonrepo.NewCounterColl().GetNextSeq(counterName)
}

// ParseImagesForProductService for product service
func ParseImagesForProductService(nested map[string]interface{}, serviceName, productName string) ([]*commonmodels.Container, error) {
	patterns, err := getServiceParsePatterns(productName)
	if err != nil {
		log.Errorf("failed to get image parse patterns for service %s in project %s, err: %s", serviceName, productName, err)
		return nil, errors.New("failed to get image parse patterns")
	}
	return parseImagesByPattern(nested, patterns)
}

// get patterns used to parse images from yaml
func getServiceParsePatterns(productName string) ([]map[string]string, error) {
	productInfo, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		return nil, err
	}
	ret := make([]map[string]string, 0)
	for _, rule := range productInfo.ImageSearchingRules {
		if !rule.InUse {
			continue
		}
		ret = append(ret, rule.GetSearchingPattern())
	}

	// rules are never edited, use preset rules
	if len(ret) == 0 {
		ret = presetPatterns
	}
	return ret, nil
}

func parseImagesByPattern(nested map[string]interface{}, patterns []map[string]string) ([]*commonmodels.Container, error) {
	flatMap, err := converter.Flatten(nested)
	if err != nil {
		return nil, err
	}
	matchedPath, err := yamlutil.SearchByPattern(flatMap, patterns)
	if err != nil {
		return nil, err
	}
	ret := make([]*commonmodels.Container, 0)
	usedImagePath := sets.NewString()
	for _, searchResult := range matchedPath {
		uniquePath := GenerateUniquePath(searchResult)
		if usedImagePath.Has(uniquePath) {
			continue
		}
		usedImagePath.Insert(uniquePath)
		imageUrl, err := GeneImageURI(searchResult, flatMap)
		if err != nil {
			return nil, err
		}
		name := ExtractImageName(imageUrl)
		ret = append(ret, &commonmodels.Container{
			Name:      name,
			ImageName: name,
			Image:     imageUrl,
			ImagePath: &commonmodels.ImagePathSpec{
				Repo:      searchResult[setting.PathSearchComponentRepo],
				Namespace: searchResult[setting.PathSearchComponentNamespace],
				Image:     searchResult[setting.PathSearchComponentImage],
				Tag:       searchResult[setting.PathSearchComponentTag],
			},
		})
	}
	return ret, nil
}

func ParseImagesByRules(nested map[string]interface{}, matchRules []*templatemodels.ImageSearchingRule) ([]*commonmodels.Container, error) {
	patterns := make([]map[string]string, 0)
	for _, rule := range matchRules {
		if !rule.InUse {
			continue
		}
		patterns = append(patterns, rule.GetSearchingPattern())
	}
	log.Debugf("nested: %v\n, patterns: %v", nested, patterns)
	return parseImagesByPattern(nested, patterns)
}

// ParseImagesByPresetRules parse images from flat yaml map with preset rules
func ParseImagesByPresetRules(flatMap map[string]interface{}) ([]map[string]string, error) {
	return yamlutil.SearchByPattern(flatMap, presetPatterns)
}

func GetPresetRules() []*templatemodels.ImageSearchingRule {
	ret := make([]*templatemodels.ImageSearchingRule, 0, len(presetPatterns))
	for id, pattern := range presetPatterns {
		ret = append(ret, &templatemodels.ImageSearchingRule{
			Repo:      pattern[setting.PathSearchComponentRepo],
			Namespace: pattern[setting.PathSearchComponentNamespace],
			Image:     pattern[setting.PathSearchComponentImage],
			Tag:       pattern[setting.PathSearchComponentTag],
			InUse:     true,
			PresetId:  id + 1,
		})
	}
	return ret
}

func GenerateUniquePath(pathData map[string]string) string {
	keys := []string{setting.PathSearchComponentRepo, setting.PathSearchComponentNamespace, setting.PathSearchComponentImage, setting.PathSearchComponentTag}
	values := make([]string, 0)
	for _, key := range keys {
		if value := pathData[key]; value != "" {
			values = append(values, value)
		}
	}
	return strings.Join(values, "-")
}

// GeneImageURI generate valid image uri, legal formats:
// {repo}
// {repo}/ {namespace}/{image}
// {repo}/{image}
// {repo}/{image}:{tag}
// {repo}:{tag}
// {image}:{tag}
// {image}
func GeneImageURI(pathData map[string]string, flatMap map[string]interface{}) (string, error) {
	valuesMap := getValuesByPath(pathData, flatMap)
	ret := ""
	// if repo value is set, use as repo
	if repo, ok := valuesMap[setting.PathSearchComponentRepo]; ok {
		ret = fmt.Sprintf("%v", repo)
		ret = strings.TrimSuffix(ret, "/")
	}
	// if namespace is set, use repo/namespace
	if namespace, ok := valuesMap[setting.PathSearchComponentNamespace]; ok {
		if len(ret) == 0 {
			ret = fmt.Sprintf("%v", namespace)
		} else {
			ret = fmt.Sprintf("%s/%s", ret, namespace)
		}
		ret = strings.TrimSuffix(ret, "/")
	}
	// if image value is set, append to repo, if repo is not set, image values represents repo+image
	if image, ok := valuesMap[setting.PathSearchComponentImage]; ok {
		imageStr := fmt.Sprintf("%v", image)
		if ret == "" {
			ret = imageStr
		} else {
			ret = fmt.Sprintf("%s/%s", ret, imageStr)
		}
	}
	if ret == "" {
		return "", errors.New("image name not found")
	}
	// if tag is set, append to current uri, if not set ignore
	if tag, ok := valuesMap[setting.PathSearchComponentTag]; ok {
		tagStr := fmt.Sprintf("%v", tag)
		if tagStr != "" {
			ret = fmt.Sprintf("%s:%s", ret, tagStr)
		}
	}
	return ret, nil
}

// get values from source flat map
// convert map[k]absolutePath  to  map[k]value
func getValuesByPath(paths map[string]string, flatMap map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for k, path := range paths {
		if value, ok := flatMap[path]; ok {
			ret[k] = value
		} else {
			ret[k] = nil
		}
	}
	return ret
}

func parseContainer(val map[string]interface{}, containerType setting.ContainerType) (*commonmodels.Container, error) {
	if _, ok := val["name"]; !ok {
		return nil, errors.New("yaml file missing name key")
	}

	nameStr, ok := val["name"].(string)
	if !ok {
		return nil, errors.New("error name value")
	}

	if _, ok := val["image"]; !ok {
		return nil, errors.New("yaml file missing image key")
	}

	imageStr, ok := val["image"].(string)
	if !ok {
		return nil, errors.New("error image value")
	}

	container := &commonmodels.Container{
		Name:      nameStr,
		Type:      containerType,
		Image:     imageStr,
		ImageName: ExtractImageName(imageStr),
	}
	return container, nil
}
