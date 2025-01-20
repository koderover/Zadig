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

package helm

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	fsservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/fs"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	"github.com/koderover/zadig/v2/pkg/tool/crypto"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/tool/mongo"
	"github.com/koderover/zadig/v2/pkg/util/converter"
	"github.com/pkg/errors"
)

const (
	UpdateHelmEnvLockKey = "UpdateHelmEnv"
)

func ListHelmRepos(encryptedKey string, log *zap.SugaredLogger) ([]*commonmodels.HelmRepo, error) {
	aesKey, err := commonutil.GetAesKeyFromEncryptedKey(encryptedKey, log)
	if err != nil {
		log.Errorf("ListHelmRepos GetAesKeyFromEncryptedKey err:%v", err)
		return nil, err
	}
	helmRepos, err := commonrepo.NewHelmRepoColl().List()
	if err != nil {
		log.Errorf("ListHelmRepos err:%v", err)
		return []*commonmodels.HelmRepo{}, nil
	}
	for _, helmRepo := range helmRepos {
		helmRepo.Password, err = crypto.AesEncryptByKey(helmRepo.Password, aesKey.PlainText)
		if err != nil {
			log.Errorf("ListHelmRepos AesEncryptByKey err:%v", err)
			return nil, err
		}
	}
	return helmRepos, nil
}

func ListHelmReposByProject(projectName string, log *zap.SugaredLogger) ([]*commonmodels.HelmRepo, error) {
	helmRepos, err := commonrepo.NewHelmRepoColl().ListByProject(projectName)
	if err != nil {
		log.Errorf("ListHelmRepos err:%v", err)
		return []*commonmodels.HelmRepo{}, nil
	}
	for _, helmRepo := range helmRepos {
		helmRepo.Password = ""
		helmRepo.Projects = nil
	}
	return helmRepos, nil
}

func ListHelmReposPublic() ([]*commonmodels.HelmRepo, error) {
	return commonrepo.NewHelmRepoColl().List()
}

func SaveAndUploadService(projectName, serviceName string, copies []string, fileTree fs.FS, isProduction bool) error {
	var localBase, s3Base string
	if !isProduction {
		localBase = config.LocalTestServicePath(projectName, serviceName)
		s3Base = config.ObjectStorageTestServicePath(projectName, serviceName)
	} else {
		localBase = config.LocalProductionServicePath(projectName, serviceName)
		s3Base = config.ObjectStorageProductionServicePath(projectName, serviceName)
	}
	names := append([]string{serviceName}, copies...)
	return fsservice.SaveAndUploadFiles(fileTree, names, localBase, s3Base, log.SugaredLogger())
}

func CopyAndUploadService(projectName, serviceName, currentChartPath string, copies []string, isProduction bool) error {
	var localBase, s3Base string
	if !isProduction {
		localBase = config.LocalTestServicePath(projectName, serviceName)
		s3Base = config.ObjectStorageTestServicePath(projectName, serviceName)
	} else {
		localBase = config.LocalProductionServicePath(projectName, serviceName)
		s3Base = config.ObjectStorageProductionServicePath(projectName, serviceName)
	}
	names := append([]string{serviceName}, copies...)

	return fsservice.CopyAndUploadFiles(names, path.Join(localBase, serviceName), s3Base, localBase, currentChartPath, log.SugaredLogger())
}

// Update Service and ServiceDeployStrategy for a single service in environment
func UpdateServiceInEnv(product *commonmodels.Product, productSvc *commonmodels.ProductService, user string) error {
	session := mongo.Session()
	defer session.EndSession(context.TODO())

	err := mongo.StartTransaction(session)
	if err != nil {
		return err
	}

	product.LintServices()
	err = commonutil.CreateEnvServiceVersion(product, productSvc, user, session, log.SugaredLogger())
	if err != nil {
		log.Errorf("failed to create helm service version, err: %v", err)
	}

	envLock := cache.NewRedisLock(fmt.Sprintf("%s:%s:%s", UpdateHelmEnvLockKey, product.ProductName, product.EnvName))
	envLock.Lock()
	defer envLock.Unlock()

	productColl := commonrepo.NewProductCollWithSession(session)
	newProductInfo, err := productColl.Find(&commonrepo.ProductFindOptions{Name: product.ProductName, EnvName: product.EnvName})
	if err != nil {
		mongo.AbortTransaction(session)
		return errors.Wrapf(err, "failed to find product %s", product.ProductName)
	}

	newProductInfo.LintServices()
	productSvcMap := newProductInfo.GetServiceMap()
	productChartSvcMap := newProductInfo.GetChartServiceMap()
	if productSvc.FromZadig() {
		productSvcMap[productSvc.ServiceName] = productSvc
		productSvcMap[productSvc.ServiceName].UpdateTime = time.Now().Unix()
		delete(productChartSvcMap, productSvc.ReleaseName)
	} else {
		productChartSvcMap[productSvc.ReleaseName] = productSvc
		productChartSvcMap[productSvc.ReleaseName].UpdateTime = time.Now().Unix()
		for _, svc := range productSvcMap {
			if svc.ReleaseName == productSvc.ReleaseName {
				delete(productSvcMap, svc.ServiceName)
				break
			}
		}
	}

	templateProduct, err := template.NewProductCollWithSess(session).Find(product.ProductName)
	if err != nil {
		mongo.AbortTransaction(session)
		return errors.Wrapf(err, "failed to find template product %s", product.ProductName)
	}

	newProductInfo.Services = [][]*commonmodels.ProductService{}
	serviceOrchestration := templateProduct.Services
	if product.Production {
		serviceOrchestration = templateProduct.ProductionServices
	}

	for i, svcGroup := range serviceOrchestration {
		// init slice
		if len(newProductInfo.Services) >= i {
			newProductInfo.Services = append(newProductInfo.Services, []*commonmodels.ProductService{})
		}

		// set services in order
		for _, svc := range svcGroup {
			// if svc exists in productSvcMap
			if productSvcMap[svc] != nil {
				newProductInfo.Services[i] = append(newProductInfo.Services[i], productSvcMap[svc])
			}
		}
	}
	// append chart services to the last group
	for _, service := range productChartSvcMap {
		newProductInfo.Services[len(newProductInfo.Services)-1] = append(newProductInfo.Services[len(newProductInfo.Services)-1], service)
	}

	if productSvc.DeployStrategy == setting.ServiceDeployStrategyDeploy {
		if productSvc.FromZadig() {
			newProductInfo.ServiceDeployStrategy = commonutil.SetServiceDeployStrategyDepoly(newProductInfo.ServiceDeployStrategy, productSvc.ServiceName)
		} else {
			newProductInfo.ServiceDeployStrategy = commonutil.SetChartServiceDeployStrategyDepoly(newProductInfo.ServiceDeployStrategy, productSvc.ReleaseName)
		}
	} else if productSvc.DeployStrategy == setting.ServiceDeployStrategyImport {
		if productSvc.FromZadig() {
			newProductInfo.ServiceDeployStrategy = commonutil.SetServiceDeployStrategyImport(newProductInfo.ServiceDeployStrategy, productSvc.ServiceName)
		} else {
			newProductInfo.ServiceDeployStrategy = commonutil.SetChartServiceDeployStrategyImport(newProductInfo.ServiceDeployStrategy, productSvc.ReleaseName)
		}
	}

	if err = productColl.Update(newProductInfo); err != nil {
		log.Errorf("update product %s error: %s", newProductInfo.ProductName, err.Error())
		mongo.AbortTransaction(session)
		return fmt.Errorf("failed to update product info, name %s", newProductInfo.ProductName)
	}

	return mongo.CommitTransaction(session)
}

// Update all services in environment
func UpdateAllServicesInEnv(productName, envName string, services [][]*models.ProductService, production bool) error {
	session := mongo.Session()
	defer session.EndSession(context.TODO())

	err := mongo.StartTransaction(session)
	if err != nil {
		return err
	}

	productColl := commonrepo.NewProductCollWithSession(session)

	envLock := cache.NewRedisLock(fmt.Sprintf("%s:%s:%s", UpdateHelmEnvLockKey, productName, envName))
	envLock.Lock()
	defer envLock.Unlock()

	templateProduct, err := template.NewProductCollWithSess(session).Find(productName)
	if err != nil {
		mongo.AbortTransaction(session)
		return errors.Wrapf(err, "failed to find template product %s", productName)
	}

	serviceOrchestration := templateProduct.Services
	if production {
		serviceOrchestration = templateProduct.ProductionServices
	}

	dummyEnv := &commonmodels.Product{
		Services: services,
	}
	dummyEnv.LintServices()
	productSvcMap := dummyEnv.GetServiceMap()
	productChartSvcMap := dummyEnv.GetChartServiceMap()

	newServices := [][]*commonmodels.ProductService{}
	for i, svcGroup := range serviceOrchestration {
		// init slice
		if len(newServices) >= i {
			newServices = append(newServices, []*commonmodels.ProductService{})
		}

		// set services in order
		for _, svc := range svcGroup {
			// if svc exists in productSvcMap
			if productSvcMap[svc] != nil {
				newServices[i] = append(newServices[i], productSvcMap[svc])
			}
		}
	}
	// append chart services to the last group
	for _, service := range productChartSvcMap {
		newServices[len(newServices)-1] = append(newServices[len(newServices)-1], service)
	}

	if err = productColl.UpdateAllServices(productName, envName, newServices); err != nil {
		err = fmt.Errorf("failed to update %s/%s product services, err %s", productName, envName, err)
		mongo.AbortTransaction(session)
		log.Error(err)
		return err
	}

	return mongo.CommitTransaction(session)
}

// Update a services group in environment
func UpdateServicesGroupInEnv(productName, envName string, index int, group []*models.ProductService, production bool) error {
	session := mongo.Session()
	defer session.EndSession(context.TODO())

	err := mongo.StartTransaction(session)
	if err != nil {
		return err
	}

	productColl := commonrepo.NewProductCollWithSession(session)

	envLock := cache.NewRedisLock(fmt.Sprintf("%s:%s:%s", UpdateHelmEnvLockKey, productName, envName))
	envLock.Lock()
	defer envLock.Unlock()

	templateProduct, err := template.NewProductCollWithSess(session).Find(productName)
	if err != nil {
		mongo.AbortTransaction(session)
		return errors.Wrapf(err, "failed to find template product %s", productName)
	}

	serviceOrchestration := templateProduct.Services
	if production {
		serviceOrchestration = templateProduct.ProductionServices
	}

	dummyEnv := &commonmodels.Product{
		Services: [][]*commonmodels.ProductService{group},
	}
	dummyEnv.LintServices()
	productSvcMap := dummyEnv.GetServiceMap()
	productChartSvcMap := dummyEnv.GetChartServiceMap()

	newGroup := []*commonmodels.ProductService{}
	for _, svcGroup := range serviceOrchestration {
		// set services in order
		for _, svc := range svcGroup {
			// if svc exists in productSvcMap
			if productSvcMap[svc] != nil {
				newGroup = append(newGroup, productSvcMap[svc])
			}
		}
	}
	// append chart services to the last group
	for _, service := range productChartSvcMap {
		newGroup = append(newGroup, service)
	}

	if err = productColl.UpdateServicesGroup(productName, envName, index, newGroup); err != nil {
		err = fmt.Errorf("failed to update %s/%s product services, err %s", productName, envName, err)
		mongo.AbortTransaction(session)
		log.Error(err)
		return err
	}

	return mongo.CommitTransaction(session)
}

type HelmDeployService struct {
}

func NewHelmDeployService() *HelmDeployService {
	return &HelmDeployService{}
}

// GeneMergedValues generate values.yaml used to install or upgrade helm chart, like param in after option -f
// defaultValues: global values yaml
// productSvc: environment service, contains service's values yaml, override kvs and zadig recorded containers. And productSvc will be updated with correct image and values yaml in this function
// images: ovrride images, used in workflow and replace image in environment
func (s *HelmDeployService) NewGeneMergedValues(productSvc *commonmodels.ProductService, defaultValues string, images []string) (string, error) {
	envValuesYaml := productSvc.GetServiceRender().GetOverrideYaml()
	overrideKVs := productSvc.GetServiceRender().OverrideValues

	imageMap := make(map[string]string)
	for _, image := range images {
		name := commonutil.ExtractImageName(image)
		imageMap[name] = image
	}

	valuesMap := make(map[string]interface{})
	err := yaml.Unmarshal([]byte(envValuesYaml), &valuesMap)
	if err != nil {
		return "", fmt.Errorf("Failed to unmarshall yaml, err %s", err)
	}

	flatValuesMap, err := converter.Flatten(valuesMap)
	if err != nil {
		return "", fmt.Errorf("failed to flatten values map, err: %s", err)
	}

	// 1. calc weather to add container image into values yaml
	mergedContainers := []*commonmodels.Container{}
	for _, container := range productSvc.Containers {
		if container.ImagePath == nil {
			return "", fmt.Errorf("failed to parse image for container:%s", container.Image)
		}

		// find corresponding image in values
		imageSearchRule := &templatemodels.ImageSearchingRule{
			Repo:      container.ImagePath.Repo,
			Namespace: container.ImagePath.Namespace,
			Image:     container.ImagePath.Image,
			Tag:       container.ImagePath.Tag,
		}
		pattern := imageSearchRule.GetSearchingPattern()
		imageUrl, err := commonutil.GeneImageURI(pattern, flatValuesMap)
		if err != nil {
			return "", fmt.Errorf("failed to get image url for container:%s", container.Image)
		}

		name := commonutil.ExtractImageName(imageUrl)

		if container.ImageName == name {
			// find corresponding image in values
			if imageMap[name] != "" {
				// if found image in images, and the images are from build job, we should override it
				container.Image = imageMap[name]
				mergedContainers = append(mergedContainers, container)
			}
		} else {
			// not found corresponding image in values
			// add container image into values
			log.Debugf("not found service module %s's image in values, add it", container.ImageName)
			mergedContainers = append(mergedContainers, container)
		}
	}

	// 2. replace image into values yaml
	serviceName := productSvc.ServiceName
	imageValuesMaps := make([]map[string]interface{}, 0)
	for _, mergedContainer := range mergedContainers {
		// log.Debugf("merged container: %+v", mergedContainer)
		// prepare image replace info
		replaceValuesMap, err := commonutil.AssignImageData(mergedContainer.Image, commonutil.GetValidMatchData(mergedContainer.ImagePath))
		if err != nil {
			return "", fmt.Errorf("failed to pase image uri %s/%s, err %s", productSvc.ProductName, serviceName, err.Error())
		}
		imageValuesMaps = append(imageValuesMaps, replaceValuesMap)
	}

	// log.Debugf("imageValuesMaps: %+v", imageValuesMaps)
	replacedEnvValuesYaml, err := commonutil.ReplaceImage(envValuesYaml, imageValuesMaps...)
	if err != nil {
		return "", fmt.Errorf("failed to replace image uri %s/%s, err %s", productSvc.ProductName, serviceName, err.Error())

	}
	if replacedEnvValuesYaml == "" {
		return "", fmt.Errorf("failed to set new image uri into service's values.yaml %s/%s", productSvc.ProductName, serviceName)
	}
	productSvc.GetServiceRender().SetOverrideYaml(replacedEnvValuesYaml)

	// 3. merge override values and kvs into values yaml
	finalValuesYaml, err := helmtool.MergeOverrideValues("", defaultValues, replacedEnvValuesYaml, overrideKVs, nil)
	if err != nil {
		return "", fmt.Errorf("failed to merge override values, err: %s", err)
	}
	// log.Debugf("replacedEnvValuesYaml: %s", replacedEnvValuesYaml)
	// log.Debugf("finalValuesYaml: %s", finalValuesYaml)

	// 4. update container image in productSvc
	valuesMap = make(map[string]interface{})
	err = yaml.Unmarshal([]byte(finalValuesYaml), &valuesMap)
	if err != nil {
		return "", fmt.Errorf("Failed to unmarshall yaml, err %s", err)
	}

	flatValuesMap, err = converter.Flatten(valuesMap)
	if err != nil {
		return "", fmt.Errorf("failed to flatten values map, err: %s", err)
	}
	for _, container := range productSvc.Containers {
		if container.ImagePath == nil {
			return "", fmt.Errorf("failed to parse image for container:%s", container.Image)
		}

		imageSearchRule := &templatemodels.ImageSearchingRule{
			Repo:      container.ImagePath.Repo,
			Namespace: container.ImagePath.Namespace,
			Image:     container.ImagePath.Image,
			Tag:       container.ImagePath.Tag,
		}
		pattern := imageSearchRule.GetSearchingPattern()
		imageUrl, err := commonutil.GeneImageURI(pattern, flatValuesMap)
		if err != nil {
			return "", fmt.Errorf("failed to get image url for container:%s", container.Image)
		}
		log.Debugf("final image url: %s", imageUrl)
		container.Image = imageUrl
	}

	return finalValuesYaml, nil
}

func (s *HelmDeployService) GeneFullValues(serviceValuesYaml, envValuesYaml string) (string, error) {
	finalValuesYaml, err := helmtool.MergeOverrideValues(serviceValuesYaml, "", envValuesYaml, "", nil)
	if err != nil {
		return "", fmt.Errorf("failed to merge override values, err: %s", err)
	}

	return finalValuesYaml, nil
}

// Generate new environment service base on updateServiceRevision
// Will merge template service's containers into environment service if updateServiceRevision is true
// And get template service's values yaml
func (s *HelmDeployService) GenNewEnvService(prod *commonmodels.Product, serviceName string, updateServiceRevision bool) (*commonmodels.ProductService, *commonmodels.Service, error) {
	var err error
	tmplSvc := &commonmodels.Service{}
	prodSvc := prod.GetServiceMap()[serviceName]
	if !updateServiceRevision {
		if prodSvc == nil {
			return nil, nil, fmt.Errorf("service %s not found in env %s/%s", serviceName, prod.ProductName, prod.EnvName)
		}

		svcFindOption := &commonrepo.ServiceFindOption{
			ProductName: prod.ProductName,
			ServiceName: serviceName,
			Revision:    prodSvc.Revision,
		}
		tmplSvc, err = repository.QueryTemplateService(svcFindOption, prod.Production)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to find service %s/%d in product %s", serviceName, svcFindOption.Revision, prod.ProductName)
		}

		prodSvc.ValuesYaml = tmplSvc.HelmChart.ValuesYaml
	} else {
		svcFindOption := &commonrepo.ServiceFindOption{
			ProductName: prod.ProductName,
			ServiceName: serviceName,
		}
		tmplSvc, err = repository.QueryTemplateService(svcFindOption, prod.Production)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to find service %s/%d in product %s", serviceName, svcFindOption.Revision, prod.ProductName)
		}

		if prodSvc == nil {
			prodSvc = &commonmodels.ProductService{
				ServiceName: serviceName,
				Type:        tmplSvc.Type,
				Revision:    tmplSvc.Revision,
				Containers:  tmplSvc.Containers,
				ValuesYaml:  tmplSvc.HelmChart.ValuesYaml,
			}
		} else {
			containerMap := make(map[string]*commonmodels.Container)
			for _, container := range prodSvc.Containers {
				containerMap[container.Name] = container
			}

			for _, templateContainer := range tmplSvc.Containers {
				if containerMap[templateContainer.Name] == nil {
					prodSvc.Containers = append(prodSvc.Containers, templateContainer)
				} else {
					containerMap[templateContainer.Name].ImagePath = templateContainer.ImagePath
					containerMap[templateContainer.Name].Type = templateContainer.Type
				}
			}
		}
	}
	return prodSvc, tmplSvc, nil
}

// func (s *HelmDeployService) MergedContainerImage(prodSvc *commonmodels.ProductService, images []string) (*commonmodels.ProductService, error) {
// 	imageMap := make(map[string]string)
// 	mergedContainers := []*commonmodels.Container{}
// 	for _, image := range images {
// 		imageMap[commonutil.ExtractImageName(image)] = image
// 	}
// 	for _, container := range prodSvc.Containers {
// 		overrideImage, ok := imageMap[container.ImageName]
// 		if ok {
// 			container.Image = overrideImage
// 			mergedContainers = append(mergedContainers, container)
// 		}
// 	}

// 	imageValuesMaps := make([]map[string]interface{}, 0)
// 	for _, targetContainer := range mergedContainers {
// 		// prepare image replace info
// 		replaceValuesMap, err := commonutil.AssignImageData(targetContainer.Image, commonutil.GetValidMatchData(targetContainer.ImagePath))
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to pase image uri %s/%s, err %s", prodSvc.ProductName, prodSvc.ServiceName, err.Error())
// 		}
// 		imageValuesMaps = append(imageValuesMaps, replaceValuesMap)
// 	}

// 	// replace image into service's values.yaml
// 	mergedValuesYaml, err := commonutil.ReplaceImage(prodSvc.GetServiceRender().GetOverrideYaml(), imageValuesMaps...)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to replace image uri %s/%s, err %s", prodSvc.ProductName, prodSvc.ServiceName, err.Error())
// 	}
// 	prodSvc.Render.SetOverrideYaml(mergedValuesYaml)

// 	return prodSvc, nil
// }

func (s *HelmDeployService) CheckReleaseInstalledByOtherEnv(releaseNames sets.String, productInfo *commonmodels.Product) error {
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
