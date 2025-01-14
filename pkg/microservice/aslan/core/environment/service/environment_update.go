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

package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/notify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
)

func InstallService(helmClient *helmtool.HelmClient, param *kube.ReleaseInstallParam) error {
	handler := func(param *kube.ReleaseInstallParam, isRetry bool, log *zap.SugaredLogger) error {
		errInstall := kube.InstallOrUpgradeHelmChartWithValues(param, isRetry, helmClient)
		if errInstall != nil {
			log.Errorf("failed to upgrade service: %s, namespace: %s, isRetry: %v, err: %s", param.ServiceObj.ServiceName, param.Namespace, isRetry, errInstall)
			return errors.Wrapf(errInstall, "failed to install or upgrade service %s", param.ServiceObj.ServiceName)
		}
		return nil
	}
	errList := kube.BatchExecutorWithRetry(3, time.Millisecond*2500, []*kube.ReleaseInstallParam{param}, handler, log.SugaredLogger())
	if len(errList) > 0 {
		return errList[0]
	}
	return nil
}

func setProductServiceError(productInfo *commonmodels.Product, serviceName string, err error) error {
	foundSvc := false
	// update service revision data in product
	for _, svcGroup := range productInfo.Services {
		for _, svc := range svcGroup {
			if svc.ServiceName == serviceName {
				if err != nil {
					svc.Error = err.Error()
				} else {
					svc.Error = ""
				}
				foundSvc = true
				break
			}
		}

		if foundSvc {
			break
		}
	}
	if foundSvc {
		err := helmservice.UpdateAllServicesInEnv(productInfo.ProductName, productInfo.EnvName, productInfo.Services, productInfo.Production)
		if err != nil {
			return fmt.Errorf("failed to update %s/%s product services, err: %s ", productInfo.ProductName, productInfo.EnvName, err)
		}
	}
	return nil
}

func updateServiceRevisionInProduct(productInfo *commonmodels.Product, serviceName string, serviceRevision int64) error {
	foundSvc := false
	// update service revision data in product
	for _, svcGroup := range productInfo.Services {
		for _, svc := range svcGroup {
			if svc.ServiceName == serviceName {
				svc.Revision = serviceRevision
				foundSvc = true
				break
			}
		}

		if foundSvc {
			break
		}
	}
	if foundSvc {
		err := helmservice.UpdateAllServicesInEnv(productInfo.ProductName, productInfo.EnvName, productInfo.Services, productInfo.Production)
		if err != nil {
			return fmt.Errorf("failed to update %s/%s product services, err: %s ", productInfo.ProductName, productInfo.EnvName, err)
		}
	}
	return nil
}

// reInstallHelmServiceInEnv uninstall the service and reinstall to update releaseNaming rule
func reInstallHelmServiceInEnv(productInfo *commonmodels.Product, templateSvc *commonmodels.Service) (finalError error) {
	productSvcMap, serviceName := productInfo.GetServiceMap(), templateSvc.ServiceName
	productSvc, ok := productSvcMap[serviceName]
	// service not applied in this environment
	if !ok {
		return nil
	}

	var err error
	defer func() {
		finalError = setProductServiceError(productInfo, templateSvc.ServiceName, err)
	}()

	renderChart := productInfo.GetSvcRender(serviceName)

	helmClient, errCreateClient := helmtool.NewClientFromNamespace(productInfo.ClusterID, productInfo.Namespace)
	if errCreateClient != nil {
		err = fmt.Errorf("failed to create helm client, err: %s", errCreateClient)
		return
	}

	prodSvcTemp, errFindProd := repository.QueryTemplateService(&commonrepo.ServiceFindOption{ServiceName: serviceName, Revision: productSvc.Revision, ProductName: productInfo.ProductName}, productInfo.Production)
	if errFindProd != nil {
		err = fmt.Errorf("failed to get service: %s with revision: %d, err: %s", serviceName, productSvc.Revision, errFindProd)
		return
	}

	// nothing would happen if release naming rule are same
	if prodSvcTemp.GetReleaseNaming() == templateSvc.GetReleaseNaming() {
		return nil
	}

	err = updateServiceRevisionInProduct(productInfo, templateSvc.ServiceName, templateSvc.Revision)
	if err != nil {
		return
	}

	param, errBuildParam := kube.BuildInstallParam(productInfo.DefaultValues, productInfo, renderChart, productSvc)
	if errBuildParam != nil {
		err = fmt.Errorf("failed to generate install param, service: %s, namespace: %s, err: %s", templateSvc.ServiceName, productInfo.Namespace, errBuildParam)
		return
	}
	productSvc.ReleaseName = param.ReleaseName

	err = kube.CheckReleaseInstalledByOtherEnv(sets.NewString(param.ReleaseName), productInfo)
	if err != nil {
		return err
	}

	if err = kube.UninstallService(helmClient, productInfo, prodSvcTemp, true); err != nil {
		err = fmt.Errorf("helm uninstall service: %s in namespace: %s, err: %s", serviceName, productInfo.Namespace, err)
		return
	}

	err = InstallService(helmClient, param)
	if err != nil {
		return
	}
	return
}

// ReInstallHelmSvcInAllEnvs reinstall svc in all envs in which the svc is already installed
func ReInstallHelmSvcInAllEnvs(productName string, templateSvc *commonmodels.Service, production bool) error {
	products, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name:       productName,
		Production: util.GetBoolPointer(production),
	})
	if err != nil {
		return fmt.Errorf("failed to list envs for product: %s, err: %s", productName, err)
	}
	retErr := &multierror.Error{}
	for _, product := range products {
		err := reInstallHelmServiceInEnv(product, templateSvc)
		if err != nil {
			retErr = multierror.Append(retErr, fmt.Errorf("failed to update service: %s/%s, err: %s", product.EnvName, templateSvc.ServiceName, err))
		}
	}
	return retErr.ErrorOrNil()
}

// updateHelmSvcInAllEnvs updates helm svc in all envs in which the svc is already installed
func updateHelmSvcInAllEnvs(userName, productName string, templateSvcs []*commonmodels.Service) error {
	products, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name:       productName,
		Production: util.GetBoolPointer(false),
	})
	if err != nil {
		return fmt.Errorf("failed to list envs for product: %s, err: %s", productName, err)
	}
	retErr := &multierror.Error{}

	updateArgs := &UpdateMultiHelmProductArg{
		ProductName: productName,
		EnvNames:    nil,
		ChartValues: nil,
	}

	for _, product := range products {

		envAffected := false
		productSvcMap := product.GetServiceMap()

		for _, templateSvc := range templateSvcs {
			// only update services
			if _, ok := productSvcMap[templateSvc.ServiceName]; !ok {
				continue
			}

			renderChart := product.GetSvcRender(templateSvc.ServiceName)

			chartArg := &commonservice.HelmSvcRenderArg{
				EnvName:     product.EnvName,
				ServiceName: templateSvc.ServiceName,
			}
			chartArg.LoadFromRenderChartModel(renderChart)
			updateArgs.ChartValues = append(updateArgs.ChartValues, chartArg)
			envAffected = true
		}
		if envAffected {
			updateArgs.EnvNames = append(updateArgs.EnvNames, product.EnvName)
		}
	}

	_, err = UpdateMultipleHelmEnv("", userName, updateArgs, false, log.SugaredLogger())
	if err != nil {
		return err
	}

	return retErr.ErrorOrNil()
}

// updateK8sSvcInAllEnvs updates k8s svc in all envs in which the svc is already installed
func updateK8sSvcInAllEnvs(productName string, templateSvc *commonmodels.Service, production bool) error {
	products, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name:       productName,
		Production: util.GetBoolPointer(production),
	})
	if err != nil {
		return fmt.Errorf("failed to list envs for product: %s, err: %s", productName, err)
	}

	retErr := &multierror.Error{}
	for _, product := range products {
		productSvcMap, serviceName := product.GetServiceMap(), templateSvc.ServiceName
		// service not applied in this environment
		if _, ok := productSvcMap[serviceName]; !ok {
			continue
		}
		svcRender := product.GetSvcRender(serviceName)

		filter := func(service *commonmodels.ProductService) bool {
			if templateSvc.ServiceName == service.ServiceName {
				return true
			}
			return false
		}
		err = updateK8sProduct(product, "system", "", []string{svcRender.ServiceName}, filter, []*templatemodels.ServiceRender{svcRender}, nil, false, product.GlobalVariables, log.SugaredLogger())
		if err != nil {
			retErr = multierror.Append(retErr, err)
		}
	}
	return retErr.ErrorOrNil()
}

func updateK8sProduct(exitedProd *commonmodels.Product, user, requestID string, updateRevisionSvc []string, filter svcUpgradeFilter, updatedSvcs []*templatemodels.ServiceRender, deployStrategy map[string]string,
	force bool, globalVariables []*commontypes.GlobalVariableKV, log *zap.SugaredLogger) error {
	envName, productName := exitedProd.EnvName, exitedProd.ProductName
	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(exitedProd.ClusterID)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	if !force {
		modifiedServices := getModifiedServiceFromObjectMetaList(kube.GetDirtyResources(exitedProd.Namespace, kubeClient))
		var specifyModifiedServices []*serviceInfo
		for _, modifiedService := range modifiedServices {
			if util.InStringArray(modifiedService.Name, updateRevisionSvc) {
				specifyModifiedServices = append(specifyModifiedServices, modifiedService)
			}
		}
		if len(specifyModifiedServices) > 0 {
			data, err := json.Marshal(specifyModifiedServices)
			if err != nil {
				log.Errorf("Marshal failure: %v", err)
			}
			log.Errorf("the following services are modified since last update: %s", data)
			return fmt.Errorf("the following services are modified since last update: %s", data)
		}
	}

	err = ensureKubeEnv(exitedProd.Namespace, exitedProd.RegistryID, map[string]string{setting.ProductLabel: productName}, exitedProd.ShareEnv.Enable, kubeClient, log)
	if err != nil {
		log.Errorf("[%s][P:%s] service.updateK8sProduct create kubeEnv error: %v", envName, productName, err)
		return err
	}

	updateRevisionSvcSet := sets.NewString(updateRevisionSvc...)

	updatedSvcMap := make(map[string]*templatemodels.ServiceRender)

	// @fixme improve update revision and filter logic
	// merge render variable and global variable
	for _, svcRender := range updatedSvcs {
		updatedSvcMap[svcRender.ServiceName] = svcRender
		curSvcRender := exitedProd.GetSvcRender(svcRender.ServiceName)

		if updateRevisionSvcSet.Has(svcRender.ServiceName) {
			svcTemplate, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
				ProductName: exitedProd.ProductName,
				ServiceName: svcRender.ServiceName,
			}, exitedProd.Production)
			if err != nil {
				fmtErr := fmt.Errorf("failed to get latest service template %s, err: %v", svcRender.ServiceName, err)
				log.Error(fmtErr)
				return e.ErrUpdateEnv.AddErr(fmtErr)
			}

			globalVariables, svcRender.OverrideYaml.RenderVariableKVs, err = commontypes.UpdateGlobalVariableKVs(svcRender.ServiceName, globalVariables, svcRender.OverrideYaml.RenderVariableKVs, curSvcRender.OverrideYaml.RenderVariableKVs)
			if err != nil {
				fmtErr := fmt.Errorf("failed to merge global and render variables for service %s, err: %w", svcRender.ServiceName, err)
				log.Error(fmtErr)
				return e.ErrUpdateEnv.AddErr(fmtErr)
			}

			svcRender.OverrideYaml.YamlContent, svcRender.OverrideYaml.RenderVariableKVs, err = commontypes.MergeRenderAndServiceTemplateVariableKVs(svcRender.OverrideYaml.RenderVariableKVs, svcTemplate.ServiceVariableKVs)
			if err != nil {
				fmtErr := fmt.Errorf("failed to merge render and service template variable kv, err: %w", err)
				log.Error(fmtErr)
				return e.ErrUpdateEnv.AddErr(fmtErr)
			}
		} else {
			globalVariables, svcRender.OverrideYaml.RenderVariableKVs, err = commontypes.UpdateGlobalVariableKVs(svcRender.ServiceName, globalVariables, svcRender.OverrideYaml.RenderVariableKVs, curSvcRender.OverrideYaml.RenderVariableKVs)
			if err != nil {
				fmtErr := fmt.Errorf("failed to merge global and render variables for service %s, err: %w", svcRender.ServiceName, err)
				log.Error(fmtErr)
				return e.ErrUpdateEnv.AddErr(fmtErr)
			}

			svcRender.OverrideYaml.YamlContent, err = commontypes.RenderVariableKVToYaml(svcRender.OverrideYaml.RenderVariableKVs, true)
			if err != nil {
				fmtErr := fmt.Errorf("failed to convert render variable kvs to yaml, err: %w", err)
				log.Error(fmtErr)
				return e.ErrUpdateEnv.AddErr(fmtErr)
			}
		}
	}

	log.Infof("[%s][P:%s] updateProductImpl, services: %v", envName, productName, updateRevisionSvc)

	updateProd, err := GetInitProduct(productName, types.GeneralEnv, false, "", exitedProd.Production, log)
	if err != nil {
		log.Errorf("[%s][P:%s] GetProductTemplate error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.FindProductTmplErrMsg)
	}
	updateProd.Production = exitedProd.Production
	updateProd.GlobalVariables = exitedProd.GlobalVariables
	updateProd.DefaultValues = exitedProd.DefaultValues
	updateProd.YamlData = exitedProd.YamlData
	updateProd.ClusterID = exitedProd.ClusterID
	updateProd.Namespace = exitedProd.Namespace
	updateProd.EnvName = envName

	svcsToBeAdd := sets.NewString()

	// build services
	productSvcs := exitedProd.GetServiceMap()
	svcGroupMap := make(map[string]int)
	for i, svg := range exitedProd.Services {
		for _, svc := range svg {
			svcGroupMap[svc.ServiceName] = i
		}
	}
	svcGroups := make([][]*commonmodels.ProductService, 0)
	for _, svcGroup := range updateProd.Services {
		validSvcGroup := make([]*commonmodels.ProductService, 0)
		for _, svc := range svcGroup {
			if curSvc, ok := productSvcs[svc.ServiceName]; ok {
				// services to be updated
				validSvcGroup = append(validSvcGroup, curSvc)
				delete(svcGroupMap, curSvc.ServiceName)
			} else if util.InStringArray(svc.ServiceName, updateRevisionSvc) {
				// services to be added
				validSvcGroup = append(validSvcGroup, svc)
				svcsToBeAdd.Insert(svc.ServiceName)
			}
		}
		svcGroups = append(svcGroups, validSvcGroup)
	}

	for svcName, groupIndex := range svcGroupMap {
		if groupIndex >= len(svcGroups) {
			groupIndex = 0
		}
		svcGroups[groupIndex] = append(svcGroups[groupIndex], productSvcs[svcName])
	}

	updateProd.Services = svcGroups

	for _, updateProdSvc := range updateProd.GetServiceMap() {
		if svcRender, ok := updatedSvcMap[updateProdSvc.ServiceName]; ok {
			updateProdSvc.Render = svcRender
		}
	}

	switch exitedProd.Status {
	case setting.ProductStatusCreating, setting.ProductStatusUpdating, setting.ProductStatusDeleting:
		log.Errorf("[%s][P:%s] Product is not in valid status", envName, productName)
		return e.ErrUpdateEnv.AddDesc(e.EnvCantUpdatedMsg)
	default:
		// do nothing
	}

	// list services with max revision of project
	allServices, err := repository.ListMaxRevisionsServices(productName, exitedProd.Production)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to list services with max revision of project %s, err: %v", productName, err))
	}
	prodRevs, err := GetProductRevision(exitedProd, allServices, log)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to get product revision, err: %v", err))
	}
	serviceRevisionMap := getServiceRevisionMap(prodRevs.ServiceRevisions)

	for _, prodServiceGroup := range updateProd.Services {
		for _, prodService := range prodServiceGroup {
			if filter != nil && !filter(prodService) {
				continue
			}

			service := &commonmodels.ProductService{
				ServiceName: prodService.ServiceName,
				ProductName: prodService.ProductName,
				Type:        prodService.Type,
				Revision:    prodService.Revision,
				Render:      prodService.Render,
				Containers:  prodService.Containers,
			}

			// need update service revision
			if util.InStringArray(prodService.ServiceName, updateRevisionSvc) {
				svcRev, ok := serviceRevisionMap[prodService.ServiceName+prodService.Type]
				if !ok {
					continue
				}
				service.Revision = svcRev.NextRevision
				service.Containers = svcRev.Containers
				service.UpdateTime = time.Now().Unix()
			}

			// render check
			serviceYaml, err := kube.RenderEnvService(updateProd, service.GetServiceRender(), service)
			if err != nil {
				log.Errorf("Failed to run render check, service %s, error: %v", service.ServiceName, err)
				return e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to render service %s, error: %v", service.ServiceName, err))
			}

			err = kube.CheckResourceAppliedByOtherEnv(serviceYaml, updateProd, service.ServiceName)
			if err != nil {
				return e.ErrUpdateEnv.AddErr(err)
			}
		}
	}

	// 设置产品状态为更新中
	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	go func() {
		productErrMsg := ""
		err = updateProductImpl(updateRevisionSvc, deployStrategy, exitedProd, updateProd, filter, user, log)
		if err != nil {
			log.Errorf("[%s][P:%s] failed to update product %#v", envName, productName, err)
			// 发送更新产品失败消息给用户
			title := fmt.Sprintf("更新 [%s] 的 [%s] 环境失败", productName, envName)
			notify.SendErrorMessage(user, title, requestID, err, log)
			updateProd.Status = setting.ProductStatusFailed
			productErrMsg = err.Error()
		} else {
			updateProd.Status = setting.ProductStatusSuccess
		}
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, updateProd.Status, productErrMsg); err != nil {
			log.Errorf("[%s][%s] Product.Update set product status error: %v", envName, productName, err)
			return
		}
	}()

	return nil
}

func updateCVMProduct(exitedProd *commonmodels.Product, user, requestID string, log *zap.SugaredLogger) error {
	envName, productName := exitedProd.EnvName, exitedProd.ProductName
	var serviceNames []string
	//TODO:The host update environment cannot remove deleted services
	services, err := commonrepo.NewServiceColl().ListMaxRevisionsAllSvcByProduct(productName)
	if err != nil && !commonrepo.IsErrNoDocuments(err) {
		log.Errorf("ListMaxRevisionsAllSvcByProduct: %s", err)
		return fmt.Errorf("ListMaxRevisionsAllSvcByProduct: %s", err)
	}
	if services != nil {
		svcNames := make([]string, 0, len(services))
		for _, svc := range services {
			svcNames = append(svcNames, svc.ServiceName)
		}
		serviceNames = svcNames
	}

	log.Infof("[%s][P:%s] updateProductImpl, services: %v", envName, productName, serviceNames)

	// 查找产品模板
	updateProd, err := GetInitProduct(productName, types.GeneralEnv, false, "", false, log)
	if err != nil {
		log.Errorf("[%s][P:%s] GetProductTemplate error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.FindProductTmplErrMsg)
	}

	switch exitedProd.Status {
	case setting.ProductStatusCreating, setting.ProductStatusUpdating, setting.ProductStatusDeleting:
		log.Errorf("[%s][P:%s] Product is not in valid status", envName, productName)
		return e.ErrUpdateEnv.AddDesc(e.EnvCantUpdatedMsg)
	default:
		// do nothing
	}

	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	go func() {
		productErrMsg := ""
		err = updateProductImpl(serviceNames, nil, exitedProd, updateProd, nil, user, log)
		if err != nil {
			productErrMsg = err.Error()
			log.Errorf("[%s][P:%s] failed to update product %#v", envName, productName, err)
			// 发送更新产品失败消息给用户›
			title := fmt.Sprintf("更新 [%s] 的 [%s] 环境失败", productName, envName)
			notify.SendErrorMessage(user, title, requestID, err, log)
			updateProd.Status = setting.ProductStatusFailed
			productErrMsg = err.Error()
		} else {
			updateProd.Status = setting.ProductStatusSuccess
		}
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, updateProd.Status, productErrMsg); err != nil {
			log.Errorf("[%s][%s] Product.Update set product status error: %v", envName, productName, err)
			return
		}
	}()

	return nil
}

func AutoDeployHelmServiceToEnvs(userName, requestID, projectName string, serviceTemplates []*commonmodels.Service, log *zap.SugaredLogger) error {
	templateProduct, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		return fmt.Errorf("failed to find template product when depolying services: %s, err: %s", projectName, err)
	}
	if templateProduct.AutoDeploy == nil || !templateProduct.AutoDeploy.Enable {
		return nil
	}
	go func() {
		err = updateHelmSvcInAllEnvs(userName, projectName, serviceTemplates)
		if err != nil {
			notify.SendErrorMessage(userName, "服务自动部署失败", requestID, err, log)
		}
	}()
	return nil
}

func AutoDeployYamlServiceToEnvs(userName, requestID string, serviceTemplate *commonmodels.Service, production bool, log *zap.SugaredLogger) error {
	if production {
		return nil
	}

	templateProduct, err := templaterepo.NewProductColl().Find(serviceTemplate.ProductName)
	if err != nil {
		return fmt.Errorf("failed to find template product when depolying services: %s, err: %s", serviceTemplate.ServiceName, err)
	}
	if templateProduct.AutoDeploy == nil || !templateProduct.AutoDeploy.Enable {
		return nil
	}
	go func() {
		err = updateK8sSvcInAllEnvs(serviceTemplate.ProductName, serviceTemplate, production)
		if err != nil {
			notify.SendErrorMessage(userName, "服务自动部署失败", requestID, err, log)
		}
	}()
	return nil
}
