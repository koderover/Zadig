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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-multierror"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
	helmclient "github.com/mittwald/go-helm-client"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	versionedclient "istio.io/client-go/pkg/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/notify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	kubeutil "github.com/koderover/zadig/v2/pkg/tool/kube/util"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/koderover/zadig/v2/pkg/util/fs"
)

type IntervalExecutorHandler func(data *ReleaseInstallParam, isRetry bool, log *zap.SugaredLogger) error
type DeploySvcFilter func(svc *commonmodels.ProductService) bool

type ReleaseInstallParam struct {
	ProductName    string
	Namespace      string
	ReleaseName    string
	MergedValues   string
	IsChartInstall bool
	RenderChart    *templatemodels.ServiceRender
	ServiceObj     *commonmodels.Service
	ProdService    *commonmodels.ProductService
	Timeout        int
	DryRun         bool
	Production     bool
}

func InstallOrUpgradeHelmChartWithValues(param *ReleaseInstallParam, isRetry bool, helmClient *helmtool.HelmClient) error {
	namespace, valuesYaml, renderChart, serviceObj := param.Namespace, param.MergedValues, param.RenderChart, param.ServiceObj
	base := config.LocalServicePathWithRevision(serviceObj.ProductName, serviceObj.ServiceName, fmt.Sprint(serviceObj.Revision), param.Production)
	if param.IsChartInstall {
		base = config.LocalServicePathWithRevision(serviceObj.ProductName, serviceObj.ServiceName, param.RenderChart.ChartVersion, param.Production)
	}
	if err := commonutil.PreloadServiceManifestsByRevision(base, serviceObj, param.Production); err != nil {
		log.Warnf("failed to get chart of revision: %d for service: %s, use latest version",
			serviceObj.Revision, serviceObj.ServiceName)
		// use the latest version when it fails to download the specific version
		base = config.LocalServicePath(serviceObj.ProductName, serviceObj.ServiceName, param.Production)
		if err = commonutil.PreLoadServiceManifests(base, serviceObj, param.Production); err != nil {
			log.Errorf("failed to load chart info for service %v, production: %v", serviceObj.ServiceName, param.Production)
			return fmt.Errorf("failed to load chart info for service %s", serviceObj.ServiceName)
		}
	}

	chartFullPath := filepath.Join(base, serviceObj.ServiceName)
	if param.IsChartInstall {
		chartFullPath = filepath.Join(base, param.RenderChart.ChartName)
	}
	chartPath, err := fs.RelativeToCurrentPath(chartFullPath)
	if err != nil {
		log.Errorf("Failed to get relative path %s, err: %s", chartFullPath, err)
		return err
	}

	chartSpec := &helmclient.ChartSpec{
		ReleaseName:   param.ReleaseName,
		ChartName:     chartPath,
		Namespace:     namespace,
		Version:       renderChart.ChartVersion,
		ValuesYaml:    valuesYaml,
		UpgradeCRDs:   true,
		CleanupOnFail: true,
		MaxHistory:    10,
	}
	if isRetry {
		chartSpec.Replace = true
	}
	if param.Timeout > 0 {
		chartSpec.Timeout = time.Second * time.Duration(param.Timeout)
	}

	// If the target environment is a shared environment and a sub env, we need to clear the deployed K8s Service.
	ctx := context.TODO()
	err = EnsureDeletePreCreatedServices(ctx, param.ProductName, param.Namespace, chartSpec, helmClient)
	if err != nil {
		return fmt.Errorf("failed to ensure deleting pre-created K8s Services for product %q in namespace %q: %s", param.ProductName, param.Namespace, err)
	}

	helmClient, err = helmClient.Clone()
	if err != nil {
		return fmt.Errorf("failed to clone helm client: %s", err)
	}

	var release *release.Release
	release, err = helmClient.InstallOrUpgradeChart(ctx, chartSpec, nil)
	if err != nil {
		err = errors.WithMessagef(
			err,
			"failed to install or upgrade helm chart %s/%s",
			namespace, serviceObj.ServiceName)
	} else {
		err = EnsureZadigServiceByManifest(ctx, param.ProductName, param.Namespace, release.Manifest)
		if err != nil {
			err = errors.WithMessagef(err, "failed to ensure Zadig Service, err: %s", err)
		}
	}

	return err
}

func UninstallServiceByName(helmClient helmclient.Client, serviceName string, env *commonmodels.Product, revision int64, force bool) error {
	revisionSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ServiceName: serviceName,
		Revision:    revision,
		ProductName: env.ProductName,
	}, env.Production)
	if err != nil {
		return fmt.Errorf("failed to find service: %s with revision: %d, err: %s", serviceName, revision, err)
	}
	return UninstallService(helmClient, env, revisionSvc, force)
}

// UninstallService uninstall release deployed by zadig
func UninstallService(helmClient helmclient.Client, env *commonmodels.Product, revisionSvc *commonmodels.Service, force bool) error {
	releaseName := util.GeneReleaseName(revisionSvc.GetReleaseNaming(), env.ProductName, env.Namespace, env.EnvName, revisionSvc.ServiceName)

	err := EnsureDeleteZadigServiceByHelmRelease(context.TODO(), env, releaseName, helmClient)
	if err != nil {
		return fmt.Errorf("failed to ensure delete Zadig Service by helm release: %s", err)
	}

	return helmClient.UninstallRelease(&helmclient.ChartSpec{
		ReleaseName: releaseName,
		Namespace:   env.Namespace,
		Wait:        true,
		Force:       force,
		Timeout:     time.Second * 10,
	})
}

// UninstallService uninstall release deployed by zadig
func UninstallRelease(helmClient helmclient.Client, env *commonmodels.Product, releaseName string, force bool) error {
	err := EnsureDeleteZadigServiceByHelmRelease(context.TODO(), env, releaseName, helmClient)
	if err != nil {
		return fmt.Errorf("failed to ensure delete Zadig Service by helm release: %s", err)
	}

	return helmClient.UninstallRelease(&helmclient.ChartSpec{
		ReleaseName: releaseName,
		Namespace:   env.Namespace,
		Wait:        true,
		Force:       force,
		Timeout:     time.Second * 10,
	})
}

// 1. Uninstall related resources
// 2. Delete service info from database
func DeleteHelmReleaseFromEnv(userName, requestID string, productInfo *commonmodels.Product, releaseNames []string, log *zap.SugaredLogger) error {

	helmSvcOfflineLock := cache.NewRedisLock(fmt.Sprintf("product_svc_offline:%s:%s", productInfo.ProductName, productInfo.EnvName))

	helmSvcOfflineLock.Lock()
	defer helmSvcOfflineLock.Unlock()

	log.Infof("remove releases from env, releaseNames: %v", releaseNames)

	helmClient, err := helmtool.NewClientFromNamespace(productInfo.ClusterID, productInfo.Namespace)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	kclient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(productInfo.ClusterID)
	if err != nil {
		return err
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(productInfo.ClusterID)
	if err != nil {
		return err
	}

	releaseNameSet := sets.NewString(releaseNames...)
	prodSvcMap := productInfo.GetServiceMap()
	prodChartSvcMap := productInfo.GetChartServiceMap()
	releaseToServiceNameMap, err := commonutil.GetReleaseNameToServiceNameMap(productInfo)
	if err != nil {
		return fmt.Errorf("failed to get release name to service name map, err: %s", err)
	}
	serviceNameToProdSvcMap := make(map[string]*commonmodels.ProductService)
	releaseNameToChartProdSvcMap := make(map[string]*commonmodels.ProductService)
	for releaseName, serviceName := range releaseToServiceNameMap {
		if !releaseNameSet.Has(releaseName) {
			continue
		}
		if prodSvcMap[serviceName] == nil {
			releaseNameToChartProdSvcMap[releaseName] = prodChartSvcMap[releaseName]
		} else {
			if prodSvcMap[serviceName] == nil {
				return fmt.Errorf("failed to find service %s(release %s) in product %s", serviceName, releaseName, productInfo.ProductName)
			}
			serviceNameToProdSvcMap[serviceName] = prodSvcMap[serviceName]
		}
	}

	newSevices := make([][]*commonmodels.ProductService, 0)
	for _, serviceGroup := range productInfo.Services {
		var group []*commonmodels.ProductService
		for _, service := range serviceGroup {
			if !service.FromZadig() {
				if _, ok := releaseNameToChartProdSvcMap[service.ReleaseName]; !ok {
					group = append(group, service)
				}
			} else {
				if _, ok := serviceNameToProdSvcMap[service.ServiceName]; !ok {
					group = append(group, service)
				}
			}
		}
		newSevices = append(newSevices, group)
	}
	productInfo.Services = newSevices
	err = helmservice.UpdateAllServicesInEnv(productInfo.ProductName, productInfo.EnvName, productInfo.Services, productInfo.Production)
	if err != nil {
		log.Errorf("UpdateHelmProductServices error: %v", err)
		return err
	}

	// services deployed by zadig
	for _, prodSvc := range serviceNameToProdSvcMap {
		if !commonutil.ServiceDeployed(prodSvc.ServiceName, productInfo.ServiceDeployStrategy) {
			continue
		}
		delete(productInfo.ServiceDeployStrategy, prodSvc.ServiceName)
	}

	for _, prodSvc := range releaseNameToChartProdSvcMap {
		if !commonutil.ReleaseDeployed(prodSvc.ReleaseName, productInfo.ServiceDeployStrategy) {
			continue
		}
		delete(productInfo.ServiceDeployStrategy, commonutil.GetReleaseDeployStrategyKey(prodSvc.ReleaseName))
	}

	err = commonrepo.NewProductColl().UpdateDeployStrategy(productInfo.EnvName, productInfo.ProductName, productInfo.ServiceDeployStrategy)
	if err != nil {
		log.Errorf("failed to update product deploy strategy, err: %s", err)
	}

	go func() {
		failedServices := sync.Map{}
		wg := sync.WaitGroup{}

		for _, prodSvc := range serviceNameToProdSvcMap {
			if !commonutil.ServiceDeployed(prodSvc.ServiceName, productInfo.ServiceDeployStrategy) {
				continue
			}
			wg.Add(1)
			go func(product *models.Product, prodSvc *commonmodels.ProductService) {
				defer wg.Done()
				templateSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{ServiceName: prodSvc.ServiceName, Revision: prodSvc.Revision, ProductName: product.ProductName}, productInfo.Production)
				if err != nil {
					failedServices.Store(prodSvc.ServiceName, err.Error())
					return
				}
				if errUninstall := UninstallService(helmClient, productInfo, templateSvc, false); errUninstall != nil {
					errStr := fmt.Sprintf("helm uninstall service %s err: %s", prodSvc.ServiceName, errUninstall)
					failedServices.Store(prodSvc.ServiceName, errStr)
					log.Error(errStr)
				}
			}(productInfo, prodSvc)
		}
		for releaseName, prodSvc := range releaseNameToChartProdSvcMap {
			if !commonutil.ReleaseDeployed(prodSvc.ReleaseName, productInfo.ServiceDeployStrategy) {
				continue
			}
			wg.Add(1)
			go func(product *models.Product, releaseName string) {
				defer wg.Done()
				if errUninstall := UninstallRelease(helmClient, productInfo, releaseName, false); errUninstall != nil {
					errStr := fmt.Sprintf("helm uninstall release %s err: %s", releaseName, errUninstall)
					failedServices.Store(releaseName, errStr)
					log.Error(errStr)
				}
			}(productInfo, releaseName)
		}
		wg.Wait()
		errList := make([]string, 0)
		failedServices.Range(func(key, value interface{}) bool {
			errList = append(errList, value.(string))
			return true
		})
		// send err message to user
		if len(errList) > 0 {
			title := fmt.Sprintf("[%s] 的 [%s] 环境服务删除失败", productInfo.ProductName, productInfo.EnvName)
			notify.SendErrorMessage(userName, title, requestID, errors.New(strings.Join(errList, "\n")), log)
		}

		if productInfo.ShareEnv.Enable && !productInfo.ShareEnv.IsBase {
			err = EnsureGrayEnvConfig(ctx, productInfo, kclient, istioClient)
			if err != nil {
				log.Errorf("Failed to ensure gray env config: %s", err)
			}
		} else if productInfo.IstioGrayscale.Enable && !productInfo.IstioGrayscale.IsBase {
			err = EnsureFullPathGrayScaleConfig(ctx, productInfo, kclient, istioClient)
			if err != nil {
				log.Errorf("Failed to ensure full path gray scale config: %s", err)
			}
		}
	}()
	return nil
}

// DeleteHelmServiceFromEnv deletes the service from the environment
// 1. Uninstall related resources
// 2. Delete service info from database
func DeleteHelmServiceFromEnv(userName, requestID string, productInfo *commonmodels.Product, serviceNames []string, log *zap.SugaredLogger) error {

	helmSvcOfflineLock := cache.NewRedisLock(fmt.Sprintf("product_svc_offline:%s:%s", productInfo.ProductName, productInfo.EnvName))

	helmSvcOfflineLock.Lock()
	defer helmSvcOfflineLock.Unlock()

	log.Infof("remove svc from env, svc: %v", serviceNames)

	helmClient, err := helmtool.NewClientFromNamespace(productInfo.ClusterID, productInfo.Namespace)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	kclient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(productInfo.ClusterID)
	if err != nil {
		return err
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(productInfo.ClusterID)
	if err != nil {
		return err
	}

	deleteServiceSet := sets.NewString(serviceNames...)
	deletedSvcRevision := make(map[string]int64)

	newServices := make([][]*commonmodels.ProductService, 0)
	for _, serviceGroup := range productInfo.Services {
		var group []*commonmodels.ProductService
		for _, service := range serviceGroup {
			if !deleteServiceSet.Has(service.ServiceName) {
				group = append(group, service)
			} else {
				deletedSvcRevision[service.ServiceName] = service.Revision
			}
		}
		newServices = append(newServices, group)
	}
	productInfo.Services = newServices
	err = helmservice.UpdateAllServicesInEnv(productInfo.ProductName, productInfo.EnvName, productInfo.Services, productInfo.Production)
	if err != nil {
		err = fmt.Errorf("UpdateHelmProductServices error: %v", err)
		log.Error(err)
		return err
	}

	// services deployed by zadig
	deployedSvcs := sets.NewString()
	for _, svc := range serviceNames {
		if !commonutil.ServiceDeployed(svc, productInfo.ServiceDeployStrategy) {
			continue
		}
		deployedSvcs.Insert(svc)
	}

	for _, singleName := range serviceNames {
		delete(productInfo.ServiceDeployStrategy, singleName)
	}
	err = commonrepo.NewProductColl().UpdateDeployStrategy(productInfo.EnvName, productInfo.ProductName, productInfo.ServiceDeployStrategy)
	if err != nil {
		log.Errorf("failed to update product deploy strategy, err: %s", err)
	}

	go func() {
		failedServices := sync.Map{}
		wg := sync.WaitGroup{}
		for service, revision := range deletedSvcRevision {
			if !deployedSvcs.Has(service) {
				continue
			}
			wg.Add(1)
			go func(product *models.Product, serviceName string, revision int64) {
				defer wg.Done()
				templateSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{ServiceName: serviceName, Revision: revision, ProductName: product.ProductName}, productInfo.Production)
				if err != nil {
					failedServices.Store(serviceName, err.Error())
					return
				}
				if errUninstall := UninstallService(helmClient, productInfo, templateSvc, false); errUninstall != nil {
					errStr := fmt.Sprintf("helm uninstall service %s err: %s", serviceName, errUninstall)
					failedServices.Store(serviceName, errStr)
					log.Error(errStr)
				}
			}(productInfo, service, revision)
		}
		wg.Wait()
		errList := make([]string, 0)
		failedServices.Range(func(key, value interface{}) bool {
			errList = append(errList, value.(string))
			return true
		})
		// send err message to user
		if len(errList) > 0 {
			title := fmt.Sprintf("[%s] 的 [%s] 环境服务删除失败", productInfo.ProductName, productInfo.EnvName)
			notify.SendErrorMessage(userName, title, requestID, errors.New(strings.Join(errList, "\n")), log)
		}

		if productInfo.ShareEnv.Enable && !productInfo.ShareEnv.IsBase {
			err = EnsureGrayEnvConfig(ctx, productInfo, kclient, istioClient)
			if err != nil {
				log.Errorf("Failed to ensure gray env config: %s", err)
			}
		}
	}()
	return nil
}

func EnsureDeleteZadigServiceByHelmRelease(ctx context.Context, env *commonmodels.Product, releaseName string, helmClient helmclient.Client) error {
	if !env.ShareEnv.Enable && !env.IstioGrayscale.Enable {
		return nil
	}

	release, err := helmClient.GetRelease(releaseName)
	if err != nil {
		return fmt.Errorf("failed to get release %q in namespace %q: %s", releaseName, env.Namespace, err)
	}

	svcNames, err := kubeutil.GetSvcNamesFromManifest(release.Manifest)
	if err != nil {
		return fmt.Errorf("failed to get Service names from manifest: %s", err)
	}

	kclient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(env.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(env.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to get istio client: %s", err)
	}

	for _, svcName := range svcNames {
		err = EnsureDeleteZadigServiceBySvcName(ctx, env, svcName, kclient, istioClient)
		if err != nil {
			return fmt.Errorf("failed to ensure deleting Zadig service by service name %q for env %q of product %q: %s", svcName, env.EnvName, env.ProductName, err)
		}
	}

	return nil
}

func EnsureDeleteK8sService(ctx context.Context, ns, svcName string, kclient client.Client, systemCreatedOnly bool) error {
	svc := &corev1.Service{}
	err := kclient.Get(ctx, client.ObjectKey{
		Name:      svcName,
		Namespace: ns,
	}, svc)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	if systemCreatedOnly && !(svc.Labels != nil && svc.Labels[types.ZadigLabelKeyGlobalOwner] == types.Zadig) {
		return nil
	}

	deleteOption := metav1.DeletePropagationBackground
	return kclient.Delete(ctx, svc, &client.DeleteOptions{
		PropagationPolicy: &deleteOption,
	})
}

func EnsureDeleteZadigServiceBySvcName(ctx context.Context, env *commonmodels.Product, svcName string, kclient client.Client, istioClient versionedclient.Interface) error {
	svc := &corev1.Service{}
	err := kclient.Get(ctx, client.ObjectKey{
		Name:      svcName,
		Namespace: env.Namespace,
	}, svc)
	if err != nil {
		return fmt.Errorf("failed to find Service %q in namespace %q: %s", svcName, env.Namespace, err)
	}

	if env.ShareEnv.Enable {
		return EnsureDeleteZadigService(ctx, env, svc, kclient, istioClient)
	} else if env.IstioGrayscale.Enable {
		return EnsureDeleteGrayscaleService(ctx, env, svc, kclient, istioClient)
	}
	return nil
}

func DeploySingleHelmRelease(product *commonmodels.Product, productSvc *commonmodels.ProductService,
	svcTemp *commonmodels.Service, images []string, timeout int, user string) error {
	chartInfo := productSvc.GetServiceRender()

	var (
		err                      error
		releaseName              string
		replacedMergedValuesYaml string
	)

	releaseName = productSvc.ReleaseName
	if productSvc.FromZadig() {
		releaseName = util.GeneReleaseName(svcTemp.GetReleaseNaming(), svcTemp.ProductName, product.Namespace, product.EnvName, svcTemp.ServiceName)
	}

	err = CheckReleaseInstalledByOtherEnv(sets.NewString(releaseName), product)
	if err != nil {
		return err
	}

	if productSvc.FromZadig() {
		releaseName = util.GeneReleaseName(svcTemp.GetReleaseNaming(), svcTemp.ProductName, product.Namespace, product.EnvName, svcTemp.ServiceName)
		replacedMergedValuesYaml, err = helmservice.NewHelmDeployService().NewGeneMergedValues(productSvc, product.DefaultValues, images)
		if err != nil {
			return fmt.Errorf("failed to gene merged values, err: %s", err)
		}
	} else {
		releaseName = productSvc.ReleaseName
		svcTemp = &commonmodels.Service{
			ServiceName: releaseName,
			ProductName: product.ProductName,
			HelmChart: &commonmodels.HelmChart{
				Name:    chartInfo.ChartName,
				Repo:    chartInfo.ChartRepo,
				Version: chartInfo.ChartVersion,
			},
		}

		replacedMergedValuesYaml, err = helmservice.NewHelmDeployService().NewGeneMergedValues(productSvc, product.DefaultValues, nil)
		if err != nil {
			return fmt.Errorf("failed to gene merged values, err: %s", err)
		}

		chartRepo, err := commonrepo.NewHelmRepoColl().Find(&commonrepo.HelmRepoFindOption{RepoName: chartInfo.ChartRepo})
		if err != nil {
			return fmt.Errorf("failed to query chart-repo info, productName: %s, repoName: %s", product.ProductName, chartInfo.ChartRepo)
		}

		chartRef := fmt.Sprintf("%s/%s", chartInfo.ChartRepo, chartInfo.ChartName)
		localPath := config.LocalServicePathWithRevision(product.ProductName, releaseName, chartInfo.ChartVersion, product.Production)
		// remove local file to untar
		_ = os.RemoveAll(localPath)

		hClient, err := commonutil.NewHelmClient(chartRepo)
		if err != nil {
			return err
		}
		err = hClient.DownloadChart(commonutil.GeneHelmRepo(chartRepo), chartRef, chartInfo.ChartVersion, localPath, true)
		if err != nil {
			return fmt.Errorf("failed to download chart, chartName: %s, chartRepo: %+v, err: %s", chartInfo.ChartName, chartRepo.RepoName, err)
		}
	}

	helmClient, err := helmtool.NewClientFromNamespace(product.ClusterID, product.Namespace)
	if err != nil {
		return err
	}

	param := &ReleaseInstallParam{
		ProductName:  svcTemp.ProductName,
		Namespace:    product.Namespace,
		ReleaseName:  releaseName,
		MergedValues: replacedMergedValuesYaml,
		RenderChart:  chartInfo,
		ServiceObj:   svcTemp,
		Timeout:      timeout,
		Production:   product.Production,
	}
	if !productSvc.FromZadig() {
		param.IsChartInstall = true
	}

	ensureUpgrade := func() error {
		hrs, errHistory := helmClient.ListReleaseHistory(param.ReleaseName, 10)
		if errHistory != nil {
			// list history should not block deploy operation, error will be logged instead of returned
			return nil
		}
		if len(hrs) == 0 {
			return nil
		}
		releaseutil.Reverse(hrs, releaseutil.SortByRevision)
		rel := hrs[0]

		if rel.Info.Status.IsPending() {
			return fmt.Errorf("failed to upgrade release: %s with exceptional status: %s", param.ReleaseName, rel.Info.Status)
		}
		return nil
	}

	err = ensureUpgrade()
	if err != nil {
		return err
	}

	// when replace image, should not wait
	err = InstallOrUpgradeHelmChartWithValues(param, false, helmClient)
	if err != nil {
		return err
	}

	err = helmservice.UpdateServiceInEnv(product, productSvc, user)
	return err
}

func DeployMultiHelmRelease(productResp *commonmodels.Product, helmClient *helmtool.HelmClient, filter DeploySvcFilter, user string, log *zap.SugaredLogger) error {
	productName, envName := productResp.ProductName, productResp.EnvName

	session := mongotool.Session()
	defer session.EndSession(context.TODO())

	err := mongotool.StartTransaction(session)
	if err != nil {
		return err
	}

	handler := func(param *ReleaseInstallParam, isRetry bool, log *zap.SugaredLogger) (err error) {
		defer func() {
			if param.ProdService != nil {
				if err != nil {
					param.ProdService.Error = err.Error()
				} else {
					err = commonutil.CreateEnvServiceVersion(productResp, param.ProdService, user, session, log)
					if err != nil {
						log.Errorf("failed to create service version, err: %v", err)
					}

					param.ProdService.Error = ""
				}
			}
		}()

		if !param.ProdService.FromZadig() {
			chartRepo, err := commonrepo.NewHelmRepoColl().Find(&commonrepo.HelmRepoFindOption{RepoName: param.RenderChart.ChartRepo})
			if err != nil {
				return fmt.Errorf("failed to query chart-repo info, productName: %s, repoName: %s", productResp.ProductName, param.RenderChart.ChartRepo)
			}

			chartRef := fmt.Sprintf("%s/%s", param.RenderChart.ChartRepo, param.RenderChart.ChartName)
			localPath := config.LocalServicePathWithRevision(param.ProductName, param.ReleaseName, param.RenderChart.ChartVersion, param.Production)
			// remove local file to untar
			_ = os.RemoveAll(localPath)

			hClient, err := commonutil.NewHelmClient(chartRepo)
			if err != nil {
				return err
			}

			err = hClient.DownloadChart(commonutil.GeneHelmRepo(chartRepo), chartRef, param.RenderChart.ChartVersion, localPath, true)
			if err != nil {
				return fmt.Errorf("failed to download chart, chartName: %s, chartRepo: %+v, err: %s", param.RenderChart.ChartName, chartRepo.RepoName, err)
			}
		}

		errInstall := InstallOrUpgradeHelmChartWithValues(param, isRetry, helmClient)
		if errInstall != nil {
			log.Errorf("failed to upgrade service: %s, namespace: %s, isRetry: %v, err: %s", param.ServiceObj.ServiceName, productResp.Namespace, isRetry, errInstall)
			err = fmt.Errorf("failed to upgrade service %s, err: %s", param.ServiceObj.ServiceName, errInstall)
		}
		return
	}

	productResp.LintServices()
	errList := new(multierror.Error)
	for groupIndex, groupServices := range productResp.Services {
		installParamList := make([]*ReleaseInstallParam, 0)
		for _, prodSvc := range groupServices {
			chartInfo := findRenderChartFromList(prodSvc, productResp.ServiceRenders)
			if chartInfo == nil {
				continue
			}
			if filter != nil && !filter(prodSvc) {
				continue
			}
			if !commonutil.ChartDeployed(chartInfo, productResp.ServiceDeployStrategy) {
				continue
			}

			param, err := BuildInstallParam(productResp.DefaultValues, productResp, chartInfo, prodSvc)
			if err != nil {
				log.Errorf("failed to generate install param, service: %s, namespace: %s, err: %s", prodSvc.ServiceName, productResp.Namespace, err)
				mongotool.AbortTransaction(session)
				return err
			}
			prodSvc.Render = chartInfo
			prodSvc.UpdateTime = time.Now().Unix()
			installParamList = append(installParamList, param)

		}
		groupServiceErr := BatchExecutorWithRetry(3, time.Millisecond*500, installParamList, handler, log)
		if groupServiceErr != nil {
			errList = multierror.Append(errList, groupServiceErr...)
		}

		err := helmservice.UpdateServicesGroupInEnv(productName, envName, groupIndex, groupServices, productResp.Production)
		if err != nil {
			log.Errorf("failed to UpdateHelmProductServices %s/%s, error: %v", productName, envName, err)
			mongotool.AbortTransaction(session)
			return err
		}
	}
	err = mongotool.CommitTransaction(session)
	if err != nil {
		return err
	}
	return errList.ErrorOrNil()
}

func findRenderChartFromList(svc *commonmodels.ProductService, renderCharts []*templatemodels.ServiceRender) *templatemodels.ServiceRender {
	for _, rChart := range renderCharts {
		if rChart.DeployedFromZadig() && svc.FromZadig() && rChart.ServiceName == svc.ServiceName {
			return rChart
		}
		if !rChart.DeployedFromZadig() && !svc.FromZadig() && rChart.ReleaseName == svc.ReleaseName {
			return rChart
		}
	}
	return nil
}

func BuildInstallParam(defaultValues string, productInfo *commonmodels.Product, renderChart *templatemodels.ServiceRender, productSvc *commonmodels.ProductService) (*ReleaseInstallParam, error) {
	productName, namespace, envName := productInfo.ProductName, productInfo.Namespace, productInfo.EnvName
	productSvc.Render = renderChart

	ret := &ReleaseInstallParam{
		ProductName:    productName,
		Namespace:      namespace,
		RenderChart:    renderChart,
		ProdService:    productSvc,
		IsChartInstall: renderChart.IsHelmChartDeploy,
	}

	if productSvc.FromZadig() {
		opt := &commonrepo.ServiceFindOption{
			ServiceName: productSvc.ServiceName,
			Type:        productSvc.Type,
			Revision:    productSvc.Revision,
			ProductName: productName,
		}
		templateSvc, err := repository.QueryTemplateService(opt, productInfo.Production)
		if err != nil {
			log.Errorf("failed to find service %s, err %s", productSvc.ServiceName, err.Error())
			return nil, nil
		}
		ret.ServiceObj = templateSvc
		ret.ReleaseName = util.GeneReleaseName(templateSvc.GetReleaseNaming(), templateSvc.ProductName, namespace, envName, templateSvc.ServiceName)
	} else {
		serviceObj := &commonmodels.Service{
			ServiceName: renderChart.ReleaseName,
			ProductName: productName,
			HelmChart: &commonmodels.HelmChart{
				Name:    renderChart.ChartName,
				Repo:    renderChart.ChartRepo,
				Version: renderChart.ChartVersion,
			},
		}
		ret.ServiceObj = serviceObj
		ret.ReleaseName = renderChart.ReleaseName
	}

	helmDeploySvc := helmservice.NewHelmDeployService()
	finalValues, err := helmDeploySvc.NewGeneMergedValues(productSvc, defaultValues, nil)
	if err != nil {
		return ret, err
	}

	ret.MergedValues = finalValues
	ret.Production = productInfo.Production
	return ret, nil
}

func BatchExecutorWithRetry(retryCount uint64, interval time.Duration, paramList []*ReleaseInstallParam, handler IntervalExecutorHandler, log *zap.SugaredLogger) []error {
	bo := backoff.NewConstantBackOff(time.Second * 3)
	retryBo := backoff.WithMaxRetries(bo, retryCount)
	errList := make([]error, 0)
	isRetry := false
	_ = backoff.Retry(func() error {
		failedParams := make([]*ReleaseInstallParam, 0)
		errList = batchExecutor(interval, paramList, &failedParams, isRetry, handler, log)
		if len(errList) == 0 {
			return nil
		}
		log.Infof("%d services waiting to retry", len(failedParams))
		paramList = failedParams
		isRetry = true
		return fmt.Errorf("%d services apply failed", len(errList))
	}, retryBo)
	return errList
}

func batchExecutor(interval time.Duration, serviceList []*ReleaseInstallParam, failedParams *[]*ReleaseInstallParam, isRetry bool, handler IntervalExecutorHandler, log *zap.SugaredLogger) []error {
	if len(serviceList) == 0 {
		return nil
	}
	errList := make([]error, 0)
	for _, data := range serviceList {
		err := handler(data, isRetry, log)
		if err != nil {
			errList = append(errList, err)
			*failedParams = append(*failedParams, data)
			log.Errorf("service:%s apply failed, err %s", data.ServiceObj.ServiceName, err)
		}
		time.Sleep(interval)
	}
	return errList
}
