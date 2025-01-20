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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"istio.io/client-go/pkg/apis/networking/v1alpha3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/shared/kube/resource"
	"github.com/koderover/zadig/v2/pkg/shared/kube/wrapper"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	zadigtypes "github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
	jsonutil "github.com/koderover/zadig/v2/pkg/util/json"
)

func FillProductTemplateValuesYamls(tmpl *templatemodels.Product, production bool, log *zap.SugaredLogger) error {
	tmpl.ChartInfos = make([]*templatemodels.ServiceRender, 0)
	latestSvcs, err := repository.ListMaxRevisionsServices(tmpl.ProductName, production)
	if err != nil {
		return fmt.Errorf("failed to list max revision services, err: %s", err)
	}
	for _, svc := range latestSvcs {
		if svc.HelmChart == nil {
			log.Errorf("helm chart of service: %s is nil", svc.ServiceName)
			continue
		}
		tmpl.ChartInfos = append(tmpl.ChartInfos, &templatemodels.ServiceRender{
			ServiceName:  svc.ServiceName,
			ChartVersion: svc.HelmChart.Version,
			// ValuesYaml:   svc.HelmChart.ValuesYaml,
		})
	}
	return nil
}

type ServiceResp struct {
	ServiceName        string            `json:"service_name"`
	ReleaseName        string            `json:"release_name"`
	IsHelmChartDeploy  bool              `json:"is_helm_chart_deploy"`
	ServiceDisplayName string            `json:"service_display_name"`
	Type               string            `json:"type"`
	Status             string            `json:"status"`
	Error              string            `json:"error"`
	Images             []string          `json:"images,omitempty"`
	ProductName        string            `json:"product_name"`
	EnvName            string            `json:"env_name"`
	Ingress            *IngressInfo      `json:"ingress"`
	IstioGateway       *IstioGatewayInfo `json:"istio_gateway"`
	//deprecated
	Ready          string              `json:"ready"`
	EnvStatuses    []*models.EnvStatus `json:"env_statuses,omitempty"`
	WorkLoadType   string              `json:"workLoadType"`
	Revision       int64               `json:"revision"`
	EnvConfigs     []*models.EnvConfig `json:"env_configs"`
	Updatable      bool                `json:"updatable"`
	DeployStrategy string              `json:"deploy_strategy"`
	// ZadigXReleaseType represents the service contain created by zadigx release workflow
	// frontend should limit some operations on these services
	ZadigXReleaseType string `json:"zadigx_release_type"`
	ZadigXReleaseTag  string `json:"zadigx_release_tag"`
}

type IngressInfo struct {
	HostInfo []resource.HostInfo `json:"host_info"`
}

type IstioGatewayInfo struct {
	Servers []IstioGatewayServer `json:"servers"`
}

type IstioGatewayServer struct {
	Host         string `json:"host"`
	PortProtocol string `json:"port_protocol"`
	PortNumber   uint32 `json:"port_number"`
}

func UnMarshalSourceDetail(source interface{}) (*models.CreateFromRepo, error) {
	bs, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	ret := &models.CreateFromRepo{}
	err = json.Unmarshal(bs, ret)
	return ret, err
}

func FillGitNamespace(yamlData *templatemodels.CustomYaml) error {
	if yamlData == nil || yamlData.Source != setting.SourceFromGitRepo {
		return nil
	}
	sourceDetail, err := UnMarshalSourceDetail(yamlData.SourceDetail)
	if err != nil {
		return err
	}
	if sourceDetail.GitRepoConfig == nil {
		return nil
	}
	sourceDetail.GitRepoConfig.Namespace = sourceDetail.GitRepoConfig.GetNamespace()
	yamlData.SourceDetail = sourceDetail
	return nil
}

func latestVariables(variableKVs []*commontypes.RenderVariableKV, serviceTemplate *models.Service) ([]*commontypes.RenderVariableKV, string, error) {
	if serviceTemplate == nil {
		yamlStr, err := commontypes.RenderVariableKVToYaml(variableKVs, true)
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert render variableKVs to yaml, err: %s", err)
		}
		return variableKVs, yamlStr, nil
	}
	yamlStr, mergedVariableKVs, err := commontypes.MergeRenderAndServiceTemplateVariableKVs(variableKVs, serviceTemplate.ServiceVariableKVs)
	if err != nil {
		return nil, "", fmt.Errorf("failed to merge render and service variableKVs, err: %s", err)
	}
	return mergedVariableKVs, yamlStr, nil
}

func GetK8sProductionSvcRenderArgs(productName, envName, serviceName string, log *zap.SugaredLogger) ([]*K8sSvcRenderArg, error) {
	var productInfo *models.Product
	var err error
	productInfo, err = commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: util.GetBoolPointer(true),
	})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("failed to find envrionment : %s/%s", productName, envName))
	}

	prodSvc := productInfo.GetServiceMap()[serviceName]
	if prodSvc == nil {
		return nil, fmt.Errorf("failed to find service : %s/%s/%s", productName, envName, serviceName)
	}

	prodTemplateSvc, err := commonrepo.NewProductionServiceColl().Find(&commonrepo.ServiceFindOption{
		ProductName: productName,
		ServiceName: serviceName,
		Revision:    prodSvc.Revision,
	})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Errorf("failed to find production service : %s/%s/%s", productName, envName, serviceName).Error())
	}

	svcRender := prodSvc.GetServiceRender()

	ret := make([]*K8sSvcRenderArg, 0)
	rArg := &K8sSvcRenderArg{
		ServiceName:  serviceName,
		VariableYaml: prodTemplateSvc.VariableYaml,
	}
	//if svcRender != nil && svcRender.OverrideYaml != nil {
	if len(svcRender.OverrideYaml.RenderVariableKVs) > 0 {
		prodTemplateSvc.ServiceVars = setting.ServiceVarWildCard
		rArg.VariableKVs, rArg.VariableYaml, err = latestVariables(svcRender.OverrideYaml.RenderVariableKVs, prodTemplateSvc)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest variables, error: %w", err)
		}
	}
	ret = append(ret, rArg)
	return ret, nil
}

func GetK8sSvcRenderArgs(productName, envName, serviceName string, production bool, log *zap.SugaredLogger) ([]*K8sSvcRenderArg, *models.RenderSet, error) {
	var productInfo *models.Product
	var err error
	if len(envName) > 0 {
		productInfo, err = commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
			Name:       productName,
			EnvName:    envName,
			Production: &production,
		})
	}

	if err != nil && err != mongo.ErrNoDocuments {
		return nil, nil, err
	}
	if err != nil {
		productInfo = nil
	}

	svcRenders := make(map[string]*templatemodels.ServiceRender)

	// product template svcs
	templateSvcs, err := repository.ListMaxRevisionsServices(productName, production)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find template svcs, err: %s", err)
	}
	templateSvcMap := make(map[string]*models.Service)
	for _, svc := range templateSvcs {
		svcRenders[svc.ServiceName] = &templatemodels.ServiceRender{
			ServiceName:  svc.ServiceName,
			OverrideYaml: &templatemodels.CustomYaml{YamlContent: svc.VariableYaml},
		}
		templateSvcMap[svc.ServiceName] = svc
	}

	rendersetObj := &models.RenderSet{}

	// svc used in products
	productSvcMap := make(map[string]*models.ProductService)
	if productInfo != nil {
		rendersetObj.GlobalVariables = productInfo.GlobalVariables
		rendersetObj.DefaultValues = productInfo.DefaultValues
		rendersetObj.YamlData = productInfo.YamlData
		rendersetObj.ServiceVariables = productInfo.GetAllSvcRenders()

		svcs, err := commonutil.GetProductUsedTemplateSvcs(productInfo)
		if err != nil {
			return nil, nil, err
		}
		for _, svc := range svcs {
			svcRenders[svc.ServiceName] = productInfo.GetSvcRender(svc.ServiceName)
		}
		productSvcMap = productInfo.GetServiceMap()
	}

	validSvcs := sets.NewString(strings.Split(serviceName, ",")...)
	filter := func(name string) bool {
		// if service name is not set, use the current services in product
		if len(serviceName) == 0 {
			_, ok := productSvcMap[name]
			return ok
		}
		return validSvcs.Has(name)
	}

	ret := make([]*K8sSvcRenderArg, 0)
	for name, svcRender := range svcRenders {
		if !filter(name) {
			continue
		}
		rArg := &K8sSvcRenderArg{
			ServiceName: svcRender.ServiceName,
		}
		if svcRender.OverrideYaml != nil {
			rArg.VariableYaml = svcRender.OverrideYaml.YamlContent
			rArg.VariableKVs = svcRender.OverrideYaml.RenderVariableKVs
			rArg.LatestVariableKVs, rArg.LatestVariableYaml, err = latestVariables(rArg.VariableKVs, templateSvcMap[svcRender.ServiceName])
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get latest variables, error: %w", err)
			}
		}
		ret = append(ret, rArg)
	}
	return ret, rendersetObj, nil
}

type ValuesResp struct {
	ValuesYaml string `json:"valuesYaml"`
}

func GetChartValues(projectName, envName, serviceName string, isHelmChartDeploy bool, production bool, allValues bool) (*ValuesResp, error) {
	opt := &commonrepo.ProductFindOptions{Name: projectName, EnvName: envName, Production: util.GetBoolPointer(production)}
	prod, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to find project: %s, err: %s", projectName, err)
	}

	helmClient, err := helmtool.NewClientFromNamespace(prod.ClusterID, prod.Namespace)
	if err != nil {
		log.Errorf("[%s][%s] NewClientFromRestConf error: %s", envName, projectName, err)
		return nil, fmt.Errorf("failed to init helm client, err: %s", err)
	}

	releaseName := serviceName
	if !isHelmChartDeploy {
		serviceMap := prod.GetServiceMap()
		prodSvc, ok := serviceMap[serviceName]
		if !ok {
			return nil, fmt.Errorf("failed to find service: %s in env: %s", serviceName, envName)
		}

		revisionSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
			ServiceName: serviceName,
			Revision:    prodSvc.Revision,
			ProductName: prodSvc.ProductName,
		}, production)
		if err != nil {
			return nil, err
		}

		releaseName = util.GeneReleaseName(revisionSvc.GetReleaseNaming(), prodSvc.ProductName, prod.Namespace, prod.EnvName, prodSvc.ServiceName)
	}
	valuesMap, err := helmClient.GetReleaseValues(releaseName, allValues)
	if err != nil {
		log.Errorf("failed to get values map data, err: %s", err)
		return nil, err
	}

	currentValuesYaml, err := yaml.Marshal(valuesMap)
	if err != nil {
		return nil, err
	}

	return &ValuesResp{ValuesYaml: string(currentValuesYaml)}, nil
}

type GetSvcRenderRequest struct {
	GetSvcRendersArgs []*GetSvcRenderArg `json:"get_svc_render_args"`
}

type GetSvcRenderArg struct {
	ServiceOrReleaseName string `json:"service_or_release_name"`
	IsHelmChartDeploy    bool   `json:"is_helm_chart_deploy"`
}

func GetSvcRenderArgs(productName, envName string, getSvcRendersArgs []*GetSvcRenderArg, log *zap.SugaredLogger) ([]*HelmSvcRenderArg, *models.RenderSet, error) {
	ret := make([]*HelmSvcRenderArg, 0)
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    productName,
		EnvName: envName,
	})

	if err == mongo.ErrNoDocuments {
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	rendersetObj := &models.RenderSet{}
	rendersetObj.DefaultValues = productInfo.DefaultValues
	rendersetObj.YamlData = productInfo.YamlData
	rendersetObj.ChartInfos = productInfo.GetAllSvcRenders()

	svcChartRenderArgSet := sets.NewString()
	svcRenderArgSet := sets.NewString()
	for _, arg := range getSvcRendersArgs {
		if arg.IsHelmChartDeploy {
			svcChartRenderArgSet.Insert(arg.ServiceOrReleaseName)
		} else {
			svcRenderArgSet.Insert(arg.ServiceOrReleaseName)
		}
	}
	matchedRenderChartModels := make([]*templatemodels.ServiceRender, 0)
	if len(getSvcRendersArgs) == 0 {
		matchedRenderChartModels = productInfo.GetAllSvcRenders()
	} else {
		for _, singleChart := range productInfo.GetAllSvcRenders() {
			if singleChart.IsHelmChartDeploy {
				if svcChartRenderArgSet.Has(singleChart.ReleaseName) {
					matchedRenderChartModels = append(matchedRenderChartModels, singleChart)
				}
			} else {
				if svcRenderArgSet.Has(singleChart.ServiceName) {
					matchedRenderChartModels = append(matchedRenderChartModels, singleChart)
				}
			}
		}
	}

	for _, singleChart := range matchedRenderChartModels {
		rcaObj := &HelmSvcRenderArg{}
		rcaObj.LoadFromRenderChartModel(singleChart)
		rcaObj.EnvName = envName
		err = FillGitNamespace(productInfo.YamlData)
		if err != nil {
			// Note, since user can always reselect the git info, error should not block normal logic
			log.Warnf("failed to fill git namespace data, err: %s", err)
		}
		rcaObj.YamlData = singleChart.OverrideYaml
		ret = append(ret, rcaObj)
	}
	return ret, rendersetObj, nil
}

// fill service display name if necessary
func fillServiceInfo(svcList []*ServiceResp, productInfo *models.Product) {
	if productInfo.Source != setting.SourceFromHelm {
		return
	}
	chartSvcMap := productInfo.GetChartServiceMap()
	for _, svc := range svcList {
		chartSvc := chartSvcMap[svc.ServiceName]
		if chartSvc != nil {
			svc.IsHelmChartDeploy = true
		}
		svc.ServiceDisplayName = svc.ServiceName
	}
}

// ListWorkloadDetailsInEnv returns all workloads in the given env which meet the filter.
// this function is used for two scenarios: 1. calculate product status 2. list workflow details
func BuildWorkloadFilterFunc(productInfo *models.Product, projectInfo *templatemodels.Product, filter string, log *zap.SugaredLogger) ([]FilterFunc, error) {
	filterArray := []FilterFunc{
		func(workloads []*Workload) []*Workload {
			if !projectInfo.IsHostProduct() {
				return workloads
			}

			productServiceNames := sets.NewString()
			for _, svc := range productInfo.GetServiceMap() {
				if len(svc.Resources) > 0 {
					productServiceNames.Insert(svc.Resources[0].Name)
				}
			}

			var res []*Workload
			for _, workload := range workloads {
				if productServiceNames.Has(workload.Name) {
					res = append(res, workload)
				}
			}
			return res
		},
	}

	// for helm service, only show deploys/stss created by zadig
	if projectInfo.IsHelmProduct() {
		filterArray = append(filterArray, func(workloads []*Workload) []*Workload {
			releaseNameMap, err := commonutil.GetReleaseNameToServiceNameMap(productInfo)
			if err != nil {
				log.Errorf("failed to generate relase map for product: %s:%s", productInfo.ProductName, productInfo.EnvName)
				return workloads
			}

			var res []*Workload
			for _, workload := range workloads {
				if len(workload.Annotation) == 0 {
					continue
				}
				releaseName := workload.Annotation[setting.HelmReleaseNameAnnotation]
				if _, ok := releaseNameMap[releaseName]; ok {
					res = append(res, workload)
				}
			}
			return res
		})
	}

	if filter != "" {
		filterArray = append(filterArray, func(workloads []*Workload) []*Workload {
			data, err := jsonutil.ToJSON(filter)
			if err != nil {
				log.Errorf("Invalid filter, err: %s", err)
				return workloads
			}

			f := &workloadFilter{}
			if err = json.Unmarshal(data, f); err != nil {
				log.Errorf("Invalid filter, err: %s", err)
				return workloads
			}

			// it is a fuzzy matching
			var res []*Workload
			for _, workload := range workloads {
				if f.Match(workload) {
					res = append(res, workload)
				}
			}
			return res
		})
	}
	return filterArray, nil
}

// ListWorkloadsInEnv returns all workloads in the given env which meet the filter with no detailed info (images or pods)
func ListWorkloadsInEnv(envName, productName, filter string, perPage, page int, log *zap.SugaredLogger) (int, []*Workload, error) {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	productInfo, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, productName, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}

	// find project info
	projectInfo, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, productName, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}

	filterArray, err := BuildWorkloadFilterFunc(productInfo, projectInfo, filter, log)
	if err != nil {
		return 0, nil, err
	}

	cls, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(productInfo.ClusterID)
	if err != nil {
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}
	informer, err := clientmanager.NewKubeClientManager().GetInformer(productInfo.ClusterID, productInfo.Namespace)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, productInfo.Namespace, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}
	version, err := cls.Discovery().ServerVersion()
	if err != nil {
		log.Errorf("Failed to get server version info for cluster: %s, the error is: %s", productInfo.ClusterID, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}
	return ListWorkloads(envName, productName, perPage, page, informer, version, log, filterArray...)
}

// ListWorkloadDetailsInEnv returns all workload details in the given env which meet the filter.
// A filter is in this format: a=b,c=d, and it is a fuzzy matching. Which means it will return all records with a field called
// a and the value contain character b.
func ListWorkloadDetailsInEnv(envName, productName, filter string, perPage, page int, log *zap.SugaredLogger) (int, []*ServiceResp, error) {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	productInfo, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, productName, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}

	// find project info
	projectInfo, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, productName, err)
		return 0, nil, e.ErrListGroups.AddDesc(err.Error())
	}

	filterArray, err := BuildWorkloadFilterFunc(productInfo, projectInfo, filter, log)
	if err != nil {
		return 0, nil, err
	}

	count, resp, err := ListWorkloadDetails(envName, productInfo.ClusterID, productInfo.Namespace, productName, perPage, page, log, filterArray...)
	if err != nil {
		return count, resp, err
	}

	fillServiceInfo(resp, productInfo)
	return count, resp, nil
}

type FilterFunc func(services []*Workload) []*Workload

type workloadFilter struct {
	Name            string      `json:"name"`
	ReleaseName     string      `json:"releaseName"`
	ReleaseNameList sets.String `json:"-"`
	ChartName       string      `json:"chartName"`
	ChartNameList   sets.String `json:"-"`
}

type wfAlias workloadFilter

func (f *workloadFilter) UnmarshalJSON(data []byte) error {
	aliasData := &wfAlias{}
	if err := json.Unmarshal(data, aliasData); err != nil {
		return err
	}
	f.Name = aliasData.Name
	f.ReleaseName = aliasData.ReleaseName
	f.ChartName = aliasData.ChartName
	if f.ReleaseName != "*" && f.ReleaseName != "" {
		serviceNames := strings.Split(f.ReleaseName, "|")
		f.ReleaseNameList = sets.NewString(serviceNames...)
	}
	if f.ChartName != "*" && f.ChartName != "" {
		chartNames := strings.Split(f.ChartName, "|")
		f.ChartNameList = sets.NewString(chartNames...)
	}
	return nil
}

func (f *workloadFilter) Match(workload *Workload) bool {
	if len(f.Name) > 0 {
		if !strings.Contains(workload.Name, f.Name) {
			return false
		}
	}
	if f.ReleaseNameList.Len() > 0 {
		if !f.ReleaseNameList.Has(workload.ReleaseName) {
			return false
		}
	}
	if f.ChartNameList.Len() > 0 {
		if !f.ChartNameList.Has(workload.ChartName) {
			return false
		}
	}
	return true
}

type Workload struct {
	EnvName           string                     `json:"env_name"`
	Name              string                     `json:"name"`
	Type              string                     `json:"type"`
	ServiceName       string                     `json:"-"`
	DeployedFromZadig bool                       `json:"-"`
	ProductName       string                     `json:"product_name"`
	Replicas          int32                      `json:"-"`
	Spec              corev1.PodTemplateSpec     `json:"-"`
	Selector          *metav1.LabelSelector      `json:"-"`
	Images            []string                   `json:"-"`
	Containers        []*resource.ContainerImage `json:"-"`
	Ready             bool                       `json:"ready"`
	Annotation        map[string]string          `json:"-"`
	Status            string                     `json:"-"`
	ReleaseName       string                     `json:"-"` //ReleaseName refers to the releaseName of helm services
	ChartName         string                     `json:"-"` //ChartName refers to chartName of helm services
}

// fillServiceName set service name defined in zadig to workloads, this would be helpful for helm release view
// TODO optimize this function, use release.template to filter related resources
func fillServiceName(envName, productName string, workloads []*Workload) error {
	if len(envName) == 0 {
		return nil
	}
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    productName,
		EnvName: envName,
	})
	if err != nil {
		return err
	}
	if productInfo.Source != setting.SourceFromHelm {
		return nil
	}
	releaseChartNameMap, err := commonutil.GetReleaseNameToChartNameMap(productInfo)
	if err != nil {
		return err
	}
	releaseServiceNameMap, err := commonutil.GetReleaseNameToServiceNameMap(productInfo)
	if err != nil {
		return err
	}
	for _, wl := range workloads {
		if chartRelease, ok := wl.Annotation[setting.HelmReleaseNameAnnotation]; ok {
			wl.ReleaseName = chartRelease
			wl.ChartName = releaseChartNameMap[wl.ReleaseName]
			wl.ServiceName = releaseServiceNameMap[wl.ReleaseName]
		}
	}
	return nil
}

func ListWorkloads(envName, productName string, perPage, page int, informer informers.SharedInformerFactory, version *version.Info, log *zap.SugaredLogger, filter ...FilterFunc) (int, []*Workload, error) {
	var workLoads []*Workload
	listDeployments, err := getter.ListDeploymentsWithCache(nil, informer)
	if err != nil {
		return 0, workLoads, e.ErrListGroups.AddDesc(err.Error())
	}

	for _, v := range listDeployments {
		workLoads = append(workLoads, &Workload{
			Name:       v.Name,
			Spec:       v.Spec.Template,
			Selector:   v.Spec.Selector,
			Type:       setting.Deployment,
			Replicas:   *v.Spec.Replicas,
			Images:     wrapper.Deployment(v).ImageInfos(),
			Containers: wrapper.Deployment(v).GetContainers(),
			Ready:      wrapper.Deployment(v).Ready(),
			Annotation: v.Annotations,
		})
	}
	statefulSets, err := getter.ListStatefulSetsWithCache(nil, informer)
	if err != nil {
		return 0, workLoads, e.ErrListGroups.AddDesc(err.Error())
	}
	for _, v := range statefulSets {
		workLoads = append(workLoads, &Workload{
			Name:       v.Name,
			Spec:       v.Spec.Template,
			Selector:   v.Spec.Selector,
			Type:       setting.StatefulSet,
			Replicas:   *v.Spec.Replicas,
			Images:     wrapper.StatefulSet(v).ImageInfos(),
			Containers: wrapper.StatefulSet(v).GetContainers(),
			Ready:      wrapper.StatefulSet(v).Ready(),
			Annotation: v.Annotations,
		})
	}

	cronJobs, coronBeta, err := getter.ListCronJobsWithCache(nil, informer, kubeclient.VersionLessThan121(version))
	if err != nil {
		return 0, workLoads, e.ErrListGroups.AddDesc(err.Error())
	}
	wrappedCronJobs := make([]wrapper.CronJobItem, 0)
	for _, v := range cronJobs {
		wrappedCronJobs = append(wrappedCronJobs, wrapper.CronJob(v, nil))
	}
	for _, v := range coronBeta {
		wrappedCronJobs = append(wrappedCronJobs, wrapper.CronJob(nil, v))
	}
	getSuspendStr := func(suspend bool) string {
		if suspend {
			return "True"
		} else {
			return "False"
		}
	}
	for _, cronJob := range wrappedCronJobs {
		workLoads = append(workLoads, &Workload{
			Name:       cronJob.GetName(),
			Type:       setting.CronJob,
			Images:     cronJob.ImageInfos(),
			Annotation: cronJob.GetAnnotations(),
			Ready:      true,
			Status:     fmt.Sprintf("SUSPEND: %s", getSuspendStr(cronJob.GetSuspend())),
		})
	}

	err = fillServiceName(envName, productName, workLoads)
	// err of getting service name should not block the return of workloads
	if err != nil {
		log.Warnf("failed to set service name for workloads, error: %s", err)
	}

	for _, f := range filter {
		workLoads = f(workLoads)
	}

	sort.SliceStable(workLoads, func(i, j int) bool { return workLoads[i].Name < workLoads[j].Name })
	count := len(workLoads)
	if page > 0 && perPage > 0 {
		start := (page - 1) * perPage
		if start >= count {
			workLoads = nil
		} else if start+perPage >= count {
			workLoads = workLoads[start:]
		} else {
			workLoads = workLoads[start : start+perPage]
		}
	}
	return count, workLoads, nil
}

func ListWorkloadDetails(envName, clusterID, namespace, productName string, perPage, page int, log *zap.SugaredLogger, filter ...FilterFunc) (int, []*ServiceResp, error) {
	var resp = make([]*ServiceResp, 0)
	cls, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(clusterID)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, namespace, err)
		return 0, resp, e.ErrListGroups.AddDesc(err.Error())
	}
	informer, err := clientmanager.NewKubeClientManager().GetInformer(clusterID, namespace)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, namespace, err)
		return 0, resp, e.ErrListGroups.AddDesc(err.Error())
	}
	version, err := cls.Discovery().ServerVersion()
	if err != nil {
		log.Errorf("Failed to get server version info for cluster: %s, the error is: %s", clusterID, err)
		return 0, resp, e.ErrListGroups.AddDesc(err.Error())
	}
	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(clusterID)
	if err != nil {
		return 0, resp, e.ErrListGroups.AddErr(fmt.Errorf("failed to new istio client: %s", err))
	}

	count, workLoads, err := ListWorkloads(envName, productName, perPage, page, informer, version, log, filter...)
	if err != nil {
		log.Errorf("failed to list workloads, [%s][%s], error: %v", namespace, envName, err)
		return 0, resp, e.ErrListGroups.AddDesc(err.Error())
	}

	hostInfos := make([]resource.HostInfo, 0)
	if kubeclient.VersionLessThan122(version) {
		ingresses, err := getter.ListExtensionsV1Beta1Ingresses(nil, informer)
		if err == nil {
			for _, ingress := range ingresses {
				hostInfos = append(hostInfos, wrapper.Ingress(ingress).HostInfo()...)
			}
		} else {
			log.Warnf("Failed to list ingresses, the error is: %s", err)
		}
	} else {
		ingresses, err := getter.ListNetworkingV1Ingress(nil, informer)
		if err == nil {
			for _, ingress := range ingresses {
				hostInfos = append(hostInfos, wrapper.GetIngressHostInfo(ingress)...)
			}
		} else {
			log.Warnf("Failed to list ingresses, the error is: %s", err)
		}
	}

	var gwObjs *v1alpha3.GatewayList
	istioInstalled, err := kube.CheckIstiodInstalled(context.TODO(), cls)
	if err != nil {
		log.Warnf("failed to check istiod whether installed: %s", err)
	} else {
		if istioInstalled {
			zadigLabels := map[string]string{
				zadigtypes.ZadigLabelKeyGlobalOwner: zadigtypes.Zadig,
			}
			gwObjs, err = istioClient.NetworkingV1alpha3().Gateways(namespace).List(context.TODO(), metav1.ListOptions{
				LabelSelector: labels.FormatLabels(zadigLabels),
			})
			if err != nil {
				log.Warnf("Failed to list istio gateways, the error is: %s", err)
			}
		}
	}

	// get all services
	allServices, err := getter.ListServicesWithCache(nil, informer)
	if err != nil {
		log.Errorf("[%s][%s] list service error: %s", envName, namespace, err)
	}

	for _, workload := range workLoads {
		tmpProductName := workload.ProductName
		if tmpProductName == "" && productName != "" {
			tmpProductName = productName
		}
		productRespInfo := &ServiceResp{
			ServiceName:  workload.Name,
			ReleaseName:  workload.Annotation[setting.HelmReleaseNameAnnotation],
			EnvName:      workload.EnvName,
			Type:         setting.K8SDeployType,
			WorkLoadType: workload.Type,
			ProductName:  tmpProductName,
			Images:       workload.Images,
			Ready:        setting.PodReady,
			Status:       setting.PodRunning,
		}

		if workload.Type == setting.Deployment || workload.Type == setting.StatefulSet {
			selector := labels.SelectorFromSet(workload.Selector.MatchLabels)
			// Note: In some scenarios, such as environment sharing, there may be more containers in Pod than workload.
			// We call GetSelectedPodsInfo to get the status and readiness to keep same logic with k8s projects
			productRespInfo.Status, productRespInfo.Ready, productRespInfo.Images = kube.GetSelectedPodsInfo(selector, informer, workload.Images, log)
			productRespInfo.Ingress = &IngressInfo{
				HostInfo: FindServiceFromIngress(hostInfos, workload, allServices),
			}
			productRespInfo.IstioGateway = &IstioGatewayInfo{
				Servers: FindServiceFromIstioGateway(gwObjs, workload.ServiceName),
			}
		} else if workload.Type == setting.CronJob {
			productRespInfo.Status = workload.Status
		}
		resp = append(resp, productRespInfo)
	}

	return count, resp, nil
}

func FindServiceFromIngress(hostInfos []resource.HostInfo, currentWorkload *Workload, allServices []*corev1.Service) []resource.HostInfo {
	if len(allServices) == 0 || len(hostInfos) == 0 {
		return []resource.HostInfo{}
	}
	serviceName := ""
	podLabels := labels.Set(currentWorkload.Selector.MatchLabels)
	for _, svc := range allServices {
		if len(svc.Spec.Selector) == 0 {
			continue
		}
		if labels.SelectorFromValidatedSet(svc.Spec.Selector).Matches(podLabels) {
			serviceName = svc.Name
			break
		}
	}
	if serviceName == "" {
		return []resource.HostInfo{}
	}

	resp := make([]resource.HostInfo, 0)
	for _, hostInfo := range hostInfos {
		for _, backend := range hostInfo.Backends {
			if backend.ServiceName == serviceName {
				resp = append(resp, hostInfo)
				break
			}
		}
	}
	return resp
}

func FindServiceFromIstioGateway(gwObjs *v1alpha3.GatewayList, serviceName string) []IstioGatewayServer {
	resp := []IstioGatewayServer{}
	if gwObjs == nil || len(gwObjs.Items) == 0 {
		return resp
	}
	gatewayName := commonutil.GenIstioGatewayName(serviceName)
	for _, gwObj := range gwObjs.Items {
		if gwObj.Name == gatewayName {
			for _, serverObj := range gwObj.Spec.Servers {
				if len(serverObj.Hosts) == 0 {
					continue
				}

				server := IstioGatewayServer{
					Host:         serverObj.Hosts[0],
					PortProtocol: serverObj.Port.Protocol,
					PortNumber:   serverObj.Port.Number,
				}
				resp = append(resp, server)
			}
			break
		}
	}

	return resp
}

// GetHelmServiceName get service name from annotations of resources deployed by helm
// resType currently only support Deployment, StatefulSet and CronJob
// this function needs to be optimized
func GetHelmServiceName(prod *models.Product, resType, resName string, kubeClient client.Client, version *version.Info) (string, error) {
	res := &unstructured.Unstructured{}
	namespace := prod.Namespace

	nameMap, err := commonutil.GetReleaseNameToServiceNameMap(prod)
	if err != nil {
		return "", err
	}

	switch resType {
	case setting.Deployment:
		res.SetGroupVersionKind(getter.DeploymentGVK)
	case setting.StatefulSet:
		res.SetGroupVersionKind(getter.StatefulSetGVK)
	case setting.CronJob:
		if !kubeclient.VersionLessThan121(version) {
			res.SetGroupVersionKind(getter.CronJobGVK)
		} else {
			res.SetGroupVersionKind(getter.CronJobV1BetaGVK)
		}
	default:
		return "", fmt.Errorf("unsupported resource type %s", resType)
	}

	found, err := getter.GetResourceInCache(namespace, resName, res, kubeClient)
	if err != nil {
		return "", fmt.Errorf("failed to find resource %s, type %s, err %s", resName, resType, err.Error())
	}
	if !found {
		return "", fmt.Errorf("failed to find resource %s, type %s", resName, resType)
	}
	annotation := res.GetAnnotations()
	if len(annotation) > 0 {
		releaseName := annotation[setting.HelmReleaseNameAnnotation]
		if serviceName, ok := nameMap[releaseName]; ok {
			return serviceName, nil
		}
	}
	return "", fmt.Errorf("failed to get annotation from resource %s, type %s", resName, resType)
}

type ZadigServiceStatusResp struct {
	ServiceName string
	PodStatus   string
	Ready       string
	Ingress     []*resource.Ingress
	Images      []string
	Workloads   []*Workload
}

func QueryPodsStatus(productInfo *commonmodels.Product, serviceTmpl *commonmodels.Service, serviceName string, clientset *kubernetes.Clientset, informer informers.SharedInformerFactory, log *zap.SugaredLogger) *ZadigServiceStatusResp {
	resp := &ZadigServiceStatusResp{
		ServiceName: serviceName,
		PodStatus:   setting.PodError,
		Ready:       setting.PodNotReady,
		Ingress:     nil,
		Images:      []string{},
	}

	svcResp, err := GetServiceImpl(serviceTmpl.ServiceName, serviceTmpl, "", productInfo, clientset, informer, log)
	if err != nil {
		log.Errorf("failed to get %s service impl, error: %v", serviceTmpl.ServiceName, err)
		return resp
	}

	resp.Ingress = svcResp.Ingress
	resp.Workloads = svcResp.Workloads
	if len(serviceTmpl.Containers) == 0 {
		resp.PodStatus = setting.PodSucceeded
		resp.Ready = setting.PodReady
		return resp
	}

	suspendCronJobCount := 0
	for _, cronJob := range svcResp.CronJobs {
		if cronJob.Suspend {
			suspendCronJobCount++
		}
	}

	pods := make([]*resource.Pod, 0)
	for _, svc := range svcResp.Scales {
		pods = append(pods, svc.Pods...)
	}

	if len(pods) == 0 && len(svcResp.CronJobs) == 0 {
		imageSet := sets.String{}
		for _, workload := range svcResp.Workloads {
			imageSet.Insert(workload.Images...)
		}

		resp.Images = imageSet.List()
		resp.PodStatus, resp.Ready = setting.PodNonStarted, setting.PodNotReady
		return resp
	}

	imageSet := sets.String{}
	for _, pod := range pods {
		for _, container := range pod.Containers {
			imageSet.Insert(container.Image)
		}
	}

	for _, cronJob := range svcResp.CronJobs {
		for _, image := range cronJob.Images {
			imageSet.Insert(image.Image)
		}
	}

	resp.Images = imageSet.List()

	ready := setting.PodReady

	if len(svcResp.Workloads) == 0 && len(svcResp.CronJobs) > 0 {
		if len(svcResp.CronJobs) == suspendCronJobCount {
			resp.PodStatus = setting.ServiceStatusAllSuspended
		} else if suspendCronJobCount == 0 {
			resp.PodStatus = setting.ServiceStatusNoSuspended
		} else {
			resp.PodStatus = setting.ServiceStatusPartSuspended
		}
		return resp
	}

	succeededPods := 0
	for _, pod := range pods {
		if pod.Succeed {
			succeededPods++
			continue
		}
		if !pod.Ready {
			resp.PodStatus, resp.Ready = setting.PodUnstable, setting.PodNotReady
			return resp
		}
	}

	if len(pods) == succeededPods {
		resp.PodStatus, resp.Ready = string(corev1.PodSucceeded), setting.JobReady
		return resp
	}

	resp.PodStatus, resp.Ready = setting.PodRunning, ready
	return resp
}
