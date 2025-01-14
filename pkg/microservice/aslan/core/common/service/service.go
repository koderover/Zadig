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
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	templ "text/template"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/helm/pkg/releaseutil"
	"sigs.k8s.io/yaml"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/webhook"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	internalresource "github.com/koderover/zadig/v2/pkg/shared/kube/resource"
	"github.com/koderover/zadig/v2/pkg/shared/kube/wrapper"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
)

const (
	InterceptCommitID = 8
)

type yamlPreview struct {
	Kind string `json:"kind"`
}

type ServiceTmplResp struct {
	Data  []*ServiceProductMap `json:"data"`
	Total int                  `json:"total"`
}

type ServiceTmplBuildObject struct {
	ServiceTmplObject *ServiceTmplObject  `json:"pm_service_tmpl"`
	Build             *commonmodels.Build `json:"build"`
}

type ServiceTmplObject struct {
	ProductName        string                           `json:"product_name"`
	ServiceName        string                           `json:"service_name"`
	Visibility         string                           `json:"visibility"`
	Revision           int64                            `json:"revision"`
	Type               string                           `json:"type"`
	Username           string                           `json:"username"`
	EnvConfigs         []*commonmodels.EnvConfig        `json:"env_configs"`
	EnvStatuses        []*commonmodels.EnvStatus        `json:"env_statuses,omitempty"`
	From               string                           `json:"from,omitempty"`
	HealthChecks       []*commonmodels.PmHealthCheck    `json:"health_checks"`
	EnvName            string                           `json:"env_name"`
	VariableYaml       string                           `json:"variable_yaml"`
	StartCmd           string                           `json:"start_cmd"`
	StopCmd            string                           `json:"stop_cmd"`
	RestartCmd         string                           `json:"restart_cmd"`
	ServiceVariableKVs []*commontypes.ServiceVariableKV `json:"service_variable_kvs"`
}

type ContainerWithBuilds struct {
	*commonmodels.Container
	BuildNames []string `json:"build_names"`
}

type ServiceProductMap struct {
	Service          string                 `json:"service_name"`
	Source           string                 `json:"source"`
	Type             string                 `json:"type"`
	Product          []string               `json:"product"`
	ProductName      string                 `json:"product_name"`
	Containers       []*ContainerWithBuilds `json:"containers,omitempty"`
	CodehostID       int                    `json:"codehost_id"`
	RepoOwner        string                 `json:"repo_owner"`
	RepoNamespace    string                 `json:"repo_namespace"`
	RepoName         string                 `json:"repo_name"`
	RepoUUID         string                 `json:"repo_uuid"`
	BranchName       string                 `json:"branch_name"`
	LoadPath         string                 `json:"load_path"`
	LoadFromDir      bool                   `json:"is_dir"`
	GerritRemoteName string                 `json:"gerrit_remote_name,omitempty"`
	CreateFrom       interface{}            `json:"create_from"`
	AutoSync         bool                   `json:"auto_sync"`
	//estimated merged variable is set when the service is created from template
	EstimatedMergedVariable    string                           `json:"estimated_merged_variable"`
	EstimatedMergedVariableKVs []*commontypes.ServiceVariableKV `json:"estimated_merged_variable_kvs"`
}

type EnvService struct {
	ServiceName        string                          `json:"service_name"`
	ServiceModules     []*commonmodels.Container       `json:"service_modules"`
	VariableYaml       string                          `json:"variable_yaml"`
	OverrideKVs        string                          `json:"override_kvs"`
	VariableKVs        []*commontypes.RenderVariableKV `json:"variable_kvs"`
	LatestVariableYaml string                          `json:"latest_variable_yaml"`
	LatestVariableKVs  []*commontypes.RenderVariableKV `json:"latest_variable_kvs"`
	Updatable          bool                            `json:"updatable"`
	Deployed           bool                            `json:"deployed"`
}

type EnvServices struct {
	ProductName string        `json:"product_name"`
	EnvName     string        `json:"env_name"`
	Services    []*EnvService `json:"services"`
}

type SvcResources struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type TemplateSvcResp struct {
	*commonmodels.Service
	Resources []*SvcResources `json:"resources"`
}

var (
	imageParseRegex = regexp.MustCompile(`(?P<repo>.+/)?(?P<image>[^:]+){1}(:)?(?P<tag>.+)?`)
)

func FillContainerBuilds(source []*commonmodels.Container) []*ContainerWithBuilds {
	ret := make([]*ContainerWithBuilds, 0)
	for _, c := range source {
		ret = append(ret, &ContainerWithBuilds{
			Container:  c,
			BuildNames: nil,
		})
	}
	return ret
}

func GetCreateFromChartTemplate(createFrom interface{}) (*models.CreateFromChartTemplate, error) {
	bs, err := json.Marshal(createFrom)
	if err != nil {
		return nil, err
	}
	ret := &models.CreateFromChartTemplate{}
	err = json.Unmarshal(bs, ret)
	return ret, err
}

func FillServiceCreationInfo(serviceTemplate *models.Service) error {
	if serviceTemplate.Source != setting.SourceFromChartTemplate {
		return nil
	}
	creation, err := GetCreateFromChartTemplate(serviceTemplate.CreateFrom)
	if err != nil {
		return fmt.Errorf("failed to get creation detail: %s", err)
	}

	if creation.YamlData == nil || creation.YamlData.Source != setting.SourceFromGitRepo {
		return nil
	}

	bs, err := json.Marshal(creation.YamlData.SourceDetail)
	if err != nil {
		return err
	}
	cfr := &models.CreateFromRepo{}
	err = json.Unmarshal(bs, cfr)
	if err != nil {
		return err
	}
	if cfr.GitRepoConfig == nil {
		return nil
	}
	cfr.GitRepoConfig.Namespace = cfr.GitRepoConfig.GetNamespace()
	creation.YamlData.SourceDetail = cfr
	serviceTemplate.CreateFrom = creation
	return nil
}

// ListServiceTemplate 列出服务模板
func ListServiceTemplate(productName string, production bool, log *zap.SugaredLogger) (*ServiceTmplResp, error) {
	var err error
	resp := new(ServiceTmplResp)
	resp.Data = make([]*ServiceProductMap, 0)
	productTmpl, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		log.Errorf("Can not find project %s, error: %s", productName, err)
		return resp, e.ErrListTemplate.AddDesc(err.Error())
	}

	services, err := repository.ListMaxRevisionsServices(productName, production)
	if err != nil {
		log.Errorf("Failed to list services by %+v, err: %s", productName, err)
		return resp, e.ErrListTemplate.AddDesc(err.Error())
	}

	estimatedVariableYamlMap, estimatedVariableKVMap := GetEstimatedMergedVariables(services, productTmpl)
	for _, serviceObject := range services {
		if serviceObject.Source == setting.SourceFromGitlab {
			if serviceObject.CodehostID == 0 {
				return nil, e.ErrListTemplate.AddDesc("codehost id is empty")
			}
			err = fillServiceRepoInfo(serviceObject)
			if err != nil {
				log.Errorf("Failed to load info from url: %s, the error is: %s", serviceObject.SrcPath, err)
				return nil, e.ErrListTemplate.AddDesc(fmt.Sprintf("Failed to load info from url: %s, the error is: %+v", serviceObject.SrcPath, err))
			}
			serviceObject.LoadFromDir = true
		} else if serviceObject.Source == setting.SourceFromGithub && serviceObject.RepoName == "" {
			if serviceObject.CodehostID == 0 {
				return nil, e.ErrListTemplate.AddDesc("codehost id is empty")
			}
			err = fillServiceRepoInfo(serviceObject)
			if err != nil {
				return nil, err
			}
			serviceObject.LoadFromDir = true
		}

		err = FillServiceCreationInfo(serviceObject)
		if err != nil {
			log.Warnf("faile to fill service creation info: %s", err)
		}
		spmap := &ServiceProductMap{
			Service:                    serviceObject.ServiceName,
			Type:                       serviceObject.Type,
			Source:                     serviceObject.Source,
			ProductName:                serviceObject.ProductName,
			Containers:                 FillContainerBuilds(serviceObject.Containers),
			Product:                    []string{productName},
			CodehostID:                 serviceObject.CodehostID,
			RepoOwner:                  serviceObject.RepoOwner,
			RepoNamespace:              serviceObject.GetRepoNamespace(),
			RepoName:                   serviceObject.RepoName,
			RepoUUID:                   serviceObject.RepoUUID,
			BranchName:                 serviceObject.BranchName,
			LoadFromDir:                serviceObject.LoadFromDir,
			LoadPath:                   serviceObject.LoadPath,
			GerritRemoteName:           serviceObject.GerritRemoteName,
			CreateFrom:                 serviceObject.CreateFrom,
			AutoSync:                   serviceObject.AutoSync,
			EstimatedMergedVariable:    estimatedVariableYamlMap[serviceObject.ServiceName],
			EstimatedMergedVariableKVs: estimatedVariableKVMap[serviceObject.ServiceName],
		}

		resp.Data = append(resp.Data, spmap)
	}

	return resp, nil
}

func GetEstimatedMergedVariables(services []*commonmodels.Service, product *template.Product) (map[string]string, map[string][]*commontypes.ServiceVariableKV) {
	retYamlMap := make(map[string]string)
	retKVMap := make(map[string][]*commontypes.ServiceVariableKV)
	if product.IsK8sYamlProduct() {
		templateMap := make(map[string]*commonmodels.YamlTemplate)
		k8sYamlTemplates, _, _ := commonrepo.NewYamlTemplateColl().List(0, 0)
		for _, t := range k8sYamlTemplates {
			templateMap[t.ID.Hex()] = t
		}

		for _, service := range services {
			if service.Source != setting.ServiceSourceTemplate {
				continue
			}
			retYamlMap[service.ServiceName] = service.VariableYaml
			retKVMap[service.ServiceName] = service.ServiceVariableKVs

			sourceTemplate, ok := templateMap[service.TemplateID]
			if !ok {
				continue
			}

			_, mergedKVs, err := commontypes.MergeServiceAndServiceTemplateVariableKVs(service.ServiceVariableKVs, sourceTemplate.ServiceVariableKVs)
			if err != nil {
				log.Errorf("Failed to merge service variable kvs if not exist, error: %s", err)
				continue
			}
			mergedYaml, mergedKVs, err := commontypes.ClipServiceVariableKVs(sourceTemplate.ServiceVariableKVs, mergedKVs)
			if err != nil {
				log.Errorf("Failed to clip service variable kvs, error: %s", err)
				continue
			}

			retYamlMap[service.ServiceName] = mergedYaml
			retKVMap[service.ServiceName] = mergedKVs
		}
	}

	if product.IsHelmProduct() {
		for _, service := range services {
			if service.Source != setting.SourceFromChartTemplate {
				continue
			}
			creation, err := GetCreateFromChartTemplate(service.CreateFrom)
			if err == nil {
				if creation.YamlData != nil {
					retYamlMap[service.ServiceName] = creation.YamlData.YamlContent
				}
			}
		}
	}

	return retYamlMap, retKVMap
}

// ListWorkloadTemplate 列出实例模板
func ListWorkloadTemplate(productName, envName string, production bool, log *zap.SugaredLogger) (*ServiceTmplResp, error) {
	var err error
	resp := new(ServiceTmplResp)
	resp.Data = make([]*ServiceProductMap, 0)

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: productName, EnvName: envName})
	if err != nil {
		return nil, e.ErrListTemplate.AddErr(fmt.Errorf("failed to get productinfo: %s/%s, err: %s", productName, envName, err))
	}

	for _, serviceObject := range productInfo.GetSvcList() {
		spmap := &ServiceProductMap{
			Service:     serviceObject.ServiceName,
			Type:        serviceObject.Type,
			Source:      setting.SourceFromExternal,
			ProductName: serviceObject.ProductName,
			Containers:  FillContainerBuilds(serviceObject.Containers),
		}

		if !production {
			for _, c := range spmap.Containers {
				buildObjs, err := commonrepo.NewBuildColl().List(&commonrepo.BuildListOption{ProductName: productName, ServiceName: serviceObject.ServiceName, Targets: []string{c.Name}})
				if err != nil {
					continue
				}
				buildNames := sets.NewString()
				for _, buildObj := range buildObjs {
					buildNames.Insert(buildObj.Name)
				}
				c.BuildNames = buildNames.List()
			}
		}
		resp.Data = append(resp.Data, spmap)
	}

	return resp, nil
}

func GetServiceTemplateWithStructure(serviceName, serviceType, productName, excludeStatus string, revision int64, production bool, log *zap.SugaredLogger) (*TemplateSvcResp, error) {
	svcTemplate, err := GetServiceTemplate(serviceName, serviceType, productName, excludeStatus, revision, production, log)
	resp := &TemplateSvcResp{
		Resources: []*SvcResources{},
		Service:   svcTemplate,
	}
	if err != nil {
		return resp, err
	}
	resp.Resources = GeneSvcStructure(svcTemplate)
	return resp, nil
}

func GeneSvcStructure(svcTemplate *models.Service) []*SvcResources {
	if svcTemplate.Type != setting.K8SDeployType {
		return nil
	}
	resources := make([]*SvcResources, 0)
	renderedYaml, err := commonutil.RenderK8sSvcYamlStrict(svcTemplate.Yaml, svcTemplate.ProductName, svcTemplate.ServiceName, svcTemplate.VariableYaml)
	if err != nil {
		log.Errorf("failed to render k8s svc yaml: %s/%s, err: %s", svcTemplate.ProductName, svcTemplate.ServiceName, err)
	}
	renderedYaml = config.ServiceNameAlias.ReplaceAllLiteralString(renderedYaml, svcTemplate.ServiceName)
	renderedYaml = config.ProductNameAlias.ReplaceAllLiteralString(renderedYaml, svcTemplate.ProductName)

	renderedYaml = util.ReplaceWrapLine(renderedYaml)
	yamlDataArray := util.SplitYaml(renderedYaml)
	for _, yamlData := range yamlDataArray {
		resKind := new(types.KubeResourceKind)
		if err := yaml.Unmarshal([]byte(yamlData), &resKind); err != nil {
			log.Errorf("unmarshal ResourceKind error: %v", err)
			continue
		}
		if resKind == nil {
			continue
		}
		resources = append(resources, &SvcResources{Kind: resKind.Kind, Name: resKind.Metadata.Name})
	}
	return resources
}

func GetServiceTemplate(serviceName, serviceType, productName, excludeStatus string, revision int64, production bool, log *zap.SugaredLogger) (*commonmodels.Service, error) {
	opt := &commonrepo.ServiceFindOption{
		ServiceName: serviceName,
		Type:        serviceType,
		Revision:    revision,
		ProductName: productName,
	}
	if excludeStatus != "" {
		opt.ExcludeStatus = excludeStatus
	}
	resp, err := repository.QueryTemplateService(opt, production)
	if err != nil {
		err = func() error {
			if !commonrepo.IsErrNoDocuments(err) {
				return err
			}
			return err
		}()

		if err != nil {
			errMsg := fmt.Sprintf("[ServiceTmpl.Find] %s error: %v", serviceName, err)
			log.Error(errMsg)
			return resp, e.ErrGetTemplate.AddDesc(errMsg)
		}
	}

	if resp.Type == setting.PMDeployType {
		err = fillPmInfo(resp)
		if err != nil {
			errMsg := fmt.Sprintf("[ServiceTmpl.Find] %s fillPmInfo error: %v", serviceName, err)
			log.Error(errMsg)
			return resp, e.ErrGetTemplate.AddDesc(errMsg)
		}
	}

	if resp.Source == setting.SourceFromGitlab && resp.RepoName == "" {
		if resp.CodehostID == 0 {
			return nil, e.ErrGetTemplate.AddDesc("Please confirm if codehost exists")
		}
		err = fillServiceRepoInfo(resp)
		if err != nil {
			log.Errorf("Failed to load info from url: %s, the error is: %s", resp.SrcPath, err)
			return nil, e.ErrGetService.AddDesc(fmt.Sprintf("Failed to load info from url: %s, the error is: %+v", resp.SrcPath, err))
		}
		return resp, nil
	} else if resp.Source == setting.SourceFromGithub {
		if resp.CodehostID == 0 {
			return nil, e.ErrGetTemplate.AddDesc("Please confirm if codehost exists")
		}
		err = fillServiceRepoInfo(resp)
		if err != nil {
			return nil, err
		}

		return resp, nil

	} else if resp.Source == setting.SourceFromGUI {
		yamls := util.SplitYaml(resp.Yaml)
		for _, y := range yamls {
			data, err := yaml.YAMLToJSON([]byte(y))
			if err != nil {
				log.Errorf("convert yaml to json failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			var result interface{}
			err = json.Unmarshal(data, &result)
			if err != nil {
				log.Errorf("unmarshal yaml data failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			yamlPreview := yamlPreview{}
			err = json.Unmarshal(data, &yamlPreview)
			if err != nil {
				log.Errorf("unmarshal yaml data failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			if resp.GUIConfig == nil {
				resp.GUIConfig = new(commonmodels.GUIConfig)
			}

			switch yamlPreview.Kind {
			case "Deployment":
				resp.GUIConfig.Deployment = result
			case "Ingress":
				resp.GUIConfig.Ingress = result
			case "Service":
				resp.GUIConfig.Service = result
			}
		}
	}

	resp.RepoNamespace = resp.GetRepoNamespace()
	return resp, nil
}

// GetProductionServiceTemplate is basically the same version of GetServiceTemplate, except that it does not support PM type service. And reads from production service coll.
func GetProductionServiceTemplate(serviceName, serviceType, productName, excludeStatus string, revision int64, log *zap.SugaredLogger) (*commonmodels.Service, error) {
	opt := &commonrepo.ServiceFindOption{
		ServiceName: serviceName,
		Type:        serviceType,
		Revision:    revision,
		ProductName: productName,
	}
	if excludeStatus != "" {
		opt.ExcludeStatus = excludeStatus
	}

	resp, err := commonrepo.NewProductionServiceColl().Find(opt)
	if err != nil {
		err = func() error {
			if !commonrepo.IsErrNoDocuments(err) {
				return err
			}
			return err
		}()

		if err != nil {
			errMsg := fmt.Sprintf("[ServiceTmpl.Find] %s error: %v", serviceName, err)
			log.Error(errMsg)
			return resp, e.ErrGetTemplate.AddDesc(errMsg)
		}
	}

	if resp.Source == setting.SourceFromGitlab && resp.RepoName == "" {
		if resp.CodehostID == 0 {
			return nil, e.ErrGetTemplate.AddDesc("Please confirm if codehost exists")
		}
		err = fillServiceRepoInfo(resp)
		if err != nil {
			log.Errorf("Failed to load info from url: %s, the error is: %s", resp.SrcPath, err)
			return nil, e.ErrGetService.AddDesc(fmt.Sprintf("Failed to load info from url: %s, the error is: %+v", resp.SrcPath, err))
		}
		return resp, nil
	} else if resp.Source == setting.SourceFromGithub {
		if resp.CodehostID == 0 {
			return nil, e.ErrGetTemplate.AddDesc("Please confirm if codehost exists")
		}
		err = fillServiceRepoInfo(resp)
		if err != nil {
			return nil, err
		}

		return resp, nil

	} else if resp.Source == setting.SourceFromGUI {
		yamls := util.SplitYaml(resp.Yaml)
		for _, y := range yamls {
			data, err := yaml.YAMLToJSON([]byte(y))
			if err != nil {
				log.Errorf("convert yaml to json failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			var result interface{}
			err = json.Unmarshal(data, &result)
			if err != nil {
				log.Errorf("unmarshal yaml data failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			yamlPreview := yamlPreview{}
			err = json.Unmarshal(data, &yamlPreview)
			if err != nil {
				log.Errorf("unmarshal yaml data failed, yaml:%s, err:%v", y, err)
				return nil, err
			}

			if resp.GUIConfig == nil {
				resp.GUIConfig = new(commonmodels.GUIConfig)
			}

			switch yamlPreview.Kind {
			case "Deployment":
				resp.GUIConfig.Deployment = result
			case "Ingress":
				resp.GUIConfig.Ingress = result
			case "Service":
				resp.GUIConfig.Service = result
			}
		}
	}
	resp.RepoNamespace = resp.GetRepoNamespace()
	return resp, nil
}

func fillPmInfo(svc *commonmodels.Service) error {
	pms, err := commonrepo.NewPrivateKeyColl().List(&commonrepo.PrivateKeyArgs{})
	if err != nil {
		if commonrepo.IsErrNoDocuments(err) {
			return nil
		}
		return err
	}
	pmMaps := make(map[string]*commonmodels.PrivateKey, len(pms))
	for _, pm := range pms {
		pmMaps[pm.ID.Hex()] = pm
	}
	for _, envStatus := range svc.EnvStatuses {
		if pm, ok := pmMaps[envStatus.HostID]; ok {
			envStatus.PmInfo = &commonmodels.PmInfo{
				ID:       pm.ID,
				Name:     pm.Name,
				IP:       pm.IP,
				Port:     pm.Port,
				Status:   pm.Status,
				Label:    pm.Label,
				IsProd:   pm.IsProd,
				Provider: pm.Provider,
			}
		}
	}
	return nil
}

func UpdatePmServiceTemplate(username string, args *ServiceTmplBuildObject, log *zap.SugaredLogger) error {
	//该请求来自环境中的服务更新时，from=createEnv
	if args.ServiceTmplObject.From == "" {
		if err := UpdateBuild(username, args.Build, log); err != nil {
			return err
		}
	}

	//先比较healthcheck是否有变动
	preService, err := GetServiceTemplate(args.ServiceTmplObject.ServiceName, setting.PMDeployType, args.ServiceTmplObject.ProductName, setting.ProductStatusDeleting, args.ServiceTmplObject.Revision, false, log)
	if err != nil {
		return err
	}

	preBuildName := preService.BuildName

	//更新服务
	rev, err := commonutil.GenerateServiceNextRevision(false, preService.ServiceName, preService.ProductName)
	if err != nil {
		return err
	}
	preService.HealthChecks = args.ServiceTmplObject.HealthChecks
	preService.Revision = rev
	preService.CreateBy = username
	preService.BuildName = args.Build.Name
	preService.EnvConfigs = args.ServiceTmplObject.EnvConfigs
	preService.EnvStatuses = args.ServiceTmplObject.EnvStatuses

	// if the service template is generated from creating the env, we don't update the commands
	if args.ServiceTmplObject.From != "createEnv" {
		preService.StartCmd = args.ServiceTmplObject.StartCmd
		preService.StopCmd = args.ServiceTmplObject.StopCmd
		preService.RestartCmd = args.ServiceTmplObject.RestartCmd
	}

	if err := commonrepo.NewServiceColl().Delete(preService.ServiceName, setting.PMDeployType, args.ServiceTmplObject.ProductName, setting.ProductStatusDeleting, preService.Revision); err != nil {
		return err
	}

	if preBuildName != args.Build.Name {
		preBuild, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: preBuildName})
		if err != nil {
			return e.ErrUpdateService.AddDesc("get pre build failed")
		}

		var targets []*commonmodels.ServiceModuleTarget
		for _, serviceModule := range preBuild.Targets {
			if serviceModule.ServiceName != args.ServiceTmplObject.ServiceName {
				targets = append(targets, serviceModule)
			}
		}
		preBuild.Targets = targets

		if err = UpdateBuild(username, preBuild, log); err != nil {
			return e.ErrUpdateService.AddDesc("update pre build failed")
		}

		currentBuild, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: args.Build.Name})
		if err != nil {
			return e.ErrUpdateService.AddDesc("get current build failed")
		}
		var include bool
		for _, serviceModule := range currentBuild.Targets {
			if serviceModule.ServiceName == args.ServiceTmplObject.ServiceName {
				include = true
				break
			}
		}

		if !include {
			currentBuild.Targets = append(currentBuild.Targets, &commonmodels.ServiceModuleTarget{
				ProductName: args.ServiceTmplObject.ProductName,
				ServiceWithModule: commonmodels.ServiceWithModule{
					ServiceName:   args.ServiceTmplObject.ServiceName,
					ServiceModule: args.ServiceTmplObject.ServiceName,
				},
			})
			if err = UpdateBuild(username, currentBuild, log); err != nil {
				return e.ErrUpdateService.AddDesc("update current build failed")
			}
		}
	}

	if err := commonrepo.NewServiceColl().Create(preService); err != nil {
		return err
	}
	return nil
}

func DeleteServiceWebhookByName(serviceName, productName string, production bool, logger *zap.SugaredLogger) {
	svc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{ServiceName: serviceName, ProductName: productName}, production)
	if err != nil {
		logger.Errorf("Failed to get service %s, error: %s", serviceName, err)
		return
	}
	ProcessServiceWebhook(nil, svc, serviceName, production, logger)
}

func needProcessWebhook(source string) bool {
	if source == setting.ServiceSourceTemplate || source == setting.SourceFromZadig || source == setting.SourceFromGerrit ||
		source == "" || source == setting.SourceFromExternal || source == setting.SourceFromChartTemplate ||
		source == setting.SourceFromChartRepo || source == setting.SourceFromCustomEdit {
		return false
	}
	return true
}

// @todo add production service support
func ProcessServiceWebhook(updated, current *commonmodels.Service, serviceName string, production bool, logger *zap.SugaredLogger) {
	var action string
	var updatedHooks, currentHooks []*webhook.WebHook
	if updated != nil {
		if !needProcessWebhook(updated.Source) {
			return
		}
		action = "add"
		address, err := GetGitlabAddress(updated.SrcPath)
		if err != nil {
			log.Errorf("failed to parse codehost address, err: %s", err)
			return
		}
		if address == "" {
			return
		}
		updatedHooks = append(updatedHooks, &webhook.WebHook{
			Owner:      updated.RepoOwner,
			Namespace:  updated.GetRepoNamespace(),
			Repo:       updated.RepoName,
			Address:    address,
			Name:       "trigger",
			CodeHostID: updated.CodehostID,
		})
	}
	if current != nil {
		if !needProcessWebhook(current.Source) {
			return
		}
		action = "remove"
		address, err := GetGitlabAddress(current.SrcPath)
		if err != nil {
			log.Errorf("failed to parse codehost address, err: %s", err)
			return
		}
		//address := getAddressFromPath(current.SrcPath, current.GetRepoNamespace(), current.RepoName, logger.Desugar())
		if address == "" {
			return
		}
		currentHooks = append(currentHooks, &webhook.WebHook{
			Owner:      current.RepoOwner,
			Namespace:  current.GetRepoNamespace(),
			Repo:       current.RepoName,
			Address:    address,
			Name:       "trigger",
			CodeHostID: current.CodehostID,
		})
	}
	if updated != nil && current != nil {
		action = "update"
	}

	logger.Debugf("Start to %s webhook for service %s, production %v", action, serviceName, production)
	name := webhook.ServicePrefix + serviceName
	if production {
		name = webhook.ServicePrefix + "production-" + serviceName
	}
	err := ProcessWebhook(updatedHooks, currentHooks, name, logger)
	if err != nil {
		logger.Errorf("Failed to process WebHook, error: %s", err)
	}

}

func getAddressFromPath(path, owner, repo string, logger *zap.Logger) string {
	res := strings.Split(path, fmt.Sprintf("/%s/%s/", owner, repo))
	if len(res) != 2 {
		logger.With(zap.String("path", path), zap.String("owner", owner), zap.String("repo", repo)).DPanic("Invalid path")
		return ""
	}
	return res[0]
}

// ExtractImageRegistry extract registry url from total image uri
func ExtractImageRegistry(imageURI string) (string, error) {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageURI)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			if exNames[i] == "repo" {
				// if matchedStr format is like '10.10.1.30:8473/xxx/yyy'
				// the url.Parse will return error 'first path segment in URL cannot contain colon'
				// so we have to know it has schema or not first
				if !util.HasSchema(matchedStr) {
					return matchedStr, nil
				}

				u, err := url.Parse(matchedStr)
				if err != nil {
					return "", err
				}
				if len(u.Scheme) > 0 {
					matchedStr = strings.TrimPrefix(matchedStr, fmt.Sprintf("%s://", u.Scheme))
				}
				return matchedStr, nil
			}
		}
	}
	return "", fmt.Errorf("failed to extract registry url")
}

// ExtractImageTag extract image tag from total image uri
func ExtractImageTag(imageURI string) string {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageURI)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			if exNames[i] == "tag" {
				return matchedStr
			}
		}
	}
	return ""
}

// ExtractRegistryNamespace extract registry namespace from image uri
func ExtractRegistryNamespace(imageURI string) string {
	imageURI = strings.TrimPrefix(imageURI, "http://")
	imageURI = strings.TrimPrefix(imageURI, "https://")

	imageComponent := strings.Split(imageURI, "/")
	if len(imageComponent) <= 2 {
		return ""
	}

	nsComponent := imageComponent[1 : len(imageComponent)-1]
	return strings.Join(nsComponent, "/")
}

type Variable struct {
	SERVICE        string
	IMAGE_NAME     string
	TIMESTAMP      string
	TASK_ID        string
	REPO_COMMIT_ID string
	PROJECT        string
	ENV_NAME       string
	REPO_TAG       string
	REPO_BRANCH    string
	REPO_PR        string
}

type candidate struct {
	Branch      string
	Tag         string
	CommitID    string
	PR          int
	PRs         []int
	TaskID      int64
	Timestamp   string
	ProductName string
	ServiceName string
	ImageName   string
	EnvName     string
}

// releaseCandidate 根据 TaskID 生成编译镜像Tag或者二进制包后缀
// TODO: max length of a tag is 128
func ReleaseCandidate(builds []*types.Repository, taskID int64, productName, serviceName, envName, imageName, deliveryType string) string {
	timeStamp := time.Now().Format("20060102150405")

	if imageName == "" {
		imageName = serviceName
	}
	if len(builds) == 0 {
		switch deliveryType {
		case config.TarResourceType:
			return fmt.Sprintf("%s-%s", serviceName, timeStamp)
		default:
			return fmt.Sprintf("%s:%s", imageName, timeStamp)
		}
	}

	first := builds[0]
	for index, build := range builds {
		if build.IsPrimary {
			first = builds[index]
		}
	}

	// 替换 Tag 和 Branch 中的非法字符为 "-", 避免 docker build 失败
	var (
		reg             = regexp.MustCompile(`[^\w.-]`)
		customImageRule *template.CustomRule
		customTarRule   *template.CustomRule
		commitID        = first.CommitID
	)

	if project, err := templaterepo.NewProductColl().Find(productName); err != nil {
		log.Errorf("find project err:%s", err)
	} else {
		customImageRule = project.CustomImageRule
		customTarRule = project.CustomTarRule
	}

	if len(commitID) > InterceptCommitID {
		commitID = commitID[0:InterceptCommitID]
	}

	candidate := &candidate{
		Branch:      string(reg.ReplaceAll([]byte(first.Branch), []byte("-"))),
		CommitID:    commitID,
		PR:          first.PR,
		PRs:         first.PRs,
		Tag:         string(reg.ReplaceAll([]byte(first.Tag), []byte("-"))),
		EnvName:     envName,
		Timestamp:   timeStamp,
		TaskID:      taskID,
		ProductName: productName,
		ServiceName: serviceName,
		ImageName:   imageName,
	}
	switch deliveryType {
	case config.TarResourceType:
		newTarRule := replaceVariable(customTarRule, candidate)
		if strings.Contains(newTarRule, ":") {
			return strings.Replace(newTarRule, ":", "-", -1)
		}
		return newTarRule
	default:
		return replaceVariable(customImageRule, candidate)
	}
}

// There are four situations in total
// 1.Execute workflow selection tag build
// 2.Execute workflow selection branch and pr build
// 3.Execute workflow selection branch pr build
// 4.Execute workflow selection branch build
func replaceVariable(customRule *template.CustomRule, candidate *candidate) string {
	var currentRule string
	if candidate.Tag != "" {
		if customRule == nil {
			return fmt.Sprintf("%s:%s-%s", candidate.ImageName, candidate.Timestamp, candidate.Tag)
		}
		currentRule = customRule.TagRule
	} else if candidate.Branch != "" && (candidate.PR != 0 || len(candidate.PRs) > 0) {
		if customRule == nil {
			return fmt.Sprintf("%s:%s-%d-%s-pr-%s", candidate.ImageName, candidate.Timestamp, candidate.TaskID, candidate.Branch, getCandidatePRsStr(candidate))
		}
		currentRule = customRule.PRAndBranchRule
	} else if candidate.Branch == "" && (candidate.PR != 0 || len(candidate.PRs) > 0) {
		if customRule == nil {
			return fmt.Sprintf("%s:%s-%d-pr-%s", candidate.ImageName, candidate.Timestamp, candidate.TaskID, getCandidatePRsStr(candidate))
		}
		currentRule = customRule.PRRule
	} else if candidate.Branch != "" && candidate.PR == 0 && len(candidate.PRs) == 0 {
		if customRule == nil {
			return fmt.Sprintf("%s:%s-%d-%s", candidate.ImageName, candidate.Timestamp, candidate.TaskID, candidate.Branch)
		}
		currentRule = customRule.BranchRule
	}

	currentRule = ReplaceRuleVariable(currentRule, &Variable{
		SERVICE:        candidate.ServiceName,
		IMAGE_NAME:     candidate.ImageName,
		TIMESTAMP:      candidate.Timestamp,
		TASK_ID:        strconv.FormatInt(candidate.TaskID, 10),
		REPO_COMMIT_ID: candidate.CommitID,
		PROJECT:        candidate.ProductName,
		ENV_NAME:       candidate.EnvName,
		REPO_TAG:       candidate.Tag,
		REPO_BRANCH:    candidate.Branch,
		REPO_PR:        getCandidatePRsStr(candidate),
	})
	return currentRule
}

func getCandidatePRsStr(candidate *candidate) string {
	prStrs := []string{}
	// pr was deprecated, so we use prs first
	if candidate.PR != 0 && len(candidate.PRs) == 0 {
		prStrs = append(prStrs, strconv.Itoa(candidate.PR))
	} else {
		for _, pr := range candidate.PRs {
			prStrs = append(prStrs, strconv.Itoa(pr))
		}
	}
	return strings.Join(prStrs, "-")
}

func ReplaceRuleVariable(rule string, replaceValue *Variable) string {
	template, err := templ.New("replaceRuleVariable").Parse(rule)
	if err != nil {
		log.Errorf("replaceRuleVariable Parse err:%s", err)
		return rule
	}
	var replaceRuleVariable = templ.Must(template, err)
	payload := bytes.NewBufferString("")
	err = replaceRuleVariable.Execute(payload, replaceValue)
	if err != nil {
		log.Errorf("replaceRuleVariable Execute err:%s", err)
		return rule
	}

	return payload.String()
}

func ListServicesInEnv(envName, productName string, newSvcKVsMap map[string][]*commonmodels.ServiceKeyVal, log *zap.SugaredLogger) (*EnvServices, error) {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	env, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return nil, e.ErrGetService.AddErr(fmt.Errorf("failed to find env %s:%s", productName, envName))
	}

	latestSvcs, err := repository.ListMaxRevisionsServices(productName, env.Production)
	if err != nil {
		return nil, e.ErrGetService.AddErr(errors.Wrapf(err, "failed to find latest services for env %s:%s", productName, envName))
	}

	return BuildServiceInfoInEnv(env, latestSvcs, newSvcKVsMap, log)
}

// @fixme newSvcKVsMap is old struct kv map, which are the kv are from deploy job config
// may need to be removed, or use new kv struct
// helm values need to be refactored
func BuildServiceInfoInEnv(productInfo *commonmodels.Product, templateSvcs []*commonmodels.Service, newSvcKVsMap map[string][]*commonmodels.ServiceKeyVal, log *zap.SugaredLogger) (*EnvServices, error) {
	productName, envName := productInfo.ProductName, productInfo.EnvName
	ret := &EnvServices{
		ProductName: productName,
		EnvName:     envName,
		Services:    make([]*EnvService, 0),
	}

	project, err := templaterepo.NewProductColl().Find(productInfo.ProductName)
	if err != nil {
		return nil, e.ErrGetService.AddDesc(fmt.Sprintf("failed to find project %s, err: %v", productInfo.ProductName, err))
	}

	productTemplateSvcs, err := commonutil.GetProductUsedTemplateSvcs(productInfo)
	if err != nil {
		return nil, e.ErrGetService.AddErr(errors.Wrapf(err, "failed to find product template services for env %s:%s", productName, envName))
	}
	productTemplateSvcMap := make(map[string]*commonmodels.Service)
	for _, svc := range productTemplateSvcs {
		productTemplateSvcMap[svc.ServiceName] = svc
	}

	svcModulesMap := make(map[string]map[string]*commonmodels.Container)
	templateSvcMap := make(map[string]*commonmodels.Service)
	for _, svc := range templateSvcs {
		templateSvcMap[svc.ServiceName] = svc

		svcModulesMap[svc.ServiceName] = make(map[string]*commonmodels.Container)
		for _, container := range svc.Containers {
			// templateSvcs is default
			if _, ok := svcModulesMap[svc.ServiceName]; !ok {
				svcModulesMap[svc.ServiceName] = make(map[string]*commonmodels.Container)
			}
			svcModulesMap[svc.ServiceName][container.Name] = container
		}
	}

	for _, svc := range productInfo.GetServiceMap() {
		// prodcutInfo override default
		if _, ok := svcModulesMap[svc.ServiceName]; !ok {
			svcModulesMap[svc.ServiceName] = make(map[string]*commonmodels.Container)
		}

		for _, container := range svc.Containers {
			svcModulesMap[svc.ServiceName][container.Name] = container
		}
	}

	getSvcModules := func(svcName string) []*commonmodels.Container {
		ret := make([]*commonmodels.Container, 0)
		if modulesMap, ok := svcModulesMap[svcName]; ok {
			for _, module := range modulesMap {
				module.ImageName = commonutil.ExtractImageName(module.Image)
				ret = append(ret, module)
			}
		}
		return ret
	}

	svcUpdatable := func(svcName string, revision int64) bool {
		if !commonutil.ServiceDeployed(svcName, productInfo.ServiceDeployStrategy) {
			return true
		}
		if svc, ok := templateSvcMap[svcName]; ok {
			return svc.Revision != revision
		}
		return false
	}

	variables := func(prodSvc *commonmodels.ProductService) (string, []*commontypes.RenderVariableKV, string, []*commontypes.RenderVariableKV, error) {
		svcName := prodSvc.ServiceName
		svcRender := prodSvc.GetServiceRender()

		getVarsAndKVs := func(svcName string, updateSvcRevision bool) (string, []*commontypes.RenderVariableKV, error) {
			var tmplSvc *commonmodels.Service
			if updateSvcRevision {
				tmplSvc = templateSvcMap[svcName]
			} else {
				tmplSvc = productTemplateSvcMap[svcName]
			}
			if tmplSvc == nil {
				log.Errorf("failed to find service %s in template service, updateRevision: %v", svcName, updateSvcRevision)
				tmplSvc = &commonmodels.Service{
					ServiceName: svcName,
				}
			}

			if newSvcKVsMap[svcName] == nil {
				// config phase
				return commontypes.MergeRenderAndServiceTemplateVariableKVs(svcRender.OverrideYaml.RenderVariableKVs, tmplSvc.ServiceVariableKVs)
			} else {
				// "exec" phase
				return commontypes.MergeRenderAndServiceTemplateVariableKVs(svcRender.OverrideYaml.RenderVariableKVs, tmplSvc.ServiceVariableKVs)
			}
		}

		svcRenderYaml, kvs, err := getVarsAndKVs(svcName, false)
		if err != nil {
			return "", nil, "", nil, fmt.Errorf("failed to get variable and kvs for service %s, updateSvcRevision %v: %w", svcName, false, err)
		}
		latestSvcRenderYaml, latestKvs, err := getVarsAndKVs(svcName, true)
		if err != nil {
			return "", nil, "", nil, fmt.Errorf("failed to get variable and kvs for service %s, updateSvcRevision %v: %w", svcName, true, err)
		}

		return svcRenderYaml, kvs, latestSvcRenderYaml, latestKvs, nil
	}

	// get all service values info

	svcList := sets.NewString()
	deployType := project.ProductFeature.DeployType
	for serviceName, productSvc := range productInfo.GetServiceMap() {
		svcList.Insert(serviceName)
		svc := &EnvService{
			ServiceName:    serviceName,
			ServiceModules: getSvcModules(serviceName),
			Updatable:      svcUpdatable(serviceName, productSvc.Revision),
			Deployed:       true,
		}

		if deployType == setting.K8SDeployType {
			svc.VariableYaml, svc.VariableKVs, svc.LatestVariableYaml, svc.LatestVariableKVs, err = variables(productSvc)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get variables for service %s", serviceName)
			}
		} else if deployType == setting.HelmDeployType {
			svc.VariableYaml = productInfo.GetSvcRender(serviceName).OverrideYaml.YamlContent
			svc.OverrideKVs = productInfo.GetSvcRender(serviceName).OverrideValues
		}

		ret.Services = append(ret.Services, svc)
	}

	// exist in template but not in product
	for _, templateSvc := range templateSvcs {
		if svcList.Has(templateSvc.ServiceName) {
			continue
		}
		svc := &EnvService{
			ServiceName:    templateSvc.ServiceName,
			ServiceModules: getSvcModules(templateSvc.ServiceName),
			Updatable:      true,
			Deployed:       false,
		}

		if deployType == setting.K8SDeployType {
			svc.VariableYaml = templateSvc.VariableYaml
			svc.VariableKVs = commontypes.ServiceToRenderVariableKVs(templateSvc.ServiceVariableKVs)
			svc.LatestVariableYaml = svc.VariableYaml
			svc.LatestVariableKVs = svc.VariableKVs
		} else if deployType == setting.HelmDeployType {
			svc.VariableYaml = templateSvc.VariableYaml
		}

		ret.Services = append(ret.Services, svc)
	}

	return ret, nil
}

func prefixOverride(base, override string, kvsRange []*commonmodels.ServiceKeyVal) (string, error) {
	keySet := commonutil.KVs2Set(kvsRange)
	baseKVs, err := kube.GeneKVFromYaml(base)
	if err != nil {
		return "", fmt.Errorf("failed to gene base kvs, err %s", err)
	}
	baseKVMap := map[string]*commonmodels.VariableKV{}
	for _, kv := range baseKVs {
		baseKVMap[kv.Key] = kv
	}

	overrideKVs, err := kube.GeneKVFromYaml(override)
	if err != nil {
		return "", fmt.Errorf("failed to gene override kvs, err %s", err)
	}
	overrideKVMap := map[string]*commonmodels.VariableKV{}
	for _, kv := range overrideKVs {
		overrideKVMap[kv.Key] = kv
	}

	newKVMap := map[string]*commonmodels.VariableKV{}

	// set keys
	// if key in set, get from base
	for k, kv := range baseKVMap {
		if commonutil.FilterKV(kv, keySet) {
			newKVMap[k] = kv
		}
	}
	// if key not in set, get from override
	for k, kv := range overrideKVMap {
		if !commonutil.FilterKV(kv, keySet) {
			newKVMap[k] = kv
		}
	}

	// set values
	for k, _ := range newKVMap {
		if overrideKV, ok := overrideKVMap[k]; ok {
			// find values in override
			newKVMap[k] = overrideKV
		} else if baseKV, ok := baseKVMap[k]; ok {
			// don't find values in override
			// but find values in base
			newKVMap[k] = baseKV
		}
	}

	retKVs := []*commonmodels.VariableKV{}
	for _, kv := range newKVMap {
		retKVs = append(retKVs, kv)
	}

	merged, err := kube.GenerateYamlFromKV(retKVs)
	if err != nil {
		return "", fmt.Errorf("failed to generate yaml from ret kvs, err %s", err)
	}

	return merged, nil
}

type ConvertVaraibleKVAndYamlActionType string

const (
	ConvertVaraibleKVAndYamlActionTypeToKV   ConvertVaraibleKVAndYamlActionType = "toKV"
	ConvertVaraibleKVAndYamlActionTypeToYaml ConvertVaraibleKVAndYamlActionType = "toYaml"
)

type ConvertVaraibleKVAndYamlArgs struct {
	KVs    []*commontypes.ServiceVariableKV   `json:"kvs" binding:"required"`
	Yaml   string                             `json:"yaml"`
	Action ConvertVaraibleKVAndYamlActionType `json:"action" binding:"required"`
}

func ConvertVaraibleKVAndYaml(args *ConvertVaraibleKVAndYamlArgs) (*ConvertVaraibleKVAndYamlArgs, error) {
	var err error
	if args.Action == ConvertVaraibleKVAndYamlActionTypeToYaml {
		args.Yaml, err = commontypes.ServiceVariableKVToYaml(args.KVs, true)
		if err != nil {
			return nil, fmt.Errorf("failed to convert service variable kv to yaml, err: %w", err)
		}
	} else if args.Action == ConvertVaraibleKVAndYamlActionTypeToKV {
		args.KVs, err = commontypes.YamlToServiceVariableKV(args.Yaml, args.KVs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert yaml to service variable kv, err: %w", err)
		}
	} else {
		return nil, fmt.Errorf("invalid action %s", args.Action)
	}

	return args, nil
}

// SvcResp struct 产品-服务详情页面Response
type SvcResp struct {
	ServiceName string                       `json:"service_name"`
	Scales      []*internalresource.Workload `json:"scales"`
	Ingress     []*internalresource.Ingress  `json:"ingress"`
	Services    []*internalresource.Service  `json:"service_endpoints"`
	CronJobs    []*internalresource.CronJob  `json:"cron_jobs"`
	Namespace   string                       `json:"namespace"`
	EnvName     string                       `json:"env_name"`
	ProductName string                       `json:"product_name"`
	GroupName   string                       `json:"group_name"`
	Workloads   []*Workload                  `json:"-"`
}

func GetServiceImpl(serviceName string, serviceTmpl *commonmodels.Service, workLoadType string, env *commonmodels.Product, clientset *kubernetes.Clientset, inf informers.SharedInformerFactory, log *zap.SugaredLogger) (ret *SvcResp, err error) {
	envName, productName := env.EnvName, env.ProductName
	ret = &SvcResp{
		ServiceName: serviceName,
		EnvName:     envName,
		ProductName: productName,
		Services:    make([]*internalresource.Service, 0),
		Ingress:     make([]*internalresource.Ingress, 0),
		Scales:      make([]*internalresource.Workload, 0),
		CronJobs:    make([]*internalresource.CronJob, 0),
	}
	err = nil

	namespace := env.Namespace
	switch env.Source {
	case setting.SourceFromHelm:
		k8sServices, _ := getter.ListServicesWithCache(nil, inf)
		switch workLoadType {
		case setting.StatefulSet:
			statefulSet, err := getter.GetStatefulSetByNameWWithCache(serviceName, namespace, inf)
			if err != nil {
				return nil, e.ErrGetService.AddDesc(fmt.Sprintf("service %s not found for statuefulSet, err: %v", serviceName, err))
			}
			scale := getStatefulSetWorkloadResource(statefulSet, inf, log)
			ret.Scales = append(ret.Scales, scale)
			ret.Workloads = append(ret.Workloads, toStsWorkload(statefulSet))
			podLabels := labels.Set(statefulSet.Spec.Template.GetLabels())
			for _, svc := range k8sServices {
				if labels.SelectorFromValidatedSet(svc.Spec.Selector).Matches(podLabels) {
					ret.Services = append(ret.Services, wrapper.Service(svc).Resource())
					break
				}
			}
		case setting.Deployment:
			deploy, err := getter.GetDeploymentByNameWithCache(serviceName, namespace, inf)
			if err != nil {
				return nil, e.ErrGetService.AddDesc(fmt.Sprintf("service %s not found for deployment, err: %s", serviceName, err.Error()))
			}
			scale := GetDeploymentWorkloadResource(deploy, inf, log)
			ret.Workloads = append(ret.Workloads, ToDeploymentWorkload(deploy))
			ret.Scales = append(ret.Scales, scale)
			podLabels := labels.Set(deploy.Spec.Template.GetLabels())
			for _, svc := range k8sServices {
				if labels.SelectorFromValidatedSet(svc.Spec.Selector).Matches(podLabels) {
					ret.Services = append(ret.Services, wrapper.Service(svc).Resource())
					break
				}
			}
		case setting.CronJob:
			version, err := clientset.Discovery().ServerVersion()
			if err != nil {
				log.Warnf("Failed to determine server version, error is: %s", err)
				return ret, nil
			}
			cj, cjBeta, err := getter.GetCronJobByNameWithCache(serviceName, namespace, inf, kubeclient.VersionLessThan121(version))
			if err != nil {
				return ret, e.ErrGetService.AddDesc(fmt.Sprintf("service %s not found for cronjob, err: %s", serviceName, err.Error()))
			}
			ret.CronJobs = append(ret.CronJobs, getCronJobWorkLoadResource(cj, cjBeta, inf, log))
		default:
			return nil, e.ErrGetService.AddDesc(fmt.Sprintf("service %s not found, unknow type", serviceName))
		}
	default:
		// k8s + host
		service := env.GetServiceMap()[serviceName]
		if service == nil {
			return nil, e.ErrGetService.AddDesc(fmt.Sprintf("failed to find service in environment: %s", envName))
		}

		parsedYaml, err := kube.RenderServiceYaml(serviceTmpl.Yaml, productName, serviceTmpl.ServiceName, service.GetServiceRender())
		if err != nil {
			log.Errorf("failed to render service yaml, err: %s", err)
			return nil, err
		}
		// 渲染系统变量键值
		parsedYaml = kube.ParseSysKeys(namespace, envName, productName, service.ServiceName, parsedYaml)

		manifests := releaseutil.SplitManifests(parsedYaml)
		for _, item := range manifests {
			u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
			if err != nil {
				log.Warnf("Failed to decode yaml to Unstructured, err: %s", err)
				continue
			}

			switch u.GetKind() {
			case setting.Deployment:
				d, err := getter.GetDeploymentByNameWithCache(u.GetName(), namespace, inf)
				if err != nil {
					continue
				}

				ret.Scales = append(ret.Scales, GetDeploymentWorkloadResource(d, inf, log))
				ret.Workloads = append(ret.Workloads, ToDeploymentWorkload(d))
			case setting.StatefulSet:
				sts, err := getter.GetStatefulSetByNameWWithCache(u.GetName(), namespace, inf)
				if err != nil {
					continue
				}

				ret.Scales = append(ret.Scales, getStatefulSetWorkloadResource(sts, inf, log))
				ret.Workloads = append(ret.Workloads, toStsWorkload(sts))
			case setting.CronJob:
				version, err := clientset.Discovery().ServerVersion()
				if err != nil {
					log.Warnf("Failed to determine server version, error is: %s", err)
					continue
				}
				cj, cjBeta, err := getter.GetCronJobByNameWithCache(u.GetName(), namespace, inf, kubeclient.VersionLessThan121(version))
				if err != nil {
					continue
				}
				ret.CronJobs = append(ret.CronJobs, getCronJobWorkLoadResource(cj, cjBeta, inf, log))
			case setting.Ingress:
				version, err := clientset.Discovery().ServerVersion()
				if err != nil {
					log.Warnf("Failed to determine server version, error is: %s", err)
					continue
				}
				if kubeclient.VersionLessThan122(version) {
					ing, found, err := getter.GetExtensionsV1Beta1Ingress(namespace, u.GetName(), inf)
					if err != nil || !found {
						//log.Warnf("no ingress %s found in %s:%s %v", u.GetName(), service.ServiceName, namespace, err)
						continue
					}

					ret.Ingress = append(ret.Ingress, wrapper.Ingress(ing).Resource())
				} else {
					ing, err := getter.GetNetworkingV1Ingress(namespace, u.GetName(), inf)
					if err != nil {
						//log.Warnf("no ingress %s found in %s:%s %v", u.GetName(), service.ServiceName, namespace, err)
						continue
					}

					ingress := &internalresource.Ingress{
						Name:     ing.Name,
						Labels:   ing.Labels,
						HostInfo: wrapper.GetIngressHostInfo(ing),
						IPs:      []string{},
						Age:      util.Age(ing.CreationTimestamp.Unix()),
					}

					ret.Ingress = append(ret.Ingress, ingress)
				}

			case setting.Service:
				svc, err := getter.GetServiceByNameFromCache(u.GetName(), namespace, inf)
				if err != nil {
					//log.Warnf("no svc %s found in %s:%s %v", u.GetName(), service.ServiceName, namespace, err)
					continue
				}

				ret.Services = append(ret.Services, wrapper.Service(svc).Resource())
			}
		}
	}
	return
}

func GetDeploymentWorkloadResource(d *appsv1.Deployment, informer informers.SharedInformerFactory, log *zap.SugaredLogger) *internalresource.Workload {
	pods, err := getter.ListPodsWithCache(labels.SelectorFromValidatedSet(d.Spec.Selector.MatchLabels), informer)
	if err != nil {
		log.Warnf("Failed to get pods, err: %s", err)
	}

	return wrapper.Deployment(d).WorkloadResource(pods)
}

func getStatefulSetWorkloadResource(sts *appsv1.StatefulSet, informer informers.SharedInformerFactory, log *zap.SugaredLogger) *internalresource.Workload {
	pods, err := getter.ListPodsWithCache(labels.SelectorFromValidatedSet(sts.Spec.Selector.MatchLabels), informer)
	if err != nil {
		log.Warnf("Failed to get pods, err: %s", err)
	}

	return wrapper.StatefulSet(sts).WorkloadResource(pods)
}

func getCronJobWorkLoadResource(cornJob *batchv1.CronJob, cronJobBeta *v1beta1.CronJob, informer informers.SharedInformerFactory, log *zap.SugaredLogger) *internalresource.CronJob {
	cronJobName := wrapper.CronJob(cornJob, cronJobBeta).Name
	jobs, err := informer.Batch().V1().Jobs().Lister().List(labels.NewSelector())
	if err != nil {
		log.Errorf("failed to list jobs, err: %s", err)
		return nil
	}
	// find jobs created by particular cronjob
	matchJobs := make([]*batchv1.Job, 0)
	for _, job := range jobs {
		for _, owner := range job.OwnerReferences {
			if owner.Name == cronJobName && owner.Kind == setting.CronJob {
				matchJobs = append(matchJobs, job)
				break
			}
		}
	}

	pods := make([]*corev1.Pod, 0)
	for _, job := range matchJobs {
		cronGeneratedPods, err := getter.ListPodsWithCache(labels.SelectorFromValidatedSet(job.Spec.Selector.MatchLabels), informer)
		if err != nil {
			log.Errorf("failed to find related pods for cronjob: %s, err: %s", cronJobName, err)
			continue
		}
		pods = append(pods, cronGeneratedPods...)
	}

	return wrapper.CronJob(cornJob, cronJobBeta).CronJobResource(pods)
}

func ToDeploymentWorkload(v *appsv1.Deployment) *Workload {
	workload := &Workload{
		Name:       v.Name,
		Spec:       v.Spec.Template,
		Selector:   v.Spec.Selector,
		Type:       setting.Deployment,
		Images:     wrapper.Deployment(v).ImageInfos(),
		Containers: wrapper.Deployment(v).GetContainers(),
		Ready:      wrapper.Deployment(v).Ready(),
		Annotation: v.Annotations,
	}
	return workload
}

func toStsWorkload(v *appsv1.StatefulSet) *Workload {
	workload := &Workload{
		Name:       v.Name,
		Spec:       v.Spec.Template,
		Selector:   v.Spec.Selector,
		Type:       setting.StatefulSet,
		Images:     wrapper.StatefulSet(v).ImageInfos(),
		Containers: wrapper.StatefulSet(v).GetContainers(),
		Ready:      wrapper.StatefulSet(v).Ready(),
		Annotation: v.Annotations,
	}
	return workload
}
