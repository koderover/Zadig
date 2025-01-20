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

package models

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type Product struct {
	ID             primitive.ObjectID              `bson:"_id,omitempty"             json:"id,omitempty"`
	ProductName    string                          `bson:"product_name"              json:"product_name"`
	CreateTime     int64                           `bson:"create_time"               json:"create_time"`
	UpdateTime     int64                           `bson:"update_time"               json:"update_time"`
	Namespace      string                          `bson:"namespace,omitempty"       json:"namespace,omitempty"`
	Status         string                          `bson:"status"                    json:"status"`
	Revision       int64                           `bson:"revision"                  json:"revision"`
	Enabled        bool                            `bson:"enabled"                   json:"enabled"`
	EnvName        string                          `bson:"env_name"                  json:"env_name"`
	BaseEnvName    string                          `bson:"-"                         json:"base_env_name"`
	UpdateBy       string                          `bson:"update_by"                 json:"update_by"`
	Visibility     string                          `bson:"-"                         json:"visibility"`
	Services       [][]*ProductService             `bson:"services"                  json:"services"`
	Render         *RenderInfo                     `bson:"-"                         json:"render"` // Deprecated in 1.19.0, will be removed in 1.20.0
	Error          string                          `bson:"error"                     json:"error"`
	ServiceRenders []*templatemodels.ServiceRender `bson:"-"                         json:"chart_infos,omitempty"`
	IsPublic       bool                            `bson:"is_public"                 json:"isPublic"`
	RoleIDs        []int64                         `bson:"role_ids"                  json:"roleIds"`
	ClusterID      string                          `bson:"cluster_id,omitempty"      json:"cluster_id,omitempty"`
	RecycleDay     int                             `bson:"recycle_day"               json:"recycle_day"`
	Source         string                          `bson:"source"                    json:"source"`
	IsOpenSource   bool                            `bson:"is_opensource"             json:"is_opensource"`
	RegistryID     string                          `bson:"registry_id"               json:"registry_id"`
	BaseName       string                          `bson:"base_name"                 json:"base_name"`
	// IsExisted is true if this environment is created from an existing one
	IsExisted bool `bson:"is_existed"                json:"is_existed"`
	// TODO: Deprecated: temp flag
	IsForkedProduct bool `bson:"-" json:"-"`

	// New Since v1.11.0.
	ShareEnv ProductShareEnv `bson:"share_env" json:"share_env"`

	// New Since v1.13.0.
	EnvConfigs []*CreateUpdateCommonEnvCfgArgs `bson:"-"   json:"env_configs,omitempty"`

	// New Since v1.16.0, used to determine whether to install resources
	ServiceDeployStrategy map[string]string `bson:"service_deploy_strategy" json:"service_deploy_strategy"`

	// New Since v.1.18.0, env configs
	AnalysisConfig      *AnalysisConfig       `bson:"analysis_config"      json:"analysis_config"`
	NotificationConfigs []*NotificationConfig `bson:"notification_configs" json:"notification_configs"`

	// New Since v1.19.0, env sleep configs
	PreSleepStatus map[string]int `bson:"pre_sleep_status" json:"pre_sleep_status"`

	// New Since v1.19.0, for env global variables
	// GlobalValues for helm projects
	DefaultValues string                     `bson:"default_values,omitempty"       json:"default_values,omitempty"`
	YamlData      *templatemodels.CustomYaml `bson:"yaml_data,omitempty"            json:"yaml_data,omitempty"`
	// GlobalValues for k8s projects
	GlobalVariables []*commontypes.GlobalVariableKV `bson:"global_variables,omitempty"     json:"global_variables,omitempty"`

	// New Since v2.1.0.
	IstioGrayscale IstioGrayscale `bson:"istio_grayscale" json:"istio_grayscale"`

	// For production environment
	Production bool   `json:"production" bson:"production"`
	Alias      string `json:"alias" bson:"alias"`
}

type NotificationEvent string

const (
	NotificationEventAnalyzerNoraml   NotificationEvent = "notification_event_analyzer_normal"
	NotificationEventAnalyzerAbnormal NotificationEvent = "notification_event_analyzer_abnormal"
)

type WebHookType string

const (
	WebHookTypeFeishu   WebHookType = "feishu"
	WebHookTypeDingding WebHookType = "dingding"
	WebHookTypeWeChat   WebHookType = "wechat"
)

type NotificationConfig struct {
	WebHookType WebHookType         `bson:"webhook_type" json:"webhook_type"`
	WebHookURL  string              `bson:"webhook_url"  json:"webhook_url"`
	Events      []NotificationEvent `bson:"events"       json:"events"`
}

type ResourceType string

const (
	ResourceTypePod           ResourceType = "Pod"
	ResourceTypeDeployment    ResourceType = "Deployment"
	ResourceTypeReplicaSet    ResourceType = "ReplicaSet"
	ResourceTypePVC           ResourceType = "PersistentVolumeClaim"
	ResourceTypeService       ResourceType = "Service"
	ResourceTypeIngress       ResourceType = "Ingress"
	ResourceTypeStatefulSet   ResourceType = "StatefulSet"
	ResourceTypeCronJob       ResourceType = "CronJob"
	ResourceTypeHPA           ResourceType = "HorizontalPodAutoScaler"
	ResourceTypePDB           ResourceType = "PodDisruptionBudget"
	ResourceTypeNetworkPolicy ResourceType = "NetworkPolicy"
)

type AnalysisConfig struct {
	ResourceTypes []ResourceType `bson:"resource_types" json:"resource_types"`
}

type CreateUpdateCommonEnvCfgArgs struct {
	EnvName              string                        `json:"env_name"`
	ProductName          string                        `json:"product_name"`
	ServiceName          string                        `json:"service_name"`
	Name                 string                        `json:"name"`
	YamlData             string                        `json:"yaml_data"`
	RestartAssociatedSvc bool                          `json:"restart_associated_svc"`
	CommonEnvCfgType     config.CommonEnvCfgType       `json:"common_env_cfg_type"`
	Services             []string                      `json:"services"`
	GitRepoConfig        *templatemodels.GitRepoConfig `json:"git_repo_config"`
	SourceDetail         *CreateFromRepo               `json:"-"`
	AutoSync             bool                          `json:"auto_sync"`
	LatestEnvResource    *EnvResource                  `json:"-"`
	Production           bool                          `json:"production"`
}

type RenderInfo struct {
	Name        string `bson:"name"                     json:"name"`
	Revision    int64  `bson:"revision"                 json:"revision"`
	ProductTmpl string `bson:"product_tmpl"             json:"product_tmpl"`
	Description string `bson:"description"              json:"description"`
}

type ServiceResource struct {
	schema.GroupVersionKind
	Name string
}

func (r *ServiceResource) String() string {
	return fmt.Sprintf("%s/%s", r.GroupVersionKind.Kind, r.Name)
}

type ProductService struct {
	ServiceName string                        `bson:"service_name"               json:"service_name"`
	ReleaseName string                        `bson:"release_name"               json:"release_name"`
	ProductName string                        `bson:"product_name"               json:"product_name"`
	Type        string                        `bson:"type"                       json:"type"`
	Revision    int64                         `bson:"revision"                   json:"revision"`
	Containers  []*Container                  `bson:"containers"                 json:"containers,omitempty"`
	Error       string                        `bson:"error,omitempty"            json:"error,omitempty"`
	Resources   []*ServiceResource            `bson:"resources,omitempty"        json:"resources,omitempty"`
	UpdateTime  int64                         `bson:"update_time"                json:"update_time"`
	Render      *templatemodels.ServiceRender `bson:"render"                     json:"render,omitempty"` // New since 1.9.0 used to replace service renders in render_set

	EnvConfigs     []*EnvConfig                    `bson:"-"                          json:"env_configs,omitempty"`
	RenderedYaml   string                          `bson:"rendered_yaml,omitempty"    json:"rendered_yaml,omitempty"`
	VariableYaml   string                          `bson:"-"                          json:"variable_yaml,omitempty"`
	VariableKVs    []*commontypes.RenderVariableKV `bson:"-"                          json:"variable_kvs,omitempty"`
	ValuesYaml     string                          `bson:"-"                          json:"values_yaml,omitempty"`
	Updatable      bool                            `bson:"-"                          json:"updatable"`
	DeployStrategy string                          `bson:"-"                          json:"deploy_strategy"`
}

func (svc *ProductService) GetServiceType() config.ServiceType {
	if svc.Type == setting.K8SDeployType {
		return config.ServiceTypeK8S
	} else if svc.Type == setting.HelmDeployType {
		return config.ServiceTypeHelm
	} else if svc.Type == setting.HelmChartDeployType {
		return config.ServiceTypeHelmChart
	} else {
		return config.ServiceTypeVM
	}
}

func (svc *ProductService) FromZadig() bool {
	return svc.Type != setting.HelmChartDeployType
}

func (svc *ProductService) GetServiceRender() *templatemodels.ServiceRender {
	if svc.Render == nil {
		svc.Render = &templatemodels.ServiceRender{
			ServiceName:  svc.ServiceName,
			OverrideYaml: &templatemodels.CustomYaml{},
		}
		if !svc.FromZadig() {
			svc.Render.IsHelmChartDeploy = true
		}
	}
	if svc.Render.OverrideYaml == nil {
		svc.Render.OverrideYaml = &templatemodels.CustomYaml{}
	}
	return svc.Render
}

func (svc *ProductService) GetContainerImageMap() map[string]string {
	resp := make(map[string]string)
	if svc != nil {
		if svc.Containers != nil {
			for _, container := range svc.Containers {
				resp[container.Name] = container.Image
			}
		}
	}

	return resp
}

type ServiceConfig struct {
	ConfigName string `bson:"config_name"           json:"config_name"`
	Revision   int64  `bson:"revision"              json:"revision"`
}

type ProductShareEnv struct {
	Enable  bool   `bson:"enable"   json:"enable"`
	IsBase  bool   `bson:"is_base"  json:"is_base"`
	BaseEnv string `bson:"base_env" json:"base_env"`
}

type IstioGrayscale struct {
	Enable             bool                     `bson:"enable"   json:"enable"`
	IsBase             bool                     `bson:"is_base"  json:"is_base"`
	BaseEnv            string                   `bson:"base_env" json:"base_env"`
	GrayscaleStrategy  GrayscaleStrategyType    `bson:"grayscale_strategy" json:"grayscale_strategy"`
	WeightConfigs      []IstioWeightConfig      `bson:"weight_configs" json:"weight_configs"`
	HeaderMatchConfigs []IstioHeaderMatchConfig `bson:"header_match_configs" json:"header_match_configs"`
}

type GrayscaleStrategyType string

var (
	GrayscaleStrategyWeight      GrayscaleStrategyType = "weight"
	GrayscaleStrategyHeaderMatch GrayscaleStrategyType = "header_match"
)

type IstioWeightConfig struct {
	Env    string `bson:"env"    json:"env"    yaml:"env"`
	Weight int32  `bson:"weight" json:"weight" yaml:"weight"`
}

type IstioHeaderMatchConfig struct {
	Env          string             `bson:"env"          json:"env"            yaml:"env"`
	HeaderMatchs []IstioHeaderMatch `bson:"header_match" json:"header_match"   yaml:"header_match"`
}

type IstioHeaderMatch struct {
	Key   string          `bson:"key"   json:"key"    yaml:"key"`
	Match StringMatchType `bson:"match" json:"match"  yaml:"match"`
	Value string          `bson:"value" json:"value"  yaml:"value"`
}

type StringMatchType string

var (
	StringMatchPrefix StringMatchType = "prefix"
	StringMatchExact  StringMatchType = "exact"
	StringMatchRegex  StringMatchType = "regex"
)

func (Product) TableName() string {
	return "product"
}

// GetNamespace returns the default name of namespace created by zadig
func (p *Product) GetDefaultNamespace() string {
	return p.ProductName + "-env-" + p.EnvName
}

func (p *Product) GetGroupServiceNames() [][]string {
	var resp [][]string
	for _, group := range p.Services {
		services := make([]string, 0, len(group))
		for _, service := range group {
			services = append(services, service.ServiceName)
		}
		resp = append(resp, services)
	}
	return resp
}

func (p *Product) GetAllSvcRenders() []*templatemodels.ServiceRender {
	ret := make([]*templatemodels.ServiceRender, 0)
	for _, svcGroup := range p.Services {
		for _, svc := range svcGroup {
			ret = append(ret, svc.GetServiceRender())
		}
	}
	return ret
}

func (p *Product) GetSvcRender(svcName string) *templatemodels.ServiceRender {
	for _, group := range p.Services {
		for _, svc := range group {
			if svc.ServiceName == svcName {
				return svc.GetServiceRender()
			}
		}
	}
	return &templatemodels.ServiceRender{
		ServiceName:  svcName,
		OverrideYaml: &templatemodels.CustomYaml{},
	}
}

func (p *Product) GetSvcList() []*ProductService {
	ret := make([]*ProductService, 0)
	for _, svcGroup := range p.Services {
		ret = append(ret, svcGroup...)
	}
	return ret
}

func (p *Product) GetServiceMap() map[string]*ProductService {
	ret := make(map[string]*ProductService)
	for _, group := range p.Services {
		for _, svc := range group {
			if svc.FromZadig() {
				ret[svc.ServiceName] = svc
			}
		}
	}

	return ret
}

func (p *Product) GetChartServiceMap() map[string]*ProductService {
	ret := make(map[string]*ProductService)
	for _, group := range p.Services {
		for _, svc := range group {
			if !svc.FromZadig() {
				ret[svc.ReleaseName] = svc
			}
		}
	}

	return ret
}

func (p *Product) LintServices() {
	svcMap := make(map[string]*ProductService)
	chartSvcMap := make(map[string]*ProductService)

	shouldKeepService := func(svcMap map[string]*ProductService, svc *ProductService, key string) bool {
		if prevSvc, ok := svcMap[key]; ok {
			if prevSvc.Revision < svc.Revision {
				svcMap[key] = svc
				return true
			}
			log.Warnf("%s/%s's service %s has older revision %d, drop it", p.ProductName, p.EnvName, key, svc.Revision)
			return false
		}
		svcMap[key] = svc
		return true
	}

	for i, group := range p.Services {
		newGroup := []*ProductService{}
		for _, svc := range group {
			if svc.FromZadig() {
				if shouldKeepService(svcMap, svc, svc.ServiceName) {
					newGroup = append(newGroup, svc)
				}
			} else {
				if shouldKeepService(chartSvcMap, svc, svc.ReleaseName) {
					newGroup = append(newGroup, svc)
				}
			}
		}
		p.Services[i] = newGroup
	}
}

func (p *Product) GetProductSvcNames() []string {
	ret := make([]string, 0)
	for _, group := range p.Services {
		for _, svc := range group {
			ret = append(ret, svc.ServiceName)
		}
	}
	return ret
}

// EnsureRenderInfo For some old data, the render data mayby nil
func (p *Product) EnsureRenderInfo() {
	if p.Render != nil {
		return
	}
	p.Render = &RenderInfo{ProductTmpl: p.ProductName, Name: p.Namespace}
}

func (p *Product) IsSleeping() bool {
	return p.Status == setting.ProductStatusSleeping
}

func (p *Product) GetChartRenderMap() map[string]*templatemodels.ServiceRender {
	serviceRenderMap := make(map[string]*templatemodels.ServiceRender)
	for _, render := range p.GetAllSvcRenders() {
		if !render.IsHelmChartDeploy {
			serviceRenderMap[render.ServiceName] = render
		}
	}
	return serviceRenderMap
}

func (p *Product) GetChartDeployRenderMap() map[string]*templatemodels.ServiceRender {
	serviceRenderMap := make(map[string]*templatemodels.ServiceRender)
	for _, render := range p.GetAllSvcRenders() {
		if render.IsHelmChartDeploy {
			serviceRenderMap[render.ReleaseName] = render
		}
	}
	return serviceRenderMap
}

func (p *Product) String() string {
	return fmt.Sprintf("%s/%s/%v", p.ProductName, p.EnvName, p.Production)
}
