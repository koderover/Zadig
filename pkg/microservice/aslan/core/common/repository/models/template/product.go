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

package template

import (
	"strings"

	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type Product struct {
	GroupName                    string                `bson:"-"                         json:"group_name"`
	ProjectName                  string                `bson:"project_name"              json:"project_name"`
	ProjectNamePinyin            string                `bson:"project_name_pinyin"       json:"project_name_pinyin"`
	ProjectNamePinyinFirstLetter string                `bson:"project_name_pinyin_first_letter"       json:"project_name_pinyin_first_letter"`
	ProductName                  string                `bson:"product_name"              json:"product_name"`
	Revision                     int64                 `bson:"revision"                  json:"revision"`
	CreateTime                   int64                 `bson:"create_time"               json:"create_time"`
	UpdateTime                   int64                 `bson:"update_time"               json:"update_time"`
	UpdateBy                     string                `bson:"update_by"                 json:"update_by"`
	Enabled                      bool                  `bson:"enabled"                   json:"enabled"`
	Visibility                   string                `bson:"visibility"                json:"visibility"`
	AutoDeploy                   *AutoDeployPolicy     `bson:"auto_deploy"               json:"auto_deploy"`
	Timeout                      int                   `bson:"timeout,omitempty"         json:"timeout,omitempty"`
	Services                     [][]string            `bson:"services"                  json:"services"`
	ProductionServices           [][]string            `bson:"production_services"       json:"production_services"`
	SharedServices               []*ServiceInfo        `bson:"shared_services,omitempty" json:"shared_services,omitempty"` //Deprecated since 1.17
	Vars                         []*RenderKV           `bson:"-"                         json:"vars"`                      //Deprecated since 1.17
	EnvVars                      []*EnvRenderKV        `bson:"-"                         json:"env_vars,omitempty"`
	ChartInfos                   []*ServiceRender      `bson:"-"                         json:"chart_infos,omitempty"`
	Description                  string                `bson:"description,omitempty"     json:"desc,omitempty"`
	ProductFeature               *ProductFeature       `bson:"product_feature,omitempty" json:"product_feature,omitempty"`
	ImageSearchingRules          []*ImageSearchingRule `bson:"image_searching_rules,omitempty" json:"image_searching_rules,omitempty"`
	// onboarding状态，0表示onboarding完成，1、2、3、4代表当前onboarding所在的步骤
	OnboardingStatus int `bson:"onboarding_status"         json:"onboarding_status"`
	// CI场景的onboarding流程创建的ci工作流id，用于前端跳转
	CiPipelineID               string                           `bson:"-"                                   json:"ci_pipeline_id"`
	Role                       string                           `bson:"-"                                   json:"role,omitempty"`
	PermissionUUIDs            []string                         `bson:"-"                                   json:"permissionUUIDs"`
	TotalServiceNum            int                              `bson:"-"                                   json:"total_service_num"`
	LatestServiceUpdateTime    int64                            `bson:"-"                                   json:"latest_service_update_time"`
	LatestServiceUpdateBy      string                           `bson:"-"                                   json:"latest_service_update_by"`
	TotalBuildNum              int                              `bson:"-"                                   json:"total_build_num"`
	LatestBuildUpdateTime      int64                            `bson:"-"                                   json:"latest_build_update_time"`
	LatestBuildUpdateBy        string                           `bson:"-"                                   json:"latest_build_update_by"`
	TotalTestNum               int                              `bson:"-"                                   json:"total_test_num"`
	LatestTestUpdateTime       int64                            `bson:"-"                                   json:"latest_test_update_time"`
	LatestTestUpdateBy         string                           `bson:"-"                                   json:"latest_test_update_by"`
	TotalEnvNum                int                              `bson:"-"                                   json:"total_env_num"`
	LatestEnvUpdateTime        int64                            `bson:"-"                                   json:"latest_env_update_time"`
	LatestEnvUpdateBy          string                           `bson:"-"                                   json:"latest_env_update_by"`
	TotalWorkflowNum           int                              `bson:"-"                                   json:"total_workflow_num"`
	LatestWorkflowUpdateTime   int64                            `bson:"-"                                   json:"latest_workflow_update_time"`
	LatestWorkflowUpdateBy     string                           `bson:"-"                                   json:"latest_workflow_update_by"`
	TotalEnvTemplateServiceNum int                              `bson:"-"                                   json:"total_env_template_service_num"`
	ClusterIDs                 []string                         `bson:"-"                                   json:"cluster_ids"`
	IsOpensource               bool                             `bson:"is_opensource"                       json:"is_opensource"`
	CustomImageRule            *CustomRule                      `bson:"custom_image_rule,omitempty"         json:"custom_image_rule,omitempty"`
	CustomTarRule              *CustomRule                      `bson:"custom_tar_rule,omitempty"           json:"custom_tar_rule,omitempty"`
	DeliveryVersionHook        *DeliveryVersionHook             `bson:"delivery_version_hook"               json:"delivery_version_hook"`
	GlobalVariables            []*commontypes.ServiceVariableKV `bson:"global_variables,omitempty"          json:"global_variables,omitempty"`                       // New since 1.18.0 used to store global variables for test services
	ProductionGlobalVariables  []*commontypes.ServiceVariableKV `bson:"production_global_variables,omitempty"          json:"production_global_variables,omitempty"` // New since 1.18.0 used to store global variables for production services
	Public                     bool                             `bson:"public,omitempty"                    json:"public"`
	// created after 1.8.0, used to create default project admins
	Admins []string `bson:"-" json:"admins"`
}

type ServiceInfo struct {
	Name  string `bson:"name"  json:"name"`
	Owner string `bson:"owner" json:"owner"`
}

type Team struct {
	ID   int    `bson:"id" json:"id"`
	Name string `bson:"name" json:"name"`
}

type RenderKV struct {
	Key      string   `bson:"key"               json:"key"`
	Value    string   `bson:"value"             json:"value"`
	Alias    string   `bson:"alias"             json:"alias"`
	State    string   `bson:"state"             json:"state"`
	Services []string `bson:"services"          json:"services"`
}

type EnvRenderKV struct {
	EnvName string      `json:"env_name"`
	Vars    []*RenderKV `json:"vars"`
}

type GitRepoConfig struct {
	CodehostID  int      `bson:"codehost_id,omitempty"  json:"codehost_id"`
	Owner       string   `bson:"owner,omitempty"        json:"owner"`
	Repo        string   `bson:"repo,omitempty"         json:"repo"`
	Branch      string   `bson:"branch,omitempty"       json:"branch"`
	Namespace   string   `bson:"namespace,omitempty"    json:"namespace"` // records the actual namespace of repo, used to generate correct project name
	ValuesPaths []string `bson:"values_paths,omitempty" json:"values_paths,omitempty"`
}

func (grc *GitRepoConfig) GetNamespace() string {
	if len(grc.Namespace) > 0 {
		return grc.Namespace
	}
	return grc.Owner
}

type CustomYaml struct {
	// helm:
	// - in service: user override's values in advance setting
	// - in env: user input's override yaml
	// k8s: variable yaml
	YamlContent       string                          `bson:"yaml_content"                      json:"yaml_content"`
	RenderVariableKVs []*commontypes.RenderVariableKV `bson:"render_variable_kvs"               json:"render_variable_kvs"`
	Source            string                          `bson:"source"                            json:"source"`
	AutoSync          bool                            `bson:"auto_sync"                         json:"auto_sync"`
	AutoSyncYaml      string                          `bson:"auto_sync_yaml"                    json:"auto_sync_yaml"`
	SourceDetail      interface{}                     `bson:"source_detail"                     json:"source_detail"`
	SourceID          string                          `bson:"source_id"                         json:"source_id"`
}

// ServiceRender used for helm product service ...
type ServiceRender struct {
	ServiceName       string `bson:"service_name,omitempty"    json:"service_name,omitempty"`
	ReleaseName       string `bson:"release_name,omitempty"    json:"release_name,omitempty"`
	IsHelmChartDeploy bool   `bson:"is_helm_chart_deploy,omitempty"    json:"is_helm_chart_deploy,omitempty"`

	// ---- for helm services begin ----
	ChartRepo      string `bson:"chart_repo,omitempty"   json:"chart_repo,omitempty"`
	ChartName      string `bson:"chart_name,omitempty"   json:"chart_name,omitempty"`
	ChartVersion   string `bson:"chart_version,omitempty"   json:"chart_version,omitempty"`
	// ValuesYaml     string `bson:"values_yaml,omitempty"     json:"values_yaml,omitempty"`       // full helm service values yaml, only record, not actually used in calculation
	OverrideValues string `bson:"override_values,omitempty"   json:"override_values,omitempty"` // used for helm services, json-encoded string of kv value
	// ---- for helm services end ----

	// OverrideYaml will be used in both helm and k8s projects
	OverrideYaml *CustomYaml `bson:"override_yaml,omitempty"   json:"override_yaml,omitempty"`
}

func (rc *ServiceRender) DeployedFromZadig() bool {
	return !rc.IsHelmChartDeploy
}

func (rc *ServiceRender) GetSafeVariable() string {
	if rc.OverrideYaml != nil {
		return rc.OverrideYaml.YamlContent
	}
	return ""
}

type ProductFeature struct {
	// 基础设施，kubernetes 或者 cloud_host
	BasicFacility string `bson:"basic_facility"            json:"basic_facility"`
	// 部署方式，basic_facility=kubernetes时填写，k8s 或者 helm
	DeployType string `bson:"deploy_type"                  json:"deploy_type"`
	// 创建环境方式,system/external(系统创建/外部环境)
	CreateEnvType string `bson:"create_env_type"           json:"create_env_type"`
	AppType       string `bson:"app_type"                  json:"app_type"`
}

func (p *ProductFeature) GetDeployType() string {
	if p == nil {
		log.Errorf("product feature is nil")
		return setting.K8SDeployType
	}
	var deployType string
	if p.CreateEnvType == setting.SourceFromExternal {
		deployType = setting.SourceFromExternal
	} else if p.BasicFacility == "cloud_host" {
		deployType = "cloud_host"
	} else {
		deployType = p.DeployType
	}
	return deployType
}

type ForkProject struct {
	EnvName      string           `json:"env_name"`
	WorkflowName string           `json:"workflow_name"`
	ValuesYamls  []*ServiceRender `json:"values_yamls"`
	ProductName  string           `json:"product_name"`
}

type ImageSearchingRule struct {
	Repo      string `bson:"repo,omitempty"`
	Namespace string `bson:"namespace,omitempty"`
	Image     string `bson:"image,omitempty"`
	Tag       string `bson:"tag,omitempty"`
	InUse     bool   `bson:"in_use,omitempty"`
	PresetId  int    `bson:"preset_id,omitempty"`
}

type CustomRule struct {
	PRRule          string `bson:"pr_rule,omitempty"             json:"pr_rule,omitempty"`
	BranchRule      string `bson:"branch_rule,omitempty"         json:"branch_rule,omitempty"`
	PRAndBranchRule string `bson:"pr_and_branch_rule,omitempty"  json:"pr_and_branch_rule,omitempty"`
	TagRule         string `bson:"tag_rule,omitempty"            json:"tag_rule,omitempty"`
	JenkinsRule     string `bson:"jenkins_rule,omitempty"        json:"jenkins_rule,omitempty"`
	CommitRule      string `bson:"commit_rule,omitempty"         json:"commit_rule,omitempty"`
}

type DeliveryVersionHook struct {
	Enable   bool   `bson:"enable"     json:"enable"`
	HookHost string `bson:"hook_host"  json:"hook_host"`
	Path     string `bson:"path"       json:"path"`
}

type AutoDeployPolicy struct {
	Enable bool `bson:"enable" json:"enable"`
}

func (Product) TableName() string {
	return "template_product"
}

func (p *Product) AllTestServiceInfos() []*ServiceInfo {
	var res []*ServiceInfo
	ss := p.AllTestServiceInfoMap()
	for _, s := range ss {
		res = append(res, s)
	}

	return res
}

// AllTestServiceInfoMap returns all services which are bound to this product, including the shared ones.
// note that p.Services contains all services names including the shared ones, so we need to override their owner.
func (p *Product) AllTestServiceInfoMap() map[string]*ServiceInfo {
	res := make(map[string]*ServiceInfo)
	for _, sg := range p.Services {
		for _, name := range sg {
			res[name] = &ServiceInfo{
				Name:  name,
				Owner: p.ProductName,
			}
		}
	}
	return res
}

func (p *Product) AllProductionServiceInfoMap() map[string]*ServiceInfo {
	res := make(map[string]*ServiceInfo)
	for _, sg := range p.ProductionServices {
		for _, name := range sg {
			res[name] = &ServiceInfo{
				Name:  name,
				Owner: p.ProductName,
			}
		}
	}
	return res
}

func (p *Product) AllServiceInfoMap(production bool) map[string]*ServiceInfo {
	if production {
		return p.AllProductionServiceInfoMap()
	}
	return p.AllTestServiceInfoMap()
}

func (p *Product) IsHelmProduct() bool {
	return p.ProductFeature != nil && p.ProductFeature.DeployType == setting.HelmDeployType && p.ProductFeature.BasicFacility == setting.BasicFacilityK8S
}

func (p *Product) IsK8sYamlProduct() bool {
	if p.ProductFeature == nil {
		return true
	}
	return p.ProductFeature != nil && p.ProductFeature.DeployType == setting.K8SDeployType && p.ProductFeature.BasicFacility == setting.BasicFacilityK8S && p.ProductFeature.CreateEnvType != setting.SourceFromExternal
}

func (p *Product) IsCVMProduct() bool {
	return p.ProductFeature != nil && p.ProductFeature.BasicFacility == setting.BasicFacilityCVM
}

func (p *Product) IsHostProduct() bool {
	return p.ProductFeature != nil && p.ProductFeature.BasicFacility == setting.BasicFacilityK8S && p.ProductFeature.CreateEnvType == setting.SourceFromExternal
}

func (r *RenderKV) SetAlias() {
	r.Alias = "{{." + r.Key + "}}"
}

func (r *RenderKV) SetKeys() {
	key := r.Alias
	key = strings.Replace(key, "{{", "", -1)
	key = strings.Replace(key, "}}", "", -1)
	key = strings.TrimSpace(key)
	if strings.HasPrefix(key, ".") && len(key) >= 1 {
		key = key[1:]
	}
	r.Key = key
}

func (r *RenderKV) RemoveDupServices() {
	dic := make(map[string]bool)
	var result []string // 存放结果
	for _, key := range r.Services {
		if !dic[key] {
			dic[key] = true
			result = append(result, key)
		}
	}
	r.Services = result
}

func (rc *ServiceRender) GetAutoSync() bool {
	if rc.OverrideYaml == nil {
		return false
	}
	return rc.OverrideYaml.AutoSync
}

func (rc *ServiceRender) GetAutoSyncYaml() string {
	if rc.OverrideYaml == nil {
		return ""
	}
	return rc.OverrideYaml.AutoSyncYaml
}

func (rc *ServiceRender) SetAutoSyncYaml(yaml string) {
	if rc.OverrideYaml == nil {
		rc.OverrideYaml = &CustomYaml{}
	}
	rc.OverrideYaml.AutoSyncYaml = yaml
}

func (rc *ServiceRender) GetOverrideYaml() string {
	if rc.OverrideYaml == nil {
		return ""
	}
	return rc.OverrideYaml.YamlContent
}

func (rc *ServiceRender) SetOverrideYaml(Yaml string) {
	if rc.OverrideYaml == nil {
		rc.OverrideYaml = &CustomYaml{}
	}
	rc.OverrideYaml.YamlContent = Yaml
}

type KV struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func (rule *ImageSearchingRule) GetSearchingPattern() map[string]string {
	ret := make(map[string]string)
	if rule.Repo != "" {
		ret[setting.PathSearchComponentRepo] = rule.Repo
	}
	if rule.Namespace != "" {
		ret[setting.PathSearchComponentNamespace] = rule.Namespace
	}
	if rule.Image != "" {
		ret[setting.PathSearchComponentImage] = rule.Image
	}
	if rule.Tag != "" {
		ret[setting.PathSearchComponentTag] = rule.Tag
	}
	return ret
}
