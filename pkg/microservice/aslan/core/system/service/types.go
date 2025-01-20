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
	"errors"
	"net/url"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/util"
)

var ClusterProviderValueNames map[int8]string = map[int8]string{
	1: "阿里云 ACK",
	2: "腾讯云 TKE",
	3: "华为云 CCE",
	4: "Amazon EKS",
	5: "腾讯云 TKE Serverless",
	6: "谷歌云 GCP",
	7: "微软云 AKS",
	0: "标准 Kubernetes 集群",
}

type ExternalSystemDetail struct {
	ID      string           `json:"id"`
	Name    string           `json:"name"`
	Server  string           `json:"server"`
	Headers []*util.KeyValue `json:"headers,omitempty"`
	// @2023-04-14 APIToken is deprecated after 1.4.0-ee
	APIToken string `json:"api_token,omitempty"`
}

type WorkflowConcurrencySettings struct {
	WorkflowConcurrency int64 `json:"workflow_concurrency"`
	BuildConcurrency    int64 `json:"build_concurrency"`
}

type SonarIntegration struct {
	ID             string `json:"id"`
	SystemIdentity string `json:"system_identity"`
	ServerAddress  string `json:"server_address"`
	Token          string `json:"token"`
}

type OpenAPICreateRegistryReq struct {
	Address   string                  `json:"address"`
	Provider  config.RegistryProvider `json:"provider"`
	Namespace string                  `json:"namespace"`
	IsDefault bool                    `json:"is_default"`
	AccessKey string                  `json:"access_key"`
	SecretKey string                  `json:"secret_key"`
	EnableTLS bool                    `json:"enable_tls"`
	// Optional field below
	Region  string `json:"region"`
	TLSCert string `json:"tls_cert"`
}

func (req OpenAPICreateRegistryReq) Validate() error {
	if req.Address == "" {
		return errors.New("address cannot be empty")
	}

	switch req.Provider {
	case config.RegistryProviderECR, config.RegistryProviderDockerhub, config.RegistryProviderACR, config.RegistryProviderHarbor, config.RegistryProviderNative, config.RegistryProviderSWR, config.RegistryProviderTCR:
		break
	default:
		return errors.New("unsupported registry provider")
	}

	// only ECR can ignore namespace since it is in the address
	if req.Namespace == "" && req.Provider != config.RegistryProviderECR {
		return errors.New("namespace cannot be empty")
	}

	if req.Provider == config.RegistryProviderECR && req.Region == "" {
		return errors.New("region is a required field for ECR provider")
	}

	// address needs to be a validate url
	_, err := url.ParseRequestURI(req.Address)
	if err != nil {
		return errors.New("address needs to be a valid URL")
	}

	return nil
}

type DashBoardConfig struct {
	Cards []*DashBoardCardConfig `json:"cards"`
}

type DashBoardCardConfig struct {
	ID     string      `json:"id"`
	Name   string      `json:"name"`
	Type   string      `json:"type"`
	Config interface{} `json:"config"`
}

type MyWorkflowCardConfig struct {
	WorkflowList []*WorkflowConfig `json:"workflow_list"`
}

type MyEnvCardConfig struct {
	EnvType        string   `json:"env_type"`
	EnvName        string   `json:"env_name"`
	Production     bool     `json:"production"`
	ProjectType    string   `json:"project_type"`
	ProjectName    string   `json:"project_name"`
	ServiceModules []string `json:"service_modules"`
}

type WorkflowConfig struct {
	Name    string `json:"name"`
	Project string `json:"project_name"`
}

type WorkflowResponse struct {
	TaskID      int64  `json:"task_id,omitempty"`
	Name        string `json:"name"`
	Project     string `json:"project"`
	Creator     string `json:"creator"`
	StartTime   int64  `json:"start_time"`
	Status      string `json:"status"`
	DisplayName string `json:"display_name"`
	Type        string `json:"workflow_type"`
	TestName    string `json:"test_name,omitempty"`
	ScanName    string `json:"scan_name,omitempty"`
	ScanID      string `json:"scan_id,omitempty"`
}

type EnvResponse struct {
	Name        string          `json:"name"`
	Alias       string          `json:"alias"`
	Production  bool            `json:"production"`
	ProjectName string          `json:"project_name"`
	UpdateTime  int64           `json:"update_time"`
	UpdatedBy   string          `json:"updated_by"`
	ClusterID   string          `json:"cluster_id"`
	Services    []*EnvService   `json:"services,omitempty"`
	VMServices  []*VMEnvService `json:"vm_services,omitempty"`
}

type EnvService struct {
	ServiceName  string `json:"service_name"`
	WorkloadType string `json:"workload_type"`
	Status       string `json:"status"`
	Image        string `json:"image"`
}

type VMEnvService struct {
	ServiceName string                    `json:"service_name"`
	EnvStatus   []*commonmodels.EnvStatus `json:"env_status"`
}

type MeegoProjectResp struct {
	Projects []*MeegoProject `json:"projects"`
}

type MeegoProject struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type MeegoWorkItemTypeResp struct {
	WorkItemTypes []*MeegoWorkItemType `json:"work_item_types"`
}

type MeegoWorkItemType struct {
	TypeKey string `json:"type_key"`
	Name    string `json:"name"`
}

type MeegoWorkItemResp struct {
	WorkItems []*MeegoWorkItem `json:"work_items"`
}

type MeegoWorkItem struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	CurrentState string `json:"current_state"`
}

type MeegoTransitionResp struct {
	TargetStatus []*MeegoWorkItemStatusTransition `json:"target_status"`
}

type MeegoWorkItemStatusTransition struct {
	SourceStateKey  string `json:"source_state_key"`
	SourceStateName string `json:"source_state_name"`
	TargetStateKey  string `json:"target_state_key"`
	TargetStateName string `json:"target_state_name"`
	TransitionID    int64  `json:"transition_id"`
}

type OpenAPIRegistry struct {
	ID        string                  `json:"registry_id"`
	Address   string                  `json:"address"`
	Provider  config.RegistryProvider `json:"provider"`
	Region    string                  `json:"region"`
	Namespace string                  `json:"namespace"`
	IsDefault bool                    `json:"is_default"`
}

type OpenAPICreateClusterRequest struct {
	Name         string   `json:"name"`
	Production   bool     `json:"production"`
	Description  string   `json:"description"`
	Provider     int8     `json:"provider"`
	Type         string   `json:"type"`
	KubeConfig   string   `json:"kube_config"`
	ProjectNames []string `json:"project_names"`
}

type OpenAPICreateClusterResponse struct {
	Cluster  *OpenAPICluster `json:"cluster"`
	AgentCmd string          `json:"agent_cmd"`
}

type OpenAPICluster struct {
	ID           string   `json:"cluster_id"`
	Name         string   `json:"name"`
	Production   bool     `json:"production"`
	Description  string   `json:"description"`
	Provider     int8     `json:"provider"`
	ProviderName string   `json:"provider_name"`
	CreatedBy    string   `json:"created_by"`
	CreatedTime  int64    `json:"created_time"`
	Local        bool     `json:"local"`
	Status       string   `json:"status"`
	Type         string   `json:"type"`
	ProjectNames []string `json:"project_names"`
}

type SecurityAndPrivacySettings struct {
	TokenExpirationTime int64 `json:"token_expiration_time"`
	ImprovementPlan     bool  `json:"improvement_plan"`
}

type ApolloConfig struct {
	ConfigType string                   `json:"type"`
	Config     []*commonmodels.ApolloKV `json:"kv"`
}

type BriefNacosConfig struct {
	DataID        string `json:"data_id"`
	Format        string `json:"format"`
	Group         string `json:"group"`
	NamespaceID   string `json:"namespace_id"`
	NamespaceName string `json:"namespace_name"`
}
