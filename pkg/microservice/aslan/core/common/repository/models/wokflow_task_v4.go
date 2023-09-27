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

package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/koderover/zadig/pkg/microservice/aslan/config"
	commontypes "github.com/koderover/zadig/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/pkg/types"
)

type WorkflowTask struct {
	ID                  primitive.ObjectID `bson:"_id,omitempty"             json:"id,omitempty"`
	TaskID              int64              `bson:"task_id"                   json:"task_id"`
	WorkflowName        string             `bson:"workflow_name"             json:"workflow_name"`
	WorkflowHash        string             `bson:"workflow_hash"             json:"workflow_hash"`
	WorkflowDisplayName string             `bson:"workflow_display_name"     json:"workflow_display_name"`
	Params              []*Param           `bson:"params"                    json:"params"`
	WorkflowArgs        *WorkflowV4        `bson:"workflow_args"             json:"workflow_args"`
	OriginWorkflowArgs  *WorkflowV4        `bson:"origin_workflow_args"      json:"origin_workflow_args"`
	KeyVals             []*KeyVal          `bson:"key_vals"                  json:"key_vals"`
	GlobalContext       map[string]string  `bson:"global_context"            json:"global_context"`
	ClusterIDMap        map[string]bool    `bson:"cluster_id_map"            json:"cluster_id_map"`
	Status              config.Status      `bson:"status"                    json:"status,omitempty"`
	TaskCreator         string             `bson:"task_creator"              json:"task_creator,omitempty"`
	TaskCreatorPhone    string             `bson:"task_creator_phone"        json:"task_creator_phone"`
	TaskCreatorEmail    string             `bson:"task_creator_email"        json:"task_creator_email"`
	TaskRevoker         string             `bson:"task_revoker,omitempty"    json:"task_revoker,omitempty"`
	CreateTime          int64              `bson:"create_time"               json:"create_time,omitempty"`
	StartTime           int64              `bson:"start_time"                json:"start_time,omitempty"`
	EndTime             int64              `bson:"end_time"                  json:"end_time,omitempty"`
	Stages              []*StageTask       `bson:"stages"                    json:"stages"`
	ProjectName         string             `bson:"project_name,omitempty"    json:"project_name,omitempty"`
	IsDeleted           bool               `bson:"is_deleted"                json:"is_deleted"`
	IsArchived          bool               `bson:"is_archived"               json:"is_archived"`
	Error               string             `bson:"error,omitempty"           json:"error,omitempty"`
	IsRestart           bool               `bson:"is_restart"                json:"is_restart"`
	IsDebug             bool               `bson:"is_debug"                  json:"is_debug"`
	ShareStorages       []*ShareStorage    `bson:"share_storages"            json:"share_storages"`
}

func (WorkflowTask) TableName() string {
	return "workflow_task"
}

type StageTask struct {
	Name      string        `bson:"name"          json:"name"`
	Status    config.Status `bson:"status"        json:"status"`
	StartTime int64         `bson:"start_time"    json:"start_time,omitempty"`
	EndTime   int64         `bson:"end_time"      json:"end_time,omitempty"`
	Parallel  bool          `bson:"parallel"      json:"parallel,omitempty"`
	Approval  *Approval     `bson:"approval"      json:"approval,omitempty"`
	Jobs      []*JobTask    `bson:"jobs"          json:"jobs,omitempty"`
	Error     string        `bson:"error"         json:"error"`
}

type JobTask struct {
	Name string `bson:"name"                json:"name"`
	// jobTask unique id, unique in the workflow
	Key        string `bson:"key"                 json:"key"`
	K8sJobName string `bson:"k8s_job_name"        json:"k8s_job_name"`
	// JobInfo contains the fields that make up the job task name, for frontend display
	JobInfo          interface{}              `bson:"job_info"            json:"job_info"`
	JobType          string                   `bson:"type"                json:"type"`
	Status           config.Status            `bson:"status"              json:"status"`
	StartTime        int64                    `bson:"start_time"          json:"start_time,omitempty"`
	EndTime          int64                    `bson:"end_time"            json:"end_time,omitempty"`
	Error            string                   `bson:"error"               json:"error"`
	Timeout          int64                    `bson:"timeout"             json:"timeout"`
	Retry            int64                    `bson:"retry"               json:"retry"`
	Spec             interface{}              `bson:"spec"                json:"spec"`
	Outputs          []*Output                `bson:"outputs"             json:"outputs"`
	BreakpointBefore bool                     `bson:"breakpoint_before"   json:"breakpoint_before"`
	BreakpointAfter  bool                     `bson:"breakpoint_after"    json:"breakpoint_after"`
	ServiceModules   []*WorkflowServiceModule `bson:"service_modules"     json:"service_modules"`
}

type TaskJobInfo struct {
	RandStr       string `bson:"rand_str"        json:"rand_str"`
	TestType      string `bson:"test_type"       json:"test_type"`
	TestingName   string `bson:"testing_name"    json:"testing_name"`
	JobName       string `bson:"job_name"        json:"job_name"`
	ServiceName   string `bson:"service_name"    json:"service_name"`
	ServiceModule string `bson:"service_module"  json:"service_module"`
}

type WorkflowTaskPreview struct {
	TaskID              int64           `bson:"task_id"               json:"task_id"`
	TaskCreator         string          `bson:"task_creator"          json:"task_creator"`
	ProjectName         string          `bson:"project_name"          json:"project_name"`
	WorkflowName        string          `bson:"workflow_name"         json:"workflow_name"`
	WorkflowDisplayName string          `bson:"workflow_display_name" json:"workflow_display_name"`
	Status              config.Status   `bson:"status"                json:"status"`
	CreateTime          int64           `bson:"create_time"           json:"create_time,omitempty"`
	StartTime           int64           `bson:"start_time"            json:"start_time,omitempty"`
	EndTime             int64           `bson:"end_time"              json:"end_time,omitempty"`
	WorkflowArgs        *WorkflowV4     `bson:"workflow_args"         json:"-"`
	Stages              []*StagePreview `bson:"stages"                json:"stages,omitempty"`
}

type StagePreview struct {
	Name      string        `bson:"name"          json:"name"`
	Status    config.Status `bson:"status"        json:"status"`
	StartTime int64         `bson:"start_time"    json:"start_time,omitempty"`
	EndTime   int64         `bson:"end_time"      json:"end_time,omitempty"`
	Parallel  bool          `bson:"parallel"      json:"parallel,omitempty"`
	Approval  *Approval     `bson:"approval"      json:"approval,omitempty"`
	Jobs      []*JobPreview `bson:"jobs"          json:"jobs,omitempty"`
	Error     string        `bson:"error"         json:"error"`
}

type JobPreview struct {
	Name           string                   `bson:"name"                json:"name"`
	JobType        string                   `bson:"type"                json:"type"`
	Status         config.Status            `bson:"status"              json:"status"`
	StartTime      int64                    `bson:"start_time"          json:"start_time,omitempty"`
	EndTime        int64                    `bson:"end_time"            json:"end_time,omitempty"`
	Error          string                   `bson:"error"               json:"error"`
	Timeout        int64                    `bson:"timeout"             json:"timeout"`
	Retry          int64                    `bson:"retry"               json:"retry"`
	ServiceModules []*WorkflowServiceModule `bson:"-"                   json:"service_modules"`
	TestModules    []*WorkflowTestModule    `bson:"-"                   json:"test_modules"`
	Envs           *WorkflowEnv             `bson:"-"                   json:"envs"`
}

type WorkflowTestModule struct {
	RunningJobName string  `json:"running_job_name"`
	Type           string  `json:"type"`
	TestName       string  `json:"name"`
	TestCaseNum    int     `json:"total_case_num"`
	SuccessCaseNum int     `json:"success_case_num"`
	TestTime       float64 `json:"time"`
	Error          string  `json:"error"`
}

type WorkflowEnv struct {
	EnvName    string `json:"env_name"`
	Production bool   `json:"production"`
}

type JobTaskCustomDeploySpec struct {
	Namespace          string     `bson:"namespace"              json:"namespace"             yaml:"namespace"`
	ClusterID          string     `bson:"cluster_id"             json:"cluster_id"            yaml:"cluster_id"`
	Timeout            int64      `bson:"timeout"                json:"timeout"               yaml:"timeout"`
	WorkloadType       string     `bson:"workload_type"          json:"workload_type"         yaml:"workload_type"`
	WorkloadName       string     `bson:"workload_name"          json:"workload_name"         yaml:"workload_name"`
	ContainerName      string     `bson:"container_name"         json:"container_name"        yaml:"container_name"`
	Image              string     `bson:"image"                  json:"image"                 yaml:"image"`
	SkipCheckRunStatus bool       `bson:"skip_check_run_status"  json:"skip_check_run_status" yaml:"skip_check_run_status"`
	ReplaceResources   []Resource `bson:"replace_resources"      json:"replace_resources"     yaml:"replace_resources"`
}

type JobTaskDeploySpec struct {
	Env                string                          `bson:"env"                              json:"env"                                 yaml:"env"`
	ServiceName        string                          `bson:"service_name"                     json:"service_name"                        yaml:"service_name"`
	Production         bool                            `bson:"production"                       json:"production"                          yaml:"production"`
	DeployContents     []config.DeployContent          `bson:"deploy_contents"                  json:"deploy_contents"                     yaml:"deploy_contents"`
	KeyVals            []*ServiceKeyVal                `bson:"key_vals"                         json:"key_vals"                            yaml:"key_vals"`         // deprecated since 1.18.0
	VariableConfigs    []*DeplopyVariableConfig        `bson:"variable_configs"                 json:"variable_configs"                    yaml:"variable_configs"` // new since 1.18.0, only used for k8s
	VariableKVs        []*commontypes.RenderVariableKV `bson:"variable_kvs"                     json:"variable_kvs"                        yaml:"variable_kvs"`     // new since 1.18.0, only used for k8s
	UpdateConfig       bool                            `bson:"update_config"                    json:"update_config"                       yaml:"update_config"`
	YamlContent        string                          `bson:"yaml_content"                     json:"yaml_content"                        yaml:"yaml_content"`
	ServiceAndImages   []*DeployServiceModule          `bson:"service_and_images"               json:"service_and_images"                  yaml:"service_and_images"`
	ServiceType        string                          `bson:"service_type"                     json:"service_type"                        yaml:"service_type"`
	CreateEnvType      string                          `bson:"env_type"                         json:"env_type"                            yaml:"env_type"`
	SkipCheckRunStatus bool                            `bson:"skip_check_run_status"            json:"skip_check_run_status"               yaml:"skip_check_run_status"`
	ClusterID          string                          `bson:"cluster_id"                       json:"cluster_id"                          yaml:"cluster_id"`
	Timeout            int                             `bson:"timeout"                          json:"timeout"                             yaml:"timeout"`
	ReplaceResources   []Resource                      `bson:"replace_resources"                json:"replace_resources"                   yaml:"replace_resources"`
	RelatedPodLabels   []map[string]string             `bson:"-"                                json:"-"                                   yaml:"-"`
	// for compatibility
	ServiceModule string `bson:"service_module"                   json:"service_module"                      yaml:"-"`
	Image         string `bson:"image"                            json:"image"                               yaml:"-"`
}

type DeployServiceModule struct {
	ServiceModule string `bson:"service_module"                   json:"service_module"                      yaml:"service_module"`
	Image         string `bson:"image"                            json:"image"                               yaml:"image"`
	ImageName     string `bson:"image_name"                       json:"image_name"                          yaml:"image_name"`
}

type Resource struct {
	Name        string `bson:"name"                              json:"name"                                 yaml:"name"`
	Kind        string `bson:"kind"                              json:"kind"                                 yaml:"kind"`
	Container   string `bson:"container"                         json:"container"                            yaml:"container"`
	Origin      string `bson:"origin"                            json:"origin"                               yaml:"origin"`
	PodOwnerUID string `bson:"pod_owner_uid"                     json:"pod_owner_uid"                        yaml:"pod_owner_uid"`
}

type JobTaskHelmDeploySpec struct {
	Env            string                 `bson:"env"                              json:"env"                                 yaml:"env"`
	ServiceName    string                 `bson:"service_name"                     json:"service_name"                        yaml:"service_name"`
	ServiceType    string                 `bson:"service_type"                     json:"service_type"                        yaml:"service_type"`
	DeployContents []config.DeployContent `bson:"deploy_contents"                  json:"deploy_contents"                     yaml:"deploy_contents"`
	KeyVals        []*ServiceKeyVal       `bson:"key_vals"                         json:"key_vals"                            yaml:"key_vals"`
	// VariableYaml stores the variable YAML provided by user
	VariableYaml string `bson:"variable_yaml"                    json:"variable_yaml"                       yaml:"variable_yaml"`
	// IsProduction added since 1.18, indicator of production environment deployment job
	IsProduction bool   `bson:"is_production" yaml:"is_production" json:"is_production"`
	YamlContent  string `bson:"yaml_content"                     json:"yaml_content"                        yaml:"yaml_content"`
	// UserSuppliedValue added since 1.18, the values that users gives.
	UserSuppliedValue  string                   `bson:"user_supplied_value" json:"user_supplied_value" yaml:"user_supplied_value"`
	UpdateConfig       bool                     `bson:"update_config"                    json:"update_config"                       yaml:"update_config"`
	SkipCheckRunStatus bool                     `bson:"skip_check_run_status"            json:"skip_check_run_status"               yaml:"skip_check_run_status"`
	ImageAndModules    []*ImageAndServiceModule `bson:"image_and_service_modules"        json:"image_and_service_modules"           yaml:"image_and_service_modules"`
	ClusterID          string                   `bson:"cluster_id"                       json:"cluster_id"                          yaml:"cluster_id"`
	ReleaseName        string                   `bson:"release_name"                     json:"release_name"                        yaml:"release_name"`
	Timeout            int                      `bson:"timeout"                          json:"timeout"                             yaml:"timeout"`
	ReplaceResources   []Resource               `bson:"replace_resources"                json:"replace_resources"                   yaml:"replace_resources"`
}

type JobTaskHelmChartDeploySpec struct {
	Env                string           `bson:"env"                              json:"env"                                 yaml:"env"`
	DeployHelmChart    *DeployHelmChart `bson:"deploy_helm_chart"       yaml:"deploy_helm_chart"          json:"deploy_helm_chart"`
	SkipCheckRunStatus bool             `bson:"skip_check_run_status"            json:"skip_check_run_status"               yaml:"skip_check_run_status"`
	ClusterID          string           `bson:"cluster_id"                       json:"cluster_id"                          yaml:"cluster_id"`
	Timeout            int              `bson:"timeout"                          json:"timeout"                             yaml:"timeout"`
}

type ImageAndServiceModule struct {
	ServiceModule string `bson:"service_module"                     json:"service_module"                        yaml:"service_module"`
	Image         string `bson:"image"                              json:"image"                                 yaml:"image"`
}

type JobTaskFreestyleSpec struct {
	Properties JobProperties `bson:"properties"          json:"properties"        yaml:"properties"`
	Steps      []*StepTask   `bson:"steps"               json:"steps"             yaml:"steps"`
}

type JobTaskPluginSpec struct {
	Properties JobProperties   `bson:"properties"          json:"properties"        yaml:"properties"`
	Plugin     *PluginTemplate `bson:"plugin"              json:"plugin"            yaml:"plugin"`
}

type JobTaskBlueGreenDeploySpec struct {
	ClusterID        string `bson:"cluster_id"             json:"cluster_id"            yaml:"cluster_id"`
	Namespace        string `bson:"namespace"              json:"namespace"             yaml:"namespace"`
	DockerRegistryID string `bson:"docker_registry_id"     json:"docker_registry_id"    yaml:"docker_registry_id"`
	// unit is minute.
	DeployTimeout      int64   `bson:"deploy_timeout"              json:"deploy_timeout"             yaml:"deploy_timeout"`
	K8sServiceName     string  `bson:"k8s_service_name"            json:"k8s_service_name"           yaml:"k8s_service_name"`
	BlueK8sServiceName string  `bson:"blue_k8s_service_name"       json:"blue_k8s_service_name"      yaml:"blue_k8s_service_name"`
	WorkloadType       string  `bson:"workload_type"               json:"workload_type"              yaml:"workload_type"`
	WorkloadName       string  `bson:"workload_name"               json:"workload_name"              yaml:"workload_name"`
	BlueWorkloadName   string  `bson:"blue_workload_name"          json:"blue_workload_name"         yaml:"blue_workload_name"`
	ContainerName      string  `bson:"container_name"              json:"container_name"             yaml:"container_name"`
	Version            string  `bson:"version"                     json:"version"                    yaml:"version"`
	Image              string  `bson:"image"                       json:"image"                      yaml:"image"`
	FirstDeploy        bool    `bson:"first_deploy"                json:"first_deploy"               yaml:"first_deploy"`
	Events             *Events `bson:"events"                      json:"events"                     yaml:"events"`
}

type JobTaskBlueGreenDeployV2Spec struct {
	Production    bool                      `bson:"production"               json:"production"              yaml:"production"`
	Env           string                    `bson:"env"               json:"env"              yaml:"env"`
	Service       *BlueGreenDeployV2Service `bson:"service"                      json:"service"                     yaml:"service"`
	Events        *Events                   `bson:"events"                      json:"events"                     yaml:"events"`
	DeployTimeout int                       `bson:"deploy_timeout"              json:"deploy_timeout"             yaml:"deploy_timeout"`
}

type JobTaskBlueGreenReleaseSpec struct {
	ClusterID          string  `bson:"cluster_id"             json:"cluster_id"            yaml:"cluster_id"`
	Namespace          string  `bson:"namespace"              json:"namespace"             yaml:"namespace"`
	K8sServiceName     string  `bson:"k8s_service_name"       json:"k8s_service_name"      yaml:"k8s_service_name"`
	BlueK8sServiceName string  `bson:"blue_k8s_service_name"  json:"blue_k8s_service_name" yaml:"blue_k8s_service_name"`
	WorkloadType       string  `bson:"workload_type"          json:"workload_type"         yaml:"workload_type"`
	WorkloadName       string  `bson:"workload_name"          json:"workload_name"         yaml:"workload_name"`
	BlueWorkloadName   string  `bson:"blue_workload_name"     json:"blue_workload_name"    yaml:"blue_workload_name"`
	Version            string  `bson:"version"                json:"version"               yaml:"version"`
	Image              string  `bson:"image"                  json:"image"                 yaml:"image"`
	ContainerName      string  `bson:"container_name"         json:"container_name"        yaml:"container_name"`
	Events             *Events `bson:"events"                 json:"events"                yaml:"events"`
}

type JobTaskBlueGreenReleaseV2Spec struct {
	Production    bool                      `bson:"production"               json:"production"              yaml:"production"`
	Env           string                    `bson:"env"               json:"env"              yaml:"env"`
	Namespace     string                    `bson:"namespace"              json:"namespace"             yaml:"namespace"`
	Service       *BlueGreenDeployV2Service `bson:"service"                      json:"service"                     yaml:"service"`
	Events        *Events                   `bson:"events"                 json:"events"                yaml:"events"`
	DeployTimeout int                       `bson:"deploy_timeout"              json:"deploy_timeout"             yaml:"deploy_timeout"`
}

type JobTaskCanaryDeploySpec struct {
	ClusterID        string `bson:"cluster_id"             json:"cluster_id"            yaml:"cluster_id"`
	Namespace        string `bson:"namespace"              json:"namespace"             yaml:"namespace"`
	DockerRegistryID string `bson:"docker_registry_id"     json:"docker_registry_id"    yaml:"docker_registry_id"`
	// unit is minute.
	DeployTimeout      int64   `bson:"deploy_timeout"                 json:"deploy_timeout"                yaml:"deploy_timeout"`
	K8sServiceName     string  `bson:"k8s_service_name"               json:"k8s_service_name"              yaml:"k8s_service_name"`
	WorkloadType       string  `bson:"workload_type"                  json:"workload_type"                 yaml:"workload_type"`
	WorkloadName       string  `bson:"workload_name"                  json:"workload_name"                 yaml:"workload_name"`
	ContainerName      string  `bson:"container_name"                 json:"container_name"                yaml:"container_name"`
	CanaryPercentage   int     `bson:"canary_percentage"              json:"canary_percentage"             yaml:"canary_percentage"`
	CanaryReplica      int     `bson:"canary_replica"                 json:"canary_replica"                yaml:"canary_replica"`
	CanaryWorkloadName string  `bson:"canary_workload_name"           json:"canary_workload_name"          yaml:"canary_workload_name"`
	Version            string  `bson:"version"                        json:"version"                       yaml:"version"`
	Image              string  `bson:"image"                          json:"image"                         yaml:"image"`
	Events             *Events `bson:"events"                         json:"events"                        yaml:"events"`
}

type JobTaskCanaryReleaseSpec struct {
	ClusterID          string `bson:"cluster_id"             json:"cluster_id"             yaml:"cluster_id"`
	Namespace          string `bson:"namespace"              json:"namespace"              yaml:"namespace"`
	K8sServiceName     string `bson:"k8s_service_name"       json:"k8s_service_name"       yaml:"k8s_service_name"`
	WorkloadType       string `bson:"workload_type"          json:"workload_type"          yaml:"workload_type"`
	WorkloadName       string `bson:"workload_name"          json:"workload_name"          yaml:"workload_name"`
	ContainerName      string `bson:"container_name"         json:"container_name"         yaml:"container_name"`
	Version            string `bson:"version"                json:"version"                yaml:"version"`
	Image              string `bson:"image"                  json:"image"                  yaml:"image"`
	CanaryWorkloadName string `bson:"canary_workload_name"   json:"canary_workload_name"   yaml:"canary_workload_name"`
	// unit is minute.
	ReleaseTimeout int64   `bson:"release_timeout"        json:"release_timeout"       yaml:"release_timeout"`
	Events         *Events `bson:"events"                 json:"events"                yaml:"events"`
}

type JobTaskGrayReleaseSpec struct {
	ClusterID        string `bson:"cluster_id"             json:"cluster_id"             yaml:"cluster_id"`
	ClusterName      string `bson:"cluster_name"           json:"cluster_name"           yaml:"cluster_name"`
	Namespace        string `bson:"namespace"              json:"namespace"              yaml:"namespace"`
	WorkloadType     string `bson:"workload_type"          json:"workload_type"          yaml:"workload_type"`
	WorkloadName     string `bson:"workload_name"          json:"workload_name"          yaml:"workload_name"`
	ContainerName    string `bson:"container_name"         json:"container_name"         yaml:"container_name"`
	FirstJob         bool   `bson:"first_job"              json:"first_job"              yaml:"first_job"`
	Image            string `bson:"image"                  json:"image"                  yaml:"image"`
	GrayWorkloadName string `bson:"gray_workload_name"     json:"gray_workload_name"     yaml:"gray_workload_name"`
	// unit is minute.
	DeployTimeout int64   `bson:"deploy_timeout"        json:"deploy_timeout"       yaml:"deploy_timeout"`
	GrayScale     int     `bson:"gray_scale"            json:"gray_scale"           yaml:"gray_scale"`
	TotalReplica  int     `bson:"total_replica"         json:"total_replica"        yaml:"total_replica"`
	GrayReplica   int     `bson:"gray_replica"          json:"gray_replica"         yaml:"gray_replica"`
	Events        *Events `bson:"events"                json:"events"               yaml:"events"`
}

type JobIstioReleaseSpec struct {
	FirstJob          bool            `bson:"first_job"          json:"first_job"          yaml:"first_job"`
	Timeout           int64           `bson:"timeout"            json:"timeout"            yaml:"timeout"`
	ClusterID         string          `bson:"cluster_id"         json:"cluster_id"         yaml:"cluster_id"`
	ClusterName       string          `bson:"cluster_name"       json:"cluster_name"       yaml:"cluster_name"`
	Namespace         string          `bson:"namespace"          json:"namespace"          yaml:"namespace"`
	Weight            int64           `bson:"weight"             json:"weight"             yaml:"weight"`
	ReplicaPercentage int64           `bson:"replica_percentage" json:"replica_percentage" yaml:"replica_percentage"`
	Replicas          int64           `bson:"replicas"           json:"replicas"           yaml:"replicas"`
	Targets           *IstioJobTarget `bson:"targets"            json:"targets"            yaml:"targets"`
	Event             []*Event        `bson:"event"              json:"event"              yaml:"event"`
}

type JobIstioRollbackSpec struct {
	Namespace   string          `json:"namespace"    bson:"namespace"    yaml:"namespace"`
	ClusterID   string          `json:"cluster_id"   bson:"cluster_id"   yaml:"cluster_id"`
	ClusterName string          `json:"cluster_name" bson:"cluster_name" yaml:"cluster_name"`
	Image       string          `json:"image"        bson:"image"        yaml:"image"`
	Replicas    int             `json:"replicas"     bson:"replicas"     yaml:"replicas"`
	Targets     *IstioJobTarget `json:"targets"      bson:"targets"      yaml:"targets"`
	Timeout     int64           `json:"timeout"      bson:"timeout"      yaml:"timeout"`
}

type MeegoTransitionSpec struct {
	Link            string                     `bson:"link"               json:"link"               yaml:"link"`
	Source          string                     `bson:"source"             json:"source"             yaml:"source"`
	ProjectKey      string                     `bson:"project_key"        json:"project_key"        yaml:"project_key"`
	ProjectName     string                     `bson:"project_name"       json:"project_name"       yaml:"project_name"`
	MeegoID         string                     `bson:"meego_id"           json:"meego_id"           yaml:"meego_id"`
	WorkItemType    string                     `bson:"work_item_type"     json:"work_item_type"     yaml:"work_item_type"`
	WorkItemTypeKey string                     `bson:"work_item_type_key" json:"work_item_type_key" yaml:"work_item_type_key"`
	WorkItems       []*MeegoWorkItemTransition `bson:"work_items"         json:"work_items"         yaml:"work_items"`
}

type JobTaskGrayRollbackSpec struct {
	ClusterID        string `bson:"cluster_id"             json:"cluster_id"             yaml:"cluster_id"`
	ClusterName      string `bson:"cluster_name"           json:"cluster_name"           yaml:"cluster_name"`
	Namespace        string `bson:"namespace"              json:"namespace"              yaml:"namespace"`
	WorkloadType     string `bson:"workload_type"          json:"workload_type"          yaml:"workload_type"`
	WorkloadName     string `bson:"workload_name"          json:"workload_name"          yaml:"workload_name"`
	ContainerName    string `bson:"container_name"         json:"container_name"         yaml:"container_name"`
	Image            string `bson:"image"                  json:"image"                  yaml:"image"`
	GrayWorkloadName string `bson:"gray_workload_name"     json:"gray_workload_name"     yaml:"gray_workload_name"`
	// unit is minute.
	RollbackTimeout int64   `bson:"rollback_timeout"      json:"rollback_timeout"     yaml:"rollback_timeout"`
	TotalReplica    int     `bson:"total_replica"         json:"total_replica"        yaml:"total_replica"`
	Events          *Events `bson:"events"                json:"events"               yaml:"events"`
}

type JobTasK8sPatchSpec struct {
	ClusterID  string           `bson:"cluster_id"             json:"cluster_id"             yaml:"cluster_id"`
	Namespace  string           `bson:"namespace"              json:"namespace"              yaml:"namespace"`
	PatchItems []*PatchTaskItem `bson:"patch_items"            json:"patch_items"            yaml:"patch_items"`
}

type IssueID struct {
	Key    string `bson:"key" json:"key" yaml:"key"`
	Name   string `bson:"name" json:"name" yaml:"name"`
	Status string `bson:"status,omitempty" json:"status,omitempty" yaml:"status,omitempty"`
	Link   string `bson:"link,omitempty" json:"link,omitempty" yaml:"link,omitempty"`
}

type JobTaskJiraSpec struct {
	ProjectID    string     `bson:"project_id"  json:"project_id"  yaml:"project_id"`
	JiraID       string     `bson:"jira_id"  json:"jira_id"  yaml:"jira_id"`
	IssueType    string     `bson:"issue_type"  json:"issue_type"  yaml:"issue_type"`
	Issues       []*IssueID `bson:"issues" json:"issues" yaml:"issues"`
	TargetStatus string     `bson:"target_status" json:"target_status" yaml:"target_status"`
}

type JobTaskNacosSpec struct {
	NacosID       string       `bson:"nacos_id"         json:"nacos_id"         yaml:"nacos_id"`
	NamespaceID   string       `bson:"namespace_id"     json:"namespace_id"     yaml:"namespace_id"`
	NamespaceName string       `bson:"namespace_name"   json:"namespace_name"   yaml:"namespace_name"`
	NacosAddr     string       `bson:"nacos_addr"       json:"nacos_addr"       yaml:"nacos_addr"`
	UserName      string       `bson:"user_name"        json:"user_name"        yaml:"user_name"`
	Password      string       `bson:"password"         json:"password"         yaml:"password"`
	NacosDatas    []*NacosData `bson:"nacos_datas"      json:"nacos_datas"      yaml:"nacos_datas"`
}

type NacosData struct {
	types.NacosConfig `bson:",inline" json:",inline" yaml:",inline"`
	Error             string `bson:"error"      json:"error"      yaml:"error"`
}

type JobTaskSQLSpec struct {
	ID   string                `bson:"id" json:"id" yaml:"id"`
	Type config.DBInstanceType `bson:"type" json:"type" yaml:"type"`
	SQL  string                `bson:"sql" json:"sql" yaml:"sql"`
}

type JobTaskApolloSpec struct {
	ApolloID      string                    `bson:"apolloID" json:"apolloID" yaml:"apolloID"`
	NamespaceList []*JobTaskApolloNamespace `bson:"namespaceList" json:"namespaceList" yaml:"namespaceList"`
}

type JobTaskApolloNamespace struct {
	ApolloNamespace `bson:",inline" json:",inline" yaml:",inline"`
	Error           string `bson:"error" json:"error" yaml:"error"`
}

type ApolloKV struct {
	Key string `bson:"key" json:"key" yaml:"key"`
	Val string `bson:"val" json:"val" yaml:"val"`
}

type JobTaskJenkinsSpec struct {
	ID   string                `bson:"id" json:"id" yaml:"id"`
	Host string                `bson:"host" json:"host" yaml:"host"`
	Job  JobTaskJenkinsJobInfo `bson:"job" json:"job" yaml:"job"`
}

type JobTaskJenkinsJobInfo struct {
	JobName    string                 `bson:"job_name" json:"job_name" yaml:"job_name"`
	JobID      int                    `bson:"job_id" json:"job_id" yaml:"job_id"`
	JobOutput  string                 `bson:"job_output" json:"job_output" yaml:"job_output"`
	Parameters []*JenkinsJobParameter `bson:"parameters" json:"parameters" yaml:"parameters"`
}

type JobTaskWorkflowTriggerSpec struct {
	TriggerType           config.WorkflowTriggerType `bson:"trigger_type" json:"trigger_type" yaml:"trigger_type"`
	IsEnableCheck         bool                       `bson:"is_enable_check" json:"is_enable_check" yaml:"is_enable_check"`
	WorkflowTriggerEvents []*WorkflowTriggerEvent    `bson:"workflow_trigger_events" json:"workflow_trigger_events" yaml:"workflow_trigger_events"`
}

type WorkflowTriggerEvent struct {
	ServiceName         string        `bson:"service_name" json:"service_name" yaml:"service_name"`
	ServiceModule       string        `bson:"service_module" json:"service_module" yaml:"service_module"`
	WorkflowName        string        `bson:"workflow_name" json:"workflow_name" yaml:"workflow_name"`
	WorkflowDisplayName string        `bson:"workflow_display_name" json:"workflow_display_name" yaml:"workflow_display_name"`
	TaskID              int64         `bson:"task_id" json:"task_id" yaml:"task_id"`
	Status              config.Status `bson:"status" json:"status" yaml:"status"`
	Params              []*Param      `bson:"params" json:"params" yaml:"params"`
	ProjectName         string        `bson:"project_name" json:"project_name" yaml:"project_name"`
}

type JobTaskOfflineServiceSpec struct {
	EnvType       config.EnvType                `bson:"env_type" json:"env_type" yaml:"env_type"`
	EnvName       string                        `bson:"env_name" json:"env_name" yaml:"env_name"`
	Namespace     string                        `bson:"namespace" json:"namespace" yaml:"namespace"`
	ServiceEvents []*JobTaskOfflineServiceEvent `bson:"service_events" json:"service_events" yaml:"service_events"`
}

type JobTaskOfflineServiceEvent struct {
	ServiceName string        `bson:"service_name" json:"service_name" yaml:"service_name"`
	Status      config.Status `bson:"status" json:"status" yaml:"status"`
	Error       string        `bson:"error" json:"error" yaml:"error"`
}

type JobTaskGuanceyunCheckSpec struct {
	ID   string `bson:"id" json:"id" yaml:"id"`
	Name string `bson:"name" json:"name" yaml:"name"`
	// CheckTime minute
	CheckTime int64               `bson:"check_time" json:"check_time" yaml:"check_time"`
	CheckMode string              `bson:"check_mode" json:"check_mode" yaml:"check_mode"`
	Monitors  []*GuanceyunMonitor `bson:"monitors" json:"monitors" yaml:"monitors"`
}

type JobTaskMseGrayReleaseSpec struct {
	Production         bool                  `bson:"production" json:"production" yaml:"production"`
	GrayTag            string                `bson:"gray_tag" json:"gray_tag" yaml:"gray_tag"`
	BaseEnv            string                `bson:"base_env" json:"base_env" yaml:"base_env"`
	GrayEnv            string                `bson:"gray_env" json:"gray_env" yaml:"gray_env"`
	SkipCheckRunStatus bool                  `bson:"skip_check_run_status" json:"skip_check_run_status" yaml:"skip_check_run_status"`
	GrayService        MseGrayReleaseService `bson:"gray_service" json:"gray_service" yaml:"gray_service"`
	Events             []*Event              `bson:"events" json:"events" yaml:"events"`
	Timeout            int                   `bson:"timeout" json:"timeout" yaml:"timeout"`
}

type JobTaskMseGrayOfflineSpec struct {
	Production      bool                     `bson:"production" json:"production" yaml:"production"`
	Env             string                   `bson:"env" json:"env" yaml:"env"`
	GrayTag         string                   `bson:"gray_tag" json:"gray_tag" yaml:"gray_tag"`
	Namespace       string                   `bson:"namespace" json:"namespace" yaml:"namespace"`
	OfflineServices []*MseGrayOfflineService `bson:"offline_services" json:"offline_services" yaml:"offline_services"`
	Events          []*Event                 `bson:"events" json:"events" yaml:"events"`
}

type MseGrayOfflineService struct {
	ServiceName string        `bson:"service_name" json:"service_name" yaml:"service_name"`
	Status      config.Status `bson:"status" json:"status" yaml:"status"`
	Error       string        `bson:"error" json:"error" yaml:"error"`
}

type PatchTaskItem struct {
	ResourceName    string   `bson:"resource_name"                json:"resource_name"               yaml:"resource_name"`
	ResourceKind    string   `bson:"resource_kind"                json:"resource_kind"               yaml:"resource_kind"`
	ResourceGroup   string   `bson:"resource_group"               json:"resource_group"              yaml:"resource_group"`
	ResourceVersion string   `bson:"resource_version"             json:"resource_version"            yaml:"resource_version"`
	PatchContent    string   `bson:"patch_content"                json:"patch_content"               yaml:"patch_content"`
	Params          []*Param `bson:"params"                       json:"params"                      yaml:"params"`
	// support strategic-merge/merge/json
	PatchStrategy string `bson:"patch_strategy"          json:"patch_strategy"         yaml:"patch_strategy"`
	Error         string `bson:"error"                   json:"error"                  yaml:"error"`
}

type Event struct {
	EventType string `bson:"event_type"             json:"event_type"            yaml:"event_type"`
	Time      string `bson:"time"                   json:"time"                  yaml:"time"`
	Message   string `bson:"message"                json:"message"               yaml:"message"`
}

type Events []*Event

func (e *Events) Info(message string) {
	*e = append(*e, &Event{
		EventType: "info",
		Time:      time.Now().Format("2006-01-02 15:04:05"),
		Message:   message,
	})
}

func (e *Events) Error(message string) {
	*e = append(*e, &Event{
		EventType: "error",
		Time:      time.Now().Format("2006-01-02 15:04:05"),
		Message:   message,
	})
}

type StepTask struct {
	Name      string          `bson:"name"           json:"name"         yaml:"name"`
	JobName   string          `bson:"job_name"       json:"job_name"     yaml:"job_name"`
	Error     string          `bson:"error"          json:"error"        yaml:"error"`
	StepType  config.StepType `bson:"type"           json:"type"         yaml:"type"`
	Onfailure bool            `bson:"on_failure"     json:"on_failure"   yaml:"on_failure"`
	// step input params,differ form steps
	Spec interface{} `bson:"spec"           json:"spec"   yaml:"spec"`
	// step output results,like testing results,differ form steps
	Result interface{} `bson:"result"         json:"result"  yaml:"result"`
}

type WorkflowTaskCtx struct {
	WorkflowName                string
	WorkflowDisplayName         string
	ProjectName                 string
	TaskID                      int64
	DockerHost                  string
	Workspace                   string
	DistDir                     string
	DockerMountDir              string
	ConfigMapMountDir           string
	WorkflowTaskCreatorUsername string
	WorkflowTaskCreatorEmail    string
	WorkflowTaskCreatorMobile   string
	WorkflowKeyVals             []*KeyVal
	GlobalContextGetAll         func() map[string]string
	GlobalContextGet            func(key string) (string, bool)
	GlobalContextSet            func(key, value string)
	GlobalContextEach           func(f func(k, v string) bool)
	ClusterIDAdd                func(clusterID string)
	SetStatus                   func(status config.Status)
}
