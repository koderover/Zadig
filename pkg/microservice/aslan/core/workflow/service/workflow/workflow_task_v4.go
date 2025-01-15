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

package workflow

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	config2 "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/dingtalk"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/instantmessage"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/lark"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/s3"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/scmnotify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workflowcontroller"
	workwxservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workwx"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow/job"
	jobctl "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow/job"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/user"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	larktool "github.com/koderover/zadig/v2/pkg/tool/lark"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	s3tool "github.com/koderover/zadig/v2/pkg/tool/s3"
	"github.com/koderover/zadig/v2/pkg/tool/sonar"
	workflowtool "github.com/koderover/zadig/v2/pkg/tool/workflow"
	"github.com/koderover/zadig/v2/pkg/types"
	jobspec "github.com/koderover/zadig/v2/pkg/types/job"
	"github.com/koderover/zadig/v2/pkg/types/step"
	stepspec "github.com/koderover/zadig/v2/pkg/types/step"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm/utils"
	"k8s.io/apimachinery/pkg/util/sets"
)

type CreateTaskV4Resp struct {
	ProjectName  string `json:"project_name"`
	WorkflowName string `json:"workflow_name"`
	TaskID       int64  `json:"task_id"`
}

type WorkflowTaskPreview struct {
	TaskID              int64                 `bson:"task_id"                   json:"task_id"`
	WorkflowName        string                `bson:"workflow_name"             json:"workflow_key"`
	WorkflowDisplayName string                `bson:"workflow_display_name"     json:"workflow_name"`
	Params              []*commonmodels.Param `bson:"params"                    json:"params"`
	Status              config.Status         `bson:"status"                    json:"status,omitempty"`
	Remark              string                `bson:"remark"                    json:"remark"`
	TaskCreator         string                `bson:"task_creator"              json:"task_creator,omitempty"`
	TaskRevoker         string                `bson:"task_revoker,omitempty"    json:"task_revoker,omitempty"`
	CreateTime          int64                 `bson:"create_time"               json:"create_time,omitempty"`
	StartTime           int64                 `bson:"start_time"                json:"start_time,omitempty"`
	EndTime             int64                 `bson:"end_time"                  json:"end_time,omitempty"`
	Stages              []*StageTaskPreview   `bson:"stages"                    json:"stages"`
	ProjectName         string                `bson:"project_name"              json:"project_key"`
	Error               string                `bson:"error,omitempty"           json:"error,omitempty"`
	IsRestart           bool                  `bson:"is_restart"                json:"is_restart"`
	Debug               bool                  `bson:"debug"                     json:"debug"`
	ApprovalTicketID    string                `bson:"approval_ticket_id"        json:"approval_ticket_id"`
	ApprovalID          string                `bson:"approval_id"               json:"approval_id"`
}

type StageTaskPreview struct {
	Name       string                   `bson:"name"          json:"name"`
	Status     config.Status            `bson:"status"        json:"status"`
	StartTime  int64                    `bson:"start_time"    json:"start_time,omitempty"`
	EndTime    int64                    `bson:"end_time"      json:"end_time,omitempty"`
	Parallel   bool                     `bson:"parallel"      json:"parallel"`
	ManualExec *commonmodels.ManualExec `bson:"manual_exec"      json:"manual_exec"`
	Jobs       []*JobTaskPreview        `bson:"jobs"          json:"jobs"`
	Error      string                   `bson:"error" json:"error""`
}

type JobTaskPreview struct {
	Name                 string                       `bson:"name"           json:"name"`
	Key                  string                       `bson:"key"            json:"key"`
	DisplayName          string                       `bson:"display_name"   json:"display_name"`
	OriginName           string                       `bson:"origin_name"    json:"origin_name"`
	JobType              string                       `bson:"type"           json:"type"`
	Status               config.Status                `bson:"status"         json:"status"`
	StartTime            int64                        `bson:"start_time"     json:"start_time,omitempty"`
	EndTime              int64                        `bson:"end_time"       json:"end_time,omitempty"`
	CostSeconds          int64                        `bson:"cost_seconds"   json:"cost_seconds"`
	Error                string                       `bson:"error"          json:"error"`
	BreakpointBefore     bool                         `bson:"breakpoint_before" json:"breakpoint_before"`
	BreakpointAfter      bool                         `bson:"breakpoint_after"  json:"breakpoint_after"`
	Spec                 interface{}                  `bson:"spec"           json:"spec"`
	ErrorPolicy          *commonmodels.JobErrorPolicy `bson:"error_policy"         yaml:"error_policy"         json:"error_policy"`
	ErrorHandlerUserID   string                       `bson:"error_handler_user_id"  yaml:"error_handler_user_id" json:"error_handler_user_id"`
	ErrorHandlerUserName string                       `bson:"error_handler_username"  yaml:"error_handler_username" json:"error_handler_username"`
	RetryCount           int                          `bson:"retry_count"           yaml:"retry_count"               json:"retry_count"`
	// JobInfo contains the fields that make up the job task name, for frontend display
	JobInfo interface{} `bson:"job_info" json:"job_info"`
}

type ZadigBuildJobSpec struct {
	Repos         []*types.Repository    `bson:"repos"           json:"repos"`
	Image         string                 `bson:"image"           json:"image"`
	Package       string                 `bson:"package"         json:"package"`
	ServiceName   string                 `bson:"service_name"    json:"service_name"`
	ServiceModule string                 `bson:"service_module"  json:"service_module"`
	Envs          []*commonmodels.KeyVal `bson:"envs"            json:"envs"`
}

type ZadigTestingJobSpec struct {
	Repos         []*types.Repository    `bson:"repos"           json:"repos"`
	JunitReport   bool                   `bson:"junit_report"    json:"junit_report"`
	Archive       bool                   `bson:"archive"         json:"archive"`
	HtmlReport    bool                   `bson:"html_report"     json:"html_report"`
	ProjectName   string                 `bson:"project_name"    json:"project_name"`
	TestName      string                 `bson:"test_name"       json:"test_name"`
	TestType      string                 `bson:"test_type"       json:"test_type"`
	ServiceName   string                 `bson:"service_name"    json:"service_name"`
	ServiceModule string                 `bson:"service_module"  json:"service_module"`
	Envs          []*commonmodels.KeyVal `bson:"envs"            json:"envs"`
}

type ZadigScanningJobSpec struct {
	TestType      string                 `bson:"scanning_type"   json:"scanning_type"`
	ServiceName   string                 `bson:"service_name"    json:"service_name"`
	ServiceModule string                 `bson:"service_module"  json:"service_module"`
	Repos         []*types.Repository    `bson:"repos"           json:"repos"`
	SonarMetrics  *step.SonarMetrics     `bson:"sonar_metrics"   json:"sonar_metrics"`
	IsHasArtifact bool                   `bson:"is_has_artifact" json:"is_has_artifact"`
	LinkURL       string                 `bson:"link_url"        json:"link_url"`
	ScanningName  string                 `bson:"scanning_name"   json:"scanning_name"`
	Envs          []*commonmodels.KeyVal `bson:"envs"            json:"envs"`
}

type ZadigDeployJobPreviewSpec struct {
	Env                string             `bson:"env"                          json:"env"`
	Production         bool               `bson:"-"                            json:"production"`
	SkipCheckRunStatus bool               `bson:"skip_check_run_status"        json:"skip_check_run_status"`
	ServiceAndImages   []*ServiceAndImage `bson:"service_and_images"           json:"service_and_images"`
	YamlContent        string             `bson:"yaml_content"                 json:"yaml_content"`
	// UserSuppliedValue added since 1.18, the values that users gives.
	UserSuppliedValue string `bson:"user_supplied_value" json:"user_supplied_value" yaml:"user_supplied_value"`
	// VariableConfigs new since 1.18, only used for k8s
	VariableConfigs []*commonmodels.DeployVariableConfig `bson:"variable_configs"                 json:"variable_configs"                    yaml:"variable_configs"`
	// VariableKVs new since 1.18, only used for k8s
	VariableKVs []*commontypes.RenderVariableKV `bson:"variable_kvs"                 json:"variable_kvs"                    yaml:"variable_kvs"`
}

type CustomDeployJobSpec struct {
	Image              string `bson:"image"                        json:"image"`
	Target             string `bson:"target"                       json:"target"`
	ClusterName        string `bson:"cluster_name"                 json:"cluster_name"`
	Namespace          string `bson:"namespace"                    json:"namespace"`
	SkipCheckRunStatus bool   `bson:"skip_check_run_status"        json:"skip_check_run_status"`
}

type ServiceAndImage struct {
	ServiceName   string `bson:"service_name"           json:"service_name"`
	ServiceModule string `bson:"service_module"         json:"service_module"`
	Image         string `bson:"image"                  json:"image"`
}

type K8sCanaryDeployJobSpec struct {
	Image          string               `bson:"image"                        json:"image"`
	K8sServiceName string               `bson:"k8s_service_name"             json:"k8s_service_name"`
	ClusterName    string               `bson:"cluster_name"                 json:"cluster_name"`
	Namespace      string               `bson:"namespace"                    json:"namespace"`
	ContainerName  string               `bson:"container_name"               json:"container_name"`
	CanaryReplica  int                  `bson:"canary_replica"               json:"canary_replica"`
	Events         *commonmodels.Events `bson:"events"                       json:"events"`
}

type K8sCanaryReleaseJobSpec struct {
	Image          string               `bson:"image"                        json:"image"`
	K8sServiceName string               `bson:"k8s_service_name"             json:"k8s_service_name"`
	ClusterName    string               `bson:"cluster_name"                 json:"cluster_name"`
	Namespace      string               `bson:"namespace"                    json:"namespace"`
	ContainerName  string               `bson:"container_name"               json:"container_name"`
	Events         *commonmodels.Events `bson:"events"                       json:"events"`
}

type K8sBlueGreenDeployJobSpec struct {
	Image          string               `bson:"image"                        json:"image"`
	K8sServiceName string               `bson:"k8s_service_name"             json:"k8s_service_name"`
	ClusterName    string               `bson:"cluster_name"                 json:"cluster_name"`
	Namespace      string               `bson:"namespace"                    json:"namespace"`
	ContainerName  string               `bson:"container_name"               json:"container_name"`
	Events         *commonmodels.Events `bson:"events"                       json:"events"`
}

type K8sBlueGreenReleaseJobSpec struct {
	Image          string               `bson:"image"                        json:"image"`
	K8sServiceName string               `bson:"k8s_service_name"             json:"k8s_service_name"`
	ClusterName    string               `bson:"cluster_name"                 json:"cluster_name"`
	Namespace      string               `bson:"namespace"                    json:"namespace"`
	ContainerName  string               `bson:"container_name"               json:"container_name"`
	Events         *commonmodels.Events `bson:"events"                       json:"events"`
}

type DistributeImageJobSpec struct {
	SourceRegistryID string                       `bson:"source_registry_id"           json:"source_registry_id"`
	TargetRegistryID string                       `bson:"target_registry_id"           json:"target_registry_id"`
	DistributeTarget []*step.DistributeTaskTarget `bson:"distribute_target"            json:"distribute_target"`
}

func GetWorkflowv4Preset(encryptedKey, workflowName, uid, username, ticketID string, log *zap.SugaredLogger) (*commonmodels.WorkflowV4, error) {
	workflow, err := commonrepo.NewWorkflowV4Coll().Find(workflowName)
	if err != nil {
		log.Errorf("cannot find workflow %s, the error is: %v", workflowName, err)
		return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
	}
	var approvalTicket *commonmodels.ApprovalTicket
	if workflow.EnableApprovalTicket {
		approvalTicket, err = commonrepo.NewApprovalTicketColl().GetByID(ticketID)
		if err != nil {
			log.Errorf("cannot find approval ticket of id %s, the error is: %v", ticketID, err)
			return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
		}
	}

	for _, stage := range workflow.Stages {
		for _, job := range stage.Jobs {
			if err := jobctl.SetOptions(job, workflow, approvalTicket); err != nil {
				log.Errorf("cannot get workflow %s options for job %s, the error is: %v", workflowName, job.Name, err)
				return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
			}
			if err := jobctl.SetPreset(job, workflow); err != nil {
				log.Errorf("cannot get workflow %s preset for job %s, the error is: %v", workflowName, job.Name, err)
				return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
			}

			// for some job we need to clear its selection field
			if job.JobType == config.JobZadigBuild ||
				job.JobType == config.JobIstioRelease ||
				job.JobType == config.JobIstioRollback ||
				job.JobType == config.JobZadigHelmChartDeploy ||
				job.JobType == config.JobK8sBlueGreenDeploy ||
				job.JobType == config.JobApollo ||
				job.JobType == config.JobK8sCanaryDeploy ||
				job.JobType == config.JobK8sGrayRelease {
				if err := jobctl.ClearSelectionField(job, workflow); err != nil {
					log.Errorf("cannot clear workflow %s selection for job %s, the error is: %v", workflowName, job.Name, err)
					return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
				}
			}
		}
	}

	if err := ensureWorkflowV4Resp(encryptedKey, workflow, log); err != nil {
		return workflow, err
	}
	clearWorkflowV4Triggers(workflow)
	return workflow, nil
}

func GetWorkflowV4DynamicVariableAvailable(ctx *internalhandler.Context, workflow *commonmodels.WorkflowV4, jobName string) ([]string, error) {
	resp := make([]string, 0)

	variables, err := jobctl.GetRenderWorkflowVariables(ctx, workflow, jobName, "", "", true)
	if err != nil {
		err = fmt.Errorf("Failed to get render workflow variables, error: %v", err)
		ctx.Logger.Error(err)
		return nil, err
	}

	for _, kv := range variables {
		resp = append(resp, kv.Key)
	}

	return resp, nil
}

func RenderWorkflowV4Variables(ctx *internalhandler.Context, workflow *commonmodels.WorkflowV4, jobName, serviceName, moduleName, key string) ([]string, error) {
	resp := make([]string, 0)

	variables, err := jobctl.GetRenderWorkflowVariables(ctx, workflow, jobName, serviceName, moduleName, false)
	if err != nil {
		err = fmt.Errorf("Failed to get render workflow variables, error: %v", err)
		ctx.Logger.Error(err)
		return nil, err
	}

	buildInVarMap := make(map[string]string)
	for _, kv := range variables {
		kv.Key = strings.ReplaceAll(kv.Key, "-", "_")
		buildInVarMap[kv.Key] = kv.Value
	}

	resp, err = jobctl.RenderWorkflowVariables(ctx, workflow, jobName, serviceName, moduleName, key, buildInVarMap)
	if err != nil {
		err = fmt.Errorf("Failed to render workflow variables, error: %v", err)
		ctx.Logger.Error(err)
		return nil, err
	}

	return resp, nil
}

// CheckWorkflowV4ApprovalInitiator check if the workflow contains lark or dingtalk approval
// if so, check whether the IM information can be queried by the user's mobile phone number
func CheckWorkflowV4ApprovalInitiator(workflowName, uid string, log *zap.SugaredLogger) error {
	workflow, err := commonrepo.NewWorkflowV4Coll().Find(workflowName)
	if err != nil {
		log.Errorf("cannot find workflow %s, the error is: %v", workflowName, err)
		return e.ErrFindWorkflow.AddErr(err)
	}
	userInfo, err := user.New().GetUserByID(uid)
	if err != nil || userInfo == nil {
		return errors.New("failed to get user info by id")
	}

	if len(userInfo.Phone) == 0 {
		return e.ErrCheckApprovalPhoneNotFound.AddDesc("phone not configured")
	}

	// If default approval initiator is not set, check whether the user's mobile phone number can be queried
	// and only need to check once for each im app type
	isMobileChecked := map[string]bool{}
	for _, stage := range workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType == config.JobApproval {
				spec := new(commonmodels.ApprovalJobSpec)
				err := commonmodels.IToi(job.Spec, spec)
				if err != nil {
					return fmt.Errorf("failed to decode job: %s, error: %s", job.Name, err)
				}

				switch spec.Type {
				case config.LarkApproval:
					if spec.LarkApproval == nil {
						continue
					}
					cli, err := lark.GetLarkClientByIMAppID(spec.LarkApproval.ID)
					if err != nil {
						return errors.Errorf("failed to get lark app info by id-%s", spec.LarkApproval.ID)
					}

					if initiator := spec.LarkApproval.DefaultApprovalInitiator; initiator == nil {
						if isMobileChecked[spec.LarkApproval.ID] {
							continue
						}
						_, err = cli.GetUserIDByEmailOrMobile(larktool.QueryTypeMobile, userInfo.Phone, setting.LarkUserOpenID)
						if err != nil {
							return e.ErrCheckApprovalInitiator.AddDesc(fmt.Sprintf("lark app id: %s, phone: %s, error: %v",
								spec.LarkApproval.ID, userInfo.Phone, err))
						}
						isMobileChecked[spec.LarkApproval.ID] = true
					}
				case config.DingTalkApproval:
					if spec.DingTalkApproval == nil {
						continue
					}
					cli, err := dingtalk.GetDingTalkClientByIMAppID(spec.DingTalkApproval.ID)
					if err != nil {
						return errors.Errorf("failed to get dingtalk app info by id-%s", spec.DingTalkApproval.ID)
					}

					if initiator := spec.DingTalkApproval.DefaultApprovalInitiator; initiator == nil {
						if isMobileChecked[spec.DingTalkApproval.ID] {
							continue
						}
						_, err = cli.GetUserIDByMobile(userInfo.Phone)
						if err != nil {
							return e.ErrCheckApprovalInitiator.AddDesc(fmt.Sprintf("dingtalk app id: %s, phone: %s, error: %v",
								spec.DingTalkApproval.ID, userInfo.Phone, err))
						}
						isMobileChecked[spec.DingTalkApproval.ID] = true
					}
				case config.WorkWXApproval:
					if spec.WorkWXApproval == nil {
						continue
					}

					cli, err := workwxservice.GetLarkClientByIMAppID(spec.WorkWXApproval.ID)
					if err != nil {
						return errors.Errorf("failed to get dingtalk app info by id-%s", spec.DingTalkApproval.ID)
					}

					if initiator := spec.WorkWXApproval.CreatorUser; initiator == nil {
						if isMobileChecked[spec.DingTalkApproval.ID] {
							continue
						}
						phone, err := strconv.Atoi(userInfo.Phone)
						if err != nil {
							return e.ErrCheckApprovalInitiator.AddDesc("invalid phone number")
						}

						_, err = cli.FindUserByPhone(phone)
						if err != nil {
							return e.ErrCheckApprovalInitiator.AddDesc(fmt.Sprintf("workwx app id: %s, phone: %s, error: %v",
								spec.WorkWXApproval.ID, userInfo.Phone, err))
						}
						isMobileChecked[spec.DingTalkApproval.ID] = true
					}
				}
			}
		}
	}
	return nil
}

type CreateWorkflowTaskV4Args struct {
	Name             string
	Account          string
	UserID           string
	Type             config.CustomWorkflowTaskType
	ApprovalTicketID string
}

func CreateWorkflowTaskV4ByBuildInTrigger(triggerName string, args *commonmodels.WorkflowV4, log *zap.SugaredLogger) (*CreateTaskV4Resp, error) {
	resp := &CreateTaskV4Resp{
		ProjectName:  args.Project,
		WorkflowName: args.Name,
	}
	workflow, err := mongodb.NewWorkflowV4Coll().Find(args.Name)
	if err != nil {
		errMsg := fmt.Sprintf("cannot find workflow %s, the error is: %v", args.Name, err)
		log.Error(errMsg)
		return resp, e.ErrCreateTask.AddDesc(errMsg)
	}
	if err := job.MergeArgs(workflow, args); err != nil {
		errMsg := fmt.Sprintf("merge workflow args error: %v", err)
		log.Error(errMsg)
		return resp, e.ErrCreateTask.AddDesc(errMsg)
	}
	return CreateWorkflowTaskV4(&CreateWorkflowTaskV4Args{Name: triggerName}, workflow, log)
}

func CreateWorkflowTaskV4(args *CreateWorkflowTaskV4Args, workflow *commonmodels.WorkflowV4, log *zap.SugaredLogger) (*CreateTaskV4Resp, error) {
	resp := &CreateTaskV4Resp{
		ProjectName:  workflow.Project,
		WorkflowName: workflow.Name,
	}
	if err := LintWorkflowV4(workflow, log); err != nil {
		return resp, err
	}

	var userInfo *types.UserInfo
	var err error

	workflowTask := &commonmodels.WorkflowTask{}

	// if user info exists, get user email and put it to workflow task info
	if args.UserID != "" {
		userInfo, err = user.New().GetUserByID(args.UserID)
		if err != nil || userInfo == nil {
			return resp, errors.New("failed to get user info by uid")
		}
		workflowTask.TaskCreatorEmail = userInfo.Email
		workflowTask.TaskCreatorPhone = userInfo.Phone
	}

	if args.Type == config.WorkflowTaskTypeWorkflow || args.Type == "" {
		originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(workflow.Name)
		if err != nil {
			return resp, e.ErrCreateTask.AddErr(fmt.Errorf("cannot find workflow %s, error: %v", workflow.Name, err))
		}
		if originalWorkflow.Disabled {
			return resp, e.ErrCreateTask.AddDesc("workflow is disabled")
		}

		// do approval ticket check
		if originalWorkflow.EnableApprovalTicket {
			approvalTicket, err := commonrepo.NewApprovalTicketColl().GetByID(args.ApprovalTicketID)
			if err != nil {
				return nil, e.ErrCreateTask.AddErr(fmt.Errorf("cannot find approval ticket of id: %s, error: %s", args.ApprovalTicketID, err))
			}

			if approvalTicket.ProjectKey != originalWorkflow.Project {
				return resp, e.ErrCreateTask.AddDesc("workflow task creation denied: project key mismatch.")
			}

			// if it is not the correct time to run the workflow deny it
			if (approvalTicket.ExecutionWindowStart != 0 && time.Now().Unix() < approvalTicket.ExecutionWindowStart) ||
				(approvalTicket.ExecutionWindowEnd != 0 && time.Now().Unix() > approvalTicket.ExecutionWindowEnd) {
				return resp, e.ErrCreateTask.AddDesc("workflow task creation denied: not in execution time window.")
			}

			if len(approvalTicket.Users) != 0 {
				if args.UserID == "" {
					return resp, e.ErrCreateTask.AddDesc("workflow task creation denied: task creator cannot be identified.")
				}

				found := false
				for _, allowedUser := range approvalTicket.Users {
					if allowedUser.Email == userInfo.Email {
						found = true
						break
					}
				}

				if !found {
					return resp, e.ErrCreateTask.AddDesc(fmt.Sprintf("workflow task creation denied: user %s is not allowed to create workflow task.", userInfo.Name))
				}
			}

			workflowTask.ApprovalTicketID = args.ApprovalTicketID
			workflowTask.ApprovalID = approvalTicket.ApprovalID
		}

		workflowTask.Hash = originalWorkflow.Hash
	} else {
		if workflow.Disabled {
			return resp, e.ErrCreateTask.AddDesc("workflow is disabled")
		}
	}

	// if account is not set, use name as account
	if args.Account == "" {
		args.Account = args.Name
	}

	if err := jobctl.InstantiateWorkflow(workflow); err != nil {
		log.Errorf("instantiate workflow error: %s", err)
		return resp, e.ErrCreateTask.AddErr(err)
	}

	for _, stage := range workflow.Stages {
		for _, job := range stage.Jobs {
			err := jobctl.ClearOptions(job, workflow)
			if err != nil {
				log.Errorf("failed to remove the job options in the workflow parameters for job %s, stage: %s, error: %s", job.Name, stage.Name, err)
				return resp, fmt.Errorf("failed to remove the job options in the workflow parameters for job %s, stage: %s, error: %s", job.Name, stage.Name, err)
			}
		}
	}

	// save workflow original workflow task args.
	originTaskArgs := &commonmodels.WorkflowV4{}
	if err := commonmodels.IToi(workflow, originTaskArgs); err != nil {
		log.Errorf("save original workflow args error: %v", err)
		return resp, e.ErrCreateTask.AddDesc(err.Error())
	}
	originTaskArgs.HookCtls = nil
	originTaskArgs.MeegoHookCtls = nil
	originTaskArgs.JiraHookCtls = nil
	originTaskArgs.GeneralHookCtls = nil
	workflowTask.OriginWorkflowArgs = originTaskArgs
	nextTaskID, err := commonrepo.NewCounterColl().GetNextSeq(fmt.Sprintf(setting.WorkflowTaskV4Fmt, workflow.Name))
	if err != nil {
		log.Errorf("Counter.GetNextSeq error: %v", err)
		return resp, e.ErrGetCounter.AddDesc(err.Error())
	}
	resp.TaskID = nextTaskID

	if err := jobctl.RemoveFixedValueMarks(workflow); err != nil {
		log.Errorf("RemoveFixedValueMarks error: %v", err)
		return resp, e.ErrCreateTask.AddDesc(err.Error())
	}

	workflowTask.TaskID = nextTaskID
	workflowTask.TaskCreator = args.Name
	workflowTask.TaskCreatorID = args.UserID
	workflowTask.TaskRevoker = args.Name
	workflowTask.TaskRevokerID = args.UserID
	workflowTask.CreateTime = time.Now().Unix()
	workflowTask.WorkflowName = workflow.Name
	workflowTask.WorkflowDisplayName = workflow.DisplayName
	workflowTask.ProjectName = workflow.Project
	workflowTask.Params = workflow.Params
	workflowTask.ShareStorages = workflow.ShareStorages
	workflowTask.IsDebug = workflow.Debug
	workflowTask.Remark = workflow.Remark
	// set workflow params repo info, like commitid, branch etc.
	setZadigParamRepos(workflow, log)

	for _, stage := range workflow.Stages {
		stageTask := &commonmodels.StageTask{
			Name:       stage.Name,
			Parallel:   stage.Parallel,
			ManualExec: stage.ManualExec,
		}
		for _, job := range stage.Jobs {
			if jobctl.JobSkiped(job) {
				continue
			}
			// TODO: move this logic to job controller
			if job.JobType == config.JobZadigBuild {
				if err := setZadigBuildRepos(job, log); err != nil {
					log.Errorf("zadig build job set build info error: %v", err)
					return resp, e.ErrCreateTask.AddDesc(err.Error())
				}
			}
			if job.JobType == config.JobFreestyle {
				if err := setFreeStyleRepos(job, log); err != nil {
					log.Errorf("freestyle job set build info error: %v", err)
					return resp, e.ErrCreateTask.AddDesc(err.Error())
				}
			}
			if job.JobType == config.JobZadigTesting {
				if err := setZadigTestingRepos(job, log); err != nil {
					log.Errorf("testing job set build info error: %v", err)
					return resp, e.ErrCreateTask.AddDesc(err.Error())
				}
			}

			if job.JobType == config.JobZadigScanning {
				if err := setZadigScanningRepos(job, log); err != nil {
					log.Errorf("scanning job set build info error: %v", err)
					return resp, e.ErrCreateTask.AddDesc(err.Error())
				}
			}
		}

		if err := jobctl.RenderWorkflowParams(workflow, nextTaskID, args.Name, args.Account); err != nil {
			log.Errorf("RenderGlobalVariables error: %v", err)
			return resp, e.ErrCreateTask.AddDesc(err.Error())
		}

		for _, job := range stage.Jobs {
			if jobctl.JobSkiped(job) {
				continue
			}
			jobs, err := jobctl.ToJobs(job, workflow, nextTaskID)
			if err != nil {
				log.Errorf("cannot create workflow %s, the error is: %v", workflow.Name, err)
				return resp, e.ErrCreateTask.AddDesc(err.Error())
			}
			// add breakpoint_before when workflowTask is debug mode
			for _, jobTask := range jobs {
				switch config.JobType(jobTask.JobType) {
				case config.JobFreestyle, config.JobZadigTesting, config.JobZadigBuild, config.JobZadigScanning:
					if workflowTask.IsDebug {
						jobTask.BreakpointBefore = true
					}
				}
			}

			stageTask.Jobs = append(stageTask.Jobs, jobs...)
		}
		if len(stageTask.Jobs) > 0 {
			workflowTask.Stages = append(workflowTask.Stages, stageTask)
		}
	}

	if err := workflowTaskLint(workflowTask, log); err != nil {
		return resp, err
	}

	if err := createLarkApprovalDefinition(workflow); err != nil {
		return resp, errors.Wrap(err, "create lark approval definition")
	}

	workflow.HookCtls = nil
	workflow.JiraHookCtls = nil
	workflow.MeegoHookCtls = nil
	workflow.GeneralHookCtls = nil
	workflowTask.WorkflowArgs = workflow
	workflowTask.Status = config.StatusCreated
	workflowTask.StartTime = time.Now().Unix()
	workflowTask.Type = args.Type
	if args.Type == "" {
		workflowTask.Type = config.WorkflowTaskTypeWorkflow
	}

	workflowTask.WorkflowArgs, _, err = service.FillServiceModules2Jobs(workflowTask.WorkflowArgs)
	if err != nil {
		log.Errorf("fill serviceModules to jobs error: %v", err)
		return resp, e.ErrCreateTask.AddDesc(err.Error())
	}

	if err := instantmessage.NewWeChatClient().SendWorkflowTaskNotifications(workflowTask); err != nil {
		log.Errorf("send workflow task notification failed, error: %v", err)
	}

	if err := workflowcontroller.CreateTask(workflowTask); err != nil {
		log.Errorf("create workflow task error: %v", err)
		return resp, e.ErrCreateTask.AddDesc(err.Error())
	}
	// Updating the comment in the git repository, this will not cause the function to return error if this function call fails
	if err := scmnotify.NewService().UpdateWebhookCommentForWorkflowV4(workflowTask, log); err != nil {
		log.Warnf("Failed to update comment for custom workflow %s, taskID: %d the error is: %s", workflowTask.WorkflowName, workflowTask.TaskID, err)
	}
	if err := scmnotify.NewService().UpdateGitCheckForWorkflowV4(workflowTask.WorkflowArgs, workflowTask.TaskID, log); err != nil {
		log.Warnf("Failed to update github check status for custom workflow %s, taskID: %d the error is: %s", workflowTask.WorkflowName, workflowTask.TaskID, err)
	}

	return resp, nil
}

func GetManualExecWorkflowTaskV4Info(workflowName string, taskID int64, logger *zap.SugaredLogger) (*commonmodels.WorkflowV4, error) {
	originWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(workflowName)
	if err != nil {
		log.Errorf("find workflowV4 error: %s", err)
		return nil, e.ErrFindWorkflow.AddErr(err)
	}

	task, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("find workflowTaskV4 error: %s", err)
		return nil, e.ErrGetTask.AddErr(err)
	}

	var approvalTicket *commonmodels.ApprovalTicket
	if originWorkflow.EnableApprovalTicket {
		approvalTicket, err = commonrepo.NewApprovalTicketColl().GetByID(task.ApprovalTicketID)
		if err != nil {
			log.Errorf("cannot find approval ticket of id %s, the error is: %v", task.ApprovalTicketID, err)
			return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
		}
	}

	for _, stage := range task.OriginWorkflowArgs.Stages {
		for _, job := range stage.Jobs {
			if err := jobctl.SetOptions(job, task.WorkflowArgs, approvalTicket); err != nil {
				log.Errorf("cannot get workflow %s options for job %s, the error is: %v", workflowName, job.Name, err)
				return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
			}
		}
	}
	return task.OriginWorkflowArgs, nil
}

func CloneWorkflowTaskV4(workflowName string, taskID int64, isView bool, logger *zap.SugaredLogger) (*commonmodels.WorkflowV4, error) {
	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(workflowName)
	if err != nil {
		logger.Errorf("find workflowV4 error: %s", err)
		return nil, e.ErrFindWorkflow.AddErr(err)
	}

	if originalWorkflow.EnableApprovalTicket && !isView {
		return nil, e.ErrCloneTask.AddDesc("无法克隆开启了预审批的工作流")
	}

	task, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("find workflowTaskV4 error: %s", err)
		return nil, e.ErrGetTask.AddErr(err)
	}

	for _, stage := range task.OriginWorkflowArgs.Stages {
		for _, job := range stage.Jobs {
			if err := jobctl.SetOptions(job, task.OriginWorkflowArgs, nil); err != nil {
				log.Errorf("cannot get workflow %s options for job %s, the error is: %v", workflowName, job.Name, err)
				return nil, e.ErrPresetWorkflow.AddDesc(err.Error())
			}
		}
	}
	return task.OriginWorkflowArgs, nil
}

func RetryWorkflowTaskV4(workflowName string, taskID int64, logger *zap.SugaredLogger) error {
	task, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("find workflowTaskV4 error: %s", err)
		return e.ErrGetTask.AddErr(err)
	}
	switch task.Status {
	case config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject:
	default:
		return errors.New("工作流任务状态无法重试")
	}

	if task.OriginWorkflowArgs == nil || task.OriginWorkflowArgs.Stages == nil {
		return errors.New("工作流任务数据异常, 无法重试")
	}

	jobTaskMap := make(map[string]*commonmodels.JobTask)
	for _, stage := range task.WorkflowArgs.Stages {
		for _, job := range stage.Jobs {
			if job.Skipped {
				continue
			}
			jobCtl, err := jobctl.InitJobCtl(job, task.WorkflowArgs)
			if err != nil {
				return errors.Errorf("init jobCtl %s error: %s", job.Name, err)
			}
			jobTasks, err := jobCtl.ToJobs(taskID)
			if err != nil {
				return errors.Errorf("job %s toJobs error: %s", job.Name, err)
			}
			for _, jobTask := range jobTasks {
				jobTaskMap[jobTask.Name] = jobTask
			}
		}
	}

	for _, stage := range task.Stages {
		if stage.Status == config.StatusPassed || stage.Status == config.StatusSkipped {
			continue
		}
		stage.Status = ""
		stage.StartTime = 0
		stage.EndTime = 0
		stage.Error = ""

		for _, jobTask := range stage.Jobs {
			if jobTask.Status == config.StatusPassed {
				continue
			}
			jobTask.Status = ""
			jobTask.StartTime = 0
			jobTask.EndTime = 0
			jobTask.Error = ""
			if t, ok := jobTaskMap[jobTask.Name]; ok {
				jobTask.Spec = t.Spec
			} else {
				return errors.Errorf("failed to get jobTask %s origin spec", jobTask.Name)
			}
		}
	}

	task.Status = config.StatusCreated
	task.StartTime = time.Now().Unix()
	if err := instantmessage.NewWeChatClient().SendWorkflowTaskNotifications(task); err != nil {
		log.Errorf("send workflow task notification failed, error: %v", err)
	}

	if err := workflowcontroller.UpdateTask(task); err != nil {
		log.Errorf("retry workflow task error: %v", err)
		return e.ErrCreateTask.AddDesc(fmt.Sprintf("重试工作流任务失败: %s", err.Error()))
	}

	return nil
}

type ManualExecWorkflowTaskV4Request struct {
	Jobs []*commonmodels.Job `json:"jobs"`
}

func ManualExecWorkflowTaskV4(workflowName string, taskID int64, stageName string, jobs []*commonmodels.Job, executorID, executorName string, isSystemAdmin bool, logger *zap.SugaredLogger) error {
	task, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("find workflowTaskV4 error: %s", err)
		return e.ErrGetTask.AddErr(err)
	}
	switch task.Status {
	case config.StatusPause:
	default:
		return errors.New("工作流任务状态无法手动执行")
	}

	if task.OriginWorkflowArgs == nil || task.OriginWorkflowArgs.Stages == nil {
		return errors.New("工作流任务数据异常, 无法手动执行")
	}

	for _, stage := range task.WorkflowArgs.Stages {
		if stage.Name == stageName {
			for _, job := range stage.Jobs {
				err := jobctl.ClearOptions(job, task.WorkflowArgs)
				if err != nil {
					log.Errorf("failed to remove the job options in the workflow parameters for job %s, stage: %s, error: %s", job.Name, stage.Name, err)
					return fmt.Errorf("failed to remove the job options in the workflow parameters for job %s, stage: %s, error: %s", job.Name, stage.Name, err)
				}

				// TODO: move this logic to job controller
				if job.JobType == config.JobZadigBuild {
					if err := setZadigBuildRepos(job, logger); err != nil {
						log.Errorf("zadig build job set build info error: %v", err)
						return e.ErrCreateTask.AddDesc(err.Error())
					}
				}
				if job.JobType == config.JobFreestyle {
					if err := setFreeStyleRepos(job, logger); err != nil {
						log.Errorf("freestyle job set build info error: %v", err)
						return e.ErrCreateTask.AddDesc(err.Error())
					}
				}
				if job.JobType == config.JobZadigTesting {
					if err := setZadigTestingRepos(job, logger); err != nil {
						log.Errorf("testing job set build info error: %v", err)
						return e.ErrCreateTask.AddDesc(err.Error())
					}
				}

				if job.JobType == config.JobZadigScanning {
					if err := setZadigScanningRepos(job, logger); err != nil {
						log.Errorf("scanning job set build info error: %v", err)
						return e.ErrCreateTask.AddDesc(err.Error())
					}
				}
			}

			stage.Jobs = jobs
		}
	}
	for _, stage := range task.OriginWorkflowArgs.Stages {
		if stage.Name == stageName {
			stage.Jobs = jobs
		}
	}

	if err := jobctl.RemoveFixedValueMarks(task.OriginWorkflowArgs); err != nil {
		log.Errorf("RemoveFixedValueMarks error: %v", err)
		return e.ErrCreateTask.AddDesc(err.Error())
	}

	if err := jobctl.RenderWorkflowParams(task.OriginWorkflowArgs, task.TaskID, task.TaskCreator, task.TaskCreatorID); err != nil {
		log.Errorf("RenderGlobalVariables error: %v", err)
		return e.ErrCreateTask.AddDesc(err.Error())
	}

	// set workflow params repo info, like commitid, branch etc.
	setZadigParamRepos(task.WorkflowArgs, logger)

	foundStage := false
	newStageNameJobTasksMap := make(map[string][]*commonmodels.JobTask, 0)
	for _, stage := range task.WorkflowArgs.Stages {
		if stage.Name == stageName {
			foundStage = true
		}

		// is target stage and follow-up stages
		if foundStage {
			newStageNameJobTasksMap[stage.Name] = make([]*commonmodels.JobTask, 0)

			for _, job := range stage.Jobs {
				if job.Skipped {
					continue
				}

				jobCtl, err := jobctl.InitJobCtl(job, task.WorkflowArgs)
				if err != nil {
					return errors.Errorf("init jobCtl %s error: %s", job.Name, err)
				}
				jobTasks, err := jobCtl.ToJobs(taskID)
				if err != nil {
					return errors.Errorf("job %s toJobs error: %s", job.Name, err)
				}
				for _, jobTask := range jobTasks {
					jobTask.Status = ""
					jobTask.StartTime = 0
					jobTask.EndTime = 0
					jobTask.Error = ""

					if job.RunPolicy == config.SkipRun {
						jobTask.Status = config.StatusSkipped
					}
					newStageNameJobTasksMap[stage.Name] = append(newStageNameJobTasksMap[stage.Name], jobTask)
				}
			}
		}
	}

	found := false
	var preStage *commonmodels.StageTask
	for _, stage := range task.Stages {
		if stage.Status == config.StatusPassed || stage.Status == config.StatusSkipped {
			preStage = stage
			continue
		}

		// for the manual executed stage itself, we need to re-render the tasks, not getting them from the previous task since it might not be right
		if stage.Name == stageName {
			if preStage != nil && !(preStage.Status == config.StatusPassed || preStage.Status == config.StatusSkipped) {
				return errors.Errorf("previous stage %s status is not passed or skipped", preStage.Name)
			}

			if stage.ManualExec == nil || !stage.ManualExec.Enabled {
				return errors.Errorf("stage %s is not enabled for manual execution", stage.Name)
			}
			stage.ManualExec.Excuted = true

			approval := false
			if isSystemAdmin {
				approval = true
			}

			for _, user := range stage.ManualExec.ManualExecUsers {
				if user.Type == setting.UserTypeTaskCreator {
					if executorID == task.TaskCreatorID {
						approval = true
						break
					}
				}
			}
			if !approval {
				users, _ := util.GeneFlatUsers(stage.ManualExec.ManualExecUsers)
				for _, user := range users {
					if user.UserID == executorID {
						approval = true
						break
					}
				}
			}

			if !approval {
				return errors.Errorf("user %s is not allowed to manually execute stage %s", executorID, stage.Name)
			}

			found = true
			stage.ManualExec.ManualExectorID = executorID
			stage.ManualExec.ManualExectorName = executorName
		}

		if newStageNameJobTasksMap[stage.Name] != nil {
			stage.Status = ""
			stage.StartTime = 0
			stage.EndTime = 0
			stage.Error = ""
			stage.Jobs = newStageNameJobTasksMap[stage.Name]
		}

		preStage = stage
	}

	if !found {
		return errors.Errorf("stage %s not found in workflow %s or status is passed", stageName, workflowName)
	}

	if err := workflowTaskLint(task, logger); err != nil {
		return err
	}

	task.WorkflowArgs, _, err = service.FillServiceModules2Jobs(task.WorkflowArgs)
	if err != nil {
		log.Errorf("fill serviceModules to jobs error: %v", err)
		return e.ErrCreateTask.AddDesc(err.Error())
	}

	task.Status = config.StatusCreated
	if err := instantmessage.NewWeChatClient().SendWorkflowTaskNotifications(task); err != nil {
		log.Errorf("send workflow task notification failed, error: %v", err)
	}

	if err := workflowcontroller.UpdateTask(task); err != nil {
		log.Errorf("manual execute workflow task error: %v", err)
		return e.ErrCreateTask.AddDesc(fmt.Sprintf("手动执行工作流任务失败: %s", err.Error()))
	}

	return nil
}

func SetWorkflowTaskV4Breakpoint(workflowName, jobName string, taskID int64, set bool, position string, logger *zap.SugaredLogger) error {
	event := &workflowcontroller.WorkflowDebugEvent{
		EventType: workflowcontroller.WorkflowDebugEventSetBreakPoint,
		JobName:   jobName,
		TaskID:    taskID,
		Set:       set,
		Position:  position,
	}
	bytes, _ := json.Marshal(event)
	err := cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Publish(workflowcontroller.WorkflowDebugChanKey(workflowName, taskID), string(bytes))
	if err != nil {
		return e.ErrStopDebugShell.AddDesc(fmt.Sprintf("failed to set workflow breakpoint, err: %s", err))
	}
	return nil
}

func EnableDebugWorkflowTaskV4(workflowName string, taskID int64, logger *zap.SugaredLogger) error {
	event := &workflowcontroller.WorkflowDebugEvent{
		EventType: workflowcontroller.WorkflowDebugEventSetBreakPoint,
		TaskID:    taskID,
	}
	bytes, _ := json.Marshal(event)
	err := cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Publish(workflowcontroller.WorkflowDebugChanKey(workflowName, taskID), string(bytes))
	if err != nil {
		return e.ErrEnableDebug.AddDesc(fmt.Sprintf("failed to set workflow breakpoint, err: %s", err))
	}
	return nil
}

func StopDebugWorkflowTaskJobV4(workflowName, jobName string, taskID int64, position string, logger *zap.SugaredLogger) error {
	event := &workflowcontroller.WorkflowDebugEvent{
		EventType: workflowcontroller.WorkflowDebugEventDeleteDebug,
		JobName:   jobName,
		Position:  position,
		TaskID:    taskID,
	}
	bytes, _ := json.Marshal(event)
	err := cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Publish(workflowcontroller.WorkflowDebugChanKey(workflowName, taskID), string(bytes))
	if err != nil {
		return e.ErrEnableDebug.AddDesc(fmt.Sprintf("failed to set workflow breakpoint, err: %s", err))
	}
	return nil
}

type TaskHistoryFilter struct {
	PageSize     int64  `json:"page_size"    form:"page_size,default=20"`
	PageNum      int64  `json:"page_num"     form:"page_num,default=1"`
	WorkflowName string `json:"workflow_name" form:"workflow_name"`
	ProjectName  string `json:"projectName"  form:"projectName"`
	QueryType    string `json:"queryType"    form:"queryType"`
	Filters      string `json:"filters" form:"filters"`
	JobName      string `json:"jobName" form:"jobName"`
}

func ListWorkflowTaskV4ByFilter(filter *TaskHistoryFilter, filterList []string, logger *zap.SugaredLogger) ([]*commonmodels.WorkflowTaskPreview, int64, error) {
	var listTaskOpt *mongodb.WorkFlowTaskFilter
	switch filter.QueryType {
	case "creator":
		listTaskOpt = &mongodb.WorkFlowTaskFilter{
			WorkflowName: filter.WorkflowName,
			ProjectName:  filter.ProjectName,
			Creator:      filterList,
		}
	case "serviceName":
		listTaskOpt = &mongodb.WorkFlowTaskFilter{
			JobName:      filter.JobName,
			WorkflowName: filter.WorkflowName,
			ProjectName:  filter.ProjectName,
			Service:      filterList,
		}
	case "taskStatus":
		listTaskOpt = &mongodb.WorkFlowTaskFilter{
			WorkflowName: filter.WorkflowName,
			ProjectName:  filter.ProjectName,
			Status:       filterList,
		}
	case "envName":
		listTaskOpt = &mongodb.WorkFlowTaskFilter{
			JobName:      filter.JobName,
			WorkflowName: filter.WorkflowName,
			ProjectName:  filter.ProjectName,
			Env:          filterList,
		}
	default:
		listTaskOpt = &mongodb.WorkFlowTaskFilter{
			WorkflowName: filter.WorkflowName,
			ProjectName:  filter.ProjectName,
		}
	}
	tasks, total, err := commonrepo.NewworkflowTaskv4Coll().ListByFilter(listTaskOpt, filter.PageNum, filter.PageSize)
	if err != nil {
		logger.Errorf("list workflowTaskV4 error: %s", err)
		return nil, total, err
	}

	taskPreviews := make([]*commonmodels.WorkflowTaskPreview, 0)
	for _, task := range tasks {
		preview := &commonmodels.WorkflowTaskPreview{
			TaskID:              task.TaskID,
			TaskCreator:         task.TaskCreator,
			ProjectName:         task.ProjectName,
			WorkflowName:        task.WorkflowName,
			WorkflowDisplayName: task.WorkflowDisplayName,
			Remark:              task.Remark,
			Status:              task.Status,
			CreateTime:          task.CreateTime,
			StartTime:           task.StartTime,
			EndTime:             task.EndTime,
			Hash:                task.Hash,
		}

		stagePreviews := make([]*commonmodels.StagePreview, 0)
		for _, stage := range task.WorkflowArgs.Stages {
			stagePreview := &commonmodels.StagePreview{
				Name: stage.Name,
			}
			for _, job := range stage.Jobs {
				if job.Skipped {
					continue
				}
				jobPreview := &commonmodels.JobPreview{
					Name:    job.Name,
					JobType: string(job.JobType),
				}
				switch job.JobType {
				case config.JobZadigBuild:
					build := new(commonmodels.ZadigBuildJobSpec)
					if err := commonmodels.IToi(job.Spec, build); err != nil {
						return nil, 0, err
					}
					serviceModules := make([]*commonmodels.WorkflowServiceModule, 0)
					for _, serviceAndBuild := range build.ServiceAndBuilds {
						sm := &commonmodels.WorkflowServiceModule{
							ServiceWithModule: commonmodels.ServiceWithModule{
								ServiceName:   serviceAndBuild.ServiceName,
								ServiceModule: serviceAndBuild.ServiceModule,
							},
						}
						for _, repo := range serviceAndBuild.Repos {
							sm.CodeInfo = append(sm.CodeInfo, repo)
						}
						serviceModules = append(serviceModules, sm)
					}
					jobPreview.ServiceModules = serviceModules
				case config.JobZadigDeploy:
					deploy := new(commonmodels.ZadigDeployJobSpec)
					if err := commonmodels.IToi(job.Spec, deploy); err != nil {
						return nil, 0, err
					}
					serviceModules := make([]*commonmodels.WorkflowServiceModule, 0)
					for _, svc := range deploy.Services {
						for _, module := range svc.Modules {
							sm := &commonmodels.WorkflowServiceModule{
								ServiceWithModule: commonmodels.ServiceWithModule{
									ServiceName:   svc.ServiceName,
									ServiceModule: module.ServiceModule,
								},
							}
							serviceModules = append(serviceModules, sm)
						}
					}
					jobPreview.ServiceModules = serviceModules
					jobPreview.Envs = &commonmodels.WorkflowEnv{
						EnvName:    deploy.Env,
						Production: deploy.Production,
					}
				case config.JobZadigTesting:
					test := new(commonmodels.ZadigTestingJobSpec)
					if err := commonmodels.IToi(job.Spec, test); err != nil {
						return nil, 0, err
					}

					serviceModules := make([]*commonmodels.WorkflowServiceModule, 0)
					for _, service := range test.ServiceAndTests {
						sm := &commonmodels.WorkflowServiceModule{
							ServiceWithModule: commonmodels.ServiceWithModule{
								ServiceName:   service.ServiceName,
								ServiceModule: service.ServiceModule,
							},
						}
						for _, repo := range service.Repos {
							sm.CodeInfo = append(sm.CodeInfo, repo)
						}
						serviceModules = append(serviceModules, sm)
					}
					jobPreview.ServiceModules = serviceModules

					// get test report
					testModules := make([]*commonmodels.WorkflowTestModule, 0)
					testResultList, err := commonrepo.NewCustomWorkflowTestReportColl().ListByWorkflow(filter.WorkflowName, job.Name, task.TaskID)
					if err != nil {
						log.Errorf("failed to list junit test report for workflow: %s, error: %s", filter.WorkflowName, err)
						return nil, 0, fmt.Errorf("failed to list junit test report for workflow: %s, error: %s", filter.WorkflowName, err)
					}

					for _, testResult := range testResultList {
						testModules = append(testModules, &commonmodels.WorkflowTestModule{
							RunningJobName: job.Name,
							Type:           "function",
							TestName:       testResult.ZadigTestName,
							TestCaseNum:    testResult.TestCaseNum,
							SuccessCaseNum: testResult.SuccessCaseNum,
							TestTime:       testResult.TestTime,
						})
					}
					jobPreview.TestModules = testModules
				case config.JobZadigDistributeImage:
					distribute := new(commonmodels.ZadigDistributeImageJobSpec)
					if err := commonmodels.IToi(job.Spec, distribute); err != nil {
						return nil, 0, err
					}
					serviceModules := make([]*commonmodels.WorkflowServiceModule, 0)
					for _, target := range distribute.Targets {
						sm := &commonmodels.WorkflowServiceModule{
							ServiceWithModule: commonmodels.ServiceWithModule{
								ServiceName:   target.ServiceName,
								ServiceModule: target.ServiceModule,
							},
						}
						serviceModules = append(serviceModules, sm)
					}
				}
				stagePreview.Jobs = append(stagePreview.Jobs, jobPreview)
			}
			if len(stagePreview.Jobs) > 0 {
				stagePreviews = append(stagePreviews, stagePreview)
			}
		}

		for _, stage := range task.Stages {
			for _, stagePreview := range stagePreviews {
				if stagePreview.Name == stage.Name {
					stagePreview.Status = stage.Status
					stagePreview.StartTime = stage.StartTime
					stagePreview.EndTime = stage.EndTime
					stagePreview.ManualExec = stage.ManualExec
					stagePreview.Parallel = stage.Parallel
					stagePreview.Error = stage.Error
					break
				}
			}
		}
		preview.Stages = stagePreviews
		taskPreviews = append(taskPreviews, preview)
	}
	cleanWorkflowV4TasksPreviews(taskPreviews)
	return taskPreviews, total, nil
}

// clean extra message for list workflow
func cleanWorkflowV4TasksPreviews(workflows []*commonmodels.WorkflowTaskPreview) {
	const StatusNotRun = ""
	for _, workflow := range workflows {
		var stageList []*commonmodels.StagePreview
		workflow.WorkflowArgs = nil
		for _, stage := range workflow.Stages {
			stageList = append(stageList, stage)
		}
		workflow.Stages = stageList
	}
}

func getLatestWorkflowTaskV4(workflowName string) (*commonmodels.WorkflowTask, error) {
	resp, err := commonrepo.NewworkflowTaskv4Coll().GetLatest(workflowName)
	if err != nil {
		return nil, err
	}
	resp.WorkflowArgs = nil
	resp.OriginWorkflowArgs = nil
	resp.Stages = nil
	return resp, nil
}

func CancelWorkflowTaskV4(userName, workflowName string, taskID int64, logger *zap.SugaredLogger) error {
	if err := workflowcontroller.CancelWorkflowTask(userName, workflowName, taskID, logger); err != nil {
		logger.Errorf("cancel workflowTaskV4 error: %s", err)
		return e.ErrCancelTask.AddErr(err)
	}
	return nil
}

func GetWorkflowTaskV4(workflowName string, taskID int64, logger *zap.SugaredLogger) (*WorkflowTaskPreview, error) {
	task, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("find workflowTaskV4 error: %s", err)
		return nil, err
	}
	resp := &WorkflowTaskPreview{
		TaskID:              task.TaskID,
		WorkflowName:        task.WorkflowName,
		WorkflowDisplayName: task.WorkflowDisplayName,
		ProjectName:         task.ProjectName,
		Remark:              task.Remark,
		Status:              task.Status,
		Params:              task.Params,
		TaskCreator:         task.TaskCreator,
		TaskRevoker:         task.TaskRevoker,
		CreateTime:          task.CreateTime,
		StartTime:           task.StartTime,
		EndTime:             task.EndTime,
		Error:               task.Error,
		IsRestart:           task.IsRestart,
		Debug:               task.IsDebug,
		ApprovalTicketID:    task.ApprovalTicketID,
		ApprovalID:          task.ApprovalID,
	}
	timeNow := time.Now().Unix()
	for _, stage := range task.Stages {
		resp.Stages = append(resp.Stages, &StageTaskPreview{
			Name:       stage.Name,
			Status:     stage.Status,
			StartTime:  stage.StartTime,
			EndTime:    stage.EndTime,
			Parallel:   stage.Parallel,
			ManualExec: stage.ManualExec,
			Jobs:       jobsToJobPreviews(stage.Jobs, task.GlobalContext, timeNow, task.ProjectName),
			Error:      stage.Error,
		})
	}
	return resp, nil
}

func ApproveStage(workflowName, jobName, userName, userID, comment string, taskID int64, approve bool, logger *zap.SugaredLogger) error {
	if workflowName == "" || jobName == "" || taskID == 0 {
		errMsg := fmt.Sprintf("can not find approved workflow: %s, taskID: %d,jobName: %s", workflowName, taskID, jobName)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}
	if err := workflowcontroller.ApproveStage(workflowName, jobName, userName, userID, comment, taskID, approve); err != nil {
		logger.Error(err)
		return e.ErrApproveTask.AddErr(err)
	}
	return nil
}

func HandleJobError(workflowName, jobName, userID, username string, taskID int64, decision workflowtool.JobErrorDecision, logger *zap.SugaredLogger) error {
	if workflowName == "" || jobName == "" || taskID == 0 {
		errMsg := fmt.Sprintf("can not find approved workflow: %s, taskID: %d,jobName: %s", workflowName, taskID, jobName)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}
	workflowTask, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		errMsg := fmt.Sprintf("can not find workflow task: %s, taskID: %d to handle its error, err: %s", workflowName, taskID, err)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}

	found := false
	var errorJob *commonmodels.JobTask
	for _, stage := range workflowTask.Stages {
		if found {
			break
		}
		for _, job := range stage.Jobs {
			if job.Name == jobName {
				found = true
				errorJob = job
				break
			}
		}
	}

	if !found {
		errMsg := fmt.Sprintf("can not find job %s in workflow task: %s, taskID: %d to handle its error, err: %s", jobName, workflowName, taskID, err)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}

	if errorJob.ErrorPolicy == nil || errorJob.ErrorPolicy.Policy != config.JobErrorPolicyManualCheck {
		errMsg := fmt.Sprintf("error policy for job: %s is %s", jobName, errorJob.ErrorPolicy.Policy)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}

	_, userMap := util.GeneFlatUsersWithCaller(errorJob.ErrorPolicy.ApprovalUsers, userID)

	if _, ok := userMap[userID]; !ok {
		errMsg := fmt.Sprintf("user %s is not authorized to perform error handling", username)
		logger.Error(errMsg)
		return e.ErrApproveTask.AddDesc(errMsg)
	}

	if err := workflowtool.SetJobErrorHandlingDecision(workflowName, jobName, taskID, decision, userID, username); err != nil {
		logger.Error(err)
		return e.ErrApproveTask.AddErr(err)
	}
	return nil
}

func jobsToJobPreviews(jobs []*commonmodels.JobTask, context map[string]string, now int64, projectName string) []*JobTaskPreview {
	resp := []*JobTaskPreview{}

	envMap := make(map[string]*commonmodels.Product)
	getEnvProduction := func(envName string) bool {
		if env, ok := envMap[envName]; ok {
			return env.Production
		}
		envInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: projectName, EnvName: envName})
		if err != nil {
			log.Errorf("failed to get env production %s/%s, error : %v", projectName, envName, err)
			return false
		}
		envMap[envName] = envInfo
		return envInfo.Production
	}

	for _, job := range jobs {
		costSeconds := int64(0)
		if job.StartTime != 0 {
			costSeconds = now - job.StartTime
			if job.EndTime != 0 {
				costSeconds = job.EndTime - job.StartTime
			}
		}
		jobPreview := &JobTaskPreview{
			Name:                 job.Name,
			Key:                  job.Key,
			OriginName:           job.OriginName,
			DisplayName:          job.DisplayName,
			Status:               job.Status,
			StartTime:            job.StartTime,
			EndTime:              job.EndTime,
			Error:                job.Error,
			JobType:              job.JobType,
			BreakpointBefore:     job.BreakpointBefore,
			BreakpointAfter:      job.BreakpointAfter,
			CostSeconds:          costSeconds,
			JobInfo:              job.JobInfo,
			ErrorPolicy:          job.ErrorPolicy,
			ErrorHandlerUserID:   job.ErrorHandlerUserID,
			ErrorHandlerUserName: job.ErrorHandlerUserName,
			RetryCount:           job.RetryCount,
		}
		switch job.JobType {
		case string(config.JobFreestyle):
			fallthrough
		case string(config.JobZadigBuild):
			spec := ZadigBuildJobSpec{}
			taskJobSpec := &commonmodels.JobTaskFreestyleSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			for _, arg := range taskJobSpec.Properties.Envs {
				if arg.Key == "SERVICE_NAME" {
					spec.ServiceName = arg.Value
					continue
				}
				if arg.Key == "SERVICE_MODULE" {
					spec.ServiceModule = arg.Value
					continue
				}
			}

			// get from global context
			imageContextKey := workflowcontroller.GetContextKey(jobspec.GetJobOutputKey(job.Key, "IMAGE"))
			if context != nil {
				spec.Image = context[imageContextKey]
			}

			spec.Envs = taskJobSpec.Properties.CustomEnvs
			for _, step := range taskJobSpec.Steps {
				if step.StepType == config.StepGit {
					stepSpec := &stepspec.StepGitSpec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				} else if step.StepType == config.StepPerforce {
					stepSpec := &stepspec.StepP4Spec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				}
				if step.StepType == config.StepArchive && strings.HasSuffix(step.Name, "-pkgfile-archive") {
					stepSpec := &stepspec.StepArchiveSpec{}
					if err := commonmodels.IToi(step.Spec, &stepSpec); err != nil {
						continue
					}

					if len(stepSpec.UploadDetail) > 0 {
						spec.Package = stepSpec.UploadDetail[len(stepSpec.UploadDetail)-1].DestinationPath + "/" + stepSpec.UploadDetail[len(stepSpec.UploadDetail)-1].Name
					}
				}
			}
			jobPreview.Spec = spec
		case string(config.JobZadigDistributeImage):
			spec := &DistributeImageJobSpec{}
			taskJobSpec := &commonmodels.JobTaskFreestyleSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}

			for _, step := range taskJobSpec.Steps {
				if step.StepType == config.StepDistributeImage {
					stepSpec := &stepspec.StepImageDistributeSpec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					spec.DistributeTarget = stepSpec.DistributeTarget
					break
				}
			}
			jobPreview.Spec = spec
		case string(config.JobZadigTesting):
			spec := &ZadigTestingJobSpec{}
			jobPreview.Spec = spec
			taskJobSpec := &commonmodels.JobTaskFreestyleSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Envs = taskJobSpec.Properties.CustomEnvs
			for _, step := range taskJobSpec.Steps {
				if step.StepType == config.StepGit {
					stepSpec := &stepspec.StepGitSpec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				} else if step.StepType == config.StepPerforce {
					stepSpec := &stepspec.StepP4Spec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				}
			}
			for _, arg := range taskJobSpec.Properties.Envs {
				if arg.Key == "TESTING_PROJECT" {
					spec.ProjectName = arg.Value
					continue
				}
				if arg.Key == "TESTING_NAME" {
					spec.TestName = arg.Value
					continue
				}
				if arg.Key == "TESTING_TYPE" {
					spec.TestType = arg.Value
					continue
				}
				if arg.Key == "SERVICE_NAME" {
					spec.ServiceName = arg.Value
					continue
				}
				if arg.Key == "SERVICE_MODULE" {
					spec.ServiceModule = arg.Value
					continue
				}

			}
			if job.Status == config.StatusPassed || job.Status == config.StatusFailed {
				for _, step := range taskJobSpec.Steps {
					if step.Name == config.TestJobArchiveResultStepName {
						spec.Archive = true
					}
					if step.Name == config.TestJobHTMLReportStepName {
						spec.HtmlReport = true
					}
					if step.Name == config.TestJobJunitReportStepName {
						spec.JunitReport = true
					}
				}
			}
		case string(config.JobZadigScanning):
			spec := ZadigScanningJobSpec{}
			taskJobSpec := &commonmodels.JobTaskFreestyleSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Envs = taskJobSpec.Properties.CustomEnvs
			for _, step := range taskJobSpec.Steps {
				if step.Name == config.ScanningJobArchiveResultStepName {
					spec.IsHasArtifact = true
				}
				if step.StepType == config.StepGit {
					stepSpec := &stepspec.StepGitSpec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				}

				if step.StepType == config.StepPerforce {
					stepSpec := &stepspec.StepP4Spec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					if spec.Repos == nil {
						spec.Repos = make([]*types.Repository, 0)
					}
					spec.Repos = append(spec.Repos, stepSpec.Repos...)
					continue
				}

				if step.StepType == config.StepSonarGetMetrics {
					stepSpec := &stepspec.StepSonarGetMetricsSpec{}
					commonmodels.IToi(step.Spec, &stepSpec)
					spec.SonarMetrics = stepSpec.SonarMetrics
					continue
				}
			}
			sonarURL := ""
			for _, arg := range taskJobSpec.Properties.Envs {
				if arg.Key == "SONAR_LINK" {
					spec.LinkURL = arg.Value
					continue
				}
				if arg.Key == "SCANNING_NAME" {
					spec.ScanningName = arg.Value
					continue
				}

				if arg.Key == "SCANNING_TYPE" {
					spec.TestType = arg.Value
					continue
				}
				if arg.Key == "SERVICE_NAME" {
					spec.ServiceName = arg.Value
					continue
				}
				if arg.Key == "SERVICE_MODULE" {
					spec.ServiceModule = arg.Value
					continue
				}
				if arg.Key == "SONAR_URL" {
					sonarURL = arg.Value
					continue
				}
			}
			if spec.LinkURL == "" {
				if sonarURL != "" {
					projectKey := ""
					projectScanningOutputKey := jobspec.GetJobOutputKey(job.Key, setting.WorkflowScanningJobOutputKeyProject)
					projectScanningOutputKey = workflowcontroller.GetContextKey(projectScanningOutputKey)
					if context[projectScanningOutputKey] != "" {
						projectKey = context[projectScanningOutputKey]
					}

					branch := ""
					branchScanningOutputKey := jobspec.GetJobOutputKey(job.Key, setting.WorkflowScanningJobOutputKeyBranch)
					branchScanningOutputKey = workflowcontroller.GetContextKey(branchScanningOutputKey)
					if context[branchScanningOutputKey] != "" {
						branch = context[branchScanningOutputKey]
					}

					resultAddr, err := sonar.GetSonarAddress(sonarURL, projectKey, branch)
					if err != nil {
						log.Errorf("failed to get sonar address with project key %s, error: %v", projectKey, err)
						continue
					}
					spec.LinkURL = resultAddr
				} else {
					log.Errorf("failed to get sonar url from job task's env")
				}
			}
			jobPreview.Spec = spec
		case string(config.JobZadigDeploy):
			spec := ZadigDeployJobPreviewSpec{}
			taskJobSpec := &commonmodels.JobTaskDeploySpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Env = taskJobSpec.Env
			spec.Production = getEnvProduction(taskJobSpec.Env)
			spec.VariableConfigs = taskJobSpec.VariableConfigs
			spec.VariableKVs = taskJobSpec.VariableKVs
			spec.YamlContent = taskJobSpec.YamlContent
			spec.SkipCheckRunStatus = taskJobSpec.SkipCheckRunStatus
			// for compatibility
			if taskJobSpec.ServiceModule != "" {
				spec.ServiceAndImages = append(spec.ServiceAndImages, &ServiceAndImage{
					ServiceName:   taskJobSpec.ServiceName,
					ServiceModule: taskJobSpec.ServiceModule,
					Image:         taskJobSpec.Image,
				})
			}

			for _, imageAndmodule := range taskJobSpec.ServiceAndImages {
				spec.ServiceAndImages = append(spec.ServiceAndImages, &ServiceAndImage{
					ServiceName:   taskJobSpec.ServiceName,
					ServiceModule: imageAndmodule.ServiceModule,
					Image:         imageAndmodule.Image,
				})
			}
			jobPreview.Spec = spec
		case string(config.JobZadigHelmDeploy):
			jobPreview.JobType = string(config.JobZadigDeploy)
			spec := ZadigDeployJobPreviewSpec{}
			job.JobType = string(config.JobZadigDeploy)
			taskJobSpec := &commonmodels.JobTaskHelmDeploySpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Env = taskJobSpec.Env
			spec.Production = getEnvProduction(taskJobSpec.Env)
			spec.YamlContent = taskJobSpec.YamlContent
			spec.UserSuppliedValue = taskJobSpec.UserSuppliedValue
			spec.SkipCheckRunStatus = taskJobSpec.SkipCheckRunStatus
			for _, imageAndmodule := range taskJobSpec.ImageAndModules {
				spec.ServiceAndImages = append(spec.ServiceAndImages, &ServiceAndImage{
					ServiceName:   taskJobSpec.ServiceName,
					ServiceModule: imageAndmodule.ServiceModule,
					Image:         imageAndmodule.Image,
				})
			}
			jobPreview.Spec = spec
		case string(config.JobZadigHelmChartDeploy):
			jobPreview.JobType = string(config.JobZadigHelmChartDeploy)
			spec := commonmodels.ZadigHelmChartDeployJobSpec{}
			job.JobType = string(config.JobZadigHelmChartDeploy)
			taskJobSpec := &commonmodels.JobTaskHelmChartDeploySpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Env = taskJobSpec.Env
			spec.SkipCheckRunStatus = taskJobSpec.SkipCheckRunStatus
			spec.DeployHelmCharts = append(spec.DeployHelmCharts, taskJobSpec.DeployHelmChart)
			jobPreview.Spec = spec
		case string(config.JobZadigVMDeploy):
			spec := commonmodels.ZadigVMDeployJobSpec{}
			taskJobSpec := &commonmodels.JobTaskFreestyleSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}

			serviceModule := ""
			serviceName := ""
			for _, arg := range taskJobSpec.Properties.Envs {
				if arg.Key == "ENV_NAME" {
					spec.Env = arg.Value
					continue
				}
				if arg.Key == "SERVICE_MODULE" {
					serviceModule = arg.Value
				}
				if arg.Key == "SERVICE_NAME" {
					serviceName = arg.Value
				}
			}

			serviceAndVMDeploy := []*commonmodels.ServiceAndVMDeploy{}
			for _, step := range taskJobSpec.Steps {
				if step.StepType == config.StepDownloadArchive {
					stepSpec := &stepspec.StepDownloadArchiveSpec{}
					if err := commonmodels.IToi(step.Spec, &stepSpec); err != nil {
						continue
					}

					url := stepSpec.S3.Endpoint + "/" + stepSpec.S3.Bucket + "/"
					if len(stepSpec.S3.Subfolder) > 0 {
						url += strings.TrimLeft(stepSpec.S3.Subfolder, "/")
					}
					url += "/" + stepSpec.FileName
					serviceAndVMDeploy = append(serviceAndVMDeploy, &commonmodels.ServiceAndVMDeploy{
						ServiceName:   serviceName,
						ServiceModule: serviceModule,
						ArtifactURL:   url,
					})
				}
			}
			spec.ServiceAndVMDeploys = serviceAndVMDeploy

			jobPreview.Spec = spec
		case string(config.JobPlugin):
			taskJobSpec := &commonmodels.JobTaskPluginSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			jobPreview.Spec = taskJobSpec.Plugin
		case string(config.JobCustomDeploy):
			spec := CustomDeployJobSpec{}
			taskJobSpec := &commonmodels.JobTaskCustomDeploySpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			spec.Image = taskJobSpec.Image
			spec.Namespace = taskJobSpec.Namespace
			spec.SkipCheckRunStatus = taskJobSpec.SkipCheckRunStatus
			spec.Target = strings.Join([]string{taskJobSpec.WorkloadType, taskJobSpec.WorkloadName, taskJobSpec.ContainerName}, "/")
			cluster, err := commonrepo.NewK8SClusterColl().Get(taskJobSpec.ClusterID)
			if err != nil {
				log.Errorf("cluster id: %s not found", taskJobSpec.ClusterID)
			} else {
				spec.ClusterName = cluster.Name
			}
			jobPreview.Spec = spec
		case string(config.JobK8sCanaryDeploy):
			taskJobSpec := &commonmodels.JobTaskCanaryDeploySpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			sepc := K8sCanaryDeployJobSpec{
				Image:          taskJobSpec.Image,
				K8sServiceName: taskJobSpec.K8sServiceName,
				Namespace:      taskJobSpec.Namespace,
				ContainerName:  taskJobSpec.ContainerName,
				CanaryReplica:  taskJobSpec.CanaryReplica,
				Events:         taskJobSpec.Events,
			}
			cluster, err := commonrepo.NewK8SClusterColl().Get(taskJobSpec.ClusterID)
			if err != nil {
				log.Errorf("cluster id: %s not found", taskJobSpec.ClusterID)
			} else {
				sepc.ClusterName = cluster.Name
			}
			jobPreview.Spec = sepc
		case string(config.JobK8sCanaryRelease):
			taskJobSpec := &commonmodels.JobTaskCanaryReleaseSpec{}
			if err := commonmodels.IToi(job.Spec, taskJobSpec); err != nil {
				continue
			}
			sepc := K8sCanaryReleaseJobSpec{
				Image:          taskJobSpec.Image,
				K8sServiceName: taskJobSpec.K8sServiceName,
				Namespace:      taskJobSpec.Namespace,
				ContainerName:  taskJobSpec.ContainerName,
				Events:         taskJobSpec.Events,
			}
			cluster, err := commonrepo.NewK8SClusterColl().Get(taskJobSpec.ClusterID)
			if err != nil {
				log.Errorf("cluster id: %s not found", taskJobSpec.ClusterID)
			} else {
				sepc.ClusterName = cluster.Name
			}
			jobPreview.Spec = sepc
		default:
			jobPreview.Spec = job.Spec
		}
		resp = append(resp, jobPreview)
	}
	return resp
}

func setZadigParamRepos(workflow *commonmodels.WorkflowV4, logger *zap.SugaredLogger) {
	for _, param := range workflow.Params {
		if param.ParamsType != "repo" {
			continue
		}
		setBuildInfo(param.Repo, []*types.Repository{param.Repo}, logger)
	}
}

func setZadigBuildRepos(job *commonmodels.Job, logger *zap.SugaredLogger) error {
	spec := &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(job.Spec, spec); err != nil {
		return err
	}
	for _, build := range spec.ServiceAndBuilds {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	job.Spec = spec
	return nil
}

func setZadigTestingRepos(job *commonmodels.Job, logger *zap.SugaredLogger) error {
	spec := &commonmodels.ZadigTestingJobSpec{}
	if err := commonmodels.IToi(job.Spec, spec); err != nil {
		return err
	}
	for _, build := range spec.TestModules {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	for _, build := range spec.ServiceAndTests {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	job.Spec = spec
	return nil
}

func setZadigScanningRepos(job *commonmodels.Job, logger *zap.SugaredLogger) error {
	spec := &commonmodels.ZadigScanningJobSpec{}
	if err := commonmodels.IToi(job.Spec, spec); err != nil {
		return err
	}
	for _, build := range spec.Scannings {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	for _, build := range spec.ServiceAndScannings {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	job.Spec = spec
	return nil
}

func setFreeStyleRepos(job *commonmodels.Job, logger *zap.SugaredLogger) error {
	spec := &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(job.Spec, spec); err != nil {
		return err
	}
	for _, build := range spec.Services {
		if err := setManunalBuilds(build.Repos, build.Repos, logger); err != nil {
			return err
		}
	}
	for _, step := range spec.Steps {
		if step.StepType == config.StepGit {
			stepSpec := &stepspec.StepGitSpec{}
			if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
				return err
			}
			if err := setManunalBuilds(stepSpec.Repos, stepSpec.Repos, logger); err != nil {
				return err
			}
			step.Spec = stepSpec
		} else if step.StepType == config.StepPerforce {
			stepSpec := &stepspec.StepP4Spec{}
			if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
				return err
			}
			if err := setManunalBuilds(stepSpec.Repos, stepSpec.Repos, logger); err != nil {
				return err
			}
			step.Spec = stepSpec
		} else {
			continue
		}
	}
	job.Spec = spec
	return nil
}

func workflowTaskLint(workflowTask *commonmodels.WorkflowTask, logger *zap.SugaredLogger) error {
	if len(workflowTask.Stages) <= 0 {
		errMsg := fmt.Sprintf("no stage found in workflow task: %s,taskID: %d", workflowTask.WorkflowName, workflowTask.TaskID)
		logger.Error(errMsg)
		return e.ErrCreateTask.AddDesc(errMsg)
	}
	for _, stage := range workflowTask.Stages {
		if len(stage.Jobs) <= 0 {
			errMsg := fmt.Sprintf("no job found in workflow task: %s,taskID: %d,stage: %s", workflowTask.WorkflowName, workflowTask.TaskID, stage.Name)
			logger.Error(errMsg)
			return e.ErrCreateTask.AddDesc(errMsg)
		}

		for _, job := range stage.Jobs {
			if job.JobType == string(config.JobApproval) {
				spec := &commonmodels.JobTaskApprovalSpec{}
				err := commonmodels.IToi(job.Spec, spec)
				if err != nil {
					logger.Errorf("failed to update approval job user info, error: %s", err)
					return e.ErrCreateTask.AddDesc(fmt.Sprintf("failed to update approval job user info, error: %s", err))
				}

				if spec.Type == config.NativeApproval && spec.NativeApproval != nil && len(spec.NativeApproval.ApproveUsers) != 0 {
					newApproveUserList := make([]*commonmodels.User, 0)
					userSet := sets.NewString()
					for _, approveUser := range spec.NativeApproval.ApproveUsers {
						if approveUser.Type == "" || approveUser.Type == "user" {
							newApproveUserList = append(newApproveUserList, approveUser)
							userSet.Insert(approveUser.UserID)
						}
					}
					for _, approveUser := range spec.NativeApproval.ApproveUsers {
						if approveUser.Type == "group" {
							users, err := user.New().GetGroupDetailedInfo(approveUser.GroupID)
							if err != nil {
								errMsg := fmt.Sprintf("failed to find users for group %s in stage: %s, error: %s", approveUser.GroupName, stage.Name, err)
								logger.Errorf(errMsg)
								return e.ErrCreateTask.AddDesc(errMsg)
							}
							for _, userID := range users.UIDs {
								if userSet.Has(userID) {
									continue
								}
								userDetailedInfo, err := user.New().GetUserByID(userID)
								if err != nil {
									errMsg := fmt.Sprintf("failed to find user %s, error: %s", userID, err)
									logger.Errorf(errMsg)
									return e.ErrCreateTask.AddDesc(errMsg)
								}

								userSet.Insert(userID)
								newApproveUserList = append(newApproveUserList, &commonmodels.User{
									Type:     "user",
									UserID:   userID,
									UserName: userDetailedInfo.Name,
								})
							}
						}
					}
					spec.NativeApproval.ApproveUsers = newApproveUserList
				}

				job.Spec = spec
			}
		}
	}
	return nil
}

func GetWorkflowV4ArtifactFileContent(workflowName, jobName string, taskID int64, log *zap.SugaredLogger) ([]byte, error) {
	workflowTask, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		return []byte{}, fmt.Errorf("cannot find workflow task, workflow name: %s, task id: %d", workflowName, taskID)
	}
	var jobTask *commonmodels.JobTask
	for _, stage := range workflowTask.Stages {
		for _, job := range stage.Jobs {
			if job.Name != jobName {
				continue
			}
			if job.JobType != string(config.JobZadigTesting) && job.JobType != string(config.JobZadigScanning) {
				return []byte{}, fmt.Errorf("job: %s was not a testing or scanning job", jobName)
			}

			jobTask = job
		}
	}
	if jobTask == nil {
		return []byte{}, fmt.Errorf("cannot find job task, workflow name: %s, task id: %d, job name: %s", workflowName, taskID, jobName)
	}
	jobSpec := &commonmodels.JobTaskFreestyleSpec{}
	if err := commonmodels.IToi(jobTask.Spec, jobSpec); err != nil {
		return []byte{}, fmt.Errorf("unmashal job spec error: %v", err)
	}

	var stepTask *commonmodels.StepTask
	for _, step := range jobSpec.Steps {
		if step.Name != config.TestJobArchiveResultStepName {
			continue
		}
		if step.StepType != config.StepTarArchive {
			return []byte{}, fmt.Errorf("step: %s was not a junit report step", step.Name)
		}
		stepTask = step
	}
	if stepTask == nil {
		return []byte{}, fmt.Errorf("cannot find step task, workflow name: %s, task id: %d, job name: %s", workflowName, taskID, jobName)
	}
	stepSpec := &step.StepTarArchiveSpec{}
	if err := commonmodels.IToi(stepTask.Spec, stepSpec); err != nil {
		return []byte{}, fmt.Errorf("unmashal step spec error: %v", err)
	}

	storage, err := s3.FindDefaultS3()
	if err != nil {
		log.Errorf("GetTestArtifactInfo FindDefaultS3 err:%v", err)
		return []byte{}, fmt.Errorf("findDefaultS3 err: %v", err)
	}
	client, err := s3tool.NewClient(storage.Endpoint, storage.Ak, storage.Sk, storage.Region, storage.Insecure, storage.Provider)
	if err != nil {
		log.Errorf("GetTestArtifactInfo Create S3 client err:%+v", err)
		return []byte{}, fmt.Errorf("create S3 client err: %v", err)
	}
	objectKey := filepath.Join(stepSpec.S3DestDir, stepSpec.FileName)
	object, err := client.GetFile(storage.Bucket, objectKey, &s3tool.DownloadOption{RetryNum: 2})
	if err != nil {
		log.Errorf("GetTestArtifactInfo GetFile err:%s", err)
		return []byte{}, fmt.Errorf("GetFile err: %v", err)
	}
	fileByts, err := ioutil.ReadAll(object.Body)
	if err != nil {
		log.Errorf("GetTestArtifactInfo ioutil.ReadAll err:%s", err)
		return []byte{}, fmt.Errorf("ioutil.ReadAll err: %v", err)
	}
	return fileByts, nil
}

func GetWorkflowV4BuildJobArtifactFile(workflowName, jobName string, taskID int64, log *zap.SugaredLogger) ([]byte, string, error) {
	workflowTask, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		return []byte{}, "", fmt.Errorf("cannot find workflow task, workflow name: %s, task id: %d", workflowName, taskID)
	}
	var jobTask *commonmodels.JobTask
	for _, stage := range workflowTask.Stages {
		for _, job := range stage.Jobs {
			if job.Name != jobName {
				continue
			}
			if job.JobType != string(config.JobZadigBuild) {
				return []byte{}, "", fmt.Errorf("job: %s was not a build job", jobName)
			}

			jobTask = job
		}
	}
	if jobTask == nil {
		return []byte{}, "", fmt.Errorf("cannot find job task, workflow name: %s, task id: %d, job name: %s", workflowName, taskID, jobName)
	}
	jobSpec := &commonmodels.JobTaskFreestyleSpec{}
	if err := commonmodels.IToi(jobTask.Spec, jobSpec); err != nil {
		return []byte{}, "", fmt.Errorf("unmashal job spec error: %v", err)
	}

	var stepTask *commonmodels.StepTask
	for _, step := range jobSpec.Steps {
		if !strings.HasSuffix(step.Name, "-pkgfile-archive") {
			continue
		}
		if step.StepType != config.StepArchive {
			return []byte{}, "", fmt.Errorf("step: %s was not a archive step", step.Name)
		}
		stepTask = step
	}
	if stepTask == nil {
		return []byte{}, "", fmt.Errorf("cannot find step task, workflow name: %s, task id: %d, job name: %s", workflowName, taskID, jobName)
	}
	stepSpec := &step.StepArchiveSpec{}
	if err := commonmodels.IToi(stepTask.Spec, stepSpec); err != nil {
		return []byte{}, "", fmt.Errorf("unmashal step spec error: %v", err)
	}
	if len(stepSpec.UploadDetail) == 0 {
		return []byte{}, "", fmt.Errorf("step: %s has no upload detail", stepTask.Name)
	}

	storage, err := s3.FindDefaultS3()
	if err != nil {
		log.Errorf("GetWorkflowV4BuildJobArtifactFile FindDefaultS3 err:%v", err)
		return []byte{}, "", fmt.Errorf("findDefaultS3 err: %v", err)
	}
	client, err := s3tool.NewClient(storage.Endpoint, storage.Ak, storage.Sk, storage.Region, storage.Insecure, storage.Provider)
	if err != nil {
		log.Errorf("GetWorkflowV4BuildJobArtifactFile Create S3 client err:%+v", err)
		return []byte{}, "", fmt.Errorf("create S3 client err: %v", err)
	}

	stepSpec.UploadDetail[0].DestinationPath = strings.TrimLeft(path.Join(stepSpec.S3.Subfolder, stepSpec.UploadDetail[0].DestinationPath), "/")
	objectKey := filepath.Join(stepSpec.UploadDetail[0].DestinationPath, stepSpec.UploadDetail[0].Name)
	object, err := client.GetFile(storage.Bucket, objectKey, &s3tool.DownloadOption{RetryNum: 2})
	if err != nil {
		log.Errorf("GetWorkflowV4BuildJobArtifactFile GetFile err:%s", err)
		return []byte{}, "", fmt.Errorf("GetFile err: %v", err)
	}
	fileByts, err := ioutil.ReadAll(object.Body)
	if err != nil {
		log.Errorf("GetWorkflowV4BuildJobArtifactFile ioutil.ReadAll err:%s", err)
		return []byte{}, "", fmt.Errorf("ioutil.ReadAll err: %v", err)
	}
	return fileByts, stepSpec.UploadDetail[0].Name, nil
}

func UpdateWorkflowV4TaskRemark(workflowName string, taskID int64, remark string, log *zap.SugaredLogger) error {
	workflowTask, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		return fmt.Errorf("cannot find workflow task, workflow name: %s, task id: %d", workflowName, taskID)
	}

	workflowTask.Remark = remark
	return commonrepo.NewworkflowTaskv4Coll().Update(workflowTask.ID.Hex(), workflowTask)
}

func ListWorkflowFilterInfo(project, workflow, typeName string, jobName string, logger *zap.SugaredLogger) ([]string, error) {
	if project == "" || workflow == "" || typeName == "" {
		return []string{}, fmt.Errorf("paramerter is empty")
	}

	switch typeName {
	case "creator":
		resp, err := commonrepo.NewworkflowTaskv4Coll().ListCreator(project, workflow)
		if err != nil {
			logger.Errorf("ListWorkflowTaskCreator ListCreator err:%v", err)
			return []string{}, fmt.Errorf("ListCreator err: %v", err)
		}
		return resp, nil
	case "envName":
		workflow, err := commonrepo.NewWorkflowV4Coll().Find(workflow)
		if err != nil {
			logger.Errorf("failed to find workflow %s: %v", workflow, err)
			return nil, err
		}

		names := make([]string, 0)
		for _, stage := range workflow.Stages {
			for _, job := range stage.Jobs {
				if job.Name == jobName && job.JobType == config.JobZadigDeploy {
					deploy := &commonmodels.ZadigDeployJobSpec{}
					if err := commonmodels.IToi(job.Spec, deploy); err != nil {
						return nil, err
					}
					env, isFixed := CheckFixedMarkReturnNoFixedEnv(deploy.Env)
					if isFixed && !utils.Contains(names, env) {
						names = append(names, env)
					} else {
						envs, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
							Name:       project,
							Production: &deploy.Production,
						})
						if err != nil {
							return nil, err
						}
						for _, env := range envs {
							if !utils.Contains(names, env.EnvName) {
								names = append(names, env.EnvName)
							}
						}
					}
					return names, nil
				}
			}
		}
		return names, nil
	case "serviceName":
		workflow, err := commonrepo.NewWorkflowV4Coll().Find(workflow)
		if err != nil {
			logger.Errorf("failed to find workflow %s: %v", workflow, err)
			return nil, err
		}
		services := make([]string, 0)
		for _, stage := range workflow.Stages {
			for _, job := range stage.Jobs {
				if job.Name == jobName {
					if job.JobType == config.JobZadigDeploy {
						deploy := new(commonmodels.ZadigDeployJobSpec)
						if err := commonmodels.IToi(job.Spec, deploy); err != nil {
							return nil, err
						}
						if deploy.Source == config.SourceFromJob {
							for _, st := range workflow.Stages {
								for _, j := range st.Jobs {
									if j.Name == deploy.JobName && j.JobType == config.JobZadigBuild {
										build := new(commonmodels.ZadigBuildJobSpec)
										if err := commonmodels.IToi(j.Spec, build); err != nil {
											return nil, err
										}
										for _, s := range build.ServiceAndBuilds {
											if !utils.Contains(services, s.ServiceModule) {
												services = append(services, s.ServiceModule)
											}
										}
										return services, nil
									}
								}
							}
						}
						if deploy.Source == config.SourceRuntime {
							serviceInEnv, err := service.ListServicesInEnv(deploy.Env, project, nil, logger)
							if err != nil {
								return nil, err
							}
							for _, s := range serviceInEnv.Services {
								for _, serviceModule := range s.ServiceModules {
									if !utils.Contains(services, serviceModule.Name) {
										services = append(services, serviceModule.Name)
									}
								}
							}
						}
						return services, nil
					}
					if job.JobType == config.JobZadigBuild {
						build := new(commonmodels.ZadigBuildJobSpec)
						if err := commonmodels.IToi(job.Spec, build); err != nil {
							return nil, err
						}
						for _, s := range build.ServiceAndBuilds {
							if !utils.Contains(services, s.ServiceModule) {
								services = append(services, s.ServiceModule)
							}
						}
						return services, nil
					}
				}
			}
		}
		return services, nil
	default:
		return nil, fmt.Errorf("queryType parameter is invalid")
	}
}
