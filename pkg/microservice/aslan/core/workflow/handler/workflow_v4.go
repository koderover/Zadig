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

package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"gopkg.in/yaml.v3"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/types"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	"github.com/koderover/zadig/v2/pkg/tool/errors"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type listWorkflowV4Query struct {
	PageSize int64  `json:"page_size"    form:"page_size,default=20"`
	PageNum  int64  `json:"page_num"     form:"page_num,default=1"`
	Project  string `json:"project"      form:"project"`
	ViewName string `json:"view_name"    form:"view_name"`
}

type filterDeployServiceVarsQuery struct {
	WorkflowName string   `json:"workflow_name"`
	JobName      string   `json:"job_name"`
	EnvName      string   `json:"env_name"`
	ServiceNames []string `json:"service_names"`
}

type getHelmValuesDifferenceReq struct {
	ServiceName           string            `json:"service_name"`
	VariableYaml          string            `json:"variable_yaml"`
	EnvName               string            `json:"env_name"`
	IsProduction          bool              `json:"production"`
	IsHelmChartDeploy     bool              `json:"is_helm_chart_deploy"`
	UpdateServiceRevision bool              `json:"update_service_revision"`
	ServiceModules        []*ModuleAndImage `json:"service_modules"`
}

type ModuleAndImage struct {
	Image string `json:"image"`
	Name  string `json:"name"`
}

type listWorkflowV4Resp struct {
	WorkflowList []*workflow.Workflow `json:"workflow_list"`
	Total        int64                `json:"total"`
}

// @Summary 创建工作流
// @Description 创建工作流
// @Tags 	workflow
// @Accept 	plain
// @Produce json
// @Param 	projectName		query		string								true	"项目标识"
// @Param 	viewName 		query		string								true	"需要添加的视图名称"
// @Param 	body 			body 		commonmodels.WorkflowV4 			true 	"工作流Yaml"
// @Success 200
// @Router /api/aslan/workflow/v4 [post]
func CreateWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := new(commonmodels.WorkflowV4)
	data := getBody(c)
	if err := yaml.Unmarshal([]byte(data), args); err != nil {
		log.Errorf("CreateWorkflowv4 yaml.Unmarshal err : %s", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, args.Project, "新增", "自定义工作流", args.Name, data, ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[args.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[args.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[args.Project].Workflow.Create {
			// check if the permission is given by collaboration mode
			ctx.UnAuthorized = true
			return
		}
	}

	if err := workflow.CreateWorkflowV4(ctx.UserName, args, ctx.Logger); err != nil {
		ctx.RespErr = err
		return
	}

	if view := c.Query("viewName"); view != "" {
		workflow.AddWorkflowToView(args.Project, view, []*commonmodels.WorkflowViewDetail{
			{
				WorkflowName:        args.Name,
				WorkflowDisplayName: args.DisplayName,
				WorkflowType:        setting.CustomWorkflowType,
			},
		}, ctx.Logger)
	}
}

func SetWorkflowTasksCustomFields(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")
	projectKey := c.Query("projectName")
	args := new(commonmodels.CustomField)
	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("SetWorkflowTasksCustomFields c.GetRawData() err : %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("SetWorkflowTasksCustomFields json.Unmarshal err : %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Create {
			ctx.UnAuthorized = true
			return
		}
	}

	ctx.RespErr = workflow.SetWorkflowTasksCustomFields(projectKey, workflowName, args, ctx.Logger)
}

func GetWorkflowTasksCustomFields(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	workflowName := c.Param("workflowName")
	projectName := c.Query("projectName")

	ctx.Resp, ctx.RespErr = workflow.GetWorkflowTasksCustomFields(projectName, workflowName, ctx.Logger)
}

func LintWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)
	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("CreateWorkflowv4 c.GetRawData() err : %s", err)
	}
	if err = yaml.Unmarshal(data, args); err != nil {
		log.Errorf("CreateWorkflowv4 json.Unmarshal err : %s", err)
	}

	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))

	if err := c.ShouldBindYAML(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	ctx.RespErr = workflow.LintWorkflowV4(args, ctx.Logger)
}

func ListWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := &listWorkflowV4Query{}
	if err := c.ShouldBindQuery(args); err != nil {
		ctx.RespErr = err
		return
	}

	var authorizedWorkflow, authorizedWorkflowV4 []string
	var enableFilter bool

	// authorization checks
	if ctx.Resources.IsSystemAdmin {
		enableFilter = false
		authorizedWorkflow = make([]string, 0)
		authorizedWorkflowV4 = make([]string, 0)
		ctx.Logger.Infof("user is admin, disabling filter")
	} else if projectAuth, ok := ctx.Resources.ProjectAuthInfo[args.Project]; ok {
		if projectAuth.IsProjectAdmin || projectAuth.Workflow.View {
			enableFilter = false
			authorizedWorkflow = make([]string, 0)
			authorizedWorkflowV4 = make([]string, 0)
			ctx.Logger.Infof("user is project admin or has workflow view authorization, disabling filter")
		} else {
			var err error
			authorizedWorkflow, authorizedWorkflowV4, enableFilter, err = internalhandler.ListAuthorizedWorkflows(ctx.UserID, args.Project)
			if err != nil {
				ctx.Logger.Errorf("failed to list authorized workflow resource, error: %s", err)
				// something wrong when getting workflow authorization info, returning empty
				ctx.Resp = listWorkflowV4Resp{
					WorkflowList: make([]*workflow.Workflow, 0),
					Total:        0,
				}
				return
			}
		}
	} else {
		// if a user does not have a role in a project, it must also not have a collaboration mode
		// thus return nothing
		ctx.Logger.Warnf("failed to get project auth info, returning empty workflow")
		ctx.Resp = listWorkflowV4Resp{
			WorkflowList: make([]*workflow.Workflow, 0),
			Total:        0,
		}
		return
	}

	workflowList, err := workflow.ListWorkflowV4(args.Project, args.ViewName, ctx.UserID, authorizedWorkflow, authorizedWorkflowV4, enableFilter, ctx.Logger)
	resp := listWorkflowV4Resp{
		WorkflowList: workflowList,
		Total:        int64(len(workflowList)),
	}
	ctx.Resp = resp
	ctx.RespErr = err
}

func ListWorkflowV4CanTrigger(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	ctx.Resp, ctx.RespErr = workflow.ListWorkflowV4CanTrigger(ctx)
}

// @Summary 更新工作流
// @Description 更新工作流
// @Tags 	workflow
// @Accept 	plain
// @Produce json
// @Param 	projectName		query		string								true	"项目标识"
// @Param 	name			path		string								true	"工作流标识"
// @Param 	body 			body 		commonmodels.WorkflowV4 			true 	"工作流Yaml"
// @Success 200
// @Router /api/aslan/workflow/v4/{name} [put]
func UpdateWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := new(commonmodels.WorkflowV4)
	data := getBody(c)
	if err := yaml.Unmarshal([]byte(data), args); err != nil {
		log.Errorf("UpdateWorkflowV4 yaml.Unmarshal err : %s", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	internalhandler.InsertOperationLog(c, ctx.UserName, args.Project, "更新", "自定义工作流", args.Name, string(data), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[args.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[args.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[args.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, args.Project, types.ResourceTypeWorkflow, args.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.UpdateWorkflowV4(c.Param("name"), ctx.UserName, args, ctx.Logger)
}

func DeleteWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("name"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("DeleteWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrDeleteWorkflow.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "(OpenAPI)"+"删除", "自定义工作流", c.Param("name"), "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			ctx.UnAuthorized = true
			return
		}
	}

	ctx.RespErr = workflow.DeleteWorkflowV4(c.Param("name"), ctx.Logger)
}

func FindWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	resp, err := workflow.FindWorkflowV4(c.Query("encryptedKey"), c.Param("name"), ctx.Logger)
	if err != nil {
		c.JSON(e.ErrorMessage(err))
		c.Abort()
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[resp.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[resp.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[resp.Project].Workflow.Edit &&
			!ctx.Resources.ProjectAuthInfo[resp.Project].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, resp.Project, types.ResourceTypeWorkflow, resp.Name, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	c.YAML(200, resp)
}

// @Summary Render Workflow V4 Variables
// @Description Render Workflow V4 Variables
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	jobName			query		string									true	"job name"
// @Param 	serviceName		query		string									false	"service name"
// @Param 	moduleName		query		string									false	"service module name"
// @Param 	key				query		string									true	"render variable key"
// @Param 	body 			body 		commonmodels.WorkflowV4		  			true 	"body"
// @Success 200 			{array} 	string
// @Router /api/aslan/workflow/v4/dynamicVariable/render [post]
func RenderWorkflowV4DynamicVariables(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)
	data := getBody(c)
	if err := json.Unmarshal([]byte(data), args); err != nil {
		err = fmt.Errorf("RenderWorkflowV4Variables json.Unmarshal err : %s", err)
		ctx.Logger.Error(err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	ctx.Resp, ctx.RespErr = workflow.RenderWorkflowV4Variables(ctx, args, c.Query("jobName"), c.Query("serviceName"), c.Query("moduleName"), c.Query("key"))
}

// @Summary Get Workflow V4 Dynamic Variable's Available Variables
// @Description Get Workflow V4 Dynamic Variable's Available Variables
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	jobName			query		string									true	"job name"
// @Param 	body 			body 		commonmodels.WorkflowV4		  			true 	"body"
// @Success 200 			{array} 	string
// @Router /api/aslan/workflow/v4/dynamicVariable/available [post]
func GetWorkflowV4DynamicVariableAvailable(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)
	data := getBody(c)
	if err := json.Unmarshal([]byte(data), args); err != nil {
		err = fmt.Errorf("RenderWorkflowV4Variables json.Unmarshal err : %s", err)
		ctx.Logger.Error(err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	ctx.Resp, ctx.RespErr = workflow.GetWorkflowV4DynamicVariableAvailable(ctx, args, c.Query("jobName"))
}

func GetWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.GetWorkflowv4Preset(c.Query("encryptedKey"), c.Param("name"), ctx.UserID, ctx.UserName, c.Query("approval_ticket_id"), ctx.Logger)
}

func GetWebhookForWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.GetWebhookForWorkflowV4Preset(c.Query("workflowName"), c.Query("triggerName"), ctx.Logger)
}

func CheckWorkflowV4Approval(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.RespErr = workflow.CheckWorkflowV4ApprovalInitiator(c.Param("name"), ctx.UserID, ctx.Logger)
}

func ListWebhookForWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.ListWebhookForWorkflowV4(c.Query("workflowName"), ctx.Logger)
}

func CreateWebhookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	req := new(commonmodels.WorkflowV4Hook)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateWebhookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrCreateWebhook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "新建", "自定义工作流-webhook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.CreateWebhookForWorkflowV4(c.Param("workflowName"), req, ctx.Logger)
}

func UpdateWebhookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	req := new(commonmodels.WorkflowV4Hook)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("UpdateWebhookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpdateWebhook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "更新", "自定义工作流-webhook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.UpdateWebhookForWorkflowV4(c.Param("workflowName"), req, ctx.Logger)
}

func DeleteWebhookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("DeleteWebhookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrDeleteWebhook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "删除", "自定义工作流-webhook", w.Name, "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.DeleteWebhookForWorkflowV4(c.Param("workflowName"), c.Param("triggerName"), ctx.Logger)
}

func CreateJiraHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	jira := new(commonmodels.JiraHook)
	if err := c.ShouldBindJSON(jira); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateJiraHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrCreateJiraHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "新建", "自定义工作流-jirahook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	// license checks
	err = util.CheckZadigProfessionalLicense()
	if err != nil {
		ctx.RespErr = err
		return
	}

	ctx.RespErr = workflow.CreateJiraHookForWorkflowV4(c.Param("workflowName"), jira, ctx.Logger)
}

func GetJiraHookForWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.GetJiraHookForWorkflowV4Preset(c.Query("workflowName"), c.Query("hookName"), ctx.Logger)
}

func ListJiraHookForWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.ListJiraHookForWorkflowV4(c.Param("workflowName"), ctx.Logger)
}

func UpdateJiraHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	jira := new(commonmodels.JiraHook)
	if err := c.ShouldBindJSON(jira); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("UpdateJiraHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpdateJiraHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "更新", "自定义工作流-jirahook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	// license checks
	err = util.CheckZadigProfessionalLicense()
	if err != nil {
		ctx.RespErr = err
		return
	}

	ctx.RespErr = workflow.UpdateJiraHookForWorkflowV4(c.Param("workflowName"), jira, ctx.Logger)
}

func DeleteJiraHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("DeleteJiraHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrDeleteJiraHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "删除", "自定义工作流-jirahook", w.Name, "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.DeleteJiraHookForWorkflowV4(c.Param("workflowName"), c.Param("hookName"), ctx.Logger)
}

func CreateMeegoHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	jira := new(commonmodels.MeegoHook)
	if err := c.ShouldBindJSON(jira); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateMeegoHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrCreateMeegoHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "新建", "自定义工作流-meegohook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	// license checks
	err = util.CheckZadigProfessionalLicense()
	if err != nil {
		ctx.RespErr = err
		return
	}

	ctx.RespErr = workflow.CreateMeegoHookForWorkflowV4(c.Param("workflowName"), jira, ctx.Logger)
}

func GetMeegoHookForWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.GetMeegoHookForWorkflowV4Preset(c.Query("workflowName"), c.Query("hookName"), ctx.Logger)
}

func ListMeegoHookForWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.ListMeegoHookForWorkflowV4(c.Param("workflowName"), ctx.Logger)
}

func UpdateMeegoHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	jira := new(commonmodels.MeegoHook)
	if err := c.ShouldBindJSON(jira); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("UpdateMeegoHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpdateMeegoHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "更新", "自定义工作流-meegohook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	// license checks
	err = util.CheckZadigProfessionalLicense()
	if err != nil {
		ctx.RespErr = err
		return
	}

	ctx.RespErr = workflow.UpdateMeegoHookForWorkflowV4(c.Param("workflowName"), jira, ctx.Logger)
}

func DeleteMeegoHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("DeleteMeegoHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrDeleteMeegoHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "删除", "自定义工作流-meegohook", w.Name, "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.DeleteMeegoHookForWorkflowV4(c.Param("workflowName"), c.Param("hookName"), ctx.Logger)
}

func CreateGeneralHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	hook := new(commonmodels.GeneralHook)
	if err := c.ShouldBindJSON(hook); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateGeneralHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrCreateGeneralHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "新建", "自定义工作流-generalhook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.CreateGeneralHookForWorkflowV4(c.Param("workflowName"), hook, ctx.Logger)
}

func GetGeneralHookForWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.GetGeneralHookForWorkflowV4Preset(c.Query("workflowName"), c.Query("hookName"), ctx.Logger)
}

func ListGeneralHookForWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.Resp, ctx.RespErr = workflow.ListGeneralHookForWorkflowV4(c.Param("workflowName"), ctx.Logger)
}

func UpdateGeneralHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	hook := new(commonmodels.GeneralHook)
	if err := c.ShouldBindJSON(hook); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("UpdateGeneralHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpdateGeneralHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "更新", "自定义工作流-generalhook", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.UpdateGeneralHookForWorkflowV4(c.Param("workflowName"), hook, ctx.Logger)
}

func DeleteGeneralHookForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("DeleteGeneralHookForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrDeleteGeneralHook.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "删除", "自定义工作流-generalhook", w.Name, "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.DeleteGeneralHookForWorkflowV4(c.Param("workflowName"), c.Param("hookName"), ctx.Logger)
}

func GeneralHookEventHandler(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	ctx.RespErr = workflow.GeneralHookEventHandler(c.Param("workflowName"), c.Param("hookName"), ctx.Logger)
}

func GetCronForWorkflowV4Preset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.GetCronForWorkflowV4Preset(c.Query("workflowName"), c.Query("cronID"), ctx.Logger)
}

func ListCronForWorkflowV4(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.ListCronForWorkflowV4(c.Query("workflowName"), ctx.Logger)
}

func CreateCronForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	req := new(commonmodels.Cronjob)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateCronForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpsertCronjob.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "新建", "自定义工作流-cron", w.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.CreateCronForWorkflowV4(c.Param("workflowName"), req, ctx.Logger)
}

func UpdateCronForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	req := new(commonmodels.Cronjob)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, req.WorkflowV4Args.Project, "更新", "自定义工作流-cron", req.WorkflowV4Args.Name, getBody(c), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[req.WorkflowV4Args.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[req.WorkflowV4Args.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[req.WorkflowV4Args.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, req.WorkflowV4Args.Project, types.ResourceTypeWorkflow, req.WorkflowV4Args.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.UpdateCronForWorkflowV4(req, ctx.Logger)
}

func DeleteCronForWorkflowV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	w, err := workflow.FindWorkflowV4Raw(c.Param("workflowName"), ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateCronForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrUpsertCronjob.AddErr(err)
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "删除", "自定义工作流-cron", w.Name, "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Edit {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionEdit)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.DeleteCronForWorkflowV4(c.Param("workflowName"), c.Param("cronID"), ctx.Logger)
}

func GetPatchParams(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	req := &commonmodels.PatchItem{}
	if err := c.ShouldBindJSON(&req); err != nil {
		ctx.RespErr = errors.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	ctx.Resp, ctx.RespErr = workflow.GetPatchParams(req, ctx.Logger)
}

func GetWorkflowGlobalVars(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)

	if err := c.ShouldBindYAML(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	ctx.Resp = workflow.GetWorkflowGlabalVars(args, c.Param("jobName"), ctx.Logger)
}

func GetWorkflowRepoIndex(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)

	if err := c.ShouldBindYAML(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	ctx.Resp = workflow.GetWorkflowRepoIndex(args, c.Param("jobName"), ctx.Logger)
}

func CheckShareStorageEnabled(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.CheckShareStorageEnabled(c.Query("id"), c.Query("type"), c.Query("name"), c.Query("project"), ctx.Logger)
}

func ListAllAvailableWorkflows(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.ListAllAvailableWorkflows(c.QueryArray("projects"), ctx.Logger)
}

// @Summary Get filtered env services
// @Description Get filtered env services
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	body 		body 		filterDeployServiceVarsQuery	 	true 	"body"
// @Success 200 		{array} 	commonmodels.DeployService
// @Router /api/aslan/workflow/v4/filterEnv [post]
func GetFilteredEnvServices(c *gin.Context) {
	// TODO: fix the authorization problem for this.
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	req := new(filterDeployServiceVarsQuery)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	ctx.Resp, ctx.RespErr = workflow.GetFilteredEnvServices(req.WorkflowName, req.JobName, req.EnvName, req.ServiceNames, ctx.Logger)
}

// @Summary Compare Helm Service Yaml In Env
// @Description Compare Helm Service Yaml In Env
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	body 		body 		getHelmValuesDifferenceReq	 	true 	"body"
// @Success 200 		{object} 	workflow.GetHelmValuesDifferenceResp
// @Router /api/aslan/workflow/v4/yamlComparison [post]
func CompareHelmServiceYamlInEnv(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	req := new(getHelmValuesDifferenceReq)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	projectName := c.Query("projectName")

	images := make([]string, 0)
	for _, imageInfos := range req.ServiceModules {
		images = append(images, imageInfos.Image)
	}
	ctx.Resp, ctx.RespErr = workflow.CompareHelmServiceYamlInEnv(req.ServiceName, req.VariableYaml, req.EnvName, projectName, images, req.IsProduction, req.UpdateServiceRevision, req.IsHelmChartDeploy, ctx.Logger)
}

type YamlResponse struct {
	Yaml string `json:"yaml"`
}

func RenderMseServiceYaml(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	type RenderMseServiceYamlReq struct {
		commonmodels.MseGrayReleaseService `json:",inline"`
		LastGrayTag                        string `json:"last_gray_tag"`
		GrayTag                            string `json:"gray_tag"`
		EnvName                            string `json:"env_name"`
	}

	req := new(RenderMseServiceYamlReq)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	if req.MseGrayReleaseService.YamlContent == "" {
		mseServiceYaml, err := workflow.GetMseOriginalServiceYaml(c.Query("projectName"), req.EnvName, req.MseGrayReleaseService.ServiceName, req.GrayTag)
		if err != nil {
			ctx.RespErr = err
			return
		}
		ctx.Resp = YamlResponse{Yaml: mseServiceYaml}
		return
	}

	mseServiceYaml, err := workflow.RenderMseServiceYaml(c.Query("projectName"), req.EnvName, req.LastGrayTag, req.GrayTag, &req.MseGrayReleaseService)
	if err != nil {
		ctx.RespErr = err
		return
	}
	ctx.Resp = YamlResponse{Yaml: mseServiceYaml}
}

func GetMseOfflineResources(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	services, err := workflow.GetMseOfflineResources(c.Query("grayTag"), c.Query("envName"), c.Query("projectName"))
	if err != nil {
		ctx.RespErr = err
		return
	}
	ctx.Resp = struct {
		Services []string `json:"services"`
	}{
		Services: services,
	}
}

func GetBlueGreenServiceK8sServiceYaml(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	blueGreenServiceYaml, err := workflow.GetBlueGreenServiceK8sServiceYaml(c.Query("projectName"), c.Param("envName"), c.Param("serviceName"))
	if err != nil {
		ctx.RespErr = err
		return
	}
	ctx.Resp = YamlResponse{Yaml: blueGreenServiceYaml}
}

func GetMseTagsInEnv(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	tags, err := workflow.GetMseTagsInEnv(c.Param("envName"), c.Query("projectName"))
	if err != nil {
		ctx.RespErr = err
		return
	}
	ctx.Resp = struct {
		Tags []string `json:"tags"`
	}{
		Tags: tags,
	}
}

func GetJenkinsJobParams(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	jobParams, err := workflow.GetJenkinsJobParams(c.Param("id"), c.Param("jobName"))
	if err != nil {
		ctx.RespErr = err
		return
	}
	ctx.Resp = struct {
		Parameters []*workflow.JenkinsJobParams `json:"parameters"`
	}{
		Parameters: jobParams,
	}
}

type ValidateSQLReq struct {
	Type config.DBInstanceType `json:"type"`
	SQL  string                `json:"sql"`
}

type ValidateSQLResp struct {
	Message string `json:"message"`
}

func ValidateSQL(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	req := new(ValidateSQLReq)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	err := workflow.ValidateSQL(req.Type, req.SQL)
	if err == nil {
		ctx.Resp = []ValidateSQLResp{}
		return
	}
	ctx.Resp = []ValidateSQLResp{
		{
			Message: err.Error(),
		},
	}
	return
}

func getBody(c *gin.Context) string {
	b, err := c.GetRawData()
	if err != nil {
		return ""
	}
	return string(b)
}

type HelmDeployJobMergeImageRequest struct {
	ServiceName           string            `json:"service_name"`
	ValuesYaml            string            `json:"values_yaml"`
	EnvName               string            `json:"env_name"`
	IsProduction          bool              `json:"production"`
	UpdateServiceRevision bool              `json:"update_service_revision"`
	ServiceModules        []*ModuleAndImage `json:"service_modules"`
}

// @Summary 工作流Helm部署任务合并镜像到ValuesYaml
// @Description
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	projectName query       string                          true    "项目名称"
// @Param 	body 		body 		HelmDeployJobMergeImageRequest 	true 	"body"
// @Success 200  		{object} 	workflow.HelmDeployJobMergeImageResponse
// @Router /api/aslan/workflow/v4/deploy/mergeImage [post]
func HelmDeployJobMergeImage(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	req := new(HelmDeployJobMergeImageRequest)
	if err := c.ShouldBindJSON(req); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	projectName := c.Query("projectName")
	if projectName == "" {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("projectName is required")
		return
	}

	images := make([]string, 0)
	for _, imageInfos := range req.ServiceModules {
		images = append(images, imageInfos.Image)
	}
	ctx.Resp, ctx.RespErr = workflow.HelmDeployJobMergeImage(ctx, projectName, req.EnvName, req.ServiceName, req.ValuesYaml, images, req.IsProduction, req.UpdateServiceRevision)
}
