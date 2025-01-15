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
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	workflowtool "github.com/koderover/zadig/v2/pkg/tool/workflow"

	"github.com/koderover/zadig/v2/pkg/types"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow"
	"github.com/koderover/zadig/v2/pkg/setting"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type listWorkflowTaskV4Query struct {
	PageSize     int64  `json:"page_size"    form:"page_size,default=20"`
	PageNum      int64  `json:"page_num"     form:"page_num,default=1"`
	WorkflowName string `json:"workflow_name" form:"workflow_name"`
}

type listWorkflowTaskV4Resp struct {
	WorkflowList []*commonmodels.WorkflowTask `json:"workflow_list"`
	Total        int64                        `json:"total"`
}

type ApproveRequest struct {
	JobName      string `json:"job_name"`
	WorkflowName string `json:"workflow_name"`
	TaskID       int64  `json:"task_id"`
	Approve      bool   `json:"approve"`
	Comment      string `json:"comment"`
}

func CreateWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := new(commonmodels.WorkflowV4)
	data := getBody(c)
	if err := json.Unmarshal([]byte(data), args); err != nil {
		log.Errorf("CreateWorkflowTaskv4 json.Unmarshal err : %s", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	ticketID := c.Query("approval_ticket_id")

	internalhandler.InsertOperationLog(c, ctx.UserName, args.Project, "新建", "自定义工作流任务", args.Name, data, ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[args.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[args.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[args.Project].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, args.Project, types.ResourceTypeWorkflow, args.Name, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.Resp, ctx.RespErr = workflow.CreateWorkflowTaskV4(&workflow.CreateWorkflowTaskV4Args{
		Name:             ctx.UserName,
		Account:          ctx.Account,
		UserID:           ctx.UserID,
		ApprovalTicketID: ticketID,
	}, args, ctx.Logger)
}

// TODO: fix the authorization problem for this
func CreateWorkflowTaskV4ByBuildInTrigger(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	args := new(commonmodels.WorkflowV4)
	if err := c.ShouldBindJSON(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}
	triggerName := c.Query("triggerName")
	if triggerName == "" {
		triggerName = setting.DefaultTaskRevoker
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, args.Project, "新建", "自定义工作流任务", args.Name, getBody(c), ctx.Logger)
	ctx.Resp, ctx.RespErr = workflow.CreateWorkflowTaskV4ByBuildInTrigger(triggerName, args, ctx.Logger)
}

type listWorkflowTaskV4PreviewResp struct {
	WorkflowList []*commonmodels.WorkflowTaskPreview `json:"workflow_list"`
	Total        int64                               `json:"total"`
}

func ListWorkflowTaskV4ByFilter(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	filter := &workflow.TaskHistoryFilter{}
	if err := c.ShouldBindQuery(filter); err != nil {
		ctx.RespErr = err
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[filter.ProjectName]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[filter.ProjectName].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[filter.ProjectName].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, filter.ProjectName, types.ResourceTypeWorkflow, filter.WorkflowName, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	filterList := strings.Split(filter.Filters, ",")
	taskList, total, err := workflow.ListWorkflowTaskV4ByFilter(filter, filterList, ctx.Logger)
	resp := listWorkflowTaskV4PreviewResp{
		WorkflowList: taskList,
		Total:        total,
	}
	ctx.Resp = resp
	ctx.RespErr = err
}

func GetWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("CreateCronForWorkflowV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, w.Name, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.Resp, ctx.RespErr = workflow.GetWorkflowTaskV4(workflowName, taskID, ctx.Logger)
}

func CancelWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	workflowName := c.Param("workflowName")
	projectKey := c.Query("projectName")

	username := ctx.UserName
	if c.Query("username") != "" {
		username = c.Query("username")
	}
	internalhandler.InsertOperationLog(c, username, projectKey, "取消", "自定义工作流任务", c.Param("workflowName"), "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.CancelWorkflowTaskV4(username, workflowName, taskID, ctx.Logger)
}

func CloneWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")
	workflowName := c.Param("workflowName")

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "克隆", "自定义工作流任务", c.Param("workflowName"), "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.Resp, ctx.RespErr = workflow.CloneWorkflowTaskV4(workflowName, taskID, false, ctx.Logger)
}

func ViewWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")
	workflowName := c.Param("workflowName")

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.Resp, ctx.RespErr = workflow.CloneWorkflowTaskV4(workflowName, taskID, true, ctx.Logger)
}

func GetManualExecWorkflowTaskV4Info(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")
	workflowName := c.Param("workflowName")

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.Resp, ctx.RespErr = workflow.GetManualExecWorkflowTaskV4Info(workflowName, taskID, ctx.Logger)
}

func RetryWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")
	workflowName := c.Param("workflowName")

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}
	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "重试", "自定义工作流任务", c.Param("workflowName"), "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.RetryWorkflowTaskV4(workflowName, taskID, ctx.Logger)
}

// @Summary Manually Execute Workflow Task V4
// @Description Manually Execute Workflow Task V4
// @Tags 	workflow
// @Accept 	json
// @Produce json
// @Param 	projectName		query		string										true	"project name"
// @Param 	workflowName	path		string										true	"workflow name"
// @Param 	taskID			path		string										true	"workflow task ID"
// @Param 	stageName		query		string										true	"workflow stage name"
// @Param 	body 			body 		workflow.ManualExecWorkflowTaskV4Request 	true 	"workflow stage jobs"
// @Success 200
// @Router /api/aslan/workflow/v4/workflowtask/manualexec/workflow/{workflowName}/task/{taskID} [post]
func ManualExecWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")
	workflowName := c.Param("workflowName")
	stageName := c.Query("stageName")
	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	args := new(workflow.ManualExecWorkflowTaskV4Request)
	data := getBody(c)
	if err := json.Unmarshal([]byte(data), args); err != nil {
		log.Errorf("ManualExecWorkflowTaskV4 json.Unmarshal err : %s", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "手动执行", "自定义工作流任务", c.Param("workflowName"), string(data), ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, projectKey, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	ctx.RespErr = workflow.ManualExecWorkflowTaskV4(workflowName, taskID, stageName, args.Jobs, ctx.UserID, ctx.UserName, ctx.Resources.IsSystemAdmin, ctx.Logger)
}

func SetWorkflowTaskV4Breakpoint(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("SetWorkflowTaskV4Breakpoint error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Debug {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionDebug)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}
	var set bool
	switch c.Query("operation") {
	case "set", "unset":
		set = c.Query("operation") == "set"
	default:
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid operation")
		return
	}
	switch c.Param("position") {
	case "before", "after":
	default:
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid position")
		return
	}
	ctx.RespErr = workflow.SetWorkflowTaskV4Breakpoint(workflowName, c.Param("jobName"), taskID, set, c.Param("position"), ctx.Logger)
}

func EnableDebugWorkflowTaskV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("EnableDebugWorkflowTaskV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Debug {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionDebug)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}
	ctx.RespErr = workflow.EnableDebugWorkflowTaskV4(workflowName, taskID, ctx.Logger)
}

func StopDebugWorkflowTaskJobV4(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("EnableDebugWorkflowTaskV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Debug {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionDebug)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}
	ctx.RespErr = workflow.StopDebugWorkflowTaskJobV4(workflowName, c.Param("jobName"), taskID, c.Param("position"), ctx.Logger)
}

func ApproveStage(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	args := &ApproveRequest{}

	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("ApproveStage c.GetRawData() err : %s", err)
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("ApproveStage json.Unmarshal err : %s", err)
	}

	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))

	if err := c.ShouldBindJSON(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	ctx.RespErr = workflow.ApproveStage(args.WorkflowName, args.JobName, ctx.UserName, ctx.UserID, args.Comment, args.TaskID, args.Approve, ctx.Logger)
}

type HandleJobErrorRequest struct {
	JobName      string                        `json:"job_name"`
	WorkflowName string                        `json:"workflow_name"`
	TaskID       int64                         `json:"task_id"`
	Decision     workflowtool.JobErrorDecision `json:"decision"`
}

func HandleJobError(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	args := &HandleJobErrorRequest{}

	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("HandleJobError c.GetRawData() err : %s", err)
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("HandleJobError json.Unmarshal err : %s", err)
	}

	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))

	if err := c.ShouldBindJSON(&args); err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	ctx.RespErr = workflow.HandleJobError(args.WorkflowName, args.JobName, ctx.UserID, ctx.UserName, args.TaskID, args.Decision, ctx.Logger)
}

func GetWorkflowV4ArtifactFileContent(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("EnableDebugWorkflowTaskV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskId"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	resp, err := workflow.GetWorkflowV4ArtifactFileContent(workflowName, c.Param("jobName"), taskID, ctx.Logger)
	if err != nil {
		ctx.RespErr = err
		return
	}
	c.Writer.Header().Set("Content-Disposition", `attachment; filename="artifact.tar.gz"`)

	c.Data(200, "application/octet-stream", resp)
}

func GetWorkflowV4BuildJobArtifactFile(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("EnableDebugWorkflowTaskV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.View {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionView)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskId"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	resp, filename, err := workflow.GetWorkflowV4BuildJobArtifactFile(workflowName, c.Param("jobName"), taskID, ctx.Logger)
	if err != nil {
		ctx.RespErr = err
		return
	}
	c.Writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))

	c.Data(200, "application/octet-stream", resp)
}

type updateWorkflowV4TaskRemarkReq struct {
	Remark string `json:"remark"`
}

func UpdateWorkflowV4TaskRemark(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.RespErr = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	workflowName := c.Param("workflowName")

	w, err := workflow.FindWorkflowV4Raw(workflowName, ctx.Logger)
	if err != nil {
		ctx.Logger.Errorf("EnableDebugWorkflowTaskV4 error: %v", err)
		ctx.RespErr = e.ErrInvalidParam.AddErr(err)
		return
	}

	args := new(updateWorkflowV4TaskRemarkReq)
	data := getBody(c)
	if err := json.Unmarshal([]byte(data), args); err != nil {
		log.Errorf("CreateWorkflowTaskv4 json.Unmarshal err : %s", err)
		ctx.RespErr = e.ErrInvalidParam.AddDesc(err.Error())
		return
	}

	internalhandler.InsertOperationLog(c, ctx.UserName, w.Project, "更新", "自定义工作流任务", workflowName, data, ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[w.Project]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[w.Project].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[w.Project].Workflow.Execute {
			// check if the permission is given by collaboration mode
			permitted, err := internalhandler.GetCollaborationModePermission(ctx.UserID, w.Project, types.ResourceTypeWorkflow, workflowName, types.WorkflowActionRun)
			if err != nil || !permitted {
				ctx.UnAuthorized = true
				return
			}
		}
	}

	taskID, err := strconv.ParseInt(c.Param("taskId"), 10, 64)
	if err != nil {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("invalid task id")
		return
	}

	ctx.RespErr = workflow.UpdateWorkflowV4TaskRemark(workflowName, taskID, args.Remark, ctx.Logger)
}

func GetWorkflowTaskFilters(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	ctx.Resp, ctx.RespErr = workflow.ListWorkflowFilterInfo(c.Query("projectName"), c.Param("name"), c.Query("queryType"), c.Query("jobName"), ctx.Logger)
}
