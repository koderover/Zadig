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

package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/koderover/zadig/pkg/types"

	commonmodels "github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/models"
	commonservice "github.com/koderover/zadig/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/workflow/testing/service"
	internalhandler "github.com/koderover/zadig/pkg/shared/handler"
	e "github.com/koderover/zadig/pkg/tool/errors"
	"github.com/koderover/zadig/pkg/tool/log"
	"github.com/koderover/zadig/pkg/util/ginzap"
)

func GetTestProductName(c *gin.Context) {
	args := new(commonmodels.Testing)
	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("c.GetRawData() err : %v", err)
		return
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("json.Unmarshal err : %v", err)
		return
	}
	c.Set("productName", args.ProductName)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))
	c.Next()
}

func CreateTestModule(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.Err = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := new(commonmodels.Testing)
	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("CreateTestModule c.GetRawData() err : %v", err)
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("CreateTestModule json.Unmarshal err : %v", err)
	}
	projectKey := args.ProductName
	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "新增", "项目管理-测试", args.Name, string(data), ctx.Logger)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Test.Create {
			ctx.UnAuthorized = true
			return
		}
	}

	err = c.BindJSON(args)
	if err != nil {
		ctx.Err = e.ErrInvalidParam.AddDesc("invalid Test args")
		return
	}

	ctx.Err = service.CreateTesting(ctx.UserName, args, ctx.Logger)
}

func UpdateTestModule(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.Err = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	args := new(commonmodels.Testing)
	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("UpdateTestModule c.GetRawData() err : %v", err)
	}
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("UpdateTestModule json.Unmarshal err : %v", err)
	}
	projectKey := args.ProductName
	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "更新", "项目管理-测试", args.Name, string(data), ctx.Logger)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Test.Edit {
			ctx.UnAuthorized = true
			return
		}
	}

	err = c.BindJSON(args)
	if err != nil {
		ctx.Err = e.ErrInvalidParam.AddDesc("invalid Test args")
		return
	}

	ctx.Err = service.UpdateTesting(ctx.UserName, args, ctx.Logger)
}

func ListTestModules(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {
		ctx.Err = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projects := make([]string, 0)
	// if a specific projectName is given, we query for that
	projectName := c.Query("projectName")
	log.Infof("--------- query parameter %s", projectName)
	if len(projects) == 0 && len(projectName) > 0 {
		// authorization check
		if !ctx.Resources.IsSystemAdmin {
			if _, ok := ctx.Resources.ProjectAuthInfo[projectName]; !ok {
				ctx.UnAuthorized = true
				return
			}

			if !ctx.Resources.ProjectAuthInfo[projectName].IsProjectAdmin &&
				!ctx.Resources.ProjectAuthInfo[projectName].Test.View {
				ctx.UnAuthorized = true
				return
			}
		}
		projects = []string{projectName}
		log.Infof("-------- projects data is %v", projects)
	} else {
		log.Infof("------ allowedProjects data is %v", allowedProjects)
		log.Infof("------ auth data is %v / %v / %v", ctx.UserID, ctx.Resources.IsSystemAdmin)
		// otherwise all projects with the get testing permission will be added to the projects
		allowedProjects, found, err := internalhandler.ListAuthorizedProjectsByResourceAndVerb(ctx.UserID, types.ResourceTypeTest, types.TestActionView)
		if err != nil || !found {
			log.Infof("-------- failed to find testing data, err: %s", err)
			ctx.Resp = make([]*service.TestingOpt, 0)
			return
		}
		projects = allowedProjects
	}

	ctx.Resp, ctx.Err = service.ListTestingOpt(projects, c.Query("testType"), ctx.Logger)
}

func GetTestModule(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.Err = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")

	// TODO: Authorization leak
	// this API is sometimes used in edit/create workflow scenario, thus giving the edit/create workflow permission
	// authorization check
	permitted := false

	if ctx.Resources.IsSystemAdmin {
		permitted = true
	} else if projectAuthInfo, ok := ctx.Resources.ProjectAuthInfo[projectKey]; ok {
		// first check if the user is projectAdmin
		if projectAuthInfo.IsProjectAdmin {
			permitted = true
		}

		// then check if user has edit workflow permission
		if projectAuthInfo.Workflow.Edit ||
			projectAuthInfo.Workflow.Create ||
			projectAuthInfo.Test.View {
			permitted = true
		}

		// finally check if the permission is given by collaboration mode
		collaborationAuthorizedEdit, err := internalhandler.CheckPermissionGivenByCollaborationMode(ctx.UserID, projectKey, types.ResourceTypeWorkflow, types.WorkflowActionEdit)
		if err == nil && collaborationAuthorizedEdit {
			permitted = true
		}
	}

	if !permitted {
		ctx.UnAuthorized = true
		return
	}

	name := c.Param("name")

	if name == "" {
		ctx.Err = e.ErrInvalidParam.AddDesc("empty Name")
		return
	}
	ctx.Resp, ctx.Err = service.GetTesting(name, projectKey, ctx.Logger)
}

func DeleteTestModule(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if err != nil {

		ctx.Err = fmt.Errorf("authorization Info Generation failed: err %s", err)
		ctx.UnAuthorized = true
		return
	}

	projectKey := c.Query("projectName")

	internalhandler.InsertOperationLog(c, ctx.UserName, projectKey, "删除", "项目管理-测试", c.Param("name"), "", ctx.Logger)

	// authorization check
	if !ctx.Resources.IsSystemAdmin {
		if _, ok := ctx.Resources.ProjectAuthInfo[projectKey]; !ok {
			ctx.UnAuthorized = true
			return
		}

		if !ctx.Resources.ProjectAuthInfo[projectKey].IsProjectAdmin &&
			!ctx.Resources.ProjectAuthInfo[projectKey].Test.Delete {
			ctx.UnAuthorized = true
			return
		}
	}

	name := c.Param("name")
	if name == "" {
		ctx.Err = e.ErrInvalidParam.AddDesc("empty Name")
		return
	}

	ctx.Err = commonservice.DeleteTestModule(name, projectKey, ctx.RequestID, ctx.Logger)
}

func GetHTMLTestReport(c *gin.Context) {
	content, err := service.GetHTMLTestReport(
		c.Query("pipelineName"),
		c.Query("pipelineType"),
		c.Query("taskID"),
		c.Query("testName"),
		ginzap.WithContext(c).Sugar(),
	)
	if err != nil {
		c.JSON(500, gin.H{"err": err})
		return
	}

	c.Header("content-type", "text/html")
	c.String(200, content)
}

func GetWorkflowV4HTMLTestReport(c *gin.Context) {
	taskID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(500, gin.H{"err": err})
		return
	}
	content, err := service.GetWorkflowV4HTMLTestReport(c.Param("workflowName"), c.Param("jobName"), taskID, ginzap.WithContext(c).Sugar())
	if err != nil {
		c.JSON(500, gin.H{"err": err})
		return
	}

	c.Header("content-type", "text/html")
	c.String(200, content)
}
