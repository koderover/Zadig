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
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/koderover/zadig/pkg/microservice/aslan/core/renderset/service"
	internalhandler "github.com/koderover/zadig/pkg/shared/handler"
	e "github.com/koderover/zadig/pkg/tool/errors"
	"github.com/koderover/zadig/pkg/tool/log"
	"github.com/koderover/zadig/pkg/types/permission"
)

func GetServiceRenderset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	if c.Query("envName") == "" {
		ctx.Err = e.ErrInvalidParam.AddDesc("envName can not be null!")
		return
	}

	ctx.Resp, ctx.Err = service.ListChartRenders(c.Param("productName"), c.Query("envName"), c.Query("serviceName"), ctx.Logger)
}

func CreateOrUpdateRenderset(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	data, err := c.GetRawData()
	if err != nil {
		log.Errorf("CreateOrUpdateRenderset c.GetRawData() err : %v", err)
	}

	args := new(service.RendersetCreateArgs)
	if err = json.Unmarshal(data, args); err != nil {
		log.Errorf("CreateOrUpdateRenderset json.Unmarshal err : %v", err)
		ctx.Err = e.ErrInvalidParam.AddDesc(err.Error())
	}
	internalhandler.InsertOperationLog(c, ctx.Username, c.Param("productName"), "新增", "环境变量", c.Query("envName"), fmt.Sprintf("%s,%s", permission.TestEnvManageUUID, permission.ProdEnvManageUUID), string(data), ctx.Logger)

	if args.EnvName == "" {
		ctx.Err = e.ErrInvalidParam.AddDesc("envName can not be null!")
		return
	}

	ctx.Err = service.CreateOrUpdateChartValues(c.Param("productName"), args.EnvName, args, ctx.Username, ctx.RequestID, ctx.Logger)
}
