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
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"

	commonmodels "github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/models/template"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/environment/service"
	internalhandler "github.com/koderover/zadig/pkg/shared/handler"
	"github.com/koderover/zadig/pkg/types"
)

func CleanProductCronJob(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	service.CleanProductCronJob(ctx.RequestID, ctx.Logger)
}

type getInitProductRespone struct {
	ProductName    string                           `json:"product_name"`
	CreateTime     int64                            `json:"create_time"`
	Revision       int64                            `json:"revision"`
	UpdateBy       string                           `json:"update_by"`
	Services       [][]*commonmodels.ProductService `json:"services"`
	Render         *commonmodels.RenderInfo         `json:"render"`
	ServiceRenders []*templatemodels.ServiceRender  `json:"chart_infos,omitempty"`
	Source         string                           `json:"source"`
}

// @Summary Get init product
// @Description Get init product
// @Tags 	environment
// @Accept 	json
// @Produce json
// @Param 	name			path		string								true	"project template name"
// @Param 	envType 		query		string								true	"env type"
// @Param 	isBaseEnv 		query		string								true	"is base env"
// @Param 	baseEnv 		query		string								true	"base env"
// @Success 200 			{object} 	getInitProductRespone
// @Router /environment/init_info/{name} [get]
func GetInitProduct(c *gin.Context) {
	ctx := internalhandler.NewContext(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()

	productTemplateName := c.Param("name")

	envType := types.EnvType(c.Query("envType"))
	isBaseEnvStr := c.Query("isBaseEnv")
	baseEnvName := c.Query("baseEnv")

	if envType == "" {
		envType = types.GeneralEnv
	}

	var isBaseEnv bool
	var err error
	if envType == types.ShareEnv {
		isBaseEnv, err = strconv.ParseBool(isBaseEnvStr)
		if err != nil {
			ctx.Err = fmt.Errorf("failed to parse %s to bool: %s", isBaseEnvStr, err)
			return
		}
	}

	product, err := service.GetInitProduct(productTemplateName, envType, isBaseEnv, baseEnvName, ctx.Logger)
	if err != nil {
		ctx.Err = err
		return
	}

	ctx.Resp = getInitProductRespone{
		ProductName:    product.ProductName,
		CreateTime:     product.CreateTime,
		Revision:       product.Revision,
		UpdateBy:       product.UpdateBy,
		Services:       product.Services,
		Render:         product.Render,
		ServiceRenders: product.ServiceRenders,
		Source:         product.Source,
	}
}
