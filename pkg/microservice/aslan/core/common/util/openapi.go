package util

import (
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/types"
)

func ConvertEnvInfoToOpenAPIRollBackStat(envInfos []*models.EnvInfo) []*types.OpenAPIRollBackStat {
	if len(envInfos) == 0 {
		return nil
	}

	resp := make([]*types.OpenAPIRollBackStat, 0)
	for _, envInfo := range envInfos {
		resp = append(resp, &types.OpenAPIRollBackStat{
			ProjectKey:    envInfo.ProjectName,
			EnvName:       envInfo.EnvName,
			EnvType:       envInfo.EnvType,
			Production:    envInfo.Production,
			OperationType: envInfo.OperationType,
			ServiceName:   envInfo.ServiceName,
			ServiceType:   envInfo.ServiceType,
			OriginService: ConvertProductServiceToOpenAPIEnvService(envInfo.OriginService),
			UpdateService: ConvertProductServiceToOpenAPIEnvService(envInfo.UpdateService),
			OriginSaeApp:  ConvertSaeApplicationToOpenAPISaeApplication(envInfo.OriginSaeApp),
			UpdateSaeApp:  ConvertSaeApplicationToOpenAPISaeApplication(envInfo.UpdateSaeApp),
			CreatBy:       ConvertUserBriefInfoToOpenAPIUserBriefInfo(&envInfo.CreatedBy),
			CreatTime:     envInfo.CreatTime,
		})
	}
	return resp
}

func ConvertProductServiceToOpenAPIEnvService(productService *models.ProductService) *types.OpenAPIEnvService {
	if productService == nil {
		return nil
	}

	resp := &types.OpenAPIEnvService{
		ServiceName:    productService.ServiceName,
		ReleaseName:    productService.ReleaseName,
		RenderedYaml:   productService.RenderedYaml,
		Containers:     ConvertContainerToOpenAPIContainer(productService.Containers),
		// ValuesYaml:     productService.GetServiceRender().ValuesYaml,
		OverrideValues: productService.GetServiceRender().OverrideValues,
		UpdateTime:     productService.UpdateTime,
	}
	return resp
}

func ConvertContainerToOpenAPIContainer(containers []*models.Container) []*types.OpenAPIContainer {
	if len(containers) == 0 {
		return nil
	}

	resp := make([]*types.OpenAPIContainer, 0)
	for _, container := range containers {
		resp = append(resp, &types.OpenAPIContainer{
			Name:      container.Name,
			Type:      container.Type,
			Image:     container.Image,
			ImageName: container.ImageName,
		})
	}
	return resp
}

func ConvertSaeApplicationToOpenAPISaeApplication(saeApplication *models.SAEApplication) *types.OpenAPISaeApplication {
	if saeApplication == nil {
		return nil
	}

	resp := &types.OpenAPISaeApplication{
		AppName:    saeApplication.AppName,
		AppID:      saeApplication.AppID,
		ImageUrl:   saeApplication.ImageUrl,
		PackageUrl: saeApplication.PackageUrl,
		Instances:  saeApplication.Instances,
	}
	return resp
}

func ConvertUserBriefInfoToOpenAPIUserBriefInfo(userBriefInfo *types.UserBriefInfo) *types.OpenAPIUserBriefInfo {
	if userBriefInfo == nil {
		return nil
	}

	resp := &types.OpenAPIUserBriefInfo{
		UID:          userBriefInfo.UID,
		Account:      userBriefInfo.Account,
		Name:         userBriefInfo.Name,
		IdentityType: userBriefInfo.IdentityType,
	}
	return resp
}
