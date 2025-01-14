/*
Copyright 2023 The KodeRover Authors.

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
	"context"
	"fmt"

	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
)

type ListEnvServiceVersionsResponse struct {
	ServiceName string                    `json:"service_name"`
	Revision    int64                     `json:"revision"`
	Containers  []*commonmodels.Container `json:"containers"`
	CreateTime  int64                     `json:"create_time"`
	CreateBy    string                    `json:"create_by"`
}

func ListEnvServiceVersions(ctx *internalhandler.Context, projectName, envName, serviceName string, isHelmChart, isProduction bool, log *zap.SugaredLogger) ([]ListEnvServiceVersionsResponse, error) {
	resp := []ListEnvServiceVersionsResponse{}
	revisions, err := mongodb.NewEnvServiceVersionColl().ListServiceVersions(projectName, envName, serviceName, isHelmChart, isProduction)
	if err != nil {
		return nil, e.ErrListEnvServiceVersions.AddErr(fmt.Errorf("failed to list service revisions, error: %v", err))
	}

	for _, revision := range revisions {
		name := revision.Service.ServiceName
		if isHelmChart {
			name = revision.Service.ReleaseName
		}
		resp = append(resp, ListEnvServiceVersionsResponse{
			ServiceName: name,
			Revision:    revision.Revision,
			Containers:  revision.Service.Containers,
			CreateTime:  revision.CreateTime,
			CreateBy:    revision.CreateBy,
		})
	}
	return resp, nil
}

type GetEnvServiceVersionYamlResponse struct {
	Type         string `json:"type"`
	Yaml         string `json:"yaml"`
	VariableYaml string `json:"variable_yaml"`
	OverrideKVs  string `json:"override_kvs"`
}

func GetEnvServiceVersionYaml(ctx *internalhandler.Context, projectName, envName, serviceName string, revision int64, isHelmChart, isProduction bool, log *zap.SugaredLogger) (GetEnvServiceVersionYamlResponse, error) {
	resp := GetEnvServiceVersionYamlResponse{}

	envSvcRevision, err := mongodb.NewEnvServiceVersionColl().Find(projectName, envName, serviceName, isHelmChart, isProduction, revision)
	if err != nil {
		return resp, e.ErrDiffEnvServiceVersions.AddErr(fmt.Errorf("failed to find %s/%s/%s service for revision %d, isHelmChart %v isProduction %v, error: %v", projectName, envName, serviceName, revision, isHelmChart, isProduction, err))
	}
	resp.Type = envSvcRevision.Service.Type
	if envSvcRevision.Service.Type == setting.K8SDeployType {
		fakeEnv := &commonmodels.Product{
			ProductName: envSvcRevision.ProductName,
			EnvName:     envSvcRevision.EnvName,
			Namespace:   envSvcRevision.Namespace,
			Production:  envSvcRevision.Production,
		}
		parsedYaml, err := kube.RenderEnvService(fakeEnv, envSvcRevision.Service.GetServiceRender(), envSvcRevision.Service)
		if err != nil {
			err = fmt.Errorf("Failed to render env Service %s, error: %v", envSvcRevision.Service.ServiceName, err)
			return resp, e.ErrDiffEnvServiceVersions.AddErr(err)
		}
		resp.Yaml = parsedYaml
		resp.VariableYaml = envSvcRevision.Service.Render.GetOverrideYaml()
	} else if envSvcRevision.Service.Type == setting.HelmDeployType {
		resp.VariableYaml, err = helmservice.NewHelmDeployService().NewGeneMergedValues(envSvcRevision.Service, envSvcRevision.DefaultValues, nil)
		if err != nil {
			return resp, e.ErrDiffEnvServiceVersions.AddErr(fmt.Errorf("failed to merged values for %s/%s/%s service for version %d, isProduction %v, error: %v", projectName, envName, serviceName, revision, isProduction, err))
		}
		resp.OverrideKVs = envSvcRevision.Service.GetServiceRender().OverrideValues
	} else if envSvcRevision.Service.Type == setting.HelmChartDeployType {
		chartRepoName := envSvcRevision.Service.GetServiceRender().ChartRepo
		chartName := envSvcRevision.Service.GetServiceRender().ChartName
		chartVersion := envSvcRevision.Service.GetServiceRender().ChartVersion
		chartRepo, err := commonrepo.NewHelmRepoColl().Find(&commonrepo.HelmRepoFindOption{RepoName: chartRepoName})
		if err != nil {
			return resp, e.ErrDiffEnvServiceVersions.AddErr(fmt.Errorf("failed to query chart-repo info, repoName: %s", chartRepoName))
		}

		client, err := commonutil.NewHelmClient(chartRepo)
		if err != nil {
			return resp, e.ErrDiffEnvServiceVersions.AddErr(fmt.Errorf("failed to new helm client, err %s", err))
		}

		valuesYaml, err := client.GetChartValues(commonutil.GeneHelmRepo(chartRepo), projectName, serviceName, chartRepoName, chartName, chartVersion, isProduction)
		if err != nil {
			return resp, e.ErrDiffEnvServiceVersions.AddErr(fmt.Errorf("failed to get chart values, chartRepo: %s, chartName: %s, chartVersion: %s, err %s", chartRepoName, chartName, chartVersion, err))
		}

		helmDeploySvc := helmservice.NewHelmDeployService()
		mergedValues, err := helmDeploySvc.NewGeneMergedValues(envSvcRevision.Service, envSvcRevision.DefaultValues, nil)
		if err != nil {
			return resp, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to merge values, err %s", err))
		}
		mergedValues, err = helmDeploySvc.GeneFullValues(valuesYaml, mergedValues)
		if err != nil {
			return resp, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to generate full values, err %s", err))
		}
		resp.VariableYaml = mergedValues
	}

	return resp, nil
}

type DiffEnvServiceVersionsResponse struct {
	Type          string `json:"type"`
	YamlA         string `json:"yaml_a"`
	YamlB         string `json:"yaml_b"`
	VariableYamlA string `json:"variable_yaml_a"`
	VariableYamlB string `json:"variable_yaml_b"`
}

func DiffEnvServiceVersions(ctx *internalhandler.Context, projectName, envName, serviceName string, revisionA, revisionB int64, isHelmChart, isProduction bool, log *zap.SugaredLogger) (DiffEnvServiceVersionsResponse, error) {
	resp := DiffEnvServiceVersionsResponse{}

	respA, err := GetEnvServiceVersionYaml(ctx, projectName, envName, serviceName, revisionA, isHelmChart, isProduction, log)
	if err != nil {
		return resp, err
	}
	resp.Type = respA.Type
	resp.YamlA = respA.Yaml
	resp.VariableYamlA = respA.VariableYaml

	respB, err := GetEnvServiceVersionYaml(ctx, projectName, envName, serviceName, revisionB, isHelmChart, isProduction, log)
	if err != nil {
		return resp, err
	}
	resp.YamlB = respB.Yaml
	resp.VariableYamlB = respB.VariableYaml

	return resp, nil
}

func RollbackEnvServiceVersion(ctx *internalhandler.Context, projectName, envName, serviceName string, revision int64, isHelmChart, isProduction bool, log *zap.SugaredLogger) error {
	envSvcVersion, err := mongodb.NewEnvServiceVersionColl().Find(projectName, envName, serviceName, isHelmChart, isProduction, revision)
	if err != nil {
		return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to find %s/%s/%s service for revision %d, isProduction %v, error: %v", projectName, envName, serviceName, revision, isProduction, err))
	}

	env, err := mongodb.NewProductColl().Find(&mongodb.ProductFindOptions{
		Name:       projectName,
		EnvName:    envName,
		Production: &isProduction,
	})
	if err != nil {
		return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to find %s/%s env, isProduction %v, error: %v", projectName, envName, isProduction, err))
	}

	preProdSvc := env.GetServiceMap()[serviceName]
	if preProdSvc == nil {
		if envSvcVersion.Service.Type == setting.HelmChartDeployType {
			preProdSvc = env.GetChartServiceMap()[serviceName]
		} else {
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to find service %s in env %s", envSvcVersion.Service.ServiceName, envSvcVersion.EnvName))
		}
	}

	session := mongotool.Session()
	defer session.EndSession(context.Background())

	if envSvcVersion.Service.Type == setting.K8SDeployType {
		kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(env.ClusterID)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}

		istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(env.ClusterID)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}

		informer, err := clientmanager.NewKubeClientManager().GetInformer(env.ClusterID, env.Namespace)
		if err != nil {
			log.Errorf("[%s][%s] error: %v", envName, env.Namespace, err)
			return e.ErrRollbackEnvServiceVersion.AddDesc(err.Error())
		}

		fakeEnv := &commonmodels.Product{
			ProductName: envSvcVersion.ProductName,
			EnvName:     envSvcVersion.EnvName,
			Namespace:   envSvcVersion.Namespace,
			Production:  envSvcVersion.Production,
		}
		parsedYaml, err := kube.RenderEnvService(fakeEnv, envSvcVersion.Service.GetServiceRender(), envSvcVersion.Service)
		if err != nil {
			err = fmt.Errorf("Failed to render env %s, service %s, revision %d, error: %v", envSvcVersion.EnvName, envSvcVersion.Service.ServiceName, envSvcVersion.Service.Revision, err)
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}
		envSvcVersion.Service.RenderedYaml = parsedYaml

		preResourceYaml, err := kube.RenderEnvService(env, preProdSvc.GetServiceRender(), preProdSvc)
		if err != nil {
			err = fmt.Errorf("Failed to render env %s, service %s, revision %d, error: %v", envSvcVersion.EnvName, envSvcVersion.Service.ServiceName, envSvcVersion.Service.Revision, err)
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}
		preProdSvc.RenderedYaml = preResourceYaml

		err = kube.CheckResourceAppliedByOtherEnv(parsedYaml, env, envSvcVersion.Service.ServiceName)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}

		resourceApplyParam := &kube.ResourceApplyParam{
			ProductInfo:         env,
			ServiceName:         envSvcVersion.Service.ServiceName,
			CurrentResourceYaml: preResourceYaml,
			UpdateResourceYaml:  parsedYaml,
			Informer:            informer,
			KubeClient:          kubeClient,
			IstioClient:         istioClient,
			InjectSecrets:       true,
			Uninstall:           false,
			AddZadigLabel:       !isProduction,
			SharedEnvHandler:    EnsureUpdateZadigService,
		}

		unstructuredList, err := kube.CreateOrPatchResource(resourceApplyParam, log)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to create or patch resource for env %s, service %s, revision %d, error: %v", envSvcVersion.EnvName, envSvcVersion.Service.ServiceName, envSvcVersion.Service.Revision, err))
		}

		err = mongotool.StartTransaction(session)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}

		groupIndex := -1
		svcIndex := -1
		env.LintServices()
		for i, group := range env.Services {
			for j, svc := range group {
				if svc.ServiceName == envSvcVersion.Service.ServiceName {
					svcIndex = j
					groupIndex = i
					svc.Resources = kube.UnstructuredToResources(unstructuredList)
					for _, kv := range envSvcVersion.Service.GetServiceRender().OverrideYaml.RenderVariableKVs {
						kv.UseGlobalVariable = false
					}
					break
				}
			}
		}
		if groupIndex < 0 || svcIndex < 0 {
			mongotool.AbortTransaction(session)
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to find service %s in env %s/%s, isProudction %v", envSvcVersion.Service.ServiceName, envSvcVersion.ProductName, envSvcVersion.EnvName, envSvcVersion.Production))
		}

		env.Services[groupIndex][svcIndex] = envSvcVersion.Service
		err = helmservice.UpdateServiceInEnv(env, envSvcVersion.Service, ctx.UserName)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to update service %s in env %s/%s, isProudction %v", envSvcVersion.Service.ServiceName, envSvcVersion.ProductName, envSvcVersion.EnvName, envSvcVersion.Production))
		}

		// create rollback record
		rollbackRecord := &commonmodels.EnvInfo{
			ProjectName:   projectName,
			EnvName:       envName,
			EnvType:       config.EnvTypeZadig,
			Production:    env.Production,
			Operation:     config.EnvOperationRollback,
			OperationType: config.EnvOperationTypeZadig,
			ServiceName:   serviceName,
			ServiceType:   envSvcVersion.Service.GetServiceType(),
			OriginService: preProdSvc,
			UpdateService: envSvcVersion.Service,
		}
		err = mongodb.NewEnvInfoCollWithSession(session).Create(ctx, rollbackRecord)
		if err != nil {
			mongotool.AbortTransaction(session)
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to create rollback record for env %s/%s, isProudction %v, service: %s, err: %v", envSvcVersion.ProductName, envSvcVersion.EnvName, envSvcVersion.Production, serviceName, err))
		}

		for _, globalKV := range env.GlobalVariables {
			relatedServiceSet := sets.NewString(globalKV.RelatedServices...)
			if relatedServiceSet.Has(serviceName) {
				relatedServiceSet.Delete(serviceName)
			}
			globalKV.RelatedServices = relatedServiceSet.List()
		}
		err = commonrepo.NewProductCollWithSession(session).UpdateGlobalVariable(env)
		if err != nil {
			mongotool.AbortTransaction(session)
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to update global variables in env %s/%s, isProudction %v", envSvcVersion.ProductName, envSvcVersion.EnvName, envSvcVersion.Production))
		}
		err = mongotool.CommitTransaction(session)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}
	} else if envSvcVersion.Service.Type == setting.HelmDeployType || envSvcVersion.Service.Type == setting.HelmChartDeployType {
		var svcTmpl *commonmodels.Service
		if envSvcVersion.Service.Type == setting.HelmDeployType {
			svcTmpl, err = mongodb.NewServiceColl().Find(&mongodb.ServiceFindOption{
				ProductName: envSvcVersion.ProductName,
				ServiceName: envSvcVersion.Service.ServiceName,
				Type:        envSvcVersion.Service.Type,
				Revision:    envSvcVersion.Service.Revision,
			})
			if err != nil {
				return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to find service temlate %s/%s/%d, error: %v", envSvcVersion.EnvName, envSvcVersion.Service.ServiceName, envSvcVersion.Service.Revision, err))
			}
		}

		finalValuesYaml, err := helmservice.NewHelmDeployService().NewGeneMergedValues(preProdSvc, envSvcVersion.DefaultValues, nil)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to generate merged values yaml, err: %s", err))
		}
		envSvcVersion.Service.GetServiceRender().SetOverrideYaml(finalValuesYaml)

		env.DefaultValues = ""
		err = kube.DeploySingleHelmRelease(env, envSvcVersion.Service, svcTmpl, nil, 0, ctx.UserName)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to upgrade helm release for env %s, service %s, revision %d, error: %v", envSvcVersion.EnvName, envSvcVersion.Service.ServiceName, envSvcVersion.Service.Revision, err))
		}

		rollbackRecord := &commonmodels.EnvInfo{
			ProjectName:   projectName,
			EnvName:       envName,
			EnvType:       config.EnvTypeZadig,
			Production:    env.Production,
			Operation:     config.EnvOperationRollback,
			OperationType: config.EnvOperationTypeZadig,
			ServiceName:   serviceName,
			ServiceType:   envSvcVersion.Service.GetServiceType(),
			OriginService: preProdSvc,
			UpdateService: envSvcVersion.Service,
		}
		err = mongodb.NewEnvInfoCollWithSession(session).Create(ctx, rollbackRecord)
		if err != nil {
			mongotool.AbortTransaction(session)
			return e.ErrRollbackEnvServiceVersion.AddErr(fmt.Errorf("failed to create rollback record for env %s/%s, isProudction %v, service: %s", envSvcVersion.ProductName, envSvcVersion.EnvName, envSvcVersion.Production, serviceName))
		}

		err = mongotool.CommitTransaction(session)
		if err != nil {
			return e.ErrRollbackEnvServiceVersion.AddErr(err)
		}
	}

	return nil
}
