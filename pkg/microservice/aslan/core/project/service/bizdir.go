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
	"fmt"
	"strings"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/pm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"helm.sh/helm/v3/pkg/action"
	"k8s.io/apimachinery/pkg/util/sets"
)

type GroupDetail struct {
	GroupName string                        `json:"group_name"`
	Projects  []*commonmodels.ProjectDetail `json:"projects"`
}

func GetBizDirProject() ([]GroupDetail, error) {
	groups, err := commonrepo.NewProjectGroupColl().List()
	if err != nil {
		return nil, e.ErrGetBizDirProject.AddErr(fmt.Errorf("failed to list project groups, error: %v", err))
	}

	groupedProjectSet := sets.NewString()
	resp := []GroupDetail{}
	for _, group := range groups {
		groupDetail := GroupDetail{
			GroupName: group.Name,
		}
		for _, project := range group.Projects {
			groupDetail.Projects = append(groupDetail.Projects, project)
			groupedProjectSet.Insert(project.ProjectKey)
		}
		resp = append(resp, groupDetail)
	}

	projects, err := templaterepo.NewProductColl().List()
	if err != nil {
		return nil, e.ErrGetBizDirProject.AddErr(fmt.Errorf("failed to list projects, error: %v", err))
	}

	ungrouped := GroupDetail{
		GroupName: setting.UNGROUPED,
	}
	for _, project := range projects {
		if !groupedProjectSet.Has(project.ProductName) {
			projectDetail := *&commonmodels.ProjectDetail{
				ProjectName:       project.ProjectName,
				ProjectKey:        project.ProductName,
				ProjectDeployType: project.ProductFeature.GetDeployType(),
			}
			ungrouped.Projects = append(ungrouped.Projects, &projectDetail)
		}
	}
	resp = append(resp, ungrouped)

	return resp, nil
}

func GetBizDirProjectServices(projectName string, labels []string) ([]string, error) {
	svcSet := sets.NewString()
	productTmpl, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		return nil, e.ErrGetBizDirProjectService.AddErr(fmt.Errorf("Can not find project %s, error: %s", projectName, err))
	}

	testServices, err := commonrepo.NewServiceColl().ListMaxRevisionsForServices(productTmpl.AllTestServiceInfos(), "")
	if err != nil {
		return nil, e.ErrGetBizDirProjectService.AddErr(fmt.Errorf("Failed to list testing services by %+v, err: %s", productTmpl.AllTestServiceInfos(), err))
	}
	for _, svc := range testServices {
		svcSet.Insert(svc.ServiceName)
	}

	prodServices, err := commonrepo.NewProductionServiceColl().ListMaxRevisionsByProject(projectName, productTmpl.ProductFeature.DeployType)
	if err != nil {
		return nil, e.ErrGetBizDirProjectService.AddErr(fmt.Errorf("Failed to production list services by %+v, err: %s", projectName, err))
	}
	for _, svc := range prodServices {
		// TODO: change this logic: in patch 3.2.0 there are no production service with labels, so it is safe to ignore all production service
		if len(labels) > 0 {
			continue
		}
		svcSet.Insert(svc.ServiceName)
	}

	return svcSet.List(), nil
}

func SearchBizDirByProject(projectKeyword string, labels []string) ([]GroupDetail, error) {
	groups, err := commonrepo.NewProjectGroupColl().List()
	if err != nil {
		return nil, e.ErrSearchBizDirByProject.AddErr(fmt.Errorf("failed to list project groups, error: %v", err))
	}

	projectGroupMap := make(map[string]string)
	for _, group := range groups {
		for _, project := range group.Projects {
			projectGroupMap[project.ProjectKey] = group.Name
		}
	}

	projects, count, err := templaterepo.NewProductColl().PageListProjectByFilter(
		templaterepo.ProductListByFilterOpt{
			Filter: projectKeyword,
			Skip:   0,
			Limit:  999,
		},
	)
	if err != nil {
		return nil, e.ErrSearchBizDirByProject.AddErr(fmt.Errorf("failed to list projects by filter %s, error: %v", projectKeyword, err))
	}
	if count >= 999 {
		log.Errorf("too many projects(>=999) found by filter %s", projectKeyword)
	}

	projectedTestingServiceMap, err := getProjectServiceByLabel(labels)
	if err != nil {
		return nil, err
	}

	groupProjectMap := make(map[string][]*commonmodels.ProjectDetail)
	resp := []GroupDetail{}
	for _, project := range projects {
		if _, ok := projectedTestingServiceMap[project.Name]; !ok {
			continue
		}
		projectDetail := &commonmodels.ProjectDetail{
			ProjectKey:        project.Name,
			ProjectName:       project.Alias,
			ProjectDeployType: project.ProductFeature.GetDeployType(),
		}
		if projectGroupMap[project.Name] != "" {
			groupProjectMap[projectGroupMap[project.Name]] = append(groupProjectMap[projectGroupMap[project.Name]], projectDetail)
		} else {
			groupProjectMap[setting.UNGROUPED] = append(groupProjectMap[setting.UNGROUPED], projectDetail)
		}
	}
	for group, project := range groupProjectMap {
		groupDetail := GroupDetail{
			GroupName: group,
			Projects:  project,
		}
		resp = append(resp, groupDetail)
	}

	return resp, nil
}

type SearchBizDirByServiceGroup struct {
	GroupName string                          `json:"group_name"`
	Projects  []*SearchBizDirByServiceProject `json:"projects"`
}

type SearchBizDirByServiceProject struct {
	Project  *commonmodels.ProjectDetail `json:"project"`
	Services []string                    `json:"services"`
}

func SearchBizDirByService(serviceName string, labels []string) ([]*SearchBizDirByServiceGroup, error) {
	resp := []*SearchBizDirByServiceGroup{}
	groups, err := commonrepo.NewProjectGroupColl().List()
	if err != nil {
		return nil, e.ErrSearchBizDirByService.AddErr(fmt.Errorf("failed to list project groups, error: %v", err))
	}

	projects, err := templaterepo.NewProductColl().List()
	if err != nil {
		return nil, e.ErrSearchBizDirByService.AddErr(fmt.Errorf("failed to list template projects, error: %v", err))
	}

	templateProjectMap := make(map[string]*template.Product)
	for _, project := range projects {
		templateProjectMap[project.ProductName] = project
	}

	projectGroupMap := make(map[string]string)
	for _, group := range groups {
		for _, project := range group.Projects {
			projectGroupMap[project.ProjectKey] = group.Name
		}
	}

	projectedTestingServiceMap, err := getProjectServiceByLabel(labels)
	if err != nil {
		return nil, err
	}

	groupMap := make(map[string]*SearchBizDirByServiceGroup)
	projectMap := make(map[string]*SearchBizDirByServiceProject)
	addToRespMap := func(service *commonmodels.Service) {
		if _, ok := templateProjectMap[service.ProductName]; !ok {
			log.Warnf("project %s not found for service %s", service.ProductName, service.ServiceName)
			return
		}

		if len(labels) > 0 {
			// TODO: change this logic: in patch 3.2.0 there are no production service with labels, so it is safe to ignore all production service
			if service.Production {
				return
			}

			if _, ok := projectedTestingServiceMap[service.ProductName]; !ok {
				// if service is not shown in label filter, ignore it
				return
			}

			found := false
			for _, svc := range projectedTestingServiceMap[service.ProductName] {
				if svc == service.ServiceName {
					found = true
					break
				}
			}

			if !found {
				return
			}
		}

		groupName, ok := projectGroupMap[service.ProductName]
		if !ok {
			groupName = setting.UNGROUPED
		}
		elemGroup, ok := groupMap[groupName]
		if !ok {
			elemGroup = &SearchBizDirByServiceGroup{
				GroupName: groupName,
			}
			groupMap[groupName] = elemGroup
		}

		if elem, ok := projectMap[service.ProductName]; !ok {
			project := &SearchBizDirByServiceProject{
				Project: &commonmodels.ProjectDetail{
					ProjectKey:        templateProjectMap[service.ProductName].ProductName,
					ProjectName:       templateProjectMap[service.ProductName].ProjectName,
					ProjectDeployType: templateProjectMap[service.ProductName].ProductFeature.DeployType,
				},
				Services: []string{service.ServiceName},
			}
			elemGroup.Projects = append(elemGroup.Projects, project)
			projectMap[service.ProductName] = project
		} else {
			svcSet := sets.NewString(elem.Services...)
			svcSet.Insert(service.ServiceName)
			elem.Services = svcSet.List()
		}
	}

	testServices, err := commonrepo.NewServiceColl().SearchMaxRevisionsByService(serviceName)
	if err != nil {
		return nil, e.ErrSearchBizDirByService.AddErr(fmt.Errorf("Failed to search testing services by service name %v, err: %s", serviceName, err))
	}
	for _, svc := range testServices {
		addToRespMap(svc)
	}

	prodServices, err := commonrepo.NewProductionServiceColl().SearchMaxRevisionsByService(serviceName)
	if err != nil {
		return nil, e.ErrSearchBizDirByService.AddErr(fmt.Errorf("Failed to search production services by service name %v, err: %s", serviceName, err))
	}
	for _, svc := range prodServices {
		addToRespMap(svc)
	}

	for _, elem := range groupMap {
		resp = append(resp, elem)
	}

	return resp, nil
}

type GetBizDirServiceDetailResponse struct {
	ProjectName  string   `json:"project_name"`
	EnvName      string   `json:"env_name"`
	EnvAlias     string   `json:"env_alias"`
	Production   bool     `json:"production"`
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Status       string   `json:"status"`
	Images       []string `json:"images"`
	ChartVersion string   `json:"chart_version"`
	UpdateTime   int64    `json:"update_time"`
	Error        string   `json:"error"`
}

func GetBizDirServiceDetail(projectName, serviceName string) ([]GetBizDirServiceDetailResponse, error) {
	resp := []GetBizDirServiceDetailResponse{}

	project, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		return nil, e.ErrGetBizDirServiceDetail.AddErr(fmt.Errorf("failed to find project %s, error: %v", projectName, err))
	}

	envs, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name: projectName,
	})
	if err != nil {
		return nil, e.ErrGetBizDirServiceDetail.AddErr(fmt.Errorf("failed to list product %s, error: %v", projectName, err))
	}

	for _, env := range envs {
		prodSvc := env.GetServiceMap()[serviceName]
		if prodSvc == nil {
			// not deployed in this env
			continue
		}

		if project.IsK8sYamlProduct() || project.IsHostProduct() {
			detail := GetBizDirServiceDetailResponse{
				ProjectName: env.ProductName,
				EnvName:     env.EnvName,
				EnvAlias:    env.Alias,
				Production:  env.Production,
				Name:        serviceName,
				Type:        setting.K8SDeployType,
				UpdateTime:  prodSvc.UpdateTime,
			}
			serviceTmpl, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
				ServiceName: prodSvc.ServiceName,
				Revision:    prodSvc.Revision,
				ProductName: prodSvc.ProductName,
			}, env.Production)
			if err != nil {
				detail.Error = err.Error()
				log.Warnf("[BIZDIR] failed to get service template for productName: %s, serviceName: %s, revision %d, error: %v",
					prodSvc.ProductName, prodSvc.ServiceName, prodSvc.Revision, err)
				resp = append(resp, detail)
				continue
			}

			cls, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(env.ClusterID)
			if err != nil {
				detail.Error = err.Error()
				log.Warnf("[BIZDIR] failed to get service status & image info due to kube client creation, err: %s", err)
				resp = append(resp, detail)
				continue
			}
			inf, err := clientmanager.NewKubeClientManager().GetInformer(env.ClusterID, env.Namespace)
			if err != nil {
				detail.Error = err.Error()
				log.Warnf("[BIZDIR] failed to get service status & image info due to kube informer creation, err: %s", err)
				resp = append(resp, detail)
				continue
			}

			serviceStatus := commonservice.QueryPodsStatus(env, serviceTmpl, serviceName, cls, inf, log.SugaredLogger())
			detail.Status = serviceStatus.PodStatus
			detail.Images = serviceStatus.Images

			resp = append(resp, detail)
		} else if project.IsHelmProduct() {
			svcToReleaseNameMap, err := commonutil.GetServiceNameToReleaseNameMap(env)
			if err != nil {
				return nil, fmt.Errorf("failed to build release-service map: %s", err)
			}
			releaseName := svcToReleaseNameMap[serviceName]
			if releaseName == "" {
				return nil, fmt.Errorf("release name not found for service %s", serviceName)
			}

			detail := GetBizDirServiceDetailResponse{
				ProjectName: env.ProductName,
				EnvName:     env.EnvName,
				EnvAlias:    env.Alias,
				Production:  env.Production,
				Name:        fmt.Sprintf("%s(%s)", releaseName, serviceName),
				Type:        setting.HelmDeployType,
			}

			helmClient, err := helmtool.NewClientFromNamespace(env.ClusterID, env.Namespace)
			if err != nil {
				log.Errorf("[%s][%s] NewClientFromRestConf error: %s", env.EnvName, projectName, err)
				return nil, fmt.Errorf("failed to init helm client, err: %s", err)
			}

			listClient := action.NewList(helmClient.ActionConfig)
			listClient.Filter = releaseName
			releases, err := listClient.Run()
			if err != nil {
				return nil, e.ErrGetBizDirServiceDetail.AddErr(fmt.Errorf("failed to list helm releases by %s, error: %v", serviceName, err))
			}
			if len(releases) == 0 {
				resp = append(resp, detail)
				continue
			}
			if len(releases) > 1 {
				detail.Error = "helm release number is not equal to 1"
				log.Warnf("helm release number is not equal to 1")
				resp = append(resp, detail)
				continue
			}

			detail.Status = string(releases[0].Info.Status)
			detail.ChartVersion = releases[0].Chart.Metadata.Version
			detail.UpdateTime = releases[0].Info.LastDeployed.Unix()

			resp = append(resp, detail)
		} else if project.IsCVMProduct() {
			detail := GetBizDirServiceDetailResponse{
				ProjectName: env.ProductName,
				EnvName:     env.EnvName,
				EnvAlias:    env.Alias,
				Production:  env.Production,
				Name:        serviceName,
				Type:        project.ProductFeature.DeployType,
				UpdateTime:  prodSvc.UpdateTime,
			}

			serviceTmpl, err := commonservice.GetServiceTemplate(
				prodSvc.ServiceName, setting.PMDeployType, prodSvc.ProductName, "", prodSvc.Revision, false, log.SugaredLogger(),
			)
			if err != nil {
				detail.Error = fmt.Sprintf("failed to get service template for productName: %s, serviceName: %s, revision %d, error: %v",
					prodSvc.ProductName, prodSvc.ServiceName, prodSvc.Revision, err)
				log.Warnf("failed to get service template for productName: %s, serviceName: %s, revision %d, error: %v",
					prodSvc.ProductName, prodSvc.ServiceName, prodSvc.Revision, err)
				resp = append(resp, detail)
				continue
			}

			if len(serviceTmpl.EnvStatuses) > 0 {
				envStatuses := make([]*commonmodels.EnvStatus, 0)
				filterEnvStatuses, err := pm.GenerateEnvStatus(serviceTmpl.EnvConfigs, log.NopSugaredLogger())
				if err != nil {
					detail.Error = fmt.Sprintf("failed to generate env status for productName: %s, serviceName: %s, revision %d, error: %v", prodSvc.ProductName, prodSvc.ServiceName, prodSvc.Revision, err)
					log.Warnf("failed to generate env status for productName: %s, serviceName: %s, revision %d, error: %v", prodSvc.ProductName, prodSvc.ServiceName, prodSvc.Revision, err)
					resp = append(resp, detail)
					continue
				}
				filterEnvStatusSet := sets.NewString()
				for _, v := range filterEnvStatuses {
					filterEnvStatusSet.Insert(v.Address)
				}
				for _, envStatus := range serviceTmpl.EnvStatuses {
					if envStatus.EnvName == env.EnvName && filterEnvStatusSet.Has(envStatus.Address) {
						envStatuses = append(envStatuses, envStatus)
					}
				}

				if len(envStatuses) > 0 {
					total := 0
					running := 0
					for _, envStatus := range envStatuses {
						total++
						if envStatus.Status == setting.PodRunning {
							running++
						}
					}
					detail.Status = fmt.Sprintf("%d/%d", running, total)
				}
			}

			resp = append(resp, detail)
		}

	}

	return resp, nil
}

func getProjectServiceByLabel(labels []string) (map[string][]string, error) {
	resp := make(map[string][]string)

	labelFilter := make(map[string]string)

	labelKeys := make([]string, 0)

	for _, lb := range labels {
		kvs := strings.Split(lb, ":")
		if len(kvs) < 2 {
			log.Errorf("cannot query label without value")
			return nil, fmt.Errorf("cannot query label without value")
		}

		key := kvs[0]

		labelKeys = append(labelKeys, key)
	}

	labelsettings, err := commonrepo.NewLabelColl().List(&commonrepo.LabelListOption{Keys: labelKeys})
	if err != nil {
		log.Errorf("failed to list label settings, error: %s", err)
		return nil, fmt.Errorf("failed to list label settings, error: %s", err)
	}

	labelIDMap := make(map[string]string)

	for _, labelSetting := range labelsettings {
		labelIDMap[labelSetting.Key] = labelSetting.ID.Hex()
	}

	for _, lb := range labels {
		kvs := strings.Split(lb, ":")
		if len(kvs) < 2 {
			log.Errorf("cannot query label without value")
			return nil, fmt.Errorf("cannot query label without value")
		}

		key := kvs[0]
		value := strings.Join(kvs[1:], ":")

		labelFilter[labelIDMap[key]] = value
	}

	boundService, err := commonrepo.NewLabelBindingColl().ListService(&commonrepo.LabelBindingListOption{LabelFilter: labelFilter})
	if err != nil {
		log.Errorf("failed to list label bindings for labels, error: %s", err)
		return nil, fmt.Errorf("failed to list label bindings for labels, error: %s", err)
	}
	for _, label := range boundService {
		// TODO: currently labels can only be bound by testing service, ignoring production service.
		if _, ok := resp[label.ProjectKey]; !ok {
			resp[label.ProjectKey] = make([]string, 0)
		}
		resp[label.ProjectKey] = append(resp[label.ProjectKey], label.ServiceName)
	}

	return resp, nil
}
