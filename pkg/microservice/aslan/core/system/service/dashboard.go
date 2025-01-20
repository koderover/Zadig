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

package service

import (
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workflowcontroller"
	service2 "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/environment/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow"
	"github.com/koderover/zadig/v2/pkg/microservice/picket/client/opa"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/user"
	"github.com/koderover/zadig/v2/pkg/types"
)

const (
	CardNameRunningWorkflow        = "运行中的工作流"
	CardNameServiceUpdateFrequency = "服务热力图"
)

const (
	CardTypeRunningWorkflow        = "running_workflow"
	CardTypeServiceUpdateFrequency = "service_update_frequency"
	CardTypeMyWorkflow             = "my_workflow"
	CardTypeMyEnv                  = "my_env"
)

func CreateOrUpdateDashboardConfiguration(username, userID string, config *DashBoardConfig, log *zap.SugaredLogger) error {
	cardConfig := make([]*commonmodels.CardConfig, 0)
	for _, cfg := range config.Cards {
		cardConfig = append(cardConfig, &commonmodels.CardConfig{
			ID:     cfg.ID,
			Name:   cfg.Name,
			Type:   cfg.Type,
			Config: cfg.Config,
		})
	}
	dashboardConfig := &commonmodels.DashboardConfig{
		Cards:    cardConfig,
		UserID:   userID,
		UserName: username,
	}

	return commonrepo.NewDashboardConfigColl().CreateOrUpdate(dashboardConfig)
}

func GetDashboardConfiguration(username, userID string, log *zap.SugaredLogger) (*DashBoardConfig, error) {
	cfg, err := commonrepo.NewDashboardConfigColl().GetByUser(username, userID)
	// if there is an error and the error is not empty document then we return error
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		} else {
			return generateDefaultDashboardConfig(), nil
		}
	}
	cardConfig := make([]*DashBoardCardConfig, 0)
	for _, card := range cfg.Cards {
		retConfig := &DashBoardCardConfig{
			ID:     card.ID,
			Name:   card.Name,
			Type:   card.Type,
			Config: card.Config,
		}
		cardConfig = append(cardConfig, retConfig)
	}
	return &DashBoardConfig{Cards: cardConfig}, nil
}

func GetRunningWorkflow(log *zap.SugaredLogger) ([]*WorkflowResponse, error) {
	resp := make([]*WorkflowResponse, 0)
	runningCustomQueue := workflowcontroller.RunningTasks()
	pendingCustomQueue := workflowcontroller.PendingTasks()
	for _, runningtask := range runningCustomQueue {
		res := &WorkflowResponse{
			TaskID:      runningtask.TaskID,
			Name:        runningtask.WorkflowName,
			Project:     runningtask.ProjectName,
			Creator:     runningtask.TaskCreator,
			StartTime:   runningtask.CreateTime,
			Status:      string(runningtask.Status),
			DisplayName: runningtask.WorkflowDisplayName,
			Type:        "common_workflow",
		}
		if runningtask.Type == config.WorkflowTaskTypeTesting {
			res.Type = string(config.WorkflowTaskTypeTesting)
		}
		if runningtask.Type == config.WorkflowTaskTypeScanning {
			res.Type = "scanning"
		}
		resp = append(resp, res)
	}
	for _, pendingTask := range pendingCustomQueue {
		res := &WorkflowResponse{
			TaskID:      pendingTask.TaskID,
			Name:        pendingTask.WorkflowName,
			Project:     pendingTask.ProjectName,
			Creator:     pendingTask.TaskCreator,
			StartTime:   pendingTask.CreateTime,
			Status:      string(pendingTask.Status),
			DisplayName: pendingTask.WorkflowDisplayName,
			Type:        "common_workflow",
		}
		if pendingTask.Type == config.WorkflowTaskTypeTesting {
			res.Type = string(config.WorkflowTaskTypeTesting)
		}
		if pendingTask.Type == config.WorkflowTaskTypeScanning {
			res.Type = "scanning"
		}
		resp = append(resp, res)
	}

	return resp, nil
}

type rule struct {
	method   string
	endpoint string
}

type allowedProjectsData struct {
	Result []string `json:"result"`
}

func GetMyWorkflow(header http.Header, username, userID string, isAdmin bool, cardID string, log *zap.SugaredLogger) ([]*WorkflowResponse, error) {
	resp := make([]*WorkflowResponse, 0)

	cfg, err := commonrepo.NewDashboardConfigColl().GetByUser(username, userID)
	// if there is an error and the error is not empty document then we return error
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		} else {
			// if no config is found, then no my workflow is configured, return empty
			return resp, nil
		}
	}

	// determine the allowed project
	projects := make([]string, 0)
	if !isAdmin {
		authorizedProject, _, err := user.New().ListAuthorizedProjectsByResourceAndVerb(userID, "workflow", types.WorkflowActionView)
		if err != nil {
			log.Errorf("failed to list available project for workflows, error: %s", err)
			return nil, err
		}
		projects = authorizedProject
	} else {
		projects = append(projects, "*")
	}

	workflowList, err := workflow.ListAllAvailableWorkflows(projects, log)
	if err != nil {
		log.Errorf("failed to list all available workflows, error: %s", err)
		return nil, err
	}

	targetMap := make(map[string]int)
	for _, cardCfg := range cfg.Cards {
		if cardCfg.Type == CardTypeMyWorkflow && cardCfg.ID == cardID {
			if cardCfg.Config == nil {
				return resp, nil
			}
			configDetail := new(MyWorkflowCardConfig)
			err := commonmodels.IToi(cardCfg.Config, configDetail)
			if err != nil {
				return nil, err
			}
			for _, item := range configDetail.WorkflowList {
				key := fmt.Sprintf("%s-%s", item.Project, item.Name)
				targetMap[key] = 1
			}
		}
	}
	for _, item := range workflowList {
		key := fmt.Sprintf("%s-%s", item.ProjectName, item.Name)
		if _, ok := targetMap[key]; ok {
			startTime, creator, status := workflow.GetLatestTaskInfo(item)
			resp = append(resp, &WorkflowResponse{
				Name:        item.Name,
				Project:     item.ProjectName,
				Creator:     creator,
				StartTime:   startTime,
				Status:      status,
				DisplayName: item.DisplayName,
				Type:        item.WorkflowType,
			})
		}
	}
	return resp, nil
}

func GetMyEnvironment(projectName, envName string, production bool, username, userID string, log *zap.SugaredLogger) (*EnvResponse, error) {
	cfg, err := commonrepo.NewDashboardConfigColl().GetByUser(username, userID)
	// if there is an error and the error is not empty document then we return error
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		} else {
			// if no config is found, then no my env is configured, return empty
			return nil, nil
		}
	}
	envInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       projectName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}

		log.Infof("failed to get environment info, the error is: %s", err)
		return nil, err
	}
	serviceList := make([]*EnvService, 0)
	vmServiceList := make([]*VMEnvService, 0)

	projectInfo, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		log.Infof("failed to get project info, the error is: %s", err)
		return nil, err
	}

	getImage := func(svc *service.ServiceResp) string {
		image := ""
		if len(svc.Images) > 0 {
			image = svc.Images[0]
		}
		return image
	}

	targetServiceMap := make(map[string]int)
	var targetServiceCount int
	for _, card := range cfg.Cards {
		if card.Type == CardTypeMyEnv {
			envConfig := new(MyEnvCardConfig)
			err := commonmodels.IToi(card.Config, envConfig)
			if err != nil {
				return nil, err
			}
			if envConfig == nil {
				continue
			}
			if envConfig.EnvName == envName && envConfig.ProjectName == projectName {
				for _, svc := range envConfig.ServiceModules {
					targetServiceMap[svc] = 1
				}
				targetServiceCount = len(envConfig.ServiceModules)
				break
			}
		}
	}

	if projectInfo.ProductFeature.BasicFacility == "cloud_host" {
		// if a vm environment is detected, we simply find all the services another way.
		pmSvcList, _, err := service2.ListGroups("", envName, projectName, math.MaxInt, 1, envInfo.Production, log)
		if err != nil {
			log.Errorf("failed to get services in the env, error: %s", err)
			return nil, err
		}

		if targetServiceCount == 0 {
			for _, svc := range pmSvcList {
				entry := &VMEnvService{
					ServiceName: svc.ServiceDisplayName,
					EnvStatus:   svc.EnvStatuses,
				}
				if entry.ServiceName == "" {
					entry.ServiceName = svc.ServiceName
				}
				vmServiceList = append(vmServiceList, entry)
			}
		} else {
			for _, svc := range pmSvcList {
				if _, ok := targetServiceMap[svc.ServiceName]; ok {
					entry := &VMEnvService{
						ServiceName: svc.ServiceDisplayName,
						EnvStatus:   svc.EnvStatuses,
					}
					if entry.ServiceName == "" {
						entry.ServiceName = svc.ServiceName
					}
					vmServiceList = append(vmServiceList, entry)
				}
			}
		}
	} else if projectInfo.ProductFeature.DeployType == "k8s" && projectInfo.ProductFeature.CreateEnvType == "system" {
		// if the project is non-vm & k8s project, then we get the workloads in groups
		svcList, _, err := service2.ListGroups("", envName, projectName, math.MaxInt, 1, envInfo.Production, log)
		if err != nil {
			log.Errorf("failed to get k8s services in the env, error: %s", err)
			return nil, err
		}

		// if none of the service is configured, return all the services
		if targetServiceCount == 0 {
			for _, svc := range svcList {
				entry := &EnvService{
					ServiceName:  svc.ServiceDisplayName,
					WorkloadType: svc.WorkLoadType,
					Status:       svc.Status,
					Image:        svc.Images[0],
				}
				if entry.ServiceName == "" {
					entry.ServiceName = svc.ServiceName
				}
				serviceList = append(serviceList, entry)

			}
		} else {
			for _, svc := range svcList {
				if _, ok := targetServiceMap[svc.ServiceName]; ok {
					entry := &EnvService{
						ServiceName:  svc.ServiceDisplayName,
						WorkloadType: svc.WorkLoadType,
						Status:       svc.Status,
						Image:        getImage(svc),
					}
					if entry.ServiceName == "" {
						entry.ServiceName = svc.ServiceName
					}
					serviceList = append(serviceList, entry)
				}
			}
		}
	} else {
		// if the project is non-vm, we do it normally.
		_, svcList, err := service.ListWorkloadDetailsInEnv(envName, projectName, "", math.MaxInt, 1, log)
		if err != nil {
			log.Errorf("failed to get workloads in the env, error: %s", err)
			return nil, err
		}

		// if none of the service is configured, return all the services
		if targetServiceCount == 0 {
			for _, svc := range svcList {
				entry := &EnvService{
					ServiceName:  svc.ServiceDisplayName,
					WorkloadType: svc.WorkLoadType,
					Status:       svc.Status,
					Image:        getImage(svc),
				}
				if entry.ServiceName == "" {
					entry.ServiceName = svc.ServiceName
				}
				serviceList = append(serviceList, entry)

			}
		} else {
			for _, svc := range svcList {
				if _, ok := targetServiceMap[svc.ServiceName]; ok {
					entry := &EnvService{
						ServiceName:  svc.ServiceDisplayName,
						WorkloadType: svc.WorkLoadType,
						Status:       svc.Status,
						Image:        getImage(svc),
					}
					if entry.ServiceName == "" {
						entry.ServiceName = svc.ServiceName
					}
					serviceList = append(serviceList, entry)
				}
			}
		}
	}

	return &EnvResponse{
		Name:        envName,
		Alias:       envInfo.Alias,
		Production:  envInfo.Production,
		ProjectName: projectName,
		UpdateTime:  envInfo.UpdateTime,
		UpdatedBy:   envInfo.UpdateBy,
		ClusterID:   envInfo.ClusterID,
		Services:    serviceList,
		VMServices:  vmServiceList,
	}, nil
}

func generateDefaultDashboardConfig() *DashBoardConfig {
	cardConfig := make([]*DashBoardCardConfig, 0)
	cardConfig = append(cardConfig, &DashBoardCardConfig{
		Name: CardNameRunningWorkflow,
		Type: CardTypeRunningWorkflow,
	})
	return &DashBoardConfig{Cards: cardConfig}
}

func intersect(s [][]string) []string {
	if len(s) == 0 {
		return nil
	}
	tmp := sets.NewString(s[0]...)
	for _, v := range s[1:] {
		t := sets.NewString(v...)
		tmp = t.Intersection(tmp)
	}
	return tmp.List()
}

func generateOPAInput(header http.Header, method string, endpoint string) *opa.Input {
	authorization := header.Get(strings.ToLower(setting.AuthorizationHeader))
	headers := map[string]string{}
	parsedPath := strings.Split(strings.Trim(endpoint, "/"), "/")
	headers[strings.ToLower(setting.AuthorizationHeader)] = authorization

	return &opa.Input{
		Attributes: &opa.Attributes{
			Request: &opa.Request{HTTP: &opa.HTTPSpec{
				Headers: headers,
				Method:  method,
			}},
		},
		ParsedPath: parsedPath,
	}
}
