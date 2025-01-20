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

package job

import (
	"fmt"
	"strings"

	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/types/job"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	aslanUtil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/util"
)

const (
	ENVNAMEKEY  = "envName"
	SERVICESKEY = "services"
)

type DeployJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.ZadigDeployJobSpec
}

func (j *DeployJob) Instantiate() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.setDefaultDeployContent()
	j.job.Spec = j.spec
	return nil
}

func (j *DeployJob) setDefaultDeployContent() {
	if j.spec.DeployContents == nil || len(j.spec.DeployContents) <= 0 {
		j.spec.DeployContents = []config.DeployContent{config.DeployImage}
	}
}

// Deprecated?
func (j *DeployJob) getOriginReferredJobTargets(jobName string) ([]*commonmodels.ServiceAndImage, error) {
	serviceAndImages := []*commonmodels.ServiceAndImage{}
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.Name != j.spec.JobName {
				continue
			}
			if job.JobType == config.JobZadigBuild {
				buildSpec := &commonmodels.ZadigBuildJobSpec{}
				if err := commonmodels.IToi(job.Spec, buildSpec); err != nil {
					return serviceAndImages, err
				}
				for _, build := range buildSpec.ServiceAndBuilds {
					serviceAndImages = append(serviceAndImages, &commonmodels.ServiceAndImage{
						ServiceName:   build.ServiceName,
						ServiceModule: build.ServiceModule,
						Image:         build.Image,
					})
					log.Infof("DeployJob ToJobs getOriginReferredJobTargets: workflow %s service %s, module %s, image %s",
						j.workflow.Name, build.ServiceName, build.ServiceModule, build.Image)
				}
				return serviceAndImages, nil
			}
			if job.JobType == config.JobZadigDistributeImage {
				distributeSpec := &commonmodels.ZadigDistributeImageJobSpec{}
				if err := commonmodels.IToi(job.Spec, distributeSpec); err != nil {
					return serviceAndImages, err
				}
				for _, distribute := range distributeSpec.Targets {
					serviceAndImages = append(serviceAndImages, &commonmodels.ServiceAndImage{
						ServiceName:   distribute.ServiceName,
						ServiceModule: distribute.ServiceModule,
						Image:         distribute.TargetImage,
					})
				}
				return serviceAndImages, nil
			}
			if job.JobType == config.JobZadigDeploy {
				deploySpec := &commonmodels.ZadigDeployJobSpec{}
				if err := commonmodels.IToi(job.Spec, deploySpec); err != nil {
					return serviceAndImages, err
				}
				for _, service := range deploySpec.Services {
					for _, module := range service.Modules {
						serviceAndImages = append(serviceAndImages, &commonmodels.ServiceAndImage{
							ServiceName:   service.ServiceName,
							ServiceModule: module.ServiceModule,
							Image:         module.Image,
						})
					}
				}
				return serviceAndImages, nil
			}
		}
	}
	return nil, fmt.Errorf("qutoed build/deploy job %s not found", jobName)
}

func (j *DeployJob) getReferredJobOrder(serviceReferredJob, imageReferredJob string) ([]*commonmodels.ServiceWithModuleAndImage, error) {
	resp := []*commonmodels.ServiceWithModuleAndImage{}
	found := false

	// first determine the services with order
ServiceOrderLoop:
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.Name != serviceReferredJob {
				continue
			}
			if job.JobType == config.JobZadigBuild {
				buildSpec := &commonmodels.ZadigBuildJobSpec{}
				if err := commonmodels.IToi(job.Spec, buildSpec); err != nil {
					return nil, err
				}

				order := make([]string, 0)
				buildModuleMap := make(map[string][]*commonmodels.DeployModuleInfo)
				for _, build := range buildSpec.ServiceAndBuilds {
					if _, ok := buildModuleMap[build.ServiceName]; !ok {
						buildModuleMap[build.ServiceName] = make([]*commonmodels.DeployModuleInfo, 0)
						order = append(order, build.ServiceName)
					}

					buildModuleMap[build.ServiceName] = append(buildModuleMap[build.ServiceName], &commonmodels.DeployModuleInfo{
						ServiceModule: build.ServiceModule,
						//Image:         build.Image,
						//ImageName:     util.ExtractImageName(build.ImageName),
					})
				}

				for _, serviceName := range order {
					resp = append(resp, &commonmodels.ServiceWithModuleAndImage{
						ServiceName:    serviceName,
						ServiceModules: buildModuleMap[serviceName],
					})
				}
				found = true
				break ServiceOrderLoop
			}

			if job.JobType == config.JobZadigDistributeImage {
				distributeSpec := &commonmodels.ZadigDistributeImageJobSpec{}
				if err := commonmodels.IToi(job.Spec, distributeSpec); err != nil {
					return nil, err
				}
				order := make([]string, 0)
				distributeModuleMap := make(map[string][]*commonmodels.DeployModuleInfo)
				for _, distribute := range distributeSpec.Targets {
					if _, ok := distributeModuleMap[distribute.ServiceName]; !ok {
						distributeModuleMap[distribute.ServiceName] = make([]*commonmodels.DeployModuleInfo, 0)
						order = append(order, distribute.ServiceName)
					}

					distributeModuleMap[distribute.ServiceName] = append(distributeModuleMap[distribute.ServiceName], &commonmodels.DeployModuleInfo{
						ServiceModule: distribute.ServiceModule,
						//Image:         distribute.TargetImage,
						//ImageName:     util.ExtractImageName(distribute.ImageName),
					})
				}

				for _, serviceName := range order {
					resp = append(resp, &commonmodels.ServiceWithModuleAndImage{
						ServiceName:    serviceName,
						ServiceModules: distributeModuleMap[serviceName],
					})
				}
				found = true
				break ServiceOrderLoop
			}

			if job.JobType == config.JobZadigDeploy {
				deploySpec := &commonmodels.ZadigDeployJobSpec{}
				if err := commonmodels.IToi(job.Spec, deploySpec); err != nil {
					return nil, err
				}
				for _, service := range deploySpec.Services {
					resp = append(resp, &commonmodels.ServiceWithModuleAndImage{
						ServiceName:    service.ServiceName,
						ServiceModules: service.Modules,
					})
				}
				found = true
				break ServiceOrderLoop
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("qutoed service referrece of job %s not found", serviceReferredJob)
	}

	// then we determine the image for the selected job, use the output for each module is enough
	for _, svc := range resp {
		for _, module := range svc.ServiceModules {
			// generate real job keys
			key := job.GetJobOutputKey(fmt.Sprintf("%s.%s.%s", imageReferredJob, svc.ServiceName, module.ServiceModule), IMAGEKEY)

			module.Image = key
		}
	}

	return resp, nil
}

// SetPreset sets all info for the user-config env service.
func (j *DeployJob) SetPreset() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.setDefaultDeployContent()
	var err error
	project, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return fmt.Errorf("failed to find project %s, err: %v", j.workflow.Project, err)
	}
	if project.ProductFeature != nil {
		j.spec.DeployType = project.ProductFeature.DeployType
	}
	// if quoted job quote another job, then use the service and image of the quoted job
	if j.spec.Source == config.SourceFromJob {
		j.spec.OriginJobName = j.spec.JobName
	} else if j.spec.Source == config.SourceRuntime {
		envName := strings.ReplaceAll(j.spec.Env, setting.FixedValueMark, "")

		serviceDeployOption, _, err := generateEnvDeployServiceInfo(envName, j.workflow.Project, j.spec, nil)
		if err != nil {
			log.Errorf("failed to generate service deployment info for env: %s, error: %s", envName, err)
			return err
		}

		configuredModulesMap := make(map[string]sets.String)
		for _, module := range j.spec.ServiceAndImages {
			if _, ok := configuredModulesMap[module.ServiceName]; !ok {
				configuredModulesMap[module.ServiceName] = sets.NewString()
			}

			configuredModulesMap[module.ServiceName].Insert(module.ServiceModule)
		}

		svcResp := make([]*commonmodels.DeployServiceInfo, 0)

		for _, svc := range serviceDeployOption {
			if modulesList, ok := configuredModulesMap[svc.ServiceName]; !ok {
				continue
			} else {
				// if configured, delete all the unnecessary modules
				selectedModules := make([]*commonmodels.DeployModuleInfo, 0)
				for _, module := range svc.Modules {
					if modulesList.Has(module.ServiceModule) {
						selectedModules = append(selectedModules, module)
					}
				}

				item := &commonmodels.DeployServiceInfo{
					ServiceName:     svc.ServiceName,
					VariableConfigs: svc.VariableConfigs,
					VariableKVs:     svc.VariableKVs,
					// LatestVariableKVs: svc.LatestVariableKVs,
					VariableYaml:  svc.VariableYaml,
					UpdateConfig:  svc.UpdateConfig,
					Updatable:     svc.Updatable,
					Deployed:      svc.Deployed,
					Modules:       selectedModules,
					KeyVals:       svc.KeyVals,
					LatestKeyVals: svc.LatestKeyVals,
				}

				if !item.Updatable {
					item.UpdateConfig = false
				}

				svcResp = append(svcResp, item)
			}
		}

		j.spec.Services = svcResp
	}

	j.job.Spec = j.spec
	return nil
}

// SetOptions get the service deployment info from ALL envs and set these information into the EnvOptions Field
func (j *DeployJob) SetOptions(approvalTicket *commonmodels.ApprovalTicket) error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ZadigDeployJobSpec)
	found := false
	for _, stage := range latestWorkflow.Stages {
		if !found {
			for _, job := range stage.Jobs {
				if job.Name == j.job.Name && job.JobType == j.job.JobType {
					if err := commonmodels.IToi(job.Spec, latestSpec); err != nil {
						return err
					}
					found = true
					break
				}
			}
		} else {
			break
		}
	}

	if !found {
		return fmt.Errorf("failed to find the original workflow: %s", j.workflow.Name)
	}

	envOptions := make([]*commonmodels.ZadigDeployEnvInformation, 0)

	var allowedServices []*commonmodels.ServiceWithModule
	if approvalTicket != nil {
		allowedServices = approvalTicket.Services
	}

	if strings.HasPrefix(latestSpec.Env, setting.FixedValueMark) {
		// if the env is fixed, we put the env in the option
		envName := strings.ReplaceAll(latestSpec.Env, setting.FixedValueMark, "")

		if approvalTicket == nil || isAllowedEnv(envName, approvalTicket.Envs) {
			serviceInfo, envInfo, err := generateEnvDeployServiceInfo(envName, j.workflow.Project, latestSpec, allowedServices)
			if err != nil {
				log.Errorf("failed to generate service deployment info for env: %s, error: %s", envName, err)
				return err
			}

			envOptions = append(envOptions, &commonmodels.ZadigDeployEnvInformation{
				Env:        envName,
				EnvAlias:   envInfo.Alias,
				Production: envInfo.Production,
				RegistryID: envInfo.RegistryID,
				Services:   serviceInfo,
			})
		}
	} else {
		// otherwise list all the envs in this project
		products, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
			Name:       j.workflow.Project,
			Production: util.GetBoolPointer(j.spec.Production),
		})

		if err != nil {
			log.Errorf("can't list envs in project %s, error: %w", j.workflow.Project, err)
			return err
		}

		for _, env := range products {
			// skip the sleeping envs
			if env.IsSleeping() {
				continue
			}

			if approvalTicket != nil && !isAllowedEnv(env.EnvName, approvalTicket.Envs) {
				continue
			}

			serviceDeployOption, envInfo, err := generateEnvDeployServiceInfo(env.EnvName, j.workflow.Project, latestSpec, allowedServices)
			if err != nil {
				log.Errorf("failed to generate service deployment info for env: %s, error: %s", env.EnvName, err)
				return err
			}

			envOptions = append(envOptions, &commonmodels.ZadigDeployEnvInformation{
				Env:        env.EnvName,
				EnvAlias:   env.Alias,
				Production: env.Production,
				RegistryID: envInfo.RegistryID,
				Services:   serviceDeployOption,
			})
		}
	}

	j.spec.EnvOptions = envOptions
	j.job.Spec = j.spec
	return nil
}

func (j *DeployJob) ClearOptions() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.EnvOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *DeployJob) ClearSelectionField() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	svcResp := make([]*commonmodels.DeployServiceInfo, 0)
	j.spec.Services = svcResp
	j.job.Spec = j.spec
	return nil
}

func (j *DeployJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ZadigDeployJobSpec)
	found := false
	for _, stage := range latestWorkflow.Stages {
		if !found {
			for _, job := range stage.Jobs {
				if job.Name == j.job.Name && job.JobType == j.job.JobType {
					if err := commonmodels.IToi(job.Spec, latestSpec); err != nil {
						return err
					}
					found = true
					break
				}
			}
		} else {
			break
		}
	}

	if !found {
		return fmt.Errorf("failed to find the original workflow: %s", j.workflow.Name)
	}

	j.spec.Production = latestSpec.Production
	project, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return fmt.Errorf("failed to find project %s, err: %v", j.workflow.Project, err)
	}
	if project.ProductFeature != nil {
		j.spec.DeployType = project.ProductFeature.DeployType
	}
	j.spec.SkipCheckRunStatus = latestSpec.SkipCheckRunStatus
	j.spec.DeployContents = latestSpec.DeployContents

	// source is a bit tricky: if the saved args has a source of fromjob, but it has been change to runtime in the config
	// we need to not only update its source but also set services to empty slice.
	if j.spec.Source == config.SourceFromJob && latestSpec.Source == config.SourceRuntime {
		j.spec.Services = make([]*commonmodels.DeployServiceInfo, 0)
	}
	j.spec.Source = latestSpec.Source

	if j.spec.Source == config.SourceFromJob {
		j.spec.OriginJobName = latestSpec.JobName
	}

	// Determine service list and its corresponding kvs
	env := latestSpec.Env
	if strings.HasPrefix(latestSpec.Env, setting.FixedValueMark) {
		env = strings.TrimPrefix(latestSpec.Env, setting.FixedValueMark)
	}

	deployableService, _, err := generateEnvDeployServiceInfo(env, j.workflow.Project, latestSpec, nil)
	if err != nil {
		log.Errorf("failed to generate deployable service from latest workflow spec, err: %s", err)
		return err
	}

	mergedService := make([]*commonmodels.DeployServiceInfo, 0)
	userConfiguredService := make(map[string]*commonmodels.DeployServiceInfo)

	for _, service := range j.spec.Services {
		userConfiguredService[service.ServiceName] = service
	}

	for _, service := range deployableService {
		if userSvc, ok := userConfiguredService[service.ServiceName]; ok {
			for _, kv := range service.VariableKVs {
				for _, customKV := range userSvc.VariableKVs {
					if kv.Key == customKV.Key {
						kv.Value = customKV.Value
					}
				}
			}
			//for _, kv := range service.LatestVariableKVs {
			//	for _, customKV := range userSvc.LatestVariableKVs {
			//		if kv.Key == customKV.Key {
			//			kv.Value = customKV.Value
			//		}
			//	}
			//}

			mergedValues, err := helmtool.MergeOverrideValues("", service.VariableYaml, userSvc.VariableYaml, "", make([]*helmtool.KV, 0))
			if err != nil {
				return fmt.Errorf("failed to merge helm values, error: %s", err)
			}

			calculatedModuleMap := make(map[string]*commonmodels.DeployModuleInfo)
			mergedModules := make([]*commonmodels.DeployModuleInfo, 0)
			for _, module := range service.Modules {
				calculatedModuleMap[module.ServiceModule] = module
			}

			for _, userModule := range userSvc.Modules {
				if _, ok := calculatedModuleMap[userModule.ServiceModule]; ok {
					mergedModules = append(mergedModules, userModule)
				}
			}

			mergedService = append(mergedService, &commonmodels.DeployServiceInfo{
				ServiceName:     service.ServiceName,
				VariableConfigs: service.VariableConfigs,
				VariableKVs:     service.VariableKVs,
				//LatestVariableKVs: service.LatestVariableKVs,
				VariableYaml:  mergedValues,
				UpdateConfig:  userSvc.UpdateConfig,
				Updatable:     service.Updatable,
				Deployed:      service.Deployed,
				Modules:       mergedModules,
				KeyVals:       nil,
				LatestKeyVals: nil,
			})
		} else {
			continue
		}
	}

	j.spec.Services = mergedService
	j.job.Spec = j.spec
	return nil
}

// generateEnvDeployServiceInfo generates the valid deployable service and calculate the visible kvs defined in the spec
func generateEnvDeployServiceInfo(env, project string, spec *commonmodels.ZadigDeployJobSpec, allowedServices []*commonmodels.ServiceWithModule) ([]*commonmodels.DeployServiceInfo, *commonmodels.Product, error) {
	resp := make([]*commonmodels.DeployServiceInfo, 0)
	envInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       project,
		EnvName:    env,
		Production: util.GetBoolPointer(spec.Production),
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to find env: %s in environments, error: %s", env, err)
	}

	projectInfo, err := templaterepo.NewProductColl().Find(project)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find project %s, err: %v", project, err)
	}

	envServiceMap := envInfo.GetServiceMap()

	if projectInfo.IsHostProduct() {
		for _, service := range envServiceMap {
			modules := make([]*commonmodels.DeployModuleInfo, 0)
			for _, module := range service.Containers {
				if isAllowedService(service.ServiceName, module.Name, allowedServices) {
					modules = append(modules, &commonmodels.DeployModuleInfo{
						ServiceModule: module.Name,
						Image:         module.Image,
						ImageName:     commonutil.ExtractImageName(module.Image),
					})
				}
			}

			kvs := make([]*commontypes.RenderVariableKV, 0)

			resp = append(resp, &commonmodels.DeployServiceInfo{
				ServiceName: service.ServiceName,
				VariableKVs: kvs,
				//LatestVariableKVs: make([]*commontypes.RenderVariableKV, 0),
				VariableYaml: service.VariableYaml,
				UpdateConfig: false,
				Updatable:    false,
				Deployed:     true,
				Modules:      modules,
			})
		}
		return resp, envInfo, nil
	}

	serviceDefinitionMap := make(map[string]*commonmodels.Service)
	serviceKVSettingMap := make(map[string][]*commonmodels.DeployVariableConfig)

	updateConfig := false
	for _, contents := range spec.DeployContents {
		if contents == config.DeployConfig {
			updateConfig = true
			break
		}
	}

	svcKVsMap := map[string][]*commonmodels.ServiceKeyVal{}
	deployServiceMap := map[string]*commonmodels.DeployServiceInfo{}

	for _, svc := range spec.Services {
		serviceKVSettingMap[svc.ServiceName] = svc.VariableConfigs
		svcKVsMap[svc.ServiceName] = svc.KeyVals
		deployServiceMap[svc.ServiceName] = svc
	}

	var serviceDefinitions []*commonmodels.Service

	if spec.Production {
		serviceDefinitions, err = commonrepo.NewProductionServiceColl().ListMaxRevisions(&commonrepo.ServiceListOption{
			ProductName: project,
		})
	} else {
		serviceDefinitions, err = commonrepo.NewServiceColl().ListMaxRevisions(&commonrepo.ServiceListOption{
			ProductName: project,
		})
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to list services, error: %s", err)
	}

	for _, service := range serviceDefinitions {
		serviceDefinitionMap[service.ServiceName] = service
	}

	envServices, err := commonservice.ListServicesInEnv(env, project, svcKVsMap, log.SugaredLogger())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list envService, error: %s", err)
	}

	envServiceMap2 := map[string]*commonservice.EnvService{}
	for _, service := range envServices.Services {
		envServiceMap2[service.ServiceName] = service
	}

	/*
	   1. Throw everything in the envs into the response
	   2. Do a scan for the services that is newly created in the service list

	   Additional logics:
	   1. VariableConfig is the field user used to limit the range of kvs workflow user can see, it should not be returned.
	   2. If a new service is about to be added into the env, it bypasses the VariableConfig settings. Users should always see it.
	*/

	for _, service := range envServiceMap {
		modules := make([]*commonmodels.DeployModuleInfo, 0)
		for _, module := range service.Containers {
			if isAllowedService(service.ServiceName, module.Name, allowedServices) {
				modules = append(modules, &commonmodels.DeployModuleInfo{
					ServiceModule: module.Name,
					Image:         module.Image,
					ImageName:     commonutil.ExtractImageName(module.Image),
				})
			}
		}

		kvs := make([]*commontypes.RenderVariableKV, 0)

		for _, kv := range service.GetServiceRender().OverrideYaml.RenderVariableKVs {
			for _, configKV := range serviceKVSettingMap[service.ServiceName] {
				if kv.Key == configKV.VariableKey {
					kvs = append(kvs, kv)
				}
			}
		}

		svcInfo, err := FilterServiceVars(service.ServiceName, spec.DeployContents, deployServiceMap[service.ServiceName], envServiceMap2[service.ServiceName])
		if err != nil {
			return nil, nil, e.ErrFilterWorkflowVars.AddErr(err)
		}

		item := &commonmodels.DeployServiceInfo{
			ServiceName:     service.ServiceName,
			VariableConfigs: serviceKVSettingMap[service.ServiceName],
			// VariableKVs: kvs,
			// LatestVariableKVs: svcInfo.LatestVariableKVs,
			VariableYaml: service.GetServiceRender().OverrideYaml.YamlContent,
			UpdateConfig: updateConfig,
			Updatable:    svcInfo.Updatable,
			Deployed:     true,
			Modules:      modules,
		}

		if !item.Updatable {
			// frontend logic: update_config field need to be false for frontend to use the correct field.
			item.UpdateConfig = false
		}

		resp = append(resp, item)
	}

	for serviceName, service := range serviceDefinitionMap {
		if _, ok := envServiceMap[serviceName]; ok {
			continue
		}

		modules := make([]*commonmodels.DeployModuleInfo, 0)
		for _, module := range service.Containers {
			if isAllowedService(service.ServiceName, module.Name, allowedServices) {
				modules = append(modules, &commonmodels.DeployModuleInfo{
					ServiceModule: module.Name,
					Image:         module.Image,
					ImageName:     commonutil.ExtractImageName(module.Image),
				})
			}
		}

		kvs := make([]*commontypes.RenderVariableKV, 0)
		for _, kv := range service.ServiceVariableKVs {
			kvs = append(kvs, &commontypes.RenderVariableKV{
				ServiceVariableKV: *kv,
				UseGlobalVariable: false,
			})
		}

		resp = append(resp, &commonmodels.DeployServiceInfo{
			ServiceName:  service.ServiceName,
			VariableKVs:  kvs,
			VariableYaml: service.VariableYaml,
			UpdateConfig: true,
			Updatable:    true,
			Deployed:     false,
			Modules:      modules,
		})
	}

	if envInfo.RegistryID == "" {
		registry, err := commonrepo.NewRegistryNamespaceColl().Find(&commonrepo.FindRegOps{
			IsDefault: true,
		})

		if err != nil {
			return nil, nil, fmt.Errorf("failed to find default registry for env: %s, error: %s", env, err)
		}
		envInfo.RegistryID = registry.ID.Hex()
	}

	return resp, envInfo, nil
}

func (j *DeployJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.ZadigDeployJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}

		argsSpec := &commonmodels.ZadigDeployJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Env = argsSpec.Env
		j.spec.Services = argsSpec.Services

		j.job.Spec = j.spec
	}
	return nil
}

func (j *DeployJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.ZadigDeployJobSpec{}

	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.setDefaultDeployContent()
	j.job.Spec = j.spec

	envName := strings.ReplaceAll(j.spec.Env, setting.FixedValueMark, "")
	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: j.workflow.Project, EnvName: envName})
	if err != nil {
		return resp, fmt.Errorf("env %s not exists", envName)
	}

	project, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("failed to find project %s, err: %v", j.workflow.Project, err)
	}

	productServiceMap := product.GetServiceMap()

	// get deploy info from previous build job
	if j.spec.Source == config.SourceFromJob {
		// adapt to the front end, use the direct quoted job name
		if j.spec.OriginJobName != "" {
			j.spec.JobName = j.spec.OriginJobName
		}

		serviceReferredJob := getOriginJobName(j.workflow, j.spec.JobName)
		deployOrder, err := j.getReferredJobOrder(serviceReferredJob, j.spec.JobName)
		if err != nil {
			return resp, fmt.Errorf("get origin refered job: %s targets failed, err: %v", serviceReferredJob, err)
		}

		configurationServiceMap := make(map[string]*commonmodels.DeployServiceInfo)
		for _, svc := range j.spec.Services {
			configurationServiceMap[svc.ServiceName] = svc
		}

		deployServiceMap := make(map[string]*commonmodels.ServiceWithModuleAndImage)
		deployModuleMap := make(map[string]int)
		for _, svc := range deployOrder {
			deployServiceMap[svc.ServiceName] = svc
			for _, module := range svc.ServiceModules {
				key := fmt.Sprintf("%s++%s", svc.ServiceName, module.ServiceModule)
				deployModuleMap[key] = 1
			}
		}

		for _, service := range j.spec.Services {
			moduleList := make([]*commonmodels.DeployModuleInfo, 0)

			deployService, ok := deployServiceMap[service.ServiceName]
			if !ok {
				continue
			}

			for _, module := range deployService.ServiceModules {
				key := fmt.Sprintf("%s++%s", service.ServiceName, module.ServiceModule)
				if _, ok := deployModuleMap[key]; ok {
					moduleList = append(moduleList, module)
				}
			}
			service.Modules = moduleList
		}
	}

	serviceMap := map[string]*commonmodels.DeployServiceInfo{}
	for _, service := range j.spec.Services {
		serviceMap[service.ServiceName] = service
	}

	templateProduct, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("cannot find product %s: %w", j.workflow.Project, err)
	}
	timeout := templateProduct.Timeout * 60

	if j.spec.DeployType == setting.K8SDeployType {
		for jobSubTaskID, svc := range j.spec.Services {
			serviceName := svc.ServiceName
			jobTaskSpec := &commonmodels.JobTaskDeploySpec{
				Env:                envName,
				SkipCheckRunStatus: j.spec.SkipCheckRunStatus,
				ServiceName:        serviceName,
				ServiceType:        setting.K8SDeployType,
				CreateEnvType:      project.ProductFeature.CreateEnvType,
				ClusterID:          product.ClusterID,
				Production:         j.spec.Production,
				DeployContents:     j.spec.DeployContents,
				Timeout:            timeout,
			}

			for _, module := range svc.Modules {
				// if external env, check service exists
				if project.IsHostProduct() {
					if err := checkServiceExsistsInEnv(productServiceMap, serviceName, envName); err != nil {
						return resp, err
					}
				}
				jobTaskSpec.ServiceAndImages = append(jobTaskSpec.ServiceAndImages, &commonmodels.DeployServiceModule{
					Image:         module.Image,
					ImageName:     module.ImageName,
					ServiceModule: module.ServiceModule,
				})
			}
			if !project.IsHostProduct() {
				jobTaskSpec.DeployContents = j.spec.DeployContents
				jobTaskSpec.Production = j.spec.Production
				service := serviceMap[serviceName]
				if service != nil {
					jobTaskSpec.UpdateConfig = service.UpdateConfig
					jobTaskSpec.VariableConfigs = service.VariableConfigs
					jobTaskSpec.VariableKVs = service.VariableKVs

					serviceRender := product.GetSvcRender(serviceName)
					svcRenderVarMap := map[string]*commontypes.RenderVariableKV{}
					for _, varKV := range serviceRender.OverrideYaml.RenderVariableKVs {
						svcRenderVarMap[varKV.Key] = varKV
					}

					// filter variables that used global variable
					filteredKV := []*commontypes.RenderVariableKV{}
					for _, jobKV := range jobTaskSpec.VariableKVs {
						svcKV, ok := svcRenderVarMap[jobKV.Key]
						if !ok {
							// deploy new variable
							filteredKV = append(filteredKV, jobKV)
							continue
						}
						// deploy existed variable
						if svcKV.UseGlobalVariable {
							continue
						}
						filteredKV = append(filteredKV, jobKV)
					}
					jobTaskSpec.VariableKVs = filteredKV
				}
				// if only deploy images, clear keyvals
				if onlyDeployImage(j.spec.DeployContents) {
					jobTaskSpec.VariableConfigs = []*commonmodels.DeployVariableConfig{}
					jobTaskSpec.VariableKVs = []*commontypes.RenderVariableKV{}
				}
			}
			jobTask := &commonmodels.JobTask{
				Key:         genJobKey(j.job.Name, serviceName),
				Name:        GenJobName(j.workflow, j.job.Name, jobSubTaskID),
				DisplayName: genJobDisplayName(j.job.Name, serviceName),
				OriginName:  j.job.Name,
				JobInfo: map[string]string{
					JobNameKey:     j.job.Name,
					"service_name": serviceName,
				},
				JobType:     string(config.JobZadigDeploy),
				Spec:        jobTaskSpec,
				ErrorPolicy: j.job.ErrorPolicy,
			}
			if jobTaskSpec.CreateEnvType == "system" {
				var updateRevision bool
				if slices.Contains(jobTaskSpec.DeployContents, config.DeployConfig) && jobTaskSpec.UpdateConfig {
					updateRevision = true
				}

				varsYaml := ""
				varKVs := []*commontypes.RenderVariableKV{}
				if slices.Contains(jobTaskSpec.DeployContents, config.DeployVars) {
					varsYaml, err = commontypes.RenderVariableKVToYaml(jobTaskSpec.VariableKVs, true)
					if err != nil {
						return nil, errors.Errorf("generate vars yaml error: %v", err)
					}
					varKVs = jobTaskSpec.VariableKVs
				}
				containers := []*commonmodels.Container{}
				if slices.Contains(jobTaskSpec.DeployContents, config.DeployImage) {
					if j.spec.Source == config.SourceFromJob {
						for _, serviceImage := range jobTaskSpec.ServiceAndImages {
							containers = append(containers, &commonmodels.Container{
								Name:      serviceImage.ServiceModule,
								Image:     "{{ NOT BE RENDERED }}",
								ImageName: "{{ NOT BE RENDERED }}",
							})
						}
					} else {
						for _, serviceImage := range jobTaskSpec.ServiceAndImages {
							containers = append(containers, &commonmodels.Container{
								Name:      serviceImage.ServiceModule,
								Image:     serviceImage.Image,
								ImageName: util.ExtractImageName(serviceImage.Image),
							})
						}
					}
				}

				option := &kube.GeneSvcYamlOption{
					ProductName:           j.workflow.Project,
					EnvName:               jobTaskSpec.Env,
					ServiceName:           jobTaskSpec.ServiceName,
					UpdateServiceRevision: updateRevision,
					VariableYaml:          varsYaml,
					VariableKVs:           varKVs,
					Containers:            containers,
				}
				updatedYaml, _, _, err := kube.GenerateRenderedYaml(option)
				if err != nil {
					return nil, errors.Errorf("generate service yaml error: %v", err)
				}
				jobTaskSpec.YamlContent = updatedYaml
			}

			for _, image := range jobTaskSpec.ServiceAndImages {
				log.Infof("DeployJob ToJobs %d: workflow %s service %s, module %s, image %s",
					taskID, j.workflow.Name, serviceName, image.ServiceModule, image.Image)
			}
			resp = append(resp, jobTask)
		}
	}

	if j.spec.DeployType == setting.HelmDeployType {
		for jobSubTaskID, svc := range j.spec.Services {
			var serviceRevision int64
			if pSvc, ok := productServiceMap[svc.ServiceName]; ok {
				serviceRevision = pSvc.Revision
			}

			revisionSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
				ServiceName: svc.ServiceName,
				Revision:    serviceRevision,
				ProductName: product.ProductName,
			}, product.Production)
			if err != nil {
				return nil, fmt.Errorf("failed to find service: %s with revision: %d, err: %s", svc.ServiceName, serviceRevision, err)
			}
			releaseName := util.GeneReleaseName(revisionSvc.GetReleaseNaming(), product.ProductName, product.Namespace, product.EnvName, svc.ServiceName)

			jobTaskSpec := &commonmodels.JobTaskHelmDeploySpec{
				Env:                envName,
				ServiceName:        svc.ServiceName,
				DeployContents:     j.spec.DeployContents,
				SkipCheckRunStatus: j.spec.SkipCheckRunStatus,
				ServiceType:        setting.HelmDeployType,
				ClusterID:          product.ClusterID,
				ReleaseName:        releaseName,
				Timeout:            timeout,
				IsProduction:       j.spec.Production,
			}

			for _, module := range svc.Modules {
				service := serviceMap[svc.ServiceName]
				if service != nil {
					jobTaskSpec.UpdateConfig = service.UpdateConfig
					jobTaskSpec.KeyVals = service.KeyVals
					jobTaskSpec.VariableYaml = service.VariableYaml
					jobTaskSpec.UserSuppliedValue = jobTaskSpec.VariableYaml
				}

				jobTaskSpec.ImageAndModules = append(jobTaskSpec.ImageAndModules, &commonmodels.ImageAndServiceModule{
					ServiceModule: module.ServiceModule,
					Image:         module.Image,
				})
			}
			jobTask := &commonmodels.JobTask{
				Key:         genJobKey(j.job.Name, svc.ServiceName),
				Name:        GenJobName(j.workflow, j.job.Name, jobSubTaskID),
				DisplayName: genJobDisplayName(j.job.Name, svc.ServiceName),
				OriginName:  j.job.Name,
				JobInfo: map[string]string{
					JobNameKey:     j.job.Name,
					"service_name": svc.ServiceName,
				},
				JobType: string(config.JobZadigHelmDeploy),
				Spec:    jobTaskSpec,
			}
			resp = append(resp, jobTask)
		}
	}

	j.job.Spec = j.spec
	return resp, nil
}

func onlyDeployImage(deployContents []config.DeployContent) bool {
	return slices.Contains(deployContents, config.DeployImage) && len(deployContents) == 1
}

func checkServiceExsistsInEnv(serviceMap map[string]*commonmodels.ProductService, serviceName, env string) error {
	if _, ok := serviceMap[serviceName]; !ok {
		return fmt.Errorf("service %s not exists in env %s", serviceName, env)
	}
	return nil
}

// isAllowedEnv calculate if the env is allowed by the allowedEnv restriction.
// note that if allowedEnv is empty, it will be seen as no restrictions
func isAllowedEnv(env string, allowedEnv []string) bool {
	if allowedEnv == nil || len(allowedEnv) == 0 {
		return true
	}

	allowedSets := sets.NewString(allowedEnv...)
	return allowedSets.Has(env)
}

func isAllowedService(serviceName, serviceModule string, allowedServices []*commonmodels.ServiceWithModule) bool {
	if allowedServices == nil || len(allowedServices) == 0 {
		return true
	}

	svcSets := sets.NewString()
	for _, allowedSvc := range allowedServices {
		key := fmt.Sprintf("%s++%s", allowedSvc.ServiceName, allowedSvc.ServiceModule)
		svcSets.Insert(key)
	}

	return svcSets.Has(fmt.Sprintf("%s++%s", serviceName, serviceModule))
}

func (j *DeployJob) LintJob() error {
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if err := aslanUtil.CheckZadigProfessionalLicense(); err != nil {
		if j.spec.Production {
			return e.ErrLicenseInvalid.AddDesc("生产环境功能需要专业版才能使用")
		}

		for _, item := range j.spec.DeployContents {
			if item == config.DeployVars || item == config.DeployConfig {
				return e.ErrLicenseInvalid.AddDesc("基础版仅能部署镜像")
			}
		}
	}
	if j.spec.Source != config.SourceFromJob {
		return nil
	}
	jobRankMap := getJobRankMap(j.workflow.Stages)
	buildJobRank, ok := jobRankMap[j.spec.JobName]
	if !ok || buildJobRank >= jobRankMap[j.job.Name] {
		return fmt.Errorf("can not quote job %s in job %s", j.spec.JobName, j.job.Name)
	}
	return nil
}

func (j *DeployJob) GetOutPuts(log *zap.SugaredLogger) []string {
	resp := []string{}

	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return []string{}
	}
	j.job.Spec = j.spec

	for _, target := range j.spec.ServiceAndImages {
		targetKey := strings.Join([]string{j.job.Name, target.ServiceName, target.ServiceModule}, ".")
		resp = append(resp, getOutputKey(targetKey, []*commonmodels.Output{{Name: "IMAGE"}})...)
	}
	resp = append(resp, getOutputKey(j.job.Name, ensureDeployInOutputs())...)

	outputs := []*commonmodels.Output{}
	outputs = append(outputs, &commonmodels.Output{
		Name: IMAGEKEY,
	})
	resp = append(resp, getOutputKey(j.job.Name+".<SERVICE>.<MODULE>", outputs)...)
	return resp
}

func (j *DeployJob) GetRenderVariables(ctx *internalhandler.Context, jobName, serviceName, moduleName string, getAvaiableVars bool) ([]*commonmodels.KeyVal, error) {
	keyValMap := NewKeyValMap()
	j.spec = &commonmodels.ZadigDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		err = fmt.Errorf("failed to convert freestyle job spec error: %v", err)
		ctx.Logger.Error(err)
		return nil, err
	}

	if getAvaiableVars {
		if j.spec.Source != config.SourceFromJob {
			keyValMap.Insert(&commonmodels.KeyVal{
				Key:   getJobVariableKey(j.job.Name, jobName, "<SERVICE>", "<MODULE>", "IMAGE", getAvaiableVars),
				Value: "",
			})
		}
		keyValMap.Insert(&commonmodels.KeyVal{
			Key:   getJobVariableKey(j.job.Name, jobName, "<SERVICE>", "<MODULE>", "SERVICE_NAME", getAvaiableVars),
			Value: "",
		})
		keyValMap.Insert(&commonmodels.KeyVal{
			Key:   getJobVariableKey(j.job.Name, jobName, "<SERVICE>", "<MODULE>", "SERVICE_MODULE", getAvaiableVars),
			Value: "",
		})
	} else {
		for _, service := range j.spec.Services {
			for _, module := range service.Modules {
				if j.spec.Source != config.SourceFromJob {
					keyValMap.Insert(&commonmodels.KeyVal{
						Key:   getJobVariableKey(j.job.Name, jobName, service.ServiceName, module.ServiceModule, "IMAGE", getAvaiableVars),
						Value: module.Image,
					})
				}
				keyValMap.Insert(&commonmodels.KeyVal{
					Key:   getJobVariableKey(j.job.Name, jobName, service.ServiceName, module.ServiceModule, "SERVICE_NAME", getAvaiableVars),
					Value: service.ServiceName,
				})
				keyValMap.Insert(&commonmodels.KeyVal{
					Key:   getJobVariableKey(j.job.Name, jobName, service.ServiceName, module.ServiceModule, "SERVICE_MODULE", getAvaiableVars),
					Value: module.ServiceModule,
				})
			}
		}
	}

	keyValMap.Insert(&commonmodels.KeyVal{
		Key:   getJobVariableKey(j.job.Name, jobName, "", "", "envName", getAvaiableVars),
		Value: j.spec.Env,
	})

	return keyValMap.List(), nil
}

func ensureDeployInOutputs() []*commonmodels.Output {
	return []*commonmodels.Output{
		{Name: ENVNAMEKEY},
	}
}
