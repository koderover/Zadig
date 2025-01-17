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

	"go.uber.org/zap"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	systemService "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/system/service"
	"github.com/koderover/zadig/v2/pkg/setting"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	openapitool "github.com/koderover/zadig/v2/pkg/tool/openapi"
	"github.com/koderover/zadig/v2/pkg/types"
)

func OpenAPICreateBuildModule(username string, req *OpenAPIBuildCreationReq, log *zap.SugaredLogger) error {
	buildModule, err := generateBuildModuleFromOpenAPIRequest(req, log)
	if err != nil {
		return err
	}

	return CreateBuild(username, buildModule, log)
}

func OpenAPICreateBuildModuleFromTemplate(username string, req *OpenAPIBuildCreationFromTemplateReq, log *zap.SugaredLogger) error {
	buildModule, err := generateBuildModuleFromOpenAPITemplateRequest(req, log)
	if err != nil {
		return err
	}

	return CreateBuild(username, buildModule, log)
}

func generateBuildModuleFromOpenAPIRequest(req *OpenAPIBuildCreationReq, log *zap.SugaredLogger) (*commonmodels.Build, error) {
	// generate basic build information
	ret := &commonmodels.Build{
		Name:        req.Name,
		Source:      "zadig",
		Description: req.Description,
		Scripts:     req.BuildScript,
		ProductName: req.ProjectName,
		Timeout:     int(req.AdvancedSetting.Timeout),
		// just set this to true always to make the menu open forever
		AdvancedSettingsModified: true,
	}

	// otherwise this is a zadig build, we need to generate a bunch of information from the user input
	// first we generate the pre build info from the user input
	prebuildInfo := &commonmodels.PreBuild{
		CleanWorkspace: false,
		EnableProxy:    false,
		Installs:       req.Addons,
		// for now, file upload is not supported
		UploadPkg: false,
	}

	cluster, err := commonrepo.NewK8SClusterColl().FindByName(req.AdvancedSetting.ClusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to find cluster of name: %s, the error is: %s", req.AdvancedSetting.ClusterName, err)
	}
	prebuildInfo.ClusterID = cluster.ID.Hex()
	if cluster.AdvancedConfig != nil {
		for _, strategy := range cluster.AdvancedConfig.ScheduleStrategy {
			if strategy.StrategyName == req.AdvancedSetting.StrategyName {
				prebuildInfo.StrategyID = strategy.StrategyID
			}
		}
	}

	kvList := make([]*commonmodels.KeyVal, 0)
	for _, kv := range req.Parameters {
		kvList = append(kvList, &commonmodels.KeyVal{
			Key:          kv.Key,
			Value:        kv.DefaultValue,
			Type:         commonmodels.ParameterSettingType(kv.Type),
			ChoiceOption: kv.ChoiceOption,
			Description:  kv.Description,
			IsCredential: kv.IsCredential,
		})
	}
	prebuildInfo.Envs = kvList

	// if there are special image name like bionic or focal, generate special stuff
	imageInfo, err := commonrepo.NewBasicImageColl().FindByImageName(req.ImageName)
	if err != nil {
		return nil, fmt.Errorf("failed to find the basic image of name: %s, error is: %s", req.ImageName, err)
	}
	prebuildInfo.ImageID = imageInfo.ID.Hex()
	if req.ImageName == "bionic" || req.ImageName == "focal" {
		prebuildInfo.ImageFrom = "koderover"
		prebuildInfo.BuildOS = req.ImageName
	} else {
		prebuildInfo.ImageFrom = "custom"
		prebuildInfo.BuildOS = imageInfo.Value
	}
	// generate spec request from user input spec request
	prebuildInfo.ResReqSpec = req.AdvancedSetting.Spec
	prebuildInfo.ResReq = req.AdvancedSetting.Spec.FindResourceRequestType()

	// for now, pre build is done
	ret.PreBuild = prebuildInfo

	//repo info conversion
	repoList := make([]*types.Repository, 0)
	for _, repo := range req.RepoInfo {
		systemRepoInfo, err := openapitool.ToBuildRepository(repo)
		if err != nil {
			log.Errorf("failed to convert user repository input info into system repository, codehostName: [%s], repoName[%s], err: %s", repo.CodeHostName, repo.RepoName, err)
			return nil, fmt.Errorf("failed to convert user repository input info into system repository, codehostName: [%s], repoName[%s], err: %s", repo.CodeHostName, repo.RepoName, err)
		}
		repoList = append(repoList, systemRepoInfo)
	}

	// insert repo info into our build module
	ret.Repos = repoList

	// create target info for the build to work
	targetInfo := make([]*commonmodels.ServiceModuleTarget, 0)
	for _, target := range req.TargetServices {
		targetInfo = append(targetInfo, &commonmodels.ServiceModuleTarget{
			ProductName: req.ProjectName,
			ServiceWithModule: commonmodels.ServiceWithModule{
				ServiceName:   target.ServiceName,
				ServiceModule: target.ServiceModule,
			},
		})
	}
	ret.Targets = targetInfo

	// apply the rest of the advanced settings to the build settings
	ret.CacheEnable = req.AdvancedSetting.CacheSetting.Enabled
	if ret.CacheEnable {
		ret.CacheDirType = "user_defined"
		ret.CacheUserDir = req.AdvancedSetting.CacheSetting.CacheDir
	}

	// generate post build information from user generated input
	postBuildInfo := &commonmodels.PostBuild{
		FileArchive: &commonmodels.FileArchive{FileLocation: req.FileArchivePath},
		Scripts:     req.PostBuildScript,
	}

	if req.DockerBuildInfo != nil {
		dockerBuildInfo := &commonmodels.DockerBuild{
			WorkDir:   req.DockerBuildInfo.WorkingDirectory,
			BuildArgs: req.DockerBuildInfo.BuildArgs,
			Source:    req.DockerBuildInfo.DockerfileType,
		}
		switch req.DockerBuildInfo.DockerfileType {
		case "local":
			dockerBuildInfo.DockerFile = req.DockerBuildInfo.DockerfileDirectory
		case "template":
			dockerBuildInfo.TemplateName = req.DockerBuildInfo.TemplateName
			templateInfo, err := commonrepo.NewDockerfileTemplateColl().GetByName(req.DockerBuildInfo.TemplateName)
			if err != nil {
				return nil, fmt.Errorf("failed to find the dockerfile template of name: [%s], err: %s", req.DockerBuildInfo.TemplateName, err)
			}
			dockerBuildInfo.TemplateID = templateInfo.ID.Hex()
		default:
			return nil, fmt.Errorf("unsupported dockerfile type, it can only be local or template")
		}

		postBuildInfo.DockerBuild = dockerBuildInfo
	}

	ret.PostBuild = postBuildInfo

	return ret, nil
}

func generateBuildModuleFromOpenAPITemplateRequest(req *OpenAPIBuildCreationFromTemplateReq, log *zap.SugaredLogger) (*commonmodels.Build, error) {
	// generate basic build information
	ret := &commonmodels.Build{
		Name:        req.Name,
		Source:      "zadig",
		ProductName: req.ProjectName,
		// just set this to true always to make the menu open forever
		AdvancedSettingsModified: true,
	}

	buildTemplate, err := commonrepo.NewBuildTemplateColl().Find(&commonrepo.BuildTemplateQueryOption{
		Name: req.TemplateName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find the build template specified by the user, template name: [%s], err: %s", req.TemplateName, err)
	}
	ret.TemplateID = buildTemplate.ID.Hex()

	targetRepoInfo := make([]*commonmodels.TargetRepo, 0)

	for _, targetService := range req.TargetServices {
		serviceInfo := &commonmodels.ServiceModuleTargetBase{
			ProductName: req.ProjectName,
			ServiceWithModule: commonmodels.ServiceWithModule{
				ServiceName:   targetService.ServiceName,
				ServiceModule: targetService.ServiceModule,
			},
		}

		repoInfo := make([]*types.Repository, 0)
		for _, serviceRepo := range targetService.RepoInfo {
			systemRepoInfo, err := openapitool.ToBuildRepository(serviceRepo)
			if err != nil {
				log.Errorf("failed to convert user repository input info into system repository, codehostName: [%s], repoName[%s], err: %s", serviceRepo.CodeHostName, serviceRepo.RepoName, err)
				return nil, fmt.Errorf("failed to convert user repository input info into system repository, codehostName: [%s], repoName[%s], err: %s", serviceRepo.CodeHostName, serviceRepo.RepoName, err)
			}
			repoInfo = append(repoInfo, systemRepoInfo)
		}

		kvList := make([]*commonmodels.KeyVal, 0)
		for _, kv := range targetService.Inputs {
			kvList = append(kvList, &commonmodels.KeyVal{
				Key:          kv.Key,
				Value:        kv.Value,
				Type:         commonmodels.ParameterSettingType(kv.Type),
				IsCredential: kv.IsCredential,
			})
		}

		targetRepo := &commonmodels.TargetRepo{
			Service: serviceInfo,
			Repos:   repoInfo,
			Envs:    kvList,
		}

		targetRepoInfo = append(targetRepoInfo, targetRepo)
	}

	ret.TargetRepos = targetRepoInfo

	// for fucking stupid reason, prebuild and postbuild need to be non-empty
	ret.PreBuild = &commonmodels.PreBuild{
		CleanWorkspace: false,
		ResReq:         "low",
		ResReqSpec: setting.RequestSpec{
			CpuLimit:    1000,
			MemoryLimit: 512,
		},
		BuildOS:             "focal",
		ImageFrom:           "koderover",
		ImageID:             "61af6b575c9beafb9ab130dc",
		Installs:            nil,
		Envs:                nil,
		EnableProxy:         false,
		Parameters:          nil,
		UploadPkg:           false,
		ClusterID:           "0123456789abcdef12345678",
		UseHostDockerDaemon: false,
		Namespace:           "",
	}

	ret.PostBuild = &commonmodels.PostBuild{
		DockerBuild:         nil,
		ObjectStorageUpload: nil,
		FileArchive:         nil,
		Scripts:             "",
	}

	return ret, nil
}

func OpenAPIListBuildModules(projectName string, pageNum, pageSize int64, logger *zap.SugaredLogger) (*OpenAPIBuildListResp, error) {
	opt := &commonrepo.BuildListOption{
		ProductName: projectName,
		PageNum:     pageNum,
		PageSize:    pageSize,
	}

	builds, count, err := commonrepo.NewBuildColl().ListByOptions(opt)
	if err != nil {
		logger.Errorf("failed to list build modules, err: %s", err)
		return nil, e.ErrListBuildModule.AddErr(err)
	}

	resp := &OpenAPIBuildListResp{
		Total: count,
	}
	for _, build := range builds {
		buildModule := &OpenAPIBuildBrief{
			Name:        build.Name,
			ProjectName: build.ProductName,
			Source:      build.Source,
			UpdateTime:  build.UpdateTime,
			UpdateBy:    build.UpdateBy,
		}

		services := make([]*commonmodels.ServiceWithModule, 0)
		for _, target := range build.Targets {
			service := &commonmodels.ServiceWithModule{
				ServiceName:   target.ServiceName,
				ServiceModule: target.ServiceModule,
			}

			services = append(services, service)
		}
		buildModule.TargetServices = services
		resp.Builds = append(resp.Builds, buildModule)
	}
	return resp, nil
}

func OpenAPIGetBuildModule(name, serviceName, serviceModule, projectName string, logger *zap.SugaredLogger) (*OpenAPIBuildDetailResp, error) {
	opt := &commonrepo.BuildFindOption{
		Name:        name,
		ProductName: projectName,
	}

	build, err := commonrepo.NewBuildColl().Find(opt)
	if err != nil {
		logger.Errorf("feailed to find build module, buildName:%s, projectName:%s err: %s", name, projectName, err)
		return nil, e.ErrGetBuildModule.AddErr(err)
	}

	resp := &OpenAPIBuildDetailResp{
		Name:        build.Name,
		ProjectName: build.ProductName,
		BuildScript: build.Scripts,
		Source:      build.Source,
		UpdateTime:  build.UpdateTime,
		UpdateBy:    build.UpdateBy,
	}
	if build.TemplateID != "" {
		template, err := commonrepo.NewBuildTemplateColl().Find(&commonrepo.BuildTemplateQueryOption{
			ID: build.TemplateID,
		})
		if err != nil {
			logger.Errorf("feailed to find build template, templateID:%s, err: %s", build.TemplateID, err)
			return nil, e.ErrGetBuildModule.AddErr(err)
		}
		resp.TemplateName = template.Name
	}

	resp.Repos = make([]*OpenAPIRepo, 0)
	if serviceName == "" || serviceModule == "" || build.TemplateID == "" {
		for _, rp := range build.Repos {
			repo := &OpenAPIRepo{
				RepoName:     rp.RepoName,
				Branch:       rp.Branch,
				Source:       rp.Source,
				RepoOwner:    rp.RepoOwner,
				RemoteName:   rp.RemoteName,
				CheckoutPath: rp.CheckoutPath,
				Submodules:   rp.SubModules,
				Hidden:       rp.Hidden,
			}
			resp.Repos = append(resp.Repos, repo)
		}
	} else {
		for _, svcBuild := range build.Targets {
			if svcBuild.ServiceName == serviceName && svcBuild.ServiceModule == serviceModule {
				for _, rp := range svcBuild.Repos {
					repo := &OpenAPIRepo{
						RepoName:     rp.RepoName,
						Branch:       rp.Branch,
						Source:       rp.Source,
						RepoOwner:    rp.RepoOwner,
						RemoteName:   rp.RemoteName,
						CheckoutPath: rp.CheckoutPath,
						Submodules:   rp.SubModules,
						Hidden:       rp.Hidden,
					}
					resp.Repos = append(resp.Repos, repo)
				}

				for _, kv := range svcBuild.Envs {
					newKV := &commonmodels.ServiceKeyVal{
						Key:          kv.Key,
						Value:        kv.Value,
						Type:         kv.Type,
						ChoiceOption: kv.ChoiceOption,
						ChoiceValue:  kv.ChoiceValue,
						IsCredential: kv.IsCredential,
					}

					resp.Parameters = append(resp.Parameters, newKV)
				}

				break
			}
		}
	}

	resp.TargetServices = make([]*commonmodels.ServiceWithModule, 0)
	for _, target := range build.Targets {
		service := &commonmodels.ServiceWithModule{
			ServiceName:   target.ServiceName,
			ServiceModule: target.ServiceModule,
		}
		resp.TargetServices = append(resp.TargetServices, service)
	}

	basic, err := systemService.GetBasicImage(build.PreBuild.ImageID, logger)
	resp.BuildEnv = &OpenAPIBuildEnv{
		BasicImageID:    basic.ID.Hex(),
		BasicImageLabel: basic.Label,
	}
	resp.BuildEnv.Installs = build.PreBuild.Installs

	cluster, err := commonrepo.NewK8SClusterColl().FindByID(build.PreBuild.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to find cluster of ID: %s, the error is: %s", build.PreBuild.ClusterID, err)
	}

	strategy := &commonmodels.ScheduleStrategy{}
	if cluster.AdvancedConfig != nil {
		for _, s := range cluster.AdvancedConfig.ScheduleStrategy {
			if s.StrategyID == build.PreBuild.StrategyID {
				strategy = s
			}
		}
	}
	resp.AdvancedSetting = &types.OpenAPIAdvancedSetting{
		ClusterName:  cluster.Name,
		StrategyName: strategy.StrategyName,
		Timeout:      int64(build.Timeout),
		Spec:         build.PreBuild.ResReqSpec,
		CacheSetting: &types.OpenAPICacheSetting{
			Enabled:  build.CacheEnable,
			CacheDir: build.CacheUserDir,
		},
		UseHostDockerDaemon: build.PreBuild.UseHostDockerDaemon,
	}
	resp.AdvancedSetting.CacheSetting = &types.OpenAPICacheSetting{
		Enabled:  build.CacheEnable,
		CacheDir: build.CacheUserDir,
	}

	if len(resp.Parameters) == 0 {
		resp.Parameters = make([]*commonmodels.ServiceKeyVal, 0)
		for _, kv := range build.PreBuild.Envs {
			resp.Parameters = append(resp.Parameters, &commonmodels.ServiceKeyVal{
				Key:          kv.Key,
				Value:        kv.Value,
				Type:         kv.Type,
				IsCredential: kv.IsCredential,
			})
		}
	}

	resp.Outputs = build.Outputs

	resp.PostBuild = build.PostBuild

	return resp, nil
}
