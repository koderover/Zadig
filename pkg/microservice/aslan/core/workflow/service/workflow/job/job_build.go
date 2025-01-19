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
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"go.uber.org/zap"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	templ "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/template"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/types/job"
	"github.com/koderover/zadig/v2/pkg/types/step"
)

const (
	IMAGEKEY    = "IMAGE"
	IMAGETAGKEY = "imageTag"
	PKGFILEKEY  = "PKG_FILE"
)

type BuildJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.ZadigBuildJobSpec
}

func (j *BuildJob) Instantiate() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

// SetPreset will clear the selected field (ServiceAndBuilds
func (j *BuildJob) SetPreset() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	servicesMap, err := repository.GetMaxRevisionsServicesMap(j.workflow.Project, false)
	if err != nil {
		return fmt.Errorf("get services map error: %v", err)
	}
	var buildMap sync.Map
	var buildTemplateMap sync.Map
	newBuilds := make([]*commonmodels.ServiceAndBuild, 0)
	for _, build := range j.spec.ServiceAndBuilds {
		var buildInfo *commonmodels.Build
		buildMapValue, ok := buildMap.Load(build.BuildName)
		if !ok {
			buildInfo, err = commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: build.BuildName, ProductName: j.workflow.Project})
			if err != nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				buildMap.Store(build.BuildName, nil)
				continue
			}
			buildMap.Store(build.BuildName, buildInfo)
		} else {
			if buildMapValue == nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				continue
			}
			buildInfo = buildMapValue.(*commonmodels.Build)
		}

		if err := fillBuildDetail(buildInfo, build.ServiceName, build.ServiceModule, &buildTemplateMap); err != nil {
			log.Errorf("fill build: %s detail error: %v", build.BuildName, err)
			continue
		}
		for _, target := range buildInfo.Targets {
			if target.ServiceName == build.ServiceName && target.ServiceModule == build.ServiceModule {
				build.Repos = mergeRepos(buildInfo.Repos, build.Repos)
				build.KeyVals = renderKeyVals(build.KeyVals, buildInfo.PreBuild.Envs)
				break
			}
		}

		build.ImageName = build.ServiceModule
		service, ok := servicesMap[build.ServiceName]
		if !ok {
			log.Errorf("service %s not found", build.ServiceName)
			continue
		}

		for _, container := range service.Containers {
			if container.Name == build.ServiceModule {
				build.ImageName = container.ImageName
				break
			}
		}

		newBuilds = append(newBuilds, build)
	}
	j.spec.ServiceAndBuilds = newBuilds

	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) ClearSelectionField() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	chosenObject := make([]*commonmodels.ServiceAndBuild, 0)

	// some weird logic says we shouldn't clear user's selection if there are only one service in the selection pool.
	if len(j.spec.ServiceAndBuilds) != 1 {
		j.spec.ServiceAndBuilds = chosenObject
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) SetOptions() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.ZadigBuildJobSpec)
	found := false
	for _, stage := range originalWorkflow.Stages {
		if !found {
			for _, job := range stage.Jobs {
				if job.Name == j.job.Name && job.JobType == j.job.JobType {
					if err := commonmodels.IToi(job.Spec, originalSpec); err != nil {
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

	servicesMap, err := repository.GetMaxRevisionsServicesMap(j.workflow.Project, false)
	if err != nil {
		return fmt.Errorf("get services map error: %v", err)
	}

	var buildMap sync.Map
	var buildTemplateMap sync.Map
	newBuilds := make([]*commonmodels.ServiceAndBuild, 0)
	for _, build := range originalSpec.ServiceAndBuilds {
		var buildInfo *commonmodels.Build
		buildMapValue, ok := buildMap.Load(build.BuildName)
		if !ok {
			buildInfo, err = commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: build.BuildName, ProductName: j.workflow.Project})
			if err != nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				buildMap.Store(build.BuildName, nil)
				continue
			}
			buildMap.Store(build.BuildName, buildInfo)
		} else {
			if buildMapValue == nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				continue
			}
			buildInfo = buildMapValue.(*commonmodels.Build)
		}

		if err := fillBuildDetail(buildInfo, build.ServiceName, build.ServiceModule, &buildTemplateMap); err != nil {
			log.Errorf("fill build: %s detail error: %v", build.BuildName, err)
			continue
		}
		for _, target := range buildInfo.Targets {
			if target.ServiceName == build.ServiceName && target.ServiceModule == build.ServiceModule {
				build.Repos = mergeRepos(buildInfo.Repos, build.Repos)
				build.KeyVals = renderKeyVals(build.KeyVals, buildInfo.PreBuild.Envs)
				break
			}
		}

		build.ImageName = build.ServiceModule
		service, ok := servicesMap[build.ServiceName]
		if !ok {
			log.Errorf("service %s not found", build.ServiceName)
			continue
		}

		for _, container := range service.Containers {
			if container.Name == build.ServiceModule {
				build.ImageName = container.ImageName
				break
			}
		}

		newBuilds = append(newBuilds, build)
	}

	j.spec.ServiceAndBuildsOptions = newBuilds
	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) ClearOptions() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.ServiceAndBuildsOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) GetRepos() ([]*types.Repository, error) {
	resp := []*types.Repository{}
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	var (
		err              error
		buildMap         sync.Map
		buildTemplateMap sync.Map
	)
	for _, build := range j.spec.ServiceAndBuilds {
		var buildInfo *commonmodels.Build
		buildMapValue, ok := buildMap.Load(build.BuildName)
		if !ok {
			buildInfo, err = commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: build.BuildName, ProductName: j.workflow.Project})
			if err != nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				buildMap.Store(build.BuildName, nil)
				continue
			}
			buildMap.Store(build.BuildName, buildInfo)
		} else {
			if buildMapValue == nil {
				log.Errorf("find build: %s error: %v", build.BuildName, err)
				continue
			}
			buildInfo = buildMapValue.(*commonmodels.Build)
		}

		if err := fillBuildDetail(buildInfo, build.ServiceName, build.ServiceModule, &buildTemplateMap); err != nil {
			log.Errorf("fill build: %s detail error: %v", build.BuildName, err)
			continue
		}
		for _, target := range buildInfo.Targets {
			if target.ServiceName == build.ServiceName && target.ServiceModule == build.ServiceModule {
				resp = append(resp, mergeRepos(buildInfo.Repos, build.Repos)...)
				break
			}
		}
	}
	return resp, nil
}

func (j *BuildJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.ZadigBuildJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.ZadigBuildJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		newBuilds := []*commonmodels.ServiceAndBuild{}
		for _, build := range j.spec.ServiceAndBuilds {
			for _, argsBuild := range argsSpec.ServiceAndBuilds {
				if build.BuildName == argsBuild.BuildName && build.ServiceName == argsBuild.ServiceName && build.ServiceModule == argsBuild.ServiceModule {
					build.Repos = mergeRepos(build.Repos, argsBuild.Repos)
					build.KeyVals = renderKeyVals(argsBuild.KeyVals, build.KeyVals)
					newBuilds = append(newBuilds, build)
					break
				}
			}
		}
		j.spec.ServiceAndBuilds = newBuilds
		j.job.Spec = j.spec
	}
	return nil
}

func (j *BuildJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ZadigBuildJobSpec)
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

	// save all the user-defined args into a map
	userConfiguredService := make(map[string]*commonmodels.ServiceAndBuild)

	for _, service := range j.spec.ServiceAndBuilds {
		key := fmt.Sprintf("%s++%s", service.ServiceName, service.ServiceModule)
		userConfiguredService[key] = service
	}

	mergedServiceAndBuilds := make([]*commonmodels.ServiceAndBuild, 0)
	var buildTemplateMap sync.Map

	for _, buildInfo := range latestSpec.ServiceAndBuilds {
		key := fmt.Sprintf("%s++%s", buildInfo.ServiceName, buildInfo.ServiceModule)
		// if a service is selected (in the map above) and is in the latest build job config, add it to the list.
		// user defined kv and repo should be merged into the newly created list.
		if userDefinedArgs, ok := userConfiguredService[key]; ok {
			latestBuild, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: buildInfo.BuildName, ProductName: j.workflow.Project})
			if err != nil {
				log.Errorf("find build: %s error: %v", buildInfo.BuildName, err)
				continue
			}

			if err := fillBuildDetail(latestBuild, buildInfo.ServiceName, buildInfo.ServiceModule, &buildTemplateMap); err != nil {
				log.Errorf("fill build: %s detail error: %v", buildInfo.BuildName, err)
				continue
			}

			for _, target := range latestBuild.Targets {
				if target.ServiceName == buildInfo.ServiceName && target.ServiceModule == buildInfo.ServiceModule {
					buildInfo.Repos = mergeRepos(latestBuild.Repos, buildInfo.Repos)
					buildInfo.KeyVals = renderKeyVals(buildInfo.KeyVals, latestBuild.PreBuild.Envs)
					break
				}
			}

			newBuildInfo := &commonmodels.ServiceAndBuild{
				ServiceName:      buildInfo.ServiceName,
				ServiceModule:    buildInfo.ServiceModule,
				BuildName:        buildInfo.BuildName,
				Image:            buildInfo.Image,
				Package:          buildInfo.Package,
				ImageName:        buildInfo.ImageName,
				KeyVals:          renderKeyVals(userDefinedArgs.KeyVals, buildInfo.KeyVals),
				Repos:            mergeRepos(buildInfo.Repos, userDefinedArgs.Repos),
				ShareStorageInfo: buildInfo.ShareStorageInfo,
			}

			mergedServiceAndBuilds = append(mergedServiceAndBuilds, newBuildInfo)
		} else {
			continue
		}
	}

	j.spec.DockerRegistryID = latestSpec.DockerRegistryID
	j.spec.ServiceAndBuilds = mergedServiceAndBuilds
	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) MergeWebhookRepo(webhookRepo *types.Repository) error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	for _, build := range j.spec.ServiceAndBuilds {
		build.Repos = mergeRepos(build.Repos, []*types.Repository{webhookRepo})
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BuildJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	logger := log.SugaredLogger()
	resp := []*commonmodels.JobTask{}

	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec

	registry, _, err := commonservice.FindRegistryById(j.spec.DockerRegistryID, true, logger)
	if err != nil {
		return resp, fmt.Errorf("find docker registry: %s error: %v", j.spec.DockerRegistryID, err)
	}
	defaultS3, err := commonrepo.NewS3StorageColl().FindDefault()
	if err != nil {
		return resp, fmt.Errorf("find default s3 storage error: %v", err)
	}

	var (
		buildMap         sync.Map
		buildTemplateMap sync.Map
	)
	for _, build := range j.spec.ServiceAndBuilds {
		imageTag := commonservice.ReleaseCandidate(build.Repos, taskID, j.workflow.Project, build.ServiceModule, "", build.ImageName, "image")

		image := fmt.Sprintf("%s/%s", registry.RegAddr, imageTag)
		if len(registry.Namespace) > 0 {
			image = fmt.Sprintf("%s/%s/%s", registry.RegAddr, registry.Namespace, imageTag)
		}

		image = strings.TrimPrefix(image, "http://")
		image = strings.TrimPrefix(image, "https://")

		build.Package = fmt.Sprintf("%s.tar.gz", commonservice.ReleaseCandidate(build.Repos, taskID, j.workflow.Project, build.ServiceModule, "", build.ImageName, "tar"))

		var buildInfo *commonmodels.Build
		buildMapValue, ok := buildMap.Load(build.BuildName)
		if !ok {
			buildInfo, err = commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: build.BuildName, ProductName: j.workflow.Project})
			if err != nil {
				return resp, fmt.Errorf("find build: %s error: %v", build.BuildName, err)
			}
			buildMap.Store(build.BuildName, buildInfo)
		} else {
			buildInfo = buildMapValue.(*commonmodels.Build)
		}
		// it only fills build detail created from template
		if err := fillBuildDetail(buildInfo, build.ServiceName, build.ServiceModule, &buildTemplateMap); err != nil {
			return resp, err
		}
		basicImage, err := commonrepo.NewBasicImageColl().Find(buildInfo.PreBuild.ImageID)
		if err != nil {
			return resp, fmt.Errorf("find base image: %s error: %v", buildInfo.PreBuild.ImageID, err)
		}
		registries, err := commonservice.ListRegistryNamespaces("", true, logger)
		if err != nil {
			return resp, err
		}
		outputs := ensureBuildInOutputs(buildInfo.Outputs)
		jobTaskSpec := &commonmodels.JobTaskFreestyleSpec{}
		jobTask := &commonmodels.JobTask{
			Name: jobNameFormat(build.ServiceName + "-" + build.ServiceModule + "-" + j.job.Name),
			JobInfo: map[string]string{
				"service_name":   build.ServiceName,
				"service_module": build.ServiceModule,
				JobNameKey:       j.job.Name,
			},
			Key:            strings.Join([]string{j.job.Name, build.ServiceName, build.ServiceModule}, "."),
			JobType:        string(config.JobZadigBuild),
			Spec:           jobTaskSpec,
			Timeout:        int64(buildInfo.Timeout),
			Outputs:        outputs,
			Infrastructure: buildInfo.Infrastructure,
			VMLabels:       buildInfo.VMLabels,
		}
		jobTaskSpec.Properties = commonmodels.JobProperties{
			Timeout:             int64(buildInfo.Timeout),
			ResourceRequest:     buildInfo.PreBuild.ResReq,
			ResReqSpec:          buildInfo.PreBuild.ResReqSpec,
			CustomEnvs:          renderKeyVals(build.KeyVals, buildInfo.PreBuild.Envs),
			ClusterID:           buildInfo.PreBuild.ClusterID,
			StrategyID:          buildInfo.PreBuild.StrategyID,
			BuildOS:             basicImage.Value,
			ImageFrom:           buildInfo.PreBuild.ImageFrom,
			Registries:          registries,
			ShareStorageDetails: getShareStorageDetail(j.workflow.ShareStorages, build.ShareStorageInfo, j.workflow.Name, taskID),
		}

		jobTaskSpec.Properties.Envs = append(jobTaskSpec.Properties.CustomEnvs, getBuildJobVariables(build, taskID, j.workflow.Project, j.workflow.Name, j.workflow.DisplayName, image, jobTask.Infrastructure, registry, logger)...)
		jobTaskSpec.Properties.UseHostDockerDaemon = buildInfo.PreBuild.UseHostDockerDaemon

		cacheS3 := &commonmodels.S3Storage{}
		if jobTask.Infrastructure == setting.JobVMInfrastructure {
			jobTaskSpec.Properties.CacheEnable = buildInfo.CacheEnable
			jobTaskSpec.Properties.CacheDirType = buildInfo.CacheDirType
			jobTaskSpec.Properties.CacheUserDir = buildInfo.CacheUserDir
		} else {
			clusterInfo, err := commonrepo.NewK8SClusterColl().Get(buildInfo.PreBuild.ClusterID)
			if err != nil {
				return resp, fmt.Errorf("find cluster: %s error: %v", buildInfo.PreBuild.ClusterID, err)
			}

			if clusterInfo.Cache.MediumType == "" {
				jobTaskSpec.Properties.CacheEnable = false
			} else {
				jobTaskSpec.Properties.Cache = clusterInfo.Cache
				jobTaskSpec.Properties.CacheEnable = buildInfo.CacheEnable
				jobTaskSpec.Properties.CacheDirType = buildInfo.CacheDirType
				jobTaskSpec.Properties.CacheUserDir = buildInfo.CacheUserDir
			}

			if jobTaskSpec.Properties.CacheEnable {
				jobTaskSpec.Properties.CacheUserDir = renderEnv(jobTaskSpec.Properties.CacheUserDir, jobTaskSpec.Properties.Envs)
				if jobTaskSpec.Properties.Cache.MediumType == types.NFSMedium {
					jobTaskSpec.Properties.Cache.NFSProperties.Subpath = renderEnv(jobTaskSpec.Properties.Cache.NFSProperties.Subpath, jobTaskSpec.Properties.Envs)
				} else if jobTaskSpec.Properties.Cache.MediumType == types.ObjectMedium {
					cacheS3, err = commonrepo.NewS3StorageColl().Find(jobTaskSpec.Properties.Cache.ObjectProperties.ID)
					if err != nil {
						return resp, fmt.Errorf("find cache s3 storage: %s error: %v", jobTaskSpec.Properties.Cache.ObjectProperties.ID, err)
					}

				}
			}
		}

		// for other job refer current latest image.
		build.Image = job.GetJobOutputKey(jobTask.Key, "IMAGE")
		log.Infof("BuildJob ToJobs %d: workflow %s service %s, module %s, image %s",
			taskID, j.workflow.Name, build.ServiceName, build.ServiceModule, build.Image)

		// init tools install step
		tools := []*step.Tool{}
		for _, tool := range buildInfo.PreBuild.Installs {
			tools = append(tools, &step.Tool{
				Name:    tool.Name,
				Version: tool.Version,
			})
		}
		toolInstallStep := &commonmodels.StepTask{
			Name:     fmt.Sprintf("%s-%s", build.ServiceName, "tool-install"),
			JobName:  jobTask.Name,
			StepType: config.StepTools,
			Spec:     step.StepToolInstallSpec{Installs: tools},
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, toolInstallStep)

		// init download object cache step
		if jobTaskSpec.Properties.CacheEnable && jobTaskSpec.Properties.Cache.MediumType == types.ObjectMedium {
			cacheDir := "/workspace"
			if jobTaskSpec.Properties.CacheDirType == types.UserDefinedCacheDir {
				cacheDir = jobTaskSpec.Properties.CacheUserDir
			}
			downloadArchiveStep := &commonmodels.StepTask{
				Name:     fmt.Sprintf("%s-%s", build.ServiceName, "download-archive"),
				JobName:  jobTask.Name,
				StepType: config.StepDownloadArchive,
				Spec: step.StepDownloadArchiveSpec{
					UnTar:      true,
					IgnoreErr:  true,
					FileName:   setting.BuildOSSCacheFileName,
					ObjectPath: getBuildJobCacheObjectPath(j.workflow.Name, build.ServiceName, build.ServiceModule),
					DestDir:    cacheDir,
					S3:         modelS3toS3(cacheS3),
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, downloadArchiveStep)
		}

		// init git clone step
		repos := renderRepos(build.Repos, buildInfo.Repos, jobTaskSpec.Properties.Envs)
		gitStep := &commonmodels.StepTask{
			Name:     build.ServiceName + "-git",
			JobName:  jobTask.Name,
			StepType: config.StepGit,
			Spec:     step.StepGitSpec{Repos: repos},
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, gitStep)
		// init debug before step
		debugBeforeStep := &commonmodels.StepTask{
			Name:     build.ServiceName + "-debug_before",
			JobName:  jobTask.Name,
			StepType: config.StepDebugBefore,
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, debugBeforeStep)
		// init shell step
		scripts := []string{}
		dockerLoginCmd := `docker login -u "$DOCKER_REGISTRY_AK" -p "$DOCKER_REGISTRY_SK" "$DOCKER_REGISTRY_HOST" &> /dev/null`
		if jobTask.Infrastructure == setting.JobVMInfrastructure {
			scripts = append(scripts, strings.Split(replaceWrapLine(buildInfo.Scripts), "\n")...)
		} else {
			scripts = append([]string{dockerLoginCmd}, strings.Split(replaceWrapLine(buildInfo.Scripts), "\n")...)
			scripts = append(scripts, outputScript(outputs)...)
		}
		scriptStep := &commonmodels.StepTask{
			JobName: jobTask.Name,
		}
		if buildInfo.ScriptType == types.ScriptTypeShell || buildInfo.ScriptType == "" {
			scriptStep.Name = build.ServiceName + "-shell"
			scriptStep.StepType = config.StepShell
			scriptStep.Spec = &step.StepShellSpec{
				Scripts: scripts,
			}
		} else if buildInfo.ScriptType == types.ScriptTypeBatchFile {
			scriptStep.Name = build.ServiceName + "-batchfile"
			scriptStep.StepType = config.StepBatchFile
			scriptStep.Spec = &step.StepBatchFileSpec{
				Scripts: scripts,
			}
		} else if buildInfo.ScriptType == types.ScriptTypePowerShell {
			scriptStep.Name = build.ServiceName + "-powershell"
			scriptStep.StepType = config.StepPowerShell
			scriptStep.Spec = &step.StepPowerShellSpec{
				Scripts: scripts,
			}
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, scriptStep)
		// init debug after step
		debugAfterStep := &commonmodels.StepTask{
			Name:     build.ServiceName + "-debug_after",
			JobName:  jobTask.Name,
			StepType: config.StepDebugAfter,
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, debugAfterStep)
		// init docker build step
		if buildInfo.PostBuild != nil && buildInfo.PostBuild.DockerBuild != nil {
			dockefileContent := ""
			if buildInfo.PostBuild.DockerBuild.TemplateID != "" {
				if dockerfileDetail, err := templ.GetDockerfileTemplateDetail(buildInfo.PostBuild.DockerBuild.TemplateID, logger); err == nil {
					dockefileContent = dockerfileDetail.Content
				}
			}

			dockerBuildStep := &commonmodels.StepTask{
				Name:     build.ServiceName + "-docker-build",
				JobName:  jobTask.Name,
				StepType: config.StepDockerBuild,
				Spec: step.StepDockerBuildSpec{
					Source:                buildInfo.PostBuild.DockerBuild.Source,
					WorkDir:               buildInfo.PostBuild.DockerBuild.WorkDir,
					DockerFile:            buildInfo.PostBuild.DockerBuild.DockerFile,
					ImageName:             image,
					ImageReleaseTag:       imageTag,
					BuildArgs:             buildInfo.PostBuild.DockerBuild.BuildArgs,
					DockerTemplateContent: dockefileContent,
					DockerRegistry: &step.DockerRegistry{
						DockerRegistryID: j.spec.DockerRegistryID,
						Host:             registry.RegAddr,
						UserName:         registry.AccessKey,
						Password:         registry.SecretKey,
						Namespace:        registry.Namespace,
					},
					Repos: repos,
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, dockerBuildStep)
		}

		// init object cache step
		if jobTaskSpec.Properties.CacheEnable && jobTaskSpec.Properties.Cache.MediumType == types.ObjectMedium {
			cacheDir := "/workspace"
			if jobTaskSpec.Properties.CacheDirType == types.UserDefinedCacheDir {
				cacheDir = jobTaskSpec.Properties.CacheUserDir
			}
			tarArchiveStep := &commonmodels.StepTask{
				Name:     fmt.Sprintf("%s-%s", build.ServiceName, "tar-archive"),
				JobName:  jobTask.Name,
				StepType: config.StepTarArchive,
				Spec: step.StepTarArchiveSpec{
					FileName:     setting.BuildOSSCacheFileName,
					ResultDirs:   []string{"."},
					AbsResultDir: true,
					TarDir:       cacheDir,
					ChangeTarDir: true,
					S3DestDir:    getBuildJobCacheObjectPath(j.workflow.Name, build.ServiceName, build.ServiceModule),
					IgnoreErr:    true,
					S3Storage:    modelS3toS3(cacheS3),
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, tarArchiveStep)
		}

		// init archive step
		if buildInfo.PostBuild != nil && buildInfo.PostBuild.FileArchive != nil && buildInfo.PostBuild.FileArchive.FileLocation != "" {
			uploads := []*step.Upload{
				{
					IsFileArchive:       true,
					Name:                build.Package,
					ServiceName:         build.ServiceName,
					ServiceModule:       build.ServiceModule,
					JobTaskName:         jobTask.Name,
					PackageFileLocation: buildInfo.PostBuild.FileArchive.FileLocation,
					FilePath:            path.Join(buildInfo.PostBuild.FileArchive.FileLocation, build.Package),
					DestinationPath:     path.Join(j.workflow.Name, fmt.Sprint(taskID), jobTask.Name, "archive"),
				},
			}
			archiveStep := &commonmodels.StepTask{
				Name:     build.ServiceName + "-archive",
				JobName:  jobTask.Name,
				StepType: config.StepArchive,
				Spec: step.StepArchiveSpec{
					UploadDetail: uploads,
					S3:           modelS3toS3(defaultS3),
					Repos:        repos,
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, archiveStep)
		}

		// init object storage step
		if buildInfo.PostBuild != nil && buildInfo.PostBuild.ObjectStorageUpload != nil && buildInfo.PostBuild.ObjectStorageUpload.Enabled {
			modelS3, err := commonrepo.NewS3StorageColl().Find(buildInfo.PostBuild.ObjectStorageUpload.ObjectStorageID)
			if err != nil {
				return resp, fmt.Errorf("find object storage: %s failed, err: %v", buildInfo.PostBuild.ObjectStorageUpload.ObjectStorageID, err)
			}
			s3 := modelS3toS3(modelS3)
			s3.Subfolder = ""
			uploads := []*step.Upload{}
			for _, detail := range buildInfo.PostBuild.ObjectStorageUpload.UploadDetail {
				uploads = append(uploads, &step.Upload{
					FilePath:        detail.FilePath,
					DestinationPath: detail.DestinationPath,
				})
			}
			archiveStep := &commonmodels.StepTask{
				Name:     build.ServiceName + "-object-storage",
				JobName:  jobTask.Name,
				StepType: config.StepArchive,
				Spec: step.StepArchiveSpec{
					UploadDetail:    uploads,
					ObjectStorageID: buildInfo.PostBuild.ObjectStorageUpload.ObjectStorageID,
					S3:              s3,
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, archiveStep)
		}

		// init post build shell step
		if buildInfo.PostBuild != nil && buildInfo.PostBuild.Scripts != "" {
			scripts := append([]string{dockerLoginCmd}, strings.Split(replaceWrapLine(buildInfo.PostBuild.Scripts), "\n")...)
			shellStep := &commonmodels.StepTask{
				Name:     build.ServiceName + "-post-shell",
				JobName:  jobTask.Name,
				StepType: config.StepShell,
				Spec: &step.StepShellSpec{
					Scripts: scripts,
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, shellStep)
		}
		resp = append(resp, jobTask)
	}
	j.job.Spec = j.spec
	return resp, nil
}

func renderKeyVals(input, origin []*commonmodels.KeyVal) []*commonmodels.KeyVal {
	resp := make([]*commonmodels.KeyVal, 0)

	for _, originKV := range origin {
		item := &commonmodels.KeyVal{
			Key:          originKV.Key,
			Value:        originKV.Value,
			Type:         originKV.Type,
			IsCredential: originKV.IsCredential,
			ChoiceOption: originKV.ChoiceOption,
		}
		for _, inputKV := range input {
			if originKV.Key == inputKV.Key {
				// always use origin credential config.
				item.Value = inputKV.Value
			}
		}
		resp = append(resp, item)
	}
	return resp
}

func renderRepos(input, origin []*types.Repository, kvs []*commonmodels.KeyVal) []*types.Repository {
	for i, originRepo := range origin {
		for _, inputRepo := range input {
			if originRepo.RepoName == inputRepo.RepoName && originRepo.RepoOwner == inputRepo.RepoOwner {
				inputRepo.CheckoutPath = renderEnv(inputRepo.CheckoutPath, kvs)
				if inputRepo.RemoteName == "" {
					inputRepo.RemoteName = "origin"
				}
				origin[i] = inputRepo
			}
		}
	}
	return origin
}

func replaceWrapLine(script string) string {
	return strings.Replace(strings.Replace(
		script,
		"\r\n",
		"\n",
		-1,
	), "\r", "\n", -1)
}

func getBuildJobVariables(build *commonmodels.ServiceAndBuild, taskID int64, project, workflowName, workflowDisplayName, image, infrastructure string, registry *commonmodels.RegistryNamespace, log *zap.SugaredLogger) []*commonmodels.KeyVal {
	ret := make([]*commonmodels.KeyVal, 0)
	// basic envs
	ret = append(ret, PrepareDefaultWorkflowTaskEnvs(project, workflowName, workflowDisplayName, infrastructure, taskID)...)

	// repo envs
	ret = append(ret, getReposVariables(build.Repos)...)
	// build specific envs
	ret = append(ret, &commonmodels.KeyVal{Key: "DOCKER_REGISTRY_HOST", Value: registry.RegAddr, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "DOCKER_REGISTRY_AK", Value: registry.AccessKey, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "DOCKER_REGISTRY_SK", Value: registry.SecretKey, IsCredential: true})

	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE", Value: build.ServiceName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE_NAME", Value: build.ServiceName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE_MODULE", Value: build.ServiceModule, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "IMAGE", Value: image, IsCredential: false})
	buildURL := fmt.Sprintf("%s/v1/projects/detail/%s/pipelines/custom/%s/%d?display_name=%s", configbase.SystemAddress(), project, workflowName, taskID, url.QueryEscape(workflowDisplayName))
	ret = append(ret, &commonmodels.KeyVal{Key: "BUILD_URL", Value: buildURL, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "PKG_FILE", Value: build.Package, IsCredential: false})
	return ret
}

func modelS3toS3(modelS3 *commonmodels.S3Storage) *step.S3 {
	resp := &step.S3{
		Ak:        modelS3.Ak,
		Sk:        modelS3.Sk,
		Endpoint:  modelS3.Endpoint,
		Bucket:    modelS3.Bucket,
		Subfolder: modelS3.Subfolder,
		Insecure:  modelS3.Insecure,
		Provider:  modelS3.Provider,
		Region:    modelS3.Region,
	}
	if modelS3.Insecure {
		resp.Protocol = "http"
	}
	return resp
}

func fillBuildDetail(moduleBuild *commonmodels.Build, serviceName, serviceModule string, buildTemplateMap *sync.Map) error {
	if moduleBuild.TemplateID == "" {
		return nil
	}

	var err error
	var buildTemplate *commonmodels.BuildTemplate
	buildTemplateMapValue, ok := buildTemplateMap.Load(moduleBuild.TemplateID)
	if !ok {
		buildTemplate, err = commonrepo.NewBuildTemplateColl().Find(&commonrepo.BuildTemplateQueryOption{
			ID: moduleBuild.TemplateID,
		})
		if err != nil {
			return fmt.Errorf("failed to find build template with id: %s, err: %s", moduleBuild.TemplateID, err)
		}
		buildTemplateMap.Store(moduleBuild.TemplateID, buildTemplate)
	} else {
		buildTemplate = buildTemplateMapValue.(*commonmodels.BuildTemplate)
	}

	moduleBuild.Timeout = buildTemplate.Timeout
	moduleBuild.PreBuild = buildTemplate.PreBuild
	moduleBuild.JenkinsBuild = buildTemplate.JenkinsBuild
	moduleBuild.ScriptType = buildTemplate.ScriptType
	moduleBuild.Scripts = buildTemplate.Scripts
	moduleBuild.PostBuild = buildTemplate.PostBuild
	moduleBuild.SSHs = buildTemplate.SSHs
	moduleBuild.PMDeployScripts = buildTemplate.PMDeployScripts
	moduleBuild.CacheEnable = buildTemplate.CacheEnable
	moduleBuild.CacheDirType = buildTemplate.CacheDirType
	moduleBuild.CacheUserDir = buildTemplate.CacheUserDir
	moduleBuild.AdvancedSettingsModified = buildTemplate.AdvancedSettingsModified
	moduleBuild.Outputs = buildTemplate.Outputs
	moduleBuild.Infrastructure = buildTemplate.Infrastructure
	moduleBuild.VMLabels = buildTemplate.VmLabels

	// repos are configured by service modules
	for _, serviceConfig := range moduleBuild.Targets {
		if serviceConfig.ServiceName == serviceName && serviceConfig.ServiceModule == serviceModule {
			moduleBuild.Repos = serviceConfig.Repos
			if moduleBuild.PreBuild == nil {
				moduleBuild.PreBuild = &commonmodels.PreBuild{}
			}
			moduleBuild.PreBuild.Envs = commonservice.MergeBuildEnvs(moduleBuild.PreBuild.Envs, serviceConfig.Envs)
			break
		}
	}
	return nil
}

func renderEnv(data string, kvs []*commonmodels.KeyVal) string {
	mapper := func(data string) string {
		for _, envar := range kvs {
			if data != envar.Key {
				continue
			}

			return envar.Value
		}

		return fmt.Sprintf("$%s", data)
	}
	return os.Expand(data, mapper)
}

func mergeRepos(templateRepos []*types.Repository, customRepos []*types.Repository) []*types.Repository {
	customRepoMap := make(map[string]*types.Repository)
	for _, repo := range customRepos {
		if repo.RepoNamespace == "" {
			repo.RepoNamespace = repo.RepoOwner
		}
		repoKey := strings.Join([]string{repo.Source, repo.RepoNamespace, repo.RepoName}, "/")
		customRepoMap[repoKey] = repo
	}
	for _, repo := range templateRepos {
		if repo.RepoNamespace == "" {
			repo.RepoNamespace = repo.RepoOwner
		}
		repoKey := strings.Join([]string{repo.Source, repo.GetRepoNamespace(), repo.RepoName}, "/")
		// user can only set default branch in custom workflow.
		if cv, ok := customRepoMap[repoKey]; ok {
			repo.Branch = cv.Branch
			repo.Tag = cv.Tag
			repo.PR = cv.PR
			repo.PRs = cv.PRs
			repo.FilterRegexp = cv.FilterRegexp
		}
	}
	return templateRepos
}

func (j *BuildJob) LintJob() error {
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	return nil
}

func (j *BuildJob) GetOutPuts(log *zap.SugaredLogger) []string {
	resp := []string{}
	j.spec = &commonmodels.ZadigBuildJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return resp
	}
	for _, build := range j.spec.ServiceAndBuilds {
		jobKey := strings.Join([]string{j.job.Name, build.ServiceName, build.ServiceModule}, ".")
		buildInfo, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: build.BuildName})
		if err != nil {
			log.Errorf("found build %s failed, err: %s", build.BuildName, err)
			continue
		}
		if buildInfo.TemplateID == "" {
			resp = append(resp, getOutputKey(jobKey, ensureBuildInOutputs(buildInfo.Outputs))...)
			continue
		}
		buildTemplate, err := commonrepo.NewBuildTemplateColl().Find(&commonrepo.BuildTemplateQueryOption{ID: buildInfo.TemplateID})
		if err != nil {
			log.Errorf("found build template %s failed, err: %s", buildInfo.TemplateID, err)
			continue
		}
		resp = append(resp, getOutputKey(jobKey, ensureBuildInOutputs(buildTemplate.Outputs))...)
	}
	return resp
}

func ensureBuildInOutputs(outputs []*commonmodels.Output) []*commonmodels.Output {
	keyMap := map[string]struct{}{}
	for _, output := range outputs {
		keyMap[output.Name] = struct{}{}
	}
	if _, ok := keyMap[IMAGEKEY]; !ok {
		outputs = append(outputs, &commonmodels.Output{
			Name: IMAGEKEY,
		})
	}
	if _, ok := keyMap[IMAGETAGKEY]; !ok {
		outputs = append(outputs, &commonmodels.Output{
			Name: IMAGETAGKEY,
		})
	}
	if _, ok := keyMap[PKGFILEKEY]; !ok {
		outputs = append(outputs, &commonmodels.Output{
			Name: PKGFILEKEY,
		})
	}
	return outputs
}

func getBuildJobCacheObjectPath(workflowName, serviceName, serviceModule string) string {
	return fmt.Sprintf("%s/cache/%s/%s", workflowName, serviceName, serviceModule)
}
