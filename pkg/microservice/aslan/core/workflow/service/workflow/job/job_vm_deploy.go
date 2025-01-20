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
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/koderover/zadig/v2/pkg/util"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	codehostdb "github.com/koderover/zadig/v2/pkg/microservice/systemconfig/core/codehost/repository/mongodb"
	codehostrepo "github.com/koderover/zadig/v2/pkg/microservice/systemconfig/core/codehost/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/types/step"
)

type VMDeployJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.ZadigVMDeployJobSpec
}

func (j *VMDeployJob) Instantiate() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *VMDeployJob) SetPreset() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	var err error
	_, err = templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return fmt.Errorf("failed to find project %s, err: %v", j.workflow.Project, err)
	}
	// if quoted job quote another job, then use the service and artifact of the quoted job
	if j.spec.Source == config.SourceFromJob {
		j.spec.OriginJobName = j.spec.JobName
		j.spec.JobName = getOriginJobName(j.workflow, j.spec.JobName)
	} else if j.spec.Source == config.SourceRuntime {
		envName := strings.ReplaceAll(j.spec.Env, setting.FixedValueMark, "")
		_, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: j.workflow.Project, EnvName: envName})
		if err != nil {
			log.Errorf("can't find product %s in env %s, error: %w", j.workflow.Project, envName, err)
			return nil
		}
	}

	for _, serviceAndVMDeploy := range j.spec.ServiceAndVMDeploys {
		templateSvc, err := commonrepo.NewServiceColl().Find(
			&commonrepo.ServiceFindOption{
				ServiceName: serviceAndVMDeploy.ServiceName,
				ProductName: j.workflow.Project,
			},
		)
		if err != nil {
			err = fmt.Errorf("can't find service %s in project %s, error: %v", serviceAndVMDeploy.ServiceName, j.workflow.Project, err)
			log.Error(err)
			return err
		}
		if templateSvc.BuildName == "" {
			err = fmt.Errorf("service %s in project %s has no deploy", serviceAndVMDeploy.ServiceName, j.workflow.Project)
			log.Error(err)
			return err
		}
		build, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: templateSvc.BuildName, ProductName: j.workflow.Project})
		if err != nil {
			err = fmt.Errorf("can't find build %s in project %s, error: %v", templateSvc.BuildName, j.workflow.Project, err)
			log.Error(err)
			return err
		}
		serviceAndVMDeploy.Repos = mergeRepos(serviceAndVMDeploy.Repos, build.DeployRepos)
	}

	j.job.Spec = j.spec
	return nil
}

func (j *VMDeployJob) SetOptions(approvalTicket *commonmodels.ApprovalTicket) error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	// there are no production environment for vm projects now
	envs, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name:                j.workflow.Project,
		IsSortByProductName: true,
		Production:          util.GetBoolPointer(false),
	})

	if err != nil {
		log.Errorf("failed to list environments for project: %s, error: %s", j.workflow.Project, err)
		return err
	}

	envOptions := make([]*commonmodels.ZadigVMDeployEnvInformation, 0)

	var allowedServices []*commonmodels.ServiceWithModule
	if approvalTicket != nil {
		allowedServices = approvalTicket.Services
	}

	for _, env := range envs {
		if approvalTicket != nil && !isAllowedEnv(env.EnvName, approvalTicket.Envs) {
			continue
		}

		info, err := generateVMDeployServiceInfo(j.workflow.Project, env.EnvName, allowedServices)
		if err != nil {
			log.Errorf("failed to generate service deploy info for project: %s, error: %s", j.workflow.Project, err)
			return err
		}

		envOptions = append(envOptions, &commonmodels.ZadigVMDeployEnvInformation{
			Env:      env.EnvName,
			Services: info,
		})
	}

	j.spec.EnvOptions = envOptions
	j.job.Spec = j.spec
	return nil
}

func (j *VMDeployJob) ClearOptions() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.EnvOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *VMDeployJob) ClearSelectionField() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.ServiceAndVMDeploys = make([]*commonmodels.ServiceAndVMDeploy, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *VMDeployJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ZadigVMDeployJobSpec)
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

	j.spec.Env = latestSpec.Env
	j.spec.S3StorageID = latestSpec.S3StorageID

	// source is a bit tricky: if the saved args has a source of fromjob, but it has been change to runtime in the config
	// we need to not only update its source but also set services to empty slice.
	if j.spec.Source == config.SourceFromJob && latestSpec.Source == config.SourceRuntime {
		j.spec.ServiceAndVMDeploys = make([]*commonmodels.ServiceAndVMDeploy, 0)
	}
	j.spec.Source = latestSpec.Source

	if j.spec.Source == config.SourceFromJob {
		j.spec.JobName = latestSpec.JobName
		j.spec.OriginJobName = latestSpec.OriginJobName
	}

	deployableService, err := generateVMDeployServiceInfo(j.workflow.Project, latestSpec.Env, nil)
	if err != nil {
		log.Errorf("failed to generate deployable vm service for env: %s, project: %s, error: %s", latestSpec.Env, j.workflow.Project, err)
		return err
	}

	mergedService := make([]*commonmodels.ServiceAndVMDeploy, 0)
	userConfiguredService := make(map[string]*commonmodels.ServiceAndVMDeploy)

	for _, service := range j.spec.ServiceAndVMDeploys {
		userConfiguredService[service.ServiceName] = service
	}

	for _, service := range deployableService {
		if userSvc, ok := userConfiguredService[service.ServiceName]; ok {
			mergedService = append(mergedService, &commonmodels.ServiceAndVMDeploy{
				Repos:         mergeRepos(service.Repos, userSvc.Repos),
				ServiceName:   service.ServiceName,
				ServiceModule: service.ServiceModule,
				ArtifactURL:   userSvc.ArtifactURL,
				FileName:      userSvc.FileName,
				Image:         userSvc.Image,
				TaskID:        userSvc.TaskID,
				WorkflowType:  userSvc.WorkflowType,
				WorkflowName:  userSvc.WorkflowName,
				JobTaskName:   userSvc.JobTaskName,
			})
		} else {
			continue
		}
	}

	j.spec.ServiceAndVMDeploys = mergedService
	j.job.Spec = j.spec
	return nil
}

// generateVMDeployServiceInfo generated all deployable service and its corresponding data.
// currently it ignores the env service info, just gives all the service defined in the template.
func generateVMDeployServiceInfo(project, env string, allowedServices []*commonmodels.ServiceWithModule) ([]*commonmodels.ServiceAndVMDeploy, error) {
	resp := make([]*commonmodels.ServiceAndVMDeploy, 0)

	environmentInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName: env,
		Name:    project,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find env: %s in project: %s, error: %s", env, project, err)
	}

	svcs := environmentInfo.GetServiceMap()

	for _, svc := range svcs {
		templateSvc, err := commonrepo.NewServiceColl().Find(
			&commonrepo.ServiceFindOption{
				ServiceName: svc.ServiceName,
				ProductName: project,
				Revision:    svc.Revision,
			},
		)

		if err != nil {
			return nil, fmt.Errorf("failed to find service: %s in project: %s, error: %s", svc.ServiceName, project, err)
		}

		if !isAllowedService(templateSvc.ServiceName, templateSvc.ServiceName, allowedServices) {
			continue
		}

		if templateSvc.BuildName == "" {
			return nil, fmt.Errorf("service %s in project %s has no deploy info", svc.ServiceName, project)
		}

		build, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: templateSvc.BuildName, ProductName: project})
		if err != nil {
			return nil, fmt.Errorf("can't find build %s in project %s, error: %v", templateSvc.BuildName, project, err)
		}

		resp = append(resp, &commonmodels.ServiceAndVMDeploy{
			Repos:         build.DeployRepos,
			ServiceName:   templateSvc.ServiceName,
			ServiceModule: templateSvc.ServiceName,
		})
	}

	return resp, nil
}

func (j *VMDeployJob) GetRepos() ([]*types.Repository, error) {
	resp := []*types.Repository{}
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	for _, serviceAndVMDeploy := range j.spec.ServiceAndVMDeploys {
		templateSvc, err := commonrepo.NewServiceColl().Find(
			&commonrepo.ServiceFindOption{
				ServiceName: serviceAndVMDeploy.ServiceName,
				ProductName: j.workflow.Project,
			},
		)
		if err != nil {
			err = fmt.Errorf("can't find service %s in project %s, error: %v", serviceAndVMDeploy.ServiceName, j.workflow.Project, err)
			log.Error(err)
			return nil, fmt.Errorf(err.Error())
		}
		if templateSvc.BuildName == "" {
			err = fmt.Errorf("service %s in project %s has no deploy", serviceAndVMDeploy.ServiceName, j.workflow.Project)
			log.Error(err)
			return nil, fmt.Errorf(err.Error())
		}
		build, err := commonrepo.NewBuildColl().Find(&commonrepo.BuildFindOption{Name: templateSvc.BuildName, ProductName: j.workflow.Project})
		if err != nil {
			err = fmt.Errorf("can't find build %s in project %s, error: %v", templateSvc.BuildName, j.workflow.Project, err)
			log.Error(err)
			return nil, fmt.Errorf(err.Error())
		}
		repos := mergeRepos(serviceAndVMDeploy.Repos, build.DeployRepos)
		resp = append(resp, repos...)
	}
	return resp, nil
}

func (j *VMDeployJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.ZadigVMDeployJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.ZadigVMDeployJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}

		j.spec.ServiceAndVMDeploys = argsSpec.ServiceAndVMDeploys

		j.job.Spec = j.spec
	}
	return nil
}

func (j *VMDeployJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}

	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec

	envName := strings.ReplaceAll(j.spec.Env, setting.FixedValueMark, "")
	_, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: j.workflow.Project, EnvName: envName})
	if err != nil {
		return resp, fmt.Errorf("env %s not exists", envName)
	}

	templateProduct, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("cannot find product %s: %w", j.workflow.Project, err)
	}
	timeout := templateProduct.Timeout * 60

	vms, err := commonrepo.NewPrivateKeyColl().List(&commonrepo.PrivateKeyArgs{})
	if err != nil {
		return resp, fmt.Errorf("list private keys error: %v", err)
	}

	services, err := commonrepo.NewServiceColl().ListMaxRevisionsByProduct(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("list project %s's services error: %v", j.workflow.Project, err)
	}
	serviceMap := map[string]*commonmodels.Service{}
	for _, service := range services {
		serviceMap[service.ServiceName] = service
	}

	var s3Storage *commonmodels.S3Storage
	originS3StorageSubfolder := ""
	// get deploy info from previous build job
	if j.spec.Source == config.SourceFromJob {
		// adapt to the front end, use the direct quoted job name
		if j.spec.OriginJobName != "" {
			j.spec.JobName = j.spec.OriginJobName
		}

		referredJob := getOriginJobName(j.workflow, j.spec.JobName)
		targets, err := j.getOriginReferedJobTargets(referredJob, int(taskID))
		if err != nil {
			return resp, fmt.Errorf("get origin refered job: %s targets failed, err: %v", referredJob, err)
		}

		s3Storage, err = commonrepo.NewS3StorageColl().FindDefault()
		if err != nil {
			return resp, fmt.Errorf("find default s3 storage error: %v", err)
		}
		originS3StorageSubfolder = s3Storage.Subfolder
		// clear service and image list to prevent old data from remaining
		j.spec.ServiceAndVMDeploys = targets
		j.spec.S3StorageID = s3Storage.ID.Hex()
	} else {
		s3Storage, err = commonrepo.NewS3StorageColl().Find(j.spec.S3StorageID)
		if err != nil {
			return resp, fmt.Errorf("find s3 storage id: %s, error: %v", j.spec.S3StorageID, err)
		}
		originS3StorageSubfolder = s3Storage.Subfolder
	}

	buildSvc := commonservice.NewBuildService()
	for jobSubTaskID, vmDeployInfo := range j.spec.ServiceAndVMDeploys {
		s3Storage.Subfolder = originS3StorageSubfolder

		service, ok := serviceMap[vmDeployInfo.ServiceName]
		if !ok {
			return resp, fmt.Errorf("service %s not found", vmDeployInfo.ServiceName)
		}
		buildInfo, err := buildSvc.GetBuild(service.BuildName, vmDeployInfo.ServiceName, vmDeployInfo.ServiceModule)
		if err != nil {
			return resp, fmt.Errorf("get build info for service %s error: %v", vmDeployInfo.ServiceName, err)
		}
		basicImage, err := commonrepo.NewBasicImageColl().Find(buildInfo.PreDeploy.ImageID)
		if err != nil {
			return resp, fmt.Errorf("find base image: %s error: %v", buildInfo.PreBuild.ImageID, err)
		}

		jobTaskSpec := &commonmodels.JobTaskFreestyleSpec{}
		jobTask := &commonmodels.JobTask{
			Name:        GenJobName(j.workflow, j.job.Name, jobSubTaskID),
			Key:         genJobKey(j.job.Name, vmDeployInfo.ServiceName, vmDeployInfo.ServiceModule),
			DisplayName: genJobDisplayName(j.job.Name, vmDeployInfo.ServiceName, vmDeployInfo.ServiceModule),
			OriginName:  j.job.Name,
			JobInfo: map[string]string{
				"service_name":   vmDeployInfo.ServiceName,
				"service_module": vmDeployInfo.ServiceModule,
				JobNameKey:       j.job.Name,
			},
			JobType:        string(config.JobZadigVMDeploy),
			Spec:           jobTaskSpec,
			Timeout:        int64(buildInfo.Timeout),
			Infrastructure: buildInfo.DeployInfrastructure,
			VMLabels:       buildInfo.DeployVMLabels,
			ErrorPolicy:    j.job.ErrorPolicy,
		}
		jobTaskSpec.Properties = commonmodels.JobProperties{
			Timeout:         int64(timeout),
			ResourceRequest: buildInfo.PreBuild.ResReq,
			ResReqSpec:      buildInfo.PreBuild.ResReqSpec,
			CustomEnvs:      buildInfo.PreBuild.Envs,
			ClusterID:       buildInfo.PreBuild.ClusterID,
			StrategyID:      buildInfo.PreBuild.StrategyID,
			BuildOS:         basicImage.Value,
			ImageFrom:       buildInfo.PreDeploy.ImageFrom,
			ServiceName:     vmDeployInfo.ServiceName,
		}

		initShellScripts := []string{}
		vmDeployVars := []*commonmodels.KeyVal{}
		tmpVmDeployVars := getVMDeployJobVariables(vmDeployInfo, buildInfo, taskID, j.spec.Env, j.workflow.Project, j.workflow.Name, j.workflow.DisplayName, jobTask.Infrastructure, vms, services, log.SugaredLogger())
		for _, kv := range tmpVmDeployVars {
			if strings.HasSuffix(kv.Key, "_PK_CONTENT") {
				name := strings.TrimSuffix(kv.Key, "_PK_CONTENT")
				vmDeployVars = append(vmDeployVars, &commonmodels.KeyVal{Key: name + "_PK", Value: "/tmp/" + name + "_PK", IsCredential: false})

				initShellScripts = append(initShellScripts, "echo \""+kv.Value+"\" > /tmp/"+name+"_PK")
				initShellScripts = append(initShellScripts, "chmod 600 /tmp/"+name+"_PK")
			} else {
				vmDeployVars = append(vmDeployVars, kv)
			}
		}

		jobTaskSpec.Properties.Envs = append(jobTaskSpec.Properties.CustomEnvs, vmDeployVars...)
		jobTaskSpec.Properties.UseHostDockerDaemon = buildInfo.PreBuild.UseHostDockerDaemon
		jobTaskSpec.Properties.CacheEnable = false

		// init tools install step
		tools := []*step.Tool{}
		for _, tool := range buildInfo.PreDeploy.Installs {
			tools = append(tools, &step.Tool{
				Name:    tool.Name,
				Version: tool.Version,
			})
		}
		toolInstallStep := &commonmodels.StepTask{
			Name:     fmt.Sprintf("%s-%s", vmDeployInfo.ServiceName, "tool-install"),
			JobName:  jobTask.Name,
			StepType: config.StepTools,
			Spec:     step.StepToolInstallSpec{Installs: tools},
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, toolInstallStep)
		// init git clone step
		repos := vmRenderRepos(buildInfo.DeployRepos, jobTaskSpec.Properties.Envs)
		gitRepos, p4Repos := splitReposByType(repos)

		codehosts, err := codehostrepo.NewCodehostColl().AvailableCodeHost(j.workflow.Project)
		if err != nil {
			return nil, fmt.Errorf("find %s project codehost error: %v", j.workflow.Project, err)
		}

		gitStep := &commonmodels.StepTask{
			Name:     vmDeployInfo.ServiceName + "-git",
			JobName:  jobTask.Name,
			StepType: config.StepGit,
			Spec:     step.StepGitSpec{Repos: gitRepos, CodeHosts: codehosts},
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, gitStep)

		p4Step := &commonmodels.StepTask{
			Name:     vmDeployInfo.ServiceName + "-perforce",
			JobName:  jobTask.Name,
			StepType: config.StepPerforce,
			Spec:     step.StepP4Spec{Repos: p4Repos},
		}

		jobTaskSpec.Steps = append(jobTaskSpec.Steps, p4Step)

		objectPath := ""
		if vmDeployInfo.WorkflowType == config.WorkflowType {
			if s3Storage.Subfolder != "" {
				objectPath = fmt.Sprintf("%s/%s/%d/%s", s3Storage.Subfolder, vmDeployInfo.WorkflowName, vmDeployInfo.TaskID, "file")
			} else {
				objectPath = fmt.Sprintf("%s/%d/%s", vmDeployInfo.WorkflowName, vmDeployInfo.TaskID, "file")
			}
		} else if vmDeployInfo.WorkflowType == config.WorkflowTypeV4 {
			if s3Storage.Subfolder != "" {
				objectPath = fmt.Sprintf("%s/%s/%d/%s/%s", s3Storage.Subfolder, vmDeployInfo.WorkflowName, vmDeployInfo.TaskID, vmDeployInfo.JobTaskName, "archive")
			} else {
				objectPath = fmt.Sprintf("%s/%d/%s/%s", vmDeployInfo.WorkflowName, vmDeployInfo.TaskID, vmDeployInfo.JobTaskName, "archive")
			}
		} else {
			return resp, fmt.Errorf("unknown workflow type %s", vmDeployInfo.WorkflowType)
		}

		if buildInfo.PostBuild.FileArchive != nil {
			// init download artifact step
			downloadArtifactStep := &commonmodels.StepTask{
				Name:     vmDeployInfo.ServiceName + "-download-artifact",
				JobName:  jobTask.Name,
				StepType: config.StepDownloadArchive,
				Spec: step.StepDownloadArchiveSpec{
					FileName:   vmDeployInfo.FileName,
					DestDir:    "artifact",
					ObjectPath: objectPath,
					S3:         modelS3toS3(s3Storage),
				},
			}
			jobTaskSpec.Steps = append(jobTaskSpec.Steps, downloadArtifactStep)
		}
		// init debug before step
		debugBeforeStep := &commonmodels.StepTask{
			Name:     vmDeployInfo.ServiceName + "-debug_before",
			JobName:  jobTask.Name,
			StepType: config.StepDebugBefore,
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, debugBeforeStep)
		// init shell step
		scripts := append([]string{}, initShellScripts...)
		scripts = append(scripts, strings.Split(replaceWrapLine(buildInfo.PMDeployScripts), "\n")...)
		scriptStep := &commonmodels.StepTask{
			JobName: jobTask.Name,
		}
		if buildInfo.PMDeployScriptType == types.ScriptTypeShell || buildInfo.PMDeployScriptType == "" {
			scriptStep.Name = vmDeployInfo.ServiceName + "-shell"
			scriptStep.StepType = config.StepShell
			scriptStep.Spec = &step.StepShellSpec{
				Scripts: scripts,
			}
		} else if buildInfo.PMDeployScriptType == types.ScriptTypeBatchFile {
			scriptStep.Name = vmDeployInfo.ServiceName + "-batchfile"
			scriptStep.StepType = config.StepBatchFile
			scriptStep.Spec = &step.StepBatchFileSpec{
				Scripts: scripts,
			}
		} else if buildInfo.PMDeployScriptType == types.ScriptTypePowerShell {
			scriptStep.Name = vmDeployInfo.ServiceName + "-powershell"
			scriptStep.StepType = config.StepPowerShell
			scriptStep.Spec = &step.StepPowerShellSpec{
				Scripts: scripts,
			}
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, scriptStep)
		// init debug after step
		debugAfterStep := &commonmodels.StepTask{
			Name:     vmDeployInfo.ServiceName + "-debug_after",
			JobName:  jobTask.Name,
			StepType: config.StepDebugAfter,
		}
		jobTaskSpec.Steps = append(jobTaskSpec.Steps, debugAfterStep)

		resp = append(resp, jobTask)
	}

	j.job.Spec = j.spec
	return resp, nil
}

func (j *VMDeployJob) LintJob() error {
	j.spec = &commonmodels.ZadigVMDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
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

func (j *VMDeployJob) GetOutPuts(log *zap.SugaredLogger) []string {
	return getOutputKey(j.job.Name, ensureDeployInOutputs())
}

// TODO: since vm deploy is for VM type, now we only search for the build job, the reference for deploy/distribute is not supported.
func (j *VMDeployJob) getOriginReferedJobTargets(jobName string, taskID int) ([]*commonmodels.ServiceAndVMDeploy, error) {
	serviceAndVMDeploys := []*commonmodels.ServiceAndVMDeploy{}
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.Name != j.spec.JobName {
				continue
			}
			if job.JobType == config.JobZadigBuild {
				buildSpec := &commonmodels.ZadigBuildJobSpec{}
				if err := commonmodels.IToi(job.Spec, buildSpec); err != nil {
					return serviceAndVMDeploys, err
				}
				for i, build := range buildSpec.ServiceAndBuilds {
					serviceAndVMDeploys = append(serviceAndVMDeploys, &commonmodels.ServiceAndVMDeploy{
						ServiceName:   build.ServiceName,
						ServiceModule: build.ServiceModule,
						FileName:      build.Package,
						Image:         build.Image,
						TaskID:        taskID,
						WorkflowName:  j.workflow.Name,
						WorkflowType:  config.WorkflowTypeV4,
						JobTaskName:   GenJobName(j.workflow, job.Name, i),
					})
					log.Infof("DeployJob ToJobs getOriginReferedJobTargets: workflow %s service %s, module %s, fileName %s",
						j.workflow.Name, build.ServiceName, build.ServiceModule, build.Package)
				}
				return serviceAndVMDeploys, nil
			}
		}
	}
	return nil, fmt.Errorf("build job %s not found", jobName)
}

func getVMDeployJobVariables(vmDeploy *commonmodels.ServiceAndVMDeploy, buildInfo *commonmodels.Build, taskID int64, envName, project, workflowName, workflowDisplayName, infrastructure string, vms []*commonmodels.PrivateKey, services []*commonmodels.Service, log *zap.SugaredLogger) []*commonmodels.KeyVal {
	ret := make([]*commonmodels.KeyVal, 0)
	// basic envs
	ret = append(ret, PrepareDefaultWorkflowTaskEnvs(project, workflowName, workflowDisplayName, infrastructure, taskID)...)

	// repo envs
	ret = append(ret, getReposVariables(buildInfo.Repos)...)

	// vm deploy specific envs
	ret = append(ret, &commonmodels.KeyVal{Key: "ENV_NAME", Value: envName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE", Value: vmDeploy.ServiceName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE_NAME", Value: vmDeploy.ServiceName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "SERVICE_MODULE", Value: vmDeploy.ServiceModule, IsCredential: false})

	privateKeys := sets.String{}
	IDvmMap := map[string]*commonmodels.PrivateKey{}
	labelVMsMap := map[string][]*commonmodels.PrivateKey{}
	for _, vm := range vms {
		privateKeys.Insert(vm.Name)
		IDvmMap[vm.ID.Hex()] = vm
		labelVMsMap[vm.Label] = append(labelVMsMap[vm.Label], vm)
	}
	ret = append(ret, &commonmodels.KeyVal{Key: "AGENTS", Value: strings.Join(privateKeys.List(), ","), IsCredential: false})

	agentVMIDs := sets.String{}
	if len(buildInfo.SSHs) > 0 {
		// privateKeys := make([]*taskmodels.SSH, 0)
		for _, sshID := range buildInfo.SSHs {
			//私钥信息可能被更新，而构建中存储的信息是旧的，需要根据id获取最新的私钥信息
			latestKeyInfo, err := commonrepo.NewPrivateKeyColl().Find(commonrepo.FindPrivateKeyOption{ID: sshID})
			if err != nil || latestKeyInfo == nil {
				log.Errorf("PrivateKey.Find failed, id:%s, err:%s", sshID, err)
				continue
			}
			agentName := latestKeyInfo.Name
			userName := latestKeyInfo.UserName
			ip := latestKeyInfo.IP
			port := latestKeyInfo.Port
			if port == 0 {
				port = setting.PMHostDefaultPort
			}
			privateKey, err := base64.StdEncoding.DecodeString(latestKeyInfo.PrivateKey)
			if err != nil {
				log.Errorf("base64 decode failed ip:%s, error:%s", ip, err)
				continue
			}
			ret = append(ret, &commonmodels.KeyVal{Key: agentName + "_PK_CONTENT", Value: string(privateKey), IsCredential: false})
			ret = append(ret, &commonmodels.KeyVal{Key: agentName + "_USERNAME", Value: userName, IsCredential: false})
			ret = append(ret, &commonmodels.KeyVal{Key: agentName + "_IP", Value: ip, IsCredential: false})
			ret = append(ret, &commonmodels.KeyVal{Key: agentName + "_PORT", Value: strconv.Itoa(int(port)), IsCredential: false})
			agentVMIDs.Insert(sshID)
		}
	}

	envHostNamesMap := map[string][]string{}
	envHostIPsMap := map[string][]string{}
	addedHostIDs := sets.String{}
	for _, svc := range services {
		if svc.ServiceName != vmDeploy.ServiceName {
			continue
		}
		for _, envConfig := range svc.EnvConfigs {
			for _, hostID := range envConfig.HostIDs {
				if vm, ok := IDvmMap[hostID]; ok {
					if envName == envConfig.EnvName {
						envHostNamesMap[envConfig.EnvName] = append(envHostNamesMap[envConfig.EnvName], vm.Name)
						envHostIPsMap[envConfig.EnvName] = append(envHostIPsMap[envConfig.EnvName], vm.IP)
					}

					if agentVMIDs.Has(hostID) || addedHostIDs.Has(hostID) {
						continue
					}
					addedHostIDs.Insert(hostID)
					hostName := vm.Name
					userName := vm.UserName
					ip := vm.IP
					port := vm.Port
					if port == 0 {
						port = setting.PMHostDefaultPort
					}
					privateKey, err := base64.StdEncoding.DecodeString(vm.PrivateKey)
					if err != nil {
						log.Errorf("base64 decode failed ip:%s, error:%s", ip, err)
						continue
					}
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_PK_CONTENT", Value: string(privateKey), IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_USERNAME", Value: userName, IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_IP", Value: ip, IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_PORT", Value: strconv.Itoa(int(port)), IsCredential: false})
				}
			}
			for _, label := range envConfig.Labels {
				for _, vm := range labelVMsMap[label] {
					if envName == envConfig.EnvName {
						envHostNamesMap[envConfig.EnvName] = append(envHostNamesMap[envConfig.EnvName], vm.Name)
						envHostIPsMap[envConfig.EnvName] = append(envHostIPsMap[envConfig.EnvName], vm.IP)
					}

					if agentVMIDs.Has(vm.ID.Hex()) || addedHostIDs.Has(vm.ID.Hex()) {
						continue
					}
					addedHostIDs.Insert(vm.ID.Hex())

					hostName := vm.Name
					userName := vm.UserName
					ip := vm.IP
					port := vm.Port
					if port == 0 {
						port = setting.PMHostDefaultPort
					}
					privateKey := vm.PrivateKey
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_PK_CONTENT", Value: privateKey, IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_USERNAME", Value: userName, IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_IP", Value: ip, IsCredential: false})
					ret = append(ret, &commonmodels.KeyVal{Key: hostName + "_PORT", Value: strconv.Itoa(int(port)), IsCredential: false})
				}
			}
		}
	}
	// env host ips
	for envName, HostIPs := range envHostIPsMap {
		ret = append(ret, &commonmodels.KeyVal{Key: envName + "_HOST_IPs", Value: strings.Join(HostIPs, ","), IsCredential: false})
	}
	// env host names
	for envName, names := range envHostNamesMap {
		ret = append(ret, &commonmodels.KeyVal{Key: envName + "_HOST_NAMEs", Value: strings.Join(names, ","), IsCredential: false})
	}

	if infrastructure != setting.JobVMInfrastructure {
		ret = append(ret, &commonmodels.KeyVal{Key: "ARTIFACT", Value: "/workspace/artifact/" + vmDeploy.FileName, IsCredential: false})
	} else {
		if buildInfo.PMDeployScriptType == types.ScriptTypeShell || buildInfo.PMDeployScriptType == "" {
			ret = append(ret, &commonmodels.KeyVal{Key: "ARTIFACT", Value: "$WORKSPACE/artifact/" + vmDeploy.FileName, IsCredential: false})
		} else if buildInfo.PMDeployScriptType == types.ScriptTypeBatchFile {
			ret = append(ret, &commonmodels.KeyVal{Key: "ARTIFACT", Value: "%WORKSPACE%\\artifact\\" + vmDeploy.FileName, IsCredential: false})
		} else if buildInfo.PMDeployScriptType == types.ScriptTypePowerShell {
			ret = append(ret, &commonmodels.KeyVal{Key: "ARTIFACT", Value: "$env:WORKSPACE\\artifact\\" + vmDeploy.FileName, IsCredential: false})
		}
	}
	ret = append(ret, &commonmodels.KeyVal{Key: "PKG_FILE", Value: vmDeploy.FileName, IsCredential: false})
	ret = append(ret, &commonmodels.KeyVal{Key: "IMAGE", Value: vmDeploy.Image, IsCredential: false})
	return ret
}

func vmRenderRepos(repos []*types.Repository, kvs []*commonmodels.KeyVal) []*types.Repository {
	for _, inputRepo := range repos {
		inputRepo.CheckoutPath = commonutil.RenderEnv(inputRepo.CheckoutPath, kvs)
		if inputRepo.RemoteName == "" {
			inputRepo.RemoteName = "origin"
		}
		if inputRepo.Source == types.ProviderOther {
			codeHostInfo, err := codehostdb.NewCodehostColl().GetCodeHostByID(inputRepo.CodehostID, false)
			if err == nil {
				inputRepo.PrivateAccessToken = codeHostInfo.PrivateAccessToken
				inputRepo.SSHKey = codeHostInfo.SSHKey
			}
		}
	}
	return repos
}
