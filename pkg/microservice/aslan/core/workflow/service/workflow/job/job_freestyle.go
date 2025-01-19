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
	"strings"

	"go.uber.org/zap"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	steptypes "github.com/koderover/zadig/v2/pkg/types/step"
)

type FreeStyleJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.FreestyleJobSpec
}

func (j *FreeStyleJob) Instantiate() error {
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	if err := util.CheckDefineResourceParam(j.spec.Properties.ResourceRequest, j.spec.Properties.ResReqSpec); err != nil {
		return err
	}

	for _, step := range j.spec.Steps {
		switch step.StepType {
		case config.StepTools:
			stepSpec := &steptypes.StepToolInstallSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse tool install step spec error: %v", err)
			}
			step.Spec = stepSpec
		case config.StepGit:
			stepSpec := &steptypes.StepGitSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse git step spec error: %v", err)
			}
			step.Spec = stepSpec
		case config.StepShell:
			stepSpec := &steptypes.StepShellSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse shell step spec error: %v", err)
			}
			step.Spec = stepSpec
		case config.StepBatchFile:
			stepSpec := &steptypes.StepBatchFileSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse shell step spec error: %v", err)
			}
			step.Spec = stepSpec
		case config.StepPowerShell:
			stepSpec := &steptypes.StepPowerShellSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse shell step spec error: %v", err)
			}
			step.Spec = stepSpec
		case config.StepArchive:
			stepSpec := &steptypes.StepArchiveSpec{}
			if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
				return fmt.Errorf("parse archive step spec error: %v", err)
			}
			step.Spec = stepSpec
		default:
			return fmt.Errorf("freestyle job step type %s not supported", step.StepType)
		}

	}
	j.job.Spec = j.spec
	return nil
}

func (j *FreeStyleJob) SetPreset() error {
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *FreeStyleJob) SetOptions() error {
	return nil
}

func (j *FreeStyleJob) ClearOptions() error {
	return nil
}

func (j *FreeStyleJob) ClearSelectionField() error {
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.FreestyleJobSpec)
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

	j.spec.Properties.Envs = renderKeyVals(j.spec.Properties.Envs, latestSpec.Properties.Envs)

	for _, step := range j.spec.Steps {
		for _, latestStep := range latestSpec.Steps {
			if step.StepType == latestStep.StepType && step.Name == latestStep.Name {
				if step.StepType == config.StepGit {
					stepSpec := &steptypes.StepGitSpec{}
					if err := commonmodels.IToiYaml(step.Spec, stepSpec); err != nil {
						return fmt.Errorf("parse git step spec error: %v", err)
					}
					latestStepSpec := &steptypes.StepGitSpec{}
					if err := commonmodels.IToiYaml(step.Spec, latestStepSpec); err != nil {
						return fmt.Errorf("parse git step spec error: %v", err)
					}
					latestStepSpec.Repos = mergeRepos(latestStepSpec.Repos, stepSpec.Repos)
					step.Spec = latestStepSpec
				}
			}
		}
	}

	j.job.Spec = j.spec
	return nil
}

func (j *FreeStyleJob) GetRepos() ([]*types.Repository, error) {
	resp := []*types.Repository{}
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	for _, step := range j.spec.Steps {
		if step.StepType != config.StepGit {
			continue
		}
		stepSpec := &steptypes.StepGitSpec{}
		if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
			return resp, err
		}
		resp = append(resp, stepSpec.Repos...)
	}
	return resp, nil
}

func (j *FreeStyleJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.FreestyleJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.FreestyleJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Properties.Envs = renderKeyVals(argsSpec.Properties.Envs, j.spec.Properties.Envs)

		for _, step := range j.spec.Steps {
			if step.StepType != config.StepGit {
				continue
			}
			for _, stepArgs := range argsSpec.Steps {
				if stepArgs.StepType != config.StepGit {
					continue
				}
				if stepArgs.Name != step.Name {
					continue
				}
				stepSpec := &steptypes.StepGitSpec{}
				if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
					return fmt.Errorf("parse git step spec error: %v", err)
				}
				stepArgsSpec := &steptypes.StepGitSpec{}
				if err := commonmodels.IToi(stepArgs.Spec, stepArgsSpec); err != nil {
					return fmt.Errorf("parse git step spec error: %v", err)
				}
				stepSpec.Repos = mergeRepos(stepSpec.Repos, stepArgsSpec.Repos)
				step.Spec = stepSpec
				break
			}
		}
		j.job.Spec = j.spec
	}
	return nil
}

func (j *FreeStyleJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *FreeStyleJob) MergeWebhookRepo(webhookRepo *types.Repository) error {
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	for _, step := range j.spec.Steps {
		if step.StepType != config.StepGit {
			continue
		}
		stepSpec := &steptypes.StepGitSpec{}
		if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
			return fmt.Errorf("parse git step spec error: %v", err)
		}
		stepSpec.Repos = mergeRepos(stepSpec.Repos, []*types.Repository{webhookRepo})
		step.Spec = stepSpec
	}
	j.job.Spec = j.spec
	return nil
}

func (j *FreeStyleJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	logger := log.SugaredLogger()
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	jobTaskSpec := &commonmodels.JobTaskFreestyleSpec{
		Properties: *j.spec.Properties,
		Steps:      j.stepsToStepTasks(j.spec.Steps),
	}
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobFreestyle),
		Spec:    jobTaskSpec,
		Timeout: j.spec.Properties.Timeout,
		Outputs: j.spec.Outputs,
	}

	if j.spec != nil && j.spec.Properties != nil && j.spec.Properties.Infrastructure != "" && len(j.spec.Properties.VMLabels) > 0 {
		jobTask.Infrastructure = j.spec.Properties.Infrastructure
		jobTask.VMLabels = j.spec.Properties.VMLabels
	}

	registries, err := commonservice.ListRegistryNamespaces("", true, logger)
	if err != nil {
		return resp, err
	}
	jobTaskSpec.Properties.Registries = registries
	jobTaskSpec.Properties.ShareStorageDetails = getShareStorageDetail(j.workflow.ShareStorages, j.spec.Properties.ShareStorageInfo, j.workflow.Name, taskID)

	basicImage, err := commonrepo.NewBasicImageColl().Find(jobTaskSpec.Properties.ImageID)
	if err != nil {
		return resp, fmt.Errorf("failed to find base image: %s,error :%v", jobTaskSpec.Properties.ImageID, err)
	}
	jobTaskSpec.Properties.BuildOS = basicImage.Value
	// save user defined variables.
	jobTaskSpec.Properties.CustomEnvs = jobTaskSpec.Properties.Envs
	jobTaskSpec.Properties.Envs = append(jobTaskSpec.Properties.Envs, getfreestyleJobVariables(jobTaskSpec.Steps, taskID, j.workflow.Project, j.workflow.Name, j.workflow.DisplayName, jobTask.Infrastructure)...)
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *FreeStyleJob) stepsToStepTasks(step []*commonmodels.Step) []*commonmodels.StepTask {
	logger := log.SugaredLogger()
	resp := []*commonmodels.StepTask{}
	for _, step := range step {
		stepTask := &commonmodels.StepTask{
			Name:     step.Name,
			StepType: step.StepType,
			Spec:     step.Spec,
		}
		if stepTask.StepType == config.StepGit {
			stepTaskSpec := &steptypes.StepGitSpec{}
			if err := commonmodels.IToi(stepTask.Spec, stepTaskSpec); err != nil {
				continue
			}
			newRepos := []*types.Repository{}
			for _, repo := range stepTaskSpec.Repos {
				if repo.SourceFrom == types.RepoSourceParam {
					paramRepo, err := findMatchedRepoFromParams(j.workflow.Params, repo.GlobalParamName)
					if err != nil {
						logger.Errorf("findMatchedRepoFromParams error: %v", err)
						continue
					}
					newRepos = append(newRepos, paramRepo)
					continue
				}
				newRepos = append(newRepos, repo)
			}
			stepTaskSpec.Repos = newRepos
			stepTask.Spec = stepTaskSpec

		}
		if stepTask.StepType == config.StepDockerBuild {
			stepTaskSpec := &steptypes.StepDockerBuildSpec{}
			if err := commonmodels.IToi(stepTask.Spec, stepTaskSpec); err != nil {
				continue
			}
			registryID := ""
			if stepTaskSpec.DockerRegistry != nil {
				registryID = stepTaskSpec.DockerRegistry.DockerRegistryID
			}
			registry, _, err := commonservice.FindRegistryById(registryID, true, logger)
			if err != nil {
				logger.Errorf("FindRegistryById error: %v", err)
			}
			stepTaskSpec.DockerRegistry = &steptypes.DockerRegistry{
				DockerRegistryID: registryID,
				Host:             registry.RegAddr,
				UserName:         registry.AccessKey,
				Password:         registry.SecretKey,
				Namespace:        registry.Namespace,
			}
			stepTask.Spec = stepTaskSpec
		}
		if stepTask.StepType == config.StepShell {
			stepTaskSpec := &steptypes.StepShellSpec{}
			if err := commonmodels.IToi(stepTask.Spec, stepTaskSpec); err != nil {
				continue
			}
			stepTaskSpec.Scripts = append(strings.Split(replaceWrapLine(stepTaskSpec.Script), "\n"), outputScript(j.spec.Outputs)...)
			stepTask.Spec = stepTaskSpec
			// add debug step before shell step
			debugBeforeStep := &commonmodels.StepTask{
				Name:     "debug-before",
				StepType: config.StepDebugBefore,
			}
			resp = append(resp, debugBeforeStep)
		}

		resp = append(resp, stepTask)
		if stepTask.StepType == config.StepShell {
			// add debug step after shell step
			debugAfterStep := &commonmodels.StepTask{
				Name:     "debug-after",
				StepType: config.StepDebugAfter,
			}
			resp = append(resp, debugAfterStep)
		}
	}
	return resp
}

func getfreestyleJobVariables(steps []*commonmodels.StepTask, taskID int64, project, workflowName, workflowDisplayName, infrastructure string) []*commonmodels.KeyVal {
	ret := []*commonmodels.KeyVal{}
	repos := []*types.Repository{}
	for _, step := range steps {
		if step.StepType != config.StepGit {
			continue
		}
		stepSpec := &steptypes.StepGitSpec{}
		if err := commonmodels.IToi(step.Spec, stepSpec); err != nil {
			log.Errorf("failed to convert step spec error: %v", err)
			continue
		}
		repos = append(repos, stepSpec.Repos...)
	}
	// basic envs
	ret = append(ret, PrepareDefaultWorkflowTaskEnvs(project, workflowName, workflowDisplayName, infrastructure, taskID)...)
	// repo envs
	ret = append(ret, getReposVariables(repos)...)

	buildURL := fmt.Sprintf("%s/v1/projects/detail/%s/pipelines/custom/%s/%d?display_name=%s", configbase.SystemAddress(), project, workflowName, taskID, url.QueryEscape(workflowDisplayName))
	ret = append(ret, &commonmodels.KeyVal{Key: "BUILD_URL", Value: buildURL, IsCredential: false})
	return ret
}

func (j *FreeStyleJob) LintJob() error {
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	return checkOutputNames(j.spec.Outputs)
}

func (j *FreeStyleJob) GetOutPuts(log *zap.SugaredLogger) []string {
	resp := []string{}
	j.spec = &commonmodels.FreestyleJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return resp
	}

	jobKey := j.job.Name
	resp = append(resp, getOutputKey(jobKey, j.spec.Outputs)...)
	return resp
}
