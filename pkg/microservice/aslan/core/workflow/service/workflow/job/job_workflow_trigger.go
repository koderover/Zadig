/*
 * Copyright 2023 The KodeRover Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package job

import (
	"fmt"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/types"
)

type WorkflowTriggerJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.WorkflowTriggerJobSpec
}

func (j *WorkflowTriggerJob) Instantiate() error {
	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *WorkflowTriggerJob) SetPreset() error {
	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.job.Spec = j.spec
	return nil
}

func (j *WorkflowTriggerJob) SetOptions() error {
	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.WorkflowTriggerJobSpec)
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

	//j.spec.ServiceTriggerWorkflow = latestSpec.ServiceTriggerWorkflow
	//j.spec.FixedWorkflowList = latestSpec.FixedWorkflowList
	j.job.Spec = j.spec
	return nil
}

func (j *WorkflowTriggerJob) ClearOptions() error {
	return nil
}

func (j *WorkflowTriggerJob) ClearSelectionField() error {
	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.spec.SourceService = make([]*commonmodels.ServiceNameAndModule, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *WorkflowTriggerJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.WorkflowTriggerJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}

		argsSpec := &commonmodels.WorkflowTriggerJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}

		j.job.Spec = argsSpec
	}
	return nil
}

func (j *WorkflowTriggerJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *WorkflowTriggerJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec

	var workflowTriggerEvents []*commonmodels.WorkflowTriggerEvent
	switch j.spec.TriggerType {
	case config.WorkflowTriggerTypeCommon:
		m := make(map[commonmodels.ServiceNameAndModule]*commonmodels.ServiceTriggerWorkflowInfo)
		for _, info := range j.spec.ServiceTriggerWorkflow {
			m[commonmodels.ServiceNameAndModule{
				ServiceName:   info.ServiceName,
				ServiceModule: info.ServiceModule,
			}] = info
		}
		switch j.spec.Source {
		case config.TriggerWorkflowSourceRuntime:
			for _, service := range j.spec.SourceService {
				// Every SourceService must exist in ServiceTriggerWorkflow
				if info, ok := m[commonmodels.ServiceNameAndModule{
					ServiceName:   service.ServiceName,
					ServiceModule: service.ServiceModule,
				}]; ok {
					workflowTriggerEvents = append(workflowTriggerEvents, &commonmodels.WorkflowTriggerEvent{
						WorkflowName:  info.WorkflowName,
						Params:        info.Params,
						ServiceName:   service.ServiceName,
						ServiceModule: service.ServiceModule,
						ProjectName:   info.ProjectName,
					})
				} else {
					return nil, errors.Errorf("no workflow trigger info for service %s-%s", service.ServiceName, service.ServiceModule)
				}
			}
		case config.TriggerWorkflowSourceFromJob:
			var err error
			workflowTriggerEvents, err = j.getSourceJobTargets(j.spec.SourceJobName, m)
			if err != nil {
				return nil, err
			}
		}
	case config.WorkflowTriggerTypeFixed:
		for _, w := range j.spec.FixedWorkflowList {
			workflowTriggerEvents = append(workflowTriggerEvents, &commonmodels.WorkflowTriggerEvent{
				WorkflowName: w.WorkflowName,
				Params:       w.Params,
				ProjectName:  w.ProjectName,
			})
		}
	default:
		return nil, errors.Errorf("invalid trigger type: %s", j.spec.TriggerType)
	}

	for _, event := range workflowTriggerEvents {
		for _, param := range event.Params {
			j.getRepoFromJob(param)
		}
	}

	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobWorkflowTrigger),
		Spec: &commonmodels.JobTaskWorkflowTriggerSpec{
			TriggerType:           j.spec.TriggerType,
			IsEnableCheck:         j.spec.IsEnableCheck,
			WorkflowTriggerEvents: workflowTriggerEvents,
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

// get repo from job config, current only support zadig build job
func (j *WorkflowTriggerJob) getRepoFromJob(param *commonmodels.Param) {
	if param.ParamsType != "repo" {
		return
	}
	if param.Repo == nil {
		return
	}
	if param.Repo.SourceFrom == types.RepoSourceJob {
		for _, stage := range j.workflow.Stages {
			for _, job := range stage.Jobs {
				if job.Name != param.Repo.JobName {
					continue
				}
				switch v := job.Spec.(type) {
				case *commonmodels.ZadigBuildJobSpec:
					for _, build := range v.ServiceAndBuilds {
						if build.ServiceName != param.Repo.ServiceName || build.ServiceModule != param.Repo.ServiceModule {
							continue
						}
						if len(build.Repos) >= param.Repo.JobRepoIndex {
							param.Repo = build.Repos[param.Repo.JobRepoIndex]
							return
						}
					}
				}
			}
		}
	}
}

func (j *WorkflowTriggerJob) getSourceJobTargets(jobName string, m map[commonmodels.ServiceNameAndModule]*commonmodels.ServiceTriggerWorkflowInfo) (resp []*commonmodels.WorkflowTriggerEvent, err error) {
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if j.spec.SourceJobName != job.Name {
				continue
			}
			switch job.JobType {
			case config.JobZadigBuild:
				buildSpec := &commonmodels.ZadigBuildJobSpec{}
				if err := commonmodels.IToi(job.Spec, buildSpec); err != nil {
					return nil, err
				}
				for _, build := range buildSpec.ServiceAndBuilds {
					if info, ok := m[commonmodels.ServiceNameAndModule{
						ServiceName:   build.ServiceName,
						ServiceModule: build.ServiceModule,
					}]; ok {
						resp = append(resp, &commonmodels.WorkflowTriggerEvent{
							WorkflowName:  info.WorkflowName,
							Params:        info.Params,
							ServiceName:   build.ServiceName,
							ServiceModule: build.ServiceModule,
							ProjectName:   info.ProjectName,
						})
					}
				}
				return
			case config.JobZadigDistributeImage:
				distributeSpec := &commonmodels.ZadigDistributeImageJobSpec{}
				if err := commonmodels.IToi(job.Spec, distributeSpec); err != nil {
					return nil, err
				}
				for _, distribute := range distributeSpec.Targets {
					if info, ok := m[commonmodels.ServiceNameAndModule{
						ServiceName:   distribute.ServiceName,
						ServiceModule: distribute.ServiceModule,
					}]; ok {
						resp = append(resp, &commonmodels.WorkflowTriggerEvent{
							WorkflowName:  info.WorkflowName,
							Params:        info.Params,
							ServiceName:   distribute.ServiceName,
							ServiceModule: distribute.ServiceModule,
							ProjectName:   info.ProjectName,
						})
					}
				}
			case config.JobZadigDeploy:
				deploySpec := &commonmodels.ZadigDeployJobSpec{}
				if err := commonmodels.IToi(job.Spec, deploySpec); err != nil {
					return nil, err
				}
				for _, svc := range deploySpec.Services {
					for _, module := range svc.Modules {
						if info, ok := m[commonmodels.ServiceNameAndModule{
							ServiceName:   svc.ServiceName,
							ServiceModule: module.ServiceModule,
						}]; ok {
							resp = append(resp, &commonmodels.WorkflowTriggerEvent{
								WorkflowName:  info.WorkflowName,
								Params:        info.Params,
								ServiceName:   svc.ServiceName,
								ServiceModule: module.ServiceModule,
								ProjectName:   info.ProjectName,
							})
						}
					}
				}
			}
			return
		}
	}
	return nil, fmt.Errorf("service from job %s not found", jobName)
}

func (j *WorkflowTriggerJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	j.spec = &commonmodels.WorkflowTriggerJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec

	workflowSet := sets.NewString(j.workflow.Name)
	// every workflow only need check loop once
	checkedWorkflow := sets.NewString()
	for _, info := range j.spec.ServiceTriggerWorkflow {
		if checkedWorkflow.Has(info.WorkflowName) {
			continue
		}
		workflow, err := mongodb.NewWorkflowV4Coll().Find(info.WorkflowName)
		if err != nil {
			return fmt.Errorf("can't found workflow %s: %v", info.WorkflowName, err)
		}
		if workflowSet.Has(workflow.Name) {
			return fmt.Errorf("工作流不能循环触发, 工作流名称: %s", workflow.Name)
		}
		checkedWorkflow.Insert(workflow.Name)

		if err := checkWorkflowTriggerLoop(workflow, sets.NewString(append(workflowSet.List(), workflow.Name)...)); err != nil {
			return err
		}

		for _, stage := range workflow.Stages {
			for _, job := range stage.Jobs {
				switch job.JobType {
				case config.JobFreestyle, config.JobPlugin, config.JobWorkflowTrigger:
				default:
					return fmt.Errorf("工作流 %s 中的任务 %s 类型不支持被触发", workflow.Name, job.Name)
				}
			}
		}
	}

	if j.spec.TriggerType != config.WorkflowTriggerTypeCommon || j.spec.Source != config.TriggerWorkflowSourceFromJob {
		return nil
	}
	jobRankMap := getJobRankMap(j.workflow.Stages)
	sourceJobRank, ok := jobRankMap[j.spec.SourceJobName]
	if !ok || sourceJobRank >= jobRankMap[j.job.Name] {
		return fmt.Errorf("can not quote job %s in job %s", j.spec.SourceJobName, j.job.Name)
	}

	return nil
}

func checkWorkflowTriggerLoop(workflow *commonmodels.WorkflowV4, workflowSet sets.String) error {
	// every workflow only need check loop once
	checkedWorkflow := sets.NewString()
	for _, stage := range workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType == config.JobWorkflowTrigger {
				triggerSpec := &commonmodels.WorkflowTriggerJobSpec{}
				if err := commonmodels.IToi(job.Spec, triggerSpec); err != nil {
					return err
				}
				for _, info := range triggerSpec.ServiceTriggerWorkflow {
					if checkedWorkflow.Has(info.WorkflowName) {
						continue
					}
					w, err := mongodb.NewWorkflowV4Coll().Find(info.WorkflowName)
					if err != nil {
						return fmt.Errorf("can't found workflow %s: %v", info.WorkflowName, err)
					}

					if workflowSet.Has(info.WorkflowName) {
						return fmt.Errorf("工作流不能循环触发, 工作流名称: %s", workflow.Name)
					}
					checkedWorkflow.Insert(info.WorkflowName)

					if err := checkWorkflowTriggerLoop(w, sets.NewString(append(workflowSet.List(), info.WorkflowName)...)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
