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

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type CustomDeployJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.CustomDeployJobSpec
}

func (j *CustomDeployJob) Instantiate() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *CustomDeployJob) SetPreset() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

// SetOptions get the options from the original workflow regardless of the currently selected items
func (j *CustomDeployJob) SetOptions() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.CustomDeployJobSpec)
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

	j.spec.TargetOptions = originalSpec.Targets
	j.job.Spec = j.spec
	return nil
}

func (j *CustomDeployJob) ClearOptions() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.TargetOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *CustomDeployJob) ClearSelectionField() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.Targets = make([]*commonmodels.DeployTargets, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *CustomDeployJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.CustomDeployJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.CustomDeployJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Source = argsSpec.Source
		j.spec.Targets = argsSpec.Targets
		j.job.Spec = j.spec
	}
	return nil
}

func (j *CustomDeployJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.CustomDeployJobSpec)
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

	if j.spec.ClusterID != latestSpec.ClusterID {
		j.spec.ClusterID = latestSpec.ClusterID
		j.spec.Namespace = ""
		j.spec.Targets = make([]*commonmodels.DeployTargets, 0)
	} else if j.spec.Namespace != latestSpec.Namespace {
		j.spec.Namespace = latestSpec.Namespace
		j.spec.Targets = make([]*commonmodels.DeployTargets, 0)
	}

	j.spec.SkipCheckRunStatus = latestSpec.SkipCheckRunStatus
	j.spec.Timeout = latestSpec.Timeout
	j.spec.DockerRegistryID = latestSpec.DockerRegistryID
	j.job.Spec = j.spec
	return nil
}

func (j *CustomDeployJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}

	j.spec = &commonmodels.CustomDeployJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	for _, target := range j.spec.Targets {
		t := strings.Split(target.Target, "/")
		if len(t) != 3 {
			log.Errorf("target string: %s wrong format", target.Target)
			continue
		}
		workloadType := t[0]
		workloadName := t[1]
		containerName := t[2]
		jobTaskSpec := &commonmodels.JobTaskCustomDeploySpec{
			Namespace:          j.spec.Namespace,
			ClusterID:          j.spec.ClusterID,
			Timeout:            j.spec.Timeout,
			WorkloadType:       workloadType,
			WorkloadName:       workloadName,
			ContainerName:      containerName,
			Image:              target.Image,
			SkipCheckRunStatus: j.spec.SkipCheckRunStatus,
		}
		jobTask := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + workloadType + "-" + workloadName + "-" + containerName),
			Key:  strings.Join([]string{j.job.Name, workloadType, workloadName, containerName}, "."),
			JobInfo: map[string]string{
				JobNameKey:       j.job.Name,
				"workload_type":  workloadType,
				"workload_name":  workloadName,
				"container_name": containerName,
			},
			JobType: string(config.JobCustomDeploy),
			Spec:    jobTaskSpec,
		}
		resp = append(resp, jobTask)
	}
	j.job.Spec = j.spec
	return resp, nil
}

func (j *CustomDeployJob) LintJob() error {
	return nil
}
