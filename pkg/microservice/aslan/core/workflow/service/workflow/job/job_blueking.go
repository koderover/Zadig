/*
Copyright 2024 The KodeRover Authors.

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
	"strconv"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type BlueKingJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.BlueKingJobSpec
}

func (j *BlueKingJob) Instantiate() error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueKingJob) SetPreset() error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueKingJob) SetOptions() error {
	return nil
}

func (j *BlueKingJob) ClearOptions() error {
	return nil
}

func (j *BlueKingJob) ClearSelectionField() error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.ExecutionPlanID = 0

	j.job.Spec = j.spec
	return nil
}

func (j *BlueKingJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.BlueKingJobSpec)
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

	if latestSpec.BusinessID != j.spec.BusinessID {
		j.spec.BusinessID = latestSpec.BusinessID
		j.spec.ExecutionPlanID = 0
	}

	j.job.Spec = j.spec
	return nil
}

func (j *BlueKingJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueKingJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return nil, err
	}

	resp := make([]*commonmodels.JobTask, 0)
	resp = append(resp, &commonmodels.JobTask{
		Name: j.job.Name,
		JobInfo: map[string]string{
			JobNameKey:        j.job.Name,
			"blueking_job_id": strconv.FormatInt(j.spec.ExecutionPlanID, 10),
		},
		Key:     j.job.Name + "." + strconv.FormatInt(j.spec.ExecutionPlanID, 10),
		JobType: string(config.JobBlueKing),
		Spec: &commonmodels.JobTaskBlueKingSpec{
			ToolID:          j.spec.ToolID,
			BusinessID:      j.spec.BusinessID,
			ExecutionPlanID: j.spec.ExecutionPlanID,
			Parameters:      j.spec.Parameters,
		},
		Timeout: 0,
	})

	return resp, nil
}

func (j *BlueKingJob) LintJob() error {
	j.spec = &commonmodels.BlueKingJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if _, err := mongodb.NewCICDToolColl().Get(j.spec.ToolID); err != nil {
		return fmt.Errorf("not found Jenkins in mongo, err: %v", err)
	}
	return nil
}
