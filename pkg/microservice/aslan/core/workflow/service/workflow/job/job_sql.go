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

package job

import (
	"fmt"

	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
)

type SQLJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.SQLJobSpec
}

func (j *SQLJob) Instantiate() error {
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *SQLJob) SetPreset() error {
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *SQLJob) SetOptions() error {
	return nil
}

func (j *SQLJob) ClearOptions() error {
	return nil
}

func (j *SQLJob) ClearSelectionField() error {
	return nil
}

func (j *SQLJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.SQLJobSpec)
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

	if j.spec.ID != latestSpec.ID {
		j.spec.SQL = ""
	}
	j.spec.ID = latestSpec.ID
	j.spec.Type = latestSpec.Type
	j.job.Spec = j.spec
	return nil
}

func (j *SQLJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *SQLJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		Key:     j.job.Name,
		JobType: string(config.JobSQL),
		Spec: &commonmodels.JobTaskSQLSpec{
			ID:   j.spec.ID,
			Type: j.spec.Type,
			SQL:  j.spec.SQL,
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *SQLJob) LintJob() error {
	j.spec = &commonmodels.SQLJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if _, err := mongodb.NewDBInstanceColl().Find(&mongodb.DBInstanceCollFindOption{Id: j.spec.ID}); err != nil {
		return errors.Errorf("not found db instance in mongo, err: %v", err)
	}
	return nil
}
