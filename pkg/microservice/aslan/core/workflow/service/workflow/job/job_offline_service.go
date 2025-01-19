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

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type OfflineServiceJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.OfflineServiceJobSpec
}

func (j *OfflineServiceJob) Instantiate() error {
	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *OfflineServiceJob) SetPreset() error {
	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.job.Spec = j.spec
	return nil
}

func (j *OfflineServiceJob) SetOptions() error {
	return nil
}

func (j *OfflineServiceJob) ClearOptions() error {
	return nil
}

func (j *OfflineServiceJob) ClearSelectionField() error {
	return nil
}

func (j *OfflineServiceJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.OfflineServiceJobSpec)
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

	if j.spec.EnvName != latestSpec.EnvName && j.spec.EnvType != latestSpec.EnvType {
		j.spec.Services = make([]string, 0)
	}
	j.spec.Source = latestSpec.Source
	j.spec.EnvType = latestSpec.EnvType
	j.job.Spec = j.spec
	return nil
}

func (j *OfflineServiceJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *OfflineServiceJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec

	jobTask := &commonmodels.JobTask{
		Name:    j.job.Name,
		Key:     j.job.Name,
		JobType: string(config.JobOfflineService),
		Spec: &commonmodels.JobTaskOfflineServiceSpec{
			EnvType: j.spec.EnvType,
			EnvName: j.spec.EnvName,
			ServiceEvents: func() (resp []*commonmodels.JobTaskOfflineServiceEvent) {
				for _, serviceName := range j.spec.Services {
					resp = append(resp, &commonmodels.JobTaskOfflineServiceEvent{
						ServiceName: serviceName,
					})
				}
				return
			}(),
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *OfflineServiceJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	j.spec = &commonmodels.OfflineServiceJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec

	return nil
}
