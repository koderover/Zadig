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

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
)

type UpdateEnvIstioConfigJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.UpdateEnvIstioConfigJobSpec
}

func (j *UpdateEnvIstioConfigJob) Instantiate() error {
	j.spec = &commonmodels.UpdateEnvIstioConfigJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *UpdateEnvIstioConfigJob) SetPreset() error {
	j.spec = &commonmodels.UpdateEnvIstioConfigJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *UpdateEnvIstioConfigJob) SetOptions() error {
	return nil
}

func (j *UpdateEnvIstioConfigJob) ClearOptions() error {
	return nil
}

func (j *UpdateEnvIstioConfigJob) ClearSelectionField() error {
	return nil
}

func (j *UpdateEnvIstioConfigJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *UpdateEnvIstioConfigJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.UpdateEnvIstioConfigJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.UpdateEnvIstioConfigJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec = argsSpec
		j.job.Spec = j.spec
	}
	return nil
}

func (j *UpdateEnvIstioConfigJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.UpdateEnvIstioConfigJobSpec{}

	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	jobTask := &commonmodels.JobTask{
		Name: jobNameFormat(j.job.Name),
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobUpdateEnvIstioConfig),
		Spec:    j.spec,
	}
	resp = append(resp, jobTask)
	return resp, nil
}

func (j *UpdateEnvIstioConfigJob) LintJob() error {
	if err := util.CheckZadigEnterpriseLicense(); err != nil {
		return err
	}

	j.spec = &commonmodels.UpdateEnvIstioConfigJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if j.spec.GrayscaleStrategy == commonmodels.GrayscaleStrategyWeight {
		weight := int32(0)
		for _, config := range j.spec.WeightConfigs {
			weight += config.Weight
		}
		if weight != 100 {
			return fmt.Errorf("Weight sum should be 100, but got %d", weight)
		}
	}

	return nil
}
