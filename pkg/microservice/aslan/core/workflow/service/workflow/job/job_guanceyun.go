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
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
)

type GuanceyunCheckJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.GuanceyunCheckJobSpec
}

func (j *GuanceyunCheckJob) Instantiate() error {
	j.spec = &commonmodels.GuanceyunCheckJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GuanceyunCheckJob) SetPreset() error {
	j.spec = &commonmodels.GuanceyunCheckJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GuanceyunCheckJob) SetOptions() error {
	return nil
}

func (j *GuanceyunCheckJob) ClearOptions() error {
	return nil
}

func (j *GuanceyunCheckJob) ClearSelectionField() error {
	return nil
}

func (j *GuanceyunCheckJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *GuanceyunCheckJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.GuanceyunCheckJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GuanceyunCheckJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	j.spec = &commonmodels.GuanceyunCheckJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return nil, err
	}
	j.job.Spec = j.spec

	nameSet := sets.NewString()
	for _, monitor := range j.spec.Monitors {
		if nameSet.Has(monitor.Name) {
			return nil, errors.Errorf("duplicate monitor name %s", monitor.Name)
		}
		nameSet.Insert(monitor.Name)
		monitor.Status = "checking"
	}

	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobGuanceyunCheck),
		Spec: &commonmodels.JobTaskGuanceyunCheckSpec{
			ID:        j.spec.ID,
			Name:      j.spec.Name,
			CheckTime: j.spec.CheckTime,
			CheckMode: j.spec.CheckMode,
			Monitors:  j.spec.Monitors,
		},
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *GuanceyunCheckJob) LintJob() error {
	j.spec = &commonmodels.GuanceyunCheckJobSpec{}

	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	if j.spec.CheckTime <= 0 {
		return errors.Errorf("check time must be greater than 0")
	}
	if len(j.spec.Monitors) == 0 {
		return errors.Errorf("num of check monitor must be greater than 0")
	}
	switch j.spec.CheckMode {
	case "monitor", "trigger":
	default:
		return errors.Errorf("The failed policy is invalid")
	}
	return nil
}
