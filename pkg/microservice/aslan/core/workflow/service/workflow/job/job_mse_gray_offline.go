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
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/types"
)

type MseGrayOfflineJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.MseGrayOfflineJobSpec
}

func (j *MseGrayOfflineJob) Instantiate() error {
	j.spec = &commonmodels.MseGrayOfflineJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *MseGrayOfflineJob) SetPreset() error {
	j.spec = &commonmodels.MseGrayOfflineJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.job.Spec = j.spec
	return nil
}

func (j *MseGrayOfflineJob) SetOptions() error {
	return nil
}

func (j *MseGrayOfflineJob) ClearOptions() error {
	return nil
}

func (j *MseGrayOfflineJob) ClearSelectionField() error {
	return nil
}

func (j *MseGrayOfflineJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.MseGrayOfflineJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *MseGrayOfflineJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *MseGrayOfflineJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.MseGrayOfflineJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	if j.spec.GrayTag == types.ZadigReleaseVersionOriginal {
		return nil, errors.Errorf("gray tag must not be 'original'")
	}

	resp = append(resp, &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobMseGrayOffline),
		Spec: commonmodels.JobTaskMseGrayOfflineSpec{
			Env:        j.spec.Env,
			GrayTag:    j.spec.GrayTag,
			Production: j.spec.Production,
		},
	})

	return resp, nil
}

func (j *MseGrayOfflineJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	j.spec = &commonmodels.MseGrayOfflineJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec

	return nil
}
