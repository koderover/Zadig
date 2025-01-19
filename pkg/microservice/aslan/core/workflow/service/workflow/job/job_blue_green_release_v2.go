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
	"strings"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
)

type BlueGreenReleaseV2Job struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.BlueGreenReleaseV2JobSpec
}

func (j *BlueGreenReleaseV2Job) Instantiate() error {
	j.spec = &commonmodels.BlueGreenReleaseV2JobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenReleaseV2Job) SetPreset() error {
	j.spec = &commonmodels.BlueGreenReleaseV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenReleaseV2Job) SetOptions() error {
	return nil
}

func (j *BlueGreenReleaseV2Job) ClearOptions() error {
	return nil
}

func (j *BlueGreenReleaseV2Job) ClearSelectionField() error {
	return nil
}

func (j *BlueGreenReleaseV2Job) MergeArgs(args *commonmodels.Job) error {
	return nil
}

func (j *BlueGreenReleaseV2Job) UpdateWithLatestSetting() error {
	return nil
}

func (j *BlueGreenReleaseV2Job) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}

	j.spec = &commonmodels.BlueGreenReleaseV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	deployJobSpec := &commonmodels.BlueGreenDeployV2JobSpec{}
	found := false
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobK8sBlueGreenDeploy || job.Name != j.spec.FromJob {
				continue
			}
			if err := commonmodels.IToi(job.Spec, deployJobSpec); err != nil {
				return resp, err
			}
			found = true
			break
		}
	}
	if !found {
		return resp, fmt.Errorf("no blue-green release job: %s found, please check workflow configuration", j.spec.FromJob)
	}

	templateProduct, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("cannot find product %s: %w", j.workflow.Project, err)
	}
	timeout := templateProduct.Timeout * 60

	for _, target := range deployJobSpec.Services {
		task := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.ServiceName),
			Key:  strings.Join([]string{j.job.Name, target.ServiceName}, "."),
			JobInfo: map[string]string{
				JobNameKey:     j.job.Name,
				"service_name": target.ServiceName,
			},
			JobType: string(config.JobK8sBlueGreenRelease),
			Spec: &commonmodels.JobTaskBlueGreenReleaseV2Spec{
				Production:    deployJobSpec.Production,
				Env:           deployJobSpec.Env,
				Service:       target,
				DeployTimeout: timeout,
			},
		}
		resp = append(resp, task)
	}

	j.job.Spec = j.spec
	return resp, nil
}

func (j *BlueGreenReleaseV2Job) LintJob() error {
	j.spec = &commonmodels.BlueGreenReleaseV2JobSpec{}

	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	jobRankMap := getJobRankMap(j.workflow.Stages)
	buildJobRank, ok := jobRankMap[j.spec.FromJob]
	if !ok || buildJobRank >= jobRankMap[j.job.Name] {
		return fmt.Errorf("can not quote job %s in job %s", j.spec.FromJob, j.job.Name)
	}
	return nil
}
