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
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
)

type CanaryReleaseJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.CanaryReleaseJobSpec
}

func (j *CanaryReleaseJob) Instantiate() error {
	j.spec = &commonmodels.CanaryReleaseJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *CanaryReleaseJob) SetPreset() error {
	j.spec = &commonmodels.CanaryReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *CanaryReleaseJob) SetOptions() error {
	return nil
}

func (j *CanaryReleaseJob) ClearOptions() error {
	return nil
}

func (j *CanaryReleaseJob) ClearSelectionField() error {
	return nil
}

func (j *CanaryReleaseJob) MergeArgs(args *commonmodels.Job) error {
	return nil
}

func (j *CanaryReleaseJob) UpdateWithLatestSetting() error {
	return nil
}

func (j *CanaryReleaseJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}

	j.spec = &commonmodels.CanaryReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	deployJobSpec := &commonmodels.CanaryDeployJobSpec{}
	found := false
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobK8sCanaryDeploy || job.Name != j.spec.FromJob {
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
		return resp, fmt.Errorf("no canary release job: %s found, please check workflow configuration", j.spec.FromJob)
	}
	for _, target := range deployJobSpec.Targets {
		if target.WorkloadName == "" {
			continue
		}
		task := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.K8sServiceName),
			Key:  strings.Join([]string{j.job.Name, target.K8sServiceName}, "."),
			JobInfo: map[string]string{
				JobNameKey:         j.job.Name,
				"k8s_service_name": target.K8sServiceName,
			},
			JobType: string(config.JobK8sCanaryRelease),
			Spec: &commonmodels.JobTaskCanaryReleaseSpec{
				Namespace:      deployJobSpec.Namespace,
				ClusterID:      deployJobSpec.ClusterID,
				ReleaseTimeout: j.spec.ReleaseTimeout,
				K8sServiceName: target.K8sServiceName,
				WorkloadType:   target.WorkloadType,
				WorkloadName:   target.WorkloadName,
				ContainerName:  target.ContainerName,
				Image:          target.Image,
			},
		}
		resp = append(resp, task)
	}
	j.job.Spec = j.spec
	return resp, nil
}

func (j *CanaryReleaseJob) LintJob() error {
	j.spec = &commonmodels.CanaryReleaseJobSpec{}

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
