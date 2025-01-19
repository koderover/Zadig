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

	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
)

type GrafanaJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.GrafanaJobSpec
}

func (j *GrafanaJob) Instantiate() error {
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrafanaJob) SetPreset() error {
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrafanaJob) SetOptions() error {
	return nil
}

func (j *GrafanaJob) ClearOptions() error {
	return nil
}

func (j *GrafanaJob) ClearSelectionField() error {
	return nil
}

func (j *GrafanaJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.GrafanaJobSpec)
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

	j.spec.ID = latestSpec.ID
	j.spec.Name = latestSpec.Name
	j.spec.CheckTime = latestSpec.CheckTime
	j.spec.CheckMode = latestSpec.CheckMode
	userConfiguredAlerts := make(map[string]*commonmodels.GrafanaAlert)
	for _, userAlert := range j.spec.Alerts {
		userConfiguredAlerts[userAlert.ID] = userAlert
	}
	mergedAlerts := make([]*commonmodels.GrafanaAlert, 0)
	for _, alert := range latestSpec.Alerts {
		if userConfiguredAlert, ok := userConfiguredAlerts[alert.ID]; ok {
			mergedAlerts = append(mergedAlerts, userConfiguredAlert)
		}
	}
	j.spec.Alerts = mergedAlerts
	j.job.Spec = j.spec
	return nil
}

func (j *GrafanaJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrafanaJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	if len(j.spec.Alerts) == 0 {
		return nil, errors.New("no alert")
	}
	for _, alert := range j.spec.Alerts {
		alert.Status = "checking"
	}

	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		Key:     j.job.Name,
		JobType: string(config.JobGrafana),
		Spec: &commonmodels.JobTaskGrafanaSpec{
			ID:        j.spec.ID,
			Name:      j.spec.Name,
			CheckTime: j.spec.CheckTime,
			CheckMode: j.spec.CheckMode,
			Alerts:    j.spec.Alerts,
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *GrafanaJob) LintJob() error {
	j.spec = &commonmodels.GrafanaJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	return nil
}
