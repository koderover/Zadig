/*
 * Copyright 2022 The KodeRover Authors.
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
	"errors"
	"fmt"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type MeegoTransitionJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.MeegoTransitionJobSpec
}

func (j *MeegoTransitionJob) Instantiate() error {
	j.spec = &commonmodels.MeegoTransitionJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *MeegoTransitionJob) SetPreset() error {
	j.spec = &commonmodels.MeegoTransitionJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *MeegoTransitionJob) SetOptions() error {
	return nil
}

func (j *MeegoTransitionJob) ClearOptions() error {
	return nil
}

func (j *MeegoTransitionJob) ClearSelectionField() error {
	return nil
}

func (j *MeegoTransitionJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.MeegoTransitionJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.MeegoTransitionJobSpec)
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

	if j.spec.MeegoID != latestSpec.MeegoID {
		j.spec.MeegoID = latestSpec.MeegoID
		j.spec.ProjectKey = ""
		j.spec.ProjectName = ""
		j.spec.WorkItemTypeKey = ""
		j.spec.WorkItemType = ""
	} else if j.spec.ProjectKey != latestSpec.ProjectKey {
		j.spec.ProjectKey = latestSpec.ProjectKey
		j.spec.ProjectName = latestSpec.ProjectName
		j.spec.WorkItemTypeKey = ""
		j.spec.WorkItemType = ""
	}
	j.job.Spec = j.spec
	return nil
}

func (j *MeegoTransitionJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.MeegoTransitionJobSpec{}
		if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
	}
	return nil
}

func (j *MeegoTransitionJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.MeegoTransitionJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	meegoInfo, err := commonrepo.NewProjectManagementColl().GetMeegoByID(j.spec.MeegoID)
	if err != nil {
		return nil, errors.New("failed to find meego integration info")
	}
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobMeegoTransition),
		Spec: &commonmodels.MeegoTransitionSpec{
			MeegoID:         meegoInfo.ID.Hex(),
			Link:            meegoInfo.MeegoHost,
			Source:          j.spec.Source,
			ProjectKey:      j.spec.ProjectKey,
			ProjectName:     j.spec.ProjectName,
			WorkItemType:    j.spec.WorkItemType,
			WorkItemTypeKey: j.spec.WorkItemTypeKey,
			WorkItems:       j.spec.WorkItems,
		},
	}
	resp = append(resp, jobTask)
	return resp, nil
}

func (j *MeegoTransitionJob) LintJob() error {
	if err := util.CheckZadigEnterpriseLicense(); err != nil {
		return err
	}

	return nil
}
