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

	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
)

type JiraJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.JiraJobSpec
}

func (j *JiraJob) Instantiate() error {
	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JiraJob) SetPreset() error {
	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JiraJob) SetOptions() error {
	return nil
}

func (j *JiraJob) ClearOptions() error {
	return nil
}

func (j *JiraJob) ClearSelectionField() error {
	return nil
}

func (j *JiraJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JiraJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.JiraJobSpec)
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

	if j.spec.JiraID != latestSpec.JiraID {
		j.spec.JiraID = latestSpec.JiraID
		j.spec.ProjectID = ""
		j.spec.QueryType = ""
		j.spec.JQL = ""
		j.spec.IssueType = ""
		j.spec.Issues = make([]*commonmodels.IssueID, 0)
		j.spec.TargetStatus = ""
	} else if j.spec.ProjectID != latestSpec.ProjectID {
		j.spec.ProjectID = latestSpec.ProjectID
		j.spec.QueryType = ""
		j.spec.JQL = ""
		j.spec.IssueType = ""
		j.spec.Issues = make([]*commonmodels.IssueID, 0)
		j.spec.TargetStatus = ""
	} else {
		j.spec.QueryType = latestSpec.QueryType
		j.spec.JQL = latestSpec.JQL
		j.spec.IssueType = latestSpec.IssueType
		j.spec.TargetStatus = latestSpec.TargetStatus
	}

	j.job.Spec = j.spec
	return nil
}

func (j *JiraJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	if len(j.spec.Issues) == 0 {
		return nil, errors.New("需要指定至少一个 Jira Issue")
	}
	j.job.Spec = j.spec
	info, err := mongodb.NewProjectManagementColl().GetJiraByID(j.spec.JiraID)
	if err != nil {
		return nil, errors.Errorf("get jira info error: %v", err)
	}
	for _, issue := range j.spec.Issues {
		issue.Link = fmt.Sprintf("%s/browse/%s", info.JiraHost, issue.Key)
	}
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobJira),
		Spec: &commonmodels.JobTaskJiraSpec{
			ProjectID:    j.spec.ProjectID,
			JiraID:       j.spec.JiraID,
			IssueType:    j.spec.IssueType,
			Issues:       j.spec.Issues,
			TargetStatus: j.spec.TargetStatus,
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *JiraJob) LintJob() error {
	if err := util.CheckZadigEnterpriseLicense(); err != nil {
		return err
	}

	j.spec = &commonmodels.JiraJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	switch j.spec.Source {
	case setting.VariableSourceRuntime, setting.VariableSourceOther:
	default:
		return errors.New("invalid source")
	}
	return nil
}
