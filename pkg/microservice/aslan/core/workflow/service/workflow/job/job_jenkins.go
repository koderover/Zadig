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

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/tool/jenkins"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type JenkinsJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.JenkinsJobSpec
}

func (j *JenkinsJob) Instantiate() error {
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JenkinsJob) SetPreset() error {
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	info, err := mongodb.NewCICDToolColl().Get(j.spec.ID)
	if err != nil {
		return errors.Errorf("failed to get Jenkins info from mongo: %v", err)
	}

	// merge current jenkins job parameters
	client := jenkins.NewClient(info.URL, info.Username, info.Password)
	var eg errgroup.Group
	for _, job := range j.spec.Jobs {
		job := job
		eg.Go(func() error {
			currentJob, err := client.GetJob(job.JobName)
			if err != nil {
				return errors.Errorf("Preset JenkinsJob: get job %s error: %v", job.JobName, err)
			}
			if currentParameters := currentJob.GetParameters(); len(currentParameters) > 0 {
				finalParameters := make([]*commonmodels.JenkinsJobParameter, 0)
				rawParametersMap := make(map[string]*commonmodels.JenkinsJobParameter)
				for _, parameter := range job.Parameters {
					rawParametersMap[parameter.Name] = parameter
				}
				for _, currentParameter := range currentParameters {
					if rawParameter, ok := rawParametersMap[currentParameter.Name]; !ok {
						finalParameters = append(finalParameters, &commonmodels.JenkinsJobParameter{
							Name:  currentParameter.Name,
							Value: fmt.Sprintf("%v", currentParameter.DefaultParameterValue.Value),
							Type: func() config.ParamType {
								if t, ok := jenkins.ParameterTypeMap[currentParameter.Type]; ok {
									return t
								}
								return config.ParamTypeString
							}(),
							Choices: currentParameter.Choices,
						})
					} else {
						finalParameters = append(finalParameters, rawParameter)
					}
				}
				job.Parameters = finalParameters
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JenkinsJob) SetOptions() error {
	return nil
}

func (j *JenkinsJob) ClearOptions() error {
	return nil
}

func (j *JenkinsJob) ClearSelectionField() error {
	return nil
}

func (j *JenkinsJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.JenkinsJobSpec)
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

	j.job.Spec = j.spec
	return nil
}

func (j *JenkinsJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *JenkinsJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	if len(j.spec.Jobs) == 0 {
		return nil, errors.New("Jenkins job list is empty")
	}
	for _, job := range j.spec.Jobs {
		resp = append(resp, &commonmodels.JobTask{
			Name: j.job.Name,
			JobInfo: map[string]string{
				JobNameKey:         j.job.Name,
				"jenkins_job_name": job.JobName,
			},
			Key:     j.job.Name + "." + job.JobName,
			JobType: string(config.JobJenkins),
			Spec: &commonmodels.JobTaskJenkinsSpec{
				ID: j.spec.ID,
				Job: commonmodels.JobTaskJenkinsJobInfo{
					JobName:    job.JobName,
					Parameters: job.Parameters,
				},
			},
			Timeout: 0,
		})
	}

	return resp, nil
}

func (j *JenkinsJob) LintJob() error {
	j.spec = &commonmodels.JenkinsJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if _, err := mongodb.NewCICDToolColl().Get(j.spec.ID); err != nil {
		return errors.Errorf("not found Jenkins in mongo, err: %v", err)
	}
	return nil
}
