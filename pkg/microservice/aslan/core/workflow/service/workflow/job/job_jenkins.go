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
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/koderover/zadig/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/pkg/tool/jenkins"
	"github.com/koderover/zadig/pkg/tool/log"
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
	info, err := mongodb.NewJenkinsIntegrationColl().Get(j.spec.ID)
	if err != nil {
		return errors.Errorf("failed to get Jenkins info from mongo: %v", err)
	}

	client := jenkins.NewClient(info.URL, info.Username, info.Password)
	for _, job := range j.spec.Jobs {
		currentJob, err := client.GetJob(job.JobName)
		if err != nil {
			log.Warnf("Preset JenkinsJob: get job %s error: %v", job.JobName, err)
			continue
		}
		if currentParameters := currentJob.GetParameters(); len(currentParameters) > 0 {
			finalParameters := make([]*commonmodels.JenkinsJobParameter, 0)
			rawParametersMap := make(map[string]*commonmodels.JenkinsJobParameter)
			for _, parameter := range job.Parameters {
				rawParametersMap[parameter.Name] = parameter
			}
			// debug
			b, _ := json.MarshalIndent(rawParametersMap, "", "  ")
			log.Infof("rawParametersMap: %s", string(b))
			for _, currentParameter := range currentParameters {
				if rawParameter, ok := rawParametersMap[currentParameter.Name]; !ok {
					finalParameters = append(finalParameters, &commonmodels.JenkinsJobParameter{
						Name:    currentParameter.Name,
						Value:   fmt.Sprintf("%v", currentParameter.DefaultParameterValue.Value),
						Type:    jenkins.ParameterTypeMap[currentParameter.Type],
						Choices: currentParameter.Choices,
					})
				} else {
					finalParameters = append(finalParameters, rawParameter)
				}
			}
			job.Parameters = finalParameters
			// debug
			b2, _ := json.MarshalIndent(finalParameters, "", "  ")
			log.Infof("finalParametersMap: %s", string(b2))
		}
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
	if _, err := mongodb.NewJenkinsIntegrationColl().Get(j.spec.ID); err != nil {
		return errors.Errorf("not found Jenkins in mongo, err: %v", err)
	}
	return nil
}
