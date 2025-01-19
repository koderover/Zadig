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

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type K8sPacthJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.K8sPatchJobSpec
}

func (j *K8sPacthJob) Instantiate() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) SetPreset() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) SetOptions() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.K8sPatchJobSpec)
	found := false
	for _, stage := range originalWorkflow.Stages {
		if !found {
			for _, job := range stage.Jobs {
				if job.Name == j.job.Name && job.JobType == j.job.JobType {
					if err := commonmodels.IToi(job.Spec, originalSpec); err != nil {
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

	j.spec.PatchItemOptions = originalSpec.PatchItems
	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) ClearOptions() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.PatchItemOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) ClearSelectionField() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.PatchItems = make([]*commonmodels.PatchItem, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.K8sPatchJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.K8sPatchJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.PatchItems = argsSpec.PatchItems
		j.job.Spec = j.spec
	}
	return nil
}

func (j *K8sPacthJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.K8sPatchJobSpec)
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

	if j.spec.ClusterID != latestSpec.ClusterID {
		j.spec.ClusterID = latestSpec.ClusterID
		j.spec.Namespace = ""
		j.spec.PatchItems = make([]*commonmodels.PatchItem, 0)
	} else if j.spec.Namespace != latestSpec.Namespace {
		j.spec.Namespace = latestSpec.Namespace
		j.spec.PatchItems = make([]*commonmodels.PatchItem, 0)
	} else {
		mergedItems := make([]*commonmodels.PatchItem, 0)
		userConfiguredPatchItems := make(map[string]*commonmodels.PatchItem)
		for _, userItem := range j.spec.PatchItems {
			key := fmt.Sprintf("%s++%s++%s++%s", userItem.ResourceName, userItem.ResourceKind, userItem.ResourceGroup, userItem.ResourceVersion)
			userConfiguredPatchItems[key] = userItem
		}

		for _, item := range latestSpec.PatchItems {
			key := fmt.Sprintf("%s++%s++%s++%s", item.ResourceName, item.ResourceKind, item.ResourceGroup, item.ResourceVersion)
			if userConfiguredPatchItem, ok := userConfiguredPatchItems[key]; ok {
				mergedParam := make([]*commonmodels.Param, 0)

				for _, originalParam := range item.Params {
					newParam := &commonmodels.Param{
						Name:         originalParam.Name,
						Description:  originalParam.Description,
						ParamsType:   originalParam.ParamsType,
						Value:        originalParam.Value,
						Repo:         originalParam.Repo,
						ChoiceOption: originalParam.ChoiceOption,
						Default:      originalParam.Default,
						IsCredential: originalParam.IsCredential,
						Source:       originalParam.Source,
					}
					for _, inputKV := range userConfiguredPatchItem.Params {
						if originalParam.Name == inputKV.Name {
							newParam.Value = inputKV.Value
						}
					}
					mergedParam = append(mergedParam, newParam)
				}

				mergedItems = append(mergedItems, &commonmodels.PatchItem{
					ResourceName:    item.ResourceName,
					ResourceKind:    item.ResourceKind,
					ResourceGroup:   item.ResourceGroup,
					ResourceVersion: item.ResourceVersion,
					PatchContent:    userConfiguredPatchItem.PatchContent,
					Params:          mergedParam,
					PatchStrategy:   item.PatchStrategy,
				})
			} else {
				continue
			}
		}
		j.spec.PatchItems = mergedItems
	}

	j.job.Spec = j.spec
	return nil
}

func (j *K8sPacthJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	// logger := log.SugaredLogger()
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.K8sPatchJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobK8sPatch),
		Spec:    patchJobToTaskJob(j.spec),
	}
	resp = append(resp, jobTask)
	j.job.Spec = j.spec
	return resp, nil
}

func (j *K8sPacthJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	return nil
}

func patchJobToTaskJob(job *commonmodels.K8sPatchJobSpec) *commonmodels.JobTasK8sPatchSpec {
	resp := &commonmodels.JobTasK8sPatchSpec{
		ClusterID: job.ClusterID,
		Namespace: job.Namespace,
	}
	for _, patch := range job.PatchItems {
		patchTaskItem := &commonmodels.PatchTaskItem{
			ResourceName:    patch.ResourceName,
			ResourceKind:    patch.ResourceKind,
			ResourceGroup:   patch.ResourceGroup,
			ResourceVersion: patch.ResourceVersion,
			PatchContent:    renderString(patch.PatchContent, setting.RenderValueTemplate, patch.Params),
			PatchStrategy:   patch.PatchStrategy,
			Params:          patch.Params,
		}
		resp.PatchItems = append(resp.PatchItems, patchTaskItem)
	}
	return resp
}
