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
	"context"
	"fmt"

	"github.com/koderover/zadig/v2/pkg/tool/apollo"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
)

type ApolloJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.ApolloJobSpec
}

func (j *ApolloJob) Instantiate() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) SetPreset() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) SetOptions() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	info, err := mongodb.NewConfigurationManagementColl().GetApolloByID(context.Background(), j.spec.ApolloID)
	if err != nil {
		return errors.Errorf("failed to get apollo info from mongo: %v", err)
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ApolloJobSpec)
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

	client := apollo.NewClient(info.ServerAddress, info.Token)
	for _, namespace := range latestSpec.NamespaceList {
		result, err := client.GetNamespace(namespace.AppID, namespace.Env, namespace.ClusterID, namespace.Namespace)
		if err != nil {
			log.Warnf("Preset ApolloJob: get namespace %s-%s-%s-%s error: %v", namespace.AppID, namespace.Env, namespace.ClusterID, namespace.Namespace, err)
			continue
		}
		for _, item := range result.Items {
			if item.Key == "" {
				continue
			}
			namespace.OriginalConfig = append(namespace.OriginalConfig, &commonmodels.ApolloKV{
				Key: item.Key,
				Val: item.Value,
			})
			namespace.KeyValList = append(namespace.KeyValList, &commonmodels.ApolloKV{
				Key: item.Key,
				Val: item.Value,
			})
		}
		if result.Format != "properties" && len(result.Items) == 0 {
			namespace.OriginalConfig = append(namespace.OriginalConfig, &commonmodels.ApolloKV{
				Key: "content",
				Val: "",
			})
			namespace.KeyValList = append(namespace.KeyValList, &commonmodels.ApolloKV{
				Key: "content",
				Val: "",
			})
		}
	}

	j.spec.NamespaceListOption = latestSpec.NamespaceList
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) ClearOptions() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.NamespaceListOption = nil
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) ClearSelectionField() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.NamespaceList = make([]*commonmodels.ApolloNamespace, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.ApolloJobSpec)
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

	mergedNSList := make([]*commonmodels.ApolloNamespace, 0)
	if j.spec.ApolloID != latestSpec.ApolloID {
		j.spec.ApolloID = latestSpec.ApolloID
	} else {
		info, err := mongodb.NewConfigurationManagementColl().GetApolloByID(context.Background(), latestSpec.ApolloID)
		if err != nil {
			return errors.Errorf("failed to get apollo info from mongo: %v", err)
		}

		newNamespaceMap := make(map[string]*commonmodels.ApolloNamespace)
		client := apollo.NewClient(info.ServerAddress, info.Token)
		for _, namespace := range latestSpec.NamespaceList {
			result, err := client.GetNamespace(namespace.AppID, namespace.Env, namespace.ClusterID, namespace.Namespace)
			if err != nil {
				log.Warnf("Preset ApolloJob: get namespace %s-%s-%s-%s error: %v", namespace.AppID, namespace.Env, namespace.ClusterID, namespace.Namespace, err)
				continue
			}
			kvList := make([]*commonmodels.ApolloKV, 0)
			for _, item := range result.Items {
				if item.Key == "" {
					continue
				}
				kvList = append(kvList, &commonmodels.ApolloKV{
					Key: item.Key,
					Val: item.Value,
				})
			}
			if result.Format != "properties" && len(result.Items) == 0 {
				kvList = append(kvList, &commonmodels.ApolloKV{
					Key: "content",
					Val: "",
				})
			}

			key := fmt.Sprintf("%s++%s++%s++%s", namespace.ClusterID, namespace.AppID, namespace.Env, namespace.Namespace)

			newNamespaceMap[key] = &commonmodels.ApolloNamespace{
				AppID:          namespace.AppID,
				ClusterID:      namespace.ClusterID,
				Env:            namespace.Env,
				Namespace:      namespace.Namespace,
				Type:           namespace.Type,
				OriginalConfig: nil,
				KeyValList:     kvList,
			}
		}

		for _, ns := range j.spec.NamespaceList {
			key := fmt.Sprintf("%s++%s++%s++%s", ns.ClusterID, ns.AppID, ns.Env, ns.Namespace)
			if newNs, ok := newNamespaceMap[key]; ok {
				mergedNSList = append(mergedNSList, &commonmodels.ApolloNamespace{
					AppID:          newNs.AppID,
					ClusterID:      newNs.ClusterID,
					Env:            newNs.Env,
					Namespace:      newNs.Namespace,
					Type:           newNs.Type,
					OriginalConfig: newNs.KeyValList,
					KeyValList:     ns.KeyValList,
				})
			}
		}
	}

	j.spec.NamespaceList = mergedNSList
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) MergeArgs(args *commonmodels.Job) error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(args.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *ApolloJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec
	if len(j.spec.NamespaceList) == 0 {
		return nil, errors.New("apollo issue list is empty")
	}
	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		Key:     j.job.Name,
		JobType: string(config.JobApollo),
		Spec: &commonmodels.JobTaskApolloSpec{
			ApolloID: j.spec.ApolloID,
			NamespaceList: func() (list []*commonmodels.JobTaskApolloNamespace) {
				for _, namespace := range j.spec.NamespaceList {
					list = append(list, &commonmodels.JobTaskApolloNamespace{
						ApolloNamespace: *namespace,
					})
				}
				return list
			}(),
		},
		Timeout: 0,
	}
	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *ApolloJob) LintJob() error {
	j.spec = &commonmodels.ApolloJobSpec{}
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if _, err := mongodb.NewConfigurationManagementColl().GetApolloByID(context.Background(), j.spec.ApolloID); err != nil {
		return errors.Errorf("not found apollo in mongo, err: %v", err)
	}
	return nil
}
