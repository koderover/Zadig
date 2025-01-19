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
	"strconv"
	"strings"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type GrayRollbackJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.GrayRollbackJobSpec
}

func (j *GrayRollbackJob) Instantiate() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) SetPreset() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	kubeClient, err := kubeclient.GetKubeClient(config.HubServerAddress(), j.spec.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client, err: %v", err)
	}
	newTargets := []*commonmodels.GrayRollbackTarget{}
	for _, target := range j.spec.Targets {
		deployment, found, err := getter.GetDeployment(j.spec.Namespace, target.WorkloadName, kubeClient)
		if err != nil || !found {
			log.Errorf("deployment %s not found in namespace: %s", target.WorkloadName, j.spec.Namespace)
			continue
		}
		rollbackInfo, err := getGrayRollbackInfoFromAnnotations(deployment.GetAnnotations())
		if err != nil {
			log.Errorf("deployment %s get gray rollback info failed: %v", target.WorkloadName, err)
			continue
		}
		target.OriginImage = rollbackInfo.image
		target.OriginReplica = rollbackInfo.replica
		newTargets = append(newTargets, target)
	}
	j.spec.Targets = newTargets
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) SetOptions() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.GrayRollbackJobSpec)
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

	kubeClient, err := kubeclient.GetKubeClient(config.HubServerAddress(), j.spec.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client, err: %v", err)
	}
	newTargets := []*commonmodels.GrayRollbackTarget{}
	for _, target := range originalSpec.Targets {
		deployment, found, err := getter.GetDeployment(originalSpec.Namespace, target.WorkloadName, kubeClient)
		if err != nil || !found {
			log.Errorf("deployment %s not found in namespace: %s", target.WorkloadName, originalSpec.Namespace)
			continue
		}
		rollbackInfo, err := getGrayRollbackInfoFromAnnotations(deployment.GetAnnotations())
		if err != nil {
			log.Errorf("deployment %s get gray rollback info failed: %v", target.WorkloadName, err)
			continue
		}
		target.OriginImage = rollbackInfo.image
		target.OriginReplica = rollbackInfo.replica
		newTargets = append(newTargets, target)
	}

	j.spec.TargetOptions = newTargets
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) ClearOptions() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.Targets = make([]*commonmodels.GrayRollbackTarget, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) ClearSelectionField() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.Targets = make([]*commonmodels.GrayRollbackTarget, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.GrayRollbackJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.GrayRollbackJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Targets = argsSpec.Targets
		j.job.Spec = j.spec
	}
	return nil
}

func (j *GrayRollbackJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.GrayRollbackJobSpec)
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

	// if cluster is changed, remove all settings
	if j.spec.ClusterID != latestSpec.ClusterID {
		j.spec.ClusterID = latestSpec.ClusterID
		j.spec.Namespace = ""
		j.spec.RollbackTimeout = 0
		j.spec.Targets = make([]*commonmodels.GrayRollbackTarget, 0)
	} else if j.spec.Namespace != latestSpec.Namespace {
		j.spec.Namespace = latestSpec.Namespace
		j.spec.RollbackTimeout = 0
		j.spec.Targets = make([]*commonmodels.GrayRollbackTarget, 0)
	} else {
		j.spec.RollbackTimeout = latestSpec.RollbackTimeout
	}

	userConfiguredService := make(map[string]*commonmodels.GrayRollbackTarget)
	for _, svc := range j.spec.Targets {
		key := fmt.Sprintf("%s++%s", svc.WorkloadType, svc.WorkloadName)
		userConfiguredService[key] = svc
	}

	mergedServices := make([]*commonmodels.GrayRollbackTarget, 0)
	for _, svc := range latestSpec.Targets {
		key := fmt.Sprintf("%s++%s", svc.WorkloadType, svc.WorkloadName)
		if userSvc, ok := userConfiguredService[key]; ok {
			mergedServices = append(mergedServices, userSvc)
		}
	}

	j.spec.Targets = mergedServices
	j.job.Spec = j.spec
	return nil
}

func (j *GrayRollbackJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	// logger := log.SugaredLogger()
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.GrayRollbackJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	cluster, err := commonrepo.NewK8SClusterColl().Get(j.spec.ClusterID)
	if err != nil {
		return resp, fmt.Errorf("cluster id: %s not found", j.spec.ClusterID)
	}

	kubeClient, err := kubeclient.GetKubeClient(config.HubServerAddress(), j.spec.ClusterID)
	if err != nil {
		return resp, fmt.Errorf("failed to get kube client, err: %v", err)
	}
	for _, target := range j.spec.Targets {
		deployment, found, err := getter.GetDeployment(j.spec.Namespace, target.WorkloadName, kubeClient)
		if err != nil || !found {
			return resp, fmt.Errorf("deployment %s not found in namespace: %s", target.WorkloadName, j.spec.Namespace)
		}
		rollbackInfo, err := getGrayRollbackInfoFromAnnotations(deployment.GetAnnotations())
		if err != nil {
			return resp, fmt.Errorf("deployment %s get gray rollback info failed: %v", target.WorkloadName, err)
		}
		jobTask := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.WorkloadName),
			Key:  strings.Join([]string{j.job.Name, target.WorkloadName}, "."),
			JobInfo: map[string]string{
				JobNameKey:      j.job.Name,
				"workload_name": target.WorkloadName,
			},
			JobType: string(config.JobK8sGrayRollback),
			Spec: &commonmodels.JobTaskGrayRollbackSpec{
				ClusterID:        j.spec.ClusterID,
				ClusterName:      cluster.Name,
				Namespace:        j.spec.Namespace,
				WorkloadType:     target.WorkloadType,
				WorkloadName:     target.WorkloadName,
				ContainerName:    rollbackInfo.containerName,
				GrayWorkloadName: target.WorkloadName + config.GrayDeploymentSuffix,
				Image:            rollbackInfo.image,
				RollbackTimeout:  j.spec.RollbackTimeout,
				TotalReplica:     rollbackInfo.replica,
			},
		}
		resp = append(resp, jobTask)
	}
	j.job.Spec = j.spec
	return resp, nil
}

type grayRollbackInfo struct {
	image         string
	replica       int
	containerName string
}

func getGrayRollbackInfoFromAnnotations(annotations map[string]string) (*grayRollbackInfo, error) {
	image, ok := annotations[config.GrayImageAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("deployment annotations has no zadig gray image info")
	}
	containerName, ok := annotations[config.GrayContainerAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("deployment annotations has no zadig gray container info")
	}
	replicaStr, ok := annotations[config.GrayReplicaAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("deployment annotations has no zadig gray replica info")
	}
	replica, err := strconv.Atoi(replicaStr)
	if err != nil {
		return nil, fmt.Errorf("annotation replica: %s is not a number", replicaStr)
	}

	return &grayRollbackInfo{
		image:         image,
		replica:       replica,
		containerName: containerName,
	}, nil
}

func (j *GrayRollbackJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	return nil
}
