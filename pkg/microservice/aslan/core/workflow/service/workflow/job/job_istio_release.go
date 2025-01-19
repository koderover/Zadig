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
	"math"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type IstioReleaseJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.IstioJobSpec
}

func (j *IstioReleaseJob) Instantiate() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) SetPreset() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) SetOptions() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.IstioJobSpec)
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

	j.spec.TargetOptions = originalSpec.Targets
	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) ClearOptions() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.TargetOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) ClearSelectionField() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.Targets = make([]*commonmodels.IstioJobTarget, 0)

	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.IstioJobSpec)
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

	// if the job type is changed, use the latest and delete everything else
	if j.spec.First != latestSpec.First {
		j.spec.First = latestSpec.First
		if !j.spec.First {
			j.spec.FromJob = latestSpec.FromJob
			j.spec.Targets = make([]*commonmodels.IstioJobTarget, 0)
		}
		j.spec.ClusterID = ""
		j.spec.RegistryID = ""
		j.spec.Namespace = ""
	} else if j.spec.ClusterID != latestSpec.ClusterID {
		j.spec.ClusterID = latestSpec.ClusterID
		j.spec.RegistryID = ""
		j.spec.Namespace = ""
		j.spec.Targets = make([]*commonmodels.IstioJobTarget, 0)
	} else if j.spec.Namespace != latestSpec.Namespace {
		j.spec.Namespace = latestSpec.Namespace
		j.spec.Targets = make([]*commonmodels.IstioJobTarget, 0)
	}

	j.spec.Timeout = latestSpec.Timeout
	j.spec.ReplicaPercentage = latestSpec.ReplicaPercentage
	j.spec.Weight = latestSpec.Weight

	userConfiguredService := make(map[string]*commonmodels.IstioJobTarget)
	for _, svc := range j.spec.Targets {
		key := fmt.Sprintf("%s++%s", svc.WorkloadName, svc.ContainerName)
		userConfiguredService[key] = svc
	}

	mergedServices := make([]*commonmodels.IstioJobTarget, 0)
	for _, svc := range latestSpec.Targets {
		key := fmt.Sprintf("%s++%s", svc.WorkloadName, svc.ContainerName)
		if userSvc, ok := userConfiguredService[key]; ok {
			mergedServices = append(mergedServices, userSvc)
		}
	}

	j.spec.Targets = mergedServices
	j.job.Spec = j.spec
	return nil
}

func (j *IstioReleaseJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.IstioJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.IstioJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Targets = argsSpec.Targets
		j.job.Spec = j.spec
	}
	return nil
}

func (j *IstioReleaseJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.IstioJobSpec{}

	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	// if from job is empty, it was the first deploy Job.
	firstJob := false
	if j.spec.FromJob != "" {
		if j.spec.Weight > 100 {
			return resp, fmt.Errorf("istio release job: %s release percentage cannot largger than 100", j.job.Name)
		}
		found := false
		for _, stage := range j.workflow.Stages {
			for _, job := range stage.Jobs {
				if job.Name != j.spec.FromJob || job.JobType != config.JobIstioRelease {
					continue
				}
				found = true
				fromJobSpec := &commonmodels.IstioJobSpec{}
				if err := commonmodels.IToi(job.Spec, fromJobSpec); err != nil {
					return resp, err
				}
				j.spec.ClusterID = fromJobSpec.ClusterID
				j.spec.Namespace = fromJobSpec.Namespace
				j.spec.RegistryID = fromJobSpec.RegistryID
				j.spec.Targets = fromJobSpec.Targets
			}
		}
		if !found {
			return resp, fmt.Errorf("gray release job: %s not found", j.spec.FromJob)
		}
	} else {
		firstJob = true
		if j.spec.Weight >= 100 {
			return resp, fmt.Errorf("the first istio release job: %s cannot be released in full", j.job.Name)
		}
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
		target.CurrentReplica = int(*deployment.Spec.Replicas)
	}

	cluster, err := commonrepo.NewK8SClusterColl().Get(j.spec.ClusterID)
	if err != nil {
		return resp, fmt.Errorf("cluster id: %s not found", j.spec.ClusterID)
	}

	for _, target := range j.spec.Targets {
		newReplicaCount := math.Ceil(float64(target.CurrentReplica) * (float64(j.spec.ReplicaPercentage) / 100))
		jobTask := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.WorkloadName),
			JobInfo: map[string]string{
				JobNameKey:      j.job.Name,
				"workload_name": target.WorkloadName,
			},
			JobType: string(config.JobIstioRelease),
			Spec: &commonmodels.JobIstioReleaseSpec{
				FirstJob:          firstJob,
				ClusterID:         j.spec.ClusterID,
				ClusterName:       cluster.Name,
				Namespace:         j.spec.Namespace,
				Weight:            j.spec.Weight,
				Timeout:           j.spec.Timeout,
				ReplicaPercentage: j.spec.ReplicaPercentage,
				Replicas:          int64(newReplicaCount),
				Targets:           target,
			},
		}
		resp = append(resp, jobTask)
	}
	return resp, nil
}

func (j *IstioReleaseJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	j.spec = &commonmodels.IstioJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if j.spec.Weight > 100 {
		return fmt.Errorf("istio release job: [%s] weight cannot be more than 100", j.job.Name)
	}

	//from job was empty means it is the first deploy job.
	if j.spec.FromJob == "" {
		if err := lintFirstIstioReleaseJob(j.job.Name, j.workflow.Stages); err != nil {
			return err
		}
		return nil
	}

	var quoteJobSpec *commonmodels.IstioJobSpec
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobIstioRelease || job.Name != j.spec.FromJob {
				continue
			}
			quoteJobSpec = &commonmodels.IstioJobSpec{}
			if err := commonmodels.IToiYaml(job.Spec, quoteJobSpec); err != nil {
				return err
			}
			break
		}
	}

	if quoteJobSpec == nil {
		return fmt.Errorf("[%s] quote istio relase job: [%s] not found", j.job.Name, j.spec.FromJob)
	}
	if quoteJobSpec.FromJob != "" {
		return fmt.Errorf("[%s] cannot quote a non-first-release job [%s]", j.job.Name, j.spec.FromJob)
	}

	return nil
}

type lintIstioReleaseJob struct {
	jobName string
	weight  int64
}

func lintFirstIstioReleaseJob(jobName string, stages []*commonmodels.WorkflowStage) error {
	jobRankmap := getJobRankMap(stages)
	releaseJobs := []*lintIstioReleaseJob{}
	for _, stage := range stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobIstioRelease {
				continue
			}
			jobSpec := &commonmodels.IstioJobSpec{}
			if err := commonmodels.IToiYaml(job.Spec, jobSpec); err != nil {
				return err
			}
			if jobSpec.FromJob != jobName {
				continue
			}
			releaseJobs = append(releaseJobs, &lintIstioReleaseJob{jobName: job.Name, weight: jobSpec.Weight})
		}
	}
	if len(releaseJobs) == 0 {
		return fmt.Errorf("no release job found for job [%s]", jobName)
	}
	for i, releaseJob := range releaseJobs {
		if jobRankmap[jobName] >= jobRankmap[releaseJob.jobName] {
			return fmt.Errorf("istio release job: [%s] must be run before [%s]", jobName, releaseJob.jobName)
		}
		if i < len(releaseJobs)-1 && releaseJob.weight >= 100 {
			return fmt.Errorf("istio release job: [%s] cannot full release in the middle", releaseJob.jobName)
		}
		if i == len(releaseJobs)-1 && releaseJob.weight != 100 {
			return fmt.Errorf("istio last release job: [%s] must be full released", releaseJob.jobName)
		}
	}
	return nil
}
