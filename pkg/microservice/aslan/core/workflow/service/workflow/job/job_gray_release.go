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

type GrayReleaseJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.GrayReleaseJobSpec
}

func (j *GrayReleaseJob) Instantiate() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrayReleaseJob) SetPreset() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *GrayReleaseJob) SetOptions() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.GrayReleaseJobSpec)
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

func (j *GrayReleaseJob) ClearOptions() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.TargetOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *GrayReleaseJob) ClearSelectionField() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.spec.Targets = make([]*commonmodels.GrayReleaseTarget, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *GrayReleaseJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.GrayReleaseJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.GrayReleaseJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Targets = argsSpec.Targets
		j.job.Spec = j.spec
	}
	return nil
}

func (j *GrayReleaseJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.GrayReleaseJobSpec)
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

	j.spec.DockerRegistryID = latestSpec.DockerRegistryID
	// if cluster is changed, remove all user settings
	if latestSpec.ClusterID != j.spec.ClusterID {
		j.spec.ClusterID = latestSpec.ClusterID
		j.spec.GrayScale = 0
		j.spec.Namespace = ""
		j.spec.FromJob = ""
		j.spec.DeployTimeout = 0
		j.spec.Targets = make([]*commonmodels.GrayReleaseTarget, 0)
	} else if latestSpec.Namespace != j.spec.Namespace {
		j.spec.Namespace = latestSpec.Namespace
		j.spec.Targets = make([]*commonmodels.GrayReleaseTarget, 0)
		j.spec.DeployTimeout = 0
		j.spec.GrayScale = 0
	} else {
		j.spec.DeployTimeout = latestSpec.DeployTimeout
		j.spec.GrayScale = latestSpec.GrayScale
	}

	userConfiguredService := make(map[string]*commonmodels.GrayReleaseTarget)
	for _, svc := range j.spec.Targets {
		key := fmt.Sprintf("%s++%s++%s", svc.WorkloadType, svc.WorkloadName, svc.ContainerName)
		userConfiguredService[key] = svc
	}

	mergedServices := make([]*commonmodels.GrayReleaseTarget, 0)
	for _, svc := range latestSpec.Targets {
		key := fmt.Sprintf("%s++%s++%s", svc.WorkloadType, svc.WorkloadName, svc.ContainerName)
		if userSvc, ok := userConfiguredService[key]; ok {
			mergedServices = append(mergedServices, userSvc)
		}
	}

	j.spec.Targets = mergedServices
	j.job.Spec = j.spec
	return nil
}

func (j *GrayReleaseJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.GrayReleaseJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	// if from job is empty, it was the first deploy Job.
	firstJob := false
	if j.spec.FromJob != "" {
		if j.spec.GrayScale > 100 {
			return resp, fmt.Errorf("release job: %s release percentage cannot largger than 100", j.job.Name)
		}
		found := false
		for _, stage := range j.workflow.Stages {
			for _, job := range stage.Jobs {
				if job.Name != j.spec.FromJob || job.JobType != config.JobK8sGrayRelease {
					continue
				}
				found = true
				fromJobSpec := &commonmodels.GrayReleaseJobSpec{}
				if err := commonmodels.IToi(job.Spec, fromJobSpec); err != nil {
					return resp, err
				}
				j.spec.ClusterID = fromJobSpec.ClusterID
				j.spec.Namespace = fromJobSpec.Namespace
				j.spec.DockerRegistryID = fromJobSpec.DockerRegistryID
				j.spec.Targets = fromJobSpec.Targets
			}
		}
		if !found {
			return resp, fmt.Errorf("gray release job: %s not found", j.spec.FromJob)
		}
	} else {
		firstJob = true
		if j.spec.GrayScale >= 100 {
			return resp, fmt.Errorf("the first release job: %s cannot be released in full", j.job.Name)
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
			target.Replica = int(*deployment.Spec.Replicas)
		}
	}

	cluster, err := commonrepo.NewK8SClusterColl().Get(j.spec.ClusterID)
	if err != nil {
		return resp, fmt.Errorf("cluster id: %s not found", j.spec.ClusterID)
	}

	for _, target := range j.spec.Targets {
		grayReplica := math.Ceil(float64(*&target.Replica) * (float64(j.spec.GrayScale) / 100))
		jobTask := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.WorkloadName),
			Key:  strings.Join([]string{j.job.Name, target.WorkloadName}, "."),
			JobInfo: map[string]string{
				JobNameKey:      j.job.Name,
				"workload_name": target.WorkloadName,
			},
			JobType: string(config.JobK8sGrayRelease),
			Spec: &commonmodels.JobTaskGrayReleaseSpec{
				ClusterID:        j.spec.ClusterID,
				ClusterName:      cluster.Name,
				Namespace:        j.spec.Namespace,
				WorkloadType:     target.WorkloadType,
				WorkloadName:     target.WorkloadName,
				ContainerName:    target.ContainerName,
				FirstJob:         firstJob,
				GrayWorkloadName: target.WorkloadName + config.GrayDeploymentSuffix,
				Image:            target.Image,
				DeployTimeout:    j.spec.DeployTimeout,
				GrayScale:        j.spec.GrayScale,
				TotalReplica:     target.Replica,
				GrayReplica:      int(grayReplica),
			},
		}
		resp = append(resp, jobTask)
	}
	j.job.Spec = j.spec
	return resp, nil
}

func (j *GrayReleaseJob) LintJob() error {
	j.spec = &commonmodels.GrayReleaseJobSpec{}

	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if j.spec.GrayScale > 100 {
		return fmt.Errorf("release job: [%s] release percentage cannot largger than 100", j.job.Name)
	}
	// from job was empty means it is the first deploy job.
	if j.spec.FromJob == "" {
		if err := lintFirstGrayReleaseJob(j.job.Name, j.workflow.Stages); err != nil {
			return err
		}
		return nil
	}
	var quoteJobSpec *commonmodels.GrayReleaseJobSpec
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobK8sGrayRelease || job.Name != j.spec.FromJob {
				continue
			}
			quoteJobSpec = &commonmodels.GrayReleaseJobSpec{}
			if err := commonmodels.IToiYaml(job.Spec, quoteJobSpec); err != nil {
				return err
			}
			break
		}
	}
	if quoteJobSpec == nil {
		return fmt.Errorf("[%s] quote release job: [%s] not found", j.job.Name, j.spec.FromJob)
	}
	if quoteJobSpec.FromJob != "" {
		return fmt.Errorf("[%s] cannot quote a non-first-release job [%s]", j.job.Name, j.spec.FromJob)
	}
	return nil
}

type lintGrayReleaseJob struct {
	jobName   string
	GrayScale int
}

func lintFirstGrayReleaseJob(jobName string, stages []*commonmodels.WorkflowStage) error {
	jobRankmap := getJobRankMap(stages)
	releaseJobs := []*lintGrayReleaseJob{}
	for _, stage := range stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobK8sGrayRelease {
				continue
			}
			jobSpec := &commonmodels.GrayReleaseJobSpec{}
			if err := commonmodels.IToiYaml(job.Spec, jobSpec); err != nil {
				return err
			}
			if jobSpec.FromJob != jobName {
				continue
			}
			releaseJobs = append(releaseJobs, &lintGrayReleaseJob{jobName: job.Name, GrayScale: jobSpec.GrayScale})
		}
	}
	if len(releaseJobs) == 0 {
		return fmt.Errorf("no release job found for job [%s]", jobName)
	}
	for i, releaseJob := range releaseJobs {
		if jobRankmap[jobName] >= jobRankmap[releaseJob.jobName] {
			return fmt.Errorf("release job: [%s] must be run before [%s]", jobName, releaseJob.jobName)
		}
		if i < len(releaseJobs)-1 && releaseJob.GrayScale >= 100 {
			return fmt.Errorf("release job: [%s] cannot full release in the middle", releaseJob.jobName)
		}
		if i == len(releaseJobs)-1 && releaseJob.GrayScale != 100 {
			return fmt.Errorf("last release job: [%s] must be full released", releaseJob.jobName)
		}
	}
	return nil
}
