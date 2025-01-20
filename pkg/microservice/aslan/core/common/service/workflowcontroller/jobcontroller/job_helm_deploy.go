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

package jobcontroller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"k8s.io/client-go/rest"
	crClient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/types/job"
)

type HelmDeployJobCtl struct {
	job         *commonmodels.JobTask
	namespace   string
	workflowCtx *commonmodels.WorkflowTaskCtx
	logger      *zap.SugaredLogger
	kubeClient  crClient.Client
	restConfig  *rest.Config
	jobTaskSpec *commonmodels.JobTaskHelmDeploySpec
	ack         func()
}

func NewHelmDeployJobCtl(job *commonmodels.JobTask, workflowCtx *commonmodels.WorkflowTaskCtx, ack func(), logger *zap.SugaredLogger) *HelmDeployJobCtl {
	jobTaskSpec := &commonmodels.JobTaskHelmDeploySpec{}
	if err := commonmodels.IToi(job.Spec, jobTaskSpec); err != nil {
		logger.Error(err)
	}
	job.Spec = jobTaskSpec
	return &HelmDeployJobCtl{
		job:         job,
		workflowCtx: workflowCtx,
		logger:      logger,
		ack:         ack,
		jobTaskSpec: jobTaskSpec,
	}
}

func (c *HelmDeployJobCtl) Clean(ctx context.Context) {}

func (c *HelmDeployJobCtl) Run(ctx context.Context) {
	c.job.Status = config.StatusRunning
	c.ack()

	// set IMAGE job output
	for _, svc := range c.jobTaskSpec.ImageAndModules {
		// helm deploy job key is jobName.serviceName
		c.workflowCtx.GlobalContextSet(job.GetJobOutputKey(c.job.Key+"."+svc.ServiceModule, IMAGEKEY), svc.Image)
	}

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    c.workflowCtx.ProjectName,
		EnvName: c.jobTaskSpec.Env,
	})
	if err != nil {
		msg := fmt.Sprintf("find project %s error: %v", c.workflowCtx.ProjectName, err)
		logError(c.job, msg, c.logger)
		return
	}
	if productInfo.IsSleeping() {
		msg := fmt.Sprintf("Environment %s/%s is sleeping", productInfo.ProductName, productInfo.EnvName)
		logError(c.job, msg, c.logger)
		return
	}

	c.namespace = productInfo.Namespace
	c.jobTaskSpec.ClusterID = productInfo.ClusterID

	updateServiceRevision := false
	if slices.Contains(c.jobTaskSpec.DeployContents, config.DeployConfig) && c.jobTaskSpec.UpdateConfig {
		updateServiceRevision = true
	}

	helmDeploySvc := helmservice.NewHelmDeployService()
	newEnvService, tmplSvc, err := helmDeploySvc.GenNewEnvService(productInfo, c.jobTaskSpec.ServiceName, updateServiceRevision)
	if err != nil {
		msg := fmt.Sprintf("failed to generate new env service, error: %v", err)
		logError(c.job, msg, c.logger)
		return
	}

	if slices.Contains(c.jobTaskSpec.DeployContents, config.DeployConfig) {
		newEnvService.DeployStrategy = setting.ServiceDeployStrategyDeploy
	}

	finalValuesYaml := ""
	if len(c.jobTaskSpec.DeployContents) == 1 && slices.Contains(c.jobTaskSpec.DeployContents, config.DeployImage) {
		finalValuesYaml, err = helmDeploySvc.NewGeneMergedValues(newEnvService, productInfo.DefaultValues, c.jobTaskSpec.GetDeployImages())
		if err != nil {
			msg := fmt.Sprintf("failed to generate merged values yaml, err: %s", err)
			logError(c.job, msg, c.logger)
			return
		}
	} else if len(c.jobTaskSpec.DeployContents) == 1 && slices.Contains(c.jobTaskSpec.DeployContents, config.DeployConfig) {
		// only deploy config
		finalValuesYaml, err = helmDeploySvc.NewGeneMergedValues(newEnvService, productInfo.DefaultValues, nil)
		if err != nil {
			msg := fmt.Sprintf("failed to generate merged values yaml, err: %s", err)
			logError(c.job, msg, c.logger)
			return
		}
	} else {
		images := []string{}
		if c.jobTaskSpec.Source == config.SourceFromJob {
			images = c.jobTaskSpec.GetDeployImages()
		}

		newEnvService.GetServiceRender().SetOverrideYaml(c.jobTaskSpec.VariableYaml)
		finalValuesYaml, err = helmDeploySvc.NewGeneMergedValues(newEnvService, productInfo.DefaultValues, images)
		if err != nil {
			msg := fmt.Sprintf("failed to generate merged values yaml, err: %s", err)
			logError(c.job, msg, c.logger)
			return
		}
	}

	c.jobTaskSpec.YamlContent = finalValuesYaml
	c.jobTaskSpec.UserSuppliedValue = newEnvService.GetServiceRender().GetOverrideYaml()

	c.ack()

	c.logger.Infof("start helm deploy, productName %s serviceName %s namespace %s, values %s, overrideKVs: %s updateServiceRevision %v",
		c.workflowCtx.ProjectName, c.jobTaskSpec.ServiceName, c.namespace, finalValuesYaml, newEnvService.GetServiceRender().OverrideValues, updateServiceRevision)

	timeOut := c.timeout()

	done := make(chan bool)
	go func(chan bool) {
		if err = kube.DeploySingleHelmRelease(productInfo, newEnvService, tmplSvc, nil, c.jobTaskSpec.Timeout, c.workflowCtx.WorkflowTaskCreatorUsername); err != nil {
			err = errors.WithMessagef(err,
				"failed to upgrade helm chart %s/%s",
				c.namespace, c.jobTaskSpec.ServiceName)
			done <- false
		} else {
			done <- true
		}
	}(done)

	// we add timeout check here in case helm stuck in pending status
	select {
	case result := <-done:
		if !result {
			logError(c.job, err.Error(), c.logger)
			return
		}
		break
	case <-time.After(time.Second*time.Duration(timeOut) + time.Minute):
		err = fmt.Errorf("failed to upgrade relase for service: %s, timeout", c.jobTaskSpec.ServiceName)
	}
	if err != nil {
		logError(c.job, err.Error(), c.logger)
		return
	}

	c.job.Status = config.StatusPassed
}

func (c *HelmDeployJobCtl) timeout() int {
	if c.jobTaskSpec.Timeout == 0 {
		c.jobTaskSpec.Timeout = setting.DeployTimeout
	}
	return c.jobTaskSpec.Timeout
}

func (c *HelmDeployJobCtl) SaveInfo(ctx context.Context) error {
	modules := make([]string, 0)
	for _, module := range c.jobTaskSpec.ImageAndModules {
		modules = append(modules, module.ServiceModule)
	}
	moduleList := strings.Join(modules, ",")
	return commonrepo.NewJobInfoColl().Create(context.TODO(), &commonmodels.JobInfo{
		Type:                c.job.JobType,
		WorkflowName:        c.workflowCtx.WorkflowName,
		WorkflowDisplayName: c.workflowCtx.WorkflowDisplayName,
		TaskID:              c.workflowCtx.TaskID,
		ProductName:         c.workflowCtx.ProjectName,
		StartTime:           c.job.StartTime,
		EndTime:             c.job.EndTime,
		Duration:            c.job.EndTime - c.job.StartTime,
		Status:              string(c.job.Status),

		ServiceType:   c.jobTaskSpec.ServiceType,
		ServiceName:   c.jobTaskSpec.ServiceName,
		TargetEnv:     c.jobTaskSpec.Env,
		ServiceModule: moduleList,
		Production:    c.jobTaskSpec.IsProduction,
	})
}

func (c *HelmDeployJobCtl) getVarsYaml() (string, error) {
	vars := []*commonmodels.VariableKV{}
	for _, v := range c.jobTaskSpec.KeyVals {
		vars = append(vars, &commonmodels.VariableKV{Key: v.Key, Value: v.Value})
	}
	return kube.GenerateYamlFromKV(vars)
}
