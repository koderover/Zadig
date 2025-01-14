/*
Copyright 2021 The KodeRover Authors.

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

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"helm.sh/helm/v3/pkg/releaseutil"
	versionedclient "istio.io/client-go/pkg/clientset/versioned"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/ai"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/msg_queue"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	airepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/ai"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/collaboration"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/imnotify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/notify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/render"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/tool/analysis"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/types/step"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/koderover/zadig/v2/pkg/util/boolptr"
	"github.com/koderover/zadig/v2/pkg/util/converter"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

func GetProductDeployType(projectName string) (string, error) {
	projectInfo, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		return "", err
	}
	if projectInfo.IsCVMProduct() {
		return setting.PMDeployType, nil
	}
	if projectInfo.IsHelmProduct() {
		return setting.HelmDeployType, nil
	}
	return setting.K8SDeployType, nil
}

func ListProducts(userID, projectName string, envNames []string, production bool, log *zap.SugaredLogger) ([]*EnvResp, error) {
	envs, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
		Name:                projectName,
		InEnvs:              envNames,
		IsSortByProductName: true,
		Production:          util.GetBoolPointer(production),
	})
	if err != nil {
		log.Errorf("Failed to list envs, err: %s", err)
		return nil, e.ErrListEnvs.AddDesc(err.Error())
	}

	var res []*EnvResp
	defaultRegID := ""
	defaultReg, err := commonservice.FindDefaultRegistry(false, log)
	if err != nil {
		log.Errorf("FindDefaultRegistry error: %v", err)
	} else {
		defaultRegID = defaultReg.ID.Hex()
	}

	clusters, err := commonrepo.NewK8SClusterColl().List(&commonrepo.ClusterListOpts{})
	if err != nil {
		log.Errorf("failed to list clusters, err: %s", err)
		return nil, e.ErrListEnvs.AddErr(err)
	}
	clusterMap := make(map[string]*models.K8SCluster)
	for _, cluster := range clusters {
		clusterMap[cluster.ID.Hex()] = cluster
	}
	getClusterName := func(clusterID string) string {
		cluster, ok := clusterMap[clusterID]
		if ok {
			return cluster.Name
		}
		return ""
	}

	list, err := commonservice.ListFavorites(&mongodb.FavoriteArgs{
		UserID:      userID,
		ProductName: projectName,
		Type:        commonservice.FavoriteTypeEnv,
	})
	if err != nil {
		return nil, errors.Wrap(err, "list favorite environments")
	}
	// add personal favorite data in response
	favSet := sets.NewString(func() []string {
		var nameList []string
		for _, fav := range list {
			nameList = append(nameList, fav.Name)
		}
		return nameList
	}()...)

	envCMMap, err := collaboration.GetEnvCMMap([]string{projectName}, log)
	if err != nil {
		return nil, err
	}
	for _, env := range envs {
		if len(env.RegistryID) == 0 {
			env.RegistryID = defaultRegID
		}

		var baseRefs []string
		if cmSet, ok := envCMMap[collaboration.BuildEnvCMMapKey(env.ProductName, env.EnvName)]; ok {
			baseRefs = append(baseRefs, cmSet.List()...)
		}
		res = append(res, &EnvResp{
			ProjectName:           projectName,
			Name:                  env.EnvName,
			IsPublic:              env.IsPublic,
			IsExisted:             env.IsExisted,
			ClusterName:           getClusterName(env.ClusterID),
			Source:                env.Source,
			Production:            env.Production,
			Status:                env.Status,
			Error:                 env.Error,
			UpdateTime:            env.UpdateTime,
			UpdateBy:              env.UpdateBy,
			RegistryID:            env.RegistryID,
			ClusterID:             env.ClusterID,
			Namespace:             env.Namespace,
			Alias:                 env.Alias,
			BaseRefs:              baseRefs,
			BaseName:              env.BaseName,
			ShareEnvEnable:        env.ShareEnv.Enable,
			ShareEnvIsBase:        env.ShareEnv.IsBase,
			ShareEnvBaseEnv:       env.ShareEnv.BaseEnv,
			IstioGrayscaleEnable:  env.IstioGrayscale.Enable,
			IstioGrayscaleIsBase:  env.IstioGrayscale.IsBase,
			IstioGrayscaleBaseEnv: env.IstioGrayscale.BaseEnv,
			IsFavorite:            favSet.Has(env.EnvName),
		})
	}

	return res, nil
}

// AutoCreateProduct happens in onboarding progress of pm project
func AutoCreateProduct(productName, envType, requestID string, log *zap.SugaredLogger) []*EnvStatus {
	mutexAutoCreate := cache.NewRedisLock(fmt.Sprintf("auto_create_project:%s", productName))
	mutexAutoCreate.Lock()
	defer func() {
		mutexAutoCreate.Unlock()
	}()

	envStatus := make([]*EnvStatus, 0)
	envNames := []string{"dev", "qa"}
	for _, envName := range envNames {
		devStatus := &EnvStatus{
			EnvName: envName,
		}
		status, err := autoCreateProduct(envType, envName, productName, requestID, setting.SystemUser, log)
		devStatus.Status = status
		if err != nil {
			devStatus.ErrMessage = err.Error()
		}
		envStatus = append(envStatus, devStatus)
	}
	return envStatus
}

func InitializeEnvironment(projectKey string, envArgs []*commonmodels.Product, envType string, appType setting.ProjectApplicationType, log *zap.SugaredLogger) error {
	switch envType {
	case config.ProjectTypeVM:
		return initializeVMEnvironmentAndWorkflow(projectKey, appType, envArgs, log)
	default:
		return fmt.Errorf("unsupported env type: %s", envType)
	}
}

func initializeVMEnvironmentAndWorkflow(projectKey string, appType setting.ProjectApplicationType, envArgs []*commonmodels.Product, log *zap.SugaredLogger) error {
	mutexAutoCreate := cache.NewRedisLock(fmt.Sprintf("initialize_vm_project:%s", projectKey))
	err := mutexAutoCreate.TryLock()
	defer func() {
		mutexAutoCreate.Unlock()
	}()

	if err != nil {
		log.Errorf("failed to acquire lock to initialize vm environment, err: %s", err)
		return fmt.Errorf("failed to acquire lock to initialize vm environment, err: %s", err)
	}

	retErr := new(multierror.Error)

	if appType != setting.ProjectApplicationTypeMobile {
		if len(envArgs) == 0 || envArgs == nil {
			return fmt.Errorf("env cannot be empty")
		}

		for _, arg := range envArgs {
			// modify the service revision for the creation process to get the correct env config from the service template.
			for _, serviceList := range arg.Services {
				for _, service := range serviceList {
					svc, err := commonservice.GetServiceTemplate(service.ServiceName, setting.PMDeployType, projectKey, setting.ProductStatusDeleting, 0, false, log)
					if err != nil {
						log.Errorf("failed to find service info for service: %s, error: %s", service.ServiceName, err)
						return fmt.Errorf("failed to find service info for service: %s, error: %s", service.ServiceName, err)
					}
					service.Revision = svc.Revision
				}
			}

			err := CreateProduct("system", "", &ProductCreateArg{Product: arg}, log)
			if err != nil {
				log.Errorf("failed to initialize project env: create env [%s] error: %s", arg.EnvName, err)
				retErr = multierror.Append(retErr, err)
			}

			time.Sleep(2 * time.Second)
		}
	}

	if appType == "" || appType == setting.ProjectApplicationTypeHost {
		for _, arg := range envArgs {
			wf, err := generateHostCustomWorkflow(arg, true)
			if err != nil {
				log.Errorf("failed to generate workflow: %s, error: %s", fmt.Sprintf("%s-workflow-%s", arg.ProductName, arg.EnvName), err)
				retErr = multierror.Append(retErr, fmt.Errorf("failed to generate workflow: %s, error: %s", fmt.Sprintf("%s-workflow-%s", arg.ProductName, arg.EnvName), err))
				continue
			}
			err = workflow.CreateWorkflowV4(setting.SystemUser, wf, log)
			if err != nil {
				log.Errorf("failed to create workflow: %s, error: %s", wf.Name, err)
				retErr = multierror.Append(retErr, fmt.Errorf("failed to create workflow: %s, error: %s", wf.Name, err))
				continue
			}
		}

		opsWorkflow, err := generateHostCustomWorkflow(envArgs[0], false)
		if err != nil {
			log.Errorf("failed to generate workflow: %s, error: %s", fmt.Sprintf("%s-workflow-ops", envArgs[0].ProductName), err)
			retErr = multierror.Append(retErr, fmt.Errorf("failed to generate workflow: %s, error: %s", fmt.Sprintf("%s-workflow-ops", envArgs[0].ProductName), err))
		} else {
			err = workflow.CreateWorkflowV4(setting.SystemUser, opsWorkflow, log)
			if err != nil {
				log.Errorf("failed to create workflow: %s, error: %s", opsWorkflow.Name, err)
				retErr = multierror.Append(retErr, fmt.Errorf("failed to create workflow: %s, error: %s", opsWorkflow.Name, err))
			}
		}
	} else {
		focalBasicImage, err := mongodb.NewBasicImageColl().FindByImageName("ubuntu 20.04")
		if err != nil {
			err = fmt.Errorf("failed to find basic image ubuntu 20.04, error: %v", err)
			log.Error(err)
			retErr = multierror.Append(retErr, err)
		}

		workflowNames := []string{
			fmt.Sprintf("%s-workflow-dev", projectKey),
			fmt.Sprintf("%s-workflow-release", projectKey),
		}

		for _, workflowName := range workflowNames {
			wf, err := generateMobileCustomWorkflow(projectKey, workflowName, focalBasicImage)
			if err != nil {
				log.Errorf("failed to generate mobile workflow, error: %s", err)
				retErr = multierror.Append(retErr, fmt.Errorf("failed to generate workflow, error: %s", err))
			}
			err = workflow.CreateWorkflowV4(setting.SystemUser, wf, log)
			if err != nil {
				log.Errorf("failed to create workflow: %s, error: %s", wf.Name, err)
				retErr = multierror.Append(retErr, fmt.Errorf("failed to create workflow: %s, error: %s", wf.Name, err))
			}
		}

	}

	return retErr.ErrorOrNil()
}

func generateHostCustomWorkflow(arg *models.Product, enableBuildStage bool) (*models.WorkflowV4, error) {
	workflowName := fmt.Sprintf("%s-workflow-%s", arg.ProductName, arg.EnvName)
	if !enableBuildStage {
		workflowName = fmt.Sprintf("%s-workflow-ops", arg.ProductName)
	}
	ret := &models.WorkflowV4{
		Name:             workflowName,
		DisplayName:      workflowName,
		Stages:           nil,
		Project:          arg.ProductName,
		CreatedBy:        setting.SystemUser,
		CreateTime:       time.Now().Unix(),
		ConcurrencyLimit: -1,
	}

	stages := make([]*commonmodels.WorkflowStage, 0)

	s3storageID := ""
	s3storage, err := commonrepo.NewS3StorageColl().FindDefault()
	if err != nil {
		log.Errorf("S3Storage.FindDefault error: %v", err)
	} else {
		projectSet := sets.NewString(s3storage.Projects...)
		if projectSet.Has(arg.ProductName) || projectSet.Has(setting.AllProjects) {
			s3storageID = s3storage.ID.Hex()
		}
	}

	spec := &commonmodels.ZadigVMDeployJobSpec{
		Env:         arg.EnvName,
		Source:      config.SourceRuntime,
		S3StorageID: s3storageID,
	}
	if enableBuildStage {
		productTmpl, err := templaterepo.NewProductColl().Find(arg.ProductName)
		if err != nil {
			errMsg := fmt.Sprintf("[ProductTmpl.Find] %s error: %v", arg.ProductName, err)
			log.Error(errMsg)
			return nil, err
		}

		services, err := commonrepo.NewServiceColl().ListMaxRevisionsForServices(productTmpl.AllTestServiceInfos(), "")
		if err != nil {
			log.Errorf("ServiceTmpl.ListMaxRevisionsByProject error: %v", err)
			return nil, err
		}

		buildList, err := commonrepo.NewBuildColl().List(&commonrepo.BuildListOption{
			ProductName: arg.ProductName,
		})
		if err != nil {
			log.Errorf("[Build.List] error: %v", err)
			return nil, err
		}
		buildMap := map[string]*commonmodels.Build{}
		for _, build := range buildList {
			for _, target := range build.Targets {
				buildMap[target.ServiceName] = build
			}
		}

		buildTargetSet := sets.NewString()
		serviceAndBuilds := []*commonmodels.ServiceAndBuild{}
		for _, serviceTmpl := range services {
			if build, ok := buildMap[serviceTmpl.ServiceName]; ok {
				for _, target := range build.Targets {
					key := fmt.Sprintf("%s-%s", target.ServiceName, target.ServiceModule)
					if buildTargetSet.Has(key) {
						continue
					}
					buildTargetSet.Insert(key)

					serviceAndBuild := &commonmodels.ServiceAndBuild{
						ServiceName:   target.ServiceName,
						ServiceModule: target.ServiceModule,
						BuildName:     build.Name,
					}
					serviceAndBuilds = append(serviceAndBuilds, serviceAndBuild)
				}
			}
		}

		buildJobName := "构建"
		buildJob := &commonmodels.Job{
			Name:    buildJobName,
			JobType: config.JobZadigBuild,
			Spec: &commonmodels.ZadigBuildJobSpec{
				DockerRegistryID: arg.RegistryID,
				ServiceAndBuilds: serviceAndBuilds,
			},
		}
		stage := &commonmodels.WorkflowStage{
			Name:     "构建",
			Jobs:     []*commonmodels.Job{buildJob},
			Parallel: true,
		}

		stages = append(stages, stage)

		spec.Source = config.SourceFromJob
		spec.JobName = buildJobName
	}
	deployJob := &commonmodels.Job{
		Name:    "主机部署",
		JobType: config.JobZadigVMDeploy,
		Spec:    spec,
	}
	stage := &commonmodels.WorkflowStage{
		Name: "主机部署",
		Jobs: []*commonmodels.Job{deployJob},
	}
	stages = append(stages, stage)

	ret.Stages = stages

	return ret, nil
}

func generateMobileCustomWorkflow(projectName, workflowName string, focalBasicImage *commonmodels.BasicImage) (*models.WorkflowV4, error) {

	ret := &models.WorkflowV4{
		Name:             workflowName,
		DisplayName:      workflowName,
		Stages:           nil,
		Project:          projectName,
		CreatedBy:        setting.SystemUser,
		CreateTime:       time.Now().Unix(),
		ConcurrencyLimit: -1,
	}

	stages := make([]*commonmodels.WorkflowStage, 0)

	stageName := "编译并分发"
	freestyleJobName := "编译并分发"
	if strings.HasSuffix(workflowName, "-workflow-release") {
		stageName = "打包并上架"
		freestyleJobName = "打包并上架"
	}
	freestyleJob := &commonmodels.Job{
		Name:    freestyleJobName,
		JobType: config.JobFreestyle,
		Spec: &commonmodels.FreestyleJobSpec{
			Properties: &commonmodels.JobProperties{
				Timeout:         60,
				ResourceRequest: setting.LowRequest,
				ResReqSpec:      setting.LowRequestSpec,
				BuildOS:         focalBasicImage.Value,
				ImageID:         focalBasicImage.ID.Hex(),
				ImageFrom:       focalBasicImage.ImageFrom,
				CacheEnable:     true,
				CacheDirType:    "workspace",
				Infrastructure:  "kubernetes",
				VMLabels:        []string{"VM_LABEL_ANY_ONE"},
			},
			Steps: []*commonmodels.Step{
				{
					Name:     "tools",
					StepType: config.StepTools,
					Spec: &step.StepToolInstallSpec{
						Installs:  []*step.Tool{},
						S3Storage: nil,
					},
				},
				{
					Name:     "git",
					StepType: config.StepGit,
					Spec: &step.StepGitSpec{
						Proxy: nil,
						Repos: []*types.Repository{},
					},
				},
				{
					Name:     "perforce",
					StepType: config.StepPerforce,
					Spec: &step.StepP4Spec{
						Repos: []*types.Repository{},
					},
				},
				{
					Name:     "shell",
					StepType: config.StepShell,
					Spec: &step.StepShellSpec{
						Scripts: []string{},
						Script:  "#!/bin/bash\nset -e",
					},
				},
			},
		},
	}
	stage := &commonmodels.WorkflowStage{
		Name:     stageName,
		Jobs:     []*commonmodels.Job{freestyleJob},
		Parallel: true,
	}

	stages = append(stages, stage)

	ret.Stages = stages

	return ret, nil
}

type UpdateServiceArg struct {
	ServiceName    string                          `json:"service_name"`
	DeployStrategy string                          `json:"deploy_strategy"`
	VariableKVs    []*commontypes.RenderVariableKV `json:"variable_kvs"`
}

type UpdateEnv struct {
	EnvName  string              `json:"env_name"`
	Services []*UpdateServiceArg `json:"services"`
}

func UpdateMultipleK8sEnv(args []*UpdateEnv, envNames []string, productName, requestID string, force, production bool, username string, log *zap.SugaredLogger) ([]*EnvStatus, error) {
	mutexAutoUpdate := cache.NewRedisLock(fmt.Sprintf("update_multiple_product:%s", productName))
	err := mutexAutoUpdate.Lock()
	if err != nil {
		return nil, e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to acquire lock, err: %s", err))
	}
	defer func() {
		mutexAutoUpdate.Unlock()
	}()

	envStatuses := make([]*EnvStatus, 0)

	productsRevision, err := ListProductsRevision(productName, "", production, log)
	if err != nil {
		log.Errorf("UpdateMultipleK8sEnv ListProductsRevision err:%v", err)
		return envStatuses, err
	}
	productMap := make(map[string]*ProductRevision)
	for _, productRevision := range productsRevision {
		if productRevision.ProductName == productName && sets.NewString(envNames...).Has(productRevision.EnvName) && productRevision.Updatable {
			productMap[productRevision.EnvName] = productRevision
			if len(productMap) == len(envNames) {
				break
			}
		}
	}

	errList := &multierror.Error{}
	for _, arg := range args {
		if len(arg.EnvName) == 0 {
			log.Warnf("UpdateMultipleK8sEnv arg.EnvName is empty, skipped")
			continue
		}

		opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: arg.EnvName}
		exitedProd, err := commonrepo.NewProductColl().Find(opt)
		if err != nil {
			log.Errorf("[%s][P:%s] Product.FindByOwner error: %v", arg.EnvName, productName, err)
			errList = multierror.Append(errList, e.ErrUpdateEnv.AddDesc(e.EnvNotFoundErrMsg))
			continue
		}
		if exitedProd.IsSleeping() {
			errList = multierror.Append(errList, e.ErrUpdateEnv.AddDesc("environment is sleeping"))
			continue
		}

		strategyMap := make(map[string]string)
		updateSvcs := make([]*templatemodels.ServiceRender, 0)
		updateRevisionSvcs := make([]string, 0)
		for _, svc := range arg.Services {
			strategyMap[svc.ServiceName] = svc.DeployStrategy

			err = commontypes.ValidateRenderVariables(exitedProd.GlobalVariables, svc.VariableKVs)
			if err != nil {
				errList = multierror.Append(errList, e.ErrUpdateEnv.AddErr(err))
				continue
			}

			updateSvcs = append(updateSvcs, &templatemodels.ServiceRender{
				ServiceName: svc.ServiceName,
				OverrideYaml: &templatemodels.CustomYaml{
					// set YamlContent later
					RenderVariableKVs: svc.VariableKVs,
				},
			})
			updateRevisionSvcs = append(updateRevisionSvcs, svc.ServiceName)
		}

		filter := func(svc *commonmodels.ProductService) bool {
			return util.InStringArray(svc.ServiceName, updateRevisionSvcs)
		}

		// update env default variable, particular svcs from client are involved
		// svc revision will not be updated
		err = updateK8sProduct(exitedProd, username, requestID, updateRevisionSvcs, filter, updateSvcs, strategyMap, force, exitedProd.GlobalVariables, log)
		if err != nil {
			log.Errorf("UpdateMultipleK8sEnv UpdateProductV2 err:%v", err)
			errList = multierror.Append(errList, err)
		}
	}

	productResps := make([]*ProductResp, 0)
	for _, envName := range envNames {
		productResp, err := GetProduct(username, envName, productName, log)
		if err == nil && productResp != nil {
			productResps = append(productResps, productResp)
		}
	}

	for _, productResp := range productResps {
		if productResp.Error != "" {
			envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: setting.ProductStatusFailed, ErrMessage: productResp.Error})
			continue
		}
		envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: productResp.Status})
	}
	return envStatuses, errList.ErrorOrNil()
}

// TODO need optimize
// cvm and k8s yaml projects should not be handled together
func updateProductImpl(updateRevisionSvcs []string, deployStrategy map[string]string, existedProd, updateProd *commonmodels.Product, filter svcUpgradeFilter, user string, log *zap.SugaredLogger) (err error) {
	productName := existedProd.ProductName
	envName := existedProd.EnvName
	namespace := existedProd.Namespace
	updateProd.EnvName = existedProd.EnvName
	updateProd.Namespace = existedProd.Namespace
	updateProd.ClusterID = existedProd.ClusterID

	var allServices []*commonmodels.Service
	var prodRevs *ProductRevision

	// list services with max revision of project
	allServices, err = repository.ListMaxRevisionsServices(productName, existedProd.Production)
	if err != nil {
		log.Errorf("ListAllRevisions error: %s", err)
		err = e.ErrUpdateEnv.AddDesc(err.Error())
		return
	}

	prodRevs, err = GetProductRevision(existedProd, allServices, log)
	if err != nil {
		err = e.ErrUpdateEnv.AddDesc(e.GetEnvRevErrMsg)
		return
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(existedProd.ClusterID)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(existedProd.ClusterID)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	inf, err := clientmanager.NewKubeClientManager().GetInformer(existedProd.ClusterID, namespace)
	if err != nil {
		log.Errorf("[%s][%s] error: %v", envName, namespace, err)
		return e.ErrUpdateEnv.AddDesc(err.Error())
	}

	session := mongotool.Session()
	defer session.EndSession(context.TODO())

	err = mongotool.StartTransaction(session)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	// 遍历产品环境和产品模板交叉对比的结果
	// 四个状态：待删除，待添加，待更新，无需更新
	//var deletedServices []string
	deletedServices := sets.NewString()
	// 1. 如果服务待删除：将产品模板中已经不存在，产品环境中待删除的服务进行删除。
	for _, serviceRev := range prodRevs.ServiceRevisions {
		if serviceRev.Updatable && serviceRev.Deleted && util.InStringArray(serviceRev.ServiceName, updateRevisionSvcs) {
			log.Infof("[%s][P:%s][S:%s] start to delete service", envName, productName, serviceRev.ServiceName)
			//根据namespace: EnvName, selector: productName + serviceName来删除属于该服务的所有资源
			selector := labels.Set{setting.ProductLabel: productName, setting.ServiceLabel: serviceRev.ServiceName}.AsSelector()
			err = commonservice.DeleteNamespacedResource(namespace, selector, existedProd.ClusterID, log)
			if err != nil {
				//删除失败仅记录失败日志
				log.Errorf("delete resource of service %s error:%v", serviceRev.ServiceName, err)
			}
			deletedServices.Insert(serviceRev.ServiceName)
			clusterSelector := labels.Set{setting.ProductLabel: productName, setting.ServiceLabel: serviceRev.ServiceName, setting.EnvNameLabel: envName}.AsSelector()
			err = commonservice.DeleteClusterResource(clusterSelector, existedProd.ClusterID, log)
			if err != nil {
				//删除失败仅记录失败日志
				log.Errorf("delete cluster resource of service %s error:%v", serviceRev.ServiceName, err)
			}
		}
	}

	serviceRevisionMap := getServiceRevisionMap(prodRevs.ServiceRevisions)

	updateProd.Status = setting.ProductStatusUpdating
	updateProd.Error = ""
	updateProd.ShareEnv = existedProd.ShareEnv

	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		mongotool.AbortTransaction(session)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	updateProd.LintServices()
	// 按照产品模板的顺序来创建或者更新服务
	for groupIndex, prodServiceGroup := range updateProd.Services {
		//Mark if there is k8s type service in this group
		var wg sync.WaitGroup

		groupSvcs := make([]*commonmodels.ProductService, 0)
		for svcIndex, prodService := range prodServiceGroup {
			if deletedServices.Has(prodService.ServiceName) {
				continue
			}
			// no need to update service
			if filter != nil && !filter(prodService) {
				groupSvcs = append(groupSvcs, prodService)
				continue
			}

			service := &commonmodels.ProductService{
				ServiceName: prodService.ServiceName,
				ProductName: prodService.ProductName,
				Type:        prodService.Type,
				Revision:    prodService.Revision,
				Render:      prodService.Render,
				Containers:  prodService.Containers,
			}

			// need update service revision
			if util.InStringArray(prodService.ServiceName, updateRevisionSvcs) {
				svcRev, ok := serviceRevisionMap[prodService.ServiceName+prodService.Type]
				if !ok {
					groupSvcs = append(groupSvcs, prodService)
					continue
				}
				service.Revision = svcRev.NextRevision
				service.Containers = svcRev.Containers
				service.UpdateTime = time.Now().Unix()
			}
			groupSvcs = append(groupSvcs, service)

			if prodService.Type == setting.K8SDeployType {
				log.Infof("[Namespace:%s][Product:%s][Service:%s] upsert service", envName, productName, prodService.ServiceName)
				wg.Add(1)
				go func(pSvc *commonmodels.ProductService) {
					defer wg.Done()
					if !commonutil.ServiceDeployed(pSvc.ServiceName, deployStrategy) {
						containers, errFetchImage := fetchWorkloadImages(pSvc, existedProd, kubeClient)
						if errFetchImage != nil {
							service.Error = errFetchImage.Error()
							return
						}
						service.Containers = containers
						return
					}

					curEnv, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: productName, EnvName: envName})
					if err != nil {
						log.Errorf("Failed to find current env %s/%s, error: %v", productName, envName, err)
						service.Error = err.Error()
						return
					}

					items, errUpsertService := upsertService(
						updateProd,
						service,
						curEnv.GetServiceMap()[service.ServiceName],
						!updateProd.Production, inf, kubeClient, istioClient, log)
					if errUpsertService != nil {
						service.Error = errUpsertService.Error()
					} else {
						service.Error = ""
					}
					service.Resources = kube.UnstructuredToResources(items)

					err = commonutil.CreateEnvServiceVersion(updateProd, service, user, session, log)
					if err != nil {
						log.Errorf("CreateK8SEnvServiceVersion error: %v", err)
					}

				}(prodServiceGroup[svcIndex])
			} else if prodService.Type == setting.PMDeployType {
				opt := &commonrepo.ServiceFindOption{
					ServiceName: prodService.ServiceName,
					Type:        prodService.Type,
					Revision:    prodService.Revision,
					ProductName: productName,
				}
				serviceTmpl, err := commonrepo.NewServiceColl().Find(opt)
				if err != nil {
					err = fmt.Errorf("serviceTmpl.Find %s/%s/%d error: %v", prodService.ProductName, prodService.ServiceName, prodService.Revision, err)
					log.Error(err)
					return err
				}

				found := false
				for _, envConfig := range serviceTmpl.EnvConfigs {
					if envConfig.EnvName == envName {
						found = true
					}
				}
				if !found {
					serviceTmpl.EnvConfigs = append(serviceTmpl.EnvConfigs, &commonmodels.EnvConfig{
						EnvName: envName,
					})

					err = commonrepo.NewServiceColl().UpdateServiceEnvConfigs(serviceTmpl)
					if err != nil {
						err = fmt.Errorf("update service %s/%s/%d env configs error: %v", serviceTmpl.ProductName, serviceTmpl.ServiceName, serviceTmpl.Revision, err)
						log.Error(err)
						return err
					}
				}

			}
		}
		wg.Wait()

		err = helmservice.UpdateServicesGroupInEnv(productName, envName, groupIndex, groupSvcs, updateProd.Production)
		if err != nil {
			log.Errorf("Failed to update %s/%s - service group %d. Error: %v", productName, envName, groupIndex, err)
			err = e.ErrUpdateEnv.AddDesc(err.Error())
			mongotool.AbortTransaction(session)
			return
		}
	}

	err = commonrepo.NewProductCollWithSession(session).UpdateGlobalVariable(updateProd)
	if err != nil {
		log.Errorf("failed to update product globalvariable error: %v", err)
		err = e.ErrUpdateEnv.AddDesc(err.Error())
		mongotool.AbortTransaction(session)
		return
	}

	// store deploy strategy
	if deployStrategy != nil {
		if existedProd.ServiceDeployStrategy == nil {
			existedProd.ServiceDeployStrategy = deployStrategy
		} else {
			for k, v := range deployStrategy {
				existedProd.ServiceDeployStrategy[k] = v
			}
		}
		err = commonrepo.NewProductCollWithSession(session).UpdateDeployStrategy(envName, productName, existedProd.ServiceDeployStrategy)
		if err != nil {
			log.Errorf("Failed to update deploy strategy data, error: %v", err)
			err = e.ErrUpdateEnv.AddDesc(err.Error())
			mongotool.AbortTransaction(session)
			return
		}
	}

	return mongotool.CommitTransaction(session)
}

func UpdateProductRegistry(envName, productName, registryID string, production bool, log *zap.SugaredLogger) (err error) {
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       productName,
		Production: &production,
	}
	exitedProd, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("UpdateProductRegistry find product by envName:%s,error: %v", envName, err)
		return e.ErrUpdateEnv.AddDesc(e.EnvNotFoundErrMsg)
	}
	err = commonrepo.NewProductColl().UpdateRegistry(envName, productName, registryID)
	if err != nil {
		log.Errorf("UpdateProductRegistry UpdateRegistry by envName:%s registryID:%s error: %v", envName, registryID, err)
		return e.ErrUpdateEnv.AddErr(err)
	}
	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(exitedProd.ClusterID)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}
	err = ensureKubeEnv(exitedProd.Namespace, registryID, map[string]string{setting.ProductLabel: productName}, false, kubeClient, log)

	if err != nil {
		log.Errorf("UpdateProductRegistry ensureKubeEnv by envName:%s,error: %v", envName, err)
		return err
	}
	return nil
}

func UpdateMultiCVMProducts(envNames []string, productName, user, requestID string, log *zap.SugaredLogger) ([]*EnvStatus, error) {
	errList := &multierror.Error{}
	for _, env := range envNames {
		err := UpdateCVMProduct(env, productName, user, requestID, log)
		if err != nil {
			errList = multierror.Append(errList, err)
		}
	}

	productResps := make([]*ProductResp, 0)
	for _, envName := range envNames {
		productResp, err := GetProduct(user, envName, productName, log)
		if err == nil && productResp != nil {
			productResps = append(productResps, productResp)
		}
	}

	envStatuses := make([]*EnvStatus, 0)
	for _, productResp := range productResps {
		if productResp.Error != "" {
			envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: setting.ProductStatusFailed, ErrMessage: productResp.Error})
			continue
		}
		envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: productResp.Status})
	}
	return envStatuses, errList.ErrorOrNil()
}

func UpdateCVMProduct(envName, productName, user, requestID string, log *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	exitedProd, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("[%s][P:%s] Product.FindByOwner error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.EnvNotFoundErrMsg)
	}
	return updateCVMProduct(exitedProd, user, requestID, log)
}

// CreateProduct create a new product with its dependent stacks
func CreateProduct(user, requestID string, args *ProductCreateArg, log *zap.SugaredLogger) (err error) {
	log.Infof("[%s][P:%s] CreateProduct", args.EnvName, args.ProductName)
	creator := getCreatorBySource(args.Source)
	args.UpdateBy = user
	return creator.Create(user, requestID, args, log)
}

func UpdateProductRecycleDay(envName, productName string, recycleDay int) error {
	return commonrepo.NewProductColl().UpdateProductRecycleDay(envName, productName, recycleDay)
}

func UpdateProductAlias(envName, productName, alias string, production bool) error {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to query product info, name %s", envName))
	}
	if !productInfo.Production {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("cannot set alias for non-production environment %s", envName))
	}
	err = commonrepo.NewProductColl().UpdateProductAlias(envName, productName, alias)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}
	return nil
}

func updateHelmProduct(productName, envName, username, requestID string, overrideCharts []*commonservice.HelmSvcRenderArg, deletedServices []string, log *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	productResp, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("GetProduct envName:%s, productName:%s, err:%+v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(err.Error())
	}
	if productResp.IsSleeping() {
		log.Errorf("Environment is sleeping, cannot update")
		return e.ErrUpdateEnv.AddDesc("Environment is sleeping, cannot update")
	}

	// create product data from product template
	templateProd, err := GetInitProduct(productName, types.GeneralEnv, false, "", productResp.Production, log)
	if err != nil {
		log.Errorf("[%s][P:%s] GetProductTemplate error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.FindProductTmplErrMsg)
	}

	// set image and render to the value used on current environment
	deletedSvcSet := sets.NewString(deletedServices...)
	deletedSvcRevision := make(map[string]int64)
	// services need to be created or updated
	serviceNeedUpdateOrCreate := sets.NewString()
	overrideChartMap := make(map[string]*commonservice.HelmSvcRenderArg)
	for _, chart := range overrideCharts {
		overrideChartMap[chart.ServiceName] = chart
		serviceNeedUpdateOrCreate.Insert(chart.ServiceName)
	}

	productServiceMap := productResp.GetServiceMap()

	// get deleted services map[serviceName]=>serviceRevision
	for _, svc := range productServiceMap {
		if deletedSvcSet.Has(svc.ServiceName) {
			deletedSvcRevision[svc.ServiceName] = svc.Revision
		}
	}

	// get services map[serviceName]=>service
	option := &mongodb.SvcRevisionListOption{
		ProductName:      productResp.ProductName,
		ServiceRevisions: []*mongodb.ServiceRevision{},
	}
	for _, svc := range productServiceMap {
		option.ServiceRevisions = append(option.ServiceRevisions, &mongodb.ServiceRevision{
			ServiceName: svc.ServiceName,
			Revision:    svc.Revision,
		})
	}
	services, err := repository.ListServicesWithSRevision(option, productResp.Production)
	if err != nil {
		log.Errorf("ListServicesWithSRevision error: %v", err)
	}
	serviceMap := make(map[string]*commonmodels.Service)
	for _, svc := range services {
		serviceMap[svc.ServiceName] = svc
	}

	templateSvcMap, err := repository.GetMaxRevisionsServicesMap(productName, productResp.Production)
	if err != nil {
		return fmt.Errorf("GetMaxRevisionsServicesMap product: %s, error: %v", productName, err)
	}

	// use service definition from service template, but keep the image info
	addedReleaseNameSet := sets.NewString()
	allServices := make([][]*commonmodels.ProductService, 0)
	for _, svrs := range templateProd.Services {
		svcGroup := make([]*commonmodels.ProductService, 0)
		for _, svr := range svrs {
			if deletedSvcSet.Has(svr.ServiceName) {
				continue
			}
			ps, ok := productServiceMap[svr.ServiceName]
			// only update or insert services
			if !ok && !serviceNeedUpdateOrCreate.Has(svr.ServiceName) {
				continue
			}

			// existed service has nothing to update
			if ok && !serviceNeedUpdateOrCreate.Has(svr.ServiceName) {
				svcGroup = append(svcGroup, ps)
				continue
			}

			if _, ok := serviceMap[svr.ServiceName]; ok {
				releaseName := util.GeneReleaseName(serviceMap[svr.ServiceName].GetReleaseNaming(), svr.ProductName, productResp.Namespace, productResp.EnvName, svr.ServiceName)
				overrideChartMap[svr.ServiceName].ReleaseName = releaseName
				addedReleaseNameSet.Insert(releaseName)
				svr.ReleaseName = releaseName
			} else if _, ok := templateSvcMap[svr.ServiceName]; ok {
				releaseName := util.GeneReleaseName(templateSvcMap[svr.ServiceName].GetReleaseNaming(), svr.ProductName, productResp.Namespace, productResp.EnvName, svr.ServiceName)
				overrideChartMap[svr.ServiceName].ReleaseName = releaseName
				addedReleaseNameSet.Insert(releaseName)

				if svr.Render == nil {
					svr.GetServiceRender().ChartVersion = templateSvcMap[svr.ServiceName].HelmChart.Version
					svr.GetServiceRender().ValuesYaml = templateSvcMap[svr.ServiceName].HelmChart.ValuesYaml
				}
				svr.ReleaseName = releaseName
			}
			svcGroup = append(svcGroup, svr)

			if ps == nil {
				continue
			}

			svr.Containers = kube.CalculateContainer(ps, serviceMap[svr.ServiceName], svr.Containers, productResp)
		}
		allServices = append(allServices, svcGroup)
	}

	chartSvcMap := productResp.GetChartServiceMap()
	for _, svc := range chartSvcMap {
		if addedReleaseNameSet.Has(svc.ReleaseName) {
			continue
		}
		allServices[0] = append(allServices[0], svc)
	}
	productResp.Services = allServices

	svcNameSet := sets.NewString()
	for _, singleChart := range overrideCharts {
		if singleChart.EnvName != envName {
			continue
		}
		svcNameSet.Insert(singleChart.ServiceName)
	}

	filter := func(svc *commonmodels.ProductService) bool {
		return svcNameSet.Has(svc.ServiceName)
	}

	releases := sets.NewString()
	for _, svc := range productResp.GetSvcList() {
		if filter(svc) {
			releases.Insert(svc.ReleaseName)
		}
	}
	err = kube.CheckReleaseInstalledByOtherEnv(releases, productResp)
	if err != nil {
		return err
	}

	// set status to updating
	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	go func() {
		errMsg := ""
		err := updateHelmProductGroup(username, productName, envName, productResp, overrideCharts, deletedSvcRevision, addedReleaseNameSet, filter, log)
		if err != nil {
			errMsg = err.Error()
			log.Errorf("[%s][P:%s] failed to update product %#v", envName, productName, err)
			// 发送更新产品失败消息给用户
			title := fmt.Sprintf("更新 [%s] 的 [%s] 环境失败", productName, envName)
			notify.SendErrorMessage(username, title, requestID, err, log)

			log.Infof("[%s][P:%s] update error to => %s", envName, productName, err)
			productResp.Status = setting.ProductStatusFailed
		} else {
			productResp.Status = setting.ProductStatusSuccess
		}
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, productResp.Status, errMsg); err != nil {
			log.Errorf("[%s][%s] Product.Update error: %v", envName, productName, err)
			return
		}
	}()
	return nil
}

// updateHelmChartProduct update products with services from helm chart repo
func updateHelmChartProduct(productName, envName, username, requestID string, overrideCharts []*commonservice.HelmSvcRenderArg, deletedReleases []string, log *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	productResp, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("GetProduct envName:%s, productName:%s, err:%+v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(err.Error())
	}
	if productResp.IsSleeping() {
		log.Errorf("Environment is sleeping, cannot update")
		return e.ErrUpdateEnv.AddDesc("Environment is sleeping, cannot update")
	}

	deletedReleaseSet := sets.NewString(deletedReleases...)
	deletedReleaseRevision := make(map[string]int64)
	// services need to be created or updated
	releaseNeedUpdateOrCreate := sets.NewString()
	for _, chart := range overrideCharts {
		releaseNeedUpdateOrCreate.Insert(chart.ReleaseName)
	}

	productServiceMap := productResp.GetServiceMap()
	productChartServiceMap := productResp.GetChartServiceMap()

	// get deleted services map[releaseName]=>serviceRevision
	for _, svc := range productChartServiceMap {
		if deletedReleaseSet.Has(svc.ReleaseName) {
			deletedReleaseRevision[svc.ReleaseName] = svc.Revision
		}
	}

	addedReleaseNameSet := sets.NewString()
	chartSvcMap := make(map[string]*commonmodels.ProductService)
	for _, chart := range overrideCharts {
		svc := &commonmodels.ProductService{
			ServiceName: chart.ServiceName,
			ReleaseName: chart.ReleaseName,
			ProductName: productName,
			Type:        setting.HelmChartDeployType,
		}
		chartSvcMap[svc.ReleaseName] = svc
		addedReleaseNameSet.Insert(svc.ReleaseName)
	}

	option := &mongodb.SvcRevisionListOption{
		ProductName:      productResp.ProductName,
		ServiceRevisions: []*mongodb.ServiceRevision{},
	}
	for _, svc := range productServiceMap {
		option.ServiceRevisions = append(option.ServiceRevisions, &mongodb.ServiceRevision{
			ServiceName: svc.ServiceName,
			Revision:    svc.Revision,
		})
	}
	services, err := repository.ListServicesWithSRevision(option, productResp.Production)
	if err != nil {
		log.Errorf("ListServicesWithSRevision error: %v", err)
	}
	serviceMap := make(map[string]*commonmodels.Service)
	for _, svc := range services {
		serviceMap[svc.ServiceName] = svc
	}

	dupSvcNameSet := sets.NewString()
	allServices := make([][]*commonmodels.ProductService, 0)
	for _, svrs := range productResp.Services {
		svcGroup := make([]*commonmodels.ProductService, 0)
		for _, svr := range svrs {
			if svr.FromZadig() {
				if _, ok := serviceMap[svr.ServiceName]; ok {
					releaseName := util.GeneReleaseName(serviceMap[svr.ServiceName].GetReleaseNaming(), svr.ProductName, productResp.Namespace, productResp.EnvName, svr.ServiceName)
					if addedReleaseNameSet.Has(releaseName) {
						continue
					}
					svr.ReleaseName = releaseName
					dupSvcNameSet.Insert(svr.ServiceName)
				}

				svcGroup = append(svcGroup, svr)
			} else {
				if deletedReleaseSet.Has(svr.ReleaseName) {
					continue
				}

				_, ok := chartSvcMap[svr.ReleaseName]
				if ok {
					delete(chartSvcMap, svr.ReleaseName)
				}

				svcGroup = append(svcGroup, svr)
			}
		}
		allServices = append(allServices, svcGroup)
	}

	if len(allServices) == 0 {
		allServices = append(allServices, []*commonmodels.ProductService{})
	}
	for _, svc := range chartSvcMap {
		allServices[0] = append(allServices[0], svc)
	}

	productResp.Services = allServices

	svcNameSet := sets.NewString()
	for _, singleChart := range overrideCharts {
		if singleChart.EnvName != envName {
			continue
		}
		if singleChart.IsChartDeploy {
			svcNameSet.Insert(singleChart.ReleaseName)
		} else {
			svcNameSet.Insert(singleChart.ServiceName)
		}
	}

	filter := func(svc *commonmodels.ProductService) bool {
		if svc.FromZadig() {
			return svcNameSet.Has(svc.ServiceName)
		} else {
			return svcNameSet.Has(svc.ReleaseName)
		}
	}

	// check if release is installed in other env
	releases := sets.NewString()
	for _, svc := range productResp.GetSvcList() {
		if filter(svc) {
			releases.Insert(svc.ReleaseName)
		}
	}
	err = kube.CheckReleaseInstalledByOtherEnv(releases, productResp)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	// set status to updating
	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	go func() {
		errMsg := ""
		err := updateHelmChartProductGroup(username, productName, envName, productResp, overrideCharts, deletedReleaseRevision, dupSvcNameSet, filter, log)
		if err != nil {
			errMsg = err.Error()
			log.Errorf("[%s][P:%s] failed to update product %#v", envName, productName, err)
			// 发送更新产品失败消息给用户
			title := fmt.Sprintf("更新 [%s] 的 [%s] 环境失败", productName, envName)
			notify.SendErrorMessage(username, title, requestID, err, log)

			log.Infof("[%s][P:%s] update error to => %s", envName, productName, err)
			productResp.Status = setting.ProductStatusFailed
		} else {
			productResp.Status = setting.ProductStatusSuccess
		}
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, productResp.Status, errMsg); err != nil {
			log.Errorf("[%s][%s] Product.Update error: %v", envName, productName, err)
			return
		}
	}()
	return nil
}

func genImageFromYaml(c *commonmodels.Container, valuesYaml, defaultValues, overrideYaml, overrideValues string) (string, error) {
	mergeYaml, err := helmtool.MergeOverrideValues(valuesYaml, defaultValues, overrideYaml, overrideValues, nil)
	if err != nil {
		return "", err
	}
	mergedValuesYamlFlattenMap, err := converter.YamlToFlatMap([]byte(mergeYaml))
	if err != nil {
		return "", err
	}
	imageRule := templatemodels.ImageSearchingRule{
		Repo:      c.ImagePath.Repo,
		Namespace: c.ImagePath.Namespace,
		Image:     c.ImagePath.Image,
		Tag:       c.ImagePath.Tag,
	}
	image, err := commonutil.GeneImageURI(imageRule.GetSearchingPattern(), mergedValuesYamlFlattenMap)
	if err != nil {
		return "", err
	}
	return image, nil
}

func prepareEstimateDataForEnvCreation(productName, serviceName string, production bool, isHelmChartDeploy bool, log *zap.SugaredLogger) (*commonmodels.ProductService, *commonmodels.Service, error) {
	if isHelmChartDeploy {
		prodSvc := &commonmodels.ProductService{
			ServiceName: serviceName,
			ReleaseName: serviceName,
			ProductName: productName,
			Type:        setting.HelmChartDeployType,
			Render: &templatemodels.ServiceRender{
				ServiceName:       serviceName,
				ReleaseName:       serviceName,
				IsHelmChartDeploy: true,
				OverrideYaml:      &templatemodels.CustomYaml{},
			},
		}

		return prodSvc, nil, nil
	} else {
		templateService, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
			ServiceName: serviceName,
			ProductName: productName,
			Type:        setting.HelmDeployType,
		}, production)
		if err != nil {
			log.Errorf("failed to query service, name %s, err %s", serviceName, err)
			return nil, nil, fmt.Errorf("failed to query service, name %s", serviceName)
		}

		prodSvc := &commonmodels.ProductService{
			ServiceName:  serviceName,
			ProductName:  productName,
			Revision:     templateService.Revision,
			Containers:   templateService.Containers,
			VariableYaml: templateService.VariableYaml,
			Render: &templatemodels.ServiceRender{
				ServiceName:  serviceName,
				OverrideYaml: &templatemodels.CustomYaml{},
				ValuesYaml:   templateService.HelmChart.ValuesYaml,
			},
		}

		return prodSvc, templateService, nil
	}
}

func prepareEstimateDataForEnvUpdate(productName, envName, serviceOrReleaseName, scene string, production bool, isHelmChartDeploy bool, log *zap.SugaredLogger) (
	*commonmodels.ProductService, *commonmodels.Service, *commonmodels.Product, error) {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: util.GetBoolPointer(production),
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query product info, name %s", envName)
	}

	var prodSvc *commonmodels.ProductService
	var templateService *commonmodels.Service
	if isHelmChartDeploy {
		prodSvc = productInfo.GetChartServiceMap()[serviceOrReleaseName]
		if prodSvc == nil {
			prodSvc = &commonmodels.ProductService{
				ServiceName: serviceOrReleaseName,
				ReleaseName: serviceOrReleaseName,
				ProductName: productName,
				Type:        setting.HelmChartDeployType,
			}
		}

		if prodSvc.Render == nil {
			prodSvc.Render = &templatemodels.ServiceRender{
				ServiceName:  serviceOrReleaseName,
				OverrideYaml: &templatemodels.CustomYaml{},
			}
		}
	} else {
		targetSvcTmplRevision := int64(0)
		prodSvc = productInfo.GetServiceMap()[serviceOrReleaseName]
		if scene == usageScenarioUpdateRenderSet {
			if prodSvc == nil {
				return nil, nil, nil, fmt.Errorf("can't find service in env: %s, name %s", productInfo.EnvName, serviceOrReleaseName)
			}
			targetSvcTmplRevision = prodSvc.Revision
		}

		templateService, err = repository.QueryTemplateService(&commonrepo.ServiceFindOption{
			ServiceName: serviceOrReleaseName,
			ProductName: productName,
			Type:        setting.HelmDeployType,
			Revision:    targetSvcTmplRevision,
		}, production)
		if err != nil {
			log.Errorf("failed to query service, name %s, err %s", serviceOrReleaseName, err)
			return nil, nil, nil, fmt.Errorf("failed to query service, name %s", serviceOrReleaseName)
		}

		if prodSvc == nil {
			prodSvc = &commonmodels.ProductService{
				ServiceName: serviceOrReleaseName,
				ProductName: productName,
				Revision:    templateService.Revision,
				Containers:  templateService.Containers,
			}
		}

		if prodSvc.Render == nil {
			prodSvc.Render = &templatemodels.ServiceRender{
				ServiceName:  serviceOrReleaseName,
				OverrideYaml: &templatemodels.CustomYaml{},
			}
		}
		prodSvc.Render.ValuesYaml = templateService.HelmChart.ValuesYaml
		prodSvc.Render.ChartVersion = templateService.HelmChart.Version
	}

	return prodSvc, templateService, productInfo, nil
}

func GetAffectedServices(productName, envName string, arg *K8sRendersetArg, log *zap.SugaredLogger) (map[string][]string, error) {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: productName, EnvName: envName})
	if err != nil {
		return nil, fmt.Errorf("failed to find product info, err: %s", err)
	}
	productServiceRevisions, err := commonutil.GetProductUsedTemplateSvcs(productInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to find revision services, err: %s", err)
	}

	ret := make(map[string][]string)
	ret["services"] = make([]string, 0)
	diffKeys, err := yamlutil.DiffFlatKeys(productInfo.DefaultValues, arg.VariableYaml)
	if err != nil {
		return ret, err
	}

	for _, singleSvc := range productServiceRevisions {
		if !commonutil.ServiceDeployed(singleSvc.ServiceName, productInfo.ServiceDeployStrategy) {
			continue
		}
		containsKey, err := yamlutil.ContainsFlatKey(singleSvc.VariableYaml, singleSvc.ServiceVars, diffKeys)
		if err != nil {
			return ret, err
		}
		if containsKey {
			ret["services"] = append(ret["services"], singleSvc.ServiceName)
		}
	}
	return ret, nil
}

func GeneEstimatedValues(productName, envName, serviceOrReleaseName, scene, format string, arg *EstimateValuesArg, isHelmChartDeploy bool, log *zap.SugaredLogger) (interface{}, error) {
	var (
		productSvc  *commonmodels.ProductService
		tmplSvc     *commonmodels.Service
		productInfo *commonmodels.Product
		err         error
	)

	switch scene {
	case usageScenarioCreateEnv:
		productInfo = &commonmodels.Product{}
		productSvc, tmplSvc, err = prepareEstimateDataForEnvCreation(productName, serviceOrReleaseName, arg.Production, isHelmChartDeploy, log)
	default:
		productSvc, tmplSvc, productInfo, err = prepareEstimateDataForEnvUpdate(productName, envName, serviceOrReleaseName, scene, arg.Production, isHelmChartDeploy, log)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to prepare estimated value data, err %s", err)
	}

	targetChart := productSvc.Render

	tempArg := &commonservice.HelmSvcRenderArg{OverrideValues: arg.OverrideValues}
	if targetChart.OverrideYaml == nil {
		targetChart.OverrideYaml = &templatemodels.CustomYaml{}
	}
	targetChart.OverrideYaml.YamlContent = arg.OverrideYaml
	targetChart.OverrideValues = tempArg.ToOverrideValueString()

	mergedValues := ""
	if isHelmChartDeploy {
		chartRepo, err := commonrepo.NewHelmRepoColl().Find(&commonrepo.HelmRepoFindOption{RepoName: arg.ChartRepo})
		if err != nil {
			return nil, fmt.Errorf("failed to query chart-repo info, repoName: %s", arg.ChartRepo)
		}

		client, err := commonutil.NewHelmClient(chartRepo)
		if err != nil {
			return nil, fmt.Errorf("failed to new helm client, err %s", err)
		}

		valuesYaml, err := client.GetChartValues(commonutil.GeneHelmRepo(chartRepo), productName, serviceOrReleaseName, arg.ChartRepo, arg.ChartName, arg.ChartVersion, arg.Production)
		if err != nil {
			return nil, fmt.Errorf("failed to get chart values, chartRepo: %s, chartName: %s, chartVersion: %s, err %s", arg.ChartRepo, arg.ChartName, arg.ChartVersion, err)
		}

		helmDeploySvc := helmservice.NewHelmDeployService()
		mergedValues, err = helmDeploySvc.NewGeneMergedValues(productSvc, productInfo.DefaultValues, nil)
		if err != nil {
			return nil, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to merge values, err %s", err))
		}
		mergedValues, err = helmDeploySvc.GeneFullValues(valuesYaml, mergedValues)
		if err != nil {
			return nil, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to generate full values, err %s", err))
		}
	} else {
		helmDeploySvc := helmservice.NewHelmDeployService()
		mergedValues, err = helmDeploySvc.NewGeneMergedValues(productSvc, productInfo.DefaultValues, nil)
		if err != nil {
			return nil, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to merge values, err %s", err))
		}
		mergedValues, err = helmDeploySvc.GeneFullValues(tmplSvc.HelmChart.ValuesYaml, mergedValues)
		if err != nil {
			return nil, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to generate full values, err %s", err))
		}
	}

	switch format {
	case "flatMap":
		mapData, err := converter.YamlToFlatMap([]byte(mergedValues))
		if err != nil {
			return nil, e.ErrUpdateRenderSet.AddDesc(fmt.Sprintf("failed to generate flat map , err %s", err))
		}
		return mapData, nil
	default:
		return &RawYamlResp{YamlContent: mergedValues}, nil
	}
}

// check if override values or yaml content changes
// return [need-Redeploy] and [need-SaveToDB]
func checkOverrideValuesChange(source *templatemodels.ServiceRender, args *commonservice.HelmSvcRenderArg) (bool, bool) {
	sourceArg := &commonservice.HelmSvcRenderArg{}
	sourceArg.LoadFromRenderChartModel(source)

	same := sourceArg.DiffValues(args)
	switch same {
	case commonservice.Different:
		return true, true
	case commonservice.LogicSame:
		return false, true
	case commonservice.Same:
		return false, false
	}
	return false, false
}

func validateArgs(args *commonservice.ValuesDataArgs) error {
	if args == nil || args.YamlSource != setting.SourceFromVariableSet {
		return nil
	}
	_, err := commonrepo.NewVariableSetColl().Find(&commonrepo.VariableSetFindOption{ID: args.SourceID})
	if err != nil {
		return err
	}
	return nil
}

func UpdateProductDefaultValues(productName, envName, userName, requestID string, args *EnvRendersetArg, production bool, log *zap.SugaredLogger) error {
	// validate if yaml content is legal
	err := yaml.Unmarshal([]byte(args.DefaultValues), map[string]interface{}{})
	if err != nil {
		return err
	}

	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return err
	}
	if product.IsSleeping() {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("environment is sleeping"))
	}

	err = validateArgs(args.ValuesData)
	if err != nil {
		return e.ErrUpdateEnv.AddDesc(fmt.Sprintf("failed to validate args: %s", err))
	}

	err = UpdateProductDefaultValuesWithRender(product, nil, userName, requestID, args, production, log)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(product.ClusterID)
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetKubeClient error, error msg:%s", err)
		return err
	}
	return ensureKubeEnv(product.Namespace, product.RegistryID, map[string]string{setting.ProductLabel: product.ProductName}, false, kubeClient, log)
}

func UpdateProductDefaultValuesWithRender(product *commonmodels.Product, _ *models.RenderSet, userName, requestID string, args *EnvRendersetArg, production bool, log *zap.SugaredLogger) error {
	equal, err := yamlutil.Equal(product.DefaultValues, args.DefaultValues)
	if err != nil {
		return fmt.Errorf("failed to unmarshal default values in renderset, err: %s", err)
	}
	product.DefaultValues = args.DefaultValues
	product.YamlData = geneYamlData(args.ValuesData)
	updatedSvcList := make([]*templatemodels.ServiceRender, 0)
	if !equal {
		diffSvcs, err := PreviewHelmProductGlobalVariables(product.ProductName, product.EnvName, args.DefaultValues, production, log)
		if err != nil {
			return fmt.Errorf("failed to fetch diff services, err: %s", err)
		}
		svcSet := sets.NewString()
		releaseSet := sets.NewString()
		for _, svc := range diffSvcs {
			if !svc.DeployedFromChart {
				svcSet.Insert(svc.ServiceName)
			} else {
				releaseSet.Insert(svc.ReleaseName)
			}
		}
		for _, svc := range product.GetSvcList() {
			if svc.FromZadig() && svcSet.Has(svc.ServiceName) {
				updatedSvcList = append(updatedSvcList, svc.GetServiceRender())
			}
			if !svc.FromZadig() && releaseSet.Has(svc.ReleaseName) {
				updatedSvcList = append(updatedSvcList, svc.GetServiceRender())
			}
		}
	}
	return UpdateProductVariable(product.ProductName, product.EnvName, userName, requestID, updatedSvcList, nil, product.DefaultValues, product.YamlData, log)
}

func UpdateHelmProductCharts(productName, envName, userName, requestID string, production bool, args *EnvRendersetArg, log *zap.SugaredLogger) error {
	if len(args.ChartValues) == 0 {
		return nil
	}

	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return err
	}
	if product.IsSleeping() {
		return e.ErrUpdateEnv.AddDesc("environment is sleeping")
	}

	requestValueMap := make(map[string]*commonservice.HelmSvcRenderArg)
	for _, arg := range args.ChartValues {
		requestValueMap[arg.ServiceName] = arg
	}

	valuesInRenderset := make(map[string]*templatemodels.ServiceRender)
	for _, rc := range product.GetChartRenderMap() {
		valuesInRenderset[rc.ServiceName] = rc
	}

	updatedRcMap := make(map[string]*templatemodels.ServiceRender)
	changedCharts := make([]*commonservice.HelmSvcRenderArg, 0)

	if args.DeployType == setting.HelmChartDeployType {
		for _, arg := range requestValueMap {
			arg.EnvName = envName
			changedCharts = append(changedCharts, arg)
		}

		updateEnvArg := &UpdateMultiHelmProductArg{
			ProductName: productName,
			EnvNames:    []string{envName},
			ChartValues: changedCharts,
		}
		_, err = UpdateMultipleHelmChartEnv(requestID, userName, updateEnvArg, product.Production, log)
		return err
	} else {
		// update override values
		for serviceName, arg := range requestValueMap {
			arg.EnvName = envName
			rcValues, ok := valuesInRenderset[serviceName]
			if !ok {
				log.Errorf("failed to find current chart values for service: %s", serviceName)
				return e.ErrUpdateEnv.AddDesc(fmt.Sprintf("failed to find current chart values for service: %s", serviceName))
			}

			arg.FillRenderChartModel(rcValues, rcValues.ChartVersion, rcValues.ValuesYaml)
			changedCharts = append(changedCharts, arg)
			updatedRcMap[serviceName] = rcValues
		}

		if args.UpdateServiceTmpl {
			// update service to latest revision acts like update service templates
			updateEnvArg := &UpdateMultiHelmProductArg{
				ProductName: productName,
				EnvNames:    []string{envName},
				ChartValues: changedCharts,
			}
			_, err = UpdateMultipleHelmEnv(requestID, userName, updateEnvArg, product.Production, log)
			return err
		} else {
			// update values
			rcList := make([]*templatemodels.ServiceRender, 0)
			for _, rc := range updatedRcMap {
				rcList = append(rcList, rc)
			}

			return UpdateProductVariable(productName, envName, userName, requestID, rcList, nil, product.DefaultValues, product.YamlData, log)
		}
	}
}

func geneYamlData(args *commonservice.ValuesDataArgs) *templatemodels.CustomYaml {
	if args == nil {
		return nil
	}
	ret := &templatemodels.CustomYaml{
		Source:   args.YamlSource,
		AutoSync: args.AutoSync,
	}
	if args.YamlSource == setting.SourceFromVariableSet {
		ret.Source = setting.SourceFromVariableSet
		ret.SourceID = args.SourceID
	} else if args.GitRepoConfig != nil && args.GitRepoConfig.CodehostID > 0 {
		repoData := &models.CreateFromRepo{
			GitRepoConfig: &templatemodels.GitRepoConfig{
				CodehostID: args.GitRepoConfig.CodehostID,
				Owner:      args.GitRepoConfig.Owner,
				Namespace:  args.GitRepoConfig.Namespace,
				Repo:       args.GitRepoConfig.Repo,
				Branch:     args.GitRepoConfig.Branch,
			},
		}
		if len(args.GitRepoConfig.ValuesPaths) > 0 {
			repoData.LoadPath = args.GitRepoConfig.ValuesPaths[0]
		}
		args.YamlSource = setting.SourceFromGitRepo
		ret.SourceDetail = repoData
	}
	return ret
}

func SyncHelmProductEnvironment(productName, envName, requestID string, log *zap.SugaredLogger) error {
	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    productName,
		EnvName: envName,
	})
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return err
	}

	updatedRCMap := make(map[string]*templatemodels.ServiceRender)

	changed, defaultValues, err := SyncYamlFromSource(product.YamlData, product.DefaultValues, product.DefaultValues)
	if err != nil {
		log.Errorf("failed to update default values of env %s:%s", product.ProductName, product.EnvName)
		return err
	}
	if changed {
		product.DefaultValues = defaultValues
		for _, curRenderChart := range product.GetChartRenderMap() {
			updatedRCMap[curRenderChart.ServiceName] = curRenderChart
		}
	}
	for _, chartInfo := range product.GetChartRenderMap() {
		if chartInfo.OverrideYaml == nil {
			continue
		}
		changed, values, err := SyncYamlFromSource(chartInfo.OverrideYaml, chartInfo.OverrideYaml.YamlContent, chartInfo.OverrideYaml.AutoSyncYaml)
		if err != nil {
			return err
		}
		if changed {
			chartInfo.OverrideYaml.YamlContent = values
			chartInfo.OverrideYaml.AutoSyncYaml = values
			updatedRCMap[chartInfo.ServiceName] = chartInfo
		}
	}
	if len(updatedRCMap) == 0 {
		return nil
	}

	// content of values.yaml changed, environment will be updated
	updatedRcList := make([]*templatemodels.ServiceRender, 0)
	for _, updatedRc := range updatedRCMap {
		updatedRcList = append(updatedRcList, updatedRc)
	}

	err = UpdateProductVariable(productName, envName, "cron", requestID, updatedRcList, nil, product.DefaultValues, product.YamlData, log)
	if err != nil {
		return err
	}
	return err
}

func UpdateProductVariable(productName, envName, username, requestID string, updatedSvcs []*templatemodels.ServiceRender,
	_ []*commontypes.GlobalVariableKV, defaultValue string, yamlData *templatemodels.CustomYaml, log *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	productResp, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		log.Errorf("GetProduct envName:%s, productName:%s, err:%+v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(err.Error())
	}
	productResp.ServiceRenders = updatedSvcs

	if productResp.ServiceDeployStrategy == nil {
		productResp.ServiceDeployStrategy = make(map[string]string)
	}
	needUpdateStrategy := false
	for _, rc := range updatedSvcs {
		if !commonutil.ChartDeployed(rc, productResp.ServiceDeployStrategy) {
			needUpdateStrategy = true
			commonutil.SetChartDeployed(rc, productResp.ServiceDeployStrategy)
		}
	}
	if needUpdateStrategy {
		err = commonrepo.NewProductColl().UpdateDeployStrategy(envName, productResp.ProductName, productResp.ServiceDeployStrategy)
		if err != nil {
			log.Errorf("[%s][P:%s] failed to update product deploy strategy: %s", productResp.EnvName, productResp.ProductName, err)
			return e.ErrUpdateEnv.AddErr(err)
		}
	}

	productResp.DefaultValues = defaultValue
	productResp.YamlData = yamlData
	// only update renderset value to db, no need to upgrade chart release
	if len(updatedSvcs) == 0 {
		log.Infof("no need to update svc")
		return commonrepo.NewProductColl().UpdateProductVariables(productResp)
	}

	return updateHelmProductVariable(productResp, username, requestID, log)
}

func updateK8sProductVariable(productResp *commonmodels.Product, userName, requestID string, log *zap.SugaredLogger) error {
	filter := func(service *commonmodels.ProductService) bool {
		for _, sr := range productResp.ServiceRenders {
			if sr.ServiceName == service.ServiceName {
				return true
			}
		}
		return false
	}
	return updateK8sProduct(productResp, userName, requestID, nil, filter, productResp.ServiceRenders, nil, false, productResp.GlobalVariables, log)
}

func updateHelmProductVariable(productResp *commonmodels.Product, userName, requestID string, log *zap.SugaredLogger) error {
	envName, productName := productResp.EnvName, productResp.ProductName

	helmClient, err := helmtool.NewClientFromNamespace(productResp.ClusterID, productResp.Namespace)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	err = commonrepo.NewProductColl().UpdateProductVariables(productResp)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	// set product status to updating
	if err := commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, setting.ProductStatusUpdating, ""); err != nil {
		log.Errorf("[%s][P:%s] Product.UpdateStatus error: %v", envName, productName, err)
		return e.ErrUpdateEnv.AddDesc(e.UpdateEnvStatusErrMsg)
	}

	go func() {
		err := kube.DeployMultiHelmRelease(productResp, helmClient, nil, userName, log)
		if err != nil {
			log.Errorf("error occurred when upgrading services in env: %s/%s, err: %s ", productName, envName, err)
			// 发送更新产品失败消息给用户
			title := fmt.Sprintf("更新 [%s] 的 [%s] 环境失败", productName, envName)
			notify.SendErrorMessage(userName, title, requestID, err, log)
		}
		productResp.Status = setting.ProductStatusSuccess
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, productName, productResp.Status, ""); err != nil {
			log.Errorf("[%s][%s] Product.Update error: %v", envName, productName, err)
			return
		}
	}()
	return nil
}

func UpdateMultipleHelmEnv(requestID, userName string, args *UpdateMultiHelmProductArg, production bool, log *zap.SugaredLogger) ([]*EnvStatus, error) {
	mutexAutoUpdate := cache.NewRedisLock(fmt.Sprintf("update_multiple_product:%s", args.ProductName))
	err := mutexAutoUpdate.Lock()
	if err != nil {
		return nil, e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to acquire lock, err: %s", err))
	}
	defer func() {
		mutexAutoUpdate.Unlock()
	}()

	envNames, productName := args.EnvNames, args.ProductName

	envStatuses := make([]*EnvStatus, 0)
	productsRevision, err := ListProductsRevision(productName, "", production, log)
	if err != nil {
		log.Errorf("UpdateMultiHelmProduct ListProductsRevision err:%v", err)
		return envStatuses, err
	}

	envNameSet := sets.NewString(envNames...)
	productMap := make(map[string]*ProductRevision)
	for _, productRevision := range productsRevision {
		if productRevision.ProductName != productName || !envNameSet.Has(productRevision.EnvName) {
			continue
		}
		// NOTE. there is no need to check if product is updatable anymore
		productMap[productRevision.EnvName] = productRevision
		if len(productMap) == len(envNames) {
			break
		}
	}

	// ensure related services exist in template services
	templateProduct, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		log.Errorf("failed to find template pruduct: %s, err: %s", productName, err)
		return envStatuses, err
	}
	serviceNameSet := sets.NewString()
	allSvcMap := templateProduct.AllServiceInfoMap(production)
	for _, svc := range allSvcMap {
		serviceNameSet.Insert(svc.Name)
	}
	for _, chartValue := range args.ChartValues {
		if !serviceNameSet.Has(chartValue.ServiceName) {
			return envStatuses, fmt.Errorf("failed to find service: %s in product template", chartValue.ServiceName)
		}
	}

	// extract values.yaml and update renderset
	for envName := range productMap {
		err = updateHelmProduct(productName, envName, userName, requestID, args.ChartValues, args.DeletedServices, log)
		if err != nil {
			log.Errorf("UpdateMultiHelmProduct UpdateProductV2 err:%v", err)
			return envStatuses, e.ErrUpdateEnv.AddDesc(err.Error())
		}
	}

	productResps := make([]*ProductResp, 0)
	for _, envName := range envNames {
		productResp, err := GetProduct(setting.SystemUser, envName, productName, log)
		if err == nil && productResp != nil {
			productResps = append(productResps, productResp)
		}
	}

	for _, productResp := range productResps {
		if productResp.Error != "" {
			envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: setting.ProductStatusFailed, ErrMessage: productResp.Error})
			continue
		}
		envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: productResp.Status})
	}

	return envStatuses, nil
}

func UpdateMultipleHelmChartEnv(requestID, userName string, args *UpdateMultiHelmProductArg, production bool, log *zap.SugaredLogger) ([]*EnvStatus, error) {
	mutexUpdateMultiHelm := cache.NewRedisLock(fmt.Sprintf("update_multiple_product:%s", args.ProductName))

	err := mutexUpdateMultiHelm.Lock()
	if err != nil {
		return nil, e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to acquire lock, err: %s", err))
	}
	defer func() {
		mutexUpdateMultiHelm.Unlock()
	}()

	envNames, productName := args.EnvNames, args.ProductName

	envStatuses := make([]*EnvStatus, 0)
	productsRevision, err := ListProductsRevision(productName, "", production, log)
	if err != nil {
		log.Errorf("UpdateMultiHelmProduct ListProductsRevision err:%v", err)
		return envStatuses, err
	}

	envNameSet := sets.NewString(envNames...)
	productMap := make(map[string]*ProductRevision)
	for _, productRevision := range productsRevision {
		if productRevision.ProductName != productName || !envNameSet.Has(productRevision.EnvName) {
			continue
		}
		// NOTE. there is no need to check if product is updatable anymore
		productMap[productRevision.EnvName] = productRevision
		if len(productMap) == len(envNames) {
			break
		}
	}

	// extract values.yaml and update renderset
	for envName := range productMap {
		err = updateHelmChartProduct(productName, envName, userName, requestID, args.ChartValues, args.DeletedServices, log)
		if err != nil {
			log.Errorf("UpdateMultiHelmProduct UpdateProductV2 err:%v", err)
			return envStatuses, e.ErrUpdateEnv.AddDesc(err.Error())
		}
	}

	productResps := make([]*ProductResp, 0)
	for _, envName := range envNames {
		productResp, err := GetProduct(setting.SystemUser, envName, productName, log)
		if err == nil && productResp != nil {
			productResps = append(productResps, productResp)
		}
	}

	for _, productResp := range productResps {
		if productResp.Error != "" {
			envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: setting.ProductStatusFailed, ErrMessage: productResp.Error})
			continue
		}
		envStatuses = append(envStatuses, &EnvStatus{EnvName: productResp.EnvName, Status: productResp.Status})
	}

	return envStatuses, nil
}

func DeleteProduct(username, envName, productName, requestID string, isDelete bool, log *zap.SugaredLogger) (err error) {
	eventStart := time.Now().Unix()
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: util.GetBoolPointer(false),
	})
	if err != nil {
		err = fmt.Errorf("find product error: %v", err)
		log.Error(err)
		return err
	}

	err = commonservice.DeleteManyFavorites(&mongodb.FavoriteArgs{
		ProductName: productName,
		Name:        envName,
		Type:        commonservice.FavoriteTypeEnv,
	})
	if err != nil {
		log.Errorf("DeleteManyFavorites product-%s env-%s error: %v", productName, envName, err)
	}

	// delete informer's cache
	clientmanager.NewKubeClientManager().DeleteInformer(productInfo.ClusterID, productInfo.Namespace)

	envCMMap, err := collaboration.GetEnvCMMap([]string{productName}, log)
	if err != nil {
		return err
	}
	if cmSets, ok := envCMMap[collaboration.BuildEnvCMMapKey(productName, envName)]; ok {
		return fmt.Errorf("this is a base environment, collaborations:%v is related", cmSets.List())
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(productInfo.ClusterID)
	if err != nil {
		return e.ErrDeleteEnv.AddErr(err)
	}

	err = commonrepo.NewProductColl().UpdateStatus(envName, productName, setting.ProductStatusDeleting)
	if err != nil {
		log.Errorf("[%s][%s] update product status error: %v", username, productInfo.Namespace, err)
		return e.ErrDeleteEnv.AddDesc("更新环境状态失败: " + err.Error())
	}

	log.Infof("[%s] delete product %s", username, productInfo.Namespace)
	commonservice.LogProductStats(username, setting.DeleteProductEvent, productName, requestID, eventStart, log)

	err = deleteEnvSleepCron(productInfo.ProductName, productInfo.EnvName)
	if err != nil {
		log.Errorf("deleteEnvSleepCron error: %v", err)
	}

	ctx := context.TODO()
	switch productInfo.Source {
	case setting.SourceFromHelm:
		// Handles environment sharing related operations.
		err = EnsureDeleteShareEnvConfig(ctx, productInfo, istioClient)
		if err != nil {
			log.Errorf("Failed to delete share env config for env %s of product %s: %s", productInfo.EnvName, productInfo.ProductName, err)
		}

		err = commonrepo.NewProductColl().Delete(envName, productName)
		if err != nil {
			log.Errorf("Product.Delete error: %v", err)
		}

		go func() {
			errList := &multierror.Error{}
			defer func() {
				if errList.ErrorOrNil() != nil {
					title := fmt.Sprintf("删除项目:[%s] 环境:[%s] 失败!", productName, envName)
					notify.SendErrorMessage(username, title, requestID, errList.ErrorOrNil(), log)
					_ = commonrepo.NewProductColl().UpdateStatus(envName, productName, setting.ProductStatusUnknown)
				} else {
					title := fmt.Sprintf("删除项目:[%s] 环境:[%s] 成功!", productName, envName)
					content := fmt.Sprintf("namespace:%s", productInfo.Namespace)
					notify.SendMessage(username, title, content, requestID, log)
				}
			}()

			if productInfo.Production {
				return
			}
			if isDelete {
				if hc, errHelmClient := helmtool.NewClientFromNamespace(productInfo.ClusterID, productInfo.Namespace); errHelmClient == nil {
					for _, service := range productInfo.GetServiceMap() {
						if !commonutil.ServiceDeployed(service.ServiceName, productInfo.ServiceDeployStrategy) {
							continue
						}
						if err = kube.UninstallServiceByName(hc, service.ServiceName, productInfo, service.Revision, true); err != nil {
							log.Warnf("UninstallRelease for service %s err:%s", service.ServiceName, err)
							errList = multierror.Append(errList, err)
						}
					}
				} else {
					log.Errorf("failed to get helmClient, err: %s", errHelmClient)
					errList = multierror.Append(errList, e.ErrDeleteEnv.AddErr(errHelmClient))
					return
				}

				sharedNSEnvs, errFindNS := FindNsUseEnvs(productInfo, log)
				if errFindNS != nil {
					err = e.ErrDeleteProduct.AddErr(errFindNS)
				}
				if len(sharedNSEnvs) == 0 {
					s := labels.Set{setting.EnvCreatedBy: setting.EnvCreator}.AsSelector()
					if err = commonservice.DeleteNamespaceIfMatch(productInfo.Namespace, s, productInfo.ClusterID, log); err != nil {
						err = e.ErrDeleteEnv.AddDesc(e.DeleteNamespaceErrMsg + ": " + err.Error())
						return
					}
				}
			} else {
				if err := commonservice.DeleteZadigLabelFromNamespace(productInfo.Namespace, productInfo.ClusterID, log); err != nil {
					errList = multierror.Append(errList, e.ErrDeleteEnv.AddDesc(e.DeleteNamespaceErrMsg+": "+err.Error()))
					return
				}
			}
		}()
	case setting.SourceFromExternal:
		err = commonrepo.NewProductColl().Delete(envName, productName)
		if err != nil {
			log.Errorf("Product.Delete error: %v", err)
		}

	case setting.SourceFromPM:
		err = commonrepo.NewProductColl().Delete(envName, productName)
		if err != nil {
			log.Errorf("Product.Delete error: %v", err)
		}
	default:
		go func() {
			var err error

			defer func() {
				if err != nil {
					title := fmt.Sprintf("删除项目:[%s] 环境:[%s] 失败!", productName, envName)
					notify.SendErrorMessage(username, title, requestID, err, log)
					_ = commonrepo.NewProductColl().UpdateStatus(envName, productName, setting.ProductStatusUnknown)
				} else {
					title := fmt.Sprintf("删除项目:[%s] 环境:[%s] 成功!", productName, envName)
					content := fmt.Sprintf("namespace:%s", productInfo.Namespace)
					notify.SendMessage(username, title, content, requestID, log)
				}
			}()

			err = commonrepo.NewProductColl().Delete(envName, productName)
			if err != nil {
				log.Errorf("Product.Delete error: %v", err)
			}
			if productInfo.Production {
				return
			}

			if isDelete {
				svcNames := make([]string, 0)
				for svcName := range productInfo.GetServiceMap() {
					svcNames = append(svcNames, svcName)
				}

				// @todo fix env already deleted issue, may cause service not really deleted in k8s
				err = DeleteProductServices("", requestID, envName, productName, svcNames, false, log)
				if err != nil {
					log.Warnf("DeleteProductServices error: %v", err)
				}

				// Handles environment sharing related operations.
				err = EnsureDeleteShareEnvConfig(ctx, productInfo, istioClient)
				if err != nil {
					log.Errorf("Failed to delete share env config: %s, env: %s/%s", err, productInfo.ProductName, productInfo.EnvName)
					err = e.ErrDeleteProduct.AddDesc(e.DeleteVirtualServiceErrMsg + ": " + err.Error())
					return
				}

				sharedNSEnvs, errFindNS := FindNsUseEnvs(productInfo, log)
				if errFindNS != nil {
					err = e.ErrDeleteProduct.AddErr(errFindNS)
				}
				if len(sharedNSEnvs) == 0 {
					s := labels.Set{setting.EnvCreatedBy: setting.EnvCreator}.AsSelector()
					if err = commonservice.DeleteNamespaceIfMatch(productInfo.Namespace, s, productInfo.ClusterID, log); err != nil {
						err = e.ErrDeleteEnv.AddDesc(e.DeleteNamespaceErrMsg + ": " + err.Error())
						return
					}
				}
			} else {
				if err := commonservice.DeleteZadigLabelFromNamespace(productInfo.Namespace, productInfo.ClusterID, log); err != nil {
					err = e.ErrDeleteEnv.AddDesc(e.DeleteNamespaceErrMsg + ": " + err.Error())
					return
				}
			}
		}()
	}

	return nil
}

func DeleteProductServices(userName, requestID, envName, productName string, serviceNames []string, production bool, log *zap.SugaredLogger) (err error) {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: productName, EnvName: envName, Production: util.GetBoolPointer(production)})
	if err != nil {
		err = fmt.Errorf("failed to find product, productName: %s, envName: %s, production: %v, error: %v", productName, envName, production, err)
		log.Error(err)
		return err
	}
	if getProjectType(productName) == setting.HelmDeployType {
		return deleteHelmProductServices(userName, requestID, productInfo, serviceNames, log)
	}
	return deleteK8sProductServices(productInfo, serviceNames, log)
}

func DeleteProductHelmReleases(userName, requestID, envName, productName string, releases []string, production bool, log *zap.SugaredLogger) (err error) {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{Name: productName, EnvName: envName, Production: util.GetBoolPointer(production)})
	if err != nil {
		log.Errorf("find product error: %v", err)
		return err
	}
	return kube.DeleteHelmReleaseFromEnv(userName, requestID, productInfo, releases, log)
}

func deleteHelmProductServices(userName, requestID string, productInfo *commonmodels.Product, serviceNames []string, log *zap.SugaredLogger) error {
	return kube.DeleteHelmServiceFromEnv(userName, requestID, productInfo, serviceNames, log)
}

func deleteK8sProductServices(productInfo *commonmodels.Product, serviceNames []string, log *zap.SugaredLogger) error {
	serviceRelatedYaml := make(map[string]string)
	for _, service := range productInfo.GetServiceMap() {
		if !commonutil.ServiceDeployed(service.ServiceName, productInfo.ServiceDeployStrategy) {
			continue
		}
		if util.InStringArray(service.ServiceName, serviceNames) {
			yaml, _, err := kube.FetchCurrentAppliedYaml(&kube.GeneSvcYamlOption{
				ProductName: productInfo.ProductName,
				EnvName:     productInfo.EnvName,
				ServiceName: service.ServiceName,
				UnInstall:   true,
			})
			if err != nil {
				log.Errorf("failed to remove k8s resources when rendering yaml for service : %s, err: %s", service.ServiceName, err)
				return fmt.Errorf("failed to remove k8s resources when rendering yaml for service : %s, err: %s", service.ServiceName, err)
			}
			serviceRelatedYaml[service.ServiceName] = yaml
		}
	}

	newServices := make([][]*commonmodels.ProductService, 0)
	for _, serviceGroup := range productInfo.Services {
		var group []*commonmodels.ProductService
		for _, service := range serviceGroup {
			if !util.InStringArray(service.ServiceName, serviceNames) {
				group = append(group, service)
			}
		}
		newServices = append(newServices, group)
	}
	err := helmservice.UpdateAllServicesInEnv(productInfo.ProductName, productInfo.EnvName, newServices, productInfo.Production)
	if err != nil {
		log.Errorf("failed to UpdateHelmProductServices %s/%s, error: %v", productInfo.ProductName, productInfo.EnvName, err)
		return err
	}

	// remove related service in global variables
	productInfo.GlobalVariables = commontypes.RemoveGlobalVariableRelatedService(productInfo.GlobalVariables, serviceNames...)

	for _, singleName := range serviceNames {
		delete(productInfo.ServiceDeployStrategy, singleName)
	}
	err = commonrepo.NewProductColl().UpdateDeployStrategyAndGlobalVariable(productInfo.EnvName, productInfo.ProductName, productInfo.ServiceDeployStrategy, productInfo.GlobalVariables)
	if err != nil {
		log.Errorf("failed to update product deploy strategy, err: %s", err)
	}

	ctx := context.TODO()
	kclient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(productInfo.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}

	istioClient, err := clientmanager.NewKubeClientManager().GetIstioClientSet(productInfo.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to new istio client: %s", err)
	}

	for _, name := range serviceNames {
		if !commonutil.ServiceDeployed(name, productInfo.ServiceDeployStrategy) {
			continue
		}

		unstructuredList, _, err := kube.ManifestToUnstructured(serviceRelatedYaml[name])
		if err != nil {
			// Only record and do not block subsequent traversals.
			log.Errorf("failed to convert k8s manifest to unstructured list when deleting service: %s, err: %s", name, err)
		}
		for _, unstructured := range unstructuredList {
			if unstructured.GetKind() == setting.Service {
				svcName := unstructured.GetName()
				err = EnsureDeleteZadigService(ctx, productInfo, svcName, kclient, istioClient)
				if err != nil {
					// Only record and do not block subsequent traversals.
					log.Errorf("Failed to delete Zadig service %s, err: %s", svcName, err)
				}
			}
		}

		param := &kube.ResourceApplyParam{
			ProductInfo:         productInfo,
			ServiceName:         name,
			KubeClient:          kclient,
			CurrentResourceYaml: serviceRelatedYaml[name],
			Uninstall:           true,
			WaitForUninstall:    true,
		}
		_, err = kube.CreateOrPatchResource(param, log)
		if err != nil {
			// Only record and do not block subsequent traversals.
			log.Errorf("failed to remove k8s resources when deleting service: %s, err: %s", name, err)
		}
	}

	if productInfo.ShareEnv.Enable && !productInfo.ShareEnv.IsBase {
		err = kube.EnsureGrayEnvConfig(ctx, productInfo, kclient, istioClient)
		if err != nil {
			log.Errorf("Failed to ensure gray env config: %s", err)
			return fmt.Errorf("failed to ensure gray env config: %s", err)
		}
	} else if productInfo.IstioGrayscale.Enable && !productInfo.IstioGrayscale.IsBase {
		err = kube.EnsureFullPathGrayScaleConfig(ctx, productInfo, kclient, istioClient)
		if err != nil {
			log.Errorf("Failed to ensure full path gray scale config: %s", err)
			return fmt.Errorf("Failed to ensure full path gray scale config: %s", err)
		}
	}
	return nil
}

func GetEstimatedRenderCharts(productName, envName string, getSvcRenderArgs []*commonservice.GetSvcRenderArg, production bool, log *zap.SugaredLogger) ([]*commonservice.HelmSvcRenderArg, error) {
	// find renderchart info in env
	renderChartInEnv, _, err := commonservice.GetSvcRenderArgs(productName, envName, getSvcRenderArgs, log)
	if err != nil {
		log.Errorf("find render charts in env fail, env %s err %s", envName, err.Error())
		return nil, e.ErrGetRenderSet.AddDesc("failed to get render charts in env")
	}

	rcMap := make(map[string]*commonservice.HelmSvcRenderArg)
	rcChartMap := make(map[string]*commonservice.HelmSvcRenderArg)
	for _, rc := range renderChartInEnv {
		if rc.IsChartDeploy {
			rcChartMap[rc.ReleaseName] = rc
		} else {
			rcMap[rc.ServiceName] = rc
		}
	}

	serviceOption := &commonrepo.ServiceListOption{
		ProductName: productName,
		Type:        setting.HelmDeployType,
	}

	for _, arg := range getSvcRenderArgs {
		if arg.IsHelmChartDeploy {
			continue
		}

		if _, ok := rcMap[arg.ServiceOrReleaseName]; ok {
			continue
		}
		serviceOption.InServices = append(serviceOption.InServices, &templatemodels.ServiceInfo{
			Name:  arg.ServiceOrReleaseName,
			Owner: productName,
		})
	}

	if len(serviceOption.InServices) > 0 {
		serviceList, err := repository.ListMaxRevisions(serviceOption, production)
		if err != nil {
			log.Errorf("list service fail, productName %s err %s", productName, err.Error())
			return nil, e.ErrGetRenderSet.AddDesc("failed to get service template info")
		}
		for _, singleService := range serviceList {
			rcMap[singleService.ServiceName] = &commonservice.HelmSvcRenderArg{
				EnvName:      envName,
				ServiceName:  singleService.ServiceName,
				ChartVersion: singleService.HelmChart.Version,
			}
		}
	}

	ret := make([]*commonservice.HelmSvcRenderArg, 0, len(rcMap))
	for _, rc := range rcMap {
		ret = append(ret, rc)
	}
	for _, rc := range rcChartMap {
		ret = append(ret, rc)
	}
	return ret, nil
}

func createGroups(user, requestID string, args *commonmodels.Product, eventStart int64, informer informers.SharedInformerFactory, kubeClient client.Client, istioClient versionedclient.Interface, log *zap.SugaredLogger) {
	var err error
	envName := args.EnvName
	defer func() {
		status := setting.ProductStatusSuccess
		errorMsg := ""
		if err != nil {
			status = setting.ProductStatusFailed
			errorMsg = err.Error()

			// 发送创建产品失败消息给用户
			title := fmt.Sprintf("创建 [%s] 的 [%s] 环境失败:%s", args.ProductName, args.EnvName, errorMsg)
			notify.SendErrorMessage(user, title, requestID, err, log)
		}

		commonservice.LogProductStats(envName, setting.CreateProductEvent, args.ProductName, requestID, eventStart, log)

		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, args.ProductName, status, errorMsg); err != nil {
			log.Errorf("[%s][%s] Product.Update set product status error: %v", envName, args.ProductName, err)
			return
		}
	}()

	err = initEnvConfigSetAction(args.EnvName, args.Namespace, args.ProductName, user, args.EnvConfigs, false, kubeClient)
	if err != nil {
		args.Status = setting.ProductStatusFailed
		log.Errorf("initEnvConfigSet error :%s", err)
		return
	}

	args.LintServices()
	for groupIndex, group := range args.Services {
		err = envHandleFunc(getProjectType(args.ProductName), log).createGroup(user, args, group, informer, kubeClient)
		if err != nil {
			args.Status = setting.ProductStatusFailed
			log.Errorf("createGroup error :%+v", err)
			return
		}
		err = helmservice.UpdateServicesGroupInEnv(args.ProductName, args.EnvName, groupIndex, group, args.Production)
		if err != nil {
			log.Errorf("Failed to update helm product %s/%s - service group %d. Error: %v", args.ProductName, args.EnvName, groupIndex, err)
			err = e.ErrUpdateEnv.AddDesc(err.Error())
			return
		}
	}

	if !args.Production && args.ShareEnv.Enable && !args.ShareEnv.IsBase {
		// Note: Currently, only sub-environments can be created, but baseline environments cannot be created.
		err = kube.EnsureGrayEnvConfig(context.TODO(), args, kubeClient, istioClient)
		if err != nil {
			args.Status = setting.ProductStatusFailed
			log.Errorf("Failed to ensure environment sharing in env %s of product %s: %s", args.EnvName, args.ProductName, err)
			return
		}
	} else if args.Production && args.IstioGrayscale.Enable && !args.IstioGrayscale.IsBase {
		err = kube.EnsureFullPathGrayScaleConfig(context.TODO(), args, kubeClient, istioClient)
		if err != nil {
			args.Status = setting.ProductStatusFailed
			log.Errorf("Failed to ensure full path grayscale in env %s of product %s: %s", args.EnvName, args.ProductName, err)
			return
		}
	}

	return
}

func getProjectType(productName string) string {
	projectInfo, _ := templaterepo.NewProductColl().Find(productName)
	projectType := setting.K8SDeployType
	if projectInfo == nil || projectInfo.ProductFeature == nil {
		return projectType
	}

	if projectInfo.ProductFeature.DeployType == setting.HelmDeployType {
		return setting.HelmDeployType
	}

	if projectInfo.ProductFeature.DeployType == setting.K8SDeployType && projectInfo.ProductFeature.BasicFacility == setting.BasicFacilityK8S {
		return projectType
	}

	if projectInfo.ProductFeature.DeployType == setting.K8SDeployType && projectInfo.ProductFeature.BasicFacility == setting.BasicFacilityCVM {
		return setting.PMDeployType
	}
	return projectType
}

func restartRelatedWorkloads(env *commonmodels.Product, service *commonmodels.ProductService,
	kubeClient client.Client, log *zap.SugaredLogger) error {
	parsedYaml, err := kube.RenderEnvService(env, service.GetServiceRender(), service)
	if err != nil {
		return fmt.Errorf("service template %s error: %v", service.ServiceName, err)
	}

	manifests := releaseutil.SplitManifests(parsedYaml)
	resources := make([]*unstructured.Unstructured, 0, len(manifests))
	for _, item := range manifests {
		u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
		if err != nil {
			log.Errorf("Failed to convert yaml to Unstructured, manifest is\n%s\n, error: %v", item, err)
			continue
		}
		resources = append(resources, u)
	}

	for _, u := range resources {
		switch u.GetKind() {
		case setting.Deployment:
			err = updater.RestartDeployment(env.Namespace, u.GetName(), kubeClient)
			return errors.Wrapf(err, "failed to restart deployment %s", u.GetName())
		case setting.StatefulSet:
			err = updater.RestartStatefulSet(env.Namespace, u.GetName(), kubeClient)
			return errors.Wrapf(err, "failed to restart statefulset %s", u.GetName())
		}
	}
	return nil
}

// upsertService
func upsertService(env *commonmodels.Product, newService *commonmodels.ProductService, prevSvc *commonmodels.ProductService, addLabel bool, informer informers.SharedInformerFactory,
	kubeClient client.Client, istioClient versionedclient.Interface, log *zap.SugaredLogger) ([]*unstructured.Unstructured, error) {
	isUpdate := prevSvc == nil
	errList := &multierror.Error{
		ErrorFormat: func(es []error) string {
			format := "更新服务"
			if !isUpdate {
				format = "创建服务"
			}

			if len(es) == 1 {
				return fmt.Sprintf(format+" %s 失败:%v", newService.ServiceName, es[0])
			}

			points := make([]string, len(es))
			for i, err := range es {
				points[i] = fmt.Sprintf("* %v", err)
			}

			return fmt.Sprintf(format+" %s 失败:\n%s", newService.ServiceName, strings.Join(points, "\n"))
		},
	}

	if newService.Type != setting.K8SDeployType {
		return nil, nil
	}

	parsedYaml, err := kube.RenderEnvService(env, newService.GetServiceRender(), newService)
	if err != nil {
		log.Errorf("Failed to render newService %s, error: %v", newService.ServiceName, err)
		errList = multierror.Append(errList, fmt.Errorf("newService template %s error: %v", newService.ServiceName, err))
		return nil, errList
	}

	if prevSvc == nil {
		fakeTemplateSvc := &commonmodels.Service{ServiceName: newService.ServiceName, ProductName: newService.ServiceName, KubeYamls: util.SplitYaml(parsedYaml)}
		commonutil.SetCurrentContainerImages(fakeTemplateSvc)
		newService.Containers = fakeTemplateSvc.Containers
	}

	preResourceYaml := ""
	// compatibility: prevSvc.Render could be null when prev update failed
	if prevSvc != nil {
		preResourceYaml, err = getOldSvcYaml(env, prevSvc, log)
		if err != nil {
			return nil, errors.Wrapf(err, "get old svc yaml failed")
		}
	}

	resourceApplyParam := &kube.ResourceApplyParam{
		ProductInfo:              env,
		ServiceName:              newService.ServiceName,
		CurrentResourceYaml:      preResourceYaml,
		UpdateResourceYaml:       parsedYaml,
		Informer:                 informer,
		KubeClient:               kubeClient,
		IstioClient:              istioClient,
		InjectSecrets:            true,
		Uninstall:                false,
		AddZadigLabel:            addLabel,
		SharedEnvHandler:         EnsureUpdateZadigService,
		IstioGrayscaleEnvHandler: kube.EnsureUpdateGrayscaleService,
	}

	return kube.CreateOrPatchResource(resourceApplyParam, log)
}

func getOldSvcYaml(env *commonmodels.Product,
	oldService *commonmodels.ProductService,
	log *zap.SugaredLogger) (string, error) {

	parsedYaml, err := kube.RenderEnvService(env, oldService.GetServiceRender(), oldService)
	if err != nil {
		log.Errorf("failed to find old service revision %s/%d", oldService.ServiceName, oldService.Revision)
		return "", err
	}
	return parsedYaml, nil
}

func preCreateProduct(envName string, args *commonmodels.Product, kubeClient client.Client,
	log *zap.SugaredLogger) error {
	var (
		productTemplateName = args.ProductName
		err                 error
	)

	var productTmpl *templatemodels.Product
	productTmpl, err = templaterepo.NewProductColl().Find(productTemplateName)
	if err != nil {
		log.Errorf("[%s][P:%s] get product template error: %v", envName, productTemplateName, err)
		return e.ErrCreateEnv.AddDesc(e.FindProductTmplErrMsg)
	}

	var serviceCount int
	for _, group := range args.Services {
		serviceCount = serviceCount + len(group)
	}
	args.Revision = productTmpl.Revision

	opt := &commonrepo.ProductFindOptions{Name: args.ProductName, EnvName: envName}
	if _, err := commonrepo.NewProductColl().Find(opt); err == nil {
		log.Errorf("[%s][P:%s] duplicate product", envName, args.ProductName)
		return e.ErrCreateEnv.AddDesc(e.DuplicateEnvErrMsg)
	}
	opt2 := &commonrepo.SAEEnvFindOptions{ProjectName: args.ProductName, EnvName: envName}
	if _, err := commonrepo.NewSAEEnvColl().Find(opt2); err == nil {
		log.Errorf("[%s][P:%s] duplicate product", envName, args.ProductName)
		return e.ErrCreateEnv.AddDesc(e.DuplicateEnvErrMsg)
	}

	if productTmpl.ProductFeature.DeployType == setting.HelmDeployType || productTmpl.ProductFeature.DeployType == setting.K8SDeployType {
		args.AnalysisConfig = &commonmodels.AnalysisConfig{
			ResourceTypes: []commonmodels.ResourceType{
				commonmodels.ResourceTypePod,
				commonmodels.ResourceTypeDeployment,
				commonmodels.ResourceTypeReplicaSet,
				commonmodels.ResourceTypePVC,
				commonmodels.ResourceTypeService,
				commonmodels.ResourceTypeIngress,
				commonmodels.ResourceTypeStatefulSet,
				commonmodels.ResourceTypeCronJob,
				commonmodels.ResourceTypeHPA,
				commonmodels.ResourceTypePDB,
				commonmodels.ResourceTypeNetworkPolicy,
			},
		}
	}

	if preCreateNSAndSecret(productTmpl.ProductFeature) {
		enableIstioInjection := false
		if args.ShareEnv.Enable || args.IstioGrayscale.Enable {
			enableIstioInjection = true
		}
		return ensureKubeEnv(args.Namespace, args.RegistryID, map[string]string{setting.ProductLabel: args.ProductName}, enableIstioInjection, kubeClient, log)
	}
	return nil
}

func preCreateNSAndSecret(productFeature *templatemodels.ProductFeature) bool {
	if productFeature == nil {
		return true
	}
	if productFeature != nil && productFeature.BasicFacility != setting.BasicFacilityCVM {
		return true
	}
	return false
}

func ensureKubeEnv(namespace, registryId string, customLabels map[string]string, enableIstioInjection bool, kubeClient client.Client, log *zap.SugaredLogger) error {
	err := kube.CreateNamespace(namespace, customLabels, enableIstioInjection, kubeClient)
	if err != nil {
		log.Errorf("[%s] get or create namespace error: %v", namespace, err)
		return e.ErrCreateNamspace.AddDesc(err.Error())
	}

	// 创建默认的镜像仓库secret
	if err := commonservice.EnsureDefaultRegistrySecret(namespace, registryId, kubeClient, log); err != nil {
		log.Errorf("[%s] get or create namespace error: %v", namespace, err)
		return e.ErrCreateSecret.AddDesc(e.CreateDefaultRegistryErrMsg)
	}

	return nil
}

func installProductHelmCharts(user, requestID string, args *commonmodels.Product, _ *commonmodels.RenderSet, eventStart int64, helmClient *helmtool.HelmClient,
	kclient client.Client, istioClient versionedclient.Interface, log *zap.SugaredLogger) {
	var (
		err     error
		errList = &multierror.Error{}
	)
	envName := args.EnvName

	defer func() {
		if err != nil {
			title := fmt.Sprintf("创建 [%s] 的 [%s] 环境失败", args.ProductName, args.EnvName)
			notify.SendErrorMessage(user, title, requestID, err, log)
		}

		commonservice.LogProductStats(envName, setting.CreateProductEvent, args.ProductName, requestID, eventStart, log)

		status := setting.ProductStatusSuccess
		if err = commonrepo.NewProductColl().UpdateStatusAndError(envName, args.ProductName, status, ""); err != nil {
			log.Errorf("[%s][P:%s] Product.UpdateStatusAndError error: %v", envName, args.ProductName, err)
			return
		}
	}()

	err = kube.DeployMultiHelmRelease(args, helmClient, nil, user, log)
	if err != nil {
		log.Errorf("error occurred when installing services in env: %s/%s, err: %s ", args.ProductName, envName, err)
		errList = multierror.Append(errList, err)
	}

	// Note: For the sub env, try to supplement information relevant to the base env.
	if args.ShareEnv.Enable && !args.ShareEnv.IsBase {
		shareEnvErr := kube.EnsureGrayEnvConfig(context.TODO(), args, kclient, istioClient)
		if shareEnvErr != nil {
			errList = multierror.Append(errList, shareEnvErr)
		}
	} else if args.IstioGrayscale.Enable && !args.IstioGrayscale.IsBase {
		err = kube.EnsureFullPathGrayScaleConfig(context.TODO(), args, kclient, istioClient)
		if err != nil {
			args.Status = setting.ProductStatusFailed
			log.Errorf("Failed to ensure full path grayscale in env %s of product %s: %s", args.EnvName, args.ProductName, err)
			return
		}
	}

	err = errList.ErrorOrNil()
}

func getServiceRevisionMap(serviceRevisionList []*SvcRevision) map[string]*SvcRevision {
	serviceRevisionMap := make(map[string]*SvcRevision)
	for _, revision := range serviceRevisionList {
		serviceRevisionMap[revision.ServiceName+revision.Type] = revision
	}
	return serviceRevisionMap
}

func updateHelmProductGroup(username, productName, envName string, productResp *commonmodels.Product,
	overrideCharts []*commonservice.HelmSvcRenderArg, deletedSvcRevision map[string]int64, addedReleaseNameSet sets.String, filter kube.DeploySvcFilter, log *zap.SugaredLogger) error {

	helmClient, err := helmtool.NewClientFromNamespace(productResp.ClusterID, productResp.Namespace)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	// uninstall services
	for serviceName, serviceRevision := range deletedSvcRevision {
		if !commonutil.ServiceDeployed(serviceName, productResp.ServiceDeployStrategy) {
			continue
		}
		if productResp.ServiceDeployStrategy != nil {
			delete(productResp.ServiceDeployStrategy, serviceName)
		}
		if err = kube.UninstallServiceByName(helmClient, serviceName, productResp, serviceRevision, true); err != nil {
			log.Errorf("UninstallRelease err:%v", err)
			return e.ErrUpdateEnv.AddErr(err)
		}
	}

	renderSet, err := diffRenderSet(username, productName, envName, productResp, overrideCharts, log)
	if err != nil {
		return e.ErrUpdateEnv.AddDesc("对比环境中的value.yaml和系统默认的value.yaml失败")
	}

	productResp.ServiceRenders = renderSet.ChartInfos

	if productResp.ServiceDeployStrategy != nil {
		for _, releaseName := range addedReleaseNameSet.List() {
			delete(productResp.ServiceDeployStrategy, commonutil.GetReleaseDeployStrategyKey(releaseName))
		}
		for _, chart := range overrideCharts {
			productResp.ServiceDeployStrategy[chart.ServiceName] = chart.DeployStrategy
		}
	}

	if err = commonrepo.NewProductColl().UpdateDeployStrategy(productResp.EnvName, productResp.ProductName, productResp.ServiceDeployStrategy); err != nil {
		log.Errorf("Failed to update env, err: %s", err)
		return err
	}

	log.Debugf("before DeployMultiHelmRelease")
	for _, svcGroup := range productResp.Services {
		for _, svc := range svcGroup {
			log.Debugf("service: %s, envValuesYaml: %s", svc.ServiceName, svc.GetServiceRender().GetOverrideYaml())
		}
	}

	err = kube.DeployMultiHelmRelease(productResp, helmClient, filter, username, log)
	if err != nil {
		log.Errorf("error occurred when upgrading services in env: %s/%s, err: %s ", productName, envName, err)
		return err
	}

	return nil
}

func updateHelmChartProductGroup(username, productName, envName string, productResp *commonmodels.Product,
	overrideCharts []*commonservice.HelmSvcRenderArg, deletedSvcRevision map[string]int64, dupSvcNameSet sets.String, filter kube.DeploySvcFilter, log *zap.SugaredLogger) error {

	helmClient, err := helmtool.NewClientFromNamespace(productResp.ClusterID, productResp.Namespace)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	// uninstall release
	deletedRelease := []string{}
	for serviceName, _ := range deletedSvcRevision {
		if !commonutil.ReleaseDeployed(serviceName, productResp.ServiceDeployStrategy) {
			continue
		}
		if productResp.ServiceDeployStrategy != nil {
			delete(productResp.ServiceDeployStrategy, commonutil.GetReleaseDeployStrategyKey(serviceName))
		}
		if err = kube.UninstallRelease(helmClient, productResp, serviceName, true); err != nil {
			log.Errorf("UninstallRelease err:%v", err)
			return e.ErrUpdateEnv.AddErr(err)
		}
		deletedRelease = append(deletedRelease, serviceName)
	}

	mergeRenderSetAndRenderChart(productResp, overrideCharts, deletedRelease)

	productResp.ServiceRenders = productResp.GetAllSvcRenders()

	if productResp.ServiceDeployStrategy != nil {
		for _, svcName := range dupSvcNameSet.List() {
			delete(productResp.ServiceDeployStrategy, svcName)
		}
		for _, chart := range overrideCharts {
			productResp.ServiceDeployStrategy[commonutil.GetReleaseDeployStrategyKey(chart.ReleaseName)] = chart.DeployStrategy
		}
	}

	productResp.UpdateBy = username
	if err = commonrepo.NewProductColl().Update(productResp); err != nil {
		log.Errorf("Failed to update env, err: %s", err)
		return err
	}

	err = kube.DeployMultiHelmRelease(productResp, helmClient, filter, username, log)
	if err != nil {
		log.Errorf("error occurred when upgrading services in env: %s/%s, err: %s ", productName, envName, err)
		return err
	}

	return nil
}

// diffRenderSet get diff between renderset in product and product template
// generate a new renderset and insert into db
func diffRenderSet(username, productName, envName string, productResp *commonmodels.Product, overrideCharts []*commonservice.HelmSvcRenderArg, log *zap.SugaredLogger) (*commonmodels.RenderSet, error) {
	// default renderset created directly from the service template
	latestRenderSet, err := render.GetLatestRenderSetFromHelmProject(productName, productResp.Production)
	if err != nil {
		log.Errorf("[RenderSet.find] err: %v", err)
		return nil, err
	}

	// chart infos in template
	latestChartInfoMap := make(map[string]*templatemodels.ServiceRender)
	for _, renderInfo := range latestRenderSet.ChartInfos {
		latestChartInfoMap[renderInfo.ServiceName] = renderInfo
	}

	// chart infos from client
	renderChartArgMap := make(map[string]*commonservice.HelmSvcRenderArg)
	renderChartDeployArgMap := make(map[string]*commonservice.HelmSvcRenderArg)
	for _, singleArg := range overrideCharts {
		if singleArg.EnvName != envName {
			continue
		}
		if singleArg.IsChartDeploy {
			renderChartDeployArgMap[singleArg.ReleaseName] = singleArg
		} else {
			renderChartArgMap[singleArg.ServiceName] = singleArg
		}
	}

	newChartInfos := make([]*templatemodels.ServiceRender, 0)

	for serviceName, latestChartInfo := range latestChartInfoMap {

		if renderChartArgMap[serviceName] == nil {
			continue
		}

		if productResp.GetServiceMap()[serviceName] == nil {
			continue
		}

		productSvc := productResp.GetServiceMap()[serviceName]
		if productSvc != nil {
			renderChartArgMap[serviceName].FillRenderChartModel(productSvc.GetServiceRender(), productSvc.GetServiceRender().ChartVersion, latestChartInfo.ValuesYaml)
			newChartInfos = append(newChartInfos, productSvc.GetServiceRender())
		} else {
			renderChartArgMap[serviceName].FillRenderChartModel(latestChartInfo, latestChartInfo.ChartVersion, latestChartInfo.ValuesYaml)
			newChartInfos = append(newChartInfos, latestChartInfo)
		}
	}

	return &commonmodels.RenderSet{
		ChartInfos: newChartInfos,
	}, nil
}

func mergeRenderSetAndRenderChart(productResp *commonmodels.Product, overrideCharts []*commonservice.HelmSvcRenderArg, deletedReleases []string) {

	requestChartInfoMap := make(map[string]*templatemodels.ServiceRender)
	for _, chartInfo := range overrideCharts {
		requestChartInfoMap[chartInfo.ReleaseName] = &templatemodels.ServiceRender{
			ServiceName:       chartInfo.ServiceName,
			ReleaseName:       chartInfo.ReleaseName,
			IsHelmChartDeploy: true,
			ChartVersion:      chartInfo.ChartVersion,
			ChartRepo:         chartInfo.ChartRepo,
			ChartName:         chartInfo.ChartName,
			OverrideValues:    chartInfo.ToOverrideValueString(),
			OverrideYaml: &templatemodels.CustomYaml{
				YamlContent: chartInfo.OverrideYaml,
			},
		}
	}
	deletedReleasesSet := sets.NewString(deletedReleases...)

	updatedGroups := make([][]*commonmodels.ProductService, 0)
	for _, svcGroup := range productResp.Services {
		updatedGroup := make([]*commonmodels.ProductService, 0)
		for _, svc := range svcGroup {
			if deletedReleasesSet.Has(svc.ReleaseName) {
				continue
			}
			if requestArg, ok := requestChartInfoMap[svc.ReleaseName]; ok && !svc.FromZadig() {
				svc.Render = requestArg
			}
			updatedGroup = append(updatedGroup, svc)
		}
		updatedGroups = append(updatedGroups, updatedGroup)
	}
	productResp.Services = updatedGroups
}

func GetGlobalVariableCandidate(productName, envName string, log *zap.SugaredLogger) ([]*commontypes.ServiceVariableKV, error) {
	templateProduct, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		return nil, fmt.Errorf("failed to find template product %s, err: %w", productName, err)
	}
	globalVariablesDefineMap := map[string]*commontypes.ServiceVariableKV{}
	for _, kv := range templateProduct.GlobalVariables {
		globalVariablesDefineMap[kv.Key] = kv
	}

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    productName,
		EnvName: envName,
	})
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		log.Errorf("failed to query product info, productName %s envName %s err %s", productName, envName, err)
		return nil, fmt.Errorf("failed to query product info, productName %s envName %s", productName, envName)
	}

	for _, kv := range productInfo.GlobalVariables {
		if _, ok := globalVariablesDefineMap[kv.Key]; ok {
			delete(globalVariablesDefineMap, kv.Key)
		}
	}

	ret := []*commontypes.ServiceVariableKV{}
	for _, kv := range globalVariablesDefineMap {
		ret = append(ret, kv)
	}

	return ret, nil
}

func PreviewProductGlobalVariables(productName, envName string, arg []*commontypes.GlobalVariableKV, production bool, log *zap.SugaredLogger) ([]*SvcDiffResult, error) {
	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return nil, err
	}
	return PreviewProductGlobalVariablesWithRender(product, arg, log)
}

func extractRootKeyFromFlat(flatKey string) string {
	splitStrs := strings.Split(flatKey, ".")
	return strings.Split(splitStrs[0], "[")[0]
}

func PreviewHelmProductGlobalVariables(productName, envName, globalVariable string, proudction bool, log *zap.SugaredLogger) ([]*SvcDiffResult, error) {
	ret := make([]*SvcDiffResult, 0)
	variableKvs, err := commontypes.YamlToServiceVariableKV(globalVariable, nil)
	if err != nil {
		return ret, fmt.Errorf("failed to parse global variable, err: %v", err)
	}
	globalKeySet := sets.NewString()
	for _, kv := range variableKvs {
		globalKeySet.Insert(kv.Key)
	}

	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &proudction,
	})
	if err != nil {
		log.Errorf("PreviewHelmProductGlobalVariables GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return nil, err
	}

	equal, err := yamlutil.Equal(product.DefaultValues, globalVariable)
	if err != nil {
		return ret, fmt.Errorf("failed to check if product and args global variable is equal, err: %s", err)
	}
	if equal {
		return ret, nil
	}

	// current default keys
	variableKvs, err = commontypes.YamlToServiceVariableKV(product.DefaultValues, nil)
	if err != nil {
		return ret, fmt.Errorf("failed to parse current global variable, err: %v", err)
	}
	for _, kv := range variableKvs {
		globalKeySet.Insert(kv.Key)
	}

	for _, chartInfo := range product.GetAllSvcRenders() {
		svcRevision := int64(0)
		if chartInfo.DeployedFromZadig() {
			prodSvc, ok := product.GetServiceMap()[chartInfo.ServiceName]
			if !ok {
				continue
			}
			svcRevision = prodSvc.Revision
		} else {
			_, ok := product.GetChartServiceMap()[chartInfo.ReleaseName]
			if !ok {
				continue
			}
		}

		svcPreview := &SvcDiffResult{}
		if chartInfo.DeployedFromZadig() {
			svcPreview.ServiceName = chartInfo.ServiceName
			tmplSvc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
				ProductName: product.ProductName,
				ServiceName: chartInfo.ServiceName,
				Revision:    svcRevision,
			}, product.Production)
			if err != nil {
				return ret, fmt.Errorf("failed to query template service %s, err: %s", chartInfo.ServiceName, err)
			}
			svcPreview.ReleaseName = util.GeneReleaseName(tmplSvc.GetReleaseNaming(), tmplSvc.ProductName, product.Namespace, product.EnvName, tmplSvc.ServiceName)
		} else {
			svcPreview.ReleaseName = chartInfo.ReleaseName
			svcPreview.ChartName = chartInfo.ChartName
			svcPreview.DeployedFromChart = true
		}

		if chartInfo.OverrideYaml == nil && len(chartInfo.OverrideValues) == 0 {
			ret = append(ret, svcPreview)
			continue
		}

		svcRootKeys := sets.NewString()

		svcVariableKvs, err := commontypes.YamlToServiceVariableKV(chartInfo.GetOverrideYaml(), nil)
		if err != nil {
			return ret, fmt.Errorf("failed to gene service varaible kv for service %s, err: %s", chartInfo.ServiceName, err)
		}
		for _, kv := range svcVariableKvs {
			svcRootKeys.Insert(kv.Key)
		}

		if len(chartInfo.OverrideValues) > 0 {
			kvList := make([]*helmtool.KV, 0)
			err = json.Unmarshal([]byte(chartInfo.OverrideValues), &kvList)
			if err != nil {
				return ret, fmt.Errorf("failed to unmarshal override values for service %s, err: %s", chartInfo.ServiceName, err)
			}
			for _, kv := range kvList {
				svcRootKeys.Insert(extractRootKeyFromFlat(kv.Key))
			}
		}

		// service variable contains all global vars means global vars change will not affect this service
		if svcRootKeys.HasAll(globalKeySet.List()...) {
			continue
		}
		ret = append(ret, svcPreview)
	}
	return ret, nil
}

func UpdateProductGlobalVariables(productName, envName, userName, requestID string, currentRevision int64, arg []*commontypes.GlobalVariableKV, production bool, log *zap.SugaredLogger) error {
	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err != nil {
		log.Errorf("UpdateProductGlobalVariables GetProductEnv envName:%s productName: %s error, error msg:%s", envName, productName, err)
		return err
	}
	if product.IsSleeping() {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("environment is sleeping"))
	}

	if product.UpdateTime != currentRevision {
		return e.ErrUpdateEnv.AddDesc("renderset revision is not the latest, please refresh and try again")
	}

	project, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(fmt.Errorf("failed to find project: %s, error: %s", productName, err))
	}

	err = UpdateProductGlobalVariablesWithRender(project, product, nil, userName, requestID, arg, log)
	if err != nil {
		return e.ErrUpdateEnv.AddErr(err)
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(product.ClusterID)
	if err != nil {
		log.Errorf("UpdateHelmProductRenderset GetKubeClient error, error msg:%s", err)
		return err
	}
	return ensureKubeEnv(product.Namespace, product.RegistryID, map[string]string{setting.ProductLabel: product.ProductName}, false, kubeClient, log)
}

func UpdateProductGlobalVariablesWithRender(templateProduct *templatemodels.Product, product *commonmodels.Product, productRenderset *models.RenderSet, userName, requestID string, args []*commontypes.GlobalVariableKV, log *zap.SugaredLogger) error {
	productYaml, err := commontypes.GlobalVariableKVToYaml(product.GlobalVariables)
	if err != nil {
		return fmt.Errorf("failed to convert proudct's global variables to yaml, err: %s", err)
	}
	argsYaml, err := commontypes.GlobalVariableKVToYaml(args)
	if err != nil {
		return fmt.Errorf("failed to convert args' global variables to yaml, err: %s", err)
	}
	equal, err := yamlutil.Equal(productYaml, argsYaml)
	if err != nil {
		return fmt.Errorf("failed to check if product and args global variable is equal, err: %s", err)
	}

	if equal {
		return nil
	}

	argMap := make(map[string]*commontypes.GlobalVariableKV)
	argSet := sets.NewString()
	for _, kv := range args {
		argMap[kv.Key] = kv
		argSet.Insert(kv.Key)
	}
	productVariableMap := make(map[string]*commontypes.GlobalVariableKV)
	productSet := sets.NewString()
	for _, kv := range product.GlobalVariables {
		productVariableMap[kv.Key] = kv
		productSet.Insert(kv.Key)
	}

	projectGlobalVariables := templateProduct.GlobalVariables
	if product.Production {
		projectGlobalVariables = templateProduct.ProductionGlobalVariables
	}
	projectGlobalVariableSet := sets.NewString()
	for _, v := range projectGlobalVariables {
		projectGlobalVariableSet.Insert(v.Key)
	}

	addedGlobalVariableSet := argSet.Difference(productSet)
	for _, v := range addedGlobalVariableSet.List() {
		if !projectGlobalVariableSet.Has(v) {
			return fmt.Errorf("added global variable %s is not in project's global variable list", v)
		}
	}

	deletedVariableSet := productSet.Difference(argSet)
	for _, key := range deletedVariableSet.List() {
		if _, ok := productVariableMap[key]; !ok {
			return fmt.Errorf("UNEXPECT ERROR: global variable %s not found in environment", key)
		}
		if len(productVariableMap[key].RelatedServices) != 0 {
			return fmt.Errorf("global variable %s is used by service %v, can't delete it", key, productVariableMap[key].RelatedServices)
		}
	}

	product.GlobalVariables = args
	updatedSvcList := make([]*templatemodels.ServiceRender, 0)
	for _, argKV := range argMap {
		productKV, ok := productVariableMap[argKV.Key]
		if !ok {
			// new global variable, don't need to update service
			if len(argKV.RelatedServices) != 0 {
				return fmt.Errorf("UNEXPECT ERROR: global variable %s is new, but RelatedServices is not empty", argKV.Key)
			}
			continue
		}

		if productKV.Value == argKV.Value {
			continue
		}

		svcSet := sets.NewString()
		for _, svc := range productKV.RelatedServices {
			svcSet.Insert(svc)
		}

		svcVariableMap := make(map[string]*templatemodels.ServiceRender)
		for _, svc := range product.GetAllSvcRenders() {
			svcVariableMap[svc.ServiceName] = svc
		}

		for _, svc := range svcSet.List() {
			if curVariable, ok := svcVariableMap[svc]; ok {
				curVariable.OverrideYaml.RenderVariableKVs = commontypes.UpdateRenderVariable(args, curVariable.OverrideYaml.RenderVariableKVs)
				curVariable.OverrideYaml.YamlContent, err = commontypes.RenderVariableKVToYaml(curVariable.OverrideYaml.RenderVariableKVs, true)
				if err != nil {
					return fmt.Errorf("failed to convert service %s's render variables to yaml, err: %s", svc, err)
				}

				updatedSvcList = append(updatedSvcList, curVariable)
			} else {
				log.Errorf("UNEXPECT ERROR: service %s not found in environment", svc)
			}
		}
	}

	product.ServiceRenders = updatedSvcList

	if product.ServiceDeployStrategy == nil {
		product.ServiceDeployStrategy = make(map[string]string)
	}
	needUpdateStrategy := false
	for _, rc := range updatedSvcList {
		if !commonutil.ChartDeployed(rc, product.ServiceDeployStrategy) {
			needUpdateStrategy = true
			commonutil.SetChartDeployed(rc, product.ServiceDeployStrategy)
		}
	}
	if needUpdateStrategy {
		err = commonrepo.NewProductColl().UpdateDeployStrategy(product.EnvName, product.ProductName, product.ServiceDeployStrategy)
		if err != nil {
			log.Errorf("[%s][P:%s] failed to update product deploy strategy: %s", product.EnvName, product.ProductName, err)
			return e.ErrUpdateEnv.AddErr(err)
		}
	}

	// only update renderset value to db, no need to upgrade chart release
	if len(updatedSvcList) == 0 {
		log.Infof("no need to update svc")
		return commonrepo.NewProductColl().UpdateProductVariables(product)
	}

	return updateK8sProductVariable(product, userName, requestID, log)
}

type EnvConfigsArgs struct {
	AnalysisConfig      *models.AnalysisConfig       `json:"analysis_config"`
	NotificationConfigs []*models.NotificationConfig `json:"notification_configs"`
}

func GetEnvConfigs(projectName, envName string, production *bool, logger *zap.SugaredLogger) (*EnvConfigsArgs, error) {
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       projectName,
		Production: production,
	}
	env, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return nil, e.ErrGetEnvConfigs.AddErr(fmt.Errorf("failed to get environment %s/%s, err: %w", projectName, envName, err))
	}

	analysisConfig := &models.AnalysisConfig{}
	if env.AnalysisConfig != nil {
		analysisConfig = env.AnalysisConfig
	}
	notificationConfigs := []*models.NotificationConfig{}
	if env.NotificationConfigs != nil {
		notificationConfigs = env.NotificationConfigs
	}

	configs := &EnvConfigsArgs{
		AnalysisConfig:      analysisConfig,
		NotificationConfigs: notificationConfigs,
	}
	return configs, nil
}

func UpdateEnvConfigs(projectName, envName string, arg *EnvConfigsArgs, production *bool, logger *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       projectName,
		Production: production,
	}
	_, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return e.ErrUpdateEnvConfigs.AddErr(fmt.Errorf("failed to get environment %s/%s, err: %w", projectName, envName, err))
	}

	_, analyzerMap := analysis.GetAnalyzerMap()
	for _, resourceType := range arg.AnalysisConfig.ResourceTypes {
		if _, ok := analyzerMap[string(resourceType)]; !ok {
			return e.ErrUpdateEnvConfigs.AddErr(fmt.Errorf("invalid analyzer %s", resourceType))
		}
	}

	err = commonrepo.NewProductColl().UpdateConfigs(envName, projectName, arg.AnalysisConfig, arg.NotificationConfigs)
	if err != nil {
		return e.ErrUpdateEnvConfigs.AddErr(fmt.Errorf("failed to update environment %s/%s, err: %w", projectName, envName, err))
	}

	return nil
}

func GetProductionEnvConfigs(projectName, envName string, logger *zap.SugaredLogger) (*EnvConfigsArgs, error) {
	return GetEnvConfigs(projectName, envName, boolptr.True(), logger)
}

func UpdateProductionEnvConfigs(projectName, envName string, arg *EnvConfigsArgs, logger *zap.SugaredLogger) error {
	return UpdateEnvConfigs(projectName, envName, arg, boolptr.True(), logger)
}

type EnvAnalysisRespone struct {
	Result string `json:"result"`
}

func EnvAnalysis(projectName, envName string, production *bool, triggerName string, userName string, logger *zap.SugaredLogger) (*EnvAnalysisRespone, error) {
	var err error
	start := time.Now()
	// get project detail
	project, err := templaterepo.NewProductColl().Find(projectName)
	if err != nil {
		return nil, err
	}
	result := &ai.EnvAIAnalysis{
		ProjectName: projectName,
		DeployType:  project.ProductFeature.DeployType,
		EnvName:     envName,
		TriggerName: triggerName,
		CreatedBy:   userName,
		Production:  *production,
		StartTime:   start.Unix(),
	}
	defer func() {
		if err != nil {
			result.Err = err.Error()
			result.Status = setting.AIEnvAnalysisStatusFailed
		} else {
			result.Status = setting.AIEnvAnalysisStatusSuccess
		}
		result.EndTime = time.Now().Unix()
		err = airepo.NewEnvAIAnalysisColl().Create(result)
		if err != nil {
			logger.Errorf("failed to add env ai analysis result to db, err: %s", err)
		}
	}()

	resp := &EnvAnalysisRespone{}
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       projectName,
		Production: production,
	}
	env, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return resp, e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to get environment %s/%s, err: %w", projectName, envName, err))
	}

	filters := []string{}
	if env.AnalysisConfig != nil {
		if len(env.AnalysisConfig.ResourceTypes) == 0 {
			return resp, nil
		} else {
			for _, resourceType := range env.AnalysisConfig.ResourceTypes {
				filters = append(filters, string(resourceType))
			}
		}
	}

	ctx := context.TODO()
	llmClient, err := commonservice.GetDefaultLLMClient(ctx)
	if err != nil {
		return resp, e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to get llm client, err: %w", err))
	}

	analysiser, err := analysis.NewAnalysis(
		ctx, env.ClusterID,
		llmClient,
		filters, env.Namespace,
		false, // noCache bool
		true,  // explain bool
		10,    // maxConcurrency int
		false, // withDoc bool
	)
	if err != nil {
		return resp, e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to create analysiser, err: %w", err))
	}

	analysiser.RunAnalysis(filters)
	err = analysiser.GetAIResults(false)
	if err != nil {
		return resp, e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to get analysis result, err: %w", err))
	}

	analysisResult, err := analysiser.PrintOutput("text")
	if err != nil {
		return resp, e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to print analysis result, err: %w", err))
	}

	if triggerName == setting.CronTaskCreator {
		util.Go(func() {
			err := EnvAnalysisNotification(projectName, envName, string(analysisResult), env.NotificationConfigs)
			if err != nil {
				log.Errorf("failed to send notification, err: %w", err)
			} else {
				log.Infof("send env analysis notification successfully")
			}
		})
	}
	result.Result = string(analysisResult)

	resp.Result = string(analysisResult)
	return resp, nil
}

type EnvAnalysisCronArg struct {
	Enable bool   `json:"enable"`
	Cron   string `json:"cron"`
}

func UpsertEnvAnalysisCron(projectName, envName string, production *bool, req *EnvAnalysisCronArg, logger *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       projectName,
		Production: production,
	}
	env, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to get environment %s/%s, err: %w", projectName, envName, err))
	}

	found := false
	name := getEnvAnalysisCronName(projectName, envName)
	cron, err := commonrepo.NewCronjobColl().GetByName(name, setting.EnvAnalysisCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return e.ErrAnalysisEnvResource.AddErr(fmt.Errorf("failed to get cron job %s, err: %w", name, err))
		}
	} else {
		found = true
	}

	var payload *commonservice.CronjobPayload
	if found {
		origEnabled := cron.Enabled
		cron.Enabled = req.Enable
		cron.Cron = req.Cron
		err = commonrepo.NewCronjobColl().Upsert(cron)
		if err != nil {
			fmtErr := fmt.Errorf("Failed to upsert cron job, error: %w", err)
			log.Error(fmtErr)
			return err
		}

		if origEnabled && !req.Enable {
			// need to disable cronjob
			payload = &commonservice.CronjobPayload{
				Name:       name,
				JobType:    setting.EnvAnalysisCronjob,
				Action:     setting.TypeEnableCronjob,
				DeleteList: []string{cron.ID.Hex()},
			}
		} else if !origEnabled && req.Enable || origEnabled && req.Enable {
			payload = &commonservice.CronjobPayload{
				Name:    name,
				JobType: setting.EnvAnalysisCronjob,
				Action:  setting.TypeEnableCronjob,
				JobList: []*commonmodels.Schedule{cronJobToSchedule(cron)},
			}
		} else {
			// !origEnabled && !req.Enable
			return nil
		}
	} else {
		input := &commonmodels.Cronjob{
			Name:    name,
			Enabled: req.Enable,
			Type:    setting.EnvAnalysisCronjob,
			Cron:    req.Cron,
			EnvAnalysisArgs: &commonmodels.EnvArgs{
				ProductName: env.ProductName,
				EnvName:     env.EnvName,
				Production:  env.Production,
			},
		}

		err = commonrepo.NewCronjobColl().Upsert(input)
		if err != nil {
			fmtErr := fmt.Errorf("Failed to upsert cron job, error: %w", err)
			log.Error(fmtErr)
			return err
		}
		if !input.Enabled {
			return nil
		}

		payload = &commonservice.CronjobPayload{
			Name:    name,
			JobType: setting.EnvAnalysisCronjob,
			Action:  setting.TypeEnableCronjob,
			JobList: []*commonmodels.Schedule{cronJobToSchedule(input)},
		}
	}

	pl, _ := json.Marshal(payload)
	err = commonrepo.NewMsgQueueCommonColl().Create(&msg_queue.MsgQueueCommon{
		Payload:   string(pl),
		QueueType: setting.TopicCronjob,
	})
	if err != nil {
		log.Errorf("Failed to publish to nsq topic: %s, the error is: %v", setting.TopicCronjob, err)
		return e.ErrUpsertCronjob.AddDesc(err.Error())
	}

	return nil
}

func getEnvAnalysisCronName(projectName, envName string) string {
	return fmt.Sprintf("%s-%s-%s", envName, projectName, setting.EnvAnalysisCronjob)
}

func cronJobToSchedule(input *commonmodels.Cronjob) *commonmodels.Schedule {
	return &commonmodels.Schedule{
		ID:              input.ID,
		Number:          input.Number,
		UnixStamp:       input.UnixStamp,
		Frequency:       input.Frequency,
		Time:            input.Time,
		MaxFailures:     input.MaxFailure,
		EnvAnalysisArgs: input.EnvAnalysisArgs,
		EnvArgs:         input.EnvArgs,
		Type:            config.ScheduleType(input.JobType),
		Cron:            input.Cron,
		Enabled:         input.Enabled,
	}
}

func GetEnvAnalysisCron(projectName, envName string, production *bool, logger *zap.SugaredLogger) (*EnvAnalysisCronArg, error) {
	name := getEnvAnalysisCronName(projectName, envName)
	crons, err := commonrepo.NewCronjobColl().List(&commonrepo.ListCronjobParam{
		ParentName: name,
		ParentType: setting.EnvAnalysisCronjob,
	})
	if err != nil {
		fmtErr := fmt.Errorf("Failed to list env analysis cron jobs, project name %s, env name: %s, error: %w", projectName, envName, err)
		logger.Error(fmtErr)
		return nil, e.ErrGetCronjob.AddErr(fmtErr)
	}
	if len(crons) == 0 {
		return &EnvAnalysisCronArg{}, nil
	}

	resp := &EnvAnalysisCronArg{
		Enable: crons[0].Enabled,
		Cron:   crons[0].Cron,
	}
	return resp, nil
}

// GetEnvAnalysisHistory get env AI analysis history
func GetEnvAnalysisHistory(projectName string, production bool, envName string, pageNum, pageSize int, log *zap.SugaredLogger) ([]*ai.EnvAIAnalysis, int64, error) {
	result, count, err := airepo.NewEnvAIAnalysisColl().ListByOptions(airepo.EnvAIAnalysisListOption{
		EnvName:     envName,
		ProjectName: projectName,
		Production:  production,
		PageNum:     int64(pageNum),
		PageSize:    int64(pageSize),
	})
	if err != nil {
		log.Errorf("Failed to list env ai analysis, project name: %s, env name: %s, error: %v", projectName, envName, err)
		return nil, 0, err
	}
	return result, count, nil
}

func EnvAnalysisNotification(projectName, envName, result string, configs []*commonmodels.NotificationConfig) error {
	for _, config := range configs {
		eventSet := sets.NewString()
		for _, event := range config.Events {
			eventSet.Insert(string(event))
		}

		status := commonmodels.NotificationEventAnalyzerNoraml
		if result != "" {
			status = commonmodels.NotificationEventAnalyzerAbnormal
		}
		if !eventSet.Has(string(status)) {
			return nil
		}

		title, content, larkCard, err := getNotificationContent(projectName, envName, result, imnotify.IMNotifyType(config.WebHookType))
		if err != nil {
			return fmt.Errorf("failed to get notification content, err: %w", err)
		}

		imnotifyClient := imnotify.NewIMNotifyClient()

		switch imnotify.IMNotifyType(config.WebHookType) {
		case imnotify.IMNotifyTypeDingDing:
			if err := imnotifyClient.SendDingDingMessage(config.WebHookURL, title, content, nil, false); err != nil {
				return err
			}
		case imnotify.IMNotifyTypeLark:
			if err := imnotifyClient.SendFeishuMessage(config.WebHookURL, larkCard); err != nil {
				return err
			}
		case imnotify.IMNotifyTypeWeChat:
			if err := imnotifyClient.SendWeChatWorkMessage(imnotify.WeChatTextTypeMarkdown, config.WebHookURL, content); err != nil {
				return err
			}
		}
	}

	return nil
}

type envAnalysisNotification struct {
	BaseURI     string                   `json:"base_uri"`
	WebHookType imnotify.IMNotifyType    `json:"web_hook_type"`
	Time        int64                    `json:"time"`
	ProjectName string                   `json:"project_name"`
	EnvName     string                   `json:"env_name"`
	Status      envAnalysisNotifiyStatus `json:"status"`
	Result      string                   `json:"result"`
}

type envAnalysisNotifiyStatus string

const (
	envAnalysisNotifiyStatusNormal   envAnalysisNotifiyStatus = "normal"
	envAnalysisNotifiyStatusAbnormal envAnalysisNotifiyStatus = "abnormal"
)

func getNotificationContent(projectName, envName, result string, webHookType imnotify.IMNotifyType) (string, string, *imnotify.LarkCard, error) {
	tplTitle := "{{if ne .WebHookType \"feishu\"}}### {{end}}{{getIcon .Status }}{{if eq .WebHookType \"wechat\"}}<font color=\"{{ getColor .Status }}\">{{.ProjectName}}/{{.EnvName}} 环境巡检{{ getStatus .Status }}</font>{{else}} {{.ProjectName}} / {{.EnvName}} 环境巡检{{ getStatus .Status }}{{end}} \n"
	tplContent := []string{"{{if eq .WebHookType \"dingding\"}}##### {{end}}**巡检时间：{{getTime}}** \n",
		"{{.Result}} \n",
	}

	buttonContent := "点击查看更多信息"
	envDetailURL := "{{.BaseURI}}/v1/projects/detail/{{.ProjectName}}/envs/detail?envName={{.EnvName}}"
	moreInformation := fmt.Sprintf("\n\n{{if eq .WebHookType \"dingding\"}}---\n\n{{end}}[%s](%s)", buttonContent, envDetailURL)

	status := envAnalysisNotifiyStatusAbnormal
	if strings.Contains(result, analysis.NormalResultOutput) {
		status = envAnalysisNotifiyStatusNormal
	}

	envAnalysisNotifyArg := &envAnalysisNotification{
		BaseURI:     configbase.SystemAddress(),
		WebHookType: webHookType,
		Time:        time.Now().Unix(),
		ProjectName: projectName,
		EnvName:     envName,
		Status:      status,
		Result:      result,
	}

	title, err := getEnvAnalysisTplExec(tplTitle, envAnalysisNotifyArg)
	if err != nil {
		return "", "", nil, err
	}

	if webHookType != imnotify.IMNotifyTypeLark {
		tplContent := strings.Join(tplContent, "")
		tplContent = fmt.Sprintf("%s%s%s", title, tplContent, moreInformation)
		content, err := getEnvAnalysisTplExec(tplContent, envAnalysisNotifyArg)
		if err != nil {
			return "", "", nil, err
		}
		return title, content, nil, nil
	} else {
		lc := imnotify.NewLarkCard()
		lc.SetConfig(true)
		lc.SetHeader(imnotify.GetColorTemplateWithStatus(config.Status(envAnalysisNotifyArg.Status)), title, "plain_text")
		for idx, feildContent := range tplContent {
			feildExecContent, _ := getEnvAnalysisTplExec(feildContent, envAnalysisNotifyArg)
			lc.AddI18NElementsZhcnFeild(feildExecContent, idx == 0)
		}
		envDetailURL, _ = getEnvAnalysisTplExec(envDetailURL, envAnalysisNotifyArg)
		lc.AddI18NElementsZhcnAction(buttonContent, envDetailURL)
		return "", "", lc, nil
	}
}

func getEnvAnalysisTplExec(tplcontent string, args *envAnalysisNotification) (string, error) {
	tmpl := template.Must(template.New("notify").Funcs(template.FuncMap{
		"getColor": func(status envAnalysisNotifiyStatus) string {
			if status == envAnalysisNotifiyStatusNormal {
				return "info"
			} else if status == envAnalysisNotifiyStatusAbnormal {
				return "warning"
			}
			return "warning"
		},
		"getStatus": func(status envAnalysisNotifiyStatus) string {
			if status == envAnalysisNotifiyStatusNormal {
				return "正常"
			} else if status == envAnalysisNotifiyStatusAbnormal {
				return "异常"
			}
			return "异常"
		},
		"getIcon": func(status envAnalysisNotifiyStatus) string {
			if status == envAnalysisNotifiyStatusNormal {
				return "👍"
			}
			return "⚠️"
		},
		"getTime": func() string {
			return time.Now().Format("2006-01-02 15:04:05")
		},
	}).Parse(tplcontent))

	buffer := bytes.NewBufferString("")
	if err := tmpl.Execute(buffer, args); err != nil {
		log.Errorf("getTplExec Execute err:%s", err)
		return "", fmt.Errorf("getTplExec Execute err:%s", err)

	}
	return buffer.String(), nil
}

func PreviewProductGlobalVariablesWithRender(product *commonmodels.Product, args []*commontypes.GlobalVariableKV, log *zap.SugaredLogger) ([]*SvcDiffResult, error) {
	var err error
	argMap := make(map[string]*commontypes.GlobalVariableKV)
	argSet := sets.NewString()
	for _, kv := range args {
		argMap[kv.Key] = kv
		argSet.Insert(kv.Key)
	}
	productMap := make(map[string]*commontypes.GlobalVariableKV)
	productSet := sets.NewString()
	for _, kv := range product.GlobalVariables {
		productMap[kv.Key] = kv
		productSet.Insert(kv.Key)
	}

	deletedVariableSet := productSet.Difference(argSet)
	for _, key := range deletedVariableSet.List() {
		if _, ok := productMap[key]; !ok {
			return nil, fmt.Errorf("UNEXPECT ERROR: global variable %s not found in environment", key)
		}
		if len(productMap[key].RelatedServices) != 0 {
			return nil, fmt.Errorf("global variable %s is used by service %v, can't delete it", key, productMap[key].RelatedServices)
		}
	}

	product.GlobalVariables = args
	serviceRenderMap := make(map[string]*templatemodels.ServiceRender)
	for _, argKV := range argMap {
		productKV, ok := productMap[argKV.Key]
		if !ok {
			// new global variable, don't need to update service
			if len(argKV.RelatedServices) != 0 {
				return nil, fmt.Errorf("UNEXPECT ERROR: global variable %s is new, but RelatedServices is not empty", argKV.Key)
			}
			continue
		}

		if productKV.Value == argKV.Value {
			continue
		}

		svcSet := sets.NewString()
		for _, svc := range productKV.RelatedServices {
			svcSet.Insert(svc)
		}

		svcVariableMap := make(map[string]*templatemodels.ServiceRender)
		for _, svc := range product.GetServiceMap() {
			svcVariableMap[svc.ServiceName] = svc.GetServiceRender()
		}

		for _, svc := range svcSet.List() {
			if curVariable, ok := svcVariableMap[svc]; ok {
				curVariable.OverrideYaml.RenderVariableKVs = commontypes.UpdateRenderVariable(args, curVariable.OverrideYaml.RenderVariableKVs)
				curVariable.OverrideYaml.YamlContent, err = commontypes.RenderVariableKVToYaml(curVariable.OverrideYaml.RenderVariableKVs, true)
				if err != nil {
					return nil, fmt.Errorf("failed to convert service %s's render variables to yaml, err: %s", svc, err)
				}
				serviceRenderMap[svc] = curVariable
			} else {
				log.Errorf("UNEXPECT ERROR: service %s not found in environment", svc)
			}
		}
	}

	retList := make([]*SvcDiffResult, 0)

	for _, svcRender := range serviceRenderMap {
		curYaml, _, err := kube.FetchCurrentAppliedYaml(&kube.GeneSvcYamlOption{
			ProductName:           product.ProductName,
			EnvName:               product.EnvName,
			ServiceName:           svcRender.ServiceName,
			UpdateServiceRevision: false,
		})
		ret := &SvcDiffResult{
			ServiceName: svcRender.ServiceName,
		}
		if err != nil {
			curYaml = ""
			ret.Error = fmt.Sprintf("failed to fetch current applied yaml, productName: %s envName: %s serviceName: %s, updateSvcRevision: %v, err: %s",
				product.ProductName, product.EnvName, svcRender.ServiceName, false, err)
			log.Errorf(ret.Error)
		}

		prodSvc := product.GetServiceMap()[svcRender.ServiceName]
		if prodSvc == nil {
			ret.Error = fmt.Sprintf("service: %s not found in product", svcRender.ServiceName)
			retList = append(retList, ret)
			continue
		}

		ret.Latest.Yaml, err = kube.RenderEnvService(product, serviceRenderMap[svcRender.ServiceName], prodSvc)
		if err != nil {
			retList = append(retList, ret)
			continue
		}

		ret.Current.Yaml = curYaml
		retList = append(retList, ret)
	}

	return retList, nil
}

func EnsureProductionNamespace(createArgs []*CreateSingleProductArg) error {
	for _, arg := range createArgs {
		// 1. check specified namespace
		filterK8sNamespaces := sets.NewString("kube-node-lease", "kube-public", "kube-system")
		if filterK8sNamespaces.Has(arg.Namespace) {
			return fmt.Errorf("namespace %s is invalid, production environment namespace cannot be set to these three namespaces: kube-node-lease, kube-public, kube-system", arg.Namespace)
		}
	}
	return nil
}

func EnvSleep(productName, envName string, isEnable, isProduction bool, log *zap.SugaredLogger) error {
	tempProd, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		err = fmt.Errorf("failed to find template product %s, err: %s", productName, err)
		log.Error(err)
		return e.ErrEnvSleep.AddErr(err)
	}

	opt := &commonrepo.ProductFindOptions{Name: productName, EnvName: envName}
	prod, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		err = fmt.Errorf("failed to find product %s/%s, err: %s", productName, envName, err)
		log.Error(err)
		return e.ErrEnvSleep.AddErr(err)
	}
	if prod.Production != isProduction {
		err = fmt.Errorf("Insufficient permissions: %s/%s, is production %v", productName, envName, prod.Production)
		log.Error(err)
		return e.ErrEnvSleep.AddErr(err)
	}
	if prod.Status == setting.ProductStatusSleeping && isEnable {
		err = fmt.Errorf("product %s/%s is already sleeping", productName, envName)
		log.Warn(err)
		return e.ErrEnvSleep.AddErr(err)
	}
	if prod.Status != setting.ProductStatusSleeping && !isEnable {
		err = fmt.Errorf("product %s/%s is already running", productName, envName)
		log.Warn(err)
		return e.ErrEnvSleep.AddErr(err)
	}

	templateProduct, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		err = fmt.Errorf("failed to get template product %s, err: %w", productName, err)
		log.Error(err)
		return e.ErrAnalysisEnvResource.AddErr(err)
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(prod.ClusterID)
	if err != nil {
		err = fmt.Errorf("failed to get kube client, err: %s", err)
		log.Error(err)
		return e.ErrEnvSleep.AddErr(err)
	}
	clientset, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(prod.ClusterID)
	if err != nil {
		wrapErr := fmt.Errorf("Failed to create kubernetes clientset for cluster id: %s, the error is: %s", prod.ClusterID, err)
		log.Error(wrapErr)
		return e.ErrEnvSleep.AddErr(wrapErr)
	}
	informer, err := clientmanager.NewKubeClientManager().GetInformer(prod.ClusterID, prod.Namespace)
	if err != nil {
		wrapErr := fmt.Errorf("[%s][%s] error: %v", envName, prod.Namespace, err)
		log.Error(wrapErr)
		return e.ErrEnvSleep.AddErr(wrapErr)
	}
	version, err := clientset.Discovery().ServerVersion()
	if err != nil {
		wrapErr := fmt.Errorf("Failed to get server version info for cluster: %s, the error is: %s", prod.ClusterID, err)
		log.Error(wrapErr)
		return e.ErrEnvSleep.AddErr(wrapErr)
	}

	oldScaleNumMap := make(map[string]int)
	newScaleNumMap := make(map[string]int)
	prod.Status = setting.ProductStatusSleeping
	if !isEnable {
		oldScaleNumMap = prod.PreSleepStatus
		prod.Status = setting.ProductStatusSuccess
	}

	filterArray, err := commonservice.BuildWorkloadFilterFunc(prod, tempProd, "", log)
	if err != nil {
		err = fmt.Errorf("failed to build workload filter func, err: %s", err)
		log.Error(err)
		return e.ErrEnvSleep.AddErr(err)
	}

	count, workLoads, err := commonservice.ListWorkloads(envName, productName, 999, 1, informer, version, log, filterArray...)
	if err != nil {
		wrapErr := fmt.Errorf("failed to list workloads, [%s][%s], error: %v", prod.Namespace, envName, err)
		log.Error(wrapErr)
		return e.ErrEnvSleep.AddErr(wrapErr)
	}
	if count > 999 {
		log.Errorf("project %s env %s: workloads count > 999", productName, envName)
	}

	scaleMap := make(map[string]*commonservice.Workload)
	cronjobMap := make(map[string]*commonservice.Workload)
	for _, workLoad := range workLoads {
		if workLoad.Type == setting.CronJob {
			cronjobMap[workLoad.Name] = workLoad
		} else {
			scaleMap[workLoad.Name] = workLoad
		}
	}

	if templateProduct.IsK8sYamlProduct() || templateProduct.IsHostProduct() {
		prodSvcMap := prod.GetServiceMap()
		svcs, err := commonutil.GetProductUsedTemplateSvcs(prod)
		if err != nil {
			wrapErr := fmt.Errorf("failed to get product used template services, err: %s", err)
			log.Error(wrapErr)
			return e.ErrEnvSleep.AddErr(wrapErr)
		}

		for _, svc := range svcs {
			prodSvc := prodSvcMap[svc.ServiceName]
			if prodSvc == nil {
				wrapErr := fmt.Errorf("service %s not found in product %s(%s)", svc.ServiceName, prod.ProductName, envName)
				log.Error(wrapErr)
				return e.ErrEnvSleep.AddErr(wrapErr)
			}

			parsedYaml, err := kube.RenderEnvServiceWithTempl(prod, prodSvc.GetServiceRender(), prodSvc, svc)
			if err != nil {
				return e.ErrEnvSleep.AddErr(fmt.Errorf("failed to render service %s, err: %s", svc.ServiceName, err))
			}

			manifests := releaseutil.SplitManifests(parsedYaml)
			for _, item := range manifests {
				u, err := serializer.NewDecoder().YamlToUnstructured([]byte(item))
				if err != nil {
					log.Warnf("Failed to decode yaml to Unstructured, err: %s", err)
					continue
				}

				switch u.GetKind() {
				case setting.Deployment, setting.StatefulSet:
					if workLoad, ok := scaleMap[u.GetName()]; ok {
						workLoad.ServiceName = svc.ServiceName
						workLoad.DeployedFromZadig = true
						newScaleNumMap[workLoad.Name] = int(workLoad.Replicas)
					}
				case setting.CronJob:
					if workLoad, ok := cronjobMap[u.GetName()]; ok {
						workLoad.ServiceName = svc.ServiceName
						workLoad.DeployedFromZadig = true
						newScaleNumMap[workLoad.Name] = int(workLoad.Replicas)
					}
				}
			}
		}
	} else if templateProduct.IsHelmProduct() {
		svcToReleaseNameMap, err := commonutil.GetServiceNameToReleaseNameMap(prod)
		if err != nil {
			err = fmt.Errorf("failed to build release-service map: %s", err)
			log.Error(err)
			return e.ErrEnvSleep.AddErr(err)
		}
		for _, svcGroup := range prod.Services {
			for _, svc := range svcGroup {
				releaseName := svcToReleaseNameMap[svc.ServiceName]
				if !svc.FromZadig() {
					releaseName = svc.ReleaseName
				}
				for _, workload := range workLoads {
					if workload.ReleaseName == releaseName {
						if workload.Type != setting.CronJob {
							newScaleNumMap[workload.Name] = int(workload.Replicas)
						}
					}
					workload.DeployedFromZadig = true
				}
			}
		}
	}

	// set boot order when resume from sleep
	if templateProduct.IsK8sYamlProduct() && !isEnable {
		bootOrderMap := make(map[string]int)
		i := 0

		for _, svcGroup := range prod.Services {
			for _, svc := range svcGroup {
				bootOrderMap[svc.ServiceName] = i
				i++
			}
		}

		sort.Slice(workLoads, func(i, j int) bool {
			order1 := 999
			order2 := 999

			svcName := workLoads[i].ServiceName
			order, ok := bootOrderMap[svcName]
			if ok {
				order1 = order
			}

			svcName = workLoads[j].ServiceName
			order, ok = bootOrderMap[svcName]
			if ok {
				order2 = order
			}
			return order1 <= order2
		})
	}

	for _, workload := range workLoads {
		if !workload.DeployedFromZadig {
			continue
		}

		scaleNum := 0
		if num, ok := oldScaleNumMap[workload.Name]; ok {
			// restore previous scale num
			scaleNum = num
		}

		switch workload.Type {
		case setting.Deployment:
			log.Infof("scale workload %s(%s) to %d", workload.Name, workload.Type, scaleNum)
			err := updater.ScaleDeployment(prod.Namespace, workload.Name, scaleNum, kubeClient)
			if err != nil {
				log.Errorf("failed to scale %s/deploy/%s to %d", prod.Namespace, workload.Name, scaleNum)
			}
		case setting.StatefulSet:
			log.Infof("scale workload %s(%s) to %d", workload.Name, workload.Type, scaleNum)
			err := updater.ScaleStatefulSet(prod.Namespace, workload.Name, scaleNum, kubeClient)
			if err != nil {
				log.Errorf("failed to scale %s/sts/%s to %d", prod.Namespace, workload.Name, scaleNum)
			}
		case setting.CronJob:
			if isEnable {
				log.Infof("suspend cronjob %s", workload.Name)
				err := updater.SuspendCronJob(prod.Namespace, workload.Name, kubeClient, kubeclient.VersionLessThan121(version))
				if err != nil {
					log.Errorf("failed to suspend %s/cronjob/%s", prod.Namespace, workload.Name)
				}
			} else {
				log.Infof("resume cronjob %s", workload.Name)
				err := updater.ResumeCronJob(prod.Namespace, workload.Name, kubeClient, kubeclient.VersionLessThan121(version))
				if err != nil {
					log.Errorf("failed to resume %s/cronjob/%s", prod.Namespace, workload.Name)
				}
			}
		}
	}

	prod.PreSleepStatus = newScaleNumMap
	err = commonrepo.NewProductColl().Update(prod)
	if err != nil {
		wrapErr := fmt.Errorf("failed to update product, err: %w", err)
		log.Error(wrapErr)
		return e.ErrEnvSleep.AddErr(wrapErr)
	}

	return nil
}

func GetEnvSleepCron(projectName, envName string, production *bool, logger *zap.SugaredLogger) (*EnvSleepCronArg, error) {
	resp := &EnvSleepCronArg{}

	sleepName := util.GetEnvSleepCronName(projectName, envName, true)
	awakeName := util.GetEnvSleepCronName(projectName, envName, false)
	sleepCron, err := commonrepo.NewCronjobColl().GetByName(sleepName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return nil, e.ErrGetCronjob.AddErr(fmt.Errorf("failed to get env sleep cron job for sleep, err: %w", err))
		}
	}
	awakeCron, err := commonrepo.NewCronjobColl().GetByName(awakeName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return nil, e.ErrGetCronjob.AddErr(fmt.Errorf("failed to get env sleep cron job for awake, err: %w", err))
		}
	}

	if sleepCron != nil {
		resp.SleepCronEnable = sleepCron.Enabled
		resp.SleepCron = sleepCron.Cron
	}
	if awakeCron != nil {
		resp.AwakeCronEnable = awakeCron.Enabled
		resp.AwakeCron = awakeCron.Cron
	}

	return resp, nil
}

type EnvSleepCronArg struct {
	SleepCronEnable bool   `json:"sleep_cron_enable"`
	SleepCron       string `json:"sleep_cron"`
	AwakeCronEnable bool   `json:"awake_cron_enable"`
	AwakeCron       string `json:"awake_cron"`
}

func UpsertEnvSleepCron(projectName, envName string, production *bool, req *EnvSleepCronArg, logger *zap.SugaredLogger) error {
	opt := &commonrepo.ProductFindOptions{
		EnvName:    envName,
		Name:       projectName,
		Production: production,
	}
	env, err := commonrepo.NewProductColl().Find(opt)
	if err != nil {
		return e.ErrUpsertCronjob.AddErr(fmt.Errorf("failed to get environment %s/%s, err: %w", projectName, envName, err))
	}

	sleepName := util.GetEnvSleepCronName(projectName, envName, true)
	awakeName := util.GetEnvSleepCronName(projectName, envName, false)
	sleepCron, err := commonrepo.NewCronjobColl().GetByName(sleepName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return e.ErrUpsertCronjob.AddErr(fmt.Errorf("failed to get env sleep cron job for sleep, err: %w", err))
		}
	}
	awakeCron, err := commonrepo.NewCronjobColl().GetByName(awakeName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return e.ErrUpsertCronjob.AddErr(fmt.Errorf("failed to get env sleep cron job for awake, err: %w", err))
		}
	}
	cronMap := make(map[string]*commonmodels.Cronjob)
	if sleepCron != nil {
		cronMap[sleepCron.Name] = sleepCron
	}
	if awakeCron != nil {
		cronMap[awakeCron.Name] = awakeCron
	}

	for _, name := range []string{sleepName, awakeName} {
		var payload *commonservice.CronjobPayload
		if cron, ok := cronMap[name]; ok {
			origSleepEnabled := cron.Enabled
			if name == sleepName {
				cron.Enabled = req.SleepCronEnable
				cron.Cron = req.SleepCron
			} else if name == awakeName {
				cron.Enabled = req.AwakeCronEnable
				cron.Cron = req.AwakeCron
			}

			err = commonrepo.NewCronjobColl().Upsert(cron)
			if err != nil {
				fmtErr := fmt.Errorf("Failed to upsert cron job, error: %w", err)
				log.Error(fmtErr)
				return err
			}

			if origSleepEnabled && !req.SleepCronEnable {
				// need to disable cronjob
				payload = &commonservice.CronjobPayload{
					Name:       name,
					JobType:    setting.EnvSleepCronjob,
					Action:     setting.TypeEnableCronjob,
					DeleteList: []string{cron.ID.Hex()},
				}
			} else if !origSleepEnabled && req.SleepCronEnable || origSleepEnabled && req.SleepCronEnable {
				payload = &commonservice.CronjobPayload{
					Name:    name,
					JobType: setting.EnvSleepCronjob,
					Action:  setting.TypeEnableCronjob,
					JobList: []*commonmodels.Schedule{cronJobToSchedule(cron)},
				}
			} else {
				// !origEnabled && !req.Enable
				continue
			}
		} else {
			input := &commonmodels.Cronjob{
				Name: name,
				Type: setting.EnvSleepCronjob,
				EnvArgs: &commonmodels.EnvArgs{
					Name:        name,
					ProductName: env.ProductName,
					EnvName:     env.EnvName,
					Production:  env.Production,
				},
			}
			if name == sleepName {
				input.Enabled = req.SleepCronEnable
				input.Cron = req.SleepCron
			} else if name == awakeName {
				input.Enabled = req.AwakeCronEnable
				input.Cron = req.AwakeCron
			}

			err = commonrepo.NewCronjobColl().Upsert(input)
			if err != nil {
				fmtErr := fmt.Errorf("Failed to upsert cron job, error: %w", err)
				log.Error(fmtErr)
				return err
			}
			if !input.Enabled {
				continue
			}
			payload = &commonservice.CronjobPayload{
				Name:    name,
				JobType: setting.EnvSleepCronjob,
				Action:  setting.TypeEnableCronjob,
				JobList: []*commonmodels.Schedule{cronJobToSchedule(input)},
			}
		}

		pl, err := json.Marshal(payload)
		if err != nil {
			log.Errorf("Failed to marshal cronjob payload, the error is: %v", err)
			return e.ErrUpsertCronjob.AddDesc(err.Error())
		}
		err = commonrepo.NewMsgQueueCommonColl().Create(&msg_queue.MsgQueueCommon{
			Payload:   string(pl),
			QueueType: setting.TopicCronjob,
		})
		if err != nil {
			log.Errorf("Failed to publish to msg queue common: %s, the error is: %v", setting.TopicCronjob, err)
			return e.ErrUpsertCronjob.AddDesc(err.Error())
		}
	}

	return nil
}

func deleteEnvSleepCron(projectName, envName string) error {
	sleepName := util.GetEnvSleepCronName(projectName, envName, true)
	awakeName := util.GetEnvSleepCronName(projectName, envName, false)
	sleepCron, err := commonrepo.NewCronjobColl().GetByName(sleepName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return fmt.Errorf("failed to get env sleep cron job for sleep, err: %w", err)
		}
	}
	awakeCron, err := commonrepo.NewCronjobColl().GetByName(awakeName, setting.EnvSleepCronjob)
	if err != nil {
		if err != mongo.ErrNoDocuments && err != mongo.ErrNilDocument {
			return fmt.Errorf("failed to get env sleep cron job for awake, err: %w", err)
		}
	}

	idList := []string{}
	if sleepCron != nil {
		idList = append(idList, sleepCron.ID.Hex())
	}
	if awakeCron != nil {
		idList = append(idList, awakeCron.ID.Hex())
	}

	payload := &commonservice.CronjobPayload{
		Name:       "delete-env-sleep-cronjob",
		JobType:    setting.EnvSleepCronjob,
		Action:     setting.TypeEnableCronjob,
		DeleteList: idList,
	}

	pl, _ := json.Marshal(payload)
	err = commonrepo.NewMsgQueueCommonColl().Create(&msg_queue.MsgQueueCommon{
		Payload:   string(pl),
		QueueType: setting.TopicCronjob,
	})
	if err != nil {
		log.Errorf("Failed to publish to nsq topic: %s, the error is: %v", setting.TopicCronjob, err)
		return err
	}

	opt := &commonrepo.CronjobDeleteOption{
		IDList: idList,
	}
	err = commonrepo.NewCronjobColl().Delete(opt)
	if err != nil {
		return fmt.Errorf("failed to delete env sleep cron job %s, err: %w", sleepName, err)
	}

	return nil
}
