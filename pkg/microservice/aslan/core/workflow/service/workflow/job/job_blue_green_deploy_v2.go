/*
 * Copyright 2023 The KodeRover Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package job

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/koderover/zadig/v2/pkg/cli/zadig-agent/helper/log"
	"github.com/pkg/errors"
	"helm.sh/helm/v3/pkg/releaseutil"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	aslanUtil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	serializer2 "github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
)

type BlueGreenDeployV2Job struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.BlueGreenDeployV2JobSpec
}

func (j *BlueGreenDeployV2Job) Instantiate() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenDeployV2Job) SetPreset() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	envName := j.spec.Env

	if strings.HasPrefix(envName, setting.FixedValueMark) {
		// if the env is fixed, we put the env in the option
		envName = strings.ReplaceAll(j.spec.Env, setting.FixedValueMark, "")
	}

	serviceInfo, _, err := generateBlueGreenEnvDeployServiceInfo(envName, j.workflow.Project, j.spec.Services)
	if err != nil {
		log.Errorf("failed to generate blue-green deploy info for env: %s, error: %s", envName, err)
		return err
	}

	j.spec.Services = serviceInfo
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenDeployV2Job) SetOptions() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	// find the original workflow to get the configured data
	originalWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	originalSpec := new(commonmodels.BlueGreenDeployV2JobSpec)
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

	envOptions := make([]*commonmodels.ZadigBlueGreenDeployEnvInformation, 0)

	if strings.HasPrefix(originalSpec.Env, setting.FixedValueMark) {
		// if the env is fixed, we put the env in the option
		envName := strings.ReplaceAll(originalSpec.Env, setting.FixedValueMark, "")

		serviceInfo, registryID, err := generateBlueGreenEnvDeployServiceInfo(envName, j.workflow.Project, originalSpec.Services)
		if err != nil {
			log.Errorf("failed to generate blue-green deploy info for env: %s, error: %s", envName, err)
			return err
		}

		envOptions = append(envOptions, &commonmodels.ZadigBlueGreenDeployEnvInformation{
			Env:        envName,
			RegistryID: registryID,
			Services:   serviceInfo,
		})
	} else {
		// otherwise list all the envs in this project
		products, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
			Name:       j.workflow.Project,
			Production: util.GetBoolPointer(originalSpec.Production),
		})
		if err != nil {
			return fmt.Errorf("can't list envs in project %s, error: %w", j.workflow.Project, err)
		}
		for _, env := range products {
			serviceInfo, registryID, err := generateBlueGreenEnvDeployServiceInfo(env.EnvName, j.workflow.Project, originalSpec.Services)
			if err != nil {
				log.Errorf("failed to generate blue-green deploy info for env: %s, error: %s", env.EnvName, err)
				continue
			}

			envOptions = append(envOptions, &commonmodels.ZadigBlueGreenDeployEnvInformation{
				Env:        env.EnvName,
				RegistryID: registryID,
				Services:   serviceInfo,
			})
		}
	}

	j.spec.EnvOptions = envOptions
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenDeployV2Job) ClearOptions() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.EnvOptions = nil
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenDeployV2Job) ClearSelectionField() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.spec.Services = make([]*commonmodels.BlueGreenDeployV2Service, 0)
	j.job.Spec = j.spec
	return nil
}

func (j *BlueGreenDeployV2Job) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	// find the original workflow to get the configured data
	latestWorkflow, err := commonrepo.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.BlueGreenDeployV2JobSpec)
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

	j.spec.Env = latestSpec.Env

	// Determine service list and its corresponding kvs
	deployableService, _, err := generateBlueGreenEnvDeployServiceInfo(latestSpec.Env, j.workflow.Project, latestSpec.Services)
	if err != nil {
		log.Errorf("failed to generate deployable service from latest workflow spec, err: %s", err)
		return err
	}

	mergedService := make([]*commonmodels.BlueGreenDeployV2Service, 0)
	userConfiguredService := make(map[string]*commonmodels.BlueGreenDeployV2Service)

	for _, service := range j.spec.Services {
		userConfiguredService[service.ServiceName] = service
	}

	for _, service := range deployableService {
		if userSvc, ok := userConfiguredService[service.ServiceName]; ok {
			mergedService = append(mergedService, &commonmodels.BlueGreenDeployV2Service{
				ServiceName:         service.ServiceName,
				BlueServiceYaml:     userSvc.BlueServiceYaml,
				BlueServiceName:     service.BlueServiceYaml,
				BlueDeploymentYaml:  service.BlueDeploymentYaml,
				BlueDeploymentName:  service.BlueDeploymentName,
				GreenDeploymentName: service.GreenDeploymentName,
				GreenServiceName:    service.GreenServiceName,
				ServiceAndImage:     userSvc.ServiceAndImage,
			})
		} else {
			continue
		}
	}

	j.spec.Services = mergedService
	j.job.Spec = j.spec
	return nil
}

// TODO: This function now can only be used for production environments
func generateBlueGreenEnvDeployServiceInfo(env, project string, services []*commonmodels.BlueGreenDeployV2Service) ([]*commonmodels.BlueGreenDeployV2Service, string, error) {
	targetEnv, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName:    env,
		Name:       project,
		Production: util.GetBoolPointer(true),
	})

	configuredServiceMap := make(map[string]*commonmodels.BlueGreenDeployV2Service)
	for _, svc := range services {
		configuredServiceMap[svc.ServiceName] = svc
	}

	if err != nil {
		return nil, "", fmt.Errorf("failed to find product %s, env %s, err: %s", project, env, err)
	}

	latestSvcList, err := repository.ListMaxRevisionsServices(project, true)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list services with max revisions in project: %s, error: %s")
	}

	serviceInfo, err := commonservice.BuildServiceInfoInEnv(targetEnv, latestSvcList, nil, log.GetSimpleLogger())
	if err != nil {
		return nil, "", fmt.Errorf("failed to build service info in env: %s, error: %s", env, err)
	}

	resp := make([]*commonmodels.BlueGreenDeployV2Service, 0)

	for _, envService := range serviceInfo.Services {
		if !envService.Deployed {
			continue
		}

		appendService := &commonmodels.BlueGreenDeployV2Service{
			ServiceName: envService.ServiceName,
		}
		modules := make([]*commonmodels.BlueGreenDeployV2ServiceModuleAndImage, 0)

		for _, module := range envService.ServiceModules {
			modules = append(modules, &commonmodels.BlueGreenDeployV2ServiceModuleAndImage{
				ServiceModule: module.Name,
				Image:         module.Image,
				ImageName:     util.ExtractImageName(module.Image),
				Name:          module.Name,
				ServiceName:   envService.ServiceName,
			})
		}
		appendService.ServiceAndImage = modules

		// if a yaml is pre-configured, use it. Otherwise, just get it from the environment and do some render.
		if configuredService, ok := configuredServiceMap[envService.ServiceName]; ok && len(configuredService.BlueServiceYaml) != 0 {
			appendService.BlueServiceYaml = configuredService.BlueServiceYaml
		} else {
			yamlContent, _, err := kube.FetchCurrentAppliedYaml(&kube.GeneSvcYamlOption{
				ProductName: project,
				EnvName:     env,
				ServiceName: envService.ServiceName,
			})
			if err != nil {
				return nil, "", fmt.Errorf("failed to fetch %s current applied yaml, err: %s", envService.ServiceName, err)
			}

			resources := make([]*unstructured.Unstructured, 0)
			manifests := releaseutil.SplitManifests(yamlContent)
			for _, item := range manifests {
				u, err := serializer2.NewDecoder().YamlToUnstructured([]byte(item))
				if err != nil {
					return nil, "", fmt.Errorf("failed to decode service %s yaml to unstructured: %v", envService.ServiceName, err)
				}
				resources = append(resources, u)
			}

			serviceNum := 0
			for _, resource := range resources {
				switch resource.GetKind() {
				case setting.Service:
					if serviceNum > 0 {
						return nil, "", fmt.Errorf("service %s has more than one service", envService.ServiceName)
					}
					serviceNum++
					service := &corev1.Service{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.Object, service)
					if err != nil {
						return nil, "", fmt.Errorf("failed to convert service %s service to service object: %v", envService.ServiceName, err)
					}
					service.Name = service.Name + "-blue"
					if service.Spec.Selector == nil {
						service.Spec.Selector = make(map[string]string)
					}
					service.Spec.Selector[config.BlueGreenVersionLabelName] = config.BlueVersion
					appendService.BlueServiceYaml, err = toYaml(service)
					if err != nil {
						return nil, "", fmt.Errorf("failed to marshal service %s service object: %v", envService.ServiceName, err)
					}
				}
			}
			if serviceNum == 0 {
				return nil, "", fmt.Errorf("service %s has no service", envService.ServiceName)
			}
		}

		resp = append(resp, appendService)
	}

	registryID := targetEnv.RegistryID
	if registryID == "" {
		registry, err := commonrepo.NewRegistryNamespaceColl().Find(&commonrepo.FindRegOps{
			IsDefault: true,
		})

		if err != nil {
			return nil, "", fmt.Errorf("failed to find default registry for env: %s, error: %s", env, err)
		}
		registryID = registry.ID.Hex()
	}
	return resp, registryID, nil
}

func (j *BlueGreenDeployV2Job) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.BlueGreenDeployV2JobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.Services = argsSpec.Services
		j.job.Spec = j.spec
	}
	return nil
}

func (j *BlueGreenDeployV2Job) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}

	templateProduct, err := templaterepo.NewProductColl().Find(j.workflow.Project)
	if err != nil {
		return resp, fmt.Errorf("cannot find product %s: %w", j.workflow.Project, err)
	}
	timeout := templateProduct.Timeout * 60

	if len(j.spec.Services) == 0 {
		return resp, errors.Errorf("target services is empty")
	}

	for _, target := range j.spec.Services {
		var (
			deployment              *v1.Deployment
			deploymentYaml          string
			greenDeploymentSelector map[string]string
			service                 *corev1.Service
			greenService            *corev1.Service
			serviceYaml             string
			greenDeploymentName     string
		)
		if target.BlueServiceYaml != "" {
			service = &corev1.Service{}
			decoder := serializer.NewCodecFactory(scheme.Scheme).UniversalDecoder()
			err := runtime.DecodeInto(decoder, []byte(target.BlueServiceYaml), service)
			if err != nil {
				return resp, errors.Errorf("failed to decode %s k8s service yaml, err: %s", target.ServiceName, err)
			}
			serviceYaml = target.BlueServiceYaml
		} else {
			return resp, errors.Errorf("service %s blue service yaml is empty", target.ServiceName)
		}

		yamlContent, _, err := kube.FetchCurrentAppliedYaml(&kube.GeneSvcYamlOption{
			ProductName: j.workflow.Project,
			EnvName:     j.spec.Env,
			ServiceName: target.ServiceName,
		})
		if err != nil {
			return resp, errors.Errorf("failed to fetch %s current applied yaml, err: %s", target.ServiceName, err)
		}
		resources := make([]*unstructured.Unstructured, 0)
		manifests := releaseutil.SplitManifests(yamlContent)
		for _, item := range manifests {
			u, err := serializer2.NewDecoder().YamlToUnstructured([]byte(item))
			if err != nil {
				return resp, errors.Errorf("failed to decode service %s yaml to unstructured: %v", target.ServiceName, err)
			}
			resources = append(resources, u)
		}

		for _, resource := range resources {
			switch resource.GetKind() {
			case setting.Deployment:
				if deployment != nil {
					return resp, errors.Errorf("service %s has more than one deployment", target.ServiceName)
				}
				deployment = &v1.Deployment{}
				err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.Object, deployment)
				if err != nil {
					return resp, errors.Errorf("failed to convert service %s deployment to deployment object: %v", target.ServiceName, err)
				}
				greenDeploymentName = deployment.Name
				if deployment.Spec.Selector == nil {
					return resp, errors.Errorf("service %s deployment selector is empty", target.ServiceName)
				}
				greenDeploymentSelector = deployment.Spec.Selector.MatchLabels
				deployment.Name = deployment.Name + "-blue"
				deployment.Labels = addLabels(deployment.Labels, map[string]string{
					types.ZadigReleaseTypeLabelKey:        types.ZadigReleaseTypeBlueGreen,
					types.ZadigReleaseServiceNameLabelKey: target.ServiceName,
				})
				deployment.Spec.Selector.MatchLabels = addLabels(deployment.Spec.Selector.MatchLabels, map[string]string{
					config.BlueGreenVersionLabelName: config.BlueVersion,
				})
				deployment.Spec.Template.Labels = addLabels(deployment.Spec.Template.Labels, map[string]string{
					config.BlueGreenVersionLabelName: config.BlueVersion,
				})
				deploymentYaml, err = toYaml(deployment)
				if err != nil {
					return resp, errors.Errorf("failed to marshal service %s deployment object: %v", target.ServiceName, err)
				}
				var newImages []*commonmodels.Container
				for _, image := range target.ServiceAndImage {
					newImages = append(newImages, &commonmodels.Container{
						Name:  image.ServiceModule,
						Image: image.Image,
					})
				}
				deploymentYaml, _, err = kube.ReplaceWorkloadImages(deploymentYaml, newImages)
				if err != nil {
					return resp, errors.Errorf("failed to replace service %s deployment image: %v", target.ServiceName, err)
				}
			case setting.Service:
				greenService = &corev1.Service{}
				err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.Object, greenService)
				if err != nil {
					return resp, errors.Errorf("failed to convert service %s service to service object: %v", target.ServiceName, err)
				}
				target.GreenServiceName = greenService.Name
			}
		}
		if deployment == nil || service == nil {
			return resp, errors.Errorf("service %s has no deployment or service", target.ServiceName)
		}
		if service.Spec.Selector == nil || deployment.Spec.Template.Labels == nil {
			return resp, errors.Errorf("service %s has no service selector or deployment has no labels", target.ServiceName)
		}
		selector, err := metav1.LabelSelectorAsSelector(metav1.SetAsLabelSelector(service.Spec.Selector))
		if err != nil {
			return resp, errors.Errorf("service %s k8s service convert to selector err: %v", target.ServiceName, err)
		}
		if !selector.Matches(labels.Set(deployment.Spec.Template.Labels)) {
			return resp, errors.Errorf("service %s k8s service selector not match deployment.spec.template labels", target.ServiceName)
		}
		if greenService == nil {
			return resp, errors.Errorf("service %s has no k8s service", target.ServiceName)
		}
		greenSelector, err := metav1.LabelSelectorAsSelector(metav1.SetAsLabelSelector(greenService.Spec.Selector))
		if err != nil {
			return resp, errors.Errorf("service %s k8s green service convert to selector err: %v", target.ServiceName, err)
		}
		if !greenSelector.Matches(labels.Set(greenDeploymentSelector)) {
			return resp, errors.Errorf("service %s k8s green service selector not match deployment.spec.template labels", target.ServiceName)
		}

		// set target value for blue_green_release ToJobs get these
		target.BlueDeploymentName = deployment.Name
		target.BlueServiceName = service.Name
		target.GreenDeploymentName = greenDeploymentName

		task := &commonmodels.JobTask{
			Name: jobNameFormat(j.job.Name + "-" + target.ServiceName),
			Key:  strings.Join([]string{j.job.Name, target.ServiceName}, "."),
			JobInfo: map[string]string{
				JobNameKey:     j.job.Name,
				"service_name": target.ServiceName,
			},
			JobType: string(config.JobK8sBlueGreenDeploy),
			Spec: &commonmodels.JobTaskBlueGreenDeployV2Spec{
				Production: j.spec.Production,
				Env:        j.spec.Env,
				Service: &commonmodels.BlueGreenDeployV2Service{
					ServiceName:         target.ServiceName,
					BlueServiceYaml:     serviceYaml,
					BlueServiceName:     service.Name,
					BlueDeploymentYaml:  deploymentYaml,
					BlueDeploymentName:  deployment.Name,
					GreenServiceName:    target.GreenServiceName,
					GreenDeploymentName: greenDeploymentName,
					ServiceAndImage:     target.ServiceAndImage,
				},
				DeployTimeout: timeout,
			},
		}
		resp = append(resp, task)
	}

	j.job.Spec = j.spec
	return resp, nil
}

func (j *BlueGreenDeployV2Job) LintJob() error {
	j.spec = &commonmodels.BlueGreenDeployV2JobSpec{}
	if err := aslanUtil.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}
	if err := commonmodels.IToiYaml(j.job.Spec, j.spec); err != nil {
		return err
	}
	if j.spec.Version == "" {
		return errors.Errorf("job %s version is too old and not supported, please remove it and create a new one", j.job.Name)
	}
	quoteJobs := []*commonmodels.Job{}
	for _, stage := range j.workflow.Stages {
		for _, job := range stage.Jobs {
			if job.JobType != config.JobK8sBlueGreenRelease {
				continue
			}
			releaseJobSpec := &commonmodels.BlueGreenReleaseV2JobSpec{}
			if err := commonmodels.IToiYaml(job.Spec, releaseJobSpec); err != nil {
				return err
			}
			if releaseJobSpec.FromJob == j.job.Name {
				quoteJobs = append(quoteJobs, job)
			}
		}
	}
	if len(quoteJobs) == 0 {
		return errors.Errorf("no blue-green release job quote blue-green deploy job %s", j.job.Name)
	}
	if len(quoteJobs) > 1 {
		return errors.Errorf("more than one blue-green release job quote blue-green deploy job %s", j.job.Name)
	}
	jobRankmap := getJobRankMap(j.workflow.Stages)
	if jobRankmap[j.job.Name] >= jobRankmap[quoteJobs[0].Name] {
		return errors.Errorf("blue-green release job %s should run before blue-green deploy job %s", quoteJobs[0].Name, j.job.Name)
	}
	return nil
}

func toYaml(obj runtime.Object) (string, error) {
	y := printers.YAMLPrinter{}
	writer := bytes.NewBuffer(nil)
	err := y.PrintObj(obj, writer)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal object to yaml")
	}
	return writer.String(), nil
}

func addLabels(labels, newLabels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range newLabels {
		labels[k] = v
	}
	return labels
}
