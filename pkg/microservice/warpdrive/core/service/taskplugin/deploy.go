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

package taskplugin

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/warpdrive/config"
	"github.com/koderover/zadig/v2/pkg/microservice/warpdrive/core/service/types"
	"github.com/koderover/zadig/v2/pkg/microservice/warpdrive/core/service/types/task"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/shared/kube/resource"
	"github.com/koderover/zadig/v2/pkg/shared/kube/wrapper"
	"github.com/koderover/zadig/v2/pkg/tool/httpclient"
	krkubeclient "github.com/koderover/zadig/v2/pkg/tool/kube/client"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/serializer"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
)

// InitializeDeployTaskPlugin to initiate deploy task plugin and return ref
func InitializeDeployTaskPlugin(taskType config.TaskType) TaskPlugin {
	return &DeployTaskPlugin{
		Name:       taskType,
		kubeClient: krkubeclient.Client(),
		ClientSet:  krkubeclient.Clientset(),
		restConfig: krkubeclient.RESTConfig(),
		httpClient: httpclient.New(
			httpclient.SetHostURL(configbase.AslanServiceAddress()),
		),
	}
}

// DeployTaskPlugin Plugin name should be compatible with task type
type DeployTaskPlugin struct {
	Name         config.TaskType
	JobName      string
	kubeClient   client.Client
	ClientSet    kubernetes.Interface
	restConfig   *rest.Config
	Task         *task.Deploy
	Log          *zap.SugaredLogger
	ReplaceImage string

	httpClient *httpclient.Client
}

func (p *DeployTaskPlugin) SetAckFunc(func()) {
}

const (
	imageUrlParseRegexString = `(?P<repo>.+/)?(?P<image>[^:]+){1}(:)?(?P<tag>.+)?`
)

var (
	imageParseRegex = regexp.MustCompile(imageUrlParseRegexString)
)

// Init ...
func (p *DeployTaskPlugin) Init(jobname, filename string, xl *zap.SugaredLogger) {
	p.JobName = jobname
	// SetLogger ...
	p.Log = xl
}

// Type ...
func (p *DeployTaskPlugin) Type() config.TaskType {
	return p.Name
}

// Status ...
func (p *DeployTaskPlugin) Status() config.Status {
	return p.Task.TaskStatus
}

// SetStatus ...
func (p *DeployTaskPlugin) SetStatus(status config.Status) {
	p.Task.TaskStatus = status
}

// TaskTimeout ...
func (p *DeployTaskPlugin) TaskTimeout() int {
	if p.Task.Timeout == 0 {
		p.Task.Timeout = setting.DeployTimeout
	}

	return p.Task.Timeout
}

type EnvArgs struct {
	EnvName     string `json:"env_name"`
	ProductName string `json:"product_name"`
}

type ResourceComponentSet interface {
	GetName() string
	GetAnnotations() map[string]string
	GetContainers() []*resource.ContainerImage
	GetKind() string
}

func (p *DeployTaskPlugin) Run(ctx context.Context, pipelineTask *task.Task, _ *task.PipelineCtx, _ string) {
	var (
		err      error
		replaced = false
	)

	defer func() {
		if err != nil {
			p.Log.Error(err)
			p.Task.TaskStatus = config.StatusFailed
			p.Task.Error = err.Error()
			return
		}
	}()

	if pipelineTask.ConfigPayload.DeployClusterID != "" {
		p.restConfig, err = kubeclient.GetRESTConfig(pipelineTask.ConfigPayload.HubServerAddr, pipelineTask.ConfigPayload.DeployClusterID)
		if err != nil {
			err = errors.WithMessage(err, "can't get k8s rest config")
			return
		}

		p.kubeClient, err = kubeclient.GetKubeClient(pipelineTask.ConfigPayload.HubServerAddr, pipelineTask.ConfigPayload.DeployClusterID)
		if err != nil {
			err = errors.WithMessage(err, "can't init k8s client")
			return
		}

		p.ClientSet, err = kubeclient.GetKubeClientSet(pipelineTask.ConfigPayload.HubServerAddr, pipelineTask.ConfigPayload.DeployClusterID)
		if err != nil {
			err = errors.WithMessagef(err, "failed to init k8s clientset")
		}
	}

	version, errGetVersion := p.ClientSet.Discovery().ServerVersion()
	if errGetVersion != nil {
		err = errors.WithMessage(errGetVersion, "failed to get kubernetes server version")
		return
	}

	productInfo, err := GetProductInfo(ctx, &EnvArgs{EnvName: p.Task.EnvName, ProductName: p.Task.ProductName})
	if err != nil {
		err = errors.WithMessagef(
			err,
			"failed to get product %s/%s",
			p.Task.Namespace, p.Task.ServiceName)
		return
	}
	if productInfo.IsSleeping() {
		err = fmt.Errorf("product %s/%s is sleeping", p.Task.ProductName, p.Task.EnvName)
		return
	}

	containerName := p.Task.ContainerName
	containerName = strings.TrimSuffix(containerName, "_"+p.Task.ServiceName)

	// get servcie info
	var (
		serviceInfo *types.ServiceTmpl
	)
	// TODO FIXME: the revision of the service info should be the value in product service
	serviceInfo, err = p.getService(ctx, p.Task.ServiceName, p.Task.ServiceType, p.Task.ProductName, 0)
	if err != nil {
		return
	}
	if serviceInfo.WorkloadType == "" {
		deployments, statefulSets, cronJobs, cronJobBetas, errFoundWorkload := fetchRelatedWorkloads(ctx, p.Task.EnvName, p.Task.Namespace, p.Task.ProductName, p.Task.ServiceName, p.kubeClient, version, p.httpClient, p.Log)
		if errFoundWorkload != nil {
			err = errors.WithMessage(errFoundWorkload, "failed to fetch related workloads")
			return
		}
	L:
		for _, deploy := range deployments {
			for _, container := range deploy.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateDeploymentImage(deploy.Namespace, deploy.Name, containerName, p.Task.Image, p.kubeClient)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/deployments/%s/%s",
							p.Task.Namespace, deploy.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.Deployment,
						Container: container.Name,
						Origin:    container.Image,
						Name:      deploy.Name,
					})
					replaced = true
					p.Task.RelatedPodLabels = append(p.Task.RelatedPodLabels, deploy.Spec.Template.Labels)
					break L
				}
			}
		}
	Loop:
		for _, sts := range statefulSets {
			for _, container := range sts.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateStatefulSetImage(sts.Namespace, sts.Name, containerName, p.Task.Image, p.kubeClient)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/statefulsets/%s/%s",
							p.Task.Namespace, sts.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.StatefulSet,
						Container: container.Name,
						Origin:    container.Image,
						Name:      sts.Name,
					})
					replaced = true
					p.Task.RelatedPodLabels = append(p.Task.RelatedPodLabels, sts.Spec.Template.Labels)
					break Loop
				}
			}
		}
	CronLoop:
		for _, cron := range cronJobs {
			for _, container := range cron.Spec.JobTemplate.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateCronJobImage(cron.Namespace, cron.Name, containerName, p.Task.Image, p.kubeClient, false)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/statefulsets/%s/%s",
							p.Task.Namespace, cron.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.CronJob,
						Container: container.Name,
						Origin:    container.Image,
						Name:      cron.Name,
					})
					replaced = true
					p.Task.RelatedPodLabels = append(p.Task.RelatedPodLabels, cron.Spec.JobTemplate.Spec.Template.Labels)
					break CronLoop
				}
			}
		}
	BetaCronLoop:
		for _, cron := range cronJobBetas {
			for _, container := range cron.Spec.JobTemplate.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateCronJobImage(cron.Namespace, cron.Name, containerName, p.Task.Image, p.kubeClient, false)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/statefulsets/%s/%s",
							p.Task.Namespace, cron.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.CronJob,
						Container: container.Name,
						Origin:    container.Image,
						Name:      cron.Name,
					})
					replaced = true
					p.Task.RelatedPodLabels = append(p.Task.RelatedPodLabels, cron.Spec.JobTemplate.Spec.Template.Labels)
					break BetaCronLoop
				}
			}
		}
	} else {
		switch serviceInfo.WorkloadType {
		case setting.StatefulSet:
			var statefulSet *appsv1.StatefulSet
			var found bool
			statefulSet, found, err = getter.GetStatefulSet(p.Task.Namespace, p.Task.ServiceName, p.kubeClient)
			if err != nil {
				return
			}
			if !found {
				err = fmt.Errorf("statefulset: %s not found", p.Task.ServiceName)
				return
			}
			for _, container := range statefulSet.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateStatefulSetImage(statefulSet.Namespace, statefulSet.Name, containerName, p.Task.Image, p.kubeClient)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/statefulsets/%s/%s",
							p.Task.Namespace, statefulSet.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.StatefulSet,
						Container: container.Name,
						Origin:    container.Image,
						Name:      statefulSet.Name,
					})
					replaced = true
					break
				}
			}
		case setting.Deployment:
			var deployment *appsv1.Deployment
			var found bool
			deployment, found, err = getter.GetDeployment(p.Task.Namespace, p.Task.ServiceName, p.kubeClient)
			if err != nil {
				return
			}
			if !found {
				err = fmt.Errorf("deployment: %s not found", p.Task.ServiceName)
				return
			}
			for _, container := range deployment.Spec.Template.Spec.Containers {
				if container.Name == containerName {
					err = updater.UpdateDeploymentImage(deployment.Namespace, deployment.Name, containerName, p.Task.Image, p.kubeClient)
					if err != nil {
						err = errors.WithMessagef(
							err,
							"failed to update container image in %s/deployments/%s/%s",
							p.Task.Namespace, deployment.Name, container.Name)
						return
					}
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.Deployment,
						Container: container.Name,
						Origin:    container.Image,
						Name:      deployment.Name,
					})
					replaced = true
					break
				}
			}
		case setting.CronJob:
			cronJob, cronJobBeta, found, errFoundCron := getter.GetCronJob(p.Task.Namespace, p.Task.ServiceName, p.kubeClient, kubeclient.VersionLessThan121(version))
			if errFoundCron != nil {
				err = errors.WithMessagef(errFoundCron, "failed to get cronjob %s", p.Task.ServiceName)
				return
			}
			if !found {
				err = fmt.Errorf("cronJob: %s not found", p.Task.ServiceName)
				return
			}
			if cronJob != nil {
				for _, container := range cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers {
					if container.Name == containerName {
						err = updater.UpdateCronJobImage(cronJob.Namespace, cronJob.Name, containerName, p.Task.Image, p.kubeClient, false)
						if err != nil {
							err = errors.WithMessagef(
								err,
								"failed to update container image in %s/cronJob/%s/%s",
								p.Task.Namespace, cronJob.Name, container.Name)
							return
						}
						p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
							Kind:      setting.CronJob,
							Container: container.Name,
							Origin:    container.Image,
							Name:      cronJob.Name,
						})
						replaced = true
						break
					}
				}
			}
			if cronJobBeta != nil {
				for _, container := range cronJobBeta.Spec.JobTemplate.Spec.Template.Spec.Containers {
					if container.Name == containerName {
						err = updater.UpdateCronJobImage(cronJobBeta.Namespace, cronJobBeta.Name, containerName, p.Task.Image, p.kubeClient, false)
						if err != nil {
							err = errors.WithMessagef(
								err,
								"failed to update container image in %s/cronJob/%s/%s",
								p.Task.Namespace, cronJobBeta.Name, container.Name)
							return
						}
						p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
							Kind:      setting.CronJob,
							Container: container.Name,
							Origin:    container.Image,
							Name:      cronJobBeta.Name,
						})
						replaced = true
						break
					}
				}
			}
		}
	}
	if !replaced {
		err = errors.Errorf(
			"container %s is not found in resources ", containerName)
		return
	}
}

func (p *DeployTaskPlugin) getService(ctx context.Context, name, serviceType, productName string, revision int64) (*types.ServiceTmpl, error) {
	url := fmt.Sprintf("/api/service/services/%s/%s", name, serviceType)

	s := &types.ServiceTmpl{}
	_, err := p.httpClient.Get(url, httpclient.SetResult(s), httpclient.SetQueryParams(map[string]string{
		"projectName": productName,
		"revision":    fmt.Sprintf("%d", revision),
	}))
	if err != nil {
		return nil, err
	}
	return s, nil
}

type CreateK8SEnvServiceVersionRequest struct {
	ServiceName     string `json:"service_name"`
	ServiceRevision int64  `json:"service_revision"`
	ContainerName   string `json:"container_name"`
	Image           string `json:"image"`
}

func getRenderedManifests(ctx context.Context, httpClient *httpclient.Client, envName, productName string, serviceName string) ([]string, error) {
	url := "/api/environment/export/service"
	prod := make([]string, 0)
	_, err := httpClient.Get(url, httpclient.SetResult(&prod),
		httpclient.SetQueryParam("projectName", productName),
		httpclient.SetQueryParam("envName", envName),
		httpclient.SetQueryParam("serviceName", serviceName),
		httpclient.SetQueryParam("source", "wd"),
		httpclient.SetQueryParam("ifPassFilter", "true"))
	if err != nil {
		return nil, err
	}
	return prod, nil
}

func serviceDeployed(strategy map[string]string, serviceName string) bool {
	if strategy == nil {
		return true
	}
	if strategy[serviceName] == setting.ServiceDeployStrategyImport {
		return false
	}
	return true
}

func fetchRelatedWorkloads(ctx context.Context, envName, namespace, productName, serviceName string, kClient client.Client, version *version.Info, httpClient *httpclient.Client, log *zap.SugaredLogger) (
	[]*appsv1.Deployment, []*appsv1.StatefulSet, []*batchv1.CronJob, []*batchv1beta1.CronJob, error) {
	selector := labels.Set{setting.ProductLabel: productName, setting.ServiceLabel: serviceName}.AsSelector()

	deployments, err := getter.ListDeployments(namespace, selector, kClient)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	statefulSets, err := getter.ListStatefulSets(namespace, selector, kClient)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	cronJobs, cronJobBeta, err := getter.ListCronJobs(namespace, selector, kClient, kubeclient.VersionLessThan121(version))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if len(deployments) > 0 || len(statefulSets) > 0 || len(cronJobs) > 0 || len(cronJobBeta) > 0 {
		return deployments, statefulSets, cronJobs, cronJobBeta, nil
	}
	// for services not deployed but only imported, we can't find workloads by 's-product' and 's-service'
	return fetchWorkloadsForImportedService(ctx, envName, namespace, productName, serviceName, kClient, version, httpClient, log)
}

func fetchWorkloadsForImportedService(ctx context.Context, envName, namespace, productName, serviceName string, kClient client.Client, version *version.Info, httpClient *httpclient.Client, log *zap.SugaredLogger) ([]*appsv1.Deployment, []*appsv1.StatefulSet, []*batchv1.CronJob, []*batchv1beta1.CronJob, error) {
	manifests, err := getRenderedManifests(ctx, httpClient, envName, productName, serviceName)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	deploys, stss, crons, cronBetas := make([]*appsv1.Deployment, 0), make([]*appsv1.StatefulSet, 0), make([]*batchv1.CronJob, 0), make([]*batchv1beta1.CronJob, 0)
	for _, manifest := range manifests {
		u, err := serializer.NewDecoder().YamlToUnstructured([]byte(manifest))
		if err != nil {
			log.Errorf("failed to convert yaml to Unstructured when check resources, manifest is\n%s\n, error: %v", manifest, err)
			continue
		}
		if u.GetKind() == setting.Deployment {
			deployment, exist, err := getter.GetDeployment(namespace, u.GetName(), kClient)
			if err != nil || !exist {
				log.Errorf("failed to find deployment with name: %s", u.GetName())
				continue
			}
			deploys = append(deploys, deployment)
		} else if u.GetKind() == setting.StatefulSet {
			sts, exist, err := getter.GetStatefulSet(namespace, u.GetName(), kClient)
			if err != nil || !exist {
				log.Errorf("failed to find sts with name: %s", u.GetName())
				continue
			}
			stss = append(stss, sts)
		} else if u.GetKind() == setting.CronJob {
			cron, cronBatch, exist, err := getter.GetCronJob(namespace, u.GetName(), kClient, kubeclient.VersionLessThan121(version))
			if err != nil || !exist {
				log.Errorf("faile to find cronjob with name: %s", u.GetName())
				continue
			}
			if cron != nil {
				crons = append(crons, cron)
			}
			if cronBatch != nil {
				cronBetas = append(cronBetas, cronBatch)
			}
		}
	}
	return deploys, stss, crons, cronBetas, nil
}

// Wait ...
func (p *DeployTaskPlugin) Wait(ctx context.Context) {
	// skip waiting for reset image task
	if p.Task.SkipWaiting {
		p.Task.TaskStatus = config.StatusPassed
		return
	}

	timeout := time.After(time.Duration(p.TaskTimeout()) * time.Second)

	resources, err := p.getResourcesPodOwnerUID()
	if err != nil {
		msg := fmt.Sprintf("get resource owner info error: %v", err)
		p.Task.Error = msg
		return
	}
	p.Task.ReplaceResources = resources

	for {
		select {
		case <-ctx.Done():
			p.Task.TaskStatus = config.StatusCancelled
			return

		case <-timeout:
			p.Task.TaskStatus = config.StatusTimeout

			var msg []string
			for _, label := range p.Task.RelatedPodLabels {
				selector := labels.Set(label).AsSelector()
				pods, err := getter.ListPods(p.Task.Namespace, selector, p.kubeClient)
				if err != nil {
					p.Task.Error = err.Error()
					return
				}

				for _, pod := range pods {
					podResource := wrapper.Pod(pod).Resource()
					if podResource.Status != setting.StatusRunning && podResource.Status != setting.StatusSucceeded {
						for _, cs := range podResource.Containers {
							// message为空不认为是错误状态，有可能还在waiting
							if cs.Message != "" {
								msg = append(msg, fmt.Sprintf("Status: %s, Reason: %s, Message: %s", cs.Status, cs.Reason, cs.Message))
							}
						}
					}
				}
			}

			if len(msg) != 0 {
				p.Task.Error = strings.Join(msg, "\n")
			}

			return

		default:
			time.Sleep(time.Second * 2)
			ready := true
			var err error
		L:
			for _, resource := range p.Task.ReplaceResources {
				//if err := workLoadDeployStat(p.kubeClient, p.Task.Namespace, p.Task.RelatedPodLabels, resource.PodOwnerUID); err != nil {
				//	p.Task.TaskStatus = config.StatusFailed
				//	p.Task.Error = err.Error()
				//	return
				//}
				switch resource.Kind {
				case setting.Deployment:
					d, found, e := getter.GetDeployment(p.Task.Namespace, resource.Name, p.kubeClient)
					if e != nil {
						err = e
					}
					if e != nil || !found {
						p.Log.Errorf(
							"failed to check deployment ready status %s/%s/%s - %v",
							p.Task.Namespace,
							resource.Kind,
							resource.Name,
							e,
						)
						ready = false
					} else {
						ready = wrapper.Deployment(d).Ready()
					}

					if !ready {
						break L
					}
				case setting.StatefulSet:
					s, found, e := getter.GetStatefulSet(p.Task.Namespace, resource.Name, p.kubeClient)
					if e != nil {
						err = e
					}
					if err != nil || !found {
						p.Log.Errorf(
							"failed to check statefulSet ready status %s/%s/%s",
							p.Task.Namespace,
							resource.Kind,
							resource.Name,
							e,
						)
						ready = false
					} else {
						ready = wrapper.StatefulSet(s).Ready()
					}

					if !ready {
						break L
					}
				}
			}

			if ready {
				p.Task.TaskStatus = config.StatusPassed
			}
			if p.IsTaskDone() {
				return
			}
		}
	}
}

func workLoadDeployStat(kubeClient client.Client, namespace string, labelMaps []map[string]string, ownerUID string) error {
	for _, label := range labelMaps {
		selector := labels.Set(label).AsSelector()
		pods, err := getter.ListPods(namespace, selector, kubeClient)
		if err != nil {
			return err
		}
		for _, pod := range pods {
			if !wrapper.Pod(pod).IsOwnerMatched(ownerUID) {
				continue
			}
			allContainerStatuses := make([]corev1.ContainerStatus, 0)
			allContainerStatuses = append(allContainerStatuses, pod.Status.InitContainerStatuses...)
			allContainerStatuses = append(allContainerStatuses, pod.Status.ContainerStatuses...)
			for _, cs := range allContainerStatuses {
				if cs.State.Waiting != nil {
					switch cs.State.Waiting.Reason {
					case "ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff", "ErrImageNeverPull":
						return fmt.Errorf("pod: %s, %s: %s", pod.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message)
					}
				}
			}
		}
	}
	return nil
}

func (p *DeployTaskPlugin) getResourcesPodOwnerUID() ([]task.Resource, error) {
	newResources := []task.Resource{}
	for _, resource := range p.Task.ReplaceResources {
		switch resource.Kind {
		case setting.StatefulSet:
			sts, _, err := getter.GetStatefulSet(p.Task.Namespace, resource.Name, p.kubeClient)
			if err != nil {
				return newResources, err
			}
			resource.PodOwnerUID = string(sts.ObjectMeta.UID)
		case setting.Deployment:
			deployment, _, err := getter.GetDeployment(p.Task.Namespace, resource.Name, p.kubeClient)
			if err != nil {
				return newResources, err
			}
			selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
			if err != nil {
				return nil, err
			}

			// esure latest replicaset to be created
			time.Sleep(3 * time.Second)

			replicaSets, err := getter.ListReplicaSets(p.Task.Namespace, selector, p.kubeClient)
			if err != nil {
				return newResources, err
			}
			// Only include those whose ControllerRef matches the Deployment.
			owned := make([]*appsv1.ReplicaSet, 0, len(replicaSets))
			for _, rs := range replicaSets {
				if metav1.IsControlledBy(rs, deployment) {
					owned = append(owned, rs)
				}
			}
			if len(owned) <= 0 {
				return newResources, fmt.Errorf("no replicaset found for deployment: %s", deployment.Name)
			}
			sort.Slice(owned, func(i, j int) bool {
				return owned[i].CreationTimestamp.After(owned[j].CreationTimestamp.Time)
			})
			resource.PodOwnerUID = string(owned[0].ObjectMeta.UID)
		}
		newResources = append(newResources, resource)
	}
	return newResources, nil
}

func (p *DeployTaskPlugin) Complete(ctx context.Context, pipelineTask *task.Task, serviceName string) {
}

func (p *DeployTaskPlugin) SetTask(t map[string]interface{}) error {
	task, err := ToDeployTask(t)
	if err != nil {
		return err
	}
	p.Task = task

	return nil
}

// GetTask ...
func (p *DeployTaskPlugin) GetTask() interface{} {
	return p.Task
}

// IsTaskDone ...
func (p *DeployTaskPlugin) IsTaskDone() bool {
	if p.Task.TaskStatus != config.StatusCreated && p.Task.TaskStatus != config.StatusRunning {
		return true
	}
	return false
}

// IsTaskFailed ...
func (p *DeployTaskPlugin) IsTaskFailed() bool {
	if p.Task.TaskStatus == config.StatusFailed || p.Task.TaskStatus == config.StatusTimeout || p.Task.TaskStatus == config.StatusCancelled {
		return true
	}
	return false
}

// SetStartTime ...
func (p *DeployTaskPlugin) SetStartTime() {
	p.Task.StartTime = time.Now().Unix()
}

// SetEndTime ...
func (p *DeployTaskPlugin) SetEndTime() {
	p.Task.EndTime = time.Now().Unix()
}

// IsTaskEnabled ...
func (p *DeployTaskPlugin) IsTaskEnabled() bool {
	return p.Task.Enabled
}

// ResetError ...
func (p *DeployTaskPlugin) ResetError() {
	p.Task.Error = ""
}
