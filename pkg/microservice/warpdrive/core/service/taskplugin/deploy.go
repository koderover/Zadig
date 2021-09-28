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
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	helmclient "github.com/mittwald/go-helm-client"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	configbase "github.com/koderover/zadig/pkg/config"
	"github.com/koderover/zadig/pkg/microservice/warpdrive/config"
	"github.com/koderover/zadig/pkg/microservice/warpdrive/core/service/taskplugin/s3"
	"github.com/koderover/zadig/pkg/microservice/warpdrive/core/service/types"
	"github.com/koderover/zadig/pkg/microservice/warpdrive/core/service/types/task"
	"github.com/koderover/zadig/pkg/setting"
	"github.com/koderover/zadig/pkg/shared/kube/wrapper"
	helmtool "github.com/koderover/zadig/pkg/tool/helmclient"
	"github.com/koderover/zadig/pkg/tool/httpclient"
	krkubeclient "github.com/koderover/zadig/pkg/tool/kube/client"
	"github.com/koderover/zadig/pkg/tool/kube/getter"
	"github.com/koderover/zadig/pkg/tool/kube/multicluster"
	"github.com/koderover/zadig/pkg/tool/kube/updater"
	"github.com/koderover/zadig/pkg/tool/log"
	s3tool "github.com/koderover/zadig/pkg/tool/s3"
	"github.com/koderover/zadig/pkg/util"
	"github.com/koderover/zadig/pkg/util/fs"
)

// InitializeDeployTaskPlugin to initiate deploy task plugin and return ref
func InitializeDeployTaskPlugin(taskType config.TaskType) TaskPlugin {
	return &DeployTaskPlugin{
		Name:       taskType,
		kubeClient: krkubeclient.Client(),
		restConfig: krkubeclient.RESTConfig(),
		httpClient: httpclient.New(
			httpclient.SetAuthScheme(setting.RootAPIKey),
			httpclient.SetAuthToken(config.PoetryAPIRootKey()),
			httpclient.SetHostURL(configbase.AslanServiceAddress()),
		),
	}
}

// DeployTaskPlugin Plugin name should be compatible with task type
type DeployTaskPlugin struct {
	Name         config.TaskType
	JobName      string
	kubeClient   client.Client
	restConfig   *rest.Config
	Task         *task.Deploy
	Log          *zap.SugaredLogger
	ReplaceImage string

	httpClient *httpclient.Client
}

func (p *DeployTaskPlugin) SetAckFunc(func()) {
}

const (
	// DeployTimeout ...
	DeployTimeout            = 60 * 10 // 10 minutes
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
		p.Task.Timeout = DeployTimeout
	} else {
		if !p.Task.IsRestart {
			p.Task.Timeout = p.Task.Timeout * 60
		}
	}

	return p.Task.Timeout
}

type EnvArgs struct {
	EnvName     string `json:"env_name"`
	ProductName string `json:"product_name"`
}

type SelectorBuilder struct {
	ProductName  string
	GroupName    string
	ServiceName  string
	ConfigBackup string
	EnvName      string
}

const (
	// ProductLabel ...
	ProductLabel = "s-product"
	// GroupLabel ...
	GroupLabel = "s-group"
	// ServiceLabel ...
	ServiceLabel = "s-service"
	// ConfigBackupLabel ...
	ConfigBackupLabel = "config-backup"
	// NamespaceLabel
	EnvNameLabel = "s-env"
	// EnvName ...
	UpdateBy   = "update-by"
	UpdateByID = "update-by-id"
	UpdateTime = "update-time"
)

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
		p.restConfig, err = multicluster.GetRESTConfig(pipelineTask.ConfigPayload.HubServerAddr, pipelineTask.ConfigPayload.DeployClusterID)
		if err != nil {
			err = errors.WithMessage(err, "can't get k8s rest config")
			return
		}

		p.kubeClient, err = multicluster.GetKubeClient(pipelineTask.ConfigPayload.HubServerAddr, pipelineTask.ConfigPayload.DeployClusterID)
		if err != nil {
			err = errors.WithMessage(err, "can't init k8s client")
			return
		}
	}

	if p.Task.ServiceType != setting.HelmDeployType {
		// get servcie info
		var (
			serviceInfo *types.ServiceTmpl
			selector    labels.Selector
		)
		serviceInfo, err = p.getService(ctx, p.Task.ServiceName, p.Task.ServiceType, p.Task.ProductName)
		if err != nil {
			return
		}
		if serviceInfo.WorkloadType == "" {
			selector := labels.Set{setting.ProductLabel: p.Task.ProductName, setting.ServiceLabel: p.Task.ServiceName}.AsSelector()

			var deployments []*appsv1.Deployment
			deployments, err = getter.ListDeployments(p.Task.Namespace, selector, p.kubeClient)
			if err != nil {
				return
			}

			var statefulSets []*appsv1.StatefulSet
			statefulSets, err = getter.ListStatefulSets(p.Task.Namespace, selector, p.kubeClient)
			if err != nil {
				return
			}

		L:
			for _, deploy := range deployments {
				for _, container := range deploy.Spec.Template.Spec.Containers {
					if container.Name == p.Task.ContainerName {
						err = updater.UpdateDeploymentImage(deploy.Namespace, deploy.Name, p.Task.ContainerName, p.Task.Image, p.kubeClient)
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
						break L
					}
				}
			}
		Loop:
			for _, sts := range statefulSets {
				for _, container := range sts.Spec.Template.Spec.Containers {
					if container.Name == p.Task.ContainerName {
						err = updater.UpdateStatefulSetImage(sts.Namespace, sts.Name, p.Task.ContainerName, p.Task.Image, p.kubeClient)
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
						break Loop
					}
				}
			}
		} else {
			switch serviceInfo.WorkloadType {
			case setting.StatefulSet:
				var statefulSet *appsv1.StatefulSet
				statefulSet, _, err = getter.GetStatefulSet(p.Task.Namespace, p.Task.ServiceName, p.kubeClient)
				if err != nil {
					return
				}
				for _, container := range statefulSet.Spec.Template.Spec.Containers {
					if container.Name == p.Task.ContainerName {
						err = updater.UpdateStatefulSetImage(statefulSet.Namespace, statefulSet.Name, p.Task.ContainerName, p.Task.Image, p.kubeClient)
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
				deployment, _, err = getter.GetDeployment(p.Task.Namespace, p.Task.ServiceName, p.kubeClient)
				if err != nil {
					return
				}
				for _, container := range deployment.Spec.Template.Spec.Containers {
					if container.Name == p.Task.ContainerName {
						err = updater.UpdateDeploymentImage(deployment.Namespace, deployment.Name, p.Task.ContainerName, p.Task.Image, p.kubeClient)
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
			}
		}
		if !replaced {
			err = errors.Errorf(
				"container %s is not found in resources with label %s", p.Task.ContainerName, selector)
			return
		}
	} else if p.Task.ServiceType == setting.HelmDeployType {
		var (
			productInfo       *types.Product
			chartInfoMap      = make(map[string]*types.RenderChart)
			renderChart       *types.RenderChart
			isExist           = false
			replaceValuesYaml string
			yamlValuesByte    []byte
			renderInfo        *types.RenderSet
			helmClient        helmclient.Client
		)

		deployments, _ := getter.ListDeployments(p.Task.Namespace, nil, p.kubeClient)
		for _, deploy := range deployments {
			for _, container := range deploy.Spec.Template.Spec.Containers {
				if strings.Contains(container.Image, p.Task.ContainerName) {
					p.Log.Infof("deployments find match container.name:%s", container.Name)
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.Deployment,
						Container: container.Name,
						Origin:    container.Image,
						Name:      deploy.Name,
					})
				}
			}
		}

		statefulSets, _ := getter.ListStatefulSets(p.Task.Namespace, nil, p.kubeClient)
		for _, sts := range statefulSets {
			for _, container := range sts.Spec.Template.Spec.Containers {
				if strings.Contains(container.Image, p.Task.ContainerName) {
					p.Log.Infof("statefulSets find match container.name:%s", container.Name)
					p.Task.ReplaceResources = append(p.Task.ReplaceResources, task.Resource{
						Kind:      setting.StatefulSet,
						Container: container.Name,
						Origin:    container.Image,
						Name:      sts.Name,
					})
				}
			}
		}

		productInfo, err = p.getProductInfo(ctx, &EnvArgs{EnvName: p.Task.EnvName, ProductName: p.Task.ProductName})
		if err != nil {
			err = errors.WithMessagef(
				err,
				"failed to get product %s/%s",
				p.Task.Namespace, p.Task.ServiceName)
			return
		}
		renderInfo, err = p.getRenderSet(ctx, productInfo.Render.Name, productInfo.Render.Revision)
		if err != nil {
			err = errors.WithMessagef(
				err,
				"failed to get getRenderSet %s/%d",
				productInfo.Render.Name, productInfo.Render.Revision)
			return
		}

		var targetContainer *types.Container
		for _, serviceGroup := range productInfo.Services {
			for _, service := range serviceGroup {
				if service.ServiceName == p.Task.ServiceName {
					for _, container := range service.Containers {
						log.Infof("checking container serviceName %s containerName %s", service.ServiceName, container.Name)
						if container.Name == p.Task.ContainerName {
							targetContainer = container
						}
					}
				}
			}
		}

		log.Infof("target container name %s target service name %s imageName %s", p.Task.ContainerName, p.Task.ServiceName, p.Task.Image)

		if targetContainer == nil {
			err = fmt.Errorf("failed to find container %s from service %s", p.Task.ContainerName, p.Task.ServiceName)
			return
		}

		for _, chartInfo := range renderInfo.ChartInfos {
			chartInfoMap[chartInfo.ServiceName] = chartInfo
		}

		if renderChart, isExist = chartInfoMap[p.Task.ServiceName]; isExist {
			yamlValuesByte, err = yaml.YAMLToJSON([]byte(renderChart.ValuesYaml))
			if err != nil {
				err = errors.WithMessagef(
					err,
					"failed to YAMLToJSON %s/%s",
					p.Task.Namespace, p.Task.ServiceName)
				return
			}

			var currentValuesYamlMap map[string]interface{}
			if err = json.Unmarshal(yamlValuesByte, &currentValuesYamlMap); err != nil {
				err = errors.WithMessagef(
					err,
					"failed to Unmarshal values.yaml %s/%s",
					p.Task.Namespace, p.Task.ServiceName)
				return
			}

			log.Infof("current values map:%s", string(yamlValuesByte))

			replaceValuesYaml, err = p.replaceImage(targetContainer.ImagePath, currentValuesYamlMap, p.Task.Image)
			if err != nil {
				log.Error("failed to replace image: %s, err %s", p.Task.Image, err.Error())
				return
			}

			log.Infof("replaced yaml is %s", replaceValuesYaml)

			if replaceValuesYaml != "" {
				helmClient, err = helmtool.NewClientFromRestConf(p.restConfig, p.Task.Namespace)
				if err != nil {
					err = errors.WithMessagef(
						err,
						"failed to create helm client %s/%s",
						p.Task.Namespace, p.Task.ServiceName)
					return
				}

				path, errDownload := p.downloadService(pipelineTask.ProductName, p.Task.ServiceName, pipelineTask.StorageURI)
				if errDownload != nil {
					err = errors.WithMessagef(
						errDownload,
						"failed to download service %s/%s",
						p.Task.Namespace, p.Task.ServiceName)
					return
				}
				chartPath, errPath := fs.RelativeToCurrentPath(path)
				if errPath != nil {
					err = errors.WithMessagef(
						errPath,
						"failed to get relative path %s",
						path,
					)
					return
				}

				mergedValuesYaml, errMerge := helmtool.MergeOverrideValues(replaceValuesYaml, renderChart.GetOverrideYaml(), renderChart.OverrideValues)
				if errMerge != nil {
					err = errors.WithMessagef(
						errMerge,
						"failed to merge override values %s",
						renderChart.OverrideValues,
					)
					return
				}

				chartSpec := helmclient.ChartSpec{
					ReleaseName: util.GeneHelmReleaseName(p.Task.Namespace, p.Task.ServiceName),
					ChartName:   chartPath,
					Namespace:   p.Task.Namespace,
					ReuseValues: true,
					Version:     renderChart.ChartVersion,
					ValuesYaml:  mergedValuesYaml,
					SkipCRDs:    false,
					UpgradeCRDs: true,
					Timeout:     time.Second * DeployTimeout,
				}

				log.Infof("final merged data is %s", mergedValuesYaml)

				if _, err = helmClient.InstallOrUpgradeChart(context.TODO(), &chartSpec); err != nil {
					err = errors.WithMessagef(
						err,
						"failed to Install helm chart %s/%s",
						p.Task.Namespace, p.Task.ServiceName)
					return
				}

				//替换环境变量中的chartInfos
				for _, chartInfo := range renderInfo.ChartInfos {
					if chartInfo.ServiceName == p.Task.ServiceName {
						chartInfo.ValuesYaml = replaceValuesYaml
						break
					}
				}
				_ = p.updateRenderSet(ctx, &types.RenderSet{
					Name:       renderInfo.Name,
					Revision:   renderInfo.Revision,
					ChartInfos: renderInfo.ChartInfos,
				})

			}
			return
		}
		err = errors.WithMessagef(
			err,
			"failed to update container image in %s/%s，not find",
			p.Task.Namespace, p.Task.ServiceName)
	}
}

func (p *DeployTaskPlugin) getProductInfo(ctx context.Context, args *EnvArgs) (*types.Product, error) {
	url := fmt.Sprintf("/api/environment/environments/%s/productInfo", args.ProductName)

	prod := &types.Product{}
	_, err := p.httpClient.Get(url, httpclient.SetResult(prod), httpclient.SetQueryParam("envName", args.EnvName))
	if err != nil {
		return nil, err
	}
	return prod, nil
}

func (p *DeployTaskPlugin) getService(ctx context.Context, name, serviceType, productName string) (*types.ServiceTmpl, error) {
	url := fmt.Sprintf("/api/service/services/%s/%s", name, serviceType)

	s := &types.ServiceTmpl{}
	_, err := p.httpClient.Get(url, httpclient.SetResult(s), httpclient.SetQueryParam("productName", productName))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (p *DeployTaskPlugin) downloadService(productName, serviceName, storageURI string) (string, error) {
	logger := p.Log

	base := configbase.LocalServicePath(productName, serviceName)
	s3Storage, err := s3.NewS3StorageFromEncryptedURI(storageURI)
	if err != nil {
		return "", err
	}

	tarball := fmt.Sprintf("%s.tar.gz", serviceName)
	tarFilePath := filepath.Join(base, tarball)
	s3Storage.Subfolder = filepath.Join(s3Storage.Subfolder, configbase.ObjectStorageServicePath(productName, serviceName))
	forcedPathStyle := true
	if s3Storage.Provider == setting.ProviderSourceAli {
		forcedPathStyle = false
	}
	client, err := s3tool.NewClient(s3Storage.Endpoint, s3Storage.Ak, s3Storage.Sk, s3Storage.Insecure, forcedPathStyle)
	if err != nil {
		p.Log.Errorf("failed to create s3 client, err: %+v", err)
		return "", err
	}
	if err = client.Download(s3Storage.Bucket, s3Storage.GetObjectPath(tarball), tarFilePath); err != nil {
		logger.Errorf("Failed to download file from s3, err: %s", err)
		return "", err
	}

	return tarFilePath, nil
}

func (p *DeployTaskPlugin) getRenderSet(ctx context.Context, name string, revision int64) (*types.RenderSet, error) {
	url := fmt.Sprintf("/api/project/renders/render/%s/revision/%d", name, revision)

	rs := &types.RenderSet{}
	_, err := p.httpClient.Get(url, httpclient.SetResult(rs))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (p *DeployTaskPlugin) updateRenderSet(ctx context.Context, args *types.RenderSet) error {
	url := "/api/project/renders"

	_, err := p.httpClient.Put(url, httpclient.SetBody(args))

	return err
}

func getValidMatchData(spec *types.ImagePathSpec) map[string]string {
	ret := make(map[string]string)
	if spec.Repo != "" {
		ret[setting.PathSearchComponentRepo] = spec.Repo
	}
	if spec.Image != "" {
		ret[setting.PathSearchComponentImage] = spec.Image
	}
	if spec.Tag != "" {
		ret[setting.PathSearchComponentTag] = spec.Tag
	}
	return ret
}

// replace image defines in yaml by new version
func (p *DeployTaskPlugin) replaceImage(imagePathSpec *types.ImagePathSpec, valuesMap map[string]interface{}, imageUri string) (string, error) {
	if imagePathSpec == nil {
		return "", errors.New("image path parse info is nil")
	}
	getValidMatchData := getValidMatchData(imagePathSpec)
	log.Info("image path spec is %v", getValidMatchData)
	replaceValuesMap, err := assignImageData(imageUri, getValidMatchData)
	log.Info("data after assign is %s err is %v", replaceValuesMap, err)
	if err != nil {
		return "", err
	}
	replaceMap := util.ReplaceMapValue(valuesMap, replaceValuesMap)
	replacedValuesYaml, err := util.JSONToYaml(replaceMap)
	if err != nil {
		return "", err
	}
	return replacedValuesYaml, nil
}

// assign image url data into match data
// matchData: image=>absolute-path repo=>absolute-path tag=>absolute-path
// return: absolute-image-path=>image-value  absolute-repo-path=>repo-value absolute-tag-path=>tag-value
func assignImageData(imageUrl string, matchData map[string]string) (map[string]interface{}, error) {
	ret := make(map[string]interface{})
	// total image url assigned into one single value
	if len(matchData) == 1 {
		for _, v := range matchData {
			ret[v] = imageUrl
		}
		return ret, nil
	}

	resolvedImageUrl := resolveImageUrl(imageUrl)
	log.Info("the resolved image is %v", resolvedImageUrl)

	// image url assigned into repo/image+tag
	if len(matchData) == 3 {
		ret[matchData[setting.PathSearchComponentRepo]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
		ret[matchData[setting.PathSearchComponentImage]] = resolvedImageUrl[setting.PathSearchComponentImage]
		ret[matchData[setting.PathSearchComponentTag]] = resolvedImageUrl[setting.PathSearchComponentTag]
		return ret, nil
	}

	if len(matchData) == 2 {
		// image url assigned into repo/image + tag
		if tagPath, ok := matchData[setting.PathSearchComponentTag]; ok {
			ret[tagPath] = resolvedImageUrl[setting.PathSearchComponentTag]
			for k, imagePath := range matchData {
				if k == setting.PathSearchComponentTag {
					continue
				}
				ret[imagePath] = fmt.Sprintf("%s%s", resolvedImageUrl[setting.PathSearchComponentRepo], resolvedImageUrl[setting.PathSearchComponentImage])
				break
			}
			return ret, nil
		} else {
			// image url assigned into repo + image(tag)
			ret[matchData[setting.PathSearchComponentRepo]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
			ret[matchData[setting.PathSearchComponentImage]] = fmt.Sprintf("%s:%s", resolvedImageUrl[setting.PathSearchComponentImage], resolvedImageUrl[setting.PathSearchComponentTag])
			return ret, nil
		}
	}

	return nil, fmt.Errorf("match data illegal, expect length: 1-3, actual length: %d", len(matchData))
}

// parse image url to map: repo=>xxx/xx/xx image=>xx tag=>xxx
func resolveImageUrl(imageUrl string) map[string]string {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageUrl)
	result := make(map[string]string)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			result[exNames[i]] = matchedStr
		}
	}
	return result
}

// Wait ...
func (p *DeployTaskPlugin) Wait(ctx context.Context) {
	// skip waiting for reset image task
	if p.Task.SkipWaiting {
		p.Task.TaskStatus = config.StatusPassed
		return
	}

	timeout := time.After(time.Duration(p.TaskTimeout()) * time.Second)

	selector := labels.Set{setting.ProductLabel: p.Task.ProductName, setting.ServiceLabel: p.Task.ServiceName}.AsSelector()

	for {
		select {
		case <-ctx.Done():
			p.Task.TaskStatus = config.StatusCancelled
			return

		case <-timeout:
			p.Task.TaskStatus = config.StatusTimeout

			pods, err := getter.ListPods(p.Task.Namespace, selector, p.kubeClient)
			if err != nil {
				p.Task.Error = err.Error()
				return
			}

			var msg []string
			for _, pod := range pods {
				podResource := wrapper.Pod(pod).Resource()
				if podResource.Status != setting.StatusRunning && podResource.Status != setting.StatusSucceeded {
					for _, cs := range podResource.ContainerStatuses {
						// message为空不认为是错误状态，有可能还在waiting
						if cs.Message != "" {
							msg = append(msg, fmt.Sprintf("Status: %s, Reason: %s, Message: %s", cs.Status, cs.Reason, cs.Message))
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
