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

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/setting"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
)

var ErrUnsupportedService = errors.New("unsupported workload type (only support Deployment/StatefulSet) or workload number is not 1")

func PatchWorkload(ctx context.Context, projectName, envName, serviceName, devImage string) (*types.WorkloadInfo, error) {
	project, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    projectName,
		EnvName: envName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query env %q of project %q: %s", envName, projectName, err)
	}
	if project.IsSleeping() {
		return nil, fmt.Errorf("environment is sleeping")
	}

	ns := project.Namespace
	clusterID := project.ClusterID

	kclient, err := kubeclient.GetKubeClient(config.HubServerAddress(), clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get kube client: %s", err)
	}

	selector := labels.Set{setting.ProductLabel: projectName, setting.ServiceLabel: serviceName}.AsSelector()
	workloadType, err := getWorkloadType(ctx, kclient, selector, ns)
	if err != nil {
		return nil, fmt.Errorf("failed to get workload type: %s", err)
	}
	log.Infof("workloadType: %s", workloadType)

	podName, err := patchWorkload(ctx, kclient, selector, ns, devImage, workloadType)

	return &types.WorkloadInfo{
		PodName:      podName,
		PodNamespace: ns,
	}, err
}

func RecoverWorkload(ctx context.Context, projectName, envName, serviceName string) error {
	project, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    projectName,
		EnvName: envName,
	})
	if err != nil {
		return fmt.Errorf("failed to query env %q of project %q: %s", envName, projectName, err)
	}
	if project.IsSleeping() {
		return fmt.Errorf("environment is sleeping")
	}

	ns := project.Namespace
	clusterID := project.ClusterID

	kclient, err := kubeclient.GetKubeClient(config.HubServerAddress(), clusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}

	selector := labels.Set{setting.ProductLabel: projectName, setting.ServiceLabel: serviceName}.AsSelector()
	workloadType, err := getWorkloadType(ctx, kclient, selector, ns)
	if err != nil {
		return fmt.Errorf("failed to get workload type: %s", err)
	}
	log.Infof("workloadType: %s", workloadType)

	return recoverWorkload(ctx, kclient, selector, ns, workloadType)
}

func getWorkloadType(ctx context.Context, kclient client.Client, selector labels.Selector, ns string) (types.WorkloadType, error) {
	res := []types.WorkloadType{}

	deploymentList := &appsv1.DeploymentList{}
	err := kclient.List(ctx, deploymentList, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list deployment: %s", err)
	}
	if len(deploymentList.Items) == 1 {
		res = append(res, types.DeploymentWorkload)
	}

	stsList := &appsv1.StatefulSetList{}
	err = kclient.List(ctx, stsList, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list statefulset: %s", err)
	}
	if len(stsList.Items) == 1 {
		res = append(res, types.StatefulSetWorkload)
	}

	if len(res) != 1 {
		return "", ErrUnsupportedService
	}

	return res[0], nil
}

func patchWorkload(ctx context.Context, kclient client.Client, selector labels.Selector, ns, devImage string, workloadType types.WorkloadType) (string, error) {
	switch workloadType {
	case types.DeploymentWorkload:
		err := patchDeployment(ctx, kclient, selector, ns, devImage)
		if err != nil {
			return "", err
		}
	case types.StatefulSetWorkload:
		err := patchStatefulSet(ctx, kclient, selector, ns, devImage)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unsupported workload type: %s", workloadType)
	}

	err := waitPodReady(ctx, kclient, selector, ns)
	if err != nil {
		return "", err
	}

	pod, err := getPod(ctx, kclient, selector, ns)
	if err != nil {
		return "", fmt.Errorf("failed to get pod: %s", err)
	}

	return pod.Name, nil
}

func patchDeployment(ctx context.Context, kclient client.Client, selector labels.Selector, ns, devImage string) error {
	objs := &appsv1.DeploymentList{}
	err := kclient.List(ctx, objs, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %s", err)
	}
	if len(objs.Items) != 1 {
		return fmt.Errorf("invalid number of deployments: %d", len(objs.Items))
	}

	deployment := objs.Items[0]

	spec, err := json.Marshal(deployment.Spec)
	if err != nil {
		return fmt.Errorf("failed to marshal deployment spec: %s", err)
	}
	if deployment.Annotations == nil {
		deployment.Annotations = map[string]string{}
	}
	deployment.Annotations[types.OriginSpec] = string(spec)

	deployment.Spec.Replicas = util.GetInt32Pointer(1)
	deployment.Spec.Template.Spec.Volumes = []corev1.Volume{
		corev1.Volume{
			Name: "syncthing",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	deployment.Spec.Template.Spec.Containers = []corev1.Container{
		corev1.Container{
			Name:            types.IDEContainerNameDev,
			Image:           devImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-c", "tail -f /dev/null"},
			WorkingDir:      types.DevmodeWorkDir,
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				corev1.VolumeMount{
					Name:      "syncthing",
					MountPath: types.DevmodeWorkDir,
				},
			},
		},
		corev1.Container{
			Name:            types.IDEContainerNameSidecar,
			Image:           types.IDESidecarImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-c", "tail -f /dev/null"},
			WorkingDir:      types.DevmodeWorkDir,
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300Mi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				corev1.VolumeMount{
					Name:      "syncthing",
					MountPath: types.DevmodeWorkDir,
				},
			},
		},
	}

	return kclient.Update(ctx, &deployment)
}

func waitPodReady(ctx context.Context, kclient client.Client, selector labels.Selector, ns string) error {
	return wait.PollImmediate(2*time.Second, 3*time.Minute, func() (done bool, err error) {
		objs := &corev1.PodList{}
		err = kclient.List(ctx, objs, &client.ListOptions{
			Namespace:     ns,
			LabelSelector: selector,
		})
		if err != nil {
			log.Warnf("failed to list pod: %s", err)
			return false, nil
		}

		for _, pod := range objs.Items {
			if len(pod.Spec.Containers) != 2 {
				continue
			}

			found := true
			for _, container := range pod.Spec.Containers {
				if container.Name == types.IDEContainerNameDev || container.Name == types.IDEContainerNameSidecar {
					continue
				}

				found = false
			}

			if found && pod.Status.Phase == corev1.PodRunning {
				return true, nil
			}
		}

		return false, nil
	})
}

func patchStatefulSet(ctx context.Context, kclient client.Client, selector labels.Selector, ns, devImage string) error {
	objs := &appsv1.StatefulSetList{}
	err := kclient.List(ctx, objs, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return fmt.Errorf("failed to list statefulsets: %s", err)
	}
	if len(objs.Items) != 1 {
		return fmt.Errorf("invalid number of statefulsets: %d", len(objs.Items))
	}

	sts := objs.Items[0]

	spec, err := json.Marshal(sts.Spec)
	if err != nil {
		return fmt.Errorf("failed to marshal statefulset spec: %s", err)
	}
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[types.OriginSpec] = string(spec)

	sts.Spec.Replicas = util.GetInt32Pointer(1)
	sts.Spec.Template.Spec.Volumes = []corev1.Volume{
		corev1.Volume{
			Name: "syncthing",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{
		corev1.Container{
			Name:            types.IDEContainerNameDev,
			Image:           devImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-c", "tail -f /dev/null"},
			WorkingDir:      types.DevmodeWorkDir,
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				corev1.VolumeMount{
					Name:      "syncthing",
					MountPath: types.DevmodeWorkDir,
				},
			},
		},
		corev1.Container{
			Name:            types.IDEContainerNameSidecar,
			Image:           types.IDESidecarImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-c", "tail -f /dev/null"},
			WorkingDir:      types.DevmodeWorkDir,
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("300m"),
					corev1.ResourceMemory: resource.MustParse("300Mi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				corev1.VolumeMount{
					Name:      "syncthing",
					MountPath: types.DevmodeWorkDir,
				},
			},
		},
	}

	return kclient.Update(ctx, &sts)
}

func recoverWorkload(ctx context.Context, kclient client.Client, selector labels.Selector, ns string, workloadType types.WorkloadType) error {
	switch workloadType {
	case types.DeploymentWorkload:
		err := recoverDeployment(ctx, kclient, selector, ns)
		if err != nil {
			return err
		}
	case types.StatefulSetWorkload:
		err := recoverStatefulSet(ctx, kclient, selector, ns)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported workload type: %s", workloadType)
	}

	return waitPodReady(ctx, kclient, selector, ns)
}

func recoverDeployment(ctx context.Context, kclient client.Client, selector labels.Selector, ns string) error {
	objs := &appsv1.DeploymentList{}
	err := kclient.List(ctx, objs, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %s", err)
	}
	if len(objs.Items) != 1 {
		return fmt.Errorf("invalid number of deployments: %d", len(objs.Items))
	}

	deployment := objs.Items[0]
	if deployment.Annotations == nil || deployment.Annotations[types.OriginSpec] == "" {
		return fmt.Errorf("cant' find original workload spec from annotation %q", types.OriginSpec)
	}

	var spec appsv1.DeploymentSpec
	if err := json.Unmarshal([]byte(deployment.Annotations[types.OriginSpec]), &spec); err != nil {
		return fmt.Errorf("failed to unmarshal original workload spec from annotation: %s", err)
	}

	deployment.Spec = spec
	delete(deployment.Annotations, types.OriginSpec)

	return kclient.Update(ctx, &deployment)
}

func recoverStatefulSet(ctx context.Context, kclient client.Client, selector labels.Selector, ns string) error {
	objs := &appsv1.StatefulSetList{}
	err := kclient.List(ctx, objs, &client.ListOptions{
		Namespace:     ns,
		LabelSelector: selector,
	})
	if err != nil {
		return fmt.Errorf("failed to list statefulsets: %s", err)
	}
	if len(objs.Items) != 1 {
		return fmt.Errorf("invalid number of statefulsets: %d", len(objs.Items))
	}

	sts := objs.Items[0]
	if sts.Annotations == nil || sts.Annotations[types.OriginSpec] == "" {
		return fmt.Errorf("cant' find original workload spec from annotation %q", types.OriginSpec)
	}

	var spec appsv1.StatefulSetSpec
	if err := json.Unmarshal([]byte(sts.Annotations[types.OriginSpec]), &spec); err != nil {
		return fmt.Errorf("failed to unmarshal original workload spec from annotation: %s", err)
	}

	sts.Spec = spec
	delete(sts.Annotations, types.OriginSpec)

	return kclient.Update(ctx, &sts)
}

func getPod(ctx context.Context, kclient client.Client, selector labels.Selector, ns string) (*corev1.Pod, error) {
	var targetPod *corev1.Pod
	err := wait.PollImmediate(2*time.Second, 6*time.Second, func() (done bool, err error) {
		objs := &corev1.PodList{}
		err = kclient.List(ctx, objs, &client.ListOptions{
			Namespace:     ns,
			LabelSelector: selector,
		})
		if err != nil {
			log.Warnf("failed to list pod: %s", err)
			return false, nil
		}

		for _, pod := range objs.Items {
			if len(pod.Spec.Containers) != 2 {
				continue
			}

			found := true
			for _, container := range pod.Spec.Containers {
				if container.Name == types.IDEContainerNameDev || container.Name == types.IDEContainerNameSidecar {
					continue
				}

				found = false
			}

			if found {
				targetPod = &pod
				return true, nil
			}
		}

		return false, nil
	})

	if targetPod == nil {
		return nil, fmt.Errorf("failed to find target pod: %s", err)
	}

	return targetPod, nil
}
