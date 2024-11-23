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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	kubeclient "github.com/koderover/zadig/v2/pkg/shared/kube/client"
)

const ZadigDebugContainerName = "zadig-debug"
const K8sBetaVersionForEphemeralContainer = "v1.23"

func PatchDebugContainer(ctx context.Context, projectName, envName, podName, debugImage string, production bool) error {
	prod, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:    projectName,
		EnvName: envName,
		Production: &production,
	})
	if err != nil {
		return fmt.Errorf("failed to query env %q in project %q: %s", envName, projectName, err)
	}

	clusterID := prod.ClusterID
	ns := prod.Namespace

	kclient, err := kubeclient.GetKubeClient(config.HubServerAddress(), clusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}

	clientset, err := kubeclient.GetKubeClientSet(config.HubServerAddress(), clusterID)
	if err != nil {
		return fmt.Errorf("failed to get kube clientset: %s", err)
	}

	restConfig, err := kubeclient.GetRESTConfig(config.HubServerAddress(), clusterID)
	if err != nil {
		return fmt.Errorf("failed to get rest config: %s", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to get discovery client: %s", err)
	}

	pod := &corev1.Pod{}
	err = kclient.Get(ctx, client.ObjectKey{
		Name:      podName,
		Namespace: ns,
	}, pod)
	if err != nil {
		return fmt.Errorf("failed to get pod %q in ns %q: %s", podName, ns, err)
	}

	k8sVersion, err := checkK8sVersion(discoveryClient)
	if err != nil {
		return fmt.Errorf("failed to check K8s version: %s", err)
	}

	debugContainer := genDebugContainer(debugImage)
	if version.CompareKubeAwareVersionStrings(K8sBetaVersionForEphemeralContainer, k8sVersion) < 0 {
		_, _, err = debugByEphemeralContainerLegacy(ctx, clientset.CoreV1(), pod, debugContainer)
	} else {
		_, _, err = debugByEphemeralContainer(ctx, clientset.CoreV1(), pod, debugContainer)
	}

	return err
}

func checkK8sVersion(client *discovery.DiscoveryClient) (string, error) {
	serverInfo, err := client.ServerVersion()
	if err != nil {
		return "", err
	}

	// Examples: v1.23.3, v1.20.6-tke.16
	items := strings.Split(serverInfo.GitVersion, ".")
	if len(items) < 2 {
		return "", fmt.Errorf("invalid server version format %q", serverInfo.GitVersion)
	}

	return fmt.Sprintf("%s.%s", items[0], items[1]), nil
}

func genDebugContainer(imageName string) *corev1.EphemeralContainer {
	return &corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:                     ZadigDebugContainerName,
			Image:                    imageName,
			Command:                  []string{"tail", "-f", "/dev/null"},
			ImagePullPolicy:          corev1.PullIfNotPresent,
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		},
	}
}

func debugByEphemeralContainerLegacy(ctx context.Context, podClient corev1client.CoreV1Interface, pod *corev1.Pod,
	debugContainer *corev1.EphemeralContainer) (*corev1.Pod, string, error) {

	patch, err := json.Marshal([]map[string]interface{}{{
		"op":    "add",
		"path":  "/ephemeralContainers/-",
		"value": debugContainer,
	}})
	if err != nil {
		return nil, "", fmt.Errorf("error creating JSON 6902 patch for old /ephemeralcontainers API: %s", err)
	}

	result := podClient.RESTClient().Patch(types.JSONPatchType).
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("ephemeralcontainers").
		Body(patch).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, "", err
	}

	newPod, err := podClient.Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}

	return newPod, debugContainer.Name, nil
}

func debugByEphemeralContainer(ctx context.Context, podClient corev1client.CoreV1Interface, pod *corev1.Pod,
	debugContainer *corev1.EphemeralContainer) (*corev1.Pod, string, error) {
	podJS, err := json.Marshal(pod)
	if err != nil {
		return nil, "", fmt.Errorf("error creating JSON for pod: %v", err)
	}

	debugPod := pod.DeepCopy()
	debugPod.Spec.EphemeralContainers = append(debugPod.Spec.EphemeralContainers, *debugContainer)
	debugJS, err := json.Marshal(debugPod)
	if err != nil {
		return nil, "", fmt.Errorf("error creating JSON for debug container: %v", err)
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(podJS, debugJS, pod)
	if err != nil {
		return nil, "", fmt.Errorf("error creating patch to add debug container: %v", err)
	}

	pods := podClient.Pods(pod.Namespace)
	result, err := pods.Patch(ctx, pod.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "ephemeralcontainers")
	if err != nil {
		return nil, "", fmt.Errorf("failed to patch: %s", err)
	}

	return result, debugContainer.Name, nil
}
