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

package wrapper

import (
	"github.com/openkruise/kruise-api/apps/v1alpha1"
	corev1 "k8s.io/api/core/v1"

	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/kube/resource"
	"github.com/koderover/zadig/v2/pkg/util"
)

// cloneset is the wrapper for v1alpha1.CloneSet type.
type cloneset struct {
	*v1alpha1.CloneSet
}

// CloneSet creates a wrapper for the given CloneSet object.
func CloneSet(c *v1alpha1.CloneSet) *cloneset {
	if c == nil {
		return nil
	}
	return &cloneset{
		CloneSet: c,
	}
}

// Unwrap returns the original v1alpha1.CloneSet object.
func (c *cloneset) Unwrap() *v1alpha1.CloneSet {
	return c.CloneSet
}

// Ready checks if the CloneSet is fully ready, which means:
// 1. Current replicas match desired replicas.
// 2. All replicas are ready.
// 3. All replicas are updated with the latest template.
func (c *cloneset) Ready() bool {
	if c.Spec.Replicas == nil {
		return false // Return false if desired replicas are not defined.
	}
	return c.Status.Replicas == *c.Spec.Replicas &&
		c.Status.ReadyReplicas == c.Status.Replicas &&
		c.Status.UpdatedReplicas == c.Status.Replicas
}

// WorkloadResource creates a Workload representation for the CloneSet.
func (c *cloneset) WorkloadResource(pods []*corev1.Pod) *resource.Workload {
	wl := &resource.Workload{
		Name:     c.Name,
		Type:     setting.CloneSet,
		Replicas: *c.Spec.Replicas,
		Pods:     make([]*resource.Pod, 0, len(pods)),
	}

	// Add container images to the workload.
	for _, container := range c.Spec.Template.Spec.Containers {
		wl.Images = append(wl.Images, resource.ContainerImage{
			Name:      container.Name,
			Image:     container.Image,
			ImageName: util.ExtractImageName(container.Image),
		})
	}

	// Add pod resources to the workload.
	for _, pod := range pods {
		wl.Pods = append(wl.Pods, Pod(pod).Resource())
	}

	return wl
}

// Images returns the names of the containers in the CloneSet.
func (c *cloneset) Images() (images []string) {
	for _, container := range c.Spec.Template.Spec.Containers {
		images = append(images, container.Name)
	}
	return
}

// ImageInfos returns the images used by the containers in the CloneSet.
func (c *cloneset) ImageInfos() (images []string) {
	for _, container := range c.Spec.Template.Spec.Containers {
		images = append(images, container.Image)
	}
	return
}

// GetKind returns the kind of the CloneSet object.
func (c *cloneset) GetKind() string {
	return c.Kind
}

// GetContainers returns the container details of the CloneSet.
func (c *cloneset) GetContainers() []*resource.ContainerImage {
	containers := make([]*resource.ContainerImage, 0, len(c.Spec.Template.Spec.Containers))
	for _, container := range c.Spec.Template.Spec.Containers {
		containers = append(containers, &resource.ContainerImage{
			Name:      container.Name,
			Image:     container.Image,
			ImageName: util.ExtractImageName(container.Image),
		})
	}
	return containers
}
