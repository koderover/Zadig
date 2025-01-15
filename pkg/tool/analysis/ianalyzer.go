/*
Copyright 2023 The K8sGPT Authors.
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

// Some parts of this file have been modified to make it functional in Zadig

package analysis

import (
	"context"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"github.com/koderover/zadig/v2/pkg/tool/llm"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	appsv1 "k8s.io/api/apps/v1"
	autov1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	networkv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
)

var (
	AnalyzerErrorsMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "analyzer_errors",
		Help: "Number of errors detected by analyzer",
	}, []string{"analyzer_name", "object_name", "namespace"})
)

var coreAnalyzerMap = map[string]IAnalyzer{
	"Pod":                   PodAnalyzer{},
	"Deployment":            DeploymentAnalyzer{},
	"ReplicaSet":            ReplicaSetAnalyzer{},
	"PersistentVolumeClaim": PvcAnalyzer{},
	"Service":               ServiceAnalyzer{},
	"Ingress":               IngressAnalyzer{},
	"StatefulSet":           StatefulSetAnalyzer{},
	"CronJob":               CronJobAnalyzer{},
	"Node":                  NodeAnalyzer{},
}

var additionalAnalyzerMap = map[string]IAnalyzer{
	"HorizontalPodAutoScaler": HpaAnalyzer{},
	"PodDisruptionBudget":     PdbAnalyzer{},
	"NetworkPolicy":           NetworkPolicyAnalyzer{},
}

type IAnalyzer interface {
	Analyze(analysis Analyzer) ([]Result, error)
}

type Analyzer struct {
	Client        *Client
	Context       context.Context
	Namespace     string
	AIClient      llm.ILLM
	PreAnalysis   map[string]PreAnalysis
	Results       []Result
	OpenapiSchema *openapi_v2.Document
}

type PreAnalysis struct {
	Pod                      v1.Pod
	FailureDetails           []Failure
	Deployment               appsv1.Deployment
	ReplicaSet               appsv1.ReplicaSet
	PersistentVolumeClaim    v1.PersistentVolumeClaim
	Endpoint                 v1.Endpoints
	Ingress                  networkv1.Ingress
	HorizontalPodAutoscalers autov1.HorizontalPodAutoscaler
	PodDisruptionBudget      policyv1.PodDisruptionBudget
	StatefulSet              appsv1.StatefulSet
	NetworkPolicy            networkv1.NetworkPolicy
	Node                     v1.Node
}

type Result struct {
	Kind         string    `json:"kind"`
	Name         string    `json:"name"`
	Error        []Failure `json:"error"`
	Details      string    `json:"details"`
	ParentObject string    `json:"parentObject"`
}

type Failure struct {
	Text          string
	KubernetesDoc string
	Sensitive     []Sensitive
}

type Sensitive struct {
	Unmasked string
	Masked   string
}

func ListFilters() ([]string, []string, []string) {
	coreKeys := make([]string, 0, len(coreAnalyzerMap))
	for k := range coreAnalyzerMap {
		coreKeys = append(coreKeys, k)
	}

	additionalKeys := make([]string, 0, len(additionalAnalyzerMap))
	for k := range additionalAnalyzerMap {
		additionalKeys = append(additionalKeys, k)
	}

	return coreKeys, additionalKeys, []string{}
}

func GetAnalyzerMap() (map[string]IAnalyzer, map[string]IAnalyzer) {

	coreAnalyzer := make(map[string]IAnalyzer)
	mergedAnalyzerMap := make(map[string]IAnalyzer)

	// add core analyzer
	for key, value := range coreAnalyzerMap {
		coreAnalyzer[key] = value
		mergedAnalyzerMap[key] = value
	}

	// add additional analyzer
	for key, value := range additionalAnalyzerMap {
		mergedAnalyzerMap[key] = value
	}

	return coreAnalyzer, mergedAnalyzerMap
}
