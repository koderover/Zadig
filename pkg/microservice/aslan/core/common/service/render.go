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
	"encoding/json"
	"reflect"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

type RepoConfig struct {
	CodehostID  int      `json:"codehostID,omitempty"`
	Owner       string   `json:"owner,omitempty"`
	Namespace   string   `json:"namespace,omitempty"`
	Repo        string   `json:"repo,omitempty"`
	Branch      string   `json:"branch,omitempty"`
	ValuesPaths []string `json:"valuesPaths,omitempty"`
}

type KVPair struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type ValuesDataArgs struct {
	YamlSource    string      `json:"yamlSource"`
	SourceID      string      `json:"source_id"`
	AutoSync      bool        `json:"autoSync"`
	GitRepoConfig *RepoConfig `json:"gitRepoConfig"`
}

type HelmSvcRenderArg struct {
	EnvName        string                     `json:"envName"`
	ServiceName    string                     `json:"serviceName"`
	IsChartDeploy  bool                       `json:"is_chart_deploy"`
	ReleaseName    string                     `json:"release_name"`
	ChartRepo      string                     `json:"chart_repo"`
	ChartName      string                     `json:"chart_name"`
	ChartVersion   string                     `json:"chartVersion"`
	OverrideValues []*KVPair                  `json:"overrideValues"`
	OverrideYaml   string                     `json:"overrideYaml"`
	ValuesData     *ValuesDataArgs            `json:"valuesData"`
	YamlData       *templatemodels.CustomYaml `json:"yaml_data"`
	VariableYaml   string                     `json:"variable_yaml"`
	DeployStrategy string                     `json:"deploy_strategy"` // New since 1.16.0, used to determine if the service will be installed
}

type K8sSvcRenderArg struct {
	EnvName            string                          `json:"env_name,omitempty"`
	ServiceName        string                          `json:"service_name,omitempty"`
	VariableYaml       string                          `json:"variable_yaml"`
	VariableKVs        []*commontypes.RenderVariableKV `json:"variable_kvs"`
	LatestVariableYaml string                          `json:"latest_variable_yaml"`
	LatestVariableKVs  []*commontypes.RenderVariableKV `json:"latest_variable_kvs"`
	DeployStrategy     string                          `json:"deploy_strategy,omitempty"` // New since 1.16.0, used to determine if the service will be installed
}

type RenderChartDiffResult string

const (
	Different RenderChartDiffResult = "different"
	Same      RenderChartDiffResult = "same"
	LogicSame RenderChartDiffResult = "logicSame"
)

func (args *HelmSvcRenderArg) ToOverrideValueString() string {
	if len(args.OverrideValues) == 0 {
		return ""
	}
	bs, err := json.Marshal(args.OverrideValues)
	if err != nil {
		log.Errorf("override values json marshal error")
		return ""
	}
	return string(bs)
}

func (args *HelmSvcRenderArg) fromOverrideValueString(valueStr string) {
	if valueStr == "" {
		args.OverrideValues = nil
		return
	}

	args.OverrideValues = make([]*KVPair, 0)
	err := json.Unmarshal([]byte(valueStr), &args.OverrideValues)
	if err != nil {
		log.Errorf("decode override value fail, err: %s", err)
	}
}

func (args *HelmSvcRenderArg) toCustomValuesYaml() *templatemodels.CustomYaml {
	ret := &templatemodels.CustomYaml{
		YamlContent: args.OverrideYaml,
	}
	if args.ValuesData != nil {
		ret.Source = args.ValuesData.YamlSource
		ret.AutoSync = args.ValuesData.AutoSync
		if args.ValuesData.GitRepoConfig != nil {
			repoData := &models.CreateFromRepo{
				GitRepoConfig: &templatemodels.GitRepoConfig{
					CodehostID: args.ValuesData.GitRepoConfig.CodehostID,
					Owner:      args.ValuesData.GitRepoConfig.Owner,
					Namespace:  args.ValuesData.GitRepoConfig.Namespace,
					Repo:       args.ValuesData.GitRepoConfig.Repo,
					Branch:     args.ValuesData.GitRepoConfig.Branch,
				},
			}
			if len(args.ValuesData.GitRepoConfig.ValuesPaths) > 0 {
				repoData.LoadPath = args.ValuesData.GitRepoConfig.ValuesPaths[0]
			}
			ret.SourceDetail = repoData
			ret.Source = setting.SourceFromGitRepo
		}
	}
	return ret
}

func (args *HelmSvcRenderArg) fromCustomValueYaml(customValuesYaml *templatemodels.CustomYaml) {
	if customValuesYaml == nil {
		return
	}
	args.OverrideYaml = customValuesYaml.YamlContent
}

// FillRenderChartModel fill render chart model
func (args *HelmSvcRenderArg) FillRenderChartModel(chart *templatemodels.ServiceRender, version string) {
	chart.ServiceName = args.ServiceName
	chart.ChartVersion = version
	// chart.ValuesYaml = valuesYaml
	chart.OverrideValues = args.ToOverrideValueString()
	chart.OverrideYaml = args.toCustomValuesYaml()
}

// LoadFromRenderChartModel load from render chart model
func (args *HelmSvcRenderArg) LoadFromRenderChartModel(chart *templatemodels.ServiceRender) {
	args.ServiceName = chart.ServiceName
	args.ChartName = chart.ChartName
	args.ChartRepo = chart.ChartRepo
	args.ChartVersion = chart.ChartVersion
	args.ReleaseName = chart.ReleaseName
	args.IsChartDeploy = chart.IsHelmChartDeploy
	args.fromOverrideValueString(chart.OverrideValues)
	args.fromCustomValueYaml(chart.OverrideYaml)
}

func (args *HelmSvcRenderArg) GetUniqueKvMap() map[string]interface{} {
	uniqueKvs := make(map[string]interface{})
	for index := range args.OverrideValues {
		kv := args.OverrideValues[len(args.OverrideValues)-index-1]
		if _, ok := uniqueKvs[kv.Key]; ok {
			continue
		}
		uniqueKvs[kv.Key] = kv.Value
	}
	return uniqueKvs
}

// DiffValues generate diff values to override from two chart args
func (args *HelmSvcRenderArg) DiffValues(target *HelmSvcRenderArg) RenderChartDiffResult {
	argsUniqueKvs := args.GetUniqueKvMap()
	targetUniqueKvs := target.GetUniqueKvMap()
	if len(argsUniqueKvs) != len(targetUniqueKvs) || !reflect.DeepEqual(argsUniqueKvs, targetUniqueKvs) {
		return Different
	}
	// override yamls have the same text content
	if args.OverrideYaml == target.OverrideYaml {
		return Same
	}
	equal, _ := yamlutil.Equal(args.OverrideYaml, target.OverrideYaml)
	if equal {
		return LogicSame
	}
	return Different
}
