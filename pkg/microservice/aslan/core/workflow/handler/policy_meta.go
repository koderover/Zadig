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

package handler

import (
	_ "embed"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/koderover/zadig/pkg/shared/client/policy"
	"github.com/koderover/zadig/pkg/tool/log"
)

//go:embed policy.yaml
var workflowPolicyMeta []byte

//go:embed system-policies.yaml
var systemPolicyMeta []byte

func (*Router) Policies() []*policy.PolicyMeta {
	res := &policy.PolicyMeta{}
	err := yaml.Unmarshal(workflowPolicyMeta, res)
	if err != nil {
		// should not have happened here
		log.DPanic(err)
	}
	for i, meta := range res.Rules {
		tmpRules := []*policy.ActionRule{}
		for _, rule := range meta.Rules {
			if rule.ResourceType == "" {
				rule.ResourceType = res.Resource
			}
			if strings.Contains(rule.Endpoint, ":name") {
				idRegex := strings.ReplaceAll(rule.Endpoint, ":name", `([\w\W].*)`)
				idRegex = strings.ReplaceAll(idRegex, "?*", `[\w\W].*`)
				endpoint := strings.ReplaceAll(rule.Endpoint, ":name", "?*")
				rule.Endpoint = endpoint
				rule.IDRegex = idRegex
			}
			tmpRules = append(tmpRules, rule)
		}
		res.Rules[i].Rules = tmpRules
	}
	systemMetas := []*policy.PolicyMeta{}
	err = yaml.Unmarshal(systemPolicyMeta, &systemMetas)
	if err != nil {
		// should not have happened here
		log.DPanic(err)
	}
	systemMetas = append(systemMetas, res)

	return systemMetas
}
