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

package stepcontroller

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/types/step"
)

type powerShellCtl struct {
	step           *commonmodels.StepTask
	powerShellSpec *step.StepPowerShellSpec
	log            *zap.SugaredLogger
}

func NewPowerShellCtl(stepTask *commonmodels.StepTask, log *zap.SugaredLogger) (*powerShellCtl, error) {
	yamlString, err := yaml.Marshal(stepTask.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal shell spec error: %v", err)
	}
	powerShellSpec := &step.StepPowerShellSpec{}
	if err := yaml.Unmarshal(yamlString, &powerShellSpec); err != nil {
		return nil, fmt.Errorf("unmarshal shell spec error: %v", err)
	}
	stepTask.Spec = powerShellSpec
	return &powerShellCtl{powerShellSpec: powerShellSpec, log: log, step: stepTask}, nil
}

func (s *powerShellCtl) PreRun(ctx context.Context) error {
	if len(s.powerShellSpec.Scripts) > 0 {
		return nil
	}
	s.powerShellSpec.Scripts = strings.Split(replaceWrapLine(s.powerShellSpec.Script), "\n")
	s.step.Spec = s.powerShellSpec
	return nil
}

func (s *powerShellCtl) AfterRun(ctx context.Context) error {
	return nil
}
