/*
 * Copyright 2025 The KodeRover Authors.
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

package migrate

import (
	"fmt"

	"github.com/koderover/zadig/v2/pkg/cli/upgradeassistant/internal/upgradepath"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/handler"
)

func init() {
	upgradepath.RegisterHandler("3.2.0", "3.2.1", V320ToV321)
	upgradepath.RegisterHandler("3.2.1", "3.2.0", V321ToV320)
}

func V321ToV330() error {
	ctx := handler.NewBackgroupContext()

	ctx.Logger.Infof("-------- start migrate release plan cronjob --------")
	err := migrateReleasePlanCron(ctx)
	if err != nil {
		err = fmt.Errorf("failed to migrate release plan cronjob, error: %w", err)
		ctx.Logger.Error(err)
		return err
	}

	ctx.Logger.Infof("-------- start migrate helm projects environment values auto sync --------")
	err = migrateHelmEnvValuesAutoSync(ctx)
	if err != nil {
		err = fmt.Errorf("failed to migrate helm projects environment values auto sync, error: %w", err)
		ctx.Logger.Error(err)
		return err
	}

	return nil
}

func V330ToV321() error {
	return nil
}

func migrateHelmEnvValuesAutoSync(ctx *handler.Context) error {
	allHelmProjects, err := template.NewProductColl().ListWithOption(&template.ProductListOpt{
		DeployType:    setting.HelmDeployType,
		BasicFacility: setting.BasicFacilityK8S,
	})
	if err != nil {
		return fmt.Errorf("failed to list all helm projects, error: %w", err)
	}

	for _, project := range allHelmProjects {
		envs, err := commonrepo.NewProductColl().List(&commonrepo.ProductListOptions{
			Name: project.ProductName,
		})
		if err != nil {
			return fmt.Errorf("failed to list products, error: %w", err)
		}

		for _, env := range envs {
			changed := false

			for _, svcGroup := range env.Services {
				for _, svc := range svcGroup {
					if svc.GetServiceRender().GetAutoSync() {
						svc.GetServiceRender().SetAutoSyncYaml(svc.GetServiceRender().GetOverrideYaml())
						changed = true
					}
				}
			}

			if changed {
				err = commonrepo.NewProductColl().Update(env)
				if err != nil {
					return fmt.Errorf("failed to update product, error: %w", err)
				}
			}
		}
	}

	return nil
}
