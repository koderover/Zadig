/*
Copyright 2023 The KodeRover Authors.

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

package job

import (
	"context"
	"fmt"
	"strings"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
)

type NacosJob struct {
	job      *commonmodels.Job
	workflow *commonmodels.WorkflowV4
	spec     *commonmodels.NacosJobSpec
}

func (j *NacosJob) Instantiate() error {
	j.spec = &commonmodels.NacosJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}
	j.job.Spec = j.spec
	return nil
}

func (j *NacosJob) SetPreset() error {
	j.spec = &commonmodels.NacosJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	j.job.Spec = j.spec
	originNamespaceID := strings.ReplaceAll(j.spec.NamespaceID, setting.FixedValueMark, "")

	nacosConfigs, err := commonservice.ListNacosConfig(j.spec.NacosID, originNamespaceID, log.SugaredLogger())
	if err != nil {
		return fmt.Errorf("fail to list nacos config: %w", err)
	}

	namespaces, err := commonservice.ListNacosNamespace(j.spec.NacosID, log.SugaredLogger())
	if err != nil {
		return fmt.Errorf("failed to list nacos namespace")
	}

	namespaceName := ""
	for _, namespace := range namespaces {
		if namespace.NamespaceID == originNamespaceID {
			namespaceName = namespace.NamespacedName
			break
		}
	}

	nacosConfigsMap := map[string]*types.NacosConfig{}
	for _, config := range nacosConfigs {
		config.NamespaceID = originNamespaceID
		config.NamespaceName = namespaceName
		config.OriginalContent = config.Content

		nacosConfigsMap[getNacosConfigKey(config.Group, config.DataID)] = config
	}

	var configSet sets.String
	if strings.HasPrefix(j.spec.NamespaceID, setting.FixedValueMark) {
		configSet = sets.NewString(j.spec.NacosDataRange...)
	}

	newDatas := []*types.NacosConfig{}
	for _, data := range j.spec.NacosDatas {
		if !isNacosDataFiltered(data, configSet) {
			continue
		}

		newData, ok := nacosConfigsMap[getNacosConfigKey(data.Group, data.DataID)]
		if !ok {
			log.Errorf("can't find nacos config %s/%s", data.DataID, data.Group)
			continue
		}
		newDatas = append(newDatas, newData)
	}

	newFilterDatas := []*types.NacosConfig{}
	for _, data := range nacosConfigsMap {
		if !isNacosDataFiltered(data, configSet) {
			continue
		}

		newFilterDatas = append(newFilterDatas, data)
	}

	j.spec.NacosDatas = newDatas
	j.spec.NacosFilteredData = newFilterDatas
	return nil
}

func (j *NacosJob) SetOptions() error {
	return nil
}

func (j *NacosJob) ClearOptions() error {
	return nil
}

func (j *NacosJob) ClearSelectionField() error {
	return nil
}

// UpdateWithLatestSetting Special thing about this is that everytime it is called, it re-calculate the latest default values.
func (j *NacosJob) UpdateWithLatestSetting() error {
	j.spec = &commonmodels.NacosJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	latestWorkflow, err := mongodb.NewWorkflowV4Coll().Find(j.workflow.Name)
	if err != nil {
		log.Errorf("Failed to find original workflow to set options, error: %s", err)
	}

	latestSpec := new(commonmodels.NacosJobSpec)
	found := false
	for _, stage := range latestWorkflow.Stages {
		if !found {
			for _, job := range stage.Jobs {
				if job.Name == j.job.Name && job.JobType == j.job.JobType {
					if err := commonmodels.IToi(job.Spec, latestSpec); err != nil {
						return err
					}
					found = true
					break
				}
			}
		} else {
			break
		}
	}

	if j.spec.NacosID != latestSpec.NacosID {
		j.spec.NacosID = latestSpec.NacosID
		j.spec.NamespaceID = ""
		j.spec.NacosDatas = make([]*types.NacosConfig, 0)
		j.spec.NacosFilteredData = make([]*types.NacosConfig, 0)
		j.spec.NacosDataRange = make([]string, 0)
	} else {
		// find the latest config and save it to the original config field
		originNamespaceID := strings.ReplaceAll(latestSpec.NamespaceID, setting.FixedValueMark, "")

		nacosConfigs, err := commonservice.ListNacosConfig(latestSpec.NacosID, originNamespaceID, log.SugaredLogger())
		if err != nil {
			return fmt.Errorf("fail to list nacos config: %w", err)
		}

		namespaces, err := commonservice.ListNacosNamespace(latestSpec.NacosID, log.SugaredLogger())
		if err != nil {
			return fmt.Errorf("failed to list nacos namespace")
		}

		namespaceName := ""
		for _, namespace := range namespaces {
			if namespace.NamespaceID == originNamespaceID {
				namespaceName = namespace.NamespacedName
				break
			}
		}

		nacosConfigsMap := map[string]*types.NacosConfig{}
		for _, config := range nacosConfigs {
			config.NamespaceID = originNamespaceID
			config.NamespaceName = namespaceName

			nacosConfigsMap[getNacosConfigKey(config.Group, config.DataID)] = config
		}

		var configSet sets.String
		if strings.HasPrefix(latestSpec.NamespaceID, setting.FixedValueMark) {
			configSet = sets.NewString(latestSpec.NacosDataRange...)
		}

		userConfiguredDatas := make(map[string]*types.NacosConfig)
		for _, selectedData := range j.spec.NacosDatas {
			userConfiguredDatas[selectedData.DataID] = selectedData
		}

		newFilterDatas := make([]*types.NacosConfig, 0)
		for _, data := range nacosConfigsMap {
			if !isNacosDataFiltered(data, configSet) {
				continue
			}

			newFilterDatas = append(newFilterDatas, data)
		}

		j.spec.NacosFilteredData = newFilterDatas

		newDatas := make([]*types.NacosConfig, 0)

		for _, data := range newFilterDatas {
			if userConfiguredData, ok := userConfiguredDatas[data.DataID]; ok {
				newDatas = append(newDatas, &types.NacosConfig{
					DataID:          data.DataID,
					Group:           data.Group,
					Desc:            data.Desc,
					Format:          data.Format,
					Content:         userConfiguredData.Content,
					OriginalContent: data.Content,
					NamespaceID:     data.NamespaceID,
					NamespaceName:   data.NamespaceName,
				})
			}
		}

		j.spec.NacosDatas = newDatas
		if j.spec.NamespaceID != latestSpec.NamespaceID {
			j.spec.NamespaceID = latestSpec.NamespaceID
		}
	}

	j.spec.NacosDataRange = latestSpec.NacosDataRange
	j.spec.DataFixed = latestSpec.DataFixed
	j.job.Spec = j.spec
	return nil
}

func (j *NacosJob) MergeArgs(args *commonmodels.Job) error {
	if j.job.Name == args.Name && j.job.JobType == args.JobType {
		j.spec = &commonmodels.NacosJobSpec{}
		if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
			return err
		}
		j.job.Spec = j.spec
		argsSpec := &commonmodels.NacosJobSpec{}
		if err := commonmodels.IToi(args.Spec, argsSpec); err != nil {
			return err
		}
		j.spec.NamespaceID = argsSpec.NamespaceID
		if !j.spec.DataFixed {
			j.spec.NacosDatas = argsSpec.NacosDatas
		}
	}
	return nil
}

func (j *NacosJob) ToJobs(taskID int64) ([]*commonmodels.JobTask, error) {
	resp := []*commonmodels.JobTask{}

	j.spec = &commonmodels.NacosJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return resp, err
	}
	j.job.Spec = j.spec

	info, err := mongodb.NewConfigurationManagementColl().GetNacosByID(context.Background(), j.spec.NacosID)
	if err != nil {
		return nil, errors.Wrap(err, "get nacos info")
	}
	client, err := commonservice.GetNacosClient(j.spec.NacosID)
	if err != nil {
		return nil, errors.Errorf("get nacos client error: %v", err)
	}
	namespaces, err := client.ListNamespaces()
	if err != nil {
		return nil, err
	}
	namespaceName := ""
	for _, namespace := range namespaces {
		if namespace.NamespaceID == j.spec.NamespaceID {
			namespaceName = namespace.NamespacedName
			break
		}
	}

	jobTask := &commonmodels.JobTask{
		Name: j.job.Name,
		Key:  j.job.Name,
		JobInfo: map[string]string{
			JobNameKey: j.job.Name,
		},
		JobType: string(config.JobNacos),
		Spec: commonmodels.JobTaskNacosSpec{
			NacosID:       j.spec.NacosID,
			NamespaceID:   j.spec.NamespaceID,
			NamespaceName: namespaceName,
			NacosAddr:     info.ServerAddress,
			UserName:      client.UserName,
			Password:      client.Password,
			NacosDatas:    transNacosDatas(j.spec.NacosDatas),
		},
	}

	return []*commonmodels.JobTask{jobTask}, nil
}

func (j *NacosJob) LintJob() error {
	if err := util.CheckZadigProfessionalLicense(); err != nil {
		return e.ErrLicenseInvalid.AddDesc("")
	}

	j.spec = &commonmodels.NacosJobSpec{}
	if err := commonmodels.IToi(j.job.Spec, j.spec); err != nil {
		return err
	}

	if strings.HasPrefix(j.spec.NamespaceID, setting.FixedValueMark) {
		return nil
	}

	configSet := sets.NewString(j.spec.NacosDataRange...)
	for _, data := range j.spec.NacosDatas {
		if !isNacosDataFiltered(data, configSet) {
			return fmt.Errorf("can't select the nacos config outside the config range, key: %s", getNacosConfigKey(data.Group, data.DataID))
		}
	}

	return nil
}

func transNacosDatas(confs []*types.NacosConfig) []*commonmodels.NacosData {
	resp := []*commonmodels.NacosData{}
	for _, conf := range confs {
		resp = append(resp, &commonmodels.NacosData{
			NacosConfig: *conf,
		})
	}
	return resp
}

func getNacosConfigKey(group, id string) string {
	return group + "/" + id
}

func filterNacosDatas(inputDatas []*types.NacosConfig, filters []string) []*types.NacosConfig {
	resp := []*types.NacosConfig{}
	set := sets.NewString(filters...)
	for _, data := range inputDatas {
		if isNacosDataFiltered(data, set) {
			resp = append(resp, data)
		}
	}
	return resp
}

func isNacosDataFiltered(data *types.NacosConfig, filters sets.String) bool {
	if filters.Len() == 0 {
		return true
	}

	if filters.Has("*") {
		return true
	}

	if filters.Has(getNacosConfigKey(data.Group, data.DataID)) {
		return true
	}

	return false
}
