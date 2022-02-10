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
	"fmt"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/pkg/config"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/label/repository/models"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/label/repository/mongodb"
	e "github.com/koderover/zadig/pkg/tool/errors"
)

type CreateLabelsResp struct {
	LabelMap map[string]string `json:"label_map"`
}

func CreateLabels(arg *CreateLabelsArgs, userName string) (*CreateLabelsResp, error) {
	filteredLabels := make([]*models.Label, 0)
	keyValues := sets.NewString()
	for _, v := range arg.Labels {
		keyValue := fmt.Sprintf("%s-%s", v.Key, v.Value)
		if keyValues.Has(keyValue) {
			continue
		}
		keyValues.Insert(keyValue)
		tmpModel := models.Label{
			Key:      v.Key,
			Value:    v.Value,
			CreateBy: userName,
			Type:     v.Type,
		}
		filteredLabels = append(filteredLabels, &tmpModel)
	}
	err := mongodb.NewLabelColl().BulkCreate(filteredLabels)
	if err != nil {
		return nil, err
	}
	var labelModels []mongodb.Label
	for _, label := range filteredLabels {
		labelModels = append(labelModels, mongodb.Label{
			Key:   label.Key,
			Value: label.Value,
		})
	}
	result := make(map[string]string)
	resp, err := mongodb.NewLabelColl().List(mongodb.ListLabelOpt{
		Labels: labelModels,
	})
	for _, label := range resp {
		result[BuildLabelString(label.Key, label.Value)] = label.ID.Hex()
	}
	return &CreateLabelsResp{
		LabelMap: result,
	}, err
}

type ListLabelsArgs struct {
	Labels []mongodb.Label `json:"labels"`
}

type ListLabelsResp struct {
	Labels []*models.Label `json:"labels"`
}

type CreateLabelsArgs struct {
	Labels []mongodb.Label `json:"labels"`
}

func ListLabels(args *ListLabelsArgs) (*ListLabelsResp, error) {
	labels, err := mongodb.NewLabelColl().List(mongodb.ListLabelOpt{args.Labels})
	if err != nil {
		return nil, err
	}
	return &ListLabelsResp{Labels: labels}, nil
}

type ListResourceByLabelsReq struct {
	LabelFilters []mongodb.Label `json:"label_filters"`
}

type ListResourcesByLabelsResp struct {
	Resources map[string][]mongodb.Resource `json:"resources"`
}

func BuildLabelString(key string, value string) string {
	return fmt.Sprintf("%s-%s", key, value)
}

func ListResourcesByLabels(filters []mongodb.Label, logger *zap.SugaredLogger) (*ListResourcesByLabelsResp, error) {
	res := make(map[string][]mongodb.Resource)
	if len(filters) == 0 {
		return &ListResourcesByLabelsResp{Resources: res}, nil
	}
	// 1.find labels by label filters
	labels, err := mongodb.NewLabelColl().List(mongodb.ListLabelOpt{Labels: filters})
	if err != nil {
		logger.Errorf("labels ListByOpt err:%s", err)
		return nil, err
	}
	if len(labels) == 0 {
		return &ListResourcesByLabelsResp{Resources: res}, nil
	}
	// 2.find labelBindings by label ids
	labelIDSet := sets.NewString()
	labelsM := make(map[string]string)
	for _, v := range labels {
		labelIDSet.Insert(v.ID.Hex())
		labelsM[v.ID.Hex()] = BuildLabelString(v.Key, v.Value)
	}

	labelBindings, err := mongodb.NewLabelBindingColl().ListByOpt(&mongodb.LabelBindingCollFindOpt{LabelIDs: labelIDSet.List()})
	if err != nil {
		logger.Errorf("labelBindings ListByOpt err:%s", err)
		return nil, err
	}

	// 3.find labels by resourceName-projectName
	for _, v := range labelBindings {
		resource := mongodb.Resource{
			Name:        v.ResourceName,
			ProjectName: v.ProjectName,
			Type:        v.ResourceType,
		}
		labelString, _ := labelsM[v.LabelID]
		if resources, ok := res[labelString]; ok {
			res[labelString] = append(resources, resource)
		} else {
			res[labelString] = []mongodb.Resource{resource}
		}
	}

	return &ListResourcesByLabelsResp{
		Resources: res,
	}, nil
}

type ListLabelsByResourcesReq struct {
	Resources []mongodb.Resource `json:"resources"`
}

type ListLabelsByResourcesResp struct {
	Labels map[string][]*models.Label `json:"labels"`
}

func ListLabelsByResources(resources []mongodb.Resource, logger *zap.SugaredLogger) (*ListLabelsByResourcesResp, error) {
	//1. find the labelBindings by resources
	labelBindings, err := mongodb.NewLabelBindingColl().ListByResources(mongodb.ListLabelBindingsByResources{Resources: resources})
	if err != nil {
		logger.Errorf("NewLabelBindingColl ListByResources err:%s", err)
		return nil, err
	}

	//2.find labels by labelBindings
	labelIDSet := sets.NewString()
	for _, v := range labelBindings {
		labelIDSet.Insert(v.LabelID)
	}
	labels, err := mongodb.NewLabelColl().ListByIDs(labelIDSet.List())
	if err != nil {
		return nil, err
	}
	labelM := make(map[string]*models.Label)
	for _, label := range labels {
		labelM[label.ID.Hex()] = label
	}
	// 3. iterate resources
	res := make(map[string][]*models.Label)
	for _, labelBinding := range labelBindings {
		resourceKey := config.BuildResourceKey(labelBinding.ResourceType, labelBinding.ProjectName, labelBinding.ResourceName)
		label, ok := labelM[labelBinding.LabelID]
		if !ok {
			return nil, fmt.Errorf("can not find label %v", labelBinding.LabelID)
		}

		if arr, ok := res[resourceKey]; ok {
			res[resourceKey] = append(arr, label)
		} else {
			res[resourceKey] = []*models.Label{label}
		}
	}

	return &ListLabelsByResourcesResp{
		Labels: res,
	}, nil
}

type DeleteLabelsArgs struct {
	IDs []string
}

func DeleteLabels(ids []string, forceDelete bool, logger *zap.SugaredLogger) error {
	if len(ids) == 0 {
		return nil
	}

	res, err := mongodb.NewLabelBindingColl().ListByOpt(&mongodb.LabelBindingCollFindOpt{LabelIDs: ids})
	if err != nil {
		logger.Errorf("list labelbingding err:%s", err)
		return err
	}

	if !forceDelete && len(res) > 0 {
		return e.ErrForbidden.AddDesc("some label has already bind resource, can not delete")
	}

	if forceDelete {
		var labelBindingIDs []string
		for _, labelBinding := range res {
			labelBindingIDs = append(ids, labelBinding.ID.Hex())
		}

		if err := mongodb.NewLabelBindingColl().BulkDeleteByIds(labelBindingIDs); err != nil {
			logger.Errorf("NewLabelBindingColl DeleteMany err :%s", err)
			return err
		}
		return mongodb.NewLabelColl().BulkDelete(ids)
	}

	return mongodb.NewLabelColl().BulkDelete(ids)
}

func DeleteLabelsAndBindingsByProject(projectName string, logger *zap.SugaredLogger) error {

	labels, err := mongodb.NewLabelColl().ListByProjectName(projectName)
	if err != nil {
		return err
	}

	var labelBindingIDs []string
	for _, labelBinding := range labels {
		labelBindingIDs = append(labelBindingIDs, labelBinding.ID.Hex())
	}

	if err := mongodb.NewLabelBindingColl().BulkDeleteByIds(labelBindingIDs); err != nil {
		logger.Errorf("NewLabelBindingColl DeleteMany err :%s", err)
		return err
	}

	if err := mongodb.NewLabelColl().BulkDeleteByProject(projectName); err != nil {
		logger.Errorf("DeleteLabelsByProject err:%s", err)
		return err
	}
	return nil
}
