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

package models

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type VariableSet struct {
	ID           primitive.ObjectID `bson:"_id,omitempty"          json:"id,omitempty"`
	Name         string             `bson:"name"                   json:"name"`
	Description  string             `bson:"description"            json:"description"`
	ProjectName  string             `bson:"project_name,omitempty" json:"project_name"`
	VariableYaml string             `bson:"variable_yaml"          json:"variable_yaml"`
	CreatedAt    int64              `bson:"created_at"             json:"created_at"`
	CreatedBy    string             `bson:"created_by"             json:"created_by"`
	UpdatedAt    int64              `bson:"updated_at"             json:"updated_at"`
	UpdatedBy    string             `bson:"updated_by"              json:"updated_by"`
}

func (VariableSet) TableName() string {
	return "variable_set"
}
