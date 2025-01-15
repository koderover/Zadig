/*
 * Copyright 2023 The KodeRover Authors.
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

package mongodb

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
)

type UpdateWorkflowTaskLogColl struct {
	*mongo.Collection

	coll string
}

type UpdateWorkflowTaskLog struct {
	WorkflowName string      `bson:"workflow_name" json:"workflow_name"`
	TaskID       int64       `bson:"task_id" json:"task_id"`
	StartTime    int64       `bson:"start_time" json:"start_time"`
	EndTime      int64       `bson:"end_time" json:"end_time"`
	Status       string      `bson:"status" json:"status"`
	Data         interface{} `bson:"data" json:"data"`
}

func (c UpdateWorkflowTaskLog) TableName() string {
	return "update_workflow_task_log"
}

func NewUpdateWorkflowTaskLogColl() *UpdateWorkflowTaskLogColl {
	name := UpdateWorkflowTaskLog{}.TableName()
	return &UpdateWorkflowTaskLogColl{
		Collection: mongotool.Database(config.MongoDatabase()).Collection(name),
		coll:       name,
	}
}

func (c *UpdateWorkflowTaskLogColl) GetCollectionName() string {
	return c.coll
}

func (c *UpdateWorkflowTaskLogColl) EnsureIndex(ctx context.Context) error {
	return nil
}

func (c *UpdateWorkflowTaskLogColl) Create(args *UpdateWorkflowTaskLog) error {
	if args == nil {
		return errors.New("nil UpdateWorkflowTaskLog")
	}

	_, err := c.InsertOne(context.Background(), args)
	return err
}
