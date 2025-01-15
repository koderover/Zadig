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

type WaitPodFinishLogColl struct {
	*mongo.Collection

	coll string
}

type WaitPodFinishLog struct {
	JobName   string `bson:"job_name" json:"job_name"`
	JobStatus string `bson:"job_status" json:"job_status"`
	Namespace string `bson:"namespace" json:"namespace"`
	PodName   string `bson:"pod_name" json:"pod_name"`
	Status    string `bson:"status" json:"status"`
}

func (c WaitPodFinishLog) TableName() string {
	return "wait_pod_finish_log"
}

func NewWaitPodFinishLogColl() *WaitPodFinishLogColl {
	name := WaitPodFinishLog{}.TableName()
	return &WaitPodFinishLogColl{
		Collection: mongotool.Database(config.MongoDatabase()).Collection(name),
		coll:       name,
	}
}

func (c *WaitPodFinishLogColl) GetCollectionName() string {
	return c.coll
}

func (c *WaitPodFinishLogColl) EnsureIndex(ctx context.Context) error {
	return nil
}

func (c *WaitPodFinishLogColl) Create(args *WaitPodFinishLog) error {
	if args == nil {
		return errors.New("nil WaitPodFinishLog")
	}

	_, err := c.InsertOne(context.Background(), args)
	return err
}
