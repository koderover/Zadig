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

package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/setting"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
)

type EnvVersionColl struct {
	*mongo.Collection
	mongo.Session
	coll string
}

func NewEnvServiceVersionColl() *EnvVersionColl {
	name := models.EnvServiceVersion{}.TableName()
	return &EnvVersionColl{Collection: mongotool.Database(config.MongoDatabase()).Collection(name), coll: name}
}

func NewEnvServiceVersionCollWithSession(session mongo.Session) *EnvVersionColl {
	name := models.EnvServiceVersion{}.TableName()
	return &EnvVersionColl{Collection: mongotool.Database(config.MongoDatabase()).Collection(name), Session: session, coll: name}
}

func (c *EnvVersionColl) GetCollectionName() string {
	return c.coll
}

func (c *EnvVersionColl) EnsureIndex(ctx context.Context) error {
	mod := []mongo.IndexModel{
		{
			Keys: bson.D{
				bson.E{Key: "product_name", Value: 1},
				bson.E{Key: "env_name", Value: 1},
				bson.E{Key: "production", Value: 1},
				bson.E{Key: "service.service_name", Value: 1},
				bson.E{Key: "service.release_name", Value: 1},
				bson.E{Key: "service.type", Value: 1},
			},
			Options: options.Index().SetUnique(false).SetName("idx_service_1"),
		},
		{
			Keys: bson.D{
				bson.E{Key: "product_name", Value: 1},
				bson.E{Key: "env_name", Value: 1},
				bson.E{Key: "production", Value: 1},
				bson.E{Key: "service.service_name", Value: 1},
				bson.E{Key: "service.release_name", Value: 1},
				bson.E{Key: "service.type", Value: 1},
				bson.E{Key: "revision", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("idx_service_revision_1"),
		},
	}

	_, _ = c.Indexes().DropOne(ctx, "idx_service")
	_, _ = c.Indexes().DropOne(ctx, "idx_service_revision")

	_, err := c.Indexes().CreateMany(ctx, mod)

	return err
}

func (c *EnvVersionColl) Find(productName, envName, serviceName string, isHelmChart, production bool, revision int64) (*models.EnvServiceVersion, error) {
	res := &models.EnvServiceVersion{}
	query := bson.M{}
	query["env_name"] = envName
	query["product_name"] = productName
	query["production"] = production
	query["revision"] = revision

	if isHelmChart {
		query["service.release_name"] = serviceName
		query["service.type"] = setting.HelmChartDeployType
	} else {
		query["service.service_name"] = serviceName
	}

	err := c.FindOne(context.TODO(), query).Decode(res)
	return res, err
}

func (c *EnvVersionColl) GetCountAndMaxRevision(productName, envName, serviceName string, isHelmChart, production bool) (int64, int64, error) {
	match := bson.M{
		"product_name":         productName,
		"env_name":             envName,
		"service.service_name": serviceName,
		"production":           production,
	}
	if isHelmChart {
		delete(match, "service.service_name")
		match["service.release_name"] = serviceName
		match["service.type"] = setting.HelmChartDeployType
	}

	pipeline := []bson.M{
		{
			"$match": match,
		},
		{
			"$group": bson.M{
				"_id":          nil,
				"count":        bson.M{"$sum": 1},
				"max_revision": bson.M{"$max": "$revision"},
			},
		},
	}

	var result struct {
		Count       int64 `bson:"count"`
		MaxRevision int64 `bson:"max_revision"`
	}

	cursor, err := c.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(context.TODO())

	if cursor.Next(context.TODO()) {
		if err := cursor.Decode(&result); err != nil {
			return 0, 0, err
		}
	}

	return result.Count, result.MaxRevision, nil
}

func (c *EnvVersionColl) ListServiceVersions(productName, envName, serviceName string, isHelmChart, production bool) ([]*models.EnvServiceVersion, error) {
	var ret []*models.EnvServiceVersion
	query := bson.M{}

	query["env_name"] = envName
	query["product_name"] = productName
	query["production"] = production

	if isHelmChart {
		query["service.release_name"] = serviceName
		query["service.type"] = setting.HelmChartDeployType
	} else {
		query["service.service_name"] = serviceName
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"revision", 1}})

	ctx := context.Background()

	cursor, err := c.Collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}

	err = cursor.All(ctx, &ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *EnvVersionColl) DeleteRevisions(productName, envName, serviceName string, isHelmChart, production bool, revision int64) error {
	query := bson.M{}
	query["env_name"] = envName
	query["product_name"] = productName
	query["production"] = production
	query["revision"] = bson.M{"$lte": revision}

	if isHelmChart {
		query["service.release_name"] = serviceName
		query["service.type"] = setting.HelmChartDeployType
	} else {
		query["service.service_name"] = serviceName
	}

	_, err := c.DeleteMany(mongotool.SessionContext(context.TODO(), c.Session), query)

	return err
}

func (c *EnvVersionColl) Create(args *models.EnvServiceVersion) error {
	// avoid panic issue
	if args == nil {
		return errors.New("nil EnvVersion")
	}

	now := time.Now().Unix()
	args.CreateTime = now
	args.UpdateTime = now
	_, err := c.InsertOne(mongotool.SessionContext(context.TODO(), c.Session), args)

	return err
}
