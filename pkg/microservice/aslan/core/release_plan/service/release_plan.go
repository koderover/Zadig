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

package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/koderover/zadig/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/pkg/microservice/user/core/repository"
	"github.com/koderover/zadig/pkg/microservice/user/core/repository/orm"
	"github.com/koderover/zadig/pkg/setting"
	"github.com/koderover/zadig/pkg/shared/handler"
	e "github.com/koderover/zadig/pkg/tool/errors"
	"github.com/koderover/zadig/pkg/tool/log"
)

func CreateReleasePlan(c *handler.Context, args *models.ReleasePlan) error {
	if args.Name == "" || args.ManagerID == "" {
		return errors.New("Required parameters are missing")
	}
	if args.StartTime > args.EndTime || args.EndTime < time.Now().Unix() {
		return errors.New("Invalid release time range")
	}
	user, err := orm.GetUserByUid(args.ManagerID, repository.DB)
	if err != nil || user == nil {
		return errors.Errorf("Failed to get user by id %s, error: %v", args.ManagerID, err)
	}
	if args.Manager != user.Name {
		return errors.Errorf("Manager %s is not consistent with the user name %s", args.Manager, user.Name)
	}

	for _, job := range args.Jobs {
		if err := lintReleaseJob(job.Type, job.Spec); err != nil {
			return errors.Errorf("lintReleaseJob %s error: %v", job.Name, err)
		}
		job.ReleaseJobRuntime = models.ReleaseJobRuntime{}
		job.ID = uuid.New().String()
	}

	if args.Approval != nil {
		// todo create approval
		if err := lintApproval(args.Approval); err != nil {
			return errors.Errorf("lintApproval error: %v", err)
		}
	}

	nextID, err := mongodb.NewCounterColl().GetNextSeq(setting.WorkflowTaskV4Fmt)
	if err != nil {
		log.Errorf("CreateReleasePlan.GetNextSeq error: %v", err)
		return e.ErrGetCounter.AddDesc(err.Error())
	}
	args.Index = nextID
	args.CreatedBy = c.UserName
	args.UpdatedBy = c.UserName
	args.CreateTime = time.Now().Unix()
	args.UpdateTime = time.Now().Unix()
	args.Status = config.StatusPlanning
	args.Logs = append(args.Logs, &models.ReleasePlanLog{
		Username:   c.UserName,
		Account:    c.Account,
		Action:     "新建",
		TargetName: args.Name,
		TargetType: "发布计划",
		Before:     nil,
		After:      nil,
		CreatedAt:  time.Now().Unix(),
	})

	return mongodb.NewReleasePlanColl().Create(args)
}

type ListReleasePlanResp struct {
	List  []*models.ReleasePlan `json:"list"`
	Total int64
}

func ListReleasePlans(pageNum, pageSize int64) (*ListReleasePlanResp, error) {
	list, total, err := mongodb.NewReleasePlanColl().ListByOptions(&mongodb.ListReleasePlanOption{
		PageNum:        pageNum,
		PageSize:       pageSize,
		IsSort:         true,
		ExcludedFields: []string{"jobs", "approval", "logs"},
	})
	if err != nil {
		return nil, errors.Wrap(err, "ListReleasePlans")
	}
	return &ListReleasePlanResp{
		List:  list,
		Total: total,
	}, nil
}

func GetReleasePlan(id string) (*models.ReleasePlan, error) {
	return mongodb.NewReleasePlanColl().GetByID(context.Background(), id)
}

func DeleteReleasePlan(id string) error {
	return mongodb.NewReleasePlanColl().DeleteByID(context.Background(), id)
}

type UpdateReleasePlanArgs struct {
	Verb string      `json:"verb"`
	Spec interface{} `json:"spec"`
}

func UpdateReleasePlan(c *handler.Context, planID string, args *UpdateReleasePlanArgs) error {
	getLock(planID).Lock()
	defer getLock(planID).Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	plan, err := mongodb.NewReleasePlanColl().GetByID(ctx, planID)
	if err != nil {
		return errors.Wrap(err, "get plan")
	}

	updater, err := NewPlanUpdater(args)
	if err != nil {
		return errors.Wrap(err, "new plan updater")
	}
	if err = updater.Lint(); err != nil {
		return errors.Wrap(err, "lint")
	}
	if err = updater.Update(plan); err != nil {
		return errors.Wrap(err, "update")
	}

	plan.UpdatedBy = c.UserName
	plan.UpdateTime = time.Now().Unix()
	plan.Logs = append(plan.Logs, &models.ReleasePlanLog{
		Username: c.UserName,
		Account:  c.Account,
		Action:   "修改",
		//TargetName: plan.Name,
		//TargetType: "发布计划",
	})

	if err = mongodb.NewReleasePlanColl().UpdateByID(ctx, planID, plan); err != nil {
		return errors.Wrap(err, "update plan")
	}
	return nil
}
