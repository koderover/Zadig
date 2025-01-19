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

package migrate

import (
	"fmt"
	"runtime"

	"github.com/koderover/zadig/v2/pkg/cli/upgradeassistant/internal/upgradepath"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	jobctl "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow/job"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	upgradepath.RegisterHandler("2.3.0", "3.0.0", V230ToV300)
	upgradepath.RegisterHandler("3.0.0", "2.3.0", V230ToV220)
}

func V230ToV300() error {
	log.Infof("-------- start migrate workflow task data --------")
	err := removeWorkflowTaskExtraData()
	if err != nil {
		log.Errorf("migrate workflow task data error: %s", err)
		return err
	}

	return nil
}

func V300ToV230() error {
	return nil
}

// remove 100 of the workflow task's extra option field to make data clean
func removeWorkflowTaskExtraData() error {
	// find all projects to do the migration
	projects, err := templaterepo.NewProductColl().List()
	if err != nil {
		log.Errorf("failed to list project list to do workflow migrations, error: %s", err)
		return fmt.Errorf("failed to list project list to do workflow migrations")
	}

	for _, project := range projects {
		// list all workflows
		workflows, _, err := mongodb.NewWorkflowV4Coll().List(&mongodb.ListWorkflowV4Option{
			ProjectName: project.ProductName,
		}, 0, 0)

		if err != nil {
			log.Errorf("failed to list workflow for project: %s, error: %s", project.ProductName, err)
			return fmt.Errorf("failed to list workflow for project: %s, error: %s", project.ProductName, err)
		}

		for _, workflow := range workflows {
			// list first 100 task to remove all unnecessary fields
			tasks, _, err := mongodb.NewworkflowTaskv4Coll().List(&mongodb.ListWorkflowTaskV4Option{
				WorkflowName: "",
				ProjectName:  "",
				Limit:        100,
				Skip:         0,
			})

			if err != nil && (err != mongo.ErrNilDocument && err != mongo.ErrNoDocuments) {
				log.Errorf("failed to list workflow task for project: %s, workflowName: %s, error: %s", project.ProductName, workflow.Name, err)
				return fmt.Errorf("failed to list workflow task for project: %s, workflowName: %s, error: %s", project.ProductName, workflow.Name, err)
			}

			if tasks != nil && len(tasks) != 0 {
				for _, task := range tasks {
					err := cleanTask(task, workflow)
					if err != nil {
						log.Errorf("failed to clean the task's data, taskID: %d, workflowName: %s, project: %s, error: %s", task.TaskID, workflow.Name, project.ProductName, err)
						return fmt.Errorf("failed to clean the task's data, taskID: %d, workflowName: %s, project: %s, error: %s", task.TaskID, workflow.Name, project.ProductName, err)
					}

					err = mongodb.NewworkflowTaskv4Coll().Update(task.ID.Hex(), task)
					if err != nil {
						log.Errorf("failed to update the task's data, taskID: %d, workflowName: %s, project: %s, error: %s", task.TaskID, workflow.Name, project.ProductName, err)
						return fmt.Errorf("failed to update the task's data, taskID: %d, workflowName: %s, project: %s, error: %s", task.TaskID, workflow.Name, project.ProductName, err)
					}
				}
			}
		}

		// after a project is done, trigger a manual gc to save memory
		runtime.GC()
	}

	return nil
}

func cleanTask(task *commonmodels.WorkflowTask, workflow *commonmodels.WorkflowV4) error {
	for _, stage := range task.OriginWorkflowArgs.Stages {
		for _, job := range stage.Jobs {
			err := jobctl.ClearOptions(job, workflow)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
