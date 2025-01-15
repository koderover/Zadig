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

package workflowcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config2 "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/instantmessage"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/notify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/scmnotify"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workflowcontroller/jobcontroller"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workflowstat"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/podexec"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

const (
	checkShellStepStart  = "ls /zadig/debug/shell_step"
	checkShellStepDone   = "ls /zadig/debug/shell_step_done"
	setOrUnsetBreakpoint = "%s /zadig/debug/breakpoint_%s"
)

const (
	WorkflowDebugEventEnableDebug   = "EnableDebug"
	WorkflowDebugEventDeleteDebug   = "DeleteDebug"
	WorkflowDebugEventSetBreakPoint = "SetBreakpoint"
)

type WorkflowDebugEvent struct {
	EventType string `json:"event_type"`
	JobName   string `json:"job_name"`
	TaskID    int64  `json:"task_id"`
	Set       bool   `json:"set"`
	Position  string `json:"position"`
}

type workflowCtl struct {
	workflowTask      *commonmodels.WorkflowTask
	workflowTaskMutex sync.RWMutex
	logger            *zap.SugaredLogger
	prefix            string
	ack               func()
}

func NewWorkflowController(workflowTask *commonmodels.WorkflowTask, logger *zap.SugaredLogger) *workflowCtl {
	ctl := &workflowCtl{
		workflowTask: workflowTask,
		logger:       logger,
		prefix:       fmt.Sprintf("workflowctl-%s-%d", workflowTask.WorkflowName, workflowTask.TaskID),
	}
	ctl.ack = ctl.updateWorkflowTask
	return ctl
}

func SendWorkflowNotifyMessage(task *commonmodels.WorkflowTask, receiver string, status config.Status, log *zap.SugaredLogger) {
	if status != config.StatusFailed && status != config.StatusPassed && status != config.StatusCancelled &&
		status != config.StatusWaitingApprove && status != config.StatusTimeout {
		return
	}
	ctx := &commonmodels.WorkflowTaskStatusCtx{
		TaskID:              task.TaskID,
		ProductName:         task.ProjectName,
		WorkflowName:        task.WorkflowName,
		WorkflowDisplayName: task.WorkflowDisplayName,
		Executor:            task.TaskCreator,
		Status:              status,
	}
	notify.SendWorkflowTaskStatusMsg(receiver, ctx, log)
}

func CancelWorkflowTask(userName, workflowName string, taskID int64, logger *zap.SugaredLogger) error {
	t, err := commonrepo.NewworkflowTaskv4Coll().Find(workflowName, taskID)
	if err != nil {
		logger.Errorf("[%s] task: %s:%d not found", userName, workflowName, taskID)
		return err
	}

	// try to remove task from queue first.
	q := ConvertTaskToQueue(t)
	if err := Remove(q); err != nil {
		logger.Errorf("[%s] remove queue task: %s:%d error: %v", userName, workflowName, taskID, err)
	}

	if t.Status == config.StatusPassed {
		logger.Errorf("[%s] task: %s:%d is passed, cannot cancel", userName, workflowName, taskID)
		return fmt.Errorf("task: %s:%d is passed, cannot cancel", workflowName, taskID)
	}

	t.Status = config.StatusCancelled
	t.TaskRevoker = userName

	logger.Infof("[%s] CancelRunningTask %s:%d", userName, workflowName, taskID)

	if err := commonrepo.NewworkflowTaskv4Coll().Update(t.ID.Hex(), t); err != nil {
		logger.Errorf("[%s] update task: %s:%d error: %v", userName, workflowName, taskID, err)
		return err
	}

	// Updating the comment in the git repository, this will not cause the function to return error if this function call fails
	if err := scmnotify.NewService().UpdateWebhookCommentForWorkflowV4(t, logger); err != nil {
		log.Warnf("Failed to update comment for custom workflow %s, taskID: %d the error is: %s", t.WorkflowName, t.TaskID, err)
	}
	if err := scmnotify.NewService().CompleteGitCheckForWorkflowV4(t.WorkflowArgs, t.TaskID, t.Status, logger); err != nil {
		log.Warnf("Failed to update github check status for custom workflow %s, taskID: %d the error is: %s", t.WorkflowName, t.TaskID, err)
	}

	err = cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Publish(fmt.Sprintf("workflowctl-cancel-%s-%d", workflowName, taskID), "cancel")
	if err != nil {
		return fmt.Errorf("failed to cancel task: %s:%d, err: %s", workflowName, taskID, err)
	}
	return nil
}

func WorkflowDebugLockKey(workflowName string, taskID int64) string {
	return fmt.Sprintf("workflowctl-debug-lock-%s-%d", workflowName, taskID)
}

func WorkflowDebugChanKey(workflowName string, taskID int64) string {
	return fmt.Sprintf("workflowctl-debug-lock-%s-%d", workflowName, taskID)
}

func (c *workflowCtl) handleDebugEvent(bytes string) error {
	event := &WorkflowDebugEvent{}
	err := json.Unmarshal([]byte(bytes), event)
	if err != nil {
		return fmt.Errorf("failed to unmarshal event, err: %s", err)
	}
	log.Infof("handing workflow debug event: %s/%d, event: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, event.EventType)

	switch event.EventType {
	case WorkflowDebugEventSetBreakPoint:
		err = c.handleWorkflowBreakpoint(event.JobName, event.Position, event.Set)
	case WorkflowDebugEventEnableDebug:
		err = c.enableWorkflowDebug()
	case WorkflowDebugEventDeleteDebug:
		err = c.stopWorkflowDebug(event.JobName, event.Position)
	default:
		err = fmt.Errorf("unknown debug event: %s", err)
	}
	return err
}

func (c *workflowCtl) Run(ctx context.Context, concurrency int) {
	if c.workflowTask.GlobalContext == nil {
		c.workflowTask.GlobalContext = make(map[string]string)
	}
	if c.workflowTask.ClusterIDMap == nil {
		c.workflowTask.ClusterIDMap = make(map[string]bool)
	}

	c.workflowTask.Status = config.StatusRunning
	c.workflowTask.StartTime = time.Now().Unix()
	c.ack()
	c.logger.Infof("start workflow: %s,status: %s", c.workflowTask.WorkflowName, c.workflowTask.Status)
	defer func() {
		c.workflowTask.EndTime = time.Now().Unix()
		c.logger.Infof("finish workflow: %s,status: %s", c.workflowTask.WorkflowName, c.workflowTask.Status)
		c.ack()
		// clean share storage after workflow finished
		go c.CleanShareStorage()
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// sub cancel signal from redis
	cancelChan, closeFunc := cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Subscribe(fmt.Sprintf("workflowctl-cancel-%s-%d", c.workflowTask.WorkflowName, c.workflowTask.TaskID))
	debugChan, closeDebugChanFunc := cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Subscribe(WorkflowDebugChanKey(c.workflowTask.WorkflowName, c.workflowTask.TaskID))
	defer func() {
		log.Infof("pubsub channel: %s/%d closed", c.workflowTask.WorkflowName, c.workflowTask.TaskID)
		_ = closeFunc()
		_ = closeDebugChanFunc()
	}()

	// receiving cancel signal from redis
	go func() {
		for {
			select {
			case data := <-debugChan:
				err := c.handleDebugEvent(data.Payload)
				if err != nil {
					c.logger.Errorf(fmt.Sprintf("workflow ctl run err: %s", err))
				}
			case <-cancelChan:
				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	workflowCtx := &commonmodels.WorkflowTaskCtx{
		WorkflowName:                c.workflowTask.WorkflowName,
		WorkflowDisplayName:         c.workflowTask.WorkflowDisplayName,
		ProjectName:                 c.workflowTask.ProjectName,
		Remark:                      c.workflowTask.Remark,
		TaskID:                      c.workflowTask.TaskID,
		WorkflowTaskCreatorUsername: c.workflowTask.TaskCreator,
		WorkflowTaskCreatorUserID:   c.workflowTask.TaskCreatorID,
		WorkflowTaskCreatorMobile:   c.workflowTask.TaskCreatorPhone,
		WorkflowTaskCreatorEmail:    c.workflowTask.TaskCreatorEmail,
		Workspace:                   "/workspace",
		DistDir:                     fmt.Sprintf("%s/%s/dist/%d", config.S3StoragePath(), c.workflowTask.WorkflowName, c.workflowTask.TaskID),
		DockerMountDir:              fmt.Sprintf("/tmp/%s/docker/%d", uuid.NewString(), time.Now().Unix()),
		ConfigMapMountDir:           fmt.Sprintf("/tmp/%s/cm/%d", uuid.NewString(), time.Now().Unix()),
		GlobalContextGetAll:         c.getGlobalContextAll,
		GlobalContextGet:            c.getGlobalContext,
		GlobalContextSet:            c.setGlobalContext,
		GlobalContextEach:           c.globalContextEach,
		ClusterIDAdd:                c.addClusterID,
		StartTime:                   time.Now(),
	}
	defer jobcontroller.CleanWorkflowJobs(ctx, c.workflowTask, workflowCtx, c.logger, c.ack)
	if err := scmnotify.NewService().UpdateWebhookCommentForWorkflowV4(c.workflowTask, c.logger); err != nil {
		log.Warnf("Failed to update comment for custom workflow %s, taskID: %d the error is: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
	}
	if err := scmnotify.NewService().UpdateGitCheckForWorkflowV4(c.workflowTask.WorkflowArgs, c.workflowTask.TaskID, c.logger); err != nil {
		log.Warnf("Failed to update github check status for custom workflow %s, taskID: %d the error is: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
	}
	RunStages(ctx, c.workflowTask.Stages, workflowCtx, concurrency, c.logger, c.ack)
	updateworkflowStatus(c.workflowTask)
}

func (c *workflowCtl) handleWorkflowBreakpoint(jobName, position string, set bool) error {
	workflowDebugLock := cache.NewRedisLockWithExpiry(WorkflowDebugLockKey(c.workflowTask.WorkflowName, c.workflowTask.TaskID), time.Second*5)
	err := workflowDebugLock.Lock()
	if err != nil {
		return e.ErrStopDebugShell.AddDesc(fmt.Sprintf("failed to acquire lock when setting breakpoint, err: %s", err))
	}
	defer workflowDebugLock.Unlock()

	logger := c.logger
	var ack func()
	defer func() {
		if ack != nil {
			c.ack()
		}
	}()
	var task *commonmodels.JobTask
FOR:
	for _, stage := range c.workflowTask.Stages {
		for _, jobTask := range stage.Jobs {
			if jobTask.Name == jobName {
				task = jobTask
				break FOR
			}
		}
	}
	if task == nil {
		logger.Error("set workflowTaskV4 breakpoint failed: not found job")
		return e.ErrSetBreakpoint.AddDesc("当前任务不存在")
	}
	// job task has not run, update data in memory and ack
	if task.Status == "" {
		switch position {
		case "before":
			task.BreakpointBefore = set
			ack = c.ack
		case "after":
			task.BreakpointAfter = set
			ack = c.ack
		}
		logger.Infof("set workflowTaskV4 breakpoint success: %s-%s %v", jobName, position, set)
		return nil
	}

	jobTaskSpec := &commonmodels.JobTaskFreestyleSpec{}
	if err := commonmodels.IToi(task.Spec, jobTaskSpec); err != nil {
		logger.Errorf("set workflowTaskV4 breakpoint failed: IToi %v", err)
		return e.ErrSetBreakpoint.AddDesc("修改断点意外失败: convert job task spec")
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(jobTaskSpec.Properties.ClusterID)
	if err != nil {
		log.Errorf("set workflowTaskV4 breakpoint failed: get kube client error: %s", err)
		return e.ErrSetBreakpoint.AddDesc("修改断点意外失败: get kube client")
	}

	// job task is running, check whether shell step has run, and touch breakpoint file
	// if job task status is debug_after, only breakpoint operation can do is unset breakpoint_after, which should be done by StopDebugWorkflowTaskJobV4
	// if job task status is prepare, setting breakpoints has a low probability of not taking effect, and the current design allows for this flaw
	if task.Status == config.StatusRunning || task.Status == config.StatusDebugBefore || task.Status == config.StatusPrepare {
		pods, err := getter.ListPods(jobTaskSpec.Properties.Namespace, labels.Set{"job-name": task.K8sJobName}.AsSelector(), kubeClient)
		if err != nil {
			logger.Errorf("set workflowTaskV4 breakpoint failed: list pods %v", err)
			return e.ErrSetBreakpoint.AddDesc("修改断点意外失败: ListPods")
		}
		if len(pods) == 0 {
			logger.Error("set workflowTaskV4 breakpoint failed: list pods num 0")
			return e.ErrSetBreakpoint.AddDesc("修改断点意外失败: ListPods num 0")
		}
		pod := pods[0]
		switch pod.Status.Phase {
		case corev1.PodRunning:
		default:
			logger.Errorf("set workflowTaskV4 breakpoint failed: pod status is %s", pod.Status.Phase)
			return e.ErrSetBreakpoint.AddDesc(fmt.Sprintf("当前任务状态 %s 无法修改断点", pod.Status.Phase))
		}
		exec := func(cmd string) bool {
			opt := podexec.ExecOptions{
				Namespace:     jobTaskSpec.Properties.Namespace,
				PodName:       pod.Name,
				ContainerName: pod.Spec.Containers[0].Name,
				Command:       []string{"sh", "-c", cmd},
			}
			_, stderr, success, _ := podexec.KubeExec(jobTaskSpec.Properties.ClusterID, opt)
			logger.Errorf("set workflowTaskV4 breakpoint exec %s error: %s", cmd, stderr)
			return success
		}
		touchOrRemove := func(set bool) string {
			if set {
				return "touch"
			}
			return "rm"
		}
		switch position {
		case "before":
			if exec(checkShellStepStart) {
				logger.Error("set workflowTaskV4 before breakpoint failed: shell step has started")
				return e.ErrSetBreakpoint.AddDesc("当前任务已开始运行脚本，无法修改前断点")
			}
			exec(fmt.Sprintf(setOrUnsetBreakpoint, touchOrRemove(set), position))
		case "after":
			if exec(checkShellStepDone) {
				logger.Error("set workflowTaskV4 after breakpoint failed: shell step has been done")
				return e.ErrSetBreakpoint.AddDesc("当前任务已运行完脚本，无法修改后断点")
			}
			exec(fmt.Sprintf(setOrUnsetBreakpoint, touchOrRemove(set), position))
		}
		// update data in memory and ack
		switch position {
		case "before":
			task.BreakpointBefore = set
			ack = c.ack
		case "after":
			task.BreakpointAfter = set
			ack = c.ack
		}
		logger.Infof("set workflowTaskV4 breakpoint success: %s-%s %v", jobName, position, set)
		return nil
	}
	logger.Errorf("set workflowTaskV4 breakpoint failed: job status is %s", task.Status)
	return e.ErrSetBreakpoint.AddDesc("当前任务状态无法修改断点 ")
}

func (c *workflowCtl) enableWorkflowDebug() error {
	workflowDebugLock := cache.NewRedisLockWithExpiry(WorkflowDebugLockKey(c.workflowTask.WorkflowName, c.workflowTask.TaskID), time.Second*5)
	err := workflowDebugLock.Lock()
	if err != nil {
		return e.ErrStopDebugShell.AddDesc(fmt.Sprintf("failed to acquire lock when setting breakpoint, err: %s", err))
	}
	defer workflowDebugLock.Unlock()

	if c.workflowTask.IsDebug {
		return e.ErrStopDebugShell.AddDesc("任务已开启调试模式")
	}
	c.workflowTask.IsDebug = true
	c.logger.Infof("enable workflowTaskV4 debug mode success: %s-%d", c.workflowTask.WorkflowName, c.workflowTask.TaskID)
	c.ack()
	return nil
}

func (c *workflowCtl) stopWorkflowDebug(jobName, position string) error {
	workflowDebugLock := cache.NewRedisLockWithExpiry(WorkflowDebugLockKey(c.workflowTask.WorkflowName, c.workflowTask.TaskID), time.Second*5)
	err := workflowDebugLock.Lock()
	if err != nil {
		return e.ErrStopDebugShell.AddDesc(fmt.Sprintf("failed to acquire lock when setting breakpoint, err: %s", err))
	}
	defer workflowDebugLock.Unlock()

	logger := c.logger
	var ack func()
	defer func() {
		if ack != nil {
			ack()
		}
	}()

	var task *commonmodels.JobTask
FOR:
	for _, stage := range c.workflowTask.Stages {
		for _, jobTask := range stage.Jobs {
			if jobTask.Name == jobName {
				task = jobTask
				break FOR
			}
		}
	}
	if task == nil {
		logger.Error("stop workflowTaskV4 debug shell failed: not found job")
		return e.ErrStopDebugShell.AddDesc("Job不存在")
	}
	jobTaskSpec := &commonmodels.JobTaskFreestyleSpec{}
	if err := commonmodels.IToi(task.Spec, jobTaskSpec); err != nil {
		logger.Errorf("stop workflowTaskV4 debug shell failed: IToi %v", err)
		return e.ErrStopDebugShell.AddDesc("结束调试意外失败")
	}

	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(jobTaskSpec.Properties.ClusterID)
	if err != nil {
		logger.Errorf("stop workflowTaskV4 debug shell failed: get kube client error: %s", err)
		return e.ErrSetBreakpoint.AddDesc("结束调试意外失败: get kube client")
	}

	pods, err := getter.ListPods(jobTaskSpec.Properties.Namespace, labels.Set{"job-name": task.K8sJobName}.AsSelector(), kubeClient)
	if err != nil {
		logger.Errorf("stop workflowTaskV4 debug shell failed: list pods %v", err)
		return e.ErrStopDebugShell.AddDesc("结束调试意外失败: ListPods")
	}
	if len(pods) == 0 {
		logger.Error("stop workflowTaskV4 debug shell failed: list pods num 0")
		return e.ErrStopDebugShell.AddDesc("结束调试意外失败: ListPods num 0")
	}
	pod := pods[0]
	switch pod.Status.Phase {
	case corev1.PodRunning:
	default:
		logger.Errorf("stop workflowTaskV4 debug shell failed: pod status is %s", pod.Status.Phase)
		return e.ErrStopDebugShell.AddDesc(fmt.Sprintf("Job 状态 %s 无法结束调试", pod.Status.Phase))
	}
	exec := func(cmd string) bool {
		opt := podexec.ExecOptions{
			Namespace:     jobTaskSpec.Properties.Namespace,
			PodName:       pod.Name,
			ContainerName: pod.Spec.Containers[0].Name,
			Command:       []string{"sh", "-c", cmd},
		}
		_, stderr, success, _ := podexec.KubeExec(jobTaskSpec.Properties.ClusterID, opt)
		if stderr != "" {
			logger.Errorf("stop workflowTaskV4 debug shell exec %s error: %s", cmd, stderr)
		}
		return success
	}

	if !exec(fmt.Sprintf("ls /zadig/debug/breakpoint_%s", position)) {
		logger.Errorf("set workflowTaskV4 %s breakpoint failed: not found file", position)
		return e.ErrStopDebugShell.AddDesc("未找到断点文件")
	}
	exec(fmt.Sprintf("rm /zadig/debug/breakpoint_%s", position))

	ack = c.ack
	logger.Infof("stop workflowTaskV4 debug shell success: %s-%d", c.workflowTask.WorkflowName, c.workflowTask.TaskID)
	return nil
}

func updateworkflowStatus(workflow *commonmodels.WorkflowTask) {
	statusMap := map[config.Status]int{
		config.StatusPause:     7,
		config.StatusReject:    6,
		config.StatusCancelled: 5,
		config.StatusTimeout:   4,
		config.StatusFailed:    3,
		config.StatusPassed:    2,
		config.StatusUnstable:  1,
		config.StatusSkipped:   0,
	}

	// 初始化workflowStatus为创建状态
	workflowStatus := config.StatusRunning

	stageStatus := make([]int, len(workflow.Stages))

	for i, j := range workflow.Stages {
		statusCode, ok := statusMap[j.Status]
		if !ok {
			statusCode = -1
		}
		stageStatus[i] = statusCode
	}
	var workflowStatusCode int
	for i, code := range stageStatus {
		if i == 0 || code > workflowStatusCode {
			workflowStatusCode = code
		}
	}

	for taskstatus, code := range statusMap {
		if workflowStatusCode == code {
			workflowStatus = taskstatus
			break
		}
	}
	if workflow.Status != workflowStatus {
		SendWorkflowNotifyMessage(workflow, workflow.TaskCreator, workflowStatus, log.SugaredLogger())
	}

	// special case: if there is only 1 stage with unstable status, we still count it as passed
	if workflowStatus == config.StatusUnstable {
		workflowStatus = config.StatusPassed
	}

	workflow.Status = workflowStatus
}

func (c *workflowCtl) updateWorkflowTask() {
	c.workflowTaskMutex.Lock()
	commonrepo.NewUpdateWorkflowTaskLogColl().Create(&commonrepo.UpdateWorkflowTaskLog{
		WorkflowName: c.workflowTask.WorkflowName,
		TaskID:       c.workflowTask.TaskID,
		StartTime:    c.workflowTask.StartTime,
		EndTime:      c.workflowTask.EndTime,
		Status:       string(c.workflowTask.Status),
		Data:         c.workflowTask,
	})
	c.workflowTaskMutex.Unlock()

	taskInColl, err := commonrepo.NewworkflowTaskv4Coll().Find(c.workflowTask.WorkflowName, c.workflowTask.TaskID)
	if err != nil {
		c.logger.Errorf("find workflow task v4 %s failed,error: %v", c.workflowTask.WorkflowName, err)
		return
	}
	// 如果当前状态已经通过或者失败, 不处理新接受到的ACK
	if taskInColl.Status == config.StatusPassed || taskInColl.Status == config.StatusFailed || taskInColl.Status == config.StatusTimeout || taskInColl.Status == config.StatusReject {
		c.logger.Infof("%s:%d:%s task already done", c.workflowTask.WorkflowName, c.workflowTask.TaskID, taskInColl.Status)
		return
	}
	// 如果当前状态已经是取消状态, 一般为用户取消了任务, 此时任务在短暂时间内会继续运行一段时间,
	if taskInColl.Status == config.StatusCancelled {
		// Task终止状态可能为Pass, Fail, Cancel, Timeout
		// backend 会继续接受到ACK, 在这种情况下, 终止状态之外的ACK都无需处理，避免出现取消之后又被重置成运行态
		if c.workflowTask.Status != config.StatusFailed && c.workflowTask.Status != config.StatusPassed && c.workflowTask.Status != config.StatusCancelled && c.workflowTask.Status != config.StatusTimeout {
			c.logger.Infof("%s:%d task has been cancelled, ACK dropped", c.workflowTask.WorkflowName, c.workflowTask.TaskID)
			return
		}
	}
	if success := UpdateQueue(c.workflowTask); !success {
		c.logger.Errorf("%s:%d update t status error", c.workflowTask.WorkflowName, c.workflowTask.TaskID)
	}

	c.workflowTask.Remark = ""

	c.workflowTaskMutex.Lock()
	if err := commonrepo.NewworkflowTaskv4Coll().Update(c.workflowTask.ID.Hex(), c.workflowTask); err != nil {
		c.workflowTaskMutex.Unlock()
		c.logger.Errorf("update workflow task v4 failed,error: %v", err)
		return
	}
	c.workflowTaskMutex.Unlock()

	if c.workflowTask.Status == config.StatusPassed || c.workflowTask.Status == config.StatusFailed || c.workflowTask.Status == config.StatusTimeout || c.workflowTask.Status == config.StatusCancelled || c.workflowTask.Status == config.StatusReject || c.workflowTask.Status == config.StatusPause {
		c.logger.Infof("%s:%d:%v task done", c.workflowTask.WorkflowName, c.workflowTask.TaskID, c.workflowTask.Status)
		if err := instantmessage.NewWeChatClient().SendWorkflowTaskNotifications(c.workflowTask); err != nil {
			c.logger.Errorf("send workflow task notification failed, error: %v", err)
		}
		q := ConvertTaskToQueue(c.workflowTask)
		if err := Remove(q); err != nil {
			c.logger.Errorf("remove queue task: %s:%d error: %v", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
		}
		// Updating the comment in the git repository, this will not cause the function to return error if this function call fails
		if err := scmnotify.NewService().UpdateWebhookCommentForWorkflowV4(c.workflowTask, c.logger); err != nil {
			log.Warnf("Failed to update comment for custom workflow %s, taskID: %d the error is: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
		}
		if err := scmnotify.NewService().CompleteGitCheckForWorkflowV4(c.workflowTask.WorkflowArgs, c.workflowTask.TaskID, c.workflowTask.Status, c.logger); err != nil {
			log.Warnf("Failed to update github check status for custom workflow %s, taskID: %d the error is: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
		}
		if err := workflowstat.UpdateWorkflowStat(c.workflowTask.WorkflowName, string(config.WorkflowTypeV4), string(c.workflowTask.Status), c.workflowTask.ProjectName, c.workflowTask.EndTime-c.workflowTask.StartTime, c.workflowTask.IsRestart); err != nil {
			log.Warnf("Failed to update workflow stat for custom workflow %s, taskID: %d the error is: %s", c.workflowTask.WorkflowName, c.workflowTask.TaskID, err)
		}
	}
}

func (c *workflowCtl) CleanShareStorage() {
	for clusterID := range c.workflowTask.ClusterIDMap {
		cleanJobName := fmt.Sprintf("clean-%s", rand.String(8))
		namespace := setting.AttachedClusterNamespace
		if clusterID == setting.LocalClusterID || clusterID == "" {
			namespace = config.Namespace()
		}
		kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(clusterID)
		if err != nil {
			c.logger.Errorf("can't init k8s client: %v", err)
			continue
		}
		kubeApiServer, err := clientmanager.NewKubeClientManager().GetControllerRuntimeAPIReader(clusterID)
		if err != nil {
			c.logger.Errorf("can't init k8s api reader: %v", err)
			continue
		}
		job, err := jobcontroller.BuildCleanJob(cleanJobName, clusterID, c.workflowTask.WorkflowName, c.workflowTask.TaskID)
		if err != nil {
			c.logger.Errorf("build clean job error: %v", err)
			continue
		}
		job.Namespace = namespace
		if err := updater.CreateJob(job, kubeClient); err != nil {
			c.logger.Errorf("create job error: %v", err)
			continue
		}
		defer func(client client.Client, name, namespace string) {
			if err := updater.DeleteJobAndWait(namespace, name, client); err != nil {
				c.logger.Errorf("delete job error: %v", err)
			}
		}(kubeClient, cleanJobName, namespace)
		status := jobcontroller.WaitPlainJobEnd(context.Background(), 10, namespace, cleanJobName, kubeClient, kubeApiServer, c.logger)
		c.logger.Infof("clean job %s finished, status: %s", cleanJobName, status)
	}

	cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Delete(WorkflowDebugLockKey(c.workflowTask.WorkflowName, c.workflowTask.TaskID))
	cache.NewRedisCache(config2.RedisCommonCacheTokenDB()).Delete(c.prefix)
}

func (c *workflowCtl) addClusterID(clusterID string) {
	c.workflowTaskMutex.Lock()
	defer c.workflowTaskMutex.Unlock()
	c.workflowTask.ClusterIDMap[clusterID] = true
}

// mongo do not support dot in keys.
const (
	split = "@?"
)

func (c *workflowCtl) getGlobalContextAll() map[string]string {
	c.workflowTaskMutex.RLock()
	defer c.workflowTaskMutex.RUnlock()
	res := make(map[string]string, len(c.workflowTask.GlobalContext))
	for k, v := range c.workflowTask.GlobalContext {
		k = strings.Join(strings.Split(k, split), ".")
		res[k] = v
	}
	return res
}

func (c *workflowCtl) getGlobalContext(key string) (string, bool) {
	c.workflowTaskMutex.RLock()
	defer c.workflowTaskMutex.RUnlock()
	v, existed := c.workflowTask.GlobalContext[GetContextKey(key)]
	return v, existed
}

func (c *workflowCtl) setGlobalContext(key, value string) {
	c.workflowTaskMutex.Lock()
	defer c.workflowTaskMutex.Unlock()
	c.workflowTask.GlobalContext[GetContextKey(key)] = value
}

func (c *workflowCtl) globalContextEach(f func(k, v string) bool) {
	c.workflowTaskMutex.RLock()
	defer c.workflowTaskMutex.RUnlock()
	for k, v := range c.workflowTask.GlobalContext {
		k = strings.Join(strings.Split(k, split), ".")
		if !f(k, v) {
			return
		}
	}
}

func GetContextKey(key string) string {
	return strings.Join(strings.Split(key, "."), split)
}
