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

package core

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	newgoCron "github.com/go-co-op/gocron/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hashicorp/go-multierror"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllerRuntimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	commonconfig "github.com/koderover/zadig/v2/pkg/config"
	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/webhook"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/workflowcontroller"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	environmentservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/environment/service"
	multiclusterservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/multicluster/service"
	releaseplanservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/release_plan/service"
	sprintservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/sprint_management/service"
	systemservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/system/service"
	workflowservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/workflow/service/workflow"
	hubserverconfig "github.com/koderover/zadig/v2/pkg/microservice/hubserver/config"
	"github.com/koderover/zadig/v2/pkg/microservice/hubserver/core/repository/mongodb"
	mongodb2 "github.com/koderover/zadig/v2/pkg/microservice/systemconfig/core/codehost/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	redisEventBus "github.com/koderover/zadig/v2/pkg/tool/eventbus/redis"
	"github.com/koderover/zadig/v2/pkg/tool/git/gitlab"
	gormtool "github.com/koderover/zadig/v2/pkg/tool/gorm"
	"github.com/koderover/zadig/v2/pkg/tool/klock"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
	"github.com/koderover/zadig/v2/pkg/tool/rsa"
)

const (
	webhookController = iota
)

type Controller interface {
	Run(workers int, stopCh <-chan struct{})
}

func StartControllers(stopCh <-chan struct{}) {
	controllerWorkers := map[int]int{
		webhookController: 1,
	}
	controllers := map[int]Controller{
		webhookController: webhook.NewWebhookController(),
	}

	var wg sync.WaitGroup
	for name, c := range controllers {
		wg.Add(1)
		go func(name int, c Controller) {
			defer wg.Done()
			c.Run(controllerWorkers[name], stopCh)
		}(name, c)
	}

	wg.Wait()
}

func initRsaKey() {
	client, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(setting.LocalClusterID)
	if err != nil {
		log.DPanic(err)
	}
	clientset, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(setting.LocalClusterID)
	if err != nil {
		log.DPanic(err)
	}
	_, err = clientset.CoreV1().Secrets(commonconfig.Namespace()).Get(context.TODO(), setting.RSASecretName, metav1.GetOptions{})

	if err != nil {
		if apierrors.IsNotFound(err) {
			err, publicKey, privateKey := rsa.GetRsaKey()
			if err != nil {
				log.DPanic(err)
			}
			err = kube.CreateOrUpdateRSASecret(publicKey, privateKey, client)
			if err != nil {
				log.DPanic(err)
			}
		} else {
			log.DPanic(err)
		}
	}
}

func Start(ctx context.Context) {
	log.Init(&log.Config{
		Level:       commonconfig.LogLevel(),
		Filename:    commonconfig.LogFile(),
		SendToFile:  commonconfig.SendLogToFile(),
		Development: commonconfig.Mode() != setting.ReleaseMode,
	})

	initDatabaseConnection()
	initKlock()
	initReleasePlanWatcher()
	initSprintManagementWatcher()

	initService()
	initDinD()
	initResourcesForExternalClusters()

	systemservice.SetProxyConfig()

	//workflowservice.InitPipelineController()

	workflowcontroller.InitWorkflowController()

	//Parse the workload dependencies configMap, PVC, ingress, secret
	go environmentservice.StartClusterInformer()

	go StartControllers(ctx.Done())

	go multiclusterservice.ClusterApplyUpgrade()

	initRsaKey()

	initCron()

	initEventBusSubscription()
}

func Stop(ctx context.Context) {
	mongotool.Close(ctx)
	gormtool.Close()
}

var Scheduler *newgoCron.Scheduler

func initCron() {
	Scheduler, err := newgoCron.NewScheduler()
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
		return
	}

	Scheduler.NewJob(newgoCron.DurationJob(5*time.Minute), newgoCron.NewTask(func() {
		log.Infof("[CRONJOB] updating tokens for gitlab....")
		codehostList, err := mongodb2.NewCodehostColl().List(&mongodb2.ListArgs{
			Source: "gitlab",
		})

		if err != nil {
			log.Errorf("failed to list gitlab codehost err:%v", err)
			return
		}
		for _, codehost := range codehostList {
			_, err := gitlab.UpdateGitlabToken(codehost.ID, codehost.AccessToken)
			if err != nil {
				log.Errorf("failed to update gitlab token for host: %d, error: %s", codehost.ID, err)
			}
		}
		log.Infof("[CRONJOB] gitlab token updated....")
	}))

	Scheduler.NewJob(newgoCron.DailyJob(1, newgoCron.NewAtTimes(newgoCron.NewAtTime(4, 0, 0))), newgoCron.NewTask(cleanCacheFiles))

	Scheduler.Start()
}

func initService() {
	errors := new(multierror.Error)

	defer func() {
		if err := errors.ErrorOrNil(); err != nil {
			errMsg := fmt.Sprintf("New Aslan Service error: %v", err)
			log.Fatal(errMsg)
		}
	}()

	if err := workflowservice.InitMongodbMsgQueueHandler(); err != nil {
		errors = multierror.Append(errors, err)
	}
}

// initResourcesForExternalClusters create role, serviceAccount and roleBinding for custom workflow
// The custom workflow requires a serviceAccount with configMap permission
func initResourcesForExternalClusters() {
	logger := log.SugaredLogger().With("func", "initResourcesForExternalClusters")
	list, err := mongodb.NewK8sClusterColl().FindConnectedClusters()
	if err != nil {
		logger.Errorf("FindConnectedClusters err: %v", err)
		return
	}
	namespace := "koderover-agent"

	for _, cluster := range list {
		if cluster.Local || cluster.Status != hubserverconfig.Normal {
			continue
		}
		var client controllerRuntimeClient.Client
		client, err = clientmanager.NewKubeClientManager().GetControllerRuntimeClient(cluster.ID.Hex())
		if err != nil {
			logger.Errorf("GetKubeClient id-%s err: %v", cluster.ID.Hex(), err)
			return
		}

		// create role
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "workflow-cm-manager",
				Namespace: namespace,
			},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"*"},
				},
			},
		}
		if err := client.Create(context.Background(), role); err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Infof("cluster %s role is already exist", cluster.Name)
			} else {
				logger.Errorf("cluster %s create role err: %s", cluster.Name, err)
				return
			}
		}

		// create service account
		serviceAccount := &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "workflow-cm-sa",
				Namespace: namespace,
			},
		}

		if cluster.AdvancedConfig.EnableIRSA {
			serviceAccount.Annotations = map[string]string{
				"eks.amazonaws.com/role-arn": cluster.AdvancedConfig.IRSARoleARM,
			}
		}

		if err := client.Create(context.Background(), serviceAccount); err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Infof("cluster %s serviceAccount is already exist", cluster.Name)
			} else {
				logger.Errorf("cluster %s create serviceAccount err: %s", cluster.Name, err)
				return
			}
		}

		// create role binding
		roleBinding := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "workflow-cm-rolebinding",
				Namespace: namespace,
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      "workflow-cm-sa",
					Namespace: namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				Kind:     "Role",
				Name:     "workflow-cm-manager",
				APIGroup: "rbac.authorization.k8s.io",
			},
		}
		if err := client.Create(context.Background(), roleBinding); err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Infof("cluster %s role binding is already exist", cluster.Name)
			} else {
				logger.Errorf("cluster %s create role binding err: %s", cluster.Name, err)
				return
			}
		}
		logger.Infof("cluster %s done", cluster.Name)
	}
}

func initDinD() {
	err := commonutil.SyncDinDForRegistries()
	if err != nil {
		log.Fatal(err)
	}
}

func initKlock() {
	_ = klock.Init(config.Namespace())
}

// initReleasePlanWatcher watch release plan status and update release plan status
// for working after aslan restart
func initReleasePlanWatcher() {
	go releaseplanservice.WatchExecutingWorkflow()
	go releaseplanservice.WatchApproval()
}

func initSprintManagementWatcher() {
	go sprintservice.WatchExecutingSprintWorkItemTask()
}

func initDatabaseConnection() {
	err := gormtool.Open(configbase.MysqlUser(),
		configbase.MysqlPassword(),
		configbase.MysqlHost(),
		configbase.MysqlDexDB(),
	)
	if err != nil {
		log.Panicf("Failed to open database %s", configbase.MysqlDexDB())
	}

	err = gormtool.Open(configbase.MysqlUser(),
		configbase.MysqlPassword(),
		configbase.MysqlHost(),
		configbase.MysqlUserDB(),
	)
	if err != nil {
		log.Panicf("Failed to open database %s", configbase.MysqlUserDB())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// mongodb initialization
	mongotool.Init(ctx, config.MongoURI())
	if err := mongotool.Ping(ctx); err != nil {
		panic(fmt.Errorf("failed to connect to mongo, error: %s", err))
	}
}

func initEventBusSubscription() {
	eb := redisEventBus.New(configbase.RedisCommonCacheTokenDB())

	eb.RegisterHandleFunc(setting.EventBusChannelClusterUpdate, kube.UpdateClusterHandler)
	eb.Subscribe(context.Background(), setting.EventBusChannelClusterUpdate)
}
