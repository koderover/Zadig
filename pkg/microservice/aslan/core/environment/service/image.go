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

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"go.uber.org/zap"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/setting"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
)

type UpdateContainerImageArgs struct {
	Type          string `json:"type"`
	ProductName   string `json:"product_name"`
	EnvName       string `json:"env_name"`
	ServiceName   string `json:"service_name"`
	Name          string `json:"name"`
	ContainerName string `json:"container_name"`
	Image         string `json:"image"`
	Production    bool   `json:"production"`
}

func updateContainerForHelmChart(username, serviceName, image, containerName string, product *models.Product) error {
	targetProductService := product.GetServiceMap()[serviceName]
	if targetProductService == nil {
		return fmt.Errorf("failed to find service in product: %s", serviceName)
	}

	opt := &commonrepo.ServiceFindOption{
		ServiceName: serviceName,
		Revision:    targetProductService.Revision,
		ProductName: product.ProductName,
	}

	serviceObj, err := repository.QueryTemplateService(opt, product.Production)
	if err != nil {
		log.Errorf("failed to find template service, opt %+v, err :%s", *opt, err.Error())
		err = fmt.Errorf("failed to find template service, opt %+v", *opt)
		return err
	}

	// targetProductService, err = helmservice.NewHelmDeployService().MergedContainerImage(targetProductService, []string{image})
	// if err != nil {
	// 	return fmt.Errorf("failed to merge container image, err: %s", err.Error())
	// }

	err = kube.DeploySingleHelmRelease(product, targetProductService, serviceObj, []string{image}, 0, username)
	if err != nil {
		return fmt.Errorf("failed to upgrade helm release, err: %s", err.Error())
	}
	return nil
}

func UpdateContainerImage(requestID, username string, args *UpdateContainerImageArgs, log *zap.SugaredLogger) error {
	product, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		EnvName:    args.EnvName,
		Name:       args.ProductName,
		Production: &args.Production,
	})
	if err != nil {
		return e.ErrUpdateConainterImage.AddErr(err)
	}

	namespace := product.Namespace
	kubeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(product.ClusterID)
	if err != nil {
		return e.ErrUpdateConainterImage.AddErr(err)
	}

	cls, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(product.ClusterID)
	if err != nil {
		return e.ErrUpdateConainterImage.AddErr(err)
	}
	version, err := cls.Discovery().ServerVersion()
	if err != nil {
		return e.ErrUpdateConainterImage.AddErr(err)
	}

	// aws secrets needs to be refreshed
	regs, err := commonservice.ListRegistryNamespaces("", true, log)
	if err != nil {
		log.Errorf("Failed to get registries to update container images, the error is: %s", err)
		return err
	}
	for _, reg := range regs {
		if reg.RegProvider == config.RegistryTypeAWS {
			if err := kube.CreateOrUpdateRegistrySecret(namespace, reg, false, kubeClient); err != nil {
				retErr := fmt.Errorf("failed to update pull secret for registry: %s, the error is: %s", reg.ID.Hex(), err)
				log.Errorf("%s\n", retErr.Error())
				return retErr
			}
		}
	}

	eventStart := time.Now().Unix()

	defer func() {
		commonservice.LogProductStats(namespace, setting.UpdateContainerImageEvent, args.ProductName, requestID, eventStart, log)
	}()

	// update service in helm way
	if product.Source == setting.HelmDeployType {
		serviceName, err := commonservice.GetHelmServiceName(product, args.Type, args.Name, kubeClient, version)
		if err != nil {
			return e.ErrUpdateConainterImage.AddErr(err)
		}
		err = updateContainerForHelmChart(username, serviceName, args.Image, args.ContainerName, product)
		if err != nil {
			return e.ErrUpdateConainterImage.AddErr(err)
		}
	} else {
		switch args.Type {
		case setting.Deployment:
			if err := updater.UpdateDeploymentImage(namespace, args.Name, args.ContainerName, args.Image, kubeClient); err != nil {
				log.Errorf("[%s] UpdateDeploymentImageByName error: %s", namespace, err.Error())
				return e.ErrUpdateConainterImage.AddDesc("更新 Deployment 容器镜像失败")
			}
		case setting.StatefulSet:
			if err := updater.UpdateStatefulSetImage(namespace, args.Name, args.ContainerName, args.Image, kubeClient); err != nil {
				log.Errorf("[%s] UpdateStatefulsetImageByName error: %s", namespace, err.Error())
				return e.ErrUpdateConainterImage.AddDesc("更新 StatefulSet 容器镜像失败")
			}
		case setting.CronJob:
			if err := updater.UpdateCronJobImage(namespace, args.Name, args.ContainerName, args.Image, kubeClient, VersionLessThan121(version)); err != nil {
				log.Errorf("[%s] UpdateCronJobImageByName error: %s", namespace, err.Error())
				return e.ErrUpdateConainterImage.AddDesc("更新 CronJob 容器镜像失败")
			}
		default:
			return e.ErrUpdateConainterImage.AddDesc(fmt.Sprintf("不支持的资源类型: %s", args.Type))
		}

		// update image info in product.services.container
		prodSvc := product.GetServiceMap()[args.ServiceName]
		if prodSvc == nil {
			return e.ErrUpdateConainterImage.AddDesc(fmt.Sprintf("服务 %s 不存在", args.ServiceName))
		}
		prodSvc.UpdateTime = time.Now().Unix()
		for _, container := range prodSvc.Containers {
			if container.Name == args.ContainerName {
				container.Image = args.Image
				break
			}
		}

		session := mongotool.Session()
		defer session.EndSession(context.TODO())

		err = mongotool.StartTransaction(session)
		if err != nil {
			return e.ErrUpdateConainterImage.AddErr(err)
		}

		if err := commonrepo.NewProductCollWithSession(session).Update(product); err != nil {
			log.Errorf("[%s] update product %s error: %s", namespace, args.ProductName, err.Error())
			mongotool.AbortTransaction(session)
			return e.ErrUpdateConainterImage.AddDesc("更新环境信息失败")
		}

		err = commonutil.CreateEnvServiceVersion(product, prodSvc, username, session, log)
		if err != nil {
			log.Errorf("create env service version for %s/%s error: %v", product.EnvName, prodSvc.ServiceName, err)
		}
		return mongotool.CommitTransaction(session)
	}
	return nil
}
