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

package kube

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"text/template"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/setting"
	aslanClient "github.com/koderover/zadig/v2/pkg/shared/client/aslan"
	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/koderover/zadig/v2/pkg/tool/crypto"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	redisEventBus "github.com/koderover/zadig/v2/pkg/tool/eventbus/redis"
	"github.com/koderover/zadig/v2/pkg/tool/kube/multicluster"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
)

func GetKubeAPIReader(clusterID string) (client.Reader, error) {
	return clientmanager.NewKubeClientManager().GetControllerRuntimeAPIReader(clusterID)
}

type Service struct {
	*multicluster.Agent

	coll *mongodb.K8SClusterColl
}

func NewService(hubServerAddr string) (*Service, error) {
	if hubServerAddr == "" {
		return &Service{coll: mongodb.NewK8SClusterColl()}, nil
	}

	agent, err := multicluster.NewAgent(hubServerAddr)
	if err != nil {
		return nil, err
	}

	return &Service{
		coll:  mongodb.NewK8SClusterColl(),
		Agent: agent,
	}, nil
}

func (s *Service) ListClusters(clusterType string, logger *zap.SugaredLogger) ([]*models.K8SCluster, error) {
	clusters, err := s.coll.Find(clusterType)
	if err != nil {
		logger.Errorf("failed to list clusters %v", err)
		return nil, e.ErrListK8SCluster.AddErr(err)
	}

	if len(clusters) == 0 {
		return make([]*models.K8SCluster, 0), nil
	}
	for _, cluster := range clusters {
		token, err := crypto.AesEncrypt(cluster.ID.Hex())
		if err != nil {
			return nil, err
		}
		cluster.Token = token
	}

	return clusters, nil
}

func (s *Service) CreateCluster(cluster *models.K8SCluster, id string, logger *zap.SugaredLogger) (*models.K8SCluster, error) {
	_, err := s.coll.FindByName(cluster.Name)
	if err == nil {
		logger.Errorf("failed to create cluster %s %v", cluster.Name, err)
		return nil, e.ErrCreateCluster.AddDesc(e.DuplicateClusterNameFound)
	}

	cluster.Status = setting.Pending
	if id == setting.LocalClusterID {
		cluster.Status = setting.Normal
		cluster.Local = true
		cluster.DindCfg = nil
	}
	if cluster.Type == setting.KubeConfigClusterType {
		// kube config type is always connected
		cluster.Status = setting.Normal
	}
	err = s.coll.Create(cluster, id)
	if err != nil {
		return nil, e.ErrCreateCluster.AddErr(err)
	}

	if cluster.Type == setting.KubeConfigClusterType {
		// if scheduleWorkflow==false, we don't need to create resources in the cluster
		// resource: Namespace: koderover-agent | Service: dind | StatefulSet: dind
		if cluster.AdvancedConfig != nil && cluster.AdvancedConfig.ScheduleWorkflow {
			// since we will always be able to connect with direct connection
			err := InitializeExternalCluster(cluster.ID.Hex())
			if err != nil {
				return nil, err
			}
		}

		cluster.Status = setting.Normal
		err = s.coll.UpdateStatus(cluster)
		if err != nil {
			return nil, err
		}
	}

	if cluster.AdvancedConfig != nil {
		for _, projectName := range cluster.AdvancedConfig.ProjectNames {
			err = commonrepo.NewProjectClusterRelationColl().Create(&commonmodels.ProjectClusterRelation{
				ProjectName: projectName,
				ClusterID:   cluster.ID.Hex(),
				CreatedBy:   cluster.CreatedBy,
			})
			if err != nil {
				logger.Errorf("Failed to create projectClusterRelation err:%s", err)
			}
		}
	}

	token, err := crypto.AesEncrypt(cluster.ID.Hex())
	if err != nil {
		return nil, err
	}
	cluster.Token = token

	return cluster, nil
}

func (s *Service) UpdateCluster(id string, cluster *models.K8SCluster, logger *zap.SugaredLogger) (*models.K8SCluster, error) {
	if existed, err := s.coll.HasDuplicateName(id, cluster.Name); existed || err != nil {
		if err != nil {
			logger.Warnf("failed to find duplicated name %v", err)
		}

		return nil, e.ErrUpdateCluster.AddDesc(e.DuplicateClusterNameFound)
	}

	eb := redisEventBus.New(configbase.RedisCommonCacheTokenDB())

	err := eb.Publish(setting.EventBusChannelClusterUpdate, id)
	if err != nil {
		logger.Errorf("failed to update cluster by id: %s, failed to publish deletion info to eventbus, error: %s", id, err)
		return nil, fmt.Errorf("failed to update cluster by id: %s, failed to publish deletion info to eventbus, error: %s", id, err)
	}

	err = s.coll.UpdateMutableFields(cluster, id)
	if err != nil {
		logger.Errorf("failed to update mutable fields %v", err)
		return nil, e.ErrUpdateCluster.AddErr(err)
	}

	token, err := crypto.AesEncrypt(cluster.ID.Hex())
	if err != nil {
		return nil, err
	}
	cluster.Token = token
	return cluster, nil
}

func (s *Service) DeleteCluster(user string, id string, logger *zap.SugaredLogger) error {
	eb := redisEventBus.New(configbase.RedisCommonCacheTokenDB())

	err := eb.Publish(setting.EventBusChannelClusterUpdate, id)
	if err != nil {
		logger.Errorf("failed to delete cluster by id: %s, failed to publish deletion info to eventbus, error: %s", id, err)
		return fmt.Errorf("failed to delete cluster by id: %s, failed to publish deletion info to eventbus, error: %s", id, err)
	}

	err = s.coll.Delete(id)
	if err != nil {
		logger.Errorf("failed to delete cluster by id %s %v", id, err)
		return e.ErrDeleteCluster.AddErr(err)
	}

	return nil
}

func (s *Service) GetCluster(id string, logger *zap.SugaredLogger) (*models.K8SCluster, error) {
	cluster, err := s.coll.Get(id)
	if err != nil {
		logger.Errorf("failed to get cluster by id %s %v", id, err)
		return nil, e.ErrClusterNotFound.AddErr(err)
	}

	token, err := crypto.AesEncrypt(cluster.ID.Hex())
	if err != nil {
		return nil, err
	}

	if cluster.DindCfg == nil {
		cluster.DindCfg = &commonmodels.DindCfg{
			Replicas: DefaultDindReplicas,
			Resources: &commonmodels.Resources{
				Limits: &commonmodels.Limits{
					CPU:    DefaultDindLimitsCPU,
					Memory: DefaultDindLimitsMemory,
				},
			},
			Storage: &commonmodels.DindStorage{
				Type: DefaultDindStorageType,
			},
		}
	}

	if cluster.AdvancedConfig != nil && cluster.AdvancedConfig.ClusterAccessYaml == "" {
		cluster.AdvancedConfig.ScheduleWorkflow = true
		cluster.AdvancedConfig.ClusterAccessYaml = ClusterAccessYamlTemplate
	}

	cluster.Token = token
	return cluster, nil
}

func (s *Service) GetClusterByToken(token string, logger *zap.SugaredLogger) (*models.K8SCluster, error) {
	id, err := crypto.AesDecrypt(token)
	if err != nil {
		return nil, err
	}

	return s.GetCluster(id, logger)
}

func (s *Service) ListConnectedClusters(logger *zap.SugaredLogger) ([]*models.K8SCluster, error) {
	clusters, err := s.coll.FindConnectedClusters()
	if err != nil {
		logger.Errorf("failed to list connected clusters %v", err)
		return nil, e.ErrListK8SCluster.AddErr(err)
	}

	if len(clusters) == 0 {
		return make([]*models.K8SCluster, 0), nil
	}
	for _, cluster := range clusters {
		token, err := crypto.AesEncrypt(cluster.ID.Hex())
		if err != nil {
			return nil, err
		}
		cluster.Token = token
	}

	return clusters, nil
}

func (s *Service) GetYaml(id, agentImage, aslanURL, hubURI string, useDeployment bool, logger *zap.SugaredLogger) ([]byte, error) {
	var (
		cluster *models.K8SCluster
		err     error
	)
	if cluster, err = s.GetCluster(id, logger); err != nil {
		return nil, err
	}

	var hubBase *url.URL
	hubBase, err = url.Parse(fmt.Sprintf("%s%s", aslanURL, hubURI))
	if err != nil {
		return nil, err
	}

	if strings.ToLower(hubBase.Scheme) == "https" {
		hubBase.Scheme = "wss"
	} else {
		hubBase.Scheme = "ws"
	}

	buffer := bytes.NewBufferString("")
	token, err := crypto.AesEncrypt(cluster.ID.Hex())
	if err != nil {
		return nil, err
	}

	dindReplicas, dindLimitsCPU, dindLimitsMemory, dindEnablePV, dindSCName, dindStorageSizeInGiB := getDindCfg(cluster)

	yaml := agentYaml
	if cluster.AdvancedConfig != nil {
		if cluster.AdvancedConfig.ClusterAccessYaml != "" {
			yaml = fmt.Sprintf("%s\n---\n%s", yaml, cluster.AdvancedConfig.ClusterAccessYaml)
			if cluster.AdvancedConfig.ScheduleWorkflow {
				yaml = fmt.Sprintf("%s\n---\n%s", yaml, WorkflowResourceYaml)
			}
			// if cluster.AdvancedConfig.ClusterAccessYaml is empty, it means the cluster is created before the cluster access function, so we need to contain the old one.
		} else {
			yaml = fmt.Sprintf("%s\n---\n%s", yaml, ClusterAccessYamlTemplate)
			yaml = fmt.Sprintf("%s\n---\n%s", yaml, WorkflowResourceYaml)
		}
	} else {
		yaml = fmt.Sprintf("%s\n---\n%s", yaml, ClusterAccessYamlTemplate)
		yaml = fmt.Sprintf("%s\n---\n%s", yaml, WorkflowResourceYaml)
	}

	var YamlTemplate = template.Must(template.New("agentYaml").Parse(yaml))
	scheduleWorkflow := true
	if cluster.AdvancedConfig != nil && cluster.AdvancedConfig.ClusterAccessYaml != "" {
		scheduleWorkflow = cluster.AdvancedConfig.ScheduleWorkflow
	}

	if cluster.Namespace == "" {
		err = YamlTemplate.Execute(buffer, TemplateSchema{
			HubAgentImage:        agentImage,
			ClientToken:          token,
			HubServerBaseAddr:    hubBase.String(),
			AslanBaseAddr:        configbase.SystemAddress(),
			UseDeployment:        useDeployment,
			DindReplicas:         dindReplicas,
			DindLimitsCPU:        dindLimitsCPU,
			DindLimitsMemory:     dindLimitsMemory,
			DindImage:            config.DindImage(),
			DindEnablePV:         dindEnablePV,
			DindStorageClassName: dindSCName,
			DindStorageSizeInGiB: dindStorageSizeInGiB,
			ScheduleWorkflow:     scheduleWorkflow,
			EnableIRSA:           cluster.AdvancedConfig.EnableIRSA,
			IRSARoleARN:          cluster.AdvancedConfig.IRSARoleARM,
			ImagePullPolicy:      configbase.ImagePullPolicy(),
		})
	} else {
		err = YamlTemplateForNamespace.Execute(buffer, TemplateSchema{
			HubAgentImage:        agentImage,
			ClientToken:          token,
			HubServerBaseAddr:    hubBase.String(),
			AslanBaseAddr:        configbase.SystemAddress(),
			UseDeployment:        useDeployment,
			Namespace:            cluster.Namespace,
			DindReplicas:         dindReplicas,
			DindLimitsCPU:        dindLimitsCPU,
			DindLimitsMemory:     dindLimitsMemory,
			DindImage:            config.DindImage(),
			DindEnablePV:         dindEnablePV,
			DindStorageClassName: dindSCName,
			DindStorageSizeInGiB: dindStorageSizeInGiB,
			EnableIRSA:           cluster.AdvancedConfig.EnableIRSA,
			IRSARoleARN:          cluster.AdvancedConfig.IRSARoleARM,
			ImagePullPolicy:      configbase.ImagePullPolicy(),
		})
	}

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (s *Service) UpdateUpgradeAgentInfo(id, updateHubagentErrorMsg string) error {
	_, err := s.coll.Get(id)
	if err != nil {
		return err
	}
	err = s.coll.UpdateUpgradeAgentInfo(id, updateHubagentErrorMsg)
	return err
}

func getDindCfg(cluster *models.K8SCluster) (replicas int, limitsCPU, limitsMemory string, enablePV bool, scName string, storageSizeInGiB int) {
	replicas = DefaultDindReplicas
	limitsCPU = strconv.Itoa(DefaultDindLimitsCPU) + setting.CpuUintM
	limitsMemory = strconv.Itoa(DefaultDindLimitsMemory) + setting.MemoryUintMi
	enablePV = DefaultDindEnablePV
	scName = DefaultDindStorageClassName
	storageSizeInGiB = DefaultDindStorageSizeInGiB

	if cluster.DindCfg != nil {
		if cluster.DindCfg.Replicas > 0 {
			replicas = cluster.DindCfg.Replicas
		}

		if cluster.DindCfg.Resources != nil && cluster.DindCfg.Resources.Limits != nil {
			if cluster.DindCfg.Resources.Limits.CPU > 0 {
				limitsCPU = strconv.Itoa(cluster.DindCfg.Resources.Limits.CPU) + setting.CpuUintM
			}
			if cluster.DindCfg.Resources.Limits.Memory > 0 {
				limitsMemory = strconv.Itoa(cluster.DindCfg.Resources.Limits.Memory) + setting.MemoryUintMi
			}
		}

		if cluster.DindCfg.Storage != nil && cluster.DindCfg.Storage.Type == commonmodels.DindStorageDynamic {
			enablePV = true
			scName = cluster.DindCfg.Storage.StorageClass
			storageSizeInGiB = int(cluster.DindCfg.Storage.StorageSizeInGiB)
		}
	}

	return
}

// InitializeExternalCluster initialized the resources in the cluster for zadig to run correctly.
// if the cluster is of type kubeconfig, we need to create following resource:
// Namespace: koderover-agent
// Service: dind
// StatefulSet: dind
func InitializeExternalCluster(clusterID string) error {
	clientset, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(clusterID)
	if err != nil {
		return err
	}

	namespace := "koderover-agent"
	// if no namespace named "koderover-agent" exists, we create one
	if _, err := clientset.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{}); err != nil {
		namespaceSpec := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		}}

		_, err := clientset.CoreV1().Namespaces().Create(context.TODO(), namespaceSpec, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create namespace \"koderover-agent\" in the new cluster, error: %s", err)
		}
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
	if _, err := clientset.RbacV1().Roles(namespace).Create(context.Background(), role, metav1.CreateOptions{}); err != nil {
		return errors.Errorf("cluster %s create role err: %s", clusterID, err)
	}

	// create service account
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workflow-cm-sa",
			Namespace: namespace,
		},
	}

	cluster, err := aslanClient.New(configbase.AslanServiceAddress()).GetClusterInfo(clusterID)
	if err != nil {
		return err
	}
	if cluster.AdvancedConfig.EnableIRSA {
		serviceAccount.Annotations = map[string]string{
			"eks.amazonaws.com/role-arn": cluster.AdvancedConfig.IRSARoleARM,
		}
	}

	if _, err := clientset.CoreV1().ServiceAccounts(namespace).Create(context.Background(), serviceAccount, metav1.CreateOptions{}); err != nil {
		return errors.Errorf("cluster %s create serviceAccount err: %s", clusterID, err)
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
	if _, err := clientset.RbacV1().RoleBindings(namespace).Create(context.Background(), roleBinding, metav1.CreateOptions{}); err != nil {
		return errors.Errorf("cluster %s create role binding err: %s", clusterID, err)
	}
	log.Infof("cluster %s create role binding successfully", clusterID)

	dindLabelMap := map[string]string{
		"app.kubernetes.io/component": "dind",
		"app.kubernetes.io/name":      "zadig",
	}

	privileged := true

	// Dind Stateful Set
	dindSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "dind",
			Labels: dindLabelMap,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "dind",
			Selector: &metav1.LabelSelector{
				MatchLabels: dindLabelMap,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: dindLabelMap,
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					Containers: []corev1.Container{
						corev1.Container{
							Name:  "dind",
							Image: config.DindImage(),
							Ports: []corev1.ContainerPort{
								{
									Protocol:      "TCP",
									ContainerPort: 2375,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "DOCKER_TLS_CERTDIR",
									Value: "",
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(2)),
									corev1.ResourceMemory: resource.MustParse(strconv.Itoa(2) + setting.MemoryUintGi),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(strconv.Itoa(100) + setting.CpuUintM),
									corev1.ResourceMemory: resource.MustParse(strconv.Itoa(128) + setting.MemoryUintMi),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
						},
					},
				},
			},
		},
	}

	_, err = clientset.AppsV1().StatefulSets("koderover-agent").Create(context.TODO(), dindSts, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create dind sts to initialize cluster, err: %s", err)
	}

	// dind service
	dindService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "dind",
			Labels: dindLabelMap,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "dind",
					Protocol:   "TCP",
					Port:       2375,
					TargetPort: intstr.FromInt(2375),
				},
			},
			ClusterIP: "None",
			Selector:  dindLabelMap,
		},
	}

	_, err = clientset.CoreV1().Services("koderover-agent").Create(context.TODO(), dindService, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create dind service to initialize cluster, err: %s", err)
	}

	return nil
}

func UpdateClusterHandler(message string) {
	err := clientmanager.NewKubeClientManager().Clear(message)
	if err != nil {
		log.Errorf("failed to clear old cache for clusterID: %s, error: %s", message, err)
	}
}

func ValidateClusterRoleYAML(k8sYaml string, logger *zap.SugaredLogger) error {
	resKind := new(types.KubeResourceKind)
	if err := yaml.Unmarshal([]byte(k8sYaml), &resKind); err != nil {
		msg := fmt.Errorf("the cluster access permission yaml file format does not conform to the k8s resource yaml format, yaml:%s", k8sYaml)
		logger.Error(msg)
		return e.ErrInvalidParam.AddErr(msg)
	}
	if resKind.APIVersion != "rbac.authorization.k8s.io/v1" {
		msg := fmt.Errorf("the cluster access permission yaml resource apiVersion is %s, the fixed value of apiVersion is rbac.authorization.k8s.io/v1", resKind.APIVersion)
		logger.Error(msg)
		return e.ErrInvalidParam.AddErr(msg)
	}
	if resKind.Kind != "ClusterRole" {
		msg := fmt.Errorf("the cluster access permission yaml resource kind is %s, the fixed value of kind is ClusterRole", resKind.Kind)
		logger.Error(msg)
		return e.ErrInvalidParam.AddErr(msg)
	}
	if resKind.Metadata.Name != "koderover-agent-admin" {
		msg := fmt.Errorf("the cluster access permission yaml resource name is %s, the fixed value of name is koderover-agent-admin", resKind.Metadata.Name)
		logger.Error(msg)
		return e.ErrInvalidParam.AddErr(msg)
	}
	return nil
}

type TemplateSchema struct {
	HubAgentImage        string
	ClientToken          string
	HubServerBaseAddr    string
	Namespace            string
	UseDeployment        bool
	AslanBaseAddr        string
	DindReplicas         int
	DindLimitsCPU        string
	DindLimitsMemory     string
	DindImage            string
	DindEnablePV         bool
	DindStorageClassName string
	DindStorageSizeInGiB int
	ScheduleWorkflow     bool
	EnableIRSA           bool
	IRSARoleARN          string
	ImagePullPolicy      string
}

const (
	DefaultDindReplicas         int                          = 1
	DefaultDindLimitsCPU        int                          = 4000
	DefaultDindLimitsMemory     int                          = 8192
	DefaultDindStorageType      commonmodels.DindStorageType = commonmodels.DindStorageRootfs
	DefaultDindEnablePV         bool                         = false
	DefaultDindStorageClassName string                       = ""
	DefaultDindStorageSizeInGiB int                          = 10
)

var agentYaml = `
---

apiVersion: v1
kind: Namespace
metadata:
  name: koderover-agent

---

apiVersion: v1
kind: ServiceAccount
metadata:
  name: koderover-agent
  namespace: koderover-agent

---

apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: koderover-agent-admin-binding
  namespace: koderover-agent
subjects:
- kind: ServiceAccount
  name: koderover-agent
  namespace: koderover-agent
roleRef:
  kind: ClusterRole
  name: koderover-agent-admin
  apiGroup: rbac.authorization.k8s.io

---

apiVersion: v1
kind: Service
metadata:
  name: hub-agent
  namespace: koderover-agent
  labels:
    app: koderover-agent-agent
spec:
  type: ClusterIP
  ports:
    - protocol: TCP
      port: 80
      targetPort: 80
  selector:
    app: koderover-agent-agent

---

apiVersion: apps/v1
{{- if .UseDeployment }}
kind: Deployment
{{- else }}
kind: DaemonSet
{{- end }}
metadata:
    name: koderover-agent-node-agent
    namespace: koderover-agent
spec:
  selector:
    matchLabels:
      app: koderover-agent-agent
  template:
    metadata:
      labels:
        app: koderover-agent-agent
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                - key: beta.kubernetes.io/os
                  operator: NotIn
                  values:
                    - windows
{{- if .UseDeployment }}
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
{{- end }}
      hostNetwork: true
      serviceAccountName: koderover-agent
      containers:
      - name: agent
        image: {{.HubAgentImage}}
        imagePullPolicy: {{.ImagePullPolicy}}
        env:
        - name: AGENT_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: HUB_AGENT_TOKEN
          value: "{{.ClientToken}}"
        - name: HUB_SERVER_BASE_ADDR
          value: "{{.HubServerBaseAddr}}"
        - name: ASLAN_BASE_ADDR
          value: "{{.AslanBaseAddr}}"
        - name: SCHEDULE_WORKFLOW
          value: "{{.ScheduleWorkflow}}"
        resources:
          limits:
            cpu: 1000m
            memory: 1Gi
          requests:
            cpu: 100m
            memory: 256Mi
{{- if .UseDeployment }}
  replicas: 1
{{- else }}
  updateStrategy:
    type: RollingUpdate
{{- end }}
`

var YamlTemplateForNamespace = template.Must(template.New("agentYaml").Parse(`
---

apiVersion: v1
kind: ServiceAccount
metadata:
  name: koderover-agent-sa
  namespace: {{.Namespace}}

---

apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: koderover-agent-admin-binding
  namespace: {{.Namespace}}
subjects:
- kind: ServiceAccount
  name: koderover-agent-sa
  namespace: {{.Namespace}}
roleRef:
  kind: Role
  name: koderover-agent-admin-role
  apiGroup: rbac.authorization.k8s.io

---

apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: koderover-agent-admin-role
  namespace: {{.Namespace}}
rules:
- apiGroups:
  - '*'
  resources:
  - '*'
  verbs:
  - '*'

---

apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-cm-manager
  namespace: {{.Namespace}}
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["*"]

---

kind: ServiceAccount
metadata:
  name: workflow-cm-sa
  namespace: {{.Namespace}}
  {{- if .EnableIRSA }}
  annotations:
    eks.amazonaws.com/role-arn: {{.IRSARoleARN}}
  {{- end }}

---

apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workflow-cm-rolebinding
  namespace: {{.Namespace}}
subjects:
- kind: ServiceAccount
  name: workflow-cm-sa
  namespace: {{.Namespace}}
roleRef:
  kind: Role
  name: workflow-cm-manager
  apiGroup: rbac.authorization.k8s.io

---

apiVersion: v1
kind: Service
metadata:
  name: hub-agent
  namespace: {{.Namespace}}
  labels:
    app: koderover-agent-agent
spec:
  type: ClusterIP
  ports:
    - protocol: TCP
      port: 80
      targetPort: 80
  selector:
    app: koderover-agent-agent

---

apiVersion: apps/v1
{{- if .UseDeployment }}
kind: Deployment
{{- else }}
kind: DaemonSet
{{- end }}
metadata:
    name: koderover-agent-node-agent
    namespace: {{.Namespace}}
spec:
  selector:
    matchLabels:
      app: koderover-agent-agent
  template:
    metadata:
      labels:
        app: koderover-agent-agent
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                - key: beta.kubernetes.io/os
                  operator: NotIn
                  values:
                    - windows
{{- if .UseDeployment }}
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
{{- end }}
      hostNetwork: true
      serviceAccountName: koderover-agent-sa
      containers:
      - name: agent
        image: {{.HubAgentImage}}
        imagePullPolicy: {{.ImagePullPolicy}}
        env:
        - name: AGENT_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: HUB_AGENT_TOKEN
          value: "{{.ClientToken}}"
        - name: HUB_SERVER_BASE_ADDR
          value: "{{.HubServerBaseAddr}}"
        - name: ASLAN_BASE_ADDR
          value: "{{.AslanBaseAddr}}"
        resources:
          limits:
            cpu: 1000m
            memory: 1Gi
          requests:
            cpu: 100m
            memory: 256Mi
{{- if .UseDeployment }}
  replicas: 1
{{- else }}
  updateStrategy:
    type: RollingUpdate
{{- end }}

---

apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dind
  namespace: {{.Namespace}}
  labels:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
spec:
  serviceName: dind
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/component: dind
      app.kubernetes.io/name: zadig
  template:
    metadata:
      labels:
        app.kubernetes.io/component: dind
        app.kubernetes.io/name: zadig
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
      containers:
        - name: dind
          image: {{.DindImage}}
          env:
            - name: DOCKER_TLS_CERTDIR
              value: ""
          securityContext:
            privileged: true
          ports:
            - protocol: TCP
              containerPort: 2375
          resources:
            limits:
              cpu: "4"
              memory: 8Gi
            requests:
              cpu: 100m
              memory: 128Mi
{{- if .DindEnablePV }}
          volumeMounts:
          - name: zadig-docker
            mountPath: /var/lib/docker
  volumeClaimTemplates:
  - metadata:
      name: zadig-docker
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: {{.DindStorageClassName}}
      resources:
        requests:
          storage: {{.DindStorageSizeInGiB}}Gi
{{- end }}

---

apiVersion: v1
kind: Service
metadata:
  name: dind
  namespace: {{.Namespace}}
  labels:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
spec:
  ports:
    - name: dind
      protocol: TCP
      port: 2375
      targetPort: 2375
  clusterIP: None
  selector:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
`))

var ClusterAccessYamlTemplate = `
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: koderover-agent-admin
rules:
- apiGroups:
  - '*'
  resources:
  - '*'
  verbs:
  - '*'
- nonResourceURLs:
  - '*'
  verbs:
  - '*'
`

var WorkflowResourceYaml = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-cm-manager
  namespace: koderover-agent
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["*"]

---

apiVersion: v1
kind: ServiceAccount
metadata:
  name: workflow-cm-sa
  namespace: koderover-agent
  {{- if .EnableIRSA }}
  annotations:
    eks.amazonaws.com/role-arn: {{.IRSARoleARN}}
  {{- end }}

---

apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workflow-cm-rolebinding
  namespace: koderover-agent
subjects:
- kind: ServiceAccount
  name: workflow-cm-sa
  namespace: koderover-agent
roleRef:
  kind: Role
  name: workflow-cm-manager
  apiGroup: rbac.authorization.k8s.io

---

apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dind
  namespace: koderover-agent
  labels:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
spec:
  serviceName: dind
  replicas: {{.DindReplicas}}
  selector:
    matchLabels:
      app.kubernetes.io/component: dind
      app.kubernetes.io/name: zadig
  template:
    metadata:
      labels:
        app.kubernetes.io/component: dind
        app.kubernetes.io/name: zadig
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
      containers:
        - name: dind
          image: {{.DindImage}}
          env:
            - name: DOCKER_TLS_CERTDIR
              value: ""
          securityContext:
            privileged: true
          ports:
            - protocol: TCP
              containerPort: 2375
          resources:
            limits:
              cpu: {{.DindLimitsCPU}}
              memory: {{.DindLimitsMemory}}
            requests:
              cpu: 100m
              memory: 128Mi
{{- if .DindEnablePV }}
          volumeMounts:
          - name: zadig-docker
            mountPath: /var/lib/docker
  volumeClaimTemplates:
  - metadata:
      name: zadig-docker
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: {{.DindStorageClassName}}
      resources:
        requests:
          storage: {{.DindStorageSizeInGiB}}Gi
{{- end }}

---

apiVersion: v1
kind: Service
metadata:
  name: dind
  namespace: koderover-agent
  labels:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
spec:
  ports:
    - name: dind
      protocol: TCP
      port: 2375
      targetPort: 2375
  clusterIP: None
  selector:
    app.kubernetes.io/component: dind
    app.kubernetes.io/name: zadig
`
