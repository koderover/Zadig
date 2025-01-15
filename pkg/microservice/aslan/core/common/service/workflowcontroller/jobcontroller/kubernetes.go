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

package jobcontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/koderover/zadig/v2/pkg/tool/clientmanager"
	"github.com/mozillazg/go-pinyin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crClient "sigs.k8s.io/controller-runtime/pkg/client"

	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/kube"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/multicluster/service"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/kube/wrapper"
	"github.com/koderover/zadig/v2/pkg/tool/kube/containerlog"
	"github.com/koderover/zadig/v2/pkg/tool/kube/getter"
	"github.com/koderover/zadig/v2/pkg/tool/kube/updater"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	s3tool "github.com/koderover/zadig/v2/pkg/tool/s3"
	commontypes "github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/types/job"
	"github.com/koderover/zadig/v2/pkg/util"
)

const (
	BusyBoxImage                 = "koderover.tencentcloudcr.com/koderover-public/busybox:latest"
	ZadigContextDir              = "/zadig/"
	ZadigLogFile                 = ZadigContextDir + "zadig.log"
	ZadigLifeCycleFile           = ZadigContextDir + "lifecycle"
	ExecutorResourceVolumeName   = "executor-resource"
	ExecutorKubeConfigVolume     = "executor-kubeconfig"
	ExecutorVolumePath           = "/executor"
	ExecutorKubeConfigVolumePath = "/root/.kube"
	JobExecutorFile              = ExecutorVolumePath + "/jobexecutor"
	defaultSecretEmail           = "bot@koderover.com"
	registrySecretSuffix         = "-registry-secret"
	workflowConfigMapRoleSA      = "workflow-cm-sa"

	defaultRetryCount    = 3
	defaultRetryInterval = time.Second * 3

	// build job outputs key
	IMAGEKEY    = "IMAGE"
	IMAGETAGKEY = "imageTag"
)

func GetK8sClients(hubServerAddr, clusterID string) (crClient.Client, kubernetes.Interface, crClient.Reader, error) {
	controllerRuntimeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(clusterID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get controller runtime client: %s", err)
	}

	clientset, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(clusterID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get clientset: %s", err)
	}

	kubeClientReader, err := clientmanager.NewKubeClientManager().GetControllerRuntimeAPIReader(clusterID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get api reader: %s", err)
	}

	return controllerRuntimeClient, clientset, kubeClientReader, nil
}

type JobLabel struct {
	JobName string
	JobType string
}

func ensureDeleteConfigMap(namespace string, jobLabel *JobLabel, kubeClient crClient.Client) error {
	ls := getJobLabels(jobLabel)
	return updater.DeleteConfigMapsAndWait(namespace, labels.Set(ls).AsSelector(), kubeClient)
}

func ensureDeleteJob(namespace string, jobLabel *JobLabel, kubeClient crClient.Client) error {
	ls := getJobLabels(jobLabel)
	return updater.DeleteJobsAndWait(namespace, labels.Set(ls).AsSelector(), kubeClient)
}

func getJobLabelsWithCustomizeData(jobLabel *JobLabel, customizedData map[string]string) map[string]string {
	retMap := getJobLabels(jobLabel)

	// customized data does not override system data
	for k, v := range customizedData {
		if _, ok := retMap[k]; ok {
			continue
		} else {
			retMap[k] = v
		}
	}

	return retMap
}

// getJobLabels get labels k-v map from JobLabel struct
func getJobLabels(jobLabel *JobLabel) map[string]string {
	retMap := map[string]string{
		setting.JobLabelNameKey:  strings.Replace(jobLabel.JobName, "_", "-", -1),
		setting.JobLabelSTypeKey: strings.Replace(jobLabel.JobType, "_", "-", -1),
	}
	// no need to add labels with empty value to a job
	for k, v := range retMap {
		if len(v) == 0 {
			delete(retMap, k)
		}
	}
	return retMap
}

func GetJobContainerName(name string) string {
	pyArgs := pinyin.NewArgs()
	pyArgs.Fallback = func(r rune, a pinyin.Args) []string {
		return []string{string(r)}
	}

	res := pinyin.Pinyin(name, pyArgs)

	pinyins := make([]string, 0)
	for _, py := range res {
		pinyins = append(pinyins, strings.Join(py, ""))
	}

	resp := strings.Join(pinyins, "")
	if len(resp) > 63 {
		resp = strings.TrimSuffix(resp[:63], "-")
		return resp
	}
	resp = strings.TrimSuffix(resp, "-")
	return resp
}

func createJobConfigMap(namespace, jobName string, jobLabel *JobLabel, jobCtx string, kubeClient crClient.Client) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels:    getJobLabels(jobLabel),
		},
		Data: map[string]string{
			"job-config.xml": jobCtx,
		},
	}

	return updater.CreateConfigMap(cm, kubeClient)
}

func getBaseImage(buildOS, imageFrom string) string {
	// for built-in image, reaperImage and buildOs can generate a complete image
	// reaperImage: koderover.tencentcloudcr.com/koderover-public/build-base:${BuildOS}-amd64
	// buildOS: focal xenial bionic
	jobImage := strings.ReplaceAll(config.ReaperImage(), "${BuildOS}", buildOS)
	// for custom image, buildOS represents the exact custom image
	if imageFrom == setting.ImageFromCustom {
		jobImage = buildOS
	}
	return jobImage
}

func buildPlainJob(jobName string, resReq setting.Request, resReqSpec setting.RequestSpec, jobTask *commonmodels.JobTask, jobTaskSpec *commonmodels.JobTaskPluginSpec, workflowCtx *commonmodels.WorkflowTaskCtx, customLabels, customAnnotations map[string]string) (*batchv1.Job, error) {
	collectJobOutput := `OLD_IFS=$IFS
export IFS=","
files='%s'
outputs='%s'
file_arr=($files)
output_arr=($outputs)
IFS="$OLD_IFS"
result="{"
for i in ${!file_arr[@]};
do
	file_value=$(cat ${file_arr[$i]})
	output_value=${output_arr[$i]}
	result="$result\"$output_value\":\"$file_value\","
done
result=$(sed 's/,$/}/' <<< $result)
echo $result > %s
`
	files := []string{}
	outputs := []string{}
	for _, output := range jobTask.Outputs {
		outputFile := path.Join(job.JobOutputDir, output.Name)
		files = append(files, outputFile)
		outputs = append(outputs, output.Name)
	}
	collectJobOutputCommand := fmt.Sprintf(collectJobOutput, strings.Join(files, ","), strings.Join(outputs, ","), job.JobTerminationFile)

	labels := getJobLabelsWithCustomizeData(&JobLabel{
		JobType: string(jobTask.JobType),
		JobName: jobTask.K8sJobName,
	}, customLabels)

	ImagePullSecrets, err := getImagePullSecrets(jobTaskSpec.Properties.Registries)
	if err != nil {
		return nil, err
	}

	envs := []corev1.EnvVar{}
	for _, env := range jobTaskSpec.Plugin.Envs {
		envs = append(envs, corev1.EnvVar{Name: env.Name, Value: env.Value})
	}

	clusterID := jobTaskSpec.Properties.ClusterID
	if clusterID == "" {
		clusterID = setting.LocalClusterID
	}
	// fetch cluster to get nodeAffinity and tolerations
	targetCluster, err := service.GetCluster(clusterID, log.SugaredLogger())
	if err != nil {
		return nil, fmt.Errorf("failed to find target cluster %s, err: %s", clusterID, err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Labels:      labels,
			Annotations: customAnnotations,
		},
		Spec: batchv1.JobSpec{
			Completions:  int32Ptr(1),
			Parallelism:  int32Ptr(1),
			BackoffLimit: int32Ptr(0),
			// in case finished zombie job not cleaned up by zadig
			TTLSecondsAfterFinished: int32Ptr(3600),
			// in case zombie job never stop
			ActiveDeadlineSeconds: int64Ptr(jobTaskSpec.Properties.Timeout*60 + 3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: customAnnotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: ImagePullSecrets,
					Containers: []corev1.Container{
						{
							ImagePullPolicy: util.ToPullPolicy(configbase.ImagePullPolicy()),
							Name:            GetJobContainerName(jobTask.Name),
							Image:           jobTaskSpec.Plugin.Image,
							Args:            jobTaskSpec.Plugin.Args,
							Command:         jobTaskSpec.Plugin.Cmds,
							Lifecycle: &corev1.Lifecycle{
								PreStop: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "-c", collectJobOutputCommand},
									},
								},
							},
							Env: envs,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "zadig-context",
									MountPath: ZadigContextDir,
								},
								{
									Name:      "zadig-output",
									MountPath: job.JobOutputDir,
								},
							},
							Resources: getResourceRequirements(resReq, resReqSpec),

							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							TerminationMessagePath:   job.JobTerminationFile,
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "zadig-context",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "zadig-output",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					Tolerations: commonutil.BuildTolerations(targetCluster.AdvancedConfig, jobTaskSpec.Properties.StrategyID),
					Affinity:    commonutil.AddNodeAffinity(targetCluster.AdvancedConfig, jobTaskSpec.Properties.StrategyID),
				},
			},
		},
	}
	setJobShareStorages(job, workflowCtx, jobTaskSpec.Properties.ShareStorageDetails, targetCluster)
	ensureVolumeMounts(job)
	return job, nil
}

func buildJob(jobType, jobImage, jobName, clusterID, currentNamespace string, resReq setting.Request, resReqSpec setting.RequestSpec, jobTask *commonmodels.JobTask, jobTaskSpec *commonmodels.JobTaskFreestyleSpec, workflowCtx *commonmodels.WorkflowTaskCtx, customLabels, customAnnotations map[string]string) (*batchv1.Job, error) {
	var (
		jobExecutorBootingScript string
		jobExecutorBinaryFile    = JobExecutorFile
	)

	if clusterID == "" {
		clusterID = setting.LocalClusterID
	}
	// fetch cluster to get nodeAffinity and tolerations
	targetCluster, err := service.GetCluster(clusterID, log.SugaredLogger())
	if err != nil {
		return nil, fmt.Errorf("failed to find target cluster %s, err: %s", clusterID, err)
	}

	jobExecutorBootingScript = fmt.Sprintf("mkdir %sdebug;", ZadigContextDir)
	if jobTask.BreakpointBefore {
		jobExecutorBootingScript += fmt.Sprintf("touch %sdebug/breakpoint_before;", ZadigContextDir)
	}
	if jobTask.BreakpointAfter {
		jobExecutorBootingScript += fmt.Sprintf("touch %sdebug/breakpoint_after;", ZadigContextDir)
	}
	jobExecutorBootingScript += jobExecutorBinaryFile

	labels := getJobLabelsWithCustomizeData(&JobLabel{
		JobType: jobType,
		JobName: jobTask.K8sJobName,
	}, customLabels)

	ImagePullSecrets, err := getImagePullSecrets(jobTaskSpec.Properties.Registries)
	if err != nil {
		return nil, err
	}

	var commands []string
	var serviceAccountName string

	if targetCluster.Type == setting.AgentClusterType {
		commands = []string{"/bin/sh", "-c", fmt.Sprintf("cp /app/* %s", ExecutorVolumePath)}

		if clusterID != setting.LocalClusterID {
			serviceAccountName = config.AgentTypeZadigDefaultServiceAccountName
		} else {
			controllerRuntimeClient, err := clientmanager.NewKubeClientManager().GetControllerRuntimeClient(clusterID)
			if err != nil {
				return nil, fmt.Errorf("failed to create client for target cluster %s, err: %s", clusterID, err)
			}
			deploy, deployExists, err := getter.GetDeployment(config.Namespace(), "aslan", controllerRuntimeClient)
			if err != nil || !deployExists {
				return nil, fmt.Errorf("failed to find aslan deployment to determine the correct service account, error: %s", err)
			}

			serviceAccountName = deploy.Spec.Template.Spec.ServiceAccountName
		}

	} else {
		commands = []string{"/bin/sh", "-c", fmt.Sprintf(
			`cp /app/* %s &&
cat <<EOF > /root/.kube/config
%s
EOF`,
			ExecutorVolumePath,
			targetCluster.KubeConfig)}
		serviceAccountName = workflowConfigMapRoleSA
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Labels:      labels,
			Annotations: customAnnotations,
		},
		Spec: batchv1.JobSpec{
			Completions:  int32Ptr(1),
			Parallelism:  int32Ptr(1),
			BackoffLimit: int32Ptr(0),
			// in case finished zombie job not cleaned up by zadig
			TTLSecondsAfterFinished: int32Ptr(3600),
			// in case zombie job never stop
			ActiveDeadlineSeconds: int64Ptr(jobTaskSpec.Properties.Timeout*60 + 3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: customAnnotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ImagePullSecrets:   ImagePullSecrets,
					ServiceAccountName: serviceAccountName,
					InitContainers: []corev1.Container{
						{
							ImagePullPolicy: corev1.PullIfNotPresent,
							Name:            "executor-resource-init",
							Image:           config.ExecutorImage(),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      ExecutorResourceVolumeName,
									MountPath: ExecutorVolumePath,
								},
								{
									Name:      ExecutorKubeConfigVolume,
									MountPath: ExecutorKubeConfigVolumePath,
								},
							},
							Command: commands,
						},
					},
					Containers: []corev1.Container{
						{
							ImagePullPolicy: util.ToPullPolicy(configbase.ImagePullPolicy()),
							Name:            GetJobContainerName(strings.ReplaceAll(jobTask.Name, "_", "-")),
							Image:           jobImage,
							Command:         []string{"/bin/sh", "-c"},
							Args:            []string{jobExecutorBootingScript},
							Env:             getEnvs(workflowCtx.ConfigMapMountDir, jobTaskSpec),
							VolumeMounts:    getVolumeMounts(workflowCtx.ConfigMapMountDir, jobTaskSpec.Properties.UseHostDockerDaemon),
							Resources:       getResourceRequirements(resReq, resReqSpec),

							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							TerminationMessagePath:   job.JobTerminationFile,
						},
					},
					Volumes:     getVolumes(jobName, jobTaskSpec.Properties.UseHostDockerDaemon),
					Tolerations: commonutil.BuildTolerations(targetCluster.AdvancedConfig, jobTaskSpec.Properties.StrategyID),
					Affinity:    commonutil.AddNodeAffinity(targetCluster.AdvancedConfig, jobTaskSpec.Properties.StrategyID),
				},
			},
		},
	}

	setJobShareStorages(job, workflowCtx, jobTaskSpec.Properties.ShareStorageDetails, targetCluster)

	if jobTaskSpec.Properties.CacheEnable && jobTaskSpec.Properties.Cache.MediumType == commontypes.NFSMedium {
		volumeName := "build-cache"
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: jobTaskSpec.Properties.Cache.NFSProperties.PVC,
				},
			},
		})

		mountPath := strings.ReplaceAll(jobTaskSpec.Properties.CacheUserDir, "$WORKSPACE", workflowCtx.Workspace)
		if jobTaskSpec.Properties.CacheDirType == commontypes.WorkspaceCacheDir {
			mountPath = workflowCtx.Workspace
		}

		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			SubPath:   jobTaskSpec.Properties.Cache.NFSProperties.Subpath,
		})
	}
	ensureVolumeMounts(job)
	return job, nil
}

func BuildCleanJob(jobName, clusterID, workflowName string, taskID int64) (*batchv1.Job, error) {
	workspace := "/workspace"
	shareStorageDir := commontypes.GetShareStorageSubPathPrefix(workflowName, taskID)
	image := strings.ReplaceAll(config.ReaperImage(), "${BuildOS}", "focal")
	targetCluster, err := service.GetCluster(clusterID, log.SugaredLogger())
	if err != nil {
		return nil, fmt.Errorf("failed to find target cluster %s, err: %s", clusterID, err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
		},
		Spec: batchv1.JobSpec{
			Completions:  int32Ptr(1),
			Parallelism:  int32Ptr(1),
			BackoffLimit: int32Ptr(0),
			// in case finished zombie job not cleaned up by zadig
			TTLSecondsAfterFinished: int32Ptr(3600),
			// in case zombie job never stop
			ActiveDeadlineSeconds: int64Ptr(3600),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							ImagePullPolicy: util.ToPullPolicy(configbase.ImagePullPolicy()),
							Name:            jobName,
							Image:           image,
							WorkingDir:      workspace,
							Command:         []string{"/bin/sh", "-c"},
							Args:            []string{fmt.Sprintf("rm -rf %s", shareStorageDir)},

							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							TerminationMessagePath:   job.JobTerminationFile,
						},
					},
					Tolerations: commonutil.BuildTolerations(targetCluster.AdvancedConfig, ""),
					Affinity:    commonutil.AddNodeAffinity(targetCluster.AdvancedConfig, ""),
				},
			},
		},
	}
	shareStorageName := "share-storage"
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: shareStorageName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: targetCluster.ShareStorage.NFSProperties.PVC,
			},
		},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      shareStorageName,
		MountPath: workspace,
	})
	return job, nil
}

func setJobShareStorages(job *batchv1.Job, workflowCtx *commonmodels.WorkflowTaskCtx, storageDetails []*commonmodels.StorageDetail, cluster *commonmodels.K8SCluster) {
	if cluster == nil {
		return
	}
	if cluster.ShareStorage.MediumType != commontypes.NFSMedium {
		return
	}
	if cluster.ShareStorage.NFSProperties.PVC == "" {
		return
	}
	if len(storageDetails) <= 0 {
		return
	}
	// save cluster id so we can clean up share storage later
	workflowCtx.ClusterIDAdd(cluster.ID.Hex())

	volumeName := "share-storage"
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: cluster.ShareStorage.NFSProperties.PVC,
			},
		},
	})
	for _, storageDetail := range storageDetails {
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: storageDetail.MountPath,
			SubPath:   storageDetail.SubPath,
		})
	}
}

func ensureVolumeMounts(job *batchv1.Job) {
	for i := range job.Spec.Template.Spec.Containers {
		mountPathMap := make(map[string]bool)
		volumeMounts := []corev1.VolumeMount{}
		for _, volumeMount := range job.Spec.Template.Spec.Containers[i].VolumeMounts {
			if _, ok := mountPathMap[volumeMount.MountPath]; !ok {
				volumeMounts = append(volumeMounts, volumeMount)
				mountPathMap[volumeMount.MountPath] = true
			}
		}
		job.Spec.Template.Spec.Containers[i].VolumeMounts = volumeMounts
	}
}

func getImagePullSecrets(registries []*commonmodels.RegistryNamespace) ([]corev1.LocalObjectReference, error) {
	ImagePullSecrets := []corev1.LocalObjectReference{
		{
			Name: setting.DefaultImagePullSecret,
		},
	}
	for _, reg := range registries {
		secretName, err := kube.GenRegistrySecretName(reg)
		if err != nil {
			return ImagePullSecrets, fmt.Errorf("failed to generate registry secret name: %s", err)
		}

		secret := corev1.LocalObjectReference{
			Name: secretName,
		}
		ImagePullSecrets = append(ImagePullSecrets, secret)
	}
	return ImagePullSecrets, nil
}

func getEnvs(configMapMountDir string, jobTaskSpec *commonmodels.JobTaskFreestyleSpec) []corev1.EnvVar {
	ret := make([]corev1.EnvVar, 0)
	ret = append(ret, corev1.EnvVar{
		Name:  setting.JobConfigFile,
		Value: path.Join(configMapMountDir, "job-config.xml"),
	})

	if !jobTaskSpec.Properties.UseHostDockerDaemon {
		ret = append(ret, corev1.EnvVar{
			Name:  setting.DockerHost,
			Value: jobTaskSpec.Properties.DockerHost,
		})
	}
	ret = append(ret, corev1.EnvVar{
		Name:  setting.ENVLogLevel,
		Value: config.ExecutorLogLevel(),
	})

	return ret
}

func getVolumeMounts(configMapMountDir string, userHostDockerDaemon bool) []corev1.VolumeMount {
	resp := make([]corev1.VolumeMount, 0)

	resp = append(resp, corev1.VolumeMount{
		Name:      "job-config",
		MountPath: configMapMountDir,
	})
	resp = append(resp, corev1.VolumeMount{
		Name:      "zadig-context",
		MountPath: ZadigContextDir,
	})
	resp = append(resp, corev1.VolumeMount{
		Name:      ExecutorResourceVolumeName,
		MountPath: ExecutorVolumePath,
	})
	resp = append(resp, corev1.VolumeMount{
		Name:      ExecutorKubeConfigVolume,
		MountPath: ExecutorKubeConfigVolumePath,
	})
	if userHostDockerDaemon {
		resp = append(resp, corev1.VolumeMount{
			Name:      "docker-sock",
			MountPath: setting.DefaultDockSock,
		})
	}
	return resp
}

func getVolumes(jobName string, userHostDockerDaemon bool) []corev1.Volume {
	resp := make([]corev1.Volume, 0)
	resp = append(resp, corev1.Volume{
		Name: "job-config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: jobName,
				},
			},
		},
	})
	resp = append(resp, corev1.Volume{
		Name: "zadig-context",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
	resp = append(resp, corev1.Volume{
		Name: ExecutorResourceVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
	resp = append(resp, corev1.Volume{
		Name: ExecutorKubeConfigVolume,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	if userHostDockerDaemon {
		resp = append(resp, corev1.Volume{
			Name: "docker-sock",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: setting.DefaultDockSock,
					Type: nil,
				},
			},
		})
	}

	return resp
}

func getResourceRequirements(resReq setting.Request, resReqSpec setting.RequestSpec) corev1.ResourceRequirements {

	switch resReq {
	case setting.HighRequest:
		return generateResourceRequirements(setting.HighRequest, setting.HighRequestSpec)

	case setting.MediumRequest:
		return generateResourceRequirements(setting.MediumRequest, setting.MediumRequestSpec)

	case setting.LowRequest:
		return generateResourceRequirements(setting.LowRequest, setting.LowRequestSpec)

	case setting.MinRequest:
		return generateResourceRequirements(setting.MinRequest, setting.MinRequestSpec)

	case setting.DefineRequest:
		return generateResourceRequirements(resReq, resReqSpec)

	default:
		return generateResourceRequirements(setting.DefaultRequest, setting.DefaultRequestSpec)
	}
}

// generateResourceRequirements
// cpu Request:Limit=1:4
// memory default Request:Limit=1:4 ; if memoryLimit>= 8Gi,Request:Limit=1:8
func generateResourceRequirements(req setting.Request, reqSpec setting.RequestSpec) corev1.ResourceRequirements {
	limits := corev1.ResourceList{}
	requests := corev1.ResourceList{}

	if reqSpec.CpuLimit != 0 {
		limits[corev1.ResourceCPU] = resource.MustParse(strconv.Itoa(reqSpec.CpuLimit) + setting.CpuUintM)
	}
	if reqSpec.CpuReq != 0 {
		requests[corev1.ResourceCPU] = resource.MustParse(strconv.Itoa(reqSpec.CpuReq) + setting.CpuUintM)
	}
	if reqSpec.MemoryReq != 0 {
		requests[corev1.ResourceMemory] = resource.MustParse(strconv.Itoa(reqSpec.MemoryReq) + setting.MemoryUintMi)
	}
	if reqSpec.MemoryLimit != 0 {
		limits[corev1.ResourceMemory] = resource.MustParse(strconv.Itoa(reqSpec.MemoryLimit) + setting.MemoryUintMi)
	}

	// add gpu limit
	if len(reqSpec.GpuLimit) > 0 {
		reqSpec.GpuLimit = strings.ReplaceAll(reqSpec.GpuLimit, " ", "")
		requestPair := strings.Split(reqSpec.GpuLimit, ":")
		if len(requestPair) == 2 {
			limits[corev1.ResourceName(requestPair[0])] = resource.MustParse(requestPair[1])
		}
	}

	return corev1.ResourceRequirements{
		Limits:   limits,
		Requests: requests,
	}
}

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }

func WaitPlainJobEnd(ctx context.Context, taskTimeout int, namespace, jobName string, kubeClient crClient.Client, apiServer crClient.Reader, xl *zap.SugaredLogger) config.Status {
	timeout := time.After(time.Duration(taskTimeout) * time.Minute)
	status, err := waitJobStart(ctx, namespace, jobName, kubeClient, apiServer, timeout, xl)
	if err != nil {
		xl.Errorf("wait job start error: %v", err)
	}
	if status != config.StatusRunning {
		return status
	}
	return waitPlainJobEnd(ctx, taskTimeout, timeout, namespace, jobName, kubeClient, xl)
}

func waitPlainJobEnd(ctx context.Context, taskTimeout int, timeout <-chan time.Time, namespace, jobName string, kubeClient crClient.Client, xl *zap.SugaredLogger) config.Status {
	// wait for the job to end.
	xl.Infof("wait job to end: %s %s", namespace, jobName)
	for {
		select {
		case <-ctx.Done():
			return config.StatusCancelled

		case <-timeout:
			return config.StatusTimeout

		default:
			job, found, err := getter.GetJob(namespace, jobName, kubeClient)
			if err != nil || !found {
				xl.Errorf("failed to get pod with label job-name=%s %v", jobName, err)
				return config.StatusFailed
			}

			if job.Status.Succeeded != 0 {
				return config.StatusPassed
			}
			if job.Status.Failed != 0 {
				return config.StatusFailed
			}
		}

		time.Sleep(time.Second * 1)
	}
}

func waitJobStart(ctx context.Context, namespace, jobName string, kubeClient crClient.Client, apiReader client.Reader, timeout <-chan time.Time, xl *zap.SugaredLogger) (config.Status, error) {
	xl.Infof("wait job to start: %s/%s", namespace, jobName)
	xl.Infof("Timeout of preparing Pod: %s.", 120*time.Second)
	waitPodReadyTimeout := time.After(120 * time.Second)

	var podReadyTimeout bool
	for {
		select {
		case <-ctx.Done():
			return config.StatusCancelled, nil
		case <-timeout:
			return config.StatusTimeout, fmt.Errorf("wait job ready timeout")
		case <-waitPodReadyTimeout:
			podReadyTimeout = true
		default:
			job, _, err := getter.GetJob(namespace, jobName, kubeClient)
			if err != nil {
				xl.Errorf("get job failed, namespace:%s, jobName:%s, err:%v", namespace, jobName, err)
			}
			if job != nil {
				// Should ensure the status of pod is running
				podList, err := getter.ListPods(namespace, labels.Set(getJobLabels(&JobLabel{
					JobName: jobName,
				})).AsSelector(), kubeClient)
				if err != nil {
					xl.Errorf("list pod failed, namespace:%s, jobName:%s, err:%v", namespace, jobName, err)
					time.Sleep(time.Second)
					continue
				}
				for _, pod := range podList {
					if pod.Status.Phase == corev1.PodFailed {
						msg := ""
						for _, condition := range pod.Status.Conditions {
							msg += fmt.Sprintf("type:%s, status:%s, reason:%s, message:%s\n", condition.Type, condition.Status, condition.Reason, condition.Message)
						}
						return config.StatusFailed, fmt.Errorf("waitJobStart: pod failed, jobName:%s, podName:%s\nconditions info: %s", jobName, pod.Name, msg)
					}
					if pod.Status.Phase != corev1.PodPending {
						xl.Infof("waitJobStart: pod status %s namespace:%s, jobName:%s podList num %d", pod.Status.Phase, namespace, jobName, len(podList))
						return config.StatusRunning, nil
					}
					// if pod is still pending afer 2 minutes, check pod events if is failed already
					if !podReadyTimeout {
						continue
					}
					if err := isPodFailed(pod.Name, namespace, apiReader, xl); err != nil {
						return config.StatusFailed, err
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func isPodFailed(podName, namespace string, apiReader client.Reader, xl *zap.SugaredLogger) error {
	selector := fields.Set{"involvedObject.name": podName, "involvedObject.kind": setting.Pod}.AsSelector()
	events, err := getter.ListEvents(namespace, selector, apiReader)
	if err != nil {
		// list events error is not fatal
		xl.Errorf("list events failed: %s", err)
		return nil
	}
	var errMsg string
	for _, event := range events {
		if event.Type != "Warning" {
			continue
		}
		// FailedScheduling means there is not enough resource to schedule the pod, so we should not fail the pod
		if event.Reason == "FailedScheduling" {
			continue
		}
		errMsg = errMsg + fmt.Sprintf("pod %s/%s event: %s\n", namespace, podName, event.Message)
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func waitJobEndByCheckingConfigMap(ctx context.Context, taskTimeout <-chan time.Time, namespace, jobName string, checkFile bool, informer informers.SharedInformerFactory, jobTask *commonmodels.JobTask, ack func(), xl *zap.SugaredLogger) (status config.Status, errMsg string) {
	xl.Infof("wait job to end: %s %s", namespace, jobName)
	for {
		select {
		case <-ctx.Done():
			return config.StatusCancelled, ""
		case <-taskTimeout:
			return config.StatusTimeout, ""
		default:
			jobLister := informer.Batch().V1().Jobs().Lister().Jobs(namespace)
			cmLister := informer.Core().V1().ConfigMaps().Lister().ConfigMaps(namespace)

			job, err := jobLister.Get(jobName)
			if err != nil {
				errMsg := fmt.Sprintf("failed to get job pod job-name=%s %v", jobName, err)
				xl.Errorf(errMsg)
				return config.StatusFailed, errMsg
			}
			// configMap name is the same as the k8s job name
			cm, err := cmLister.Get(jobName)
			if err != nil {
				errMsg := fmt.Sprintf("failed to get job context configMap job-name=%s %v", jobName, err)
				xl.Errorf(errMsg)
				return config.StatusFailed, errMsg
			}
			// pod is still running
			switch {
			case job.Status.Active != 0:
				podLister := informer.Core().V1().Pods().Lister().Pods(namespace)
				pods, err := podLister.List(labels.Set{"job-name": jobName}.AsSelector())
				if err != nil {
					errMsg := fmt.Sprintf("failed to find pod with label job-name=%s %v", jobName, err)
					xl.Errorf(errMsg)
					return config.StatusFailed, errMsg
				}
				for _, pod := range pods {
					commonrepo.NewWaitPodFinishLogColl().Create(
						&commonrepo.WaitPodFinishLog{
							Namespace: namespace,
							PodName:   pod.Name,
							JobName:   jobName,
							JobStatus: job.Status.String(),
							Status:    string(pod.Status.Phase),
						})

					ipod := wrapper.Pod(pod)
					if ipod.Pending() {
						continue
					}
					if ipod.Failed() {
						return config.StatusFailed, ""
					}
					if !ipod.Finished() {
						// check container whether is stuck in debug stage by checking stage file, if so, update job status to debug
						switch cm.Data[commontypes.JobDebugStatusKey] {
						case commontypes.JobDebugStatusBefore:
							jobTask.Status = config.StatusDebugBefore
							ack()
						case commontypes.JobDebugStatusAfter:
							jobTask.Status = config.StatusDebugAfter
							ack()
						case commontypes.JobDebugStatusNotIn:
							if jobTask.Status == config.StatusDebugBefore || jobTask.Status == config.StatusDebugAfter {
								jobTask.Status = config.StatusRunning
								ack()
							}
						}
					}
				}
			case job.Status.Succeeded != 0:
				return config.StatusPassed, ""
			case job.Status.Failed != 0:
				return config.StatusFailed, ""
			}
			if status, ok := cm.Data[commontypes.JobResultKey]; ok {
				switch commontypes.JobStatus(status) {
				case commontypes.JobFail:
					return config.StatusFailed, ""
				default:
					return config.StatusPassed, ""
				}
			}
		}

		time.Sleep(time.Second * 1)
	}
}

func getJobOutputFromTerminalMsg(namespace, containerName string, jobTask *commonmodels.JobTask, workflowCtx *commonmodels.WorkflowTaskCtx, kubeClient crClient.Client) error {
	jobLabel := &JobLabel{
		JobType: string(jobTask.JobType),
		JobName: jobTask.K8sJobName,
	}
	outputs := []*job.JobOutput{}
	ls := getJobLabels(jobLabel)
	pods, err := getter.ListPods(namespace, labels.Set(ls).AsSelector(), kubeClient)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		ipod := wrapper.Pod(pod)
		// only collect succeeed job outputs.
		if !ipod.Succeeded() {
			return nil
		}
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name != containerName {
				continue
			}
			if containerStatus.State.Terminated != nil && len(containerStatus.State.Terminated.Message) != 0 {
				if err := json.Unmarshal([]byte(containerStatus.State.Terminated.Message), &outputs); err != nil {
					return err
				}
			}
		}
	}
	writeOutputs(outputs, jobTask.Key, workflowCtx)
	return nil
}

func getJobOutputFromConfigMap(namespace, containerName string, jobTask *commonmodels.JobTask, workflowCtx *commonmodels.WorkflowTaskCtx, informer informers.SharedInformerFactory) error {
	cmLister := informer.Core().V1().ConfigMaps().Lister().ConfigMaps(namespace)
	// configMap name is the same as the k8sJobName
	cm, err := cmLister.Get(jobTask.K8sJobName)
	if err != nil {
		return errors.Wrap(err, "get config map")
	}
	outputs := []*job.JobOutput{}
	if len(cm.Data[commontypes.JobOutputsKey]) != 0 {
		if err := json.Unmarshal([]byte(cm.Data[commontypes.JobOutputsKey]), &outputs); err != nil {
			return errors.Wrap(err, "unmarshal outputs")
		}

		writeOutputs(outputs, jobTask.Key, workflowCtx)
	}
	return nil
}

// @var write jobs output info to globalcontext so other job can use like this {{.job.jobKey.output.outputName}}
func writeOutputs(outputs []*job.JobOutput, outputKey string, workflowCtx *commonmodels.WorkflowTaskCtx) {
	outputsMap := make(map[string]*job.JobOutput)
	for _, output := range outputs {
		outputsMap[output.Name] = output
	}
	if tag, ok := outputsMap[IMAGETAGKEY]; ok {
		if image, ok := outputsMap[IMAGEKEY]; ok {
			tag.Value = getTagFromImageName(image.Value)
		}
	}
	for _, output := range outputsMap {
		workflowCtx.GlobalContextSet(job.GetJobOutputKey(outputKey, output.Name), output.Value)
	}
}

func getTagFromImageName(imageName string) string {
	parts := strings.Split(imageName, ":")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}

	return "latest"
}

func saveContainerLog(namespace, clusterID, workflowName, jobName string, taskID int64, jobLabel *JobLabel, kubeClient crClient.Client) error {
	selector := labels.Set(getJobLabels(jobLabel)).AsSelector()
	pods, err := getter.ListPods(namespace, selector, kubeClient)
	if err != nil {
		return err
	}

	if len(pods) < 1 {
		return fmt.Errorf("no pod found with selector: %s", selector)
	}

	if len(pods[0].Status.ContainerStatuses) < 1 {
		return fmt.Errorf("no container statuses : %s", selector)
	}

	buf := new(bytes.Buffer)
	// 默认取第一个build job的第一个pod的第一个container的日志
	sort.SliceStable(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
	})

	clientSet, err := clientmanager.NewKubeClientManager().GetKubernetesClientSet(clusterID)
	if err != nil {
		log.Errorf("saveContainerLog, get client set error: %s", err)
		return err
	}

	if err := containerlog.GetContainerLogs(namespace, pods[0].Name, pods[0].Spec.Containers[0].Name, false, int64(0), buf, clientSet); err != nil {
		return fmt.Errorf("failed to get container logs: %s", err)
	}

	store, err := commonrepo.NewS3StorageColl().FindDefault()
	if err != nil {
		return fmt.Errorf("failed to get default s3 storage: %s", err)
	}

	if tempFileName, err := util.GenerateTmpFile(); err == nil {
		defer func() {
			_ = os.Remove(tempFileName)
		}()
		if err = saveFile(buf, tempFileName); err == nil {

			if store.Subfolder != "" {
				store.Subfolder = fmt.Sprintf("%s/%s/%d/%s", store.Subfolder, workflowName, taskID, "log")
			} else {
				store.Subfolder = fmt.Sprintf("%s/%d/%s", workflowName, taskID, "log")
			}
			s3client, err := s3tool.NewClient(store.Endpoint, store.Ak, store.Sk, store.Region, store.Insecure, store.Provider)
			if err != nil {
				return fmt.Errorf("saveContainerLog s3 create client error: %v", err)
			}
			fileName := strings.Replace(jobName, "_", "-", -1)
			objectKey := GetObjectPath(store.Subfolder, fileName+".log")
			if err = s3client.Upload(
				store.Bucket,
				tempFileName,
				objectKey,
			); err != nil {
				return fmt.Errorf("saveContainerLog s3 Upload error: %v", err)
			}
		} else {
			return fmt.Errorf("saveContainerLog saveFile error: %v", err)
		}
	} else {
		return fmt.Errorf("saveContainerLog GenerateTmpFile error: %v", err)
	}
	return nil
}

func GetObjectPath(subFolder, name string) string {
	// target should not be started with /
	if subFolder != "" {
		return strings.TrimLeft(filepath.Join(subFolder, name), "/")
	}

	return strings.TrimLeft(name, "/")
}

func waitDeploymentReady(ctx context.Context, deploymentName, namespace string, timout int64, kubeClient crClient.Client, logger *zap.SugaredLogger) (config.Status, error) {
	timeout := time.After(time.Duration(timout) * time.Second)
	tick := time.NewTicker(time.Second * 2)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return config.StatusCancelled, errors.New("job was cancelled")

		case <-timeout:
			msg := fmt.Sprintf("timeout waiting for the deployment: %s to run", deploymentName)
			return config.StatusTimeout, errors.New(msg)

		case <-tick.C:
			d, found, err := getter.GetDeployment(namespace, deploymentName, kubeClient)
			if err != nil || !found {
				logger.Errorf(
					"failed to check deployment ready status %s/%s - %v",
					namespace,
					deploymentName,
					err,
				)
			} else {
				if wrapper.Deployment(d).Ready() {
					return config.StatusRunning, nil
				}
			}
		}
	}
}

func createOrUpdateRegistrySecrets(namespace string, registries []*commonmodels.RegistryNamespace, kubeClient crClient.Client) error {
	for _, reg := range registries {
		if reg.AccessKey == "" {
			continue
		}

		secretName, err := genRegistrySecretName(reg)
		if err != nil {
			return fmt.Errorf("failed to generate registry secret name: %s", err)
		}

		data := make(map[string][]byte)
		dockerConfig := fmt.Sprintf(
			`{"%s":{"username":"%s","password":"%s","email":"%s"}}`,
			reg.RegAddr,
			reg.AccessKey,
			reg.SecretKey,
			defaultSecretEmail,
		)
		data[".dockercfg"] = []byte(dockerConfig)

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      secretName,
			},
			Data: data,
			Type: corev1.SecretTypeDockercfg,
		}
		if err := updater.UpdateOrCreateSecret(secret, kubeClient); err != nil {
			return err
		}
	}

	return nil
}

func genRegistrySecretName(reg *commonmodels.RegistryNamespace) (string, error) {
	if reg.IsDefault {
		return setting.DefaultImagePullSecret, nil
	}

	arr := strings.Split(reg.Namespace, "/")
	namespaceInRegistry := arr[len(arr)-1]

	// for AWS ECR, there are no namespace, thus we need to find the NS from the URI
	if namespaceInRegistry == "" {
		uriDecipher := strings.Split(reg.RegAddr, ".")
		namespaceInRegistry = uriDecipher[0]
	}

	filteredName, err := formatRegistryName(namespaceInRegistry)
	if err != nil {
		return "", err
	}

	secretName := filteredName + registrySecretSuffix
	if reg.RegType != "" {
		secretName = filteredName + "-" + reg.RegType + registrySecretSuffix
	}

	return secretName, nil
}

func formatRegistryName(namespaceInRegistry string) (string, error) {
	reg, err := regexp.Compile("[^a-zA-Z0-9\\.-]+")
	if err != nil {
		return "", err
	}
	processedName := reg.ReplaceAllString(namespaceInRegistry, "")
	processedName = strings.ToLower(processedName)
	if len(processedName) > 237 {
		processedName = processedName[:237]
	}
	return processedName, nil
}
