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

package setting

import "time"

const LocalConfig = "local.env"

// envs
const (
	// common
	ENVSystemAddress           = "ADDRESS"
	ENVImagePullPolicy         = "IMAGE_PULL_POLICY"
	ENVMode                    = "MODE"
	ENVMongoDBConnectionString = "MONGODB_CONNECTION_STRING"
	ENVAslanDBName             = "ASLAN_DB"
	ENVHubAgentImage           = "HUB_AGENT_IMAGE"
	ENVExecutorImage           = "EXECUTOR_IMAGE"
	ENVMysqlUser               = "MYSQL_USER"
	ENVMysqlPassword           = "MYSQL_PASSWORD"
	ENVMysqlHost               = "MYSQL_HOST"
	ENVMysqlUserDb             = "MYSQL_USER_DB"
	ENVMysqlUseDM              = "MYSQL_USE_DM"
	ENVRedisHost               = "REDIS_HOST"
	ENVRedisPort               = "REDIS_PORT"
	ENVRedisUserName           = "REDIS_USERNAME"
	ENVRedisPassword           = "REDIS_PASSWORD"
	ENVRedisUserTokenDB        = "REDIS_USER_TOKEN_DB"
	ENVRedisCommonCacheDB      = "REDIS_COMMON_CACHE_DB"
	ENVChartVersion            = "CHART_VERSION"

	// Aslan
	ENVPodName              = "BE_POD_NAME"
	ENVNamespace            = "BE_POD_NAMESPACE"
	ENVLogLevel             = "LOG_LEVEL"
	ENVExecutorLogLevel     = "EXECUTOR_LOG_LEVEL"
	ENVServiceStartTimeout  = "SERVICE_START_TIMEOUT"
	ENVDefaultEnvRecycleDay = "DEFAULT_ENV_RECYCLE_DAY"
	ENVDefaultIngressClass  = "DEFAULT_INGRESS_CLASS"

	ENVGithubSSHKey    = "GITHUB_SSH_KEY"
	ENVGithubKnownHost = "GITHUB_KNOWN_HOST"

	ENVReaperImage      = "REAPER_IMAGE"
	ENVReaperBinaryFile = "REAPER_BINARY_FILE"
	ENVPredatorImage    = "PREDATOR_IMAGE"
	EnvPackagerImage    = "PACKAGER_IMAGE"

	ENVDockerHosts = "DOCKER_HOSTS"

	ENVUseClassicBuild       = "USE_CLASSIC_BUILD"
	ENVCustomDNSNotSupported = "CUSTOM_DNS_NOT_SUPPORTED"

	ENVOldEnvSupported = "OLD_ENV_SUPPORTED"

	ENVS3StorageAK       = "S3STORAGE_AK"
	ENVS3StorageSK       = "S3STORAGE_SK"
	ENVS3StorageEndpoint = "S3STORAGE_ENDPOINT"
	ENVS3StorageBucket   = "S3STORAGE_BUCKET"
	ENVS3StorageProtocol = "S3STORAGE_PROTOCOL"
	ENVS3StoragePath     = "S3STORAGE_PATH"
	ENVKubeServerAddr    = "KUBE_SERVER_ADDR"

	// cron
	ENVRootToken = "ROOT_TOKEN"

	ENVKodespaceVersion = "KODESPACE_VERSION"

	// hubagent
	HubAgentToken         = "HUB_AGENT_TOKEN"
	HubServerBaseAddr     = "HUB_SERVER_BASE_ADDR"
	KubernetesServiceHost = "KUBERNETES_SERVICE_HOST"
	KubernetesServicePort = "KUBERNETES_SERVICE_PORT"
	Token                 = "X-API-Tunnel-Token"
	Params                = "X-API-Tunnel-Params"
	AslanBaseAddr         = "ASLAN_BASE_ADDR"
	ScheduleWorkflow      = "SCHEDULE_WORKFLOW"

	// warpdrive
	WarpDrivePodName    = "WD_POD_NAME"
	WarpDriveNamespace  = "BE_POD_NAMESPACE"
	ReleaseImageTimeout = "RELEASE_IMAGE_TIMEOUT"

	// reaper
	Home            = "HOME"
	PkgFile         = "PKG_FILE"
	JobConfigFile   = "JOB_CONFIG_FILE"
	DockerAuthDir   = "DOCKER_AUTH_DIR"
	Path            = "PATH"
	DockerHost      = "DOCKER_HOST"
	BuildURL        = "BUILD_URL"
	DefaultDockSock = "/var/run/docker.sock"

	// jenkins
	JenkinsBuildImage = "JENKINS_BUILD_IMAGE"

	// dind
	DindImage = "DIND_IMAGE"

	DebugMode   = "debug"
	ReleaseMode = "release"
	TestMode    = "test"

	// user
	ENVIssuerURL       = "ISSUER_URL"
	ENVClientID        = "CLIENT_ID"
	ENVClientSecret    = "CLIENT_SECRET"
	ENVRedirectURI     = "REDIRECT_URI"
	ENVSecretKey       = "SECRET_KEY"
	ENVMysqlUserDB     = "MYSQL_USER_DB"
	ENVScopes          = "SCOPES"
	ENVTokenExpiresAt  = "TOKEN_EXPIRES_AT"
	ENVUserPort        = "USER_PORT"
	ENVDecisionLogPath = "DECISION_LOG_PATH"

	// config
	ENVMysqlDexDB = "MYSQL_DEX_DB"
	FeatureFlag   = "feature-gates"

	ENVEnableTransaction = "ENABLE_TRANSACTION"
)

// k8s concepts
const (
	Secret                = "Secret"
	ConfigMap             = "ConfigMap"
	Ingress               = "Ingress"
	PersistentVolumeClaim = "PersistentVolumeClaim"
	Service               = "Service"
	Deployment            = "Deployment"
	StatefulSet           = "StatefulSet"
	Pod                   = "Pod"
	ReplicaSet            = "ReplicaSet"
	Job                   = "Job"
	CronJob               = "CronJob"
	ClusterRoleBinding    = "ClusterRoleBinding"
	ServiceAccount        = "ServiceAccount"
	ClusterRole           = "ClusterRole"
	Role                  = "Role"
	RoleBinding           = "RoleBinding"

	// labels
	TaskLabel                       = "s-task"
	TypeLabel                       = "s-type"
	PipelineTypeLable               = "p-type"
	ProductLabel                    = "s-product"
	GroupLabel                      = "s-group"
	ServiceLabel                    = "s-service"
	LabelHashKey                    = "hash"
	ConfigBackupLabel               = "config-backup"
	EnvNameLabel                    = "s-env"
	UpdateBy                        = "update-by"
	UpdateByID                      = "update-by-id"
	UpdateTime                      = "update-time"
	UpdatedByLabel                  = "updated-by-koderover"
	IngressClassLabel               = "kubernetes.io/ingress.class"
	IngressProxyConnectTimeoutLabel = "nginx.ingress.kubernetes.io/proxy-connect-timeout"
	IngressProxySendTimeoutLabel    = "nginx.ingress.kubernetes.io/proxy-send-timeout"
	IngressProxyReadTimeoutLabel    = "nginx.ingress.kubernetes.io/proxy-read-timeout"
	ComponentLabel                  = "app.kubernetes.io/component"
	companyLabel                    = "koderover.io"
	DirtyLabel                      = companyLabel + "/" + "modified-since-last-update"
	OwnerLabel                      = companyLabel + "/" + "owner"
	InactiveConfigLabel             = companyLabel + "/" + "inactive"
	ModifiedByAnnotation            = companyLabel + "/" + "last-modified-by"
	EditorIDAnnotation              = companyLabel + "/" + "editor-id"
	LastUpdateTimeAnnotation        = companyLabel + "/" + "last-update-time"

	JobLabelTaskKey  = "s-task"
	JobLabelNameKey  = "s-name"
	JobLabelSTypeKey = "s-type"

	LabelValueTrue = "true"

	// Pod status
	PodRunning                 = "Running"
	PodError                   = "Error"
	PodUnstable                = "Unstable"
	PodCreating                = "Creating"
	PodCreated                 = "created"
	PodUpdating                = "Updating"
	PodDeleting                = "Deleting"
	PodSucceeded               = "Succeeded"
	PodFailed                  = "Failed"
	PodPending                 = "Pending"
	PodNonStarted              = "Unstart"
	PodCompleted               = "Completed"
	ServiceStatusAllSuspended  = "AllSuspend"
	ServiceStatusPartSuspended = "PartSuspend"
	ServiceStatusNoSuspended   = "NoSuspend"

	// cluster status
	ClusterUnknown      = "Unknown"
	ClusterNotFound     = "NotFound"
	ClusterDisconnected = "Disconnected"

	// annotations
	HelmReleaseNameAnnotation = "meta.helm.sh/release-name"

	EnvCreatedBy = "createdBy"
	EnvCreator   = "koderover"
	PodReady     = "ready"
	JobReady     = "Completed"
	PodNotReady  = "not ready"

	APIVersionAppsV1 = "apps/v1"

	RSASecretName = "zadig-rsa-key"

	DefaultImagePullSecret = "default-registry-secret"

	PolicyMetaConfigMapName = "zadig-policy-meta"
	PolicyURLConfigMapName  = "zadig-policy-url"
	PolicyRoleConfigMapName = "zadig-policy-role"
)

const (
	ProductName = "zadig"
	RequestID   = "requestID"

	ProtocolHTTP  string = "http"
	ProtocolHTTPS string = "https"
	ProtocolTCP   string = "tcp"

	DefaultIngressClass = "koderover-nginx"

	// K8SDeployType Containerized Deployment
	K8SDeployType = "k8s"
	// helm deployment
	HelmDeployType      = "helm"
	HelmChartDeployType = "helm_chart"
	// PMDeployType physical machine deploy method
	PMDeployType          = "pm"
	TrusteeshipDeployType = "trusteeship"

	// Infrastructure k8s type
	BasicFacilityK8S = "kubernetes"
	// Infrastructure Cloud Hosting
	BasicFacilityCVM = "cloud_host"

	// SourceFromZadig Configuration sources are managed by the platform
	SourceFromZadig = "spock"
	// SourceFromGitlab The configuration source is gitlab
	SourceFromGitlab = "gitlab"
	// SourceFromGithub The configuration source is github
	SourceFromGithub = "github"
	// SourceFromGerrit The configuration source is gerrit
	SourceFromGerrit = "gerrit"
	// SourceFromGitee Configure the source as gitee
	SourceFromGitee = "gitee"
	// SourceFromGiteeEE Configure the source as gitee-enterprise
	SourceFromGiteeEE = "gitee-enterprise"
	// SourceFromOther Configure the source as other
	SourceFromOther = "other"
	// SourceFromChartTemplate The configuration source is helmTemplate
	SourceFromChartTemplate = "chartTemplate"
	// SourceFromPublicRepo The configuration source is publicRepo
	SourceFromPublicRepo  = "publicRepo"
	SourceFromChartRepo   = "chartRepo"
	SourceFromCustomEdit  = "customEdit"
	SourceFromVariableSet = "variableSet"

	// SourceFromGUI The configuration source is gui
	SourceFromGUI = "gui"
	//SourceFromHelm
	SourceFromHelm = "helm"
	//SourceFromExternal
	SourceFromExternal = "external"
	// service from yaml template
	ServiceSourceTemplate = "template"
	SourceFromPM          = "pm"
	SourceFromGitRepo     = "repo"
	// SourceFromApollo is the configuration_management type of apollo
	SourceFromApollo = "apollo"
	// SourceFromNacos is the configuration_management type of nacos
	SourceFromNacos = "nacos"

	ProdENV = "prod"
	TestENV = "test"
	AllENV  = "all"

	// action type
	TypeEnableCronjob  = "enable"
	TypeDisableCronjob = "disable"

	PublicService = "public"

	// The second step of the onboarding process
	OnboardingStatusSecond = 2

	Unset            = "UNSET"
	CleanSkippedList = "CLEAN_WHITE_LIST"
	PerPage          = 20

	BuildType   = "build"
	DeployType  = "deploy"
	TestType    = "test"
	PublishType = "publish"

	FunctionTestType = "function"

	AllProjects = "<all_projects>"

	BuildOSSCacheFileName    = "zadig-build-cache.tar.gz"
	ScanningOSSCacheFileName = "zadig-scanning-cache.tar.gz"
	TestingOSSCacheFileName  = "zadig-testing-cache.tar.gz"
)

const (
	DeliveryVersionTypeYaml        = "K8SYaml"
	DeliveryVersionTypeChart       = "HelmChart"
	DeliveryVersionTypeK8SWorkflow = "K8SWorkflow"
)

const (
	DeliveryDeployTypeImage = "image"
	DeliveryDeployTypeChart = "chart"
)

const (
	AuthorizationHeader = "Authorization"
)

// install script constants
const (
	StandardScriptName   = "install.sh"
	AllInOneScriptName   = "install_with_k8s.sh"
	UninstallScriptName  = "uninstall.sh"
	StandardInstallType  = "standard"
	AllInOneInstallType  = "all-in-one"
	UninstallInstallType = "uninstall"
)

// Pod Status
const (
	StatusRunning   = "Running"
	StatusSucceeded = "Succeeded"
)

// build image consts
const (
	// BuildImageJob ...
	BuildImageJob = "docker-build"
	// ReleaseImageJob ...
	ReleaseImageJob = "docker-release"
)

const (
	BuildChartPackage = "chart-package"
)

const (
	JenkinsBuildJob = "jenkins-build"
)

// counter prefix
const (
	PipelineTaskFmt   = "PipelineTask:%s"
	WorkflowTaskFmt   = "WorkflowTask:%s"
	WorkflowTaskV3Fmt = "WorkflowTaskV3:%s"
	WorkflowTaskV4Fmt = "WorkflowTaskV4:%s"
	TestTaskFmt       = "TestTask:%s"
	ServiceTaskFmt    = "ServiceTask:%s"
	ScanningTaskFmt   = "ScanningTask:%s"
	ReleasePlanFmt    = "ReleasePlan:default"
)

// Product Status
const (
	ProductStatusSuccess  = "success"
	ProductStatusFailed   = "failed"
	ProductStatusCreating = "creating"
	ProductStatusUpdating = "updating"
	ProductStatusDeleting = "deleting"
	ProductStatusSleeping = "Sleeping"
	ProductStatusUnknown  = "unknown"
	ProductStatusUnstable = "Unstable"
)

// DeliveryVersion status
const (
	DeliveryVersionStatusSuccess  = "success"
	DeliveryVersionStatusFailed   = "failed"
	DeliveryVersionStatusCreating = "creating"
	DeliveryVersionStatusRetrying = "retrying"
)

const (
	DeliveryVersionPackageStatusSuccess   = "success"
	DeliveryVersionPackageStatusFailed    = "failed"
	DeliveryVersionPackageStatusWaiting   = "waiting"
	DeliveryVersionPackageStatusUploading = "uploading"
)

const (
	NormalModeProduct = "normal"
)

const (
	SystemUser = "system"
)

// events
const (
	CreateProductEvent        = "CreateProduct"
	UpdateProductEvent        = "UpdateProduct"
	DeleteProductEvent        = "DeleteProduct"
	UpdateContainerImageEvent = "UpdateContainerImage"
)

// operation scenes
const (
	OperationSceneProject          = "project"
	OperationSceneBuild            = "build"
	OperationSceneWorkflow         = "workflow"
	OperationSceneEnv              = "environment"
	OperationSceneService          = "service"
	OperationSceneTest             = "test"
	OperationSceneScanning         = "scanning"
	OperationSceneVersion          = "version"
	OperationSceneSystem           = "system"
	OperationSceneSprintManagement = "sprint_management"
)

// Service Related
const (
	// PrivateVisibility ...
	PrivateVisibility = "private"
	// PublicAccess ...
	PublicAccess = "public"
	// ChartYaml
	ChartYaml = "Chart.yaml"
	// ValuesYaml
	ValuesYaml = "values.yaml"
	// TemplatesDir
	TemplatesDir = "templates"
	// ServiceTemplateCounterName use aslan/core/common/util.GenerateServiceNextRevision() to generate service revision
	ServiceTemplateCounterName = "service:%s&project:%s"
	// ProductionServiceTemplateCounterName use aslan/core/common/util.GenerateServiceNextRevision() to generate service revision
	ProductionServiceTemplateCounterName = "productionservice:%s&project:%s"
	EnvServiceVersionCounterName         = "project:%s&env:%s&service:%s&ishelmchart:%v"
	// GerritDefaultOwner
	GerritDefaultOwner = "dafault"
	// YamlFileSeperator ...
	YamlFileSeperator             = "\n---\n"
	HelmChartDeployStrategySuffix = "<+helm_chart>"
)

const MaskValue = "********"

// proxy
const (
	ProxyAPIAddr    = "PROXY_API_ADDR"
	ProxyHTTPSAddr  = "PROXY_HTTPS_ADDR"
	ProxyHTTPAddr   = "PROXY_HTTP_ADDR"
	ProxySocks5Addr = "PROXY_SOCKS_ADDR"
)

const (
	// WorkflowTriggerTaskCreator ...
	WorkflowTriggerTaskCreator = "workflow_trigger"
	// WebhookTaskCreator ...
	WebhookTaskCreator = "webhook"
	// JiraHookTaskCreator ...
	JiraHookTaskCreator = "jira_hook"
	// MeegoHookTaskCreator ...
	MeegoHookTaskCreator = "meego_hook"
	// GeneralHookTaskCreator ...
	GeneralHookTaskCreator = "general_hook"
	// CronTaskCreator ...
	CronTaskCreator = "timer"
	// DefaultTaskRevoker ...
	DefaultTaskRevoker = "system" // default task revoker
)

const (
	// DefaultMaxFailures ...
	DefaultMaxFailures = 10

	// FrequencySeconds ...
	FrequencySeconds = "seconds"
	// FrequencyMinutes ...
	FrequencyMinutes = "minutes"
	// FrequencyHour ...
	FrequencyHour = "hour"
	// FrequencyHours ...
	FrequencyHours = "hours"
	// FrequencyDay ...
	FrequencyDay = "day"
	// FrequencyDays ...
	FrequencyDays = "days"
	// FrequencyMondy ...
	FrequencyMondy = "monday"
	// FrequencyTuesday ...
	FrequencyTuesday = "tuesday"
	// FrequencyWednesday ...
	FrequencyWednesday = "wednesday"
	// FrequencyThursday ...
	FrequencyThursday = "thursday"
	// FrequencyFriday ...
	FrequencyFriday = "friday"
	// FrequencySaturday ...
	FrequencySaturday = "saturday"
	// FrequencySunday ...
	FrequencySunday = "sunday"
)

const (
	// FunctionTest 功能测试
	FunctionTest = "function"
	// PerformanceTest 性能测试
	PerformanceTest = "performance"

	TestWorkflowNamingConvention = "zadig-testing-%s"
	ScanWorkflowNamingConvention = "zadig-scanning-%s"
)

const (
	// UbuntuPrecis ...
	UbuntuPrecis = "precise"
	// UbuntuTrusty ...
	UbuntuTrusty = "trusty"
	// UbuntuXenial ...
	UbuntuXenial = "xenial"
	// UbuntuBionic ...
	UbuntuBionic = "bionic"
	// TestOnly ...
	TestOnly = "test"
)

const (
	CacheExpireTime = 1 * time.Hour
)

const (
	Version = "stable"

	EnvRecyclePolicyAlways     = "always"
	EnvRecyclePolicyTaskStatus = "success"
	EnvRecyclePolicyNever      = "never"
)

const (
	ImageFromCustom     = "custom"
	FixedDayTimeCronjob = "timing"
	FixedGapCronjob     = "gap"
	CrontabCronjob      = "crontab"
	UnixStampSchedule   = "unix_stamp"

	// 定时器的所属job类型
	WorkflowCronjob    = "workflow"
	WorkflowV4Cronjob  = "workflow_v4"
	TestingCronjob     = "test"
	EnvAnalysisCronjob = "env_analysis"
	EnvSleepCronjob    = "env_sleep"
	ReleasePlanCronjob = "release_plan"

	TopicProcess      = "task.process"
	TopicCancel       = "task.cancel"
	TopicAck          = "task.ack"
	TopicItReport     = "task.it.report"
	TopicNotification = "task.notification"
	TopicCronjob      = "cronjob"
)

// S3 related constants
const (
	S3DefaultRegion = "ap-shanghai"
)

// ALL provider mapping
const (
	ProviderSourceETC = iota
	ProviderSourceAli
	ProviderSourceTencent
	ProviderSourceQiniu
	ProviderSourceHuawei
	ProviderSourceSystemDefault
	ProviderSourceGoogle
	ProviderSourceVolcano
)

// helm related
const (
	// components used to search image paths from yaml
	PathSearchComponentRepo      = "repo"
	PathSearchComponentNamespace = "namespace"
	PathSearchComponentImage     = "image"
	PathSearchComponentTag       = "tag"
)

// host for multiple cloud provider
const (
	AliyunHost = ".aliyuncs.com"
	AWSHost    = ".amazonaws.com"
)

// Dockerfile parsing consts
const (
	DockerfileCmdArg = "ARG"
)

// Dockerfile template constant
const (
	DockerfileSourceLocal    = "local"
	DockerfileSourceTemplate = "template"

	ZadigDockerfilePath = "zadig-dockerfile"
)

// Yaml template constant
const (
	RegExpParameter = `{{.(\w)+}}`
)

// template common constant
const (
	TemplateVariableProduct            = "$T-Project$"
	TemplateVariableProductDescription = "项目名称"
	TemplateVariableService            = "$T-Service$"
	TemplateVariableServiceDescription = "服务名称"
)

const MaxTries = 1

const DogFood = "/var/run/koderover-dog-food"

const ProgressFile = "/var/log/job-progress"

const (
	ResponseError = "error"
	ResponseData  = "response"
)

const ChartTemplatesPath = "charts"

type RoleType string

const (
	Contributor     RoleType = "contributor"
	ReadOnly        RoleType = "read-only"
	ProjectAdmin    RoleType = "project-admin"
	SystemAdmin     RoleType = "admin"
	ReadProjectOnly RoleType = "read-project-only"
)

// ModernWorkflowType 自由编排工作流
const ModernWorkflowType = "ModernWorkflow"

const (
	Subresource        = "subresource"
	StatusSubresource  = "status"
	IngressSubresource = "ingress"
	ResourcesHeader    = "Resources"
)

type K8SClusterStatus string

const (
	Disconnected K8SClusterStatus = "disconnected"
	Pending      K8SClusterStatus = "pending"
	Normal       K8SClusterStatus = "normal"
	Abnormal     K8SClusterStatus = "abnormal"
)

type PMHostStatus string

const (
	PMHostStatusNormal   PMHostStatus = "normal"
	PMHostStatusAbnormal PMHostStatus = "abnormal"
)

const PMHostDefaultPort int64 = 22

type ResetImagePolicyType string

const (
	ResetImagePolicyTaskCompleted      ResetImagePolicyType = "taskCompleted"
	ResetImagePolicyTaskCompletedOrder ResetImagePolicyType = ""
	ResetImagePolicyDeployFailed       ResetImagePolicyType = "deployFailed"
	ResetImagePolicyTestFailed         ResetImagePolicyType = "testFailed"
)

// Cluster Management constants
const (
	AgentClusterType      = "agent"
	KubeConfigClusterType = "kubeconfig"

	LocalClusterID = "0123456789abcdef12345678"
)

const DefaultLoginLocal = "local"

const RequestModeOpenAPI = "openAPI"

const DeployTimeout = 60 * 10 // 10 minutes

const UpdateEnvTimeout = 60 * 5 * time.Second

// list namespace type
const (
	ListNamespaceTypeCreate = "create"
	ListNamespaceTypeALL    = "all"
)

const (
	InformerNamingConvention      = "%s-%s"
	IstioInformerNamingConvention = "%s-%s-istio"
)

type ResourceType string

const (
	ResourceTypeSystem ResourceType = "system"
	ResourceTypeCustom ResourceType = "custom"
)

type NewRoleType int64

const (
	RoleTypeSystem NewRoleType = 1
	RoleTypeCustom NewRoleType = 2
)

const (
	RoleTemplateTypeCustom     = 0
	RoleTemplateTypePredefined = 1
)

const (
	ActionTypeAdmin = iota
	ActionTypeProject
	ActionTypeSystem
)

// AttachedClusterNamespace is the namespace Zadig uses in attached cluster.
// Note: **Restricted because of product design since v1.9.0**.
const AttachedClusterNamespace = "koderover-agent"

// testing constants
const (
	ArtifactResultOut          = "artifactResultOut.tar.gz"
	HtmlReportArchivedFileName = "htmlReportArchived.tar.gz"
)

const (
	DefaultReleaseNaming     = "$Service$"
	ReleaseNamingPlaceholder = "$Namespace$-$Service$"
)

// custom workflow constants for variables
const (
	FixedValueMark            = "<+fixed>"
	RenderValueTemplate       = "{{.%s}}"
	RenderPluginValueTemplate = "$(%s)"
)

const (
	// normal project names are not allowed to contain special characters, so we have a special project name to distinguish the enterprise workflow
	EnterpriseProject = "DEPLOY_CENTER"
)

const (
	ProductWorkflowType = "product_workflow"
	CustomWorkflowType  = "common_workflow"
)

// cluster dodeAffinity schedule type
const (
	NormalScheduleName    = "随机调度"
	NormalSchedule        = "normal"
	RequiredScheduleName  = "强制调度"
	RequiredSchedule      = "required"
	PreferredScheduleName = "优先调度"
	PreferredSchedule     = "preferred"
)

const (
	JobNameRegx  = "^[a-z\u4e00-\u9fa5][a-z0-9\u4e00-\u9fa5-]{0,31}$"
	WorkflowRegx = "^[a-zA-Z0-9-]+$"
)

type WorkflowCategory string

const (
	CustomWorkflow  WorkflowCategory = ""
	ReleaseWorkflow WorkflowCategory = "release"
)

const (
	ServiceDeployStrategyImport = "import"
	ServiceDeployStrategyDeploy = "deploy"
)

// Instant Message System types
const (
	IMLark     = "lark"
	IMDingTalk = "dingtalk"
	IMWorkWx   = "workwx"
)

// lark app
const (
	LarkUserID           = "user_id"
	LarkUserOpenID       = "open_id"
	LarkDepartmentOpenID = "open_department_id"
)

// Project Management types
const (
	PMJira  = "jira"
	PMLark  = "lark"
	PMMeego = "meego"
)

// Workflow variable source type
const (
	VariableSourceRuntime = "runtime"
	VariableSourceOther   = "other"
)

var ServiceVarWildCard = []string{"*"}

const (
	// AI analyze env result status
	AIEnvAnalysisStatusSuccess = "success"
	AIEnvAnalysisStatusFailed  = "failed"
)

const (
	UNGROUPED = "未分组"
)

const (
	ZadigBuild   = "zadig"
	JenkinsBuild = "jenkins"
)

// CI/CD Tool Type
const (
	CICDToolTypeJenkins  = "jenkins"
	CICDToolTypeBlueKing = "blueKing"
)

type IntegrationLevel string

const (
	IntegrationLevelSystem  IntegrationLevel = "system"
	IntegrationLevelProject IntegrationLevel = "project"
)

const (
	// NewVMType agent type
	NewVMType = "agent"

	// vm status
	VMCreated    = "created"
	VMRegistered = "registered"
	VMNormal     = "normal"
	VMAbnormal   = "abnormal"
	VMOffline    = "offline"

	// VMLabelAnyOne vm preserve label key
	VMLabelAnyOne = "VM_LABEL_ANY_ONE"

	AgentDefaultHeartbeatTimeout = 10

	// vm job status
	VMJobStatusCreated     = "created"
	VMJobStatusDistributed = "distributed"
	VMJobStatusRunning     = "running"
	VMJobStatusSuccess     = "success"
	VMJobStatusFailed      = "failed"

	// vm platform type
	LinuxAmd64 = "linux_amd64"
	LinuxArm64 = "linux_arm64"
	MacOSAmd64 = "darwin_amd64"
	MacOSArm64 = "darwin_arm64"
	WinAmd64   = "windows_amd64"

	// vm cache type
	VmCache     = "vm"
	ObjectCache = "object"
)

const (
	JobK8sInfrastructure string = "kubernetes"
	JobVMInfrastructure  string = "vm"
)

const (
	WorkflowTimeFormat = "[2006-01-02 15:04:05]"
)

const (
	IstioNamespace                          = "istio-system"
	IstioProxyName                          = "istio-proxy"
	ZadigEnvoyFilter                        = "zadig-share-env"
	EnvoyFilterNetworkHttpConnectionManager = "envoy.filters.network.http_connection_manager"
	EnvoyFilterHttpRouter                   = "envoy.filters.http.router"
	EnvoyFilterLua                          = "type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua"
)

type SQLExecStatus string

const (
	SQLExecStatusSuccess SQLExecStatus = "success"
	SQLExecStatusFailed  SQLExecStatus = "failed"
	SQLExecStatusNotExec SQLExecStatus = "not_exec"
)

type ProjectApplicationType string

const (
	ProjectApplicationTypeHost   ProjectApplicationType = "host"
	ProjectApplicationTypeMobile ProjectApplicationType = "mobile"
)

const (
	WorkflowScanningJobOutputKey        = "SonarCETaskID"
	WorkflowScanningJobOutputKeyProject = "SonarProjectKey"
	WorkflowScanningJobOutputKeyBranch  = "SonarBranchKey"
)

type NotifyWebHookType string

const (
	NotifyWebHookTypeDingDing     NotifyWebHookType = "dingding"
	NotifyWebHookTypeFeishu       NotifyWebHookType = "feishu"
	NotifyWebHookTypeFeishuPerson NotifyWebHookType = "feishu_person"
	NotifyWebhookTypeFeishuApp    NotifyWebHookType = "feishu_app"
	NotifyWebHookTypeWechatWork   NotifyWebHookType = "wechat"
	NotifyWebHookTypeMail         NotifyWebHookType = "mail"
	NotifyWebHookTypeWebook       NotifyWebHookType = "webhook"
)

const (
	UserTypeUser        string = "user"
	UserTypeGroup       string = "group"
	UserTypeTaskCreator string = "task_creator"
)

type ContainerType string

const (
	ContainerTypeInit   ContainerType = "init"
	ContainerTypeNormal ContainerType = ""
)

type SprintWorkItemActivityType string

const (
	SprintWorkItemActivityTypeEvent   SprintWorkItemActivityType = "event"
	SprintWorkItemActivityTypeComment SprintWorkItemActivityType = "comment"
)

const (
	SAEZadigProjectTagKey       = "ZADIG_PROJECT"
	SAEZadigEnvTagKey           = "ZADIG_ENV"
	SAEZadigServiceTagKey       = "ZADIG_SERVICE"
	SAEZadigServiceModuleTagKey = "ZADIG_SERVICE_MODULE"
)

const (
	EventBusChannelClusterUpdate = "cluster_update"
)
