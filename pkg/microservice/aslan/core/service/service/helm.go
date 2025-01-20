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
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/27149chen/afero"
	"github.com/otiai10/copy"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/yaml"

	fileutil "github.com/koderover/zadig/v2/pkg/cli/zadig-agent/util/file"
	configbase "github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	templaterepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb/template"
	commonservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/command"
	fsservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/fs"
	helmservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/helm"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/repository"
	commonutil "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/util"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/environment/service"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/systemconfig"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

type HelmService struct {
	ServiceInfos []*commonmodels.Service `json:"service_infos"`
	FileInfos    []*types.FileInfo       `json:"file_infos"`
	Services     [][]string              `json:"services"`
}

type HelmChartEditInfo struct {
	FilePath    string `json:"file_path"`
	FileContent string `json:"file_content"`
	Production  bool
}

type HelmServiceModule struct {
	ServiceModules []*ServiceModule      `json:"service_modules"`
	Service        *commonmodels.Service `json:"service,omitempty"`
}

type Chart struct {
	APIVersion string `json:"apiVersion"`
	Name       string `json:"name"`
	Version    string `json:"version"`
	AppVersion string `json:"appVersion"`
}

type helmServiceCreationArgs struct {
	ChartName        string
	ChartVersion     string
	ServiceRevision  int64
	MergedValues     string
	ServiceName      string
	FilePath         string
	ProductName      string
	CreateBy         string
	RequestID        string
	CodehostID       int
	Owner            string
	Namespace        string
	Repo             string
	Branch           string
	RepoLink         string
	Source           string
	HelmTemplateName string
	ValuePaths       []string
	ValuesYaml       string
	Variables        []*Variable
	GiteePath        string
	GerritRepoName   string
	GerritBranchName string
	GerritRemoteName string
	GerritPath       string
	GerritCodeHostID int
	ChartRepoName    string
	ValuesSource     *commonservice.ValuesDataArgs
	CreationDetail   interface{}
	AutoSync         bool
	Production       bool
}

type ChartTemplateData struct {
	TemplateName      string
	TemplateData      *commonmodels.Chart
	ChartName         string
	ChartVersion      string
	DefaultValuesYAML []byte // content of values.yaml in template
}

type GetFileContentParam struct {
	FilePath        string `json:"filePath"        form:"filePath"`
	FileName        string `json:"fileName"        form:"fileName"`
	Revision        int64  `json:"revision"        form:"revision"`
	DeliveryVersion bool   `json:"deliveryVersion" form:"deliveryVersion"`
}

func ListHelmServices(productName string, production bool, log *zap.SugaredLogger) (*HelmService, error) {
	helmService := &HelmService{
		ServiceInfos: []*commonmodels.Service{},
		FileInfos:    []*types.FileInfo{},
		Services:     [][]string{},
	}

	services, err := repository.ListMaxRevisionsServices(productName, production)
	if err != nil {
		log.Errorf("[helmService.list] err:%v", err)
		return nil, e.ErrListTemplate.AddErr(err)
	}
	helmService.ServiceInfos = services

	if len(services) > 0 {
		fis, err := loadServiceFileInfos(services[0].ProductName, services[0].ServiceName, 0, "", production)
		if err != nil {
			log.Errorf("Failed to load service file info, err: %s", err)
			return nil, e.ErrListTemplate.AddErr(err)
		}
		helmService.FileInfos = fis
	}
	project, err := templaterepo.NewProductColl().Find(productName)
	if err != nil {
		log.Errorf("Failed to find project info, err: %s", err)
		return nil, e.ErrListTemplate.AddErr(err)
	}
	helmService.Services = project.Services
	if production {
		helmService.Services = project.ProductionServices
	}

	return helmService, nil
}

func fillServiceTemplateVariables(serviceTemplate *models.Service) error {
	if serviceTemplate.Source != setting.SourceFromChartTemplate {
		return nil
	}
	creation, err := commonservice.GetCreateFromChartTemplate(serviceTemplate.CreateFrom)
	if err != nil {
		return fmt.Errorf("failed to get creation detail: %s", err)
	}

	templateChart, err := commonrepo.NewChartColl().Get(creation.TemplateName)
	if err != nil {
		return err
	}
	variables := make([]*models.Variable, 0)
	curValueMap := make(map[string]string)
	for _, kv := range creation.Variables {
		curValueMap[kv.Key] = kv.Value
	}

	for _, v := range templateChart.ChartVariables {
		value := v.Value
		if cv, ok := curValueMap[v.Key]; ok {
			value = cv
		}
		variables = append(variables, &models.Variable{Key: v.Key, Value: value})
	}
	creation.Variables = variables
	serviceTemplate.CreateFrom = creation
	return nil
}

func GetHelmServiceModule(serviceName, productName string, revision int64, isProduction bool, log *zap.SugaredLogger) (*HelmServiceModule, error) {
	serviceTemplate, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ServiceName:   serviceName,
		ProductName:   productName,
		Revision:      revision,
		Type:          setting.HelmDeployType,
		ExcludeStatus: setting.ProductStatusDeleting,
		Source:        "",
	}, isProduction)
	if err != nil {
		return nil, err
	}

	helmServiceModule := new(HelmServiceModule)
	serviceModules := make([]*ServiceModule, 0)
	for _, container := range serviceTemplate.Containers {
		serviceModule := new(ServiceModule)
		serviceModule.Container = container

		buildObjs, err := commonrepo.NewBuildColl().List(&commonrepo.BuildListOption{ProductName: productName, ServiceName: serviceName, Targets: []string{container.Name}})
		if err != nil {
			return nil, err
		}
		buildNames := sets.NewString()
		for _, buildObj := range buildObjs {
			buildNames.Insert(buildObj.Name)
		}
		serviceModule.BuildNames = buildNames.List()
		serviceModules = append(serviceModules, serviceModule)
	}
	err = fillServiceTemplateVariables(serviceTemplate)
	if err != nil {
		// NOTE source template may be deleted, error should not block the following logic
		log.Warnf("failed to fill service template variables for service: %s, err: %s", serviceTemplate.ServiceName, err)
	}
	err = commonservice.FillServiceCreationInfo(serviceTemplate)
	if err != nil {
		// NOTE since the source of yaml can always be selected when reloading, error should not block the following logic
		log.Warnf("failed to fill git namespace for yaml source : %s, err: %s", serviceTemplate.ServiceName, err)
	}

	helmServiceModule.Service = serviceTemplate
	serviceTemplate.ReleaseNaming = serviceTemplate.GetReleaseNaming()
	helmServiceModule.ServiceModules = serviceModules
	return helmServiceModule, err
}

func GetFilePath(serviceName, productName string, revision int64, dir string, production bool, _ *zap.SugaredLogger) ([]*types.FileInfo, error) {
	return loadServiceFileInfos(productName, serviceName, revision, dir, production)
}

func GetFileContent(serviceName, productName string, param *GetFileContentParam, production bool, log *zap.SugaredLogger) (string, error) {
	filePath, fileName, revision, forDelivery := param.FilePath, param.FileName, param.Revision, param.DeliveryVersion
	svc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ProductName: productName,
		ServiceName: serviceName,
		Revision:    revision,
	}, production)
	if err != nil {
		return "", e.ErrFileContent.AddDesc(err.Error())
	}

	base := config.LocalTestServicePath(productName, serviceName)
	if production {
		base = config.LocalProductionServicePath(productName, serviceName)
	}
	if revision > 0 {
		base = config.LocalTestServicePathWithRevision(productName, serviceName, fmt.Sprint(revision))
		if production {
			base = config.LocalProductionServicePathWithRevision(productName, serviceName, fmt.Sprint(revision))
		}

		if err = commonutil.PreloadServiceManifestsByRevision(base, svc, production); err != nil {
			log.Warnf("failed to get chart of revision: %d for service: %s, use latest version",
				svc.Revision, svc.ServiceName)
		}
	}
	if err != nil || revision == 0 {
		base = config.LocalTestServicePath(productName, serviceName)
		if production {
			base = config.LocalProductionServicePath(productName, serviceName)
		}

		err = commonutil.PreLoadServiceManifests(base, svc, production)
		if err != nil {
			return "", e.ErrFileContent.AddDesc(err.Error())
		}
	}

	if forDelivery {
		originBase := base
		if production {
			base = config.LocalProductionDeliveryChartPathWithRevision(productName, serviceName, revision)
		} else {
			base = config.LocalDeliveryChartPathWithRevision(productName, serviceName, revision)
		}
		if exists, err := fileutil.PathExists(base); !exists || err != nil {
			fullPath := filepath.Join(originBase, svc.ServiceName)
			err := copy.Copy(fullPath, filepath.Join(base, svc.ServiceName))
			if err != nil {
				return "", err
			}
		}
		defer func() {
			os.RemoveAll(base)
		}()
	}

	file := filepath.Join(base, serviceName, filePath, fileName)
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Errorf("Failed to read file %s, err: %s", file, err)
		return "", e.ErrFileContent.AddDesc(err.Error())
	}

	return string(fileContent), nil
}

func EditFileContent(serviceName, productName, createdBy, requestID string, param *HelmChartEditInfo, logger *zap.SugaredLogger) error {
	if len(param.FilePath) == 0 {
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("file path can't be nil"))
	}
	if param.FilePath != setting.ValuesYaml {
		return e.ErrEditHelmCharts.AddDesc(fmt.Sprintf("only values.yaml can be edited"))
	}

	svc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ServiceName: serviceName,
		ProductName: productName,
	}, param.Production)
	if err != nil {
		return e.ErrEditHelmCharts.AddDesc(err.Error())
	}

	if svc.Source != setting.SourceFromChartTemplate && svc.Source != setting.SourceFromCustomEdit {
		return e.ErrEditHelmCharts.AddDesc(fmt.Sprintf("can't edit file"))
	}

	// preload current chart
	base := config.LocalServicePath(productName, serviceName, param.Production)
	err = commonutil.PreLoadServiceManifests(base, svc, param.Production)
	if err != nil {
		return e.ErrEditHelmCharts.AddErr(err)
	}

	// check content equals
	file := filepath.Join(base, serviceName, param.FilePath)
	content, err := os.ReadFile(file)
	if err != nil {
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("failed to read file: %s, err: %s", file, err))
	}
	if string(content) == param.FileContent {
		return nil
	}

	if err = os.WriteFile(file, []byte(param.FileContent), 0644); err != nil {
		logger.Errorf("Failed to write file, err: %s", err)
		return e.ErrEditHelmCharts.AddErr(err)
	}

	var rev int64
	rev, err = getNextServiceRevision(productName, serviceName, param.Production)
	if err != nil {
		logger.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("failed to get service next revision for service %s, err: %s", serviceName, err))
	}

	err = copyChartRevision(productName, serviceName, rev, param.Production)
	if err != nil {
		logger.Errorf("Failed to copy file %s, err: %s", serviceName, err)
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("failed to copy file for service %s, err: %s", serviceName, err))
	}

	// clear files from both s3 and local when error occurred in next stages
	defer func() {
		if err != nil {
			clearChartFiles(productName, serviceName, rev, param.Production, logger)
		}
	}()

	fsTree := os.DirFS(config.LocalServicePath(productName, serviceName, param.Production))

	// read values.yaml
	valuesYAML, errRead := util.ReadValuesYAML(fsTree, serviceName, logger)
	if errRead != nil {
		err = errRead
		return e.ErrEditHelmCharts.AddErr(err)
	}

	serviceS3Base := config.ObjectStorageServicePath(productName, serviceName, param.Production)
	if err = fsservice.ArchiveAndUploadFilesToS3(fsTree, []string{serviceName, fmt.Sprintf("%s-%d", serviceName, rev)}, serviceS3Base, logger); err != nil {
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("failed to upload files for service %s in project %s, err: %s", serviceName, productName, err))
	}

	svc, err = createOrUpdateHelmService(
		fsTree,
		&helmServiceCreationArgs{
			ChartName:       svc.HelmChart.Name,
			ChartVersion:    svc.HelmChart.Version,
			ServiceRevision: rev,
			MergedValues:    string(valuesYAML),
			ServiceName:     svc.ServiceName,
			FilePath:        base,
			ProductName:     productName,
			CreateBy:        createdBy,
			RequestID:       requestID,
			Source:          setting.SourceFromCustomEdit,
			ValuesSource:    &commonservice.ValuesDataArgs{},
			CreationDetail:  svc.CreateFrom,
			AutoSync:        svc.AutoSync,
			Production:      param.Production,
		}, true,
		logger,
	)

	if err != nil {
		return e.ErrEditHelmCharts.AddErr(fmt.Errorf("failed to create service %s in project %s, error: %s", serviceName, productName, err))
	}

	if !param.Production {
		err = service.AutoDeployHelmServiceToEnvs(createdBy, requestID, productName, []*models.Service{svc}, logger)
		if err != nil {
			return e.ErrEditHelmCharts.AddErr(err)
		}
	}

	return nil
}

func prepareChartTemplateData(templateName string, logger *zap.SugaredLogger) (*ChartTemplateData, error) {
	templateChart, err := commonrepo.NewChartColl().Get(templateName)
	if err != nil {
		logger.Errorf("Failed to get chart template %s, err: %s", templateName, err)
		return nil, fmt.Errorf("failed to get chart template: %s", templateName)
	}

	// get chart template from local disk
	localBase := configbase.LocalChartTemplatePath(templateName)
	s3Base := configbase.ObjectStorageChartTemplatePath(templateName)
	if err = fsservice.PreloadFiles(templateName, localBase, s3Base, templateChart.Source, logger); err != nil {
		logger.Errorf("Failed to download template %s, err: %s", templateName, err)
		return nil, err
	}

	base := filepath.Base(templateChart.Path)
	defaultValuesFile := filepath.Join(localBase, base, setting.ValuesYaml)
	defaultValues, _ := os.ReadFile(defaultValuesFile)

	chartFilePath := filepath.Join(localBase, base, setting.ChartYaml)
	chartFileContent, err := os.ReadFile(chartFilePath)
	if err != nil {
		logger.Errorf("Failed to read chartfile template %s, err: %s", templateName, err)
		return nil, err
	}
	chart := new(Chart)
	if err = yaml.Unmarshal(chartFileContent, chart); err != nil {
		logger.Errorf("Failed to unmarshal chart yaml %s, err: %s", setting.ChartYaml, err)
		return nil, err
	}

	return &ChartTemplateData{
		TemplateName:      templateName,
		TemplateData:      templateChart,
		ChartName:         chart.Name,
		ChartVersion:      chart.Version,
		DefaultValuesYAML: defaultValues,
	}, nil
}

func getNextServiceRevision(productName, serviceName string, isProductionService bool) (int64, error) {
	if serviceName == "" {
		return 0, fmt.Errorf("service name cannot be empty")
	}
	rev, err := commonutil.GenerateServiceNextRevision(isProductionService, serviceName, productName)
	if err != nil {
		log.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
		return 0, err
	}
	if !isProductionService {
		if err = commonrepo.NewServiceColl().Delete(serviceName, setting.HelmDeployType, productName, setting.ProductStatusDeleting, rev); err != nil {
			log.Warnf("Failed to delete stale service %s with revision %d, err: %s", serviceName, rev, err)
		}
	} else {
		if err = commonrepo.NewProductionServiceColl().Delete(serviceName, "", productName, setting.ProductStatusDeleting, rev); err != nil {
			log.Warnf("Failed to delete stale service %s with revision %d, err: %s", serviceName, rev, err)
		}
	}
	return rev, err
}

// make local chart info copy with revision
func copyChartRevision(projectName, serviceName string, revision int64, isProductionChart bool) error {
	sourceChartPath, revisionChartLocalPath := config.LocalServicePath(projectName, serviceName, isProductionChart), config.LocalServicePathWithRevision(projectName, serviceName, fmt.Sprint(revision), isProductionChart)
	err := os.RemoveAll(revisionChartLocalPath)
	if err != nil {
		log.Errorf("failed to remove old chart revision data, projectName %s serviceName %s revision %d, err %s", projectName, serviceName, revision, err)
		return err
	}

	err = copy.Copy(sourceChartPath, revisionChartLocalPath)
	if err != nil {
		log.Errorf("failed to copy chart info, projectName %s serviceName %s revision %d, err %s", projectName, serviceName, revision, err)
		return err
	}
	return nil
}

func clearChartFiles(projectName, serviceName string, revision int64, isProduction bool, logger *zap.SugaredLogger, source ...string) {
	clearChartFilesInS3Storage(projectName, serviceName, revision, isProduction, logger)
	if len(source) == 0 {
		clearLocalChartFiles(projectName, serviceName, revision, isProduction, logger)
	}
}

// clear chart files in s3 storage
func clearChartFilesInS3Storage(projectName, serviceName string, revision int64, production bool, logger *zap.SugaredLogger) {
	s3FileNames := []string{serviceName, fmt.Sprintf("%s-%d", serviceName, revision)}
	errRemoveFile := fsservice.DeleteArchivedFileFromS3(s3FileNames, config.ObjectStorageServicePath(projectName, serviceName, production), logger)
	if errRemoveFile != nil {
		logger.Errorf("Failed to remove files: %v from s3 strorage, err: %s", s3FileNames, errRemoveFile)
	}
}

// clear local chart infos
func clearLocalChartFiles(projectName, serviceName string, revision int64, production bool, logger *zap.SugaredLogger) {
	latestChartPath := config.LocalServicePath(projectName, serviceName, production)
	revisionChartLocalPath := config.LocalServicePathWithRevision(projectName, serviceName, fmt.Sprint(revision), production)
	for _, path := range []string{latestChartPath, revisionChartLocalPath} {
		err := os.RemoveAll(path)
		if err != nil {
			logger.Errorf("failed to remove local chart data, path: %s, err: %s", path, err)
		}
	}
}

func CreateOrUpdateHelmService(projectName string, args *HelmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	switch args.Source {
	case LoadFromRepo, LoadFromPublicRepo:
		return CreateOrUpdateHelmServiceFromGitRepo(projectName, args, force, logger)
	case LoadFromChartTemplate:
		return CreateOrUpdateHelmServiceFromChartTemplate(projectName, args, force, logger)
	case LoadFromGerrit, setting.SourceFromGitee, setting.SourceFromGiteeEE:
		return CreateOrUpdateHelmServiceFromRepo(projectName, args, force, logger)
	case LoadFromChartRepo:
		return CreateOrUpdateHelmServiceFromChartRepo(projectName, args, force, logger)
	default:
		return nil, fmt.Errorf("invalid source")
	}
}

func CreateOrUpdateHelmServiceFromChartRepo(projectName string, args *HelmServiceCreationArgs, force bool, log *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	chartRepoArgs, ok := args.CreateFrom.(*CreateFromChartRepo)
	if !ok {
		return nil, e.ErrCreateTemplate.AddDesc("invalid argument")
	}

	chartRepo, err := commonrepo.NewHelmRepoColl().Find(&commonrepo.HelmRepoFindOption{RepoName: chartRepoArgs.ChartRepoName})
	if err != nil {
		log.Errorf("failed to query chart-repo info, productName: %s, err: %s", projectName, err)
		return nil, e.ErrCreateTemplate.AddDesc(fmt.Sprintf("failed to query chart-repo info, productName: %s, repoName: %s", projectName, chartRepoArgs.ChartRepoName))
	}

	hClient, err := commonutil.NewHelmClient(chartRepo)
	if err != nil {
		return nil, e.ErrCreateTemplate.AddErr(errors.Wrapf(err, "failed to init chart client for repo: %s", chartRepo.RepoName))
	}

	chartRef := fmt.Sprintf("%s/%s", chartRepo.RepoName, chartRepoArgs.ChartName)
	localPath := config.LocalServicePath(projectName, chartRepoArgs.ChartName, args.Production)

	log.Infof("downloading chart %s to %s", chartRef, localPath)
	// remove local file to untar
	_ = os.RemoveAll(localPath)
	err = hClient.DownloadChart(commonutil.GeneHelmRepo(chartRepo), chartRef, chartRepoArgs.ChartVersion, localPath, true)
	if err != nil {
		return nil, e.ErrCreateTemplate.AddErr(errors.Wrapf(err, "failed to download chart %s/%s-%s", chartRepo.RepoName, chartRepoArgs.ChartName, chartRepoArgs.ChartVersion))
	}

	serviceName := chartRepoArgs.ChartName
	rev, err := getNextServiceRevision(projectName, serviceName, args.Production)
	if err != nil {
		log.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
		return nil, e.ErrCreateTemplate.AddErr(err)
	}

	var finalErr error
	// clear files from both s3 and local when error occurred in next stages
	defer func() {
		if finalErr != nil {
			clearChartFiles(projectName, serviceName, rev, args.Production, log)
		}
	}()

	// read values.yaml
	fsTree := os.DirFS(localPath)
	valuesYAML, err := util.ReadValuesYAML(fsTree, chartRepoArgs.ChartName, log)
	if err != nil {
		finalErr = e.ErrCreateTemplate.AddErr(err)
		return nil, finalErr
	}

	// upload to s3 storage
	s3Base := config.ObjectStorageServicePath(projectName, serviceName, args.Production)

	err = fsservice.ArchiveAndUploadFilesToS3(fsTree, []string{serviceName, fmt.Sprintf("%s-%d", serviceName, rev)}, s3Base, log)
	if err != nil {
		finalErr = e.ErrCreateTemplate.AddErr(err)
		return nil, finalErr
	}

	// copy service revision data from latest
	err = copyChartRevision(projectName, serviceName, rev, args.Production)
	if err != nil {
		log.Errorf("Failed to copy file %s, err: %s", serviceName, err)
		finalErr = errors.Wrapf(err, "Failed to copy chart info, service %s", serviceName)
		return nil, finalErr
	}

	svc, err := createOrUpdateHelmService(
		fsTree,
		&helmServiceCreationArgs{
			ChartName:       chartRepoArgs.ChartName,
			ChartVersion:    chartRepoArgs.ChartVersion,
			ChartRepoName:   chartRepoArgs.ChartRepoName,
			ServiceRevision: rev,
			MergedValues:    string(valuesYAML),
			ServiceName:     serviceName,
			ProductName:     projectName,
			CreateBy:        args.CreatedBy,
			RequestID:       args.RequestID,
			Source:          setting.SourceFromChartRepo,
			Production:      args.Production,
		}, force,
		log,
	)
	if err != nil {
		log.Errorf("Failed to create service %s in project %s, error: %s", serviceName, projectName, err)
		finalErr = e.ErrCreateTemplate.AddErr(err)
		return nil, finalErr
	}

	if !args.Production {
		err = service.AutoDeployHelmServiceToEnvs(args.CreatedBy, args.RequestID, svc.ProductName, []*models.Service{svc}, log)
		if err != nil {
			finalErr = e.ErrCreateTemplate.AddErr(err)
			return nil, finalErr
		}
	}

	return &BulkHelmServiceCreationResponse{
		SuccessServices: []string{serviceName},
	}, nil
}

func CreateOrUpdateHelmServiceFromChartTemplate(projectName string, args *HelmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	templateArgs, ok := args.CreateFrom.(*CreateFromChartTemplate)
	if !ok {
		return nil, fmt.Errorf("invalid argument")
	}

	if strings.ToLower(args.Name) != args.Name {
		return nil, fmt.Errorf("service name should be lowercase")
	}

	templateChartInfo, err := prepareChartTemplateData(templateArgs.TemplateName, logger)
	if err != nil {
		return nil, err
	}

	return createOrUpdateHelmServiceFromChartTemplate(templateArgs, templateChartInfo, projectName, args, force, logger)
}

func createOrUpdateHelmServiceFromChartTemplate(templateArgs *CreateFromChartTemplate, templateChartInfo *ChartTemplateData, projectName string, args *HelmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {

	// NOTE we may need a better way to handle service name with spaces
	args.Name = strings.TrimSpace(args.Name)

	var values [][]byte
	if len(templateChartInfo.DefaultValuesYAML) > 0 {
		//render variables
		renderedYaml, err := renderVariablesToYaml(string(templateChartInfo.DefaultValuesYAML), projectName, args.Name, templateArgs.Variables)
		if err != nil {
			return nil, err
		}
		values = append(values, []byte(renderedYaml))
	}

	if len(templateArgs.ValuesYAML) > 0 {
		values = append(values, []byte(templateArgs.ValuesYAML))
	}

	localBase := configbase.LocalChartTemplatePath(templateArgs.TemplateName)
	base := filepath.Base(templateChartInfo.TemplateData.Path)

	// copy template to service path and update the values.yaml
	from := filepath.Join(localBase, base)
	to := filepath.Join(config.LocalServicePath(projectName, args.Name, args.Production), args.Name)
	// remove old files
	if err := os.RemoveAll(to); err != nil {
		logger.Errorf("Failed to remove dir %s, err: %s", to, err)
		return nil, err
	}
	if err := copy.Copy(from, to); err != nil {
		logger.Errorf("Failed to copy file from %s to %s, err: %s", from, to, err)
		return nil, err
	}

	merged, err := yamlutil.Merge(values)
	if err != nil {
		logger.Errorf("Failed to merge values, err: %s", err)
		return nil, err
	}

	if err = os.WriteFile(filepath.Join(to, setting.ValuesYaml), merged, 0644); err != nil {
		logger.Errorf("Failed to write values, err: %s", err)
		return nil, err
	}

	// change the parameter below if this is supported again.
	rev, err := getNextServiceRevision(projectName, args.Name, args.Production)
	if err != nil {
		logger.Errorf("Failed to get next revision for service %s, err: %s", args.Name, err)
		return nil, errors.Wrapf(err, "Failed to get service next revision, service %s", args.Name)
	}

	err = copyChartRevision(projectName, args.Name, rev, args.Production)
	if err != nil {
		logger.Errorf("Failed to copy file %s, err: %s", args.Name, err)
		return nil, errors.Wrapf(err, "Failed to copy chart info, service %s", args.Name)
	}

	// clear files from both s3 and local when error occurred in next stages
	defer func() {
		if err != nil {
			clearChartFiles(projectName, args.Name, rev, args.Production, logger)
		}
	}()

	fsTree := os.DirFS(config.LocalServicePath(projectName, args.Name, args.Production))
	serviceS3Base := config.ObjectStorageServicePath(projectName, args.Name, args.Production)
	if err = fsservice.ArchiveAndUploadFilesToS3(fsTree, []string{args.Name, fmt.Sprintf("%s-%d", args.Name, rev)}, serviceS3Base, logger); err != nil {
		logger.Errorf("Failed to upload files for service %s in project %s, err: %s", args.Name, projectName, err)
		return nil, err
	}

	svc, errCreate := createOrUpdateHelmService(
		fsTree,
		&helmServiceCreationArgs{
			ChartName:        templateChartInfo.ChartName,
			ChartVersion:     templateChartInfo.ChartVersion,
			ServiceRevision:  rev,
			MergedValues:     string(merged),
			ServiceName:      args.Name,
			FilePath:         to,
			ProductName:      projectName,
			CreateBy:         args.CreatedBy,
			RequestID:        args.RequestID,
			Source:           setting.SourceFromChartTemplate,
			HelmTemplateName: templateArgs.TemplateName,
			ValuesYaml:       templateArgs.ValuesYAML,
			Variables:        templateArgs.Variables,
			ValuesSource:     args.ValuesData,
			CreationDetail:   args.CreationDetail,
			AutoSync:         args.AutoSync,
			Production:       args.Production,
		}, force,
		logger,
	)

	if errCreate != nil {
		err = errCreate
		logger.Errorf("Failed to create service %s in project %s, error: %s", args.Name, projectName, err)
		return nil, err
	}

	if !args.Production {
		err = service.AutoDeployHelmServiceToEnvs(args.CreatedBy, args.RequestID, svc.ProductName, []*models.Service{svc}, logger)
		if err != nil {
			return nil, err
		}
	}

	return &BulkHelmServiceCreationResponse{
		SuccessServices: []string{args.Name},
	}, nil
}

func getCodehostType(repoArgs *CreateFromRepo, repoLink string) (string, *systemconfig.CodeHost, error) {
	if repoLink != "" {
		return setting.SourceFromPublicRepo, nil, nil
	}
	ch, err := systemconfig.New().GetCodeHost(repoArgs.CodehostID)
	if err != nil {
		log.Errorf("Failed to get codeHost by id %d, err: %s", repoArgs.CodehostID, err.Error())
		return "", ch, err
	}
	return ch.Type, ch, nil
}

func CreateOrUpdateHelmServiceFromRepo(projectName string, args *HelmServiceCreationArgs, force bool, log *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	var (
		filePaths []string
		response  = &BulkHelmServiceCreationResponse{}
		base      string
	)
	resByte, resByteErr := json.Marshal(args.CreateFrom)
	if resByteErr != nil {
		log.Errorf("failed to json.Marshal err:%s", resByteErr)
		return nil, resByteErr
	}
	var createFromRepo CreateFromRepo
	jsonResErr := json.Unmarshal(resByte, &createFromRepo)
	if jsonResErr != nil {
		log.Errorf("failed to json.Unmarshal err:%s", resByteErr)
		return nil, jsonResErr
	}

	// if the repo type is other, we download the chart
	codehostDetail, err := systemconfig.New().GetCodeHost(createFromRepo.CodehostID)
	if err != nil {
		log.Errorf("failed to get codehost detail to pull the repo, error: %s", err)
		return nil, err
	}

	if codehostDetail.Type == setting.SourceFromOther {
		err = command.RunGitCmds(codehostDetail, createFromRepo.Owner, createFromRepo.Namespace, createFromRepo.Repo, createFromRepo.Branch, "origin")
		if err != nil {
			log.Errorf("Failed to clone the repo, namespace: [%s], name: [%s], branch: [%s], error: %s", createFromRepo.Namespace, createFromRepo.Repo, createFromRepo.Branch, err)
			return nil, err
		}
	}

	filePaths = createFromRepo.Paths
	base = path.Join(config.S3StoragePath(), createFromRepo.Repo)

	var wg wait.Group
	var mux sync.RWMutex
	var serviceList []*commonmodels.Service
	for _, p := range filePaths {
		filePath := strings.TrimLeft(p, "/")
		wg.Start(func() {
			var (
				serviceName  string
				chartVersion string
				valuesYAML   []byte
				finalErr     error
			)
			defer func() {
				mux.Lock()
				if finalErr != nil {
					response.FailedServices = append(response.FailedServices, &FailedService{
						Path:  filePath,
						Error: finalErr.Error(),
					})
				} else {
					response.SuccessServices = append(response.SuccessServices, serviceName)
				}
				mux.Unlock()
			}()

			currentFilePath := path.Join(base, filePath)
			log.Infof("Loading chart under path %s", currentFilePath)
			serviceName, chartVersion, finalErr = readChartYAMLFromLocal(currentFilePath, log)
			if finalErr != nil {
				return
			}
			valuesYAML, finalErr = util.ReadValuesYAMLFromLocal(currentFilePath, log)
			if finalErr != nil {
				return
			}

			log.Info("Found valid chart, Starting to save and upload files")
			rev, err := getNextServiceRevision(projectName, serviceName, args.Production)
			if err != nil {
				log.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}

			// clear files from s3 when error occurred in next stages
			defer func() {
				if finalErr != nil {
					clearChartFiles(projectName, serviceName, rev, args.Production, log, string(args.Source))
				}
			}()

			// copy to latest dir and upload to s3
			if err = helmservice.CopyAndUploadService(projectName, serviceName, currentFilePath, []string{fmt.Sprintf("%s-%d", serviceName, rev)}, args.Production); err != nil {
				log.Errorf("Failed to save or upload files for service %s in project %s, error: %s", serviceName, projectName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}

			err = copyChartRevision(projectName, serviceName, rev, args.Production)
			if err != nil {
				log.Errorf("Failed to copy file %s, err: %s", serviceName, err)
				finalErr = errors.Wrapf(err, "Failed to copy chart info, service %s", serviceName)
				return
			}

			var repoLink string
			if string(args.Source) == setting.SourceFromGitee || string(args.Source) == setting.SourceFromGiteeEE {
				codehostInfo, err := systemconfig.New().GetCodeHost(createFromRepo.CodehostID)
				if err != nil {
					finalErr = errors.Wrapf(err, "failed to get code host, id %d", createFromRepo.CodehostID)
					return
				}
				repoLink = fmt.Sprintf("%s/%s/%s/%s/%s/%s", codehostInfo.Address, createFromRepo.Owner, createFromRepo.Repo, "tree", createFromRepo.Branch, filePath)
			}

			source := string(args.Source)
			if source == setting.SourceFromGiteeEE {
				source = setting.SourceFromGitee
			}

			helmServiceCreationArgs := &helmServiceCreationArgs{
				ChartName:        serviceName,
				ChartVersion:     chartVersion,
				ServiceRevision:  rev,
				MergedValues:     string(valuesYAML),
				ServiceName:      serviceName,
				FilePath:         filePath,
				ProductName:      projectName,
				CreateBy:         args.CreatedBy,
				RequestID:        args.RequestID,
				CodehostID:       createFromRepo.CodehostID,
				Owner:            createFromRepo.Owner,
				Namespace:        createFromRepo.Namespace,
				Repo:             createFromRepo.Repo,
				Branch:           createFromRepo.Branch,
				Source:           source,
				RepoLink:         repoLink,
				GiteePath:        currentFilePath,
				GerritCodeHostID: createFromRepo.CodehostID,
				GerritPath:       currentFilePath,
				GerritRepoName:   createFromRepo.Repo,
				GerritBranchName: createFromRepo.Branch,
				GerritRemoteName: "origin",
				Production:       args.Production,
			}

			if string(args.Source) == setting.SourceFromGerrit {
				helmServiceCreationArgs.GerritCodeHostID = createFromRepo.CodehostID
				helmServiceCreationArgs.GerritPath = currentFilePath
				helmServiceCreationArgs.GerritRepoName = createFromRepo.Repo
				helmServiceCreationArgs.GerritBranchName = createFromRepo.Branch
				helmServiceCreationArgs.GerritRemoteName = "origin"
			}

			svc, err := createOrUpdateHelmService(
				nil,
				helmServiceCreationArgs,
				force,
				log,
			)
			if err != nil {
				log.Errorf("Failed to create service %s in project %s, error: %s", serviceName, projectName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}
			serviceList = append(serviceList, svc)
		})
	}

	wg.Wait()

	if !args.Production {
		return response, service.AutoDeployHelmServiceToEnvs(args.CreatedBy, args.RequestID, projectName, serviceList, log)
	} else {
		return response, nil
	}
}

func CreateOrUpdateHelmServiceFromGitRepo(projectName string, args *HelmServiceCreationArgs, force bool, log *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	var err error
	var repoLink string
	repoArgs, ok := args.CreateFrom.(*CreateFromRepo)
	if !ok {
		publicArgs, ok := args.CreateFrom.(*CreateFromPublicRepo)
		if !ok {
			return nil, fmt.Errorf("invalid argument")
		}

		repoArgs, err = PublicRepoToPrivateRepoArgs(publicArgs)
		if err != nil {
			log.Errorf("Failed to parse repo args %+v, err: %s", publicArgs, err)
			return nil, err
		}

		repoLink = publicArgs.RepoLink
	}

	response := &BulkHelmServiceCreationResponse{}
	source, codehostInfo, err := getCodehostType(repoArgs, repoLink)
	if err != nil {
		log.Errorf("Failed to get source form repo data %+v, err: %s", *repoArgs, err.Error())
		return nil, err
	}
	if source == setting.SourceFromOther {
		return CreateOrUpdateHelmServiceFromRepo(projectName, args, force, log)
	}

	helmRenderCharts := make([]*templatemodels.ServiceRender, 0, len(repoArgs.Paths))

	var wg wait.Group
	var mux sync.RWMutex
	serviceList := make([]*commonmodels.Service, 0)
	for _, p := range repoArgs.Paths {
		filePath := strings.TrimLeft(p, "/")
		wg.Start(func() {
			var (
				serviceName  string
				chartVersion string
				valuesYAML   []byte
				finalErr     error
			)
			defer func() {
				mux.Lock()
				if finalErr != nil {
					response.FailedServices = append(response.FailedServices, &FailedService{
						Path:  filePath,
						Error: finalErr.Error(),
					})
				} else {
					response.SuccessServices = append(response.SuccessServices, serviceName)
				}
				mux.Unlock()
			}()

			log.Infof("Loading chart under path %s", filePath)

			fsTree, err := fsservice.DownloadFilesFromSource(
				&fsservice.DownloadFromSourceArgs{CodehostID: repoArgs.CodehostID, Owner: repoArgs.Owner, Namespace: repoArgs.Namespace, Repo: repoArgs.Repo, Path: filePath, Branch: repoArgs.Branch, RepoLink: repoLink},
				func(chartTree afero.Fs) (string, error) {
					var err error
					serviceName, chartVersion, err = readChartYAML(afero.NewIOFS(chartTree), filepath.Base(filePath), log)
					if err != nil {
						return serviceName, err
					}
					valuesYAML, err = util.ReadValuesYAML(afero.NewIOFS(chartTree), filepath.Base(filePath), log)
					return serviceName, err
				})
			if err != nil {
				log.Errorf("Failed to download files from source, err %s", err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}

			log.Info("Found valid chart, Starting to save and upload files")

			rev, err := getNextServiceRevision(projectName, serviceName, args.Production)
			if err != nil {
				log.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}

			// clear files from both s3 and local when error occurred in next stages
			defer func() {
				if finalErr != nil {
					clearChartFiles(projectName, serviceName, rev, args.Production, log)
				}
			}()

			// save files to disk and upload them to s3
			if err = helmservice.SaveAndUploadService(projectName, serviceName, []string{fmt.Sprintf("%s-%d", serviceName, rev)}, fsTree, args.Production); err != nil {
				log.Errorf("Failed to save or upload files for service %s in project %s, error: %s", serviceName, projectName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}

			err = copyChartRevision(projectName, serviceName, rev, args.Production)
			if err != nil {
				log.Errorf("Failed to copy file %s, err: %s", serviceName, err)
				finalErr = errors.Wrapf(err, "Failed to copy chart info, service %s", serviceName)
				return
			}

			if source != setting.SourceFromPublicRepo && codehostInfo != nil {
				repoLink = fmt.Sprintf("%s/%s/%s/%s/%s/%s", codehostInfo.Address, repoArgs.Namespace, repoArgs.Repo, "tree", repoArgs.Branch, filePath)
			}

			svc, err := createOrUpdateHelmService(
				fsTree,
				&helmServiceCreationArgs{
					ChartName:       serviceName,
					ChartVersion:    chartVersion,
					ServiceRevision: rev,
					MergedValues:    string(valuesYAML),
					ServiceName:     serviceName,
					FilePath:        filePath,
					ProductName:     projectName,
					CreateBy:        args.CreatedBy,
					RequestID:       args.RequestID,
					CodehostID:      repoArgs.CodehostID,
					Owner:           repoArgs.Owner,
					Namespace:       repoArgs.Namespace,
					Repo:            repoArgs.Repo,
					Branch:          repoArgs.Branch,
					RepoLink:        repoLink,
					Source:          source,
					Production:      args.Production,
				}, force,
				log,
			)
			if err != nil {
				log.Errorf("Failed to create service %s in project %s, error: %s", serviceName, projectName, err)
				finalErr = e.ErrCreateTemplate.AddErr(err)
				return
			}
			serviceList = append(serviceList, svc)

			helmRenderCharts = append(helmRenderCharts, &templatemodels.ServiceRender{
				ServiceName:  serviceName,
				ChartVersion: svc.HelmChart.Version,
				// ValuesYaml:   svc.HelmChart.ValuesYaml,
			})
		})
	}

	wg.Wait()

	if !args.Production {
		return response, service.AutoDeployHelmServiceToEnvs(args.CreatedBy, args.RequestID, projectName, serviceList, log)
	} else {
		return response, nil
	}
}

func CreateOrUpdateBulkHelmService(projectName string, args *BulkHelmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	switch args.Source {
	case LoadFromChartTemplate:
		return CreateOrUpdateBulkHelmServiceFromTemplate(projectName, args, force, logger)
	default:
		return nil, fmt.Errorf("invalid source")
	}
}

func CreateOrUpdateBulkHelmServiceFromTemplate(projectName string, args *BulkHelmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*BulkHelmServiceCreationResponse, error) {
	templateArgs, ok := args.CreateFrom.(*CreateFromChartTemplate)
	if !ok {
		return nil, fmt.Errorf("invalid argument")
	}

	if args.ValuesData == nil || args.ValuesData.GitRepoConfig == nil || len(args.ValuesData.GitRepoConfig.ValuesPaths) == 0 {
		return nil, fmt.Errorf("invalid argument, missing values")
	}

	templateChartData, err := prepareChartTemplateData(templateArgs.TemplateName, logger)
	if err != nil {
		return nil, err
	}

	localBase := configbase.LocalChartTemplatePath(templateArgs.TemplateName)
	base := filepath.Base(templateChartData.TemplateData.Path)
	// copy template to service path and update the values.yaml
	from := filepath.Join(localBase, base)

	//record errors for every service
	failedServiceMap := &sync.Map{}
	renderChartMap := &sync.Map{}
	svcMap := &sync.Map{}
	serviceList := make([]*commonmodels.Service, 0)

	wg := sync.WaitGroup{}
	// run goroutines to speed up
	for _, singlePath := range args.ValuesData.GitRepoConfig.ValuesPaths {
		wg.Add(1)
		go func(repoConfig *commonservice.RepoConfig, path string) {
			defer wg.Done()
			renderChart, svcInfo, err := handleSingleService(projectName, repoConfig, path, from, args, templateChartData, force, logger)
			if err != nil {
				failedServiceMap.Store(path, err.Error())
			} else {
				svcMap.Store(renderChart.ServiceName, svcInfo)
				renderChartMap.Store(renderChart.ServiceName, renderChart)
			}
		}(args.ValuesData.GitRepoConfig, singlePath)
	}

	wg.Wait()

	resp := &BulkHelmServiceCreationResponse{
		SuccessServices: make([]string, 0),
		FailedServices:  make([]*FailedService, 0),
	}

	renderChars := make([]*templatemodels.ServiceRender, 0)

	renderChartMap.Range(func(key, value interface{}) bool {
		resp.SuccessServices = append(resp.SuccessServices, key.(string))
		renderChars = append(renderChars, value.(*templatemodels.ServiceRender))
		return true
	})
	svcMap.Range(func(key, value interface{}) bool {
		serviceList = append(serviceList, value.(*commonmodels.Service))
		return true
	})

	failedServiceMap.Range(func(key, value interface{}) bool {
		resp.FailedServices = append(resp.FailedServices, &FailedService{
			Path:  key.(string),
			Error: value.(string),
		})
		return true
	})

	if args.Production {
		return resp, nil
	}
	return resp, service.AutoDeployHelmServiceToEnvs(args.CreatedBy, args.RequestID, projectName, serviceList, logger)
}

// @Min TODO: handleSingleService is used by helm services creation from template
// so all the helper function will currently use 'false' in the 'isProd' parameter.
func handleSingleService(projectName string, repoConfig *commonservice.RepoConfig, path, fromPath string, args *BulkHelmServiceCreationArgs,
	templateChartData *ChartTemplateData, force bool, logger *zap.SugaredLogger) (*templatemodels.ServiceRender, *commonmodels.Service, error) {
	valuesYAML, err := fsservice.DownloadFileFromSource(&fsservice.DownloadFromSourceArgs{
		CodehostID: repoConfig.CodehostID,
		Owner:      repoConfig.Owner,
		Repo:       repoConfig.Repo,
		Namespace:  repoConfig.Namespace,
		Path:       path,
		Branch:     repoConfig.Branch,
	})
	if err != nil {
		return nil, nil, err
	}

	if len(valuesYAML) == 0 {
		return nil, nil, fmt.Errorf("values.yaml is empty")
	}

	values := [][]byte{templateChartData.DefaultValuesYAML, valuesYAML}
	mergedValues, err := yamlutil.Merge(values)
	if err != nil {
		logger.Errorf("Failed to merge values, err: %s", err)
		return nil, nil, err
	}

	serviceName := filepath.Base(path)
	serviceName = strings.TrimSuffix(serviceName, filepath.Ext(serviceName))
	serviceName = strings.TrimSpace(serviceName)
	serviceName = strings.ToLower(serviceName)

	var to string
	if args.Production {
		to = filepath.Join(config.LocalProductionServicePath(projectName, serviceName), serviceName)
	} else {
		to = filepath.Join(config.LocalTestServicePath(projectName, serviceName), serviceName)
	}
	// remove old files
	if err = os.RemoveAll(to); err != nil {
		logger.Errorf("Failed to remove dir %s, err: %s", to, err)
		return nil, nil, err
	}
	if err = copy.Copy(fromPath, to); err != nil {
		logger.Errorf("Failed to copy file from %s to %s, err: %s", fromPath, to, err)
		return nil, nil, err
	}

	// write values.yaml file
	if err = os.WriteFile(filepath.Join(to, setting.ValuesYaml), mergedValues, 0644); err != nil {
		logger.Errorf("Failed to write values, err: %s", err)
		return nil, nil, err
	}

	rev, err := getNextServiceRevision(projectName, serviceName, args.Production)
	if err != nil {
		log.Errorf("Failed to get next revision for service %s, err: %s", serviceName, err)
		return nil, nil, errors.Wrapf(err, "Failed to get service next revision, service %s", serviceName)
	}

	err = copyChartRevision(projectName, serviceName, rev, args.Production)
	if err != nil {
		log.Errorf("Failed to copy file %s, err: %s", serviceName, err)
		return nil, nil, errors.Wrapf(err, "Failed to copy chart info, service %s", serviceName)
	}

	var fsTree fs.FS
	var serviceS3Base string

	if args.Production {
		fsTree = os.DirFS(config.LocalProductionServicePath(projectName, serviceName))
		serviceS3Base = config.ObjectStorageProductionServicePath(projectName, serviceName)
	} else {
		fsTree = os.DirFS(config.LocalTestServicePath(projectName, serviceName))
		serviceS3Base = config.ObjectStorageTestServicePath(projectName, serviceName)
	}

	// clear files from both s3 and local when error occurred in next stages
	defer func() {
		if err != nil {
			clearChartFiles(projectName, serviceName, rev, false, logger)
		}
	}()

	if err = fsservice.ArchiveAndUploadFilesToS3(fsTree, []string{serviceName, fmt.Sprintf("%s-%d", serviceName, rev)}, serviceS3Base, logger); err != nil {
		logger.Errorf("Failed to upload files for service %s in project %s, err: %s", serviceName, projectName, err)
		return nil, nil, err
	}

	svc, err := createOrUpdateHelmService(
		fsTree,
		&helmServiceCreationArgs{
			ChartName:        templateChartData.ChartName,
			ChartVersion:     templateChartData.ChartVersion,
			ServiceRevision:  rev,
			MergedValues:     string(mergedValues),
			ServiceName:      serviceName,
			FilePath:         to,
			ProductName:      projectName,
			CreateBy:         args.CreatedBy,
			RequestID:        args.RequestID,
			CodehostID:       repoConfig.CodehostID,
			Source:           setting.SourceFromChartTemplate,
			HelmTemplateName: templateChartData.TemplateName,
			ValuePaths:       []string{path},
			ValuesYaml:       string(valuesYAML),
			AutoSync:         args.AutoSync,
			ValuesSource:     args.ValuesData,
			Production:       args.Production,
		},
		force,
		logger,
	)
	if err != nil {
		logger.Errorf("Failed to create service %s in project %s, error: %s", serviceName, projectName, err)
		return nil, nil, err
	}

	return &templatemodels.ServiceRender{
		ServiceName:  serviceName,
		ChartVersion: templateChartData.ChartVersion,
		// ValuesYaml:   string(mergedValues),
	}, svc, nil
}

func readChartYAML(chartTree fs.FS, base string, logger *zap.SugaredLogger) (string, string, error) {
	chartFile, err := fs.ReadFile(chartTree, filepath.Join(base, setting.ChartYaml))
	if err != nil {
		logger.Errorf("Failed to read %s, err: %s", setting.ChartYaml, err)
		return "", "", err
	}
	chart := new(Chart)
	if err = yaml.Unmarshal(chartFile, chart); err != nil {
		log.Errorf("Failed to unmarshal yaml %s, err: %s", setting.ChartYaml, err)
		return "", "", err
	}

	return chart.Name, chart.Version, nil
}

func readChartYAMLFromLocal(base string, logger *zap.SugaredLogger) (string, string, error) {
	chartFile, err := util.ReadFile(filepath.Join(base, setting.ChartYaml))
	if err != nil {
		logger.Errorf("Failed to read %s, err: %s", setting.ChartYaml, err)
		return "", "", err
	}
	chart := new(Chart)
	if err = yaml.Unmarshal(chartFile, chart); err != nil {
		log.Errorf("Failed to unmarshal yaml %s, err: %s", setting.ChartYaml, err)
		return "", "", err
	}

	return chart.Name, chart.Version, nil
}

func geneCreationDetail(args *helmServiceCreationArgs) interface{} {
	switch args.Source {
	case setting.SourceFromGitlab,
		setting.SourceFromGithub,
		setting.SourceFromGerrit,
		setting.SourceFromGitee,
		setting.SourceFromGiteeEE,
		// FIXME this is a temporary solution, remove when possible
		setting.SourceFromGitRepo:
		return &models.CreateFromRepo{
			GitRepoConfig: &templatemodels.GitRepoConfig{
				CodehostID: args.CodehostID,
				Owner:      args.Owner,
				Repo:       args.Repo,
				Branch:     args.Branch,
				Namespace:  args.Namespace,
			},
			LoadPath: args.FilePath,
		}
	case setting.SourceFromPublicRepo:
		return &models.CreateFromPublicRepo{
			RepoLink: args.RepoLink,
			LoadPath: args.FilePath,
		}
	case setting.SourceFromChartTemplate:
		yamlData := &templatemodels.CustomYaml{
			YamlContent: args.ValuesYaml,
		}
		variables := make([]*models.Variable, 0, len(args.Variables))
		for _, variable := range args.Variables {
			variables = append(variables, &models.Variable{
				Key:   variable.Key,
				Value: variable.Value,
			})
		}
		if args.ValuesSource != nil && args.ValuesSource.GitRepoConfig != nil {
			//yamlData.Source = args.ValuesSource.YamlSource
			yamlData.Source = setting.SourceFromGitRepo
			repoData := &models.CreateFromRepo{
				GitRepoConfig: &templatemodels.GitRepoConfig{
					CodehostID: args.ValuesSource.GitRepoConfig.CodehostID,
					Owner:      args.ValuesSource.GitRepoConfig.Owner,
					Repo:       args.ValuesSource.GitRepoConfig.Repo,
					Branch:     args.ValuesSource.GitRepoConfig.Branch,
					Namespace:  args.ValuesSource.GitRepoConfig.Namespace,
				},
			}
			if len(args.ValuesSource.GitRepoConfig.ValuesPaths) > 0 {
				repoData.LoadPath = args.ValuesSource.GitRepoConfig.ValuesPaths[0]
			}
			yamlData.SourceDetail = repoData
		}
		return &models.CreateFromChartTemplate{
			YamlData:     yamlData,
			TemplateName: args.HelmTemplateName,
			ServiceName:  args.ServiceName,
			Variables:    variables,
		}
	case setting.SourceFromChartRepo:
		return models.CreateFromChartRepo{
			ChartRepoName: args.ChartRepoName,
			ChartName:     args.ChartName,
			ChartVersion:  args.ChartVersion,
		}
	}
	return nil
}

func renderVariablesToYaml(valuesYaml string, productName, serviceName string, variables []*Variable) (string, error) {
	valuesYaml = strings.Replace(valuesYaml, setting.TemplateVariableProduct, productName, -1)
	valuesYaml = strings.Replace(valuesYaml, setting.TemplateVariableService, serviceName, -1)

	// build replace data
	valuesMap := make(map[string]interface{})
	for _, variable := range variables {
		valuesMap[variable.Key] = variable.Value
	}

	tmpl, err := template.New("values").Parse(valuesYaml)
	if err != nil {
		log.Errorf("failed to parse template, err %s valuesYaml %s", err, valuesYaml)
		return "", errors.Wrapf(err, "failed to parse template, err %s", err)
	}

	buf := bytes.NewBufferString("")
	err = tmpl.Execute(buf, valuesMap)
	if err != nil {
		log.Errorf("failed to render values content, err %s", err)
		return "", fmt.Errorf("failed to render variables")
	}
	valuesYaml = buf.String()
	return valuesYaml, nil
}

func createOrUpdateHelmService(fsTree fs.FS, args *helmServiceCreationArgs, force bool, logger *zap.SugaredLogger) (*commonmodels.Service, error) {
	var (
		chartName, chartVersion string
		err                     error
	)
	// FIXME: use a temporary source, make sure it is correct next time
	tempSource := args.Source
	if args.Source == setting.SourceFromGitRepo {
		ch, err := systemconfig.New().GetCodeHost(args.CodehostID)
		if err != nil {
			log.Errorf("failed to get codehost detail, err: %s", err)
			return nil, err
		}
		if ch.Type == setting.SourceFromOther {
			tempSource = setting.SourceFromOther
		}
	}

	switch tempSource {
	case string(LoadFromGerrit):
		base := path.Join(config.S3StoragePath(), args.GerritRepoName)
		chartName, chartVersion, err = readChartYAMLFromLocal(filepath.Join(base, args.FilePath), logger)
	case setting.SourceFromGitee, setting.SourceFromGiteeEE, setting.SourceFromOther:
		base := path.Join(config.S3StoragePath(), args.Repo)
		chartName, chartVersion, err = readChartYAMLFromLocal(filepath.Join(base, args.FilePath), logger)
	default:
		chartName, chartVersion, err = readChartYAML(fsTree, args.ServiceName, logger)
	}

	if err != nil {
		logger.Errorf("Failed to read chart.yaml, err %s", err)
		return nil, err
	}

	valuesYaml := args.MergedValues
	valuesMap := make(map[string]interface{})
	err = yaml.Unmarshal([]byte(valuesYaml), &valuesMap)
	if err != nil {
		logger.Errorf("Failed to unmarshall yaml, err %s", err)
		return nil, err
	}

	containerList, err := commonutil.ParseImagesForProductService(valuesMap, args.ServiceName, args.ProductName)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to parse service from yaml")
	}

	serviceObj := &commonmodels.Service{
		ServiceName:   args.ServiceName,
		Type:          setting.HelmDeployType,
		Revision:      args.ServiceRevision,
		ProductName:   args.ProductName,
		Visibility:    setting.PrivateVisibility,
		CreateTime:    time.Now().Unix(),
		CreateBy:      args.CreateBy,
		Containers:    containerList,
		CodehostID:    args.CodehostID,
		RepoOwner:     args.Owner,
		RepoNamespace: args.Namespace,
		RepoName:      args.Repo,
		BranchName:    args.Branch,
		LoadPath:      args.FilePath,
		SrcPath:       args.RepoLink,
		Source:        args.Source,
		ReleaseNaming: setting.DefaultReleaseNaming,
		AutoSync:      args.AutoSync,
		HelmChart: &commonmodels.HelmChart{
			Name:       chartName,
			Version:    chartVersion,
			ValuesYaml: valuesYaml,
		},
	}
	if args.CreationDetail != nil {
		serviceObj.CreateFrom = args.CreationDetail
	} else {
		serviceObj.CreateFrom = geneCreationDetail(args)
	}

	switch args.Source {
	case string(LoadFromGerrit):
		serviceObj.GerritPath = args.GerritPath
		serviceObj.GerritCodeHostID = args.GerritCodeHostID
		serviceObj.GerritRepoName = args.GerritRepoName
		serviceObj.GerritBranchName = args.GerritBranchName
		serviceObj.GerritRemoteName = args.GerritRemoteName
	case setting.SourceFromGitee, setting.SourceFromGiteeEE:
		serviceObj.GiteePath = args.GiteePath
	}

	log.Infof("Starting to create service %s with revision %d", args.ServiceName, args.ServiceRevision)
	currentSvcTmpl, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ProductName:         args.ProductName,
		ServiceName:         args.ServiceName,
		ExcludeStatus:       setting.ProductStatusDeleting,
		IgnoreNoDocumentErr: true,
	}, args.Production)
	if err != nil {
		log.Errorf("Failed to find current service template %s error: %s", args.ServiceName, err)
		return nil, err
	}

	// update status of current service template to deleting
	if currentSvcTmpl != nil {
		if !force {
			return nil, fmt.Errorf("service:%s already exists", args.ServiceName)
		}
		err = repository.UpdateStatus(args.ServiceName, args.ProductName, setting.ProductStatusDeleting, args.Production)
		if err != nil {
			log.Errorf("Failed to set status of current service templates, serviceName: %s, err: %s", args.ServiceName, err)
			return nil, err
		}
		serviceObj.ReleaseNaming = currentSvcTmpl.GetReleaseNaming()
	}

	// create new service template
	if !args.Production {
		if err = commonrepo.NewServiceColl().Create(serviceObj); err != nil {
			log.Errorf("Failed to create service %s error: %s", args.ServiceName, err)
			return nil, err
		}
	} else {
		if err = commonrepo.NewProductionServiceColl().Create(serviceObj); err != nil {
			log.Errorf("Failed to create production service %s error: %s", args.ServiceName, err)
			return nil, err
		}
	}

	// TODO: webhook process
	switch args.Source {
	case string(LoadFromGerrit):
		if err := createGerritWebhookByService(args.CodehostID, args.ServiceName, args.Repo, args.Branch); err != nil {
			log.Errorf("Failed to create gerrit webhook, err: %s", err)
			return nil, err
		}
	case setting.SourceFromOther:
		// no webhook is required
		break
	default:
		commonservice.ProcessServiceWebhook(serviceObj, currentSvcTmpl, args.ServiceName, args.Production, logger)
	}

	if !args.Production {
		if err = templaterepo.NewProductColl().AddService(args.ProductName, args.ServiceName); err != nil {
			log.Errorf("Failed to add service %s to project %s, err: %s", args.ProductName, args.ServiceName, err)
			return nil, err
		}
	} else {
		if err = templaterepo.NewProductColl().AddProductionService(args.ProductName, args.ServiceName); err != nil {
			log.Errorf("Failed to add production service %s to project %s, err: %s", args.ProductName, args.ServiceName, err)
			return nil, err
		}
	}

	return serviceObj, nil
}

func loadServiceFileInfos(productName, serviceName string, revision int64, dir string, production bool) ([]*types.FileInfo, error) {
	svc, err := repository.QueryTemplateService(&commonrepo.ServiceFindOption{
		ProductName: productName,
		ServiceName: serviceName,
	}, production)
	if err != nil {
		return nil, e.ErrFilePath.AddDesc(err.Error())
	}

	base := config.LocalServicePath(productName, serviceName, production)
	if revision > 0 {
		base = config.LocalServicePathWithRevision(productName, serviceName, fmt.Sprint(revision), production)
		if err = commonutil.PreloadServiceManifestsByRevision(base, svc, production); err != nil {
			log.Warnf("failed to get chart of revision: %d for service: %s, use latest version",
				svc.Revision, svc.ServiceName)
		}
	}
	if err != nil || revision == 0 {
		base = config.LocalServicePath(productName, serviceName, production)
		err = commonutil.PreLoadServiceManifests(base, svc, production)
		if err != nil {
			return nil, e.ErrFilePath.AddDesc(err.Error())
		}
	}

	err = commonutil.PreLoadServiceManifests(base, svc, production)
	if err != nil {
		return nil, e.ErrFilePath.AddDesc(err.Error())
	}

	var fis []*types.FileInfo

	files, err := os.ReadDir(filepath.Join(base, serviceName, dir))
	if err != nil {
		return nil, e.ErrFilePath.AddDesc(err.Error())
	}

	for _, file := range files {
		info, _ := file.Info()
		if info == nil {
			continue
		}
		fi := &types.FileInfo{
			Parent:  dir,
			Name:    file.Name(),
			Size:    info.Size(),
			Mode:    file.Type(),
			ModTime: info.ModTime().Unix(),
			IsDir:   file.IsDir(),
		}

		fis = append(fis, fi)
	}
	return fis, nil
}
