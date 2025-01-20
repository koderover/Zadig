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

package util

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/27149chen/afero"
	"gopkg.in/yaml.v2"
	"helm.sh/helm/v3/pkg/repo"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/command"
	fsservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/fs"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/systemconfig"
	helmtool "github.com/koderover/zadig/v2/pkg/tool/helmclient"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/koderover/zadig/v2/pkg/util/converter"
	fsutil "github.com/koderover/zadig/v2/pkg/util/fs"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

var (
	imageParseRegex = regexp.MustCompile(`(?P<repo>.+/)?(?P<image>[^:]+){1}(:)?(?P<tag>.+)?`)
)

func DownloadServiceManifests(base, projectName, serviceName string, production bool) error {
	s3Base := config.ObjectStorageServicePath(projectName, serviceName, production)
	return fsservice.DownloadAndExtractFilesFromS3(serviceName, base, s3Base, log.SugaredLogger())
}

func DownloadProductionServiceManifests(base, productName, serviceName string) error {
	s3Base := config.ObjectStorageProductionServicePath(productName, serviceName)
	return fsservice.DownloadAndExtractFilesFromS3(serviceName, base, s3Base, log.SugaredLogger())
}

func SaveAndUploadService(projectName, serviceName string, copies []string, fileTree fs.FS, isProductionService bool) error {
	localBase, s3Base := config.LocalServicePath(projectName, serviceName, isProductionService), config.ObjectStorageServicePath(projectName, serviceName, isProductionService)
	names := append([]string{serviceName}, copies...)
	return fsservice.SaveAndUploadFiles(fileTree, names, localBase, s3Base, log.SugaredLogger())
}

func CopyAndUploadService(projectName, serviceName, currentChartPath string, copies []string, isProductionService bool) error {
	localBase, s3Base := config.LocalServicePath(projectName, serviceName, isProductionService), config.ObjectStorageServicePath(projectName, serviceName, isProductionService)
	names := append([]string{serviceName}, copies...)
	return fsservice.CopyAndUploadFiles(names, path.Join(localBase, serviceName), s3Base, localBase, currentChartPath, log.SugaredLogger())
}

// ExtractImageName extract image name from total image uri
func ExtractImageName(imageURI string) string {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageURI)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			if exNames[i] == "image" {
				return matchedStr
			}
		}
	}
	return ""
}

func ExtractImageTag(imageURI string) string {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageURI)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			if exNames[i] == "tag" {
				return matchedStr
			}
		}
	}
	return ""
}

func PreloadServiceManifestsByRevision(base string, svc *commonmodels.Service, production bool) error {
	ok, err := fsutil.DirExists(base)
	if err != nil {
		log.Errorf("Failed to check if dir %s is exiting, err: %s", base, err)
		return err
	}
	if ok {
		return nil
	}

	//download chart info by revision
	serviceNameWithRevision := config.ServiceNameWithRevision(svc.ServiceName, svc.Revision)
	s3Base := config.ObjectStorageServicePath(svc.ProductName, svc.ServiceName, production)
	return fsservice.DownloadAndExtractFilesFromS3(serviceNameWithRevision, base, s3Base, log.SugaredLogger())
}

func PreloadProductionServiceManifestsByRevision(base string, svc *commonmodels.Service) error {
	ok, err := fsutil.DirExists(base)
	if err != nil {
		log.Errorf("Failed to check if dir %s is exiting, err: %s", base, err)
		return err
	}
	if ok {
		return nil
	}

	//download chart info by revision
	serviceNameWithRevision := config.ServiceNameWithRevision(svc.ServiceName, svc.Revision)
	s3Base := config.ObjectStorageProductionServicePath(svc.ProductName, svc.ServiceName)
	return fsservice.DownloadAndExtractFilesFromS3(serviceNameWithRevision, base, s3Base, log.SugaredLogger())
}

func PreLoadServiceManifests(base string, svc *commonmodels.Service, production bool) error {
	ok, err := fsutil.DirExists(base)
	if err != nil {
		log.Errorf("Failed to check if dir %s is exiting, err: %s", base, err)
		return err
	}
	if ok {
		return nil
	}

	if err = DownloadServiceManifests(base, svc.ProductName, svc.ServiceName, production); err == nil {
		return nil
	}

	log.Warnf("Failed to download service from s3, err: %s", err)
	switch svc.Source {
	case setting.SourceFromGerrit:
		return preLoadServiceManifestsFromGerrit(svc, production)
	case setting.SourceFromGitee, setting.SourceFromGiteeEE:
		return preLoadServiceManifestsFromGitee(svc, production)
	default:
		return preLoadServiceManifestsFromSource(svc, production)
	}
}

func PreLoadProductionServiceManifests(base string, svc *commonmodels.Service) error {
	return PreLoadServiceManifests(base, svc, true)
}

func preLoadServiceManifestsFromGerrit(svc *commonmodels.Service, isProductionService bool) error {
	base := path.Join(config.S3StoragePath(), svc.GerritRepoName)
	if err := os.RemoveAll(base); err != nil {
		log.Warnf("Failed to remove dir, err:%s", err)
	}
	detail, err := systemconfig.New().GetCodeHost(svc.GerritCodeHostID)
	if err != nil {
		log.Errorf("Failed to GetCodehostDetail, err:%s", err)
		return err
	}
	err = command.RunGitCmds(detail, "default", "default", svc.GerritRepoName, svc.GerritBranchName, svc.GerritRemoteName)
	if err != nil {
		log.Errorf("Failed to runGitCmds, err:%s", err)
		return err
	}
	// copy files to disk and upload them to s3
	if err := CopyAndUploadService(svc.ProductName, svc.ServiceName, svc.GerritPath, nil, isProductionService); err != nil {
		log.Errorf("Failed to save or upload files for service %s in project %s, error: %s", svc.ServiceName, svc.ProductName, err)
		return err
	}
	return nil
}

func preLoadServiceManifestsFromGitee(svc *commonmodels.Service, isProductionService bool) error {
	base := path.Join(config.S3StoragePath(), svc.RepoName)
	if err := os.RemoveAll(base); err != nil {
		log.Warnf("Failed to remove dir, err:%s", err)
	}
	detail, err := systemconfig.New().GetCodeHost(svc.CodehostID)
	if err != nil {
		log.Errorf("Failed to GetCodehostDetail, err:%s", err)
		return err
	}
	err = command.RunGitCmds(detail, svc.RepoOwner, svc.GetRepoNamespace(), svc.RepoName, svc.BranchName, "origin")
	if err != nil {
		log.Errorf("Failed to runGitCmds, err:%s", err)
		return err
	}
	// save files to disk and upload them to s3
	if err := CopyAndUploadService(svc.ProductName, svc.ServiceName, svc.GiteePath, nil, isProductionService); err != nil {
		log.Errorf("Failed to copy or upload files for service %s in project %s, error: %s", svc.ServiceName, svc.ProductName, err)
		return err
	}
	return nil
}

func preLoadServiceManifestsFromSource(svc *commonmodels.Service, isProductionService bool) error {
	tree, err := fsservice.DownloadFilesFromSource(
		&fsservice.DownloadFromSourceArgs{CodehostID: svc.CodehostID, Owner: svc.RepoOwner, Namespace: svc.RepoNamespace, Repo: svc.RepoName, Path: svc.LoadPath, Branch: svc.BranchName, RepoLink: svc.SrcPath},
		func(afero.Fs) (string, error) {
			return svc.ServiceName, nil
		})
	if err != nil {
		return err
	}

	// save files to disk and upload them to s3
	if err = SaveAndUploadService(svc.ProductName, svc.ServiceName, nil, tree, isProductionService); err != nil {
		log.Errorf("Failed to save or upload files for service %s in project %s, error: %s", svc.ServiceName, svc.ProductName, err)
		return err
	}

	return nil
}

// parse image url to map: repo=>xxx/xx/xx image=>xx tag=>xxx
func resolveImageUrl(imageUrl string) map[string]string {
	subMatchAll := imageParseRegex.FindStringSubmatch(imageUrl)
	result := make(map[string]string)
	exNames := imageParseRegex.SubexpNames()
	for i, matchedStr := range subMatchAll {
		if i != 0 && matchedStr != "" && matchedStr != ":" {
			result[exNames[i]] = matchedStr
		}
	}
	return result
}

// AssignImageData assign image url data into match data
// matchData: image=>absolute-path repo=>absolute-path tag=>absolute-path
// return: absolute-image-path=>image-value  absolute-repo-path=>repo-value absolute-tag-path=>tag-value
func AssignImageData(imageUrl string, matchData map[string]string) (map[string]interface{}, error) {
	ret := make(map[string]interface{})
	// total image url assigned into one single value
	if len(matchData) == 1 {
		for _, v := range matchData {
			ret[v] = imageUrl
		}
		return ret, nil
	}

	resolvedImageUrl := resolveImageUrl(imageUrl)

	// image url assigned into repo/namespace/image+tag
	if len(matchData) == 4 {
		// build namespace data
		namespace := strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
		repoUrlStrs := strings.Split(namespace, "/")
		if len(repoUrlStrs) > 1 {
			resolvedImageUrl[setting.PathSearchComponentNamespace] = strings.Join(repoUrlStrs[1:], "/")
			resolvedImageUrl[setting.PathSearchComponentRepo] = repoUrlStrs[0]
		}

		ret[matchData[setting.PathSearchComponentRepo]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
		ret[matchData[setting.PathSearchComponentNamespace]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentNamespace], "/")
		ret[matchData[setting.PathSearchComponentImage]] = resolvedImageUrl[setting.PathSearchComponentImage]
		ret[matchData[setting.PathSearchComponentTag]] = resolvedImageUrl[setting.PathSearchComponentTag]
		return ret, nil
	}

	// image url assigned into repo/image+tag
	if len(matchData) == 3 {
		ret[matchData[setting.PathSearchComponentRepo]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
		ret[matchData[setting.PathSearchComponentImage]] = resolvedImageUrl[setting.PathSearchComponentImage]
		ret[matchData[setting.PathSearchComponentTag]] = resolvedImageUrl[setting.PathSearchComponentTag]
		return ret, nil
	}

	if len(matchData) == 2 {
		// image url assigned into repo/image + tag
		if tagPath, ok := matchData[setting.PathSearchComponentTag]; ok {
			ret[tagPath] = resolvedImageUrl[setting.PathSearchComponentTag]
			for k, imagePath := range matchData {
				if k == setting.PathSearchComponentTag {
					continue
				}
				ret[imagePath] = fmt.Sprintf("%s%s", resolvedImageUrl[setting.PathSearchComponentRepo], resolvedImageUrl[setting.PathSearchComponentImage])
				break
			}
			return ret, nil
		}
		// image url assigned into repo + image(tag)
		ret[matchData[setting.PathSearchComponentRepo]] = strings.TrimSuffix(resolvedImageUrl[setting.PathSearchComponentRepo], "/")
		ret[matchData[setting.PathSearchComponentImage]] = fmt.Sprintf("%s:%s", resolvedImageUrl[setting.PathSearchComponentImage], resolvedImageUrl[setting.PathSearchComponentTag])
		return ret, nil
	}

	return nil, fmt.Errorf("match data illegal, expect length: 1-3, actual length: %d", len(matchData))
}

// ReplaceImage replace image defines in yaml by new version
func ReplaceImage(sourceYaml string, imageValuesMap ...map[string]interface{}) (string, error) {
	bytes := [][]byte{[]byte(sourceYaml)}
	for _, imageValues := range imageValuesMap {
		nestedMap, err := converter.Expand(imageValues)
		if err != nil {
			return "", err
		}
		bs, err := yaml.Marshal(nestedMap)
		if err != nil {
			return "", err
		}
		bytes = append(bytes, bs)
	}

	mergedBs, err := yamlutil.Merge(bytes)
	if err != nil {
		return "", err
	}
	return string(mergedBs), nil
}

func GeneHelmRepo(chartRepo *commonmodels.HelmRepo) *repo.Entry {
	return &repo.Entry{
		Name:     chartRepo.RepoName,
		URL:      chartRepo.URL,
		Username: chartRepo.Username,
		Password: chartRepo.Password,
	}
}

func GetValidMatchData(spec *commonmodels.ImagePathSpec) map[string]string {
	ret := make(map[string]string)
	if spec.Repo != "" {
		ret[setting.PathSearchComponentRepo] = spec.Repo
	}
	if spec.Namespace != "" {
		ret[setting.PathSearchComponentNamespace] = spec.Namespace
	}
	if spec.Image != "" {
		ret[setting.PathSearchComponentImage] = spec.Image
	}
	if spec.Tag != "" {
		ret[setting.PathSearchComponentTag] = spec.Tag
	}
	return ret
}

// @note can be deprecated or not?
// may duplicate with kube.GeneMergedValues
// func GeneHelmMergedValues(productSvc *commonmodels.ProductService, defaultValues string, renderChart *templatemodels.ServiceRender) (string, error) {
// 	imageKVS := make([]*helmtool.KV, 0)
// 	if productSvc != nil {
// 		targetContainers := productSvc.Containers
// 		replaceValuesMaps := make([]map[string]interface{}, 0)
// 		for _, targetContainer := range targetContainers {
// 			replaceValuesMap, err := AssignImageData(targetContainer.Image, GetValidMatchData(targetContainer.ImagePath))
// 			if err != nil {
// 				return "", fmt.Errorf("failed to pase image uri %s/%s, err %s", productSvc.ProductName, productSvc.ServiceName, err.Error())
// 			}
// 			replaceValuesMaps = append(replaceValuesMaps, replaceValuesMap)
// 		}

// 		for _, imageSecs := range replaceValuesMaps {
// 			for key, value := range imageSecs {
// 				imageKVS = append(imageKVS, &helmtool.KV{
// 					Key:   key,
// 					Value: value,
// 				})
// 			}
// 		}
// 	}

// 	mergedValues, err := helmtool.MergeOverrideValues("", defaultValues, renderChart.GetOverrideYaml(), renderChart.OverrideValues, imageKVS)
// 	if err != nil {
// 		return "", fmt.Errorf("failed to merge override yaml %s and values %s, err: %s", renderChart.GetOverrideYaml(), renderChart.OverrideValues, err)
// 	}
// 	return mergedValues, nil
// }

func NewHelmClient(chartRepo *commonmodels.HelmRepo) (*helmtool.HelmClient, error) {
	client, err := helmtool.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to new helm client, err: %s", err)
	}

	if !chartRepo.EnableProxy {
		return client, nil
	} else {
		proxy, err := commonrepo.NewProxyColl().List(&commonrepo.ProxyArgs{})
		if err != nil {
			return nil, fmt.Errorf("failed to get proxy, err: %s", err)
		}

		if len(proxy) == 0 {
			return nil, fmt.Errorf("enabled proxy for helm client, but no proxy found")
		}

		transport, err := util.NewTransport(chartRepo.URL, "", "", "", false, proxy[0].GetProxyURL())
		if err != nil {
			return nil, fmt.Errorf("failed to new transport, err: %s", err)
		}

		client.Transport = transport

		return client, nil
	}
}

func GenHelmChartProxy(chartRepo *commonmodels.HelmRepo) (*helmtool.Proxy, error) {
	proxy := &helmtool.Proxy{
		Enabled: chartRepo.EnableProxy,
		URL:     chartRepo.URL,
	}

	if chartRepo.EnableProxy {
		proxies, err := commonrepo.NewProxyColl().List(&commonrepo.ProxyArgs{})
		if err != nil {
			return nil, fmt.Errorf("failed to get proxy, err: %s", err)
		}

		if len(proxies) == 0 {
			return nil, fmt.Errorf("enabled proxy for helm chart, but no proxy found")
		}
		proxy.ProxyURL = proxies[0].GetProxyURL()
	}

	return proxy, nil
}
