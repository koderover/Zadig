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
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	templatemodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models/template"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/command"
	fsservice "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/service/fs"
	commontypes "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/types"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/systemconfig"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	yamlutil "github.com/koderover/zadig/v2/pkg/util/yaml"
)

type DefaultValuesResp struct {
	DefaultVariable string                     `json:"default_variable"`
	YamlData        *templatemodels.CustomYaml `json:"yaml_data,omitempty"`
}

type YamlContentRequestArg struct {
	CodehostID  int    `json:"codehostID" form:"codehostID"`
	Owner       string `json:"owner" form:"owner"`
	Repo        string `json:"repo" form:"repo"`
	Namespace   string `json:"namespace" form:"namespace"`
	Branch      string `json:"branch" form:"branch"`
	RepoLink    string `json:"repoLink" form:"repoLink"`
	ValuesPaths string `json:"valuesPaths" form:"valuesPaths"`
}

func fromGitRepo(source string) bool {
	if source == "" {
		return true
	}
	if source == setting.SourceFromGitRepo {
		return true
	}
	return false
}

func syncYamlFromVariableSet(yamlData *templatemodels.CustomYaml, curValue string) (bool, string, error) {
	if yamlData.Source != setting.SourceFromVariableSet {
		return false, "", nil
	}
	variableSet, err := commonrepo.NewVariableSetColl().Find(&commonrepo.VariableSetFindOption{
		ID: yamlData.SourceID,
	})
	if err != nil {
		return false, "", err
	}
	equal, err := yamlutil.Equal(variableSet.VariableYaml, curValue)
	if err != nil || equal {
		return false, "", err
	}
	return true, variableSet.VariableYaml, nil
}

func syncYamlFromGit(yamlData *templatemodels.CustomYaml, curValue string, originValue string) (bool, string, error) {
	if !fromGitRepo(yamlData.Source) {
		return false, "", nil
	}
	sourceDetail, err := service.UnMarshalSourceDetail(yamlData.SourceDetail)
	if err != nil {
		return false, "", err
	}
	if sourceDetail.GitRepoConfig == nil {
		log.Warnf("git repo config is nil")
		return false, "", nil
	}
	repoConfig := sourceDetail.GitRepoConfig

	valuesYAML, err := fsservice.DownloadFileFromSource(&fsservice.DownloadFromSourceArgs{
		CodehostID: repoConfig.CodehostID,
		Namespace:  repoConfig.Namespace,
		Owner:      repoConfig.Owner,
		Repo:       repoConfig.Repo,
		Path:       sourceDetail.LoadPath,
		Branch:     repoConfig.Branch,
	})
	if err != nil {
		return false, "", err
	}
	equal, err := yamlutil.Equal(string(valuesYAML), originValue)
	if err != nil || equal {
		return false, "", err
	}
	return true, string(valuesYAML), nil
}

// SyncYamlFromSource sync values.yaml from source
// NOTE for git source currently only support gitHub and gitlab
func SyncYamlFromSource(yamlData *templatemodels.CustomYaml, curValue string, originValue string) (bool, string, error) {
	if yamlData == nil || !yamlData.AutoSync {
		return false, "", nil
	}
	if yamlData.Source == setting.SourceFromVariableSet {
		return syncYamlFromVariableSet(yamlData, curValue)
	}
	return syncYamlFromGit(yamlData, curValue, originValue)
}

func GetDefaultValues(productName, envName string, production bool, log *zap.SugaredLogger) (*DefaultValuesResp, error) {
	ret := &DefaultValuesResp{}

	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err == mongo.ErrNoDocuments {
		return ret, nil
	}
	if err != nil {
		log.Errorf("failed to query product info, productName %s envName %s err %s", productName, envName, err)
		return nil, fmt.Errorf("failed to query product info, productName %s envName %s", productName, envName)
	}

	ret.DefaultVariable = productInfo.DefaultValues
	err = service.FillGitNamespace(productInfo.YamlData)
	if err != nil {
		// Note, since user can always reselect the git info, error should not block normal logic
		log.Warnf("failed to fill git namespace data, err: %s", err)
	}
	ret.YamlData = productInfo.YamlData

	return ret, nil
}

func GetMergedYamlContent(arg *YamlContentRequestArg, paths []string) (string, error) {
	var (
		fileContentMap sync.Map
		wg             sync.WaitGroup
		err            error
		errLock        sync.Mutex
	)
	detail, err := systemconfig.New().GetCodeHost(arg.CodehostID)
	if err != nil {
		log.Errorf("GetGitRepoInfo GetCodehostDetail err:%s", err)
		return "", e.ErrListRepoDir.AddDesc(err.Error())
	}
	if detail.Type == setting.SourceFromOther {
		err = command.RunGitCmds(detail, arg.Namespace, arg.Namespace, arg.Repo, arg.Branch, "origin")
		if err != nil {
			log.Errorf("GetGitRepoInfo runGitCmds err:%s", err)
			return "", e.ErrListRepoDir.AddDesc(err.Error())
		}
	}

	errorList := &multierror.Error{}

	for i, filePath := range paths {
		wg.Add(1)
		go func(index int, filePath string, isOtherTypeRepo bool) {
			defer wg.Done()
			if !isOtherTypeRepo {
				fileContent, errDownload := fsservice.DownloadFileFromSource(
					&fsservice.DownloadFromSourceArgs{
						CodehostID: arg.CodehostID,
						Owner:      arg.Owner,
						Namespace:  arg.Namespace,
						Repo:       arg.Repo,
						Path:       filePath,
						Branch:     arg.Branch,
						RepoLink:   arg.RepoLink,
					})
				if errDownload != nil {
					errLock.Lock()
					errorList = multierror.Append(errorList, errors.Wrapf(errDownload, fmt.Sprintf("failed to download file from git, path %s", filePath)))
					errLock.Unlock()
					return
				}
				fileContentMap.Store(index, fileContent)
			} else {
				base := path.Join(config.S3StoragePath(), arg.Repo)
				relativePath := path.Join(base, filePath)
				fileContent, errReadFile := os.ReadFile(relativePath)
				if errReadFile != nil {
					errLock.Lock()
					errorList = multierror.Append(errorList, errors.Wrapf(errReadFile, fmt.Sprintf("failed to read file from git repo, relative path %s", relativePath)))
					errLock.Unlock()
					return
				}
				fileContentMap.Store(index, fileContent)
			}
		}(i, filePath, detail.Type == setting.SourceFromOther)
	}
	wg.Wait()

	err = errorList.ErrorOrNil()
	if err != nil {
		return "", err
	}

	contentArr := make([][]byte, 0, len(paths))
	for i := 0; i < len(paths); i++ {
		contentObj, _ := fileContentMap.Load(i)
		if contentObj != nil {
			contentArr = append(contentArr, contentObj.([]byte))
		}
	}
	ret, err := yamlutil.Merge(contentArr)
	if err != nil {
		return "", errors.Wrapf(err, "failed to merge files")
	}
	return string(ret), nil
}

func GetGlobalVariables(productName, envName string, production bool, log *zap.SugaredLogger) ([]*commontypes.GlobalVariableKV, int64, error) {
	productInfo, err := commonrepo.NewProductColl().Find(&commonrepo.ProductFindOptions{
		Name:       productName,
		EnvName:    envName,
		Production: &production,
	})
	if err == mongo.ErrNoDocuments {
		return nil, 0, nil
	}
	if err != nil {
		log.Errorf("failed to query product info, productName %s envName %s err %s", productName, envName, err)
		return nil, 0, fmt.Errorf("failed to query product info, productName %s envName %s", productName, envName)
	}

	return productInfo.GlobalVariables, productInfo.UpdateTime, nil
}
