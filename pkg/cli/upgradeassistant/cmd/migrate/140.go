package migrate

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/go-multierror"

	"github.com/koderover/zadig/pkg/cli/upgradeassistant/internal/upgradepath"
	configbase "github.com/koderover/zadig/pkg/config"
	"github.com/koderover/zadig/pkg/microservice/aslan/core/common/repository/mongodb"
	s3service "github.com/koderover/zadig/pkg/microservice/aslan/core/common/service/s3"
	"github.com/koderover/zadig/pkg/setting"
	"github.com/koderover/zadig/pkg/tool/log"
	s3tool "github.com/koderover/zadig/pkg/tool/s3"
)

func init() {
	upgradepath.AddHandler(upgradepath.V131, upgradepath.V140, V131ToV140)
	upgradepath.AddHandler(upgradepath.V140, upgradepath.V131, V140ToV131)
}

func V131ToV140() error {
	log.Info("Migrating data from 1.3.1 to 1.4.0")

	services, err := mongodb.NewServiceColl().ListMaxRevisions(&mongodb.ServiceListOption{Type: setting.HelmDeployType})
	if err != nil {
		log.Errorf("Failed to get services, err: %s", err)
		return err
	}

	s3Storage, s3Client, err := getS3Client()
	if err != nil {
		log.Errorf("Failed to get s3 client, err: %s", err)
		return err
	}

	var errs *multierror.Error
	for _, s := range services {
		if err := copyFile(s.ProductName, s.ServiceName, s3Storage.Bucket, s3Storage.Subfolder, s3Client); err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	if errs.ErrorOrNil() != nil {
		log.Warn(errs.Error())
	}

	return nil
}

func V140ToV131() error {
	log.Info("Rollback data from 1.4.0 to 1.3.1")
	return nil
}

func getS3Client() (*s3service.S3, *s3tool.Client, error) {
	s3Storage, err := s3service.FindDefaultS3()
	if err != nil {
		log.Errorf("Failed to find default s3, err: %s", err)
		return nil, nil, err
	}

	forcedPathStyle := true
	if s3Storage.Provider == setting.ProviderSourceAli {
		forcedPathStyle = false
	}
	c, err := s3tool.NewClient(s3Storage.Endpoint, s3Storage.Ak, s3Storage.Sk, s3Storage.Insecure, forcedPathStyle)

	return s3Storage, c, err
}

func copyFile(projectName, serviceName, bucket, subFolder string, client *s3tool.Client) error {
	tarball := fmt.Sprintf("%s.tar.gz", serviceName)
	newKey := filepath.Join(subFolder, configbase.ObjectStorageServicePath(projectName, serviceName), tarball)
	oldKey := filepath.Join(subFolder, serviceName+"-"+setting.HelmDeployType, "service", tarball)

	if err := client.CopyObject(bucket, oldKey, newKey); err != nil {
		log.Errorf("Failed to copy object from %s to %s, err: %s", oldKey, newKey, err)
		return err
	}

	return nil
}
