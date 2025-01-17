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

package command

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/jobexecutor/core/service/step"
	"github.com/koderover/zadig/v2/pkg/setting"
	"github.com/koderover/zadig/v2/pkg/shared/client/systemconfig"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/types"
	"github.com/koderover/zadig/v2/pkg/util"
)

type Repo struct {
	Source             string         `yaml:"source"`
	Address            string         `yaml:"address"`
	Owner              string         `yaml:"owner"`
	Namespace          string         `yaml:"namespace"`
	Name               string         `yaml:"name"`
	RemoteName         string         `yaml:"remote_name"`
	Branch             string         `yaml:"branch"`
	PR                 int            `yaml:"pr"`
	Tag                string         `yaml:"tag"`
	CheckoutPath       string         `yaml:"checkout_path"`
	SubModules         bool           `yaml:"submodules"`
	OauthToken         string         `yaml:"oauthToken"`
	User               string         `yaml:"-"`
	Password           string         `yaml:"-"`
	CheckoutRef        string         `yaml:"checkout_ref"`
	AuthType           types.AuthType `yaml:"auth_type,omitempty"`
	SSHKey             string         `yaml:"ssh_key,omitempty"`
	PrivateAccessToken string         `yaml:"private_access_token,omitempty"`
}

// BranchRef returns branch refs format
// e.g. refs/heads/master
func (r *Repo) BranchRef() string {
	return fmt.Sprintf("refs/heads/%s", r.Branch)
}

type Command struct {
	Cmd *exec.Cmd
	// DisableTrace display command args
	DisableTrace bool
	// IgnoreError ingore command run error
	IgnoreError bool
}

func RunGitCmds(codehostDetail *systemconfig.CodeHost, repoOwner, repoNamespace, repoName, branchName, remoteName string) error {
	var (
		tokens []string
		repo   *Repo
		envs   = make([]string, 0)
		cmds   = make([]*Command, 0)
	)
	repo = &Repo{
		Source:             codehostDetail.Type,
		Address:            codehostDetail.Address,
		Name:               repoName,
		Namespace:          repoNamespace,
		Branch:             branchName,
		OauthToken:         codehostDetail.AccessToken,
		RemoteName:         remoteName,
		Owner:              repoOwner,
		AuthType:           codehostDetail.AuthType,
		SSHKey:             codehostDetail.SSHKey,
		PrivateAccessToken: codehostDetail.PrivateAccessToken,
	}

	userpass, _ := base64.StdEncoding.DecodeString(repo.OauthToken)
	userpassPair := strings.Split(string(userpass), ":")
	var user, password string
	var hostNames = sets.NewString()
	if len(userpassPair) > 1 {
		password = userpassPair[1]
	}
	user = userpassPair[0]
	repo.User = user
	if password != "" {
		repo.Password = password
		tokens = append(tokens, repo.Password)
	}
	tokens = append(tokens, repo.OauthToken)
	cmds = append(cmds, buildGitCommands(repo, hostNames)...)

	// write ssh key
	if len(hostNames.List()) > 0 {
		if err := writeSSHConfigFile(hostNames); err != nil {
			return err
		}
	}

	if codehostDetail.EnableProxy {
		httpsProxy := config.ProxyHTTPSAddr()
		httpProxy := config.ProxyHTTPAddr()
		if httpsProxy != "" {
			envs = append(envs, fmt.Sprintf("https_proxy=%s", httpsProxy))
		}
		if httpProxy != "" {
			envs = append(envs, fmt.Sprintf("http_proxy=%s", httpProxy))
		}
	}

	for _, c := range cmds {
		cmdOutReader, err := c.Cmd.StdoutPipe()
		if err != nil {
			return err
		}

		outScanner := bufio.NewScanner(cmdOutReader)
		go func() {
			for outScanner.Scan() {
				fmt.Printf("%s\n", util.MaskSecret(tokens, outScanner.Text()))
			}
		}()

		cmdErrReader, err := c.Cmd.StderrPipe()
		if err != nil {
			return err
		}

		errScanner := bufio.NewScanner(cmdErrReader)
		go func() {
			for errScanner.Scan() {
				fmt.Printf("%s\n", util.MaskSecret(tokens, errScanner.Text()))
			}
		}()

		c.Cmd.Env = envs
		if !c.DisableTrace {
			log.Info(strings.Join(c.Cmd.Args, " "))
		}
		if err := c.Cmd.Run(); err != nil {
			if c.IgnoreError {
				continue
			}
			return err
		}
	}
	return nil
}

func buildGitCommands(repo *Repo, hostNames sets.String) []*Command {
	cmds := make([]*Command, 0)

	if len(repo.Name) == 0 {
		return cmds
	}

	repoName := repo.Name
	if strings.Contains(repoName, "/") {
		repoName = strings.Replace(repoName, "/", "-", -1)
	}
	workDir := filepath.Join(config.S3StoragePath(), repoName)
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		os.MkdirAll(workDir, 0777)
	}
	defer func() {
		defer setCmdsWorkDir(workDir, cmds)
	}()

	if strings.Contains(repoName, "-new") {
		repo.Name = strings.TrimSuffix(repo.Name, "-new")
	}

	// 预防非正常退出导致git被锁住
	indexLockPath := path.Join(workDir, "/.git/index.lock")
	if err := os.RemoveAll(indexLockPath); err != nil {
		log.Errorf("Failed to remove %s: %s", indexLockPath, err)
	}
	shallowLockPath := path.Join(workDir, "/.git/shallow.lock")
	if err := os.RemoveAll(shallowLockPath); err != nil {
		log.Errorf("Failed to remove %s: %s", shallowLockPath, err)
	}

	if isDirEmpty(filepath.Join(workDir, ".git")) {
		cmds = append(cmds, &Command{Cmd: InitGit(workDir)})
	} else {
		cmds = append(cmds, &Command{Cmd: RemoteRemove(repo.RemoteName), DisableTrace: true, IgnoreError: true})
	}

	// namespace represents the real owner
	owner := repo.Namespace
	if len(owner) == 0 {
		owner = repo.Owner
	}

	if repo.Source == setting.SourceFromGitlab {
		u, _ := url.Parse(repo.Address)
		url := OAuthCloneURL(repo.Source, repo.OauthToken, u.Host, repo.Owner, repo.Name, u.Scheme)
		cmds = append(cmds, &Command{
			Cmd:          RemoteAdd(repo.RemoteName, url),
			DisableTrace: true,
		})
	} else if repo.Source == setting.SourceFromGerrit {
		u, _ := url.Parse(repo.Address)
		u.Path = fmt.Sprintf("/a/%s", repo.Name)
		u.User = url.UserPassword(repo.User, repo.Password)

		cmds = append(cmds, &Command{
			Cmd:          RemoteAdd(repo.RemoteName, u.String()),
			DisableTrace: true,
		})
	} else if repo.Source == setting.SourceFromGiteeEE || repo.Source == setting.SourceFromGitee {
		giteeURL := step.HTTPSCloneURL(repo.Source, repo.OauthToken, repo.Owner, repo.Name, repo.Address)
		cmds = append(cmds, &Command{Cmd: RemoteAdd(repo.RemoteName, giteeURL), DisableTrace: true})
	} else if repo.Source == setting.SourceFromOther {
		if repo.AuthType == types.SSHAuthType {
			host := getHost(repo.Address)
			if !hostNames.Has(host) {
				if err := writeSSHFile(repo.SSHKey, host); err != nil {
					log.Errorf("failed to write ssh file, err: %v", err)
				}
				hostNames.Insert(host)
			}
			remoteName := fmt.Sprintf("%s:%s/%s.git", repo.Address, repo.Owner, repo.Name)
			// Including the case of the port
			if strings.Contains(repo.Address, ":") {
				remoteName = fmt.Sprintf("%s/%s/%s.git", repo.Address, repo.Owner, repo.Name)
			}
			cmds = append(cmds, &Command{
				Cmd:          RemoteAdd(repo.RemoteName, remoteName),
				DisableTrace: true,
			})
		} else if repo.AuthType == types.PrivateAccessTokenAuthType {
			u, err := url.Parse(repo.Address)
			if err != nil {
				log.Errorf("failed to parse url,err:%s", err)
			} else {
				host := strings.TrimSuffix(strings.Join([]string{u.Host, u.Path}, "/"), "/")
				cmds = append(cmds, &Command{
					Cmd:          RemoteAdd(repo.RemoteName, OAuthCloneURL(repo.Source, repo.PrivateAccessToken, host, repo.Owner, repo.Name, u.Scheme)),
					DisableTrace: true,
				})
			}
		}
	} else {
		// github
		cmd := RemoteAdd(repo.RemoteName, fmt.Sprintf("https://x-access-token:%s@%s/%s/%s.git", repo.OauthToken, "github.com", repo.Owner, repo.Name))
		if repo.OauthToken == "" {
			cmd = RemoteAdd(repo.RemoteName, repo.Address)
		}
		cmds = append(cmds, &Command{
			Cmd:          cmd,
			DisableTrace: true,
		})
	}

	cmds = append(cmds, &Command{Cmd: Fetch(repo.RemoteName, repo.BranchRef())})
	cmds = append(cmds, &Command{Cmd: CheckoutHead()})
	cmds = append(cmds, &Command{Cmd: ShowLastLog()})

	return cmds
}

// InitGit creates an empty git repository.
// it returns command git init
func InitGit(dir string) *exec.Cmd {
	cmd := exec.Command(
		"git",
		"init",
	)
	cmd.Dir = dir
	return cmd
}

// RemoteRemove removes the remote origin for the repository.
func RemoteRemove(remoteName string) *exec.Cmd {
	return exec.Command(
		"git",
		"remote",
		"remove",
		remoteName,
	)
}

// RemoteAdd sets the remote origin for the repository.
func RemoteAdd(remoteName, remote string) *exec.Cmd {
	return exec.Command(
		"git",
		"remote",
		"add",
		remoteName,
		remote,
	)
}

// Fetch fetches changes by ref, ref can be a tag, branch or pr. --depth=1 is used to limit fetching
// to the last commit from the tip of each remote branch history.
// e.g. git fetch origin +refs/heads/onboarding --depth=1
func Fetch(remoteName, ref string) *exec.Cmd {
	return exec.Command(
		"git",
		"fetch",
		remoteName,
		"+"+ref, // "+" means overwrite
		"--depth=1",
	)
}

// CheckoutHead returns command git checkout -qf FETCH_HEAD
func CheckoutHead() *exec.Cmd {
	return exec.Command(
		"git",
		"checkout",
		"-qf",
		"FETCH_HEAD",
	)
}

// ShowLastLog returns command git --no-pager log --oneline -1
// It shows last commit messge with sha
func ShowLastLog() *exec.Cmd {
	return exec.Command(
		"git",
		"--no-pager",
		"log",
		"--oneline",
		"-1",
	)
}

func OAuthCloneURL(source, token, address, owner, name, scheme string) string {
	if strings.ToLower(source) == setting.SourceFromGitlab || strings.ToLower(source) == setting.SourceFromOther {
		// address 需要传过来
		return fmt.Sprintf("%s://%s:%s@%s/%s/%s.git", scheme, "oauth2", token, address, owner, name)
	}
	//	GITHUB
	return "github"
}

func isDirEmpty(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		return true
	}
	defer f.Close()

	_, err = f.Readdir(1)
	return err == io.EOF
}

func setCmdsWorkDir(dir string, cmds []*Command) {
	for _, c := range cmds {
		c.Cmd.Dir = dir
	}
}

// git@github.com or git@github.com:2000
// return github.com
func getHost(address string) string {
	address = strings.TrimPrefix(address, "ssh://")
	address = strings.TrimPrefix(address, "git@")
	hostArr := strings.Split(address, ":")
	return hostArr[0]
}

func writeSSHFile(sshKey, hostName string) error {
	if sshKey == "" {
		return fmt.Errorf("ssh cannot be empty")
	}

	if hostName == "" {
		return fmt.Errorf("hostName cannot be empty")
	}

	dir := path.Join(config.Home(), "/.ssh")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	hostName = strings.Replace(hostName, ".", "", -1)
	hostName = strings.Replace(hostName, ":", "", -1)
	pathName := fmt.Sprintf("/.ssh/id_rsa.%s", hostName)
	file := path.Join(config.Home(), pathName)
	return ioutil.WriteFile(file, []byte(sshKey), 0400)
}

func writeSSHConfigFile(hostNames sets.String) error {
	out := "\nHOST *\nStrictHostKeyChecking=no\nUserKnownHostsFile=/dev/null\n"
	for _, hostName := range hostNames.List() {
		name := strings.Replace(hostName, ".", "", -1)
		name = strings.Replace(name, ":", "", -1)
		out += fmt.Sprintf("\nHost %s\nIdentityFile ~/.ssh/id_rsa.%s\n", hostName, name)
	}
	file := path.Join(config.Home(), "/.ssh/config")
	return ioutil.WriteFile(file, []byte(out), 0600)
}
