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

package policy

import (
	"net/http"
	"net/url"

	"github.com/koderover/zadig/pkg/microservice/picket/config"
	"github.com/koderover/zadig/pkg/tool/httpclient"
)

type RoleBinding struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	Role   string `json:"role"`
	Public bool   `json:"public"`
}

type PolicyBinding struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	Policy string `json:"policy"`
	Public bool   `json:"public"`
}

type Binding struct {
	Name         string                         `json:"name"`
	UID          string                         `json:"uid"`
	RoleOrPolicy string                         `json:"role_or_policy"`
	BindingType  config.RoleORPolicyBindingType `json:"binding_type"`
	Public       bool                           `json:"public"`
}

func (c *Client) ListRoleBindings(header http.Header, qs url.Values) ([]*Binding, error) {
	url := "/rolebindings"

	res := make([]*RoleBinding, 0)
	_, err := c.Get(url, httpclient.SetHeadersFromHTTPHeader(header), httpclient.SetQueryParamsFromValues(qs), httpclient.SetResult(&res))
	if err != nil {
		return nil, err
	}
	resBindings := make([]*Binding, 0)
	for _, v := range res {
		tmpB := Binding{
			Name:         v.Name,
			UID:          v.UID,
			RoleOrPolicy: v.Role,
			Public:       v.Public,
			BindingType:  config.Role,
		}
		resBindings = append(resBindings, &tmpB)
	}

	return resBindings, nil
}

func (c *Client) ListPolicyBindings(header http.Header, qs url.Values) ([]*Binding, error) {
	url := "/policybindings"

	res := make([]*PolicyBinding, 0)
	_, err := c.Get(url, httpclient.SetHeadersFromHTTPHeader(header), httpclient.SetQueryParamsFromValues(qs), httpclient.SetResult(&res))
	if err != nil {
		return nil, err
	}

	resBindings := make([]*Binding, 0)
	for _, v := range res {
		tmpB := Binding{
			Name:         v.Name,
			UID:          v.UID,
			RoleOrPolicy: v.Policy,
			Public:       v.Public,
			BindingType:  config.Policy,
		}
		resBindings = append(resBindings, &tmpB)
	}
	return resBindings, nil
}

func (c *Client) DeleteRoleBindings(userID string, header http.Header, qs url.Values) ([]byte, error) {
	url := "rolebindings/bulk-delete"

	qs.Add("userID", userID)
	res, err := c.Post(url, httpclient.SetHeadersFromHTTPHeader(header), httpclient.SetQueryParamsFromValues(qs), httpclient.SetBody([]byte("{}")))
	if err != nil {
		return []byte{}, err
	}
	return res.Body(), err
}
