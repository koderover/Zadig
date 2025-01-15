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

package util

import (
	"strings"

	"github.com/koderover/zadig/v2/pkg/setting"

	"helm.sh/helm/v3/pkg/releaseutil"
)

const separator = "\n---\n"

func CombineManifests(yamls []string) string {
	var builder strings.Builder
	for _, y := range yamls {
		builder.WriteString(separator + y)
	}

	return builder.String()
}

func SplitManifests(content string) []string {
	var res []string
	manifests := releaseutil.SplitManifests(content)
	for _, m := range manifests {
		res = append(res, m)
	}

	return res
}

// value may only contain underscores or underscores
func ReturnValidLabelValue(value string) string {
	value = strings.Replace(value, "-", "", -1)
	value = strings.Replace(value, "_", "", -1)
	if len(value) > 63 {
		value = value[:63]
	}

	return value
}

func JoinYamls(files []string) string {
	return strings.Join(files, setting.YamlFileSeperator)
}

func SplitYaml(yaml string) []string {
	return strings.Split(yaml, setting.YamlFileSeperator)
}

func ConvertIntsToInt64(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			v[key] = ConvertIntsToInt64(value)
		}
		return v
	case []interface{}:
		for i, value := range v {
			v[i] = ConvertIntsToInt64(value)
		}
		return v
	case int:
		return int64(v)
	default:
		return v
	}
}
