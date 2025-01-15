/*
Copyright 2023 The K8sGPT Authors.
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

// Some parts of this file have been modified to make it functional in Zadig

package analysis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"github.com/koderover/zadig/v2/pkg/tool/cache"
	"github.com/koderover/zadig/v2/pkg/tool/llm"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

type Analysis struct {
	Context            context.Context
	Filters            []string
	Client             *Client
	AIClient           llm.ILLM
	Results            []Result
	Errors             []string
	Namespace          string
	Cache              cache.ICache
	Explain            bool
	MaxConcurrency     int
	AnalysisAIProvider string // The name of the AI Provider used for this analysis
	WithDoc            bool
}

type AnalysisStatus string
type AnalysisErrors []string

const (
	StateOK              AnalysisStatus = "OK"
	StateProblemDetected AnalysisStatus = "ProblemDetected"

	analysisPrompt = `Simplify the following Kubernetes error message delimited by triple dashes written in --- %s --- language; --- %s ---.
	Provide the most possible solution in a step by step style in no more than 280 characters. Write the output in the following format:
	错误: {Explain error here}
	解决方案: {Step by step solution here}
	`
)

type JsonOutput struct {
	Provider string         `json:"provider"`
	Errors   AnalysisErrors `json:"errors"`
	Status   AnalysisStatus `json:"status"`
	Problems int            `json:"problems"`
	Results  []Result       `json:"results"`
}

func NewAnalysis(ctx context.Context, clusterID string, llmClient llm.ILLM, filters []string, namespace string, noCache bool, explain bool, maxConcurrency int, withDoc bool) (*Analysis, error) {
	if llmClient == nil && explain {
		fmtErr := fmt.Errorf("Error: AI provider not specified in configuration")
		log.Error(fmtErr)
		return nil, fmtErr
	}

	client, err := NewClient(clusterID)
	if err != nil {
		log.Errorf("Error initialising kubernetes client: %v", err)
		return nil, err
	}

	return &Analysis{
		Context:            ctx,
		Filters:            filters,
		Client:             client,
		AIClient:           llmClient,
		Namespace:          namespace,
		Cache:              cache.New(noCache, cache.CacheTypeRedis),
		Explain:            explain,
		MaxConcurrency:     maxConcurrency,
		AnalysisAIProvider: llmClient.GetName(),
		WithDoc:            withDoc,
	}, nil
}

func (a *Analysis) RunAnalysis(activeFilters []string) {
	coreAnalyzerMap, analyzerMap := GetAnalyzerMap()

	// we get the openapi schema from the server only if required by the flag "with-doc"
	openapiSchema := &openapi_v2.Document{}
	if a.WithDoc {
		var openApiErr error
		openapiSchema, openApiErr = a.Client.Client.Discovery().OpenAPISchema()
		if openApiErr != nil {
			a.Errors = append(a.Errors, fmt.Sprintf("[KubernetesDoc] %s", openApiErr))
		}
	}

	analyzerConfig := Analyzer{
		Client:        a.Client,
		Context:       a.Context,
		Namespace:     a.Namespace,
		AIClient:      a.AIClient,
		OpenapiSchema: openapiSchema,
	}

	semaphore := make(chan struct{}, a.MaxConcurrency)
	// if there are no filters selected and no active_filters then run coreAnalyzer
	if len(a.Filters) == 0 && len(activeFilters) == 0 {
		var wg sync.WaitGroup
		var mutex sync.Mutex
		for _, analyzer := range coreAnalyzerMap {
			wg.Add(1)
			semaphore <- struct{}{}
			go func(analyzer IAnalyzer, wg *sync.WaitGroup, semaphore chan struct{}) {
				defer wg.Done()
				results, err := analyzer.Analyze(analyzerConfig)
				if err != nil {
					mutex.Lock()
					a.Errors = append(a.Errors, fmt.Sprintf("[%s] %s", reflect.TypeOf(analyzer).Name(), err))
					mutex.Unlock()
				}
				mutex.Lock()
				a.Results = append(a.Results, results...)
				mutex.Unlock()
				<-semaphore
			}(analyzer, &wg, semaphore)

		}
		wg.Wait()
		return
	}
	semaphore = make(chan struct{}, a.MaxConcurrency)
	// if the filters flag is specified
	if len(a.Filters) != 0 {
		var wg sync.WaitGroup
		var mutex sync.Mutex
		for _, filter := range a.Filters {
			if analyzer, ok := analyzerMap[filter]; ok {
				semaphore <- struct{}{}
				wg.Add(1)
				go func(analyzer IAnalyzer, filter string) {
					defer wg.Done()
					results, err := analyzer.Analyze(analyzerConfig)
					if err != nil {
						mutex.Lock()
						a.Errors = append(a.Errors, fmt.Sprintf("[%s] %s", filter, err))
						mutex.Unlock()
					}
					mutex.Lock()
					a.Results = append(a.Results, results...)
					mutex.Unlock()
					<-semaphore
				}(analyzer, filter)
			} else {
				a.Errors = append(a.Errors, fmt.Sprintf("\"%s\" filter does not exist. Please run k8sgpt filters list.", filter))
			}
		}
		wg.Wait()
		return
	}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	semaphore = make(chan struct{}, a.MaxConcurrency)
	// use active_filters
	for _, filter := range activeFilters {
		if analyzer, ok := analyzerMap[filter]; ok {
			semaphore <- struct{}{}
			wg.Add(1)
			go func(analyzer IAnalyzer, filter string) {
				defer wg.Done()
				results, err := analyzer.Analyze(analyzerConfig)
				if err != nil {
					mutex.Lock()
					a.Errors = append(a.Errors, fmt.Sprintf("[%s] %s", filter, err))
					mutex.Unlock()
				}
				mutex.Lock()
				a.Results = append(a.Results, results...)
				mutex.Unlock()
				<-semaphore
			}(analyzer, filter)
		}
	}
	wg.Wait()
}

func (a *Analysis) GetAIResults(anonymize bool) error {
	if len(a.Results) == 0 {
		return nil
	}

	for index, analysis := range a.Results {
		var texts []string

		for _, failure := range analysis.Error {
			if anonymize {
				for _, s := range failure.Sensitive {
					failure.Text = ReplaceIfMatch(failure.Text, s.Unmasked, s.Masked)
				}
			}
			texts = append(texts, failure.Text)
		}
		prompt := fmt.Sprintf(analysisPrompt, "Chinese", strings.Join(texts, " "))
		parsedText, err := a.AIClient.Parse(a.Context, prompt, a.Cache, llm.WithTemperature(0.3))
		if err != nil {
			// Check for exhaustion
			if strings.Contains(err.Error(), "status code: 429") {
				return fmt.Errorf("exhausted API quota for AI provider %s: %v", a.AIClient.GetName(), err)
			} else {
				return fmt.Errorf("failed while calling AI provider %s: %v", a.AIClient.GetName(), err)
			}
		}

		if anonymize {
			for _, failure := range analysis.Error {
				for _, s := range failure.Sensitive {
					parsedText = strings.ReplaceAll(parsedText, s.Masked, s.Unmasked)
				}
			}
		}

		analysis.Details = parsedText
		a.Results[index] = analysis
	}
	return nil
}
