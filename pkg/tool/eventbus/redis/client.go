/*
Copyright 2025 The KodeRover Authors.

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

package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/koderover/zadig/v2/pkg/config"
	"github.com/koderover/zadig/v2/pkg/tool/log"
	"github.com/koderover/zadig/v2/pkg/util"
	"github.com/redis/go-redis/v9"
)

type RedisEventBus struct {
	redisClient *redis.Client
	mu          sync.RWMutex
	handleFuncs map[string][]func(message string)
}

var redisClient *redis.Client
var handleFuncMap map[string][]func(message string)

func New(db int) *RedisEventBus {
	if redisClient == nil {
		redisConfig := &redis.Options{
			Addr: fmt.Sprintf("%s:%d", config.RedisHost(), config.RedisPort()),
			DB:   db,
		}

		if config.RedisUserName() != "" {
			redisConfig.Username = config.RedisUserName()
		}
		if config.RedisPassword() != "" {
			redisConfig.Password = config.RedisPassword()
		}
		redisClient = redis.NewClient(redisConfig)
	}

	if handleFuncMap == nil {
		handleFuncMap = make(map[string][]func(message string))
	}

	return &RedisEventBus{redisClient: redisClient, handleFuncs: handleFuncMap}
}

func (eb *RedisEventBus) RegisterHandleFunc(channel string, handleFunc func(message string)) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if _, exists := eb.handleFuncs[channel]; !exists {
		eb.handleFuncs[channel] = make([]func(string), 0) // Initialize if not already present
	}
	eb.handleFuncs[channel] = append(eb.handleFuncs[channel], handleFunc)
}

func (eb *RedisEventBus) Subscribe(ctx context.Context, channel string) {
	pubsub := eb.redisClient.Subscribe(ctx, channel)

	log.Infof("Subscribing to channel %s", channel)

	util.Go(func() {
		defer pubsub.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pubsub.Channel():
				log.Debugf("Redis Eventbus: [%s] MESSAGE IN: %s", channel, msg.Payload)

				eb.mu.RLock()
				handlers := eb.handleFuncs[channel]
				eb.mu.RUnlock()

				for _, f := range handlers {
					go f(msg.Payload)
				}
			}
		}
	})
}

func (eb *RedisEventBus) Publish(channel, message string) error {
	return eb.redisClient.Publish(context.Background(), channel, message).Err()
}
