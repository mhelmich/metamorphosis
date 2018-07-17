/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"io"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

type timer interface {
	Time(func())
	Update(time.Duration)
	UpdateSince(time.Time)
}

func writeMetricsAsJSON(w io.Writer) {
	metrics.WriteJSONOnce(metrics.DefaultRegistry, w)
}

func getOrRegisterTimer(metricName string) timer {
	return metrics.GetOrRegisterTimer(metricName, metrics.DefaultRegistry)
}

func unregisterMetric(metricName string) {
	metrics.Unregister(metricName)
}
