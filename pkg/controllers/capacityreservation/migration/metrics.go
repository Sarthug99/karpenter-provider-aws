/*
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

package migration

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	subsystem = "capacity_reservation_migration"

	resultSuccess      = "success"
	resultClientError  = "client_error"
	resultUnauthorized = "unauthorized"
	resultThrottled    = "throttled"
	resultError        = "error"

	caseOnDemand = "on_demand"
	caseOrphaned = "orphaned"

	reasonPreferenceOpen       = "preference_open"
	reasonPodIncompatible      = "pod_incompatible"
	reasonNodePoolIncompatible = "nodepool_incompatible"
	reasonNoCapacity           = "no_capacity"
	reasonNoCompatibleCR       = "no_compatible_cr"
	reasonZonalShift           = "zonal_shift"
)

var (
	resultLabel = opmetrics.Label{
		Name: "result",
		Help: "The outcome of the ModifyInstanceCapacityReservationAttributes call.",
		Values: []opmetrics.Value{
			{Name: resultSuccess, Help: "EC2 accepted the new target."},
			{Name: resultClientError, Help: "EC2 rejected the request, e.g. the transition or target is not allowed."},
			{Name: resultUnauthorized, Help: "The controller role lacks ec2:ModifyInstanceCapacityReservationAttributes."},
			{Name: resultThrottled, Help: "EC2 throttled the request."},
			{Name: resultError, Help: "Any other error."},
		},
	}
	caseLabel = opmetrics.Label{
		Name: "case",
		Help: "Why the node is being migrated.",
		Values: []opmetrics.Value{
			{Name: caseOnDemand, Help: "An on-demand node with no capacity reservation target."},
			{Name: caseOrphaned, Help: "A node whose targeted capacity reservation expired or was canceled."},
		},
	}
	reservationIDLabel = opmetrics.Label{
		Name: "capacity_reservation_id",
		Help: "The ID of the target capacity reservation.",
	}
	reasonLabel = opmetrics.Label{
		Name: metrics.ReasonLabel,
		Help: "Why a node is not eligible for migration.",
		Values: []opmetrics.Value{
			{Name: reasonPreferenceOpen, Help: "The instance has capacity reservation preference open, which EC2 can't re-target."},
			{Name: reasonPodIncompatible, Help: "A pod on the node requires labels the reserved node wouldn't have."},
			{Name: reasonNodePoolIncompatible, Help: "The NodePool requirements don't allow the reserved labels."},
			{Name: reasonNoCapacity, Help: "Every compatible capacity reservation is full."},
			{Name: reasonNoCompatibleCR, Help: "No capacity reservation selected by the EC2NodeClass matches the instance."},
			{Name: reasonZonalShift, Help: "The instance's availability zone is zonally shifted away from."},
		},
	}

	MigrationAttemptsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "attempts_total",
			Help:      "Number of ModifyInstanceCapacityReservationAttributes calls made to move a running node into a capacity reservation.",
		},
		[]opmetrics.Label{resultLabel, caseLabel},
		opmetrics.Alpha,
	)
	MigrationCompletedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "completed_total",
			Help:      "Number of nodes relabeled reserved after EC2 reported them covered by the target capacity reservation.",
		},
		[]opmetrics.Label{caseLabel},
		opmetrics.Alpha,
	)
	MigrationPending = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "pending",
			Help:      "Number of nodes re-targeted at a capacity reservation that EC2 doesn't report covered yet.",
		},
		[]opmetrics.Label{reservationIDLabel},
		opmetrics.Alpha,
	)
	MigrationStalled = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "stalled",
			Help:      "Number of pending nodes whose coverage has taken longer than 30 minutes.",
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	MigrationCoverageDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "coverage_duration_seconds",
			Help:      "Time from re-targeting a node at a capacity reservation to EC2 reporting it covered.",
			Buckets:   []float64{30, 60, 120, 300, 600, 900, 1200, 1500, 1800, 3600, 7200},
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	MigrationIneligible = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "ineligible",
			Help:      "Number of on-demand or orphaned nodes not migrated in the last pass, by reason.",
		},
		[]opmetrics.Label{reasonLabel},
		opmetrics.Alpha,
	)
)
