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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

const (
	EventReasonStarted      = "CapacityReservationMigrationStarted"
	EventReasonCompleted    = "CapacityReservationMigrationCompleted"
	EventReasonFailed       = "CapacityReservationMigrationFailed"
	EventReasonStalled      = "CapacityReservationMigrationStalled"
	EventReasonNotRelabeled = "CapacityReservationMigrationNotRelabeled"
)

func startedEvent(nc *karpv1.NodeClaim, reservationID string) events.Event {
	return events.Event{
		InvolvedObject: nc,
		Type:           corev1.EventTypeNormal,
		Reason:         EventReasonStarted,
		Message:        fmt.Sprintf("Re-targeted instance at capacity reservation %s", reservationID),
		DedupeValues:   []string{string(nc.UID), reservationID},
	}
}

func completedEvent(nc *karpv1.NodeClaim, reservationID string) events.Event {
	return events.Event{
		InvolvedObject: nc,
		Type:           corev1.EventTypeNormal,
		Reason:         EventReasonCompleted,
		Message:        fmt.Sprintf("Instance is covered by capacity reservation %s, relabeled reserved", reservationID),
		DedupeValues:   []string{string(nc.UID), reservationID},
	}
}

func failedEvent(nc *karpv1.NodeClaim, reservationID string, err error) events.Event {
	return events.Event{
		InvolvedObject: nc,
		Type:           corev1.EventTypeWarning,
		Reason:         EventReasonFailed,
		Message:        fmt.Sprintf("Failed to re-target instance at capacity reservation %s: %s", reservationID, err),
		DedupeValues:   []string{string(nc.UID), reservationID},
	}
}

func stalledEvent(nc *karpv1.NodeClaim, reservationID string, pendingFor time.Duration) events.Event {
	return events.Event{
		InvolvedObject: nc,
		Type:           corev1.EventTypeWarning,
		Reason:         EventReasonStalled,
		Message:        fmt.Sprintf("Instance not covered by capacity reservation %s after %s, waiting for a free slot", reservationID, pendingFor.Round(time.Minute)),
		DedupeValues:   []string{string(nc.UID), reservationID},
		DedupeTimeout:  time.Hour,
	}
}

func notRelabeledEvent(nc *karpv1.NodeClaim, reservationID, reason string) events.Event {
	return events.Event{
		InvolvedObject: nc,
		Type:           corev1.EventTypeWarning,
		Reason:         EventReasonNotRelabeled,
		Message:        fmt.Sprintf("Instance is covered by capacity reservation %s but left labeled on-demand: %s", reservationID, reason),
		DedupeValues:   []string{string(nc.UID), reservationID},
	}
}
