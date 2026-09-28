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

package cloudprovider

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
)

// TestRepairPolicyDefaults pins the AWS provider's default repair-policy set. RepairPolicies returns a static literal,
// so a zero-value receiver is sufficient — no cluster or provider wiring needed.
func TestRepairPolicyDefaults(t *testing.T) {
	policies := (&CloudProvider{}).RepairPolicies()
	for _, p := range policies {
		// Every repair policy MUST bound its drain so repair is never the unbounded NodeClaim TGP hang.
		if p.TerminationGracePeriod == nil {
			t.Errorf("policy %s/%s %q has a nil TerminationGracePeriod; repair drain must be bounded", p.ConditionType, p.ConditionStatus, p.ReasonRegex)
		}
	}
	// Ready=Unknown is a lost kubelet heartbeat: the drain can never make progress, so it must be forceful (0).
	assertTGP(t, policies, corev1.NodeReady, corev1.ConditionUnknown, 0)
	// A live-but-NotReady kubelet and the NMA conditions all get a bounded graceful drain.
	assertTGP(t, policies, corev1.NodeReady, corev1.ConditionFalse, 10*time.Minute)
	for _, cond := range []corev1.NodeConditionType{"AcceleratedHardwareReady", "StorageReady", "NetworkingReady", "KernelReady", "ContainerRuntimeReady"} {
		assertTGP(t, policies, cond, corev1.ConditionFalse, 10*time.Minute)
	}
}

// TestRepairPolicyMatching evaluates the policy set with core's matcher, so it checks the real contract (one global
// fallback, a required Action, most-disruptive-eligible-action wins) rather than a re-implementation of it.
func TestRepairPolicyMatching(t *testing.T) {
	matcher, err := health.NewRepairPolicyMatcher((&CloudProvider{}).RepairPolicies(), sets.New(cloudprovider.ReplaceNode, cloudprovider.RebootNode))
	if err != nil {
		t.Fatalf("core rejects the AWS repair policy set: %v", err)
	}
	now := time.Now()
	for _, tc := range []struct {
		name   string
		cond   corev1.NodeConditionType
		status corev1.ConditionStatus
		reason string
		age    time.Duration
		want   cloudprovider.RepairAction // "" = not yet eligible
	}{
		{"reboot-clearable XID reboots after 10m", "AcceleratedHardwareReady", corev1.ConditionFalse, "NvidiaXID46Error", 11 * time.Minute, cloudprovider.RebootNode},
		{"reboot-clearable XID embedded in the reason", "AcceleratedHardwareReady", corev1.ConditionFalse, "GPU-XID140-fault", 11 * time.Minute, cloudprovider.RebootNode},
		{"reboot-clearable XID isn't eligible before 10m", "AcceleratedHardwareReady", corev1.ConditionFalse, "NvidiaXID46Error", 9 * time.Minute, ""},
		{"a still-present reboot-clearable XID is replaced once the 30m policy matures", "AcceleratedHardwareReady", corev1.ConditionFalse, "NvidiaXID46Error", 31 * time.Minute, cloudprovider.ReplaceNode},
		{"fatal XID replaces after 10m", "AcceleratedHardwareReady", corev1.ConditionFalse, "NvidiaXID79Error", 11 * time.Minute, cloudprovider.ReplaceNode},
		{"a longer XID code isn't mistaken for a shorter one", "AcceleratedHardwareReady", corev1.ConditionFalse, "NvidiaXID460Error", 11 * time.Minute, ""},
		{"unknown GPU reason waits 30m", "AcceleratedHardwareReady", corev1.ConditionFalse, "SomeUnknownReason", 11 * time.Minute, ""},
		{"unknown GPU reason replaces after 30m", "AcceleratedHardwareReady", corev1.ConditionFalse, "SomeUnknownReason", 31 * time.Minute, cloudprovider.ReplaceNode},
		{"advisory DCGM code isn't a fast XID", "AcceleratedHardwareReady", corev1.ConditionFalse, "DCGM_FI_DEV_XID_ERRORS", 11 * time.Minute, ""},
		{"Ready=False replaces after 30m", corev1.NodeReady, corev1.ConditionFalse, "KubeletNotReady", 31 * time.Minute, cloudprovider.ReplaceNode},
		{"Ready=Unknown replaces after 30m", corev1.NodeReady, corev1.ConditionUnknown, "NodeStatusUnknown", 31 * time.Minute, cloudprovider.ReplaceNode},
		{"StorageReady=False replaces after 30m", "StorageReady", corev1.ConditionFalse, "IOErrors", 31 * time.Minute, cloudprovider.ReplaceNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: tc.cond, Status: tc.status, Reason: tc.reason, LastTransitionTime: metav1.Time{Time: now.Add(-tc.age)},
			}}}}
			decision := matcher.Evaluate(node, now).Decision
			got := cloudprovider.RepairAction("")
			if decision != nil && decision.EligiblePolicies > 0 {
				got = decision.Action
			}
			if got != tc.want {
				t.Errorf("got action %q, want %q (decision %+v)", got, tc.want, decision)
			}
		})
	}
}

func assertTGP(t *testing.T, policies []cloudprovider.RepairPolicy, cond corev1.NodeConditionType, status corev1.ConditionStatus, want time.Duration) {
	t.Helper()
	found := false
	for _, p := range policies {
		if p.ConditionType != cond || p.ConditionStatus != status {
			continue
		}
		found = true
		if p.TerminationGracePeriod == nil || *p.TerminationGracePeriod != want {
			t.Errorf("policy %s/%s %q: expected TerminationGracePeriod %s, got %v", cond, status, p.ReasonRegex, want, p.TerminationGracePeriod)
		}
	}
	if !found {
		t.Errorf("no repair policy found for %s/%s", cond, status)
	}
}
