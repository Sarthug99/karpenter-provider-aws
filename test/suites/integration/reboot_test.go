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

package integration_test

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	"github.com/aws/karpenter-provider-aws/test/pkg/environment/common"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Reboot specs drive the reboot node action on real EC2. Repair-driven specs inject the Node Monitoring Agent's
// AcceleratedHardwareReady condition directly on the Node (as repair_policy_test.go does), so they run on ordinary
// instances without a GPU or the agent: nothing else owns that condition there, so the kubelet leaves it in place, and
// it persists across the reboot. A reboot-clearable XID (46) reboots after 10m; a fatal XID (79) replaces.
var _ = Describe("Reboot", func() {
	var dep *appsv1.Deployment
	var selector labels.Selector

	BeforeEach(func() {
		dep = coretest.Deployment(coretest.DeploymentOptions{
			Replicas: 1,
			PodOptions: coretest.PodOptions{
				ObjectMeta:                    metav1.ObjectMeta{Labels: map[string]string{"app": "reboot"}},
				TerminationGracePeriodSeconds: lo.ToPtr[int64](0),
			},
		})
		selector = labels.SelectorFromSet(dep.Spec.Selector.MatchLabels)
	})

	// setGPUCondition sets AcceleratedHardwareReady on the node, as the Node Monitoring Agent would. A fault is
	// backdated past the 10m XID toleration (but well short of the 30m any-reason replace policy) so repair acts on
	// it immediately.
	setGPUCondition := func(node *corev1.Node, status corev1.ConditionStatus, reason string) {
		GinkgoHelper()
		node = env.ExpectExists(node).(*corev1.Node)
		env.ExpectStatusUpdated(common.ReplaceNodeConditions(node, corev1.NodeCondition{
			Type:               "AcceleratedHardwareReady",
			Status:             status,
			Reason:             reason,
			Message:            "injected by e2e",
			LastTransitionTime: metav1.Time{Time: lo.Ternary(status == corev1.ConditionFalse, time.Now().Add(-11*time.Minute), time.Now())},
		}))
	}
	getNodeClaim := func(g Gomega, nodeClaim *karpv1.NodeClaim) *karpv1.NodeClaim {
		nc := &karpv1.NodeClaim{}
		g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(nodeClaim), nc)).To(Succeed())
		return nc
	}
	getNode := func(g Gomega, node *corev1.Node) *corev1.Node {
		n := &corev1.Node{}
		g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(node), n)).To(Succeed())
		return n
	}
	// eventuallyExpectRebootSucceeded waits for a reboot to succeed with a boot other than notBootID, and returns
	// the new boot ID.
	eventuallyExpectRebootSucceeded := func(nodeClaim *karpv1.NodeClaim, node *corev1.Node, notBootID string) string {
		GinkgoHelper()
		var bootID string
		Eventually(func(g Gomega) {
			cond := getNodeClaim(g, nodeClaim).StatusConditions().Get(karpv1.ConditionTypeRebooting)
			g.Expect(cond).ToNot(BeNil())
			g.Expect(cond.IsFalse()).To(BeTrue())
			g.Expect(cond.Reason).To(Equal(karpv1.RebootReasonSucceeded))
			n := getNode(g, node)
			g.Expect(n.Status.NodeInfo.BootID).ToNot(Equal(notBootID))
			bootID = n.Status.NodeInfo.BootID
		}).WithTimeout(20 * time.Minute).Should(Succeed())
		return bootID
	}
	// expectInPlace checks that the reboot kept the node: the same Node, re-initialized with the fence removed, the
	// same NodeClaim, and the same EC2 instance, still running with its original launch time.
	expectInPlace := func(nodeClaim *karpv1.NodeClaim, node *corev1.Node, instance ec2types.Instance) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			n := getNode(g, node)
			g.Expect(n.Spec.Taints).ToNot(ContainElement(HaveField("Key", karpv1.RebootingTaintKey)))
			g.Expect(n.Labels).To(HaveKeyWithValue(karpv1.NodeInitializedLabelKey, "true"))
		}).Should(Succeed())
		nodeClaims := env.EventuallyExpectCreatedNodeClaimCount("==", 1)
		Expect(nodeClaims[0].Name).To(Equal(nodeClaim.Name))
		rebooted := env.GetInstanceByID(aws.ToString(instance.InstanceId))
		Expect(rebooted.State.Name).To(Equal(ec2types.InstanceStateNameRunning))
		Expect(aws.ToTime(rebooted.LaunchTime)).To(Equal(aws.ToTime(instance.LaunchTime)))
		env.EventuallyExpectHealthyPodCount(selector, 1)
	}
	// provision creates one node running the workload and returns its Node, NodeClaim, and EC2 instance.
	provision := func() (*corev1.Node, *karpv1.NodeClaim, ec2types.Instance) {
		GinkgoHelper()
		env.ExpectCreated(nodeClass, nodePool, dep)
		env.EventuallyExpectHealthyPodCount(selector, 1)
		node := env.EventuallyExpectInitializedNodeCount("==", 1)[0]
		nodeClaim := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
		return node, nodeClaim, env.GetInstance(node.Name)
	}

	It("should reboot the EC2 instance in place when a reboot is handed off through the Rebooting condition", func() {
		node, nodeClaim, instance := provision()

		// Commit a reboot the way any consumer does: the drain bound, then Rebooting=RebootRequested.
		nodeClaim = env.ExpectExists(nodeClaim).(*karpv1.NodeClaim)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{karpv1.RebootTerminationGracePeriodAnnotationKey: "5m"})
		env.ExpectUpdated(nodeClaim)
		nodeClaim = env.ExpectExists(nodeClaim).(*karpv1.NodeClaim)
		nodeClaim.StatusConditions().SetTrueWithReason(karpv1.ConditionTypeRebooting, karpv1.RebootReasonRequested, "rebooting for e2e")
		env.ExpectStatusUpdated(nodeClaim)

		eventuallyExpectRebootSucceeded(nodeClaim, node, node.Status.NodeInfo.BootID)
		expectInPlace(nodeClaim, node, instance)
	})

	It("should reboot in place when repair matches a reboot-clearable GPU fault", func() {
		node, nodeClaim, instance := provision()

		setGPUCondition(node, corev1.ConditionFalse, "NvidiaXID46Error")
		// Repair commits the reboot, recording the driving fault and attributing the disruption to repair.
		Eventually(func(g Gomega) {
			nc := getNodeClaim(g, nodeClaim)
			cond := nc.StatusConditions().Get(karpv1.ConditionTypeRebooting)
			g.Expect(cond).ToNot(BeNil())
			g.Expect(cond.IsTrue()).To(BeTrue())
			g.Expect(cond.Message).To(ContainSubstring("AcceleratedHardwareReady/NvidiaXID46Error"))
			g.Expect(nc.StatusConditions().Get(karpv1.ConditionTypeDisruptionReason).IsTrue()).To(BeTrue())
		}).Should(Succeed())
		// The reboot clears the fault; the agent would report the GPU healthy once the node is back.
		setGPUCondition(node, corev1.ConditionTrue, "NvidiaGPUIsReady")
		// While the node reboots, its pod waits for it rather than triggering replacement capacity.
		Consistently(func(g Gomega) {
			nodeClaims := &karpv1.NodeClaimList{}
			g.Expect(env.Client.List(env, nodeClaims)).To(Succeed())
			g.Expect(nodeClaims.Items).To(HaveLen(1))
		}, time.Minute).Should(Succeed())

		eventuallyExpectRebootSucceeded(nodeClaim, node, node.Status.NodeInfo.BootID)
		expectInPlace(nodeClaim, node, instance)
		// The cleared fault doesn't trigger another action.
		Consistently(func(g Gomega) {
			g.Expect(getNodeClaim(g, nodeClaim).StatusConditions().Get(karpv1.ConditionTypeRebooting).Reason).To(Equal(karpv1.RebootReasonSucceeded))
		}, 2*time.Minute).Should(Succeed())
	})

	It("should replace, not reboot, on a fatal GPU fault", func() {
		node, nodeClaim, _ := provision()

		setGPUCondition(node, corev1.ConditionFalse, "NvidiaXID79Error")
		// The node is replaced without ever being rebooted.
		Eventually(func(g Gomega) {
			nc := &karpv1.NodeClaim{}
			err := env.Client.Get(env, client.ObjectKeyFromObject(nodeClaim), nc)
			if err == nil {
				g.Expect(nc.StatusConditions().Get(karpv1.ConditionTypeRebooting)).To(BeNil(), "a fatal fault must not reboot")
				g.Expect(nc.DeletionTimestamp.IsZero()).To(BeFalse(), "not yet replaced")
			}
		}).Should(Succeed())
		env.EventuallyExpectNotFound(nodeClaim, node)
		env.EventuallyExpectHealthyPodCount(selector, 1)
		replacement := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
		Expect(replacement.Name).ToNot(Equal(nodeClaim.Name))
	})

	It("should reboot twice, then replace, when a reboot-clearable GPU fault keeps recurring", func() {
		node, nodeClaim, instance := provision()

		// Repair's escalation is reboot, reboot, then replace. After each successful reboot the agent reports the fault
		// again (re-injected with a fresh transition time), so the node is eligible again.
		bootID := node.Status.NodeInfo.BootID
		for range 2 {
			setGPUCondition(node, corev1.ConditionFalse, "NvidiaXID46Error")
			bootID = eventuallyExpectRebootSucceeded(nodeClaim, node, bootID)
			Expect(env.GetInstanceByID(aws.ToString(instance.InstanceId)).State.Name).To(Equal(ec2types.InstanceStateNameRunning))
		}
		setGPUCondition(node, corev1.ConditionFalse, "NvidiaXID46Error")

		// The third occurrence replaces the node instead of rebooting it again.
		Eventually(func(g Gomega) {
			nc := &karpv1.NodeClaim{}
			if err := env.Client.Get(env, client.ObjectKeyFromObject(nodeClaim), nc); err == nil {
				g.Expect(nc.StatusConditions().Get(karpv1.ConditionTypeRebooting).Reason).To(Equal(karpv1.RebootReasonSucceeded), "a third reboot was committed")
				g.Expect(nc.DeletionTimestamp.IsZero()).To(BeFalse(), "not yet replaced")
			}
		}).Should(Succeed())
		env.EventuallyExpectNotFound(nodeClaim, node)
		env.EventuallyExpectHealthyPodCount(selector, 1)
	})
})
