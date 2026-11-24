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

package capacitytype_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/awslabs/operatorpkg/object"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/cloudprovider"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/capacityreservation/capacitytype"
	"github.com/aws/karpenter-provider-aws/pkg/fake"
	"github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"

	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
)

var ctx context.Context
var stop context.CancelFunc
var env *coretest.Environment
var awsEnv *test.Environment
var controller *capacitytype.Controller

func TestAWS(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "SSM Invalidation Controller")
}

var _ = BeforeSuite(func() {
	env = coretest.NewEnvironment(coretest.WithCRDs(apis.CRDs...), coretest.WithCRDs(v1alpha1.CRDs...), coretest.WithFieldIndexers(coretest.NodeProviderIDFieldIndexer(ctx)))
	ctx = options.ToContext(ctx, test.Options())
	ctx = coreoptions.ToContext(ctx, coretest.Options(coretest.OptionsFields{FeatureGates: coretest.FeatureGates{ReservedCapacity: lo.ToPtr(true)}}))
	ctx, stop = context.WithCancel(ctx)
	awsEnv = test.NewEnvironment(ctx, env)

	cloudProvider := cloudprovider.New(awsEnv.InstanceTypesProvider, awsEnv.InstanceProvider, events.NewRecorder(&record.FakeRecorder{}),
		env.Client, awsEnv.AMIProvider, awsEnv.SecurityGroupProvider, awsEnv.CapacityReservationProvider, awsEnv.PlacementGroupProvider, awsEnv.InstanceTypeStore, lo.ToPtr(""))
	controller = capacitytype.NewController(env.Client, cloudProvider, awsEnv.CapacityReservationProvider)
})

var _ = AfterSuite(func() {
	stop()
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Capacity Reservation Capacity Type Controller", func() {
	var nodeClaim *karpv1.NodeClaim
	var node *corev1.Node
	var reservationID string
	BeforeEach(func() {
		reservationID = "cr-foo"
		instance := ec2types.Instance{
			ImageId:               lo.ToPtr(fake.ImageID()),
			InstanceType:          ec2types.InstanceType("m5.large"),
			SubnetId:              lo.ToPtr(fake.SubnetID()),
			SpotInstanceRequestId: nil,
			State: &ec2types.InstanceState{
				Name: ec2types.InstanceStateNameRunning,
			},
			InstanceId:            lo.ToPtr(fake.InstanceID()),
			CapacityReservationId: &reservationID,
			CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{
				CapacityReservationPreference: ec2types.CapacityReservationPreferenceCapacityReservationsOnly,
			},
			Placement: &ec2types.Placement{
				AvailabilityZone: lo.ToPtr("test-zone-1a"),
			},
			SecurityGroups: []ec2types.GroupIdentifier{{GroupId: lo.ToPtr(fake.SecurityGroupID())}},
		}
		awsEnv.EC2API.DescribeInstancesBehavior.Output.Set(&ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{instance}}},
		})

		nodeClaim = coretest.NodeClaim(karpv1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					karpv1.CapacityTypeLabelKey:              karpv1.CapacityTypeReserved,
					corecloudprovider.ReservationIDLabel:     reservationID,
					v1.LabelCapacityReservationType:          string(v1.CapacityReservationTypeDefault),
					v1.LabelCapacityReservationInterruptible: "false",
					karpv1.NodeRegisteredLabelKey:            "true",
				},
			},
			Status: karpv1.NodeClaimStatus{
				ProviderID: fmt.Sprintf("aws:///test-zone-1a/%s", *instance.InstanceId),
			},
		})
		node = coretest.NodeClaimLinkedNode(nodeClaim)
	})
	It("should demote nodeclaims and nodes from reserved to on-demand", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectSingletonReconciled(ctx, controller)

		// Since the backing instance is still under a capacity reservation, we shouldn't demote the nodeclaim or node
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, reservationID))
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(v1.LabelCapacityReservationType, string(v1.CapacityReservationTypeDefault)))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
		Expect(node.Labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, reservationID))
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(v1.LabelCapacityReservationType, string(v1.CapacityReservationTypeDefault)))

		out := awsEnv.EC2API.DescribeInstancesBehavior.Output.Clone()
		out.Reservations[0].Instances[0].CapacityReservationId = nil
		awsEnv.EC2API.DescribeInstancesBehavior.Output.Set(out)

		// Now that the backing instance is no longer part of a capacity reservation, we should demote the resources by
		// updating the capacity type to on-demand and removing the reservation ID label.
		ExpectSingletonReconciled(ctx, controller)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(nodeClaim.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
		Expect(nodeClaim.Labels).ToNot(HaveKey(v1.LabelCapacityReservationType))
		Expect(nodeClaim.Labels).ToNot(HaveKey(v1.LabelCapacityReservationInterruptible))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(node.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
		Expect(node.Labels).ToNot(HaveKey(v1.LabelCapacityReservationType))
		Expect(node.Labels).ToNot(HaveKey(v1.LabelCapacityReservationInterruptible))
	})
	It("should demote nodes from reserved to on-demand even if their nodeclaim was demoted previously", func() {
		out := awsEnv.EC2API.DescribeInstancesBehavior.Output.Clone()
		out.Reservations[0].Instances[0].CapacityReservationId = nil
		awsEnv.EC2API.DescribeInstancesBehavior.Output.Set(out)

		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectSingletonReconciled(ctx, controller)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(nodeClaim.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
		Expect(nodeClaim.Labels).ToNot(HaveKey(v1.LabelCapacityReservationType))

		ExpectApplied(ctx, env.Client, node)
		ExpectSingletonReconciled(ctx, controller)
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(node.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
		Expect(node.Labels).ToNot(HaveKey(v1.LabelCapacityReservationType))
	})
	Context("Promotion", func() {
		var nodeClass *v1.EC2NodeClass
		BeforeEach(func() {
			awsEnv.CapacityReservationProvider.ResetMigrations()
			// Re-targeted in place: covered by its target, with no capacity-reservations-only preference.
			out := awsEnv.EC2API.DescribeInstancesBehavior.Output.Clone()
			out.Reservations[0].Instances[0].CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
				CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr(reservationID)},
			}
			awsEnv.EC2API.DescribeInstancesBehavior.Output.Set(out)
			nodeClass = test.EC2NodeClass()
			nodeClass.Status.CapacityReservations = []v1.CapacityReservation{{
				ID: reservationID, AvailabilityZone: "test-zone-1a", InstanceType: "m5.large", OwnerID: "012345678901",
				InstanceMatchCriteria: string(ec2types.InstanceMatchCriteriaTargeted), ReservationType: v1.CapacityReservationTypeDefault, State: v1.CapacityReservationStateActive,
			}}
			ExpectApplied(ctx, env.Client, nodeClass)
			nodeClaim.Spec.NodeClassRef = &karpv1.NodeClassReference{Group: object.GVK(nodeClass).Group, Kind: object.GVK(nodeClass).Kind, Name: nodeClass.Name}
			for _, labels := range []map[string]string{nodeClaim.Labels, node.Labels} {
				labels[karpv1.CapacityTypeLabelKey] = karpv1.CapacityTypeOnDemand
				delete(labels, corecloudprovider.ReservationIDLabel)
				delete(labels, v1.LabelCapacityReservationType)
				delete(labels, v1.LabelCapacityReservationInterruptible)
			}
			nodeClaim.Annotations = map[string]string{
				v1.AnnotationCapacityReservationMigrationTarget:      reservationID,
				v1.AnnotationCapacityReservationMigrationRequestedAt: awsEnv.Clock.Now().UTC().Format(time.RFC3339),
			}
			awsEnv.CapacityReservationProvider.SetAvailableInstanceCount(reservationID, 2)
		})
		AfterEach(func() {
			ExpectCleanedUp(ctx, env.Client)
		})
		expectCapacityType := func(capacityType string) {
			GinkgoHelper()
			for _, labels := range []map[string]string{ExpectExists(ctx, env.Client, nodeClaim).Labels, ExpectExists(ctx, env.Client, node).Labels} {
				Expect(labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, capacityType))
			}
		}
		It("should finish a migration the migration controller started", func() {
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			for _, labels := range []map[string]string{ExpectExists(ctx, env.Client, nodeClaim).Labels, ExpectExists(ctx, env.Client, node).Labels} {
				Expect(labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
				Expect(labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, reservationID))
				Expect(labels).To(HaveKeyWithValue(v1.LabelCapacityReservationType, string(v1.CapacityReservationTypeDefault)))
			}
			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, reservationID))
			Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationRequestedAt))
			// EC2's count includes the instance before our cache does.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount(reservationID)).To(Equal(1))
		})
		It("should not promote coverage the migration controller didn't start", func() {
			nodeClaim.Annotations = nil
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
		})
		It("should not promote an unannotated node whose NodePool requires on-demand", func() {
			nodePool := coretest.NodePool(karpv1.NodePool{Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
				Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}},
			}}}})
			nodeClaim.Labels[karpv1.NodePoolLabelKey] = nodePool.Name
			nodeClaim.Annotations = nil
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
		})
		It("should not promote when the annotation names another reservation", func() {
			nodeClaim.Annotations[v1.AnnotationCapacityReservationMigrationTarget] = "cr-other"
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
		})
		It("should not promote a reservation the EC2NodeClass no longer selects", func() {
			nodeClass.Status.CapacityReservations = nil
			ExpectApplied(ctx, env.Client, nodeClass)
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
		})
		It("should leave completion to the migration controller while its gate is on", func() {
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			gateOn := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacityMigration: lo.ToPtr(true)}}))
			ExpectSingletonReconciled(gateOn, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
		})
		It("should relabel a Node left on-demand under a reserved NodeClaim", func() {
			nodeClaim.Annotations = nil
			nodeClaim.Labels = lo.Assign(nodeClaim.Labels, map[string]string{
				karpv1.CapacityTypeLabelKey:          karpv1.CapacityTypeReserved,
				corecloudprovider.ReservationIDLabel: reservationID,
				v1.LabelCapacityReservationType:      string(v1.CapacityReservationTypeDefault),
			})
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectSingletonReconciled(ctx, controller)
			labels := ExpectExists(ctx, env.Client, node).Labels
			Expect(labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
			Expect(labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, reservationID))
			Expect(labels).To(HaveKeyWithValue(v1.LabelCapacityReservationType, string(v1.CapacityReservationTypeDefault)))
		})
		It("should skip a deleting NodeClaim", func() {
			nodeClaim.Finalizers = []string{karpv1.TerminationFinalizer}
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
			ExpectSingletonReconciled(ctx, controller)
			expectCapacityType(karpv1.CapacityTypeOnDemand)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		})
	})
})
