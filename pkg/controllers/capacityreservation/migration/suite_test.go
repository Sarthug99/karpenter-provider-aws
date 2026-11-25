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

package migration_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	arczonalshifttypes "github.com/aws/aws-sdk-go-v2/service/arczonalshift/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/awslabs/operatorpkg/object"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/events"
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
	coretest "sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/cloudprovider"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/capacityreservation/capacitytype"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/capacityreservation/migration"
	"github.com/aws/karpenter-provider-aws/pkg/fake"
	"github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instance"
	"github.com/aws/karpenter-provider-aws/pkg/test"
	"github.com/aws/karpenter-provider-aws/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var stop context.CancelFunc
var env *coretest.Environment
var awsEnv *test.Environment
var cloudProvider *cloudprovider.CloudProvider
var recorder *coretest.EventRecorder
var controller *migration.Controller
var capacityTypeController *capacitytype.Controller

const (
	zone         = "test-zone-1a"
	instanceType = "m5.large"
	platform     = "Linux/UNIX"
	ownerID      = "012345678901"
)

func TestMigration(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Capacity Reservation Migration Controller")
}

var _ = BeforeSuite(func() {
	env = coretest.NewEnvironment(coretest.WithCRDs(apis.CRDs...), coretest.WithCRDs(v1alpha1.CRDs...), coretest.WithFieldIndexers(coretest.NodeProviderIDFieldIndexer(ctx)))
	ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacityMigration: lo.ToPtr(true)}}))
	ctx = coreoptions.ToContext(ctx, coretest.Options(coretest.OptionsFields{FeatureGates: coretest.FeatureGates{ReservedCapacity: lo.ToPtr(true)}}))
	ctx, stop = context.WithCancel(ctx)
	awsEnv = test.NewEnvironment(ctx, env)
	cloudProvider = cloudprovider.New(awsEnv.InstanceTypesProvider, awsEnv.InstanceProvider, events.NewRecorder(&record.FakeRecorder{}),
		env.Client, awsEnv.AMIProvider, awsEnv.SecurityGroupProvider, awsEnv.CapacityReservationProvider, awsEnv.PlacementGroupProvider, awsEnv.InstanceTypeStore, lo.ToPtr(""))
	recorder = coretest.NewEventRecorder()
	capacityTypeController = capacitytype.NewController(env.Client, cloudProvider, awsEnv.CapacityReservationProvider)
})

var _ = AfterSuite(func() {
	stop()
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

// nodeClaimPatchNotFound simulates a NodeClaim deleted between the List and a Patch.
type nodeClaimPatchNotFound struct{ client.Client }

func (c nodeClaimPatchNotFound) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*karpv1.NodeClaim); ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}, obj.GetName())
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// nodeClaimPatchConflict simulates a NodeClaim updated between the List and a Patch.
type nodeClaimPatchConflict struct{ client.Client }

func (c nodeClaimPatchConflict) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*karpv1.NodeClaim); ok {
		return apierrors.NewConflict(schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}, obj.GetName(), errors.New("modified"))
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// failingList counts capacity reservation queries and fails every one.
type failingList struct {
	capacityreservation.Provider
	calls *int
}

func (f failingList) List(context.Context, ...v1.CapacityReservationSelectorTerm) ([]*ec2types.CapacityReservation, error) {
	*f.calls++
	return nil, &smithy.GenericAPIError{Code: "RequestLimitExceeded"}
}

// deniedTarget fails every Modify at one reservation as unauthorized, as a resource-scoped deny would.
type deniedTarget struct {
	instance.Provider
	denied string
	calls  *int
}

func (d deniedTarget) ModifyCapacityReservationTarget(ctx context.Context, id, target string) error {
	if target == d.denied {
		*d.calls++
		return &smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}
	}
	return d.Provider.ModifyCapacityReservationTarget(ctx, id, target)
}

// deniedInstances fails every Modify of some instances as unauthorized, as a deny scoped by instance tags would.
type deniedInstances struct {
	instance.Provider
	denied sets.Set[string]
	calls  *int
}

func (d deniedInstances) ModifyCapacityReservationTarget(ctx context.Context, id, target string) error {
	if d.denied.Has(id) {
		*d.calls++
		return &smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}
	}
	return d.Provider.ModifyCapacityReservationTarget(ctx, id, target)
}

// countingList counts DescribeInstances sweeps.
type countingList struct {
	instance.Provider
	calls *int
}

func (l countingList) List(ctx context.Context) ([]*instance.Instance, error) {
	*l.calls++
	return l.Provider.List(ctx)
}

// notAttemptedOnce fails the first Modify before it reaches EC2.
type notAttemptedOnce struct {
	instance.Provider
	failed *bool
}

func (n notAttemptedOnce) ModifyCapacityReservationTarget(ctx context.Context, id, target string) error {
	if !*n.failed {
		*n.failed = true
		return fmt.Errorf("%w, %w", instance.ErrModifyNotAttempted, errors.New("describing instance"))
	}
	return n.Provider.ModifyCapacityReservationTarget(ctx, id, target)
}

// throttledBeforeModify is throttled on the DescribeInstances that precedes every Modify.
type throttledBeforeModify struct {
	instance.Provider
	calls *int
}

func (t throttledBeforeModify) ModifyCapacityReservationTarget(context.Context, string, string) error {
	*t.calls++
	return fmt.Errorf("%w, %w", instance.ErrModifyNotAttempted, &smithy.GenericAPIError{Code: "RequestLimitExceeded", Fault: smithy.FaultClient})
}

// listThenRun runs a hook right after taking the instance snapshot, to interleave a concurrent writer.
type listThenRun struct {
	*cloudprovider.CloudProvider
	hook func()
}

func (l listThenRun) List(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	ncs, err := l.CloudProvider.List(ctx)
	l.hook()
	return ncs, err
}

func reservation(id string, available int32, mutate ...func(*ec2types.CapacityReservation)) ec2types.CapacityReservation {
	cr := ec2types.CapacityReservation{
		CapacityReservationId:  lo.ToPtr(id),
		AvailabilityZone:       lo.ToPtr(zone),
		InstanceType:           lo.ToPtr(instanceType),
		InstancePlatform:       ec2types.CapacityReservationInstancePlatform(platform),
		Tenancy:                ec2types.CapacityReservationTenancyDefault,
		OwnerId:                lo.ToPtr(ownerID),
		InstanceMatchCriteria:  ec2types.InstanceMatchCriteriaTargeted,
		State:                  ec2types.CapacityReservationStateActive,
		ReservationType:        ec2types.CapacityReservationTypeDefault,
		AvailableInstanceCount: lo.ToPtr(available),
		TotalInstanceCount:     lo.ToPtr(available + 5),
	}
	for _, m := range mutate {
		m(&cr)
	}
	return cr
}

func statusFor(cr ec2types.CapacityReservation) v1.CapacityReservation {
	return lo.Must(v1.CapacityReservationFromEC2(awsEnv.Clock, &cr))
}

var _ = Describe("Capacity Reservation Migration Controller", func() {
	var nodeClass *v1.EC2NodeClass
	var nodePool *karpv1.NodePool

	// setReservations seeds EC2 with crs and puts the ones in status into the EC2NodeClass status.
	setReservations := func(inEC2 []ec2types.CapacityReservation, inStatus ...ec2types.CapacityReservation) {
		awsEnv.EC2API.DescribeCapacityReservationsOutput.Set(&ec2.DescribeCapacityReservationsOutput{CapacityReservations: inEC2})
		awsEnv.CapacityReservationCache.Flush()
		nodeClass.Spec.CapacityReservationSelectorTerms = lo.Map(inStatus, func(cr ec2types.CapacityReservation, _ int) v1.CapacityReservationSelectorTerm {
			return v1.CapacityReservationSelectorTerm{ID: *cr.CapacityReservationId}
		})
		nodeClass.Status.CapacityReservations = lo.Map(inStatus, func(cr ec2types.CapacityReservation, _ int) v1.CapacityReservation { return statusFor(cr) })
		ExpectApplied(ctx, env.Client, nodeClass)
	}

	type nodeOpts struct {
		instance  func(*ec2types.Instance)
		nodeClaim func(*karpv1.NodeClaim)
	}
	// makeNode creates an initialized on-demand NodeClaim, its Node, and the backing running instance.
	makeNode := func(opts ...nodeOpts) (*karpv1.NodeClaim, *corev1.Node, string) {
		id := fake.InstanceID()
		inst := ec2types.Instance{
			InstanceId:      lo.ToPtr(id),
			ImageId:         lo.ToPtr(fake.ImageID()),
			InstanceType:    ec2types.InstanceType(instanceType),
			SubnetId:        lo.ToPtr(fake.SubnetID()),
			PlatformDetails: lo.ToPtr(platform),
			LaunchTime:      lo.ToPtr(time.Now()),
			State:           &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
			Placement:       &ec2types.Placement{AvailabilityZone: lo.ToPtr(zone), Tenancy: ec2types.TenancyDefault},
			CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{
				CapacityReservationPreference: ec2types.CapacityReservationPreferenceNone,
			},
			Tags: []ec2types.Tag{
				{Key: lo.ToPtr(v1.NodePoolTagKey), Value: lo.ToPtr(nodePool.Name)},
				{Key: lo.ToPtr(v1.NodeClassTagKey), Value: lo.ToPtr(nodeClass.Name)},
				{Key: lo.ToPtr(v1.EKSClusterNameTagKey), Value: lo.ToPtr("test-cluster")},
			},
		}
		nc := coretest.NodeClaim(karpv1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					karpv1.NodePoolLabelKey:        nodePool.Name,
					karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
					corev1.LabelInstanceTypeStable: instanceType,
					corev1.LabelTopologyZone:       zone,
					karpv1.NodeRegisteredLabelKey:  "true",
				},
			},
			Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{Group: object.GVK(nodeClass).Group, Kind: object.GVK(nodeClass).Kind, Name: nodeClass.Name}},
			Status: karpv1.NodeClaimStatus{
				ProviderID: fmt.Sprintf("aws:///%s/%s", zone, id),
			},
		})
		nc.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(karpv1.ConditionTypeInitialized)
		for _, o := range opts {
			if o.instance != nil {
				o.instance(&inst)
			}
			if o.nodeClaim != nil {
				o.nodeClaim(nc)
			}
		}
		awsEnv.EC2API.Instances.Store(id, inst)
		node := coretest.NodeClaimLinkedNode(nc)
		ExpectApplied(ctx, env.Client, nc, node)
		return nc, node, id
	}
	ec2Instance := func(id string) ec2types.Instance {
		raw, ok := awsEnv.EC2API.Instances.Load(id)
		Expect(ok).To(BeTrue())
		return raw.(ec2types.Instance)
	}
	cover := func(id string) {
		inst := ec2Instance(id)
		inst.CapacityReservationId = inst.CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId
		awsEnv.EC2API.Instances.Store(id, inst)
	}
	modifyCalls := func() int { return awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Calls() }
	// migrationOrder returns instance IDs in the order the controller tries on-demand nodes: oldest, then by name.
	migrationOrder := func() []string {
		ncs := &karpv1.NodeClaimList{}
		Expect(env.Client.List(ctx, ncs)).To(Succeed())
		sort.SliceStable(ncs.Items, func(i, j int) bool {
			ti, tj := ncs.Items[i].CreationTimestamp, ncs.Items[j].CreationTimestamp
			if !ti.Equal(&tj) {
				return ti.Before(&tj)
			}
			return ncs.Items[i].Name < ncs.Items[j].Name
		})
		return lo.Map(ncs.Items, func(nc karpv1.NodeClaim, _ int) string { return lo.Must(utils.ParseInstanceID(nc.Status.ProviderID)) })
	}
	expectOnDemand := func(nc *karpv1.NodeClaim, node *corev1.Node) {
		GinkgoHelper()
		nc = ExpectExists(ctx, env.Client, nc)
		Expect(nc.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(nc.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
		Expect(node.Labels).ToNot(HaveKey(corecloudprovider.ReservationIDLabel))
	}
	expectReserved := func(nc *karpv1.NodeClaim, node *corev1.Node, id string) {
		GinkgoHelper()
		for _, labels := range []map[string]string{ExpectExists(ctx, env.Client, nc).Labels, ExpectExists(ctx, env.Client, node).Labels} {
			Expect(labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
			Expect(labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, id))
			Expect(labels).To(HaveKeyWithValue(v1.LabelCapacityReservationType, string(v1.CapacityReservationTypeDefault)))
			Expect(labels).To(HaveKeyWithValue(v1.LabelCapacityReservationInterruptible, "false"))
			for label := range corecloudprovider.ReservedCapacityLabels {
				Expect(labels).To(HaveKey(label))
			}
		}
	}

	BeforeEach(func() {
		awsEnv.Reset()
		recorder.Reset()
		migration.MigrationAttemptsTotal.Reset()
		migration.MigrationIneligible.Reset()
		controller = migration.NewController(awsEnv.Clock, env.Client, cloudProvider, awsEnv.InstanceProvider, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
		nodeClass = test.EC2NodeClass()
		nodePool = coretest.NodePool(karpv1.NodePool{Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn,
				Values: []string{karpv1.CapacityTypeReserved, karpv1.CapacityTypeOnDemand}}},
		}}}})
		ExpectApplied(ctx, env.Client, nodeClass, nodePool)
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})

	Context("Feature gate", func() {
		DescribeTable("should only enable the controller with both gates",
			func(migrationGate, reservedCapacity, expected bool) {
				gctx := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacityMigration: lo.ToPtr(migrationGate)}}))
				gctx = coreoptions.ToContext(gctx, coretest.Options(coretest.OptionsFields{FeatureGates: coretest.FeatureGates{ReservedCapacity: lo.ToPtr(reservedCapacity)}}))
				Expect(migration.Enabled(gctx)).To(Equal(expected))
			},
			Entry("both on", true, true, true),
			Entry("migration gate off", false, true, false),
			Entry("ReservedCapacity off", true, false, false),
			Entry("both off", false, false, false),
		)
	})

	Context("On-demand nodes", func() {
		It("should re-target, wait for coverage, then relabel the NodeClaim and Node reserved", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, node, id := makeNode()

			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			input := awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.Pop()
			Expect(*input.InstanceId).To(Equal(id))
			Expect(*input.CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-1"))
			Expect(input.CapacityReservationSpecification.CapacityReservationPreference).To(BeEmpty())
			nc = ExpectExists(ctx, env.Client, nc)
			Expect(nc.Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			Expect(nc.Annotations).To(HaveKey(v1.AnnotationCapacityReservationMigrationRequestedAt))
			Expect(recorder.Calls(migration.EventReasonStarted)).To(Equal(1))
			// The pending slot is held against the reservation even though EC2 still reports 2 free.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))

			// Coverage lags: labels stay on-demand, the capacitytype controller leaves it alone, and nothing is re-issued.
			for range 3 {
				awsEnv.Clock.Step(time.Minute)
				ExpectSingletonReconciled(ctx, controller)
				ExpectSingletonReconciled(ctx, capacityTypeController)
			}
			Expect(modifyCalls()).To(Equal(1))
			expectOnDemand(nc, node)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))

			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			nc = ExpectExists(ctx, env.Client, nc)
			Expect(nc.Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			Expect(nc.Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationRequestedAt))
			Expect(recorder.Calls(migration.EventReasonCompleted)).To(Equal(1))
			Expect(modifyCalls()).To(Equal(1))

			// Target == covering is reserved, so capacitytype doesn't demote it.
			ExpectSingletonReconciled(ctx, capacityTypeController)
			expectReserved(nc, node, "cr-1")
			cpNC, err := cloudProvider.Get(ctx, nc.Status.ProviderID)
			Expect(err).ToNot(HaveOccurred())
			Expect(cpNC.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeReserved))
			Expect(cpNC.Labels).To(HaveKeyWithValue(corecloudprovider.ReservationIDLabel, "cr-1"))
		})
		It("should skip the instance sweep only while no EC2NodeClass selects a reservation and nothing is pending", func() {
			// EC2NodeClasses outlive ExpectCleanedUp, so drop earlier specs' ones that select reservations.
			nodeClasses := &v1.EC2NodeClassList{}
			Expect(env.Client.List(ctx, nodeClasses)).To(Succeed())
			for i := range nodeClasses.Items {
				if nodeClasses.Items[i].Name != nodeClass.Name {
					ExpectDeleted(ctx, env.Client, &nodeClasses.Items[i])
				}
			}
			makeNode()
			lists := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, countingList{awsEnv.InstanceProvider, &lists}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			ExpectSingletonReconciled(ctx, c)
			Expect(lists).To(Equal(0))

			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			ExpectSingletonReconciled(ctx, c)
			Expect(lists).To(Equal(1))
			Expect(modifyCalls()).To(Equal(1))
			// The pending move keeps the sweep, and its hold, after the reservation leaves the status.
			nodeClass.Status.CapacityReservations = nil
			ExpectApplied(ctx, env.Client, nodeClass)
			ExpectSingletonReconciled(ctx, c)
			Expect(lists).To(Equal(2))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should hold a covered slot until the next EC2 sync includes it", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			_, _, id := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationCoverage.Store(true)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(ec2Instance(id).CapacityReservationId).To(Equal(lo.ToPtr("cr-1")))
			// Covered and relabeled, but EC2's stale count hasn't been re-synced yet.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))

			awsEnv.Clock.Step(time.Minute)
			awsEnv.EC2API.DescribeCapacityReservationsOutput.Set(&ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{reservation("cr-1", 1)}})
			awsEnv.CapacityReservationCache.Flush()
			_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
			Expect(err).ToNot(HaveOccurred())
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should keep holding a pending slot across a stale EC2 sync", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			awsEnv.CapacityReservationCache.Flush()
			_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
			Expect(err).ToNot(HaveOccurred())
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should not hold a covered slot that an EC2 sync issued after the Modify already counts", func() {
			cr := reservation("cr-1", 5)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			_, _, id1 := makeNode()
			_, _, id2 := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			cover(id1)
			cover(id2)
			awsEnv.Clock.Step(20 * time.Second)
			awsEnv.EC2API.DescribeCapacityReservationsOutput.Set(&ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{reservation("cr-1", 3)}})
			awsEnv.CapacityReservationCache.Flush()
			_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
			Expect(err).ToNot(HaveOccurred())
			awsEnv.Clock.Step(40 * time.Second)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(3))
		})
		It("should rebuild the ledger from annotations after a restart", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			awsEnv.CapacityReservationProvider.ResetMigrations()
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
			restarted := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, awsEnv.InstanceProvider, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			ExpectSingletonReconciled(ctx, restarted)
			Expect(modifyCalls()).To(Equal(1))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should release the pending slot when the NodeClaim is deleted", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, node, _ := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			ExpectDeleted(ctx, env.Client, node, nc)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
		})
		It("should keep holding a pending slot while the NodeClaim drains", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, _ := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) { nc.Finalizers = []string{karpv1.TerminationFinalizer} }})
			ExpectSingletonReconciled(ctx, controller)
			Expect(env.Client.Delete(ctx, nc)).To(Succeed())
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should not hold a stalled move's slot again once its NodeClaim drains", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, _ := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) { nc.Finalizers = []string{karpv1.TerminationFinalizer} }})
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.Clock.Step(31 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
			Expect(env.Client.Delete(ctx, nc)).To(Succeed())
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
		})
		It("should not hold a draining node's slot for an ambiguous Modify EC2 never applied", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, id := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) { nc.Finalizers = []string{karpv1.TerminationFinalizer} }})
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "InternalError", Fault: smithy.FaultServer}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(ec2Instance(id).CapacityReservationSpecification.CapacityReservationTarget).To(BeNil())
			Expect(env.Client.Delete(ctx, nc)).To(Succeed())
			ExpectSingletonReconciled(ctx, controller)
			// DescribeInstances may still lag the call.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.Clock.Step(10 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
		})
		It("should stop holding an out-of-band target it won't relabel once it stalls", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nodePool.Spec.Template.Spec.Requirements = []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, nodePool)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-1")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.Clock.Step(31 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
		})
		It("should not hold a draining node's slot past the EC2 sync that counts its coverage", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, id := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) { nc.Finalizers = []string{karpv1.TerminationFinalizer} }})
			ExpectSingletonReconciled(ctx, controller)
			cover(id)
			Expect(env.Client.Delete(ctx, nc)).To(Succeed())
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))

			awsEnv.Clock.Step(time.Minute)
			awsEnv.EC2API.DescribeCapacityReservationsOutput.Set(&ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{reservation("cr-1", 1)}})
			awsEnv.CapacityReservationCache.Flush()
			_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
			Expect(err).ToNot(HaveOccurred())
			for range 2 {
				ExpectSingletonReconciled(ctx, controller)
				Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			}
		})
		It("should keep holding a pending slot when the instance is briefly missing from DescribeInstances", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			_, _, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			inst := ec2Instance(id)
			awsEnv.EC2API.Instances.Delete(id)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.EC2API.Instances.Store(id, inst)
		})
		It("should keep holding a pending slot when a pass errors on that node", func() {
			cr := reservation("cr-1", 1)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			_, _, first := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(*awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.Pop().InstanceId).To(Equal(first))
			makeNode()
			// The pending node's reservation lookup fails; the second node must not get the slot it still holds.
			awsEnv.CapacityReservationCache.Flush()
			awsEnv.EC2API.NextError.Set(&smithy.GenericAPIError{Code: "RequestLimitExceeded"})
			// The error is logged and the pass keeps its regular cadence instead of a rate-limited retry.
			result, err := controller.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))
			Expect(modifyCalls()).To(Equal(1))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(0))
		})
		It("should hold a covered slot in each reservation that covers the instance across erroring passes", func() {
			cr1, cr2 := reservation("cr-1", 2), reservation("cr-2", 2)
			setReservations([]ec2types.CapacityReservation{cr1, cr2}, cr1, cr2)
			retarget := func(nc *karpv1.NodeClaim, id, target string) {
				inst := ec2Instance(id)
				inst.CapacityReservationId = lo.ToPtr(target)
				inst.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr(target)},
				}
				awsEnv.EC2API.Instances.Store(id, inst)
				nc = ExpectExists(ctx, env.Client, nc)
				nc.Annotations = map[string]string{v1.AnnotationCapacityReservationMigrationTarget: target}
				ExpectApplied(ctx, env.Client, nc)
			}
			awsEnv.CapacityReservationProvider.SetAvailableInstanceCount("cr-1", 2)
			awsEnv.CapacityReservationProvider.SetAvailableInstanceCount("cr-2", 2)
			nc, _, id := makeNode()
			// Completing fails on the NodeClaim patch, so every pass errors on this node.
			c := migration.NewController(awsEnv.Clock, nodeClaimPatchConflict{env.Client}, cloudProvider, awsEnv.InstanceProvider, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			retarget(nc, id, "cr-1")
			ExpectSingletonReconciled(ctx, c)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			retarget(nc, id, "cr-2")
			ExpectSingletonReconciled(ctx, c)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-2")).To(Equal(1))
		})
		It("should not treat a NodeClaim deleted before annotation as a failed pass", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode()
			c := migration.NewController(awsEnv.Clock, nodeClaimPatchNotFound{env.Client}, cloudProvider, awsEnv.InstanceProvider, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			result, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))
			Expect(modifyCalls()).To(Equal(0))
		})
		It("should not repeat a failing reservation query within a pass", func() {
			cr := reservation("cr-1", 5)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			for range 3 {
				makeNode()
			}
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(3))
			calls := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, awsEnv.InstanceProvider, failingList{awsEnv.CapacityReservationProvider, &calls}, awsEnv.ZonalShiftProvider, recorder)
			_, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(calls).To(Equal(1))
		})
		It("should retry soon after a conflict-only pass that made no Modify calls", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode()
			c := migration.NewController(awsEnv.Clock, nodeClaimPatchConflict{env.Client}, cloudProvider, awsEnv.InstanceProvider, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			result, err := c.Reconcile(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(5 * time.Second))
			Expect(modifyCalls()).To(Equal(0))
		})
		It("should not re-issue when DescribeInstances lags its own Modify", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			// A stale read still shows no target right after the call.
			inst := ec2Instance(id)
			applied := inst.CapacityReservationSpecification
			inst.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{CapacityReservationPreference: ec2types.CapacityReservationPreferenceNone}
			awsEnv.EC2API.Instances.Store(id, inst)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))

			inst.CapacityReservationSpecification = applied
			awsEnv.EC2API.Instances.Store(id, inst)
			awsEnv.Clock.Step(10 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
		})
		It("should return a migrated node's slot exactly once on delete", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, id := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationCoverage.Store(true)
			ExpectSingletonReconciled(ctx, controller)
			ExpectSingletonReconciled(ctx, controller)
			nc = ExpectExists(ctx, env.Client, nc)
			awsEnv.CapacityReservationProvider.ResetMigrations()
			awsEnv.CapacityReservationProvider.SetAvailableInstanceCount("cr-1", 1)
			awsEnv.EC2API.Instances.Delete(id)
			for range 2 {
				Expect(corecloudprovider.IsNodeClaimNotFoundError(cloudProvider.Delete(ctx, nc))).To(BeTrue())
			}
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
		})
		It("should migrate at most as many nodes as there are free slots", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			for range 3 {
				makeNode()
			}
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(0))
		})
		Context("Un-launched reserved NodeClaims", func() {
			var pending *karpv1.NodeClaim
			unlaunched := func(capacityTypes []string, ids ...string) *karpv1.NodeClaim {
				nc := coretest.NodeClaim(karpv1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{karpv1.NodePoolLabelKey: nodePool.Name}},
					Spec: karpv1.NodeClaimSpec{
						NodeClassRef: &karpv1.NodeClassReference{Group: object.GVK(nodeClass).Group, Kind: object.GVK(nodeClass).Kind, Name: nodeClass.Name},
						Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
							{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: capacityTypes},
							{Key: corecloudprovider.ReservationIDLabel, Operator: corev1.NodeSelectorOpIn, Values: ids},
						},
					},
				})
				nc.Status.ProviderID = ""
				return nc
			}
			BeforeEach(func() {
				cr := reservation("cr-1", 2)
				setReservations([]ec2types.CapacityReservation{cr}, cr)
				pending = unlaunched([]string{karpv1.CapacityTypeReserved}, "cr-1")
				ExpectApplied(ctx, env.Client, pending)
			})
			It("should hold a slot for a reserved NodeClaim that hasn't launched", func() {
				makeNode()
				makeNode()
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(1))
				ExpectMetricGaugeValue(migration.MigrationIneligible, 1, map[string]string{"reason": "no_capacity"})
			})
			It("should not hold a slot for a NodeClaim that may also launch on-demand", func() {
				ExpectDeleted(ctx, env.Client, pending)
				ExpectApplied(ctx, env.Client, unlaunched([]string{karpv1.CapacityTypeReserved, karpv1.CapacityTypeOnDemand}, "cr-1"))
				makeNode()
				makeNode()
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(2))
			})
			It("should not hold a slot for a deleting NodeClaim", func() {
				ExpectDeleted(ctx, env.Client, pending)
				deleting := unlaunched([]string{karpv1.CapacityTypeReserved}, "cr-1")
				deleting.Finalizers = []string{karpv1.TerminationFinalizer}
				ExpectApplied(ctx, env.Client, deleting)
				Expect(env.Client.Delete(ctx, deleting)).To(Succeed())
				makeNode()
				makeNode()
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(2))
			})
			It("should hold a slot in each reservation the NodeClaim may launch into", func() {
				a, b := reservation("cr-1", 1), reservation("cr-2", 1)
				setReservations([]ec2types.CapacityReservation{a, b}, a, b)
				ExpectDeleted(ctx, env.Client, pending)
				ExpectApplied(ctx, env.Client, unlaunched([]string{karpv1.CapacityTypeReserved}, "cr-1", "cr-2"))
				makeNode()
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(0))
				ExpectMetricGaugeValue(migration.MigrationIneligible, 1, map[string]string{"reason": "no_capacity"})
			})
			It("should stop holding once the NodeClaim launches, without double counting", func() {
				_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
				Expect(err).ToNot(HaveOccurred())
				pending.Status.ProviderID = fmt.Sprintf("aws:///%s/%s", zone, fake.InstanceID())
				ExpectApplied(ctx, env.Client, pending)
				awsEnv.CapacityReservationProvider.MarkLaunched("cr-1")
				makeNode()
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(1))
			})
		})
		It("should make at most 10 Modify calls per pass", func() {
			cr := reservation("cr-1", 20)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			for range 11 {
				makeNode()
			}
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(10))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(11))
		})
		It("should pick the reservation with the most free slots, then the lowest ID", func() {
			a, b, c := reservation("cr-a", 1), reservation("cr-b", 3), reservation("cr-c", 3)
			setReservations([]ec2types.CapacityReservation{a, b, c}, a, b, c)
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(*awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.Pop().CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-b"))
		})
		It("should prefer reservations that never end, then the latest end, over free slots", func() {
			ends := func(d time.Duration) func(*ec2types.CapacityReservation) {
				return func(cr *ec2types.CapacityReservation) { cr.EndDate = lo.ToPtr(awsEnv.Clock.Now().Add(d)) }
			}
			soon, later, never := reservation("cr-a", 5, ends(2*time.Hour)), reservation("cr-b", 2, ends(48*time.Hour)), reservation("cr-c", 1)
			setReservations([]ec2types.CapacityReservation{soon, later, never}, soon, later, never)
			makeNode()
			makeNode()
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			var targets []string
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.ForEach(func(in *ec2.ModifyInstanceCapacityReservationAttributesInput) {
				targets = append(targets, *in.CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId)
			})
			Expect(targets).To(ConsistOf("cr-c", "cr-b", "cr-b"))
		})
		It("should skip reservations ending within the hour", func() {
			ending := reservation("cr-1", 5, func(cr *ec2types.CapacityReservation) {
				cr.EndDate = lo.ToPtr(awsEnv.Clock.Now().Add(30 * time.Minute))
			})
			setReservations([]ec2types.CapacityReservation{ending}, ending)
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			ExpectMetricGaugeValue(migration.MigrationIneligible, 1, map[string]string{"reason": "no_compatible_cr"})
		})
		It("should only use reservations in the EC2NodeClass status", func() {
			inStatus, notInStatus := reservation("cr-1", 0), reservation("cr-2", 5)
			setReservations([]ec2types.CapacityReservation{inStatus, notInStatus}, inStatus)
			// cr-2 is selected and in EC2 but not yet in status.
			nodeClass.Spec.CapacityReservationSelectorTerms = append(nodeClass.Spec.CapacityReservationSelectorTerms, v1.CapacityReservationSelectorTerm{ID: "cr-2"})
			ExpectApplied(ctx, env.Client, nodeClass)
			makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
		})
		It("should stall a node that stays uncovered for 30 minutes and release its slot, without re-targeting it", func() {
			cr := reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.Clock.Step(31 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(recorder.Calls(migration.EventReasonStalled)).To(Equal(1))
			Expect(modifyCalls()).To(Equal(1))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			// The slot is released for launches, but coverage is still followed.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(2))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
		})
	})

	Context("Eligibility", func() {
		var cr ec2types.CapacityReservation
		BeforeEach(func() {
			cr = reservation("cr-1", 5)
		})
		expectNoMigration := func(reason ...string) {
			GinkgoHelper()
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			for _, r := range reason {
				ExpectMetricGaugeValue(migration.MigrationIneligible, 1, map[string]string{"reason": r})
			}
		}
		It("should skip instances with preference open", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification.CapacityReservationPreference = ec2types.CapacityReservationPreferenceOpen
			}})
			expectNoMigration("preference_open")
		})
		It("should skip instances in a zonally shifted zone without annotating them", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			awsEnv.ARCZonalShiftAPI.GetManagedResourceBehavior.Output.Set(&arczonalshift.GetManagedResourceOutput{
				ZonalShifts: []arczonalshifttypes.ZonalShiftInResource{{
					AwayFrom:      lo.ToPtr("tstz1-1a"),
					ExpiryTime:    lo.ToPtr(awsEnv.Clock.Now().Add(time.Hour)),
					AppliedStatus: arczonalshifttypes.AppliedStatusApplied,
				}},
			})
			Expect(awsEnv.ZonalShiftProvider.UpdateZonalShifts(ctx)).To(Succeed())
			nc, _, _ := makeNode(nodeOpts{instance: func(i *ec2types.Instance) { i.Placement.AvailabilityZoneId = lo.ToPtr("tstz1-1a") }})
			expectNoMigration("zonal_shift")
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			Expect(recorder.Calls(migration.EventReasonFailed)).To(Equal(0))
		})
		It("should skip spot instances", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{
				instance: func(i *ec2types.Instance) {
					i.SpotInstanceRequestId = lo.ToPtr("sir-1")
					i.InstanceLifecycle = ec2types.InstanceLifecycleTypeSpot
				},
				nodeClaim: func(nc *karpv1.NodeClaim) { nc.Labels[karpv1.CapacityTypeLabelKey] = karpv1.CapacityTypeSpot },
			})
			expectNoMigration()
		})
		It("should skip spot instances even when the NodeClaim is labeled on-demand", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.SpotInstanceRequestId = lo.ToPtr("sir-1")
				i.InstanceLifecycle = ec2types.InstanceLifecycleTypeSpot
			}})
			expectNoMigration()
		})
		It("should skip capacity-block instances", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) { i.InstanceLifecycle = ec2types.InstanceLifecycleTypeCapacityBlock }})
			expectNoMigration()
		})
		It("should skip NodeClaims that aren't initialized", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) {
				nc.StatusConditions().SetFalse(karpv1.ConditionTypeInitialized, "NotInitialized", "")
			}})
			expectNoMigration()
		})
		It("should skip nodes with the disrupted taint", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			_, node, _ := makeNode()
			node.Spec.Taints = append(node.Spec.Taints, karpv1.DisruptedNoScheduleTaint)
			ExpectApplied(ctx, env.Client, node)
			expectNoMigration()
		})
		It("should skip instances that aren't running", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) { i.State.Name = ec2types.InstanceStateNameStopping }})
			expectNoMigration()
		})
		It("should skip when the NodePool only allows on-demand", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nodePool.Spec.Template.Spec.Requirements = []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, nodePool)
			makeNode()
			expectNoMigration("nodepool_incompatible")
		})
		It("should skip when the NodePool requires another reservation", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, karpv1.NodeSelectorRequirementWithMinValues{
				Key: v1.LabelCapacityReservationID, Operator: corev1.NodeSelectorOpIn, Values: []string{"cr-other"}})
			ExpectApplied(ctx, env.Client, nodePool)
			makeNode()
			expectNoMigration()
		})
		DescribeTable("should skip incompatible reservations",
			func(mutate func(*ec2types.CapacityReservation), status func(*v1.CapacityReservation), reason string) {
				bad := reservation("cr-1", 5, mutate)
				setReservations([]ec2types.CapacityReservation{bad}, bad)
				if status != nil {
					status(&nodeClass.Status.CapacityReservations[0])
					ExpectApplied(ctx, env.Client, nodeClass)
				}
				makeNode()
				expectNoMigration(reason)
			},
			Entry("capacity block", func(r *ec2types.CapacityReservation) {
				r.ReservationType = ec2types.CapacityReservationTypeCapacityBlock
			}, nil, "no_compatible_cr"),
			Entry("interruptible", func(r *ec2types.CapacityReservation) { r.Interruptible = lo.ToPtr(true) }, nil, "no_compatible_cr"),
			Entry("other zone", func(r *ec2types.CapacityReservation) { r.AvailabilityZone = lo.ToPtr("test-zone-1b") }, nil, "no_compatible_cr"),
			Entry("other instance type", func(r *ec2types.CapacityReservation) { r.InstanceType = lo.ToPtr("m5.xlarge") }, nil, "no_compatible_cr"),
			Entry("other platform", func(r *ec2types.CapacityReservation) {
				r.InstancePlatform = ec2types.CapacityReservationInstancePlatformWindows
			}, nil, "no_compatible_cr"),
			Entry("dedicated tenancy", func(r *ec2types.CapacityReservation) { r.Tenancy = ec2types.CapacityReservationTenancyDedicated }, nil, "no_compatible_cr"),
			Entry("in a placement group", func(r *ec2types.CapacityReservation) {
				r.PlacementGroupArn = lo.ToPtr("arn:aws:ec2:test-zone-1:012345678901:placement-group/pg-1")
			}, nil, "no_compatible_cr"),
			Entry("on an Outpost", func(r *ec2types.CapacityReservation) {
				r.OutpostArn = lo.ToPtr("arn:aws:outposts:test-zone-1:012345678901:outpost/op-1")
			}, nil, "no_compatible_cr"),
			Entry("no free slot", func(r *ec2types.CapacityReservation) { r.AvailableInstanceCount = lo.ToPtr[int32](0) }, nil, "no_capacity"),
			Entry("missing from EC2", func(r *ec2types.CapacityReservation) { r.State = ec2types.CapacityReservationStateCancelled }, nil, "no_compatible_cr"),
			Entry("expiring", func(*ec2types.CapacityReservation) {}, func(s *v1.CapacityReservation) { s.State = v1.CapacityReservationStateExpiring }, "no_compatible_cr"),
		)
		It("should skip instances with an unknown platform", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) { i.PlatformDetails = nil }})
			expectNoMigration()
		})
	})

	Context("Pod compatibility", func() {
		var cr ec2types.CapacityReservation
		BeforeEach(func() {
			cr = reservation("cr-1", 5)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
		})
		DescribeTable("should check every pod's required node affinity against the reserved labels",
			func(pod func(nodeName string) *corev1.Pod, migrated bool) {
				_, node, _ := makeNode()
				ExpectApplied(ctx, env.Client, pod(node.Name))
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(lo.Ternary(migrated, 1, 0)))
				ExpectMetricGaugeValue(migration.MigrationIneligible, lo.Ternary(migrated, 0.0, 1.0), map[string]string{"reason": "pod_incompatible"})
			},
			Entry("no constraints", func(n string) *corev1.Pod { return coretest.Pod(coretest.PodOptions{NodeName: n}) }, true),
			Entry("nodeSelector on-demand", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand}})
			}, false),
			Entry("nodeSelector reserved in the target", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodeSelector: map[string]string{corecloudprovider.ReservationIDLabel: "cr-1"}})
			}, true),
			Entry("reservation-id DoesNotExist", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodeRequirements: []corev1.NodeSelectorRequirement{{Key: corecloudprovider.ReservationIDLabel, Operator: corev1.NodeSelectorOpDoesNotExist}}})
			}, false),
			Entry("reservation-id NotIn the target", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodeRequirements: []corev1.NodeSelectorRequirement{{Key: corecloudprovider.ReservationIDLabel, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"cr-1"}}}})
			}, false),
			Entry("reservation-type capacity-block", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodeSelector: map[string]string{v1.LabelCapacityReservationType: string(v1.CapacityReservationTypeCapacityBlock)}})
			}, false),
			Entry("a later ORed term allows reserved", func(n string) *corev1.Pod {
				p := coretest.Pod(coretest.PodOptions{NodeName: n})
				p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}},
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeReserved}}}},
				}}}}
				return p
			}, true),
			Entry("only preferring on-demand", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, NodePreferences: []corev1.NodeSelectorRequirement{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}})
			}, true),
			// The DaemonSet controller would delete it once the node is relabeled.
			Entry("a DaemonSet pod requiring on-demand", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{
					ObjectMeta:   metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", UID: "ds-uid", Controller: lo.ToPtr(true)}}},
					NodeName:     n,
					NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand},
				})
			}, false),
			Entry("a static pod requiring on-demand", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{
					ObjectMeta:   metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: n, UID: "node-uid", Controller: lo.ToPtr(true)}}},
					NodeName:     n,
					NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand},
				})
			}, true),
			Entry("a terminal pod requiring on-demand", func(n string) *corev1.Pod {
				return coretest.Pod(coretest.PodOptions{NodeName: n, Phase: corev1.PodSucceeded, NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand}})
			}, true),
		)
	})

	Context("Orphaned nodes", func() {
		It("should re-target a node whose reservation is gone and never pick the dead one", func() {
			live := reservation("cr-live", 2)
			setReservations([]ec2types.CapacityReservation{live}, live)
			nc, node, id := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-dead")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			Expect(*ec2Instance(id).CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-live"))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-live")
		})
		It("should not touch a node whose targeted reservation is still active", func() {
			live, other := reservation("cr-live", 2), reservation("cr-other", 2)
			setReservations([]ec2types.CapacityReservation{live, other}, other)
			makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-live")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
		})
		It("should migrate orphaned nodes before on-demand nodes", func() {
			cr := reservation("cr-1", 1)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			makeNode()
			_, _, orphan := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-dead")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			Expect(*awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.Pop().InstanceId).To(Equal(orphan))
		})
		It("should re-target a pending node whose target was canceled", func() {
			cr1, cr2 := reservation("cr-1", 1), reservation("cr-2", 1)
			setReservations([]ec2types.CapacityReservation{cr1}, cr1)
			nc, _, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))

			setReservations([]ec2types.CapacityReservation{cr2}, cr2)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			Expect(*ec2Instance(id).CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-2"))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-2"))
		})
	})

	Context("Idempotency", func() {
		var cr ec2types.CapacityReservation
		BeforeEach(func() {
			cr = reservation("cr-1", 2)
		})
		It("should re-issue when the annotation exists but the instance was never re-targeted", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, _ := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) {
				nc.Annotations = map[string]string{v1.AnnotationCapacityReservationMigrationTarget: "cr-1", v1.AnnotationCapacityReservationMigrationRequestedAt: awsEnv.Clock.Now().UTC().Format(time.RFC3339)}
			}})
			_, err := awsEnv.CapacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: "cr-1"})
			Expect(err).ToNot(HaveOccurred())
			// Within the grace window the missing target may be a stale read, so the slot is held and nothing is re-issued.
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			awsEnv.Clock.Step(6 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
		})
		It("should treat an empty annotation as untracked", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, _, _ := makeNode(nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) {
				nc.Annotations = map[string]string{v1.AnnotationCapacityReservationMigrationTarget: ""}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			Expect(recorder.Calls(migration.EventReasonNotRelabeled)).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
		})
		It("should follow the instance when its target differs from the annotation", func() {
			other := reservation("cr-2", 2)
			setReservations([]ec2types.CapacityReservation{cr, other}, cr, other)
			nc, _, _ := makeNode(
				nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) {
					nc.Annotations = map[string]string{v1.AnnotationCapacityReservationMigrationTarget: "cr-1"}
				}},
				nodeOpts{instance: func(i *ec2types.Instance) {
					i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
						CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-2")},
					}
				}},
			)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-2"))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-2")).To(Equal(1))
		})
		It("should adopt an instance re-targeted without an annotation", func() {
			setReservations([]ec2types.CapacityReservation{cr}, cr)
			nc, node, id := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-1")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
		})
	})

	Context("Adoption", func() {
		It("should not adopt an instance targeting a reservation it wouldn't choose", func() {
			block := reservation("cr-block", 2, func(r *ec2types.CapacityReservation) {
				r.ReservationType = ec2types.CapacityReservationTypeCapacityBlock
			})
			setReservations([]ec2types.CapacityReservation{block}, block)
			nc, _, _ := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-block")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
		})
		It("should not adopt an instance targeting a reservation outside the EC2NodeClass", func() {
			outside, inside := reservation("cr-outside", 2), reservation("cr-inside", 2)
			setReservations([]ec2types.CapacityReservation{outside, inside}, inside)
			nc, _, _ := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-outside")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
		})
	})

	Context("Adoption checks", func() {
		var cr ec2types.CapacityReservation
		targeted := func(id string) nodeOpts {
			return nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr(id)},
				}
			}}
		}
		BeforeEach(func() {
			cr = reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
		})
		It("should adopt an orphaned capacity-reservations-only instance re-targeted without an annotation", func() {
			nc, node, id := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
				i.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
					CapacityReservationPreference: ec2types.CapacityReservationPreferenceCapacityReservationsOnly,
					CapacityReservationTarget:     &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-1")},
				}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
		})
		It("should not adopt when the NodePool doesn't allow reserved, but still hold the slot", func() {
			nodePool.Spec.Template.Spec.Requirements = []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, nodePool)
			nc, node, id := makeNode(targeted("cr-1"))
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectOnDemand(nc, node)
		})
		It("should not adopt when a pod requires on-demand", func() {
			nc, node, id := makeNode(targeted("cr-1"))
			ExpectApplied(ctx, env.Client, coretest.Pod(coretest.PodOptions{NodeName: node.Name, NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand}}))
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			expectOnDemand(nc, node)
		})
		It("should not follow an out-of-band target the NodePool doesn't allow", func() {
			other := reservation("cr-2", 2)
			setReservations([]ec2types.CapacityReservation{cr, other}, cr, other)
			nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, karpv1.NodeSelectorRequirementWithMinValues{
				Key: v1.LabelCapacityReservationID, Operator: corev1.NodeSelectorOpIn, Values: []string{"cr-1"}})
			ExpectApplied(ctx, env.Client, nodePool)
			nc, _, _ := makeNode(targeted("cr-2"), nodeOpts{nodeClaim: func(nc *karpv1.NodeClaim) {
				nc.Annotations = map[string]string{v1.AnnotationCapacityReservationMigrationTarget: "cr-1"}
			}})
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-2")).To(Equal(1))
		})
	})

	Context("Label flip checks", func() {
		var cr ec2types.CapacityReservation
		BeforeEach(func() {
			cr = reservation("cr-1", 2)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
		})
		It("should not relabel when the target left the EC2NodeClass status while pending, until it returns", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			other := reservation("cr-2", 2)
			setReservations([]ec2types.CapacityReservation{cr, other}, other)
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectOnDemand(nc, node)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			Expect(recorder.Calls(migration.EventReasonNotRelabeled)).To(Equal(1))
			// The covered instance is counted until EC2's next sync includes it.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))

			setReservations([]ec2types.CapacityReservation{cr, other}, cr, other)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			Expect(modifyCalls()).To(Equal(1))
		})
		It("should still relabel, with a warning, when the NodePool stopped allowing reserved while pending", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			nodePool.Spec.Template.Spec.Requirements = []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, nodePool)
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).To(HaveKeyWithValue(v1.AnnotationCapacityReservationMigrationTarget, "cr-1"))
			Expect(recorder.Calls(migration.EventReasonIncompatible)).To(Equal(1))
			Expect(recorder.DetectedEvent("Instance is covered by capacity reservation cr-1 and relabeled reserved, but its NodePool no longer allows it; the node may drift or lose pods")).To(BeTrue())
			Expect(recorder.Calls(migration.EventReasonCompleted)).To(Equal(1))
			// Delete returns the slot to the reservation EC2 actually used.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		It("should still relabel, with a warning, when a pod requiring on-demand landed while pending", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			pod := coretest.Pod(coretest.PodOptions{NodeName: node.Name, NodeSelector: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand}})
			ExpectApplied(ctx, env.Client, pod)
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			Expect(recorder.Calls(migration.EventReasonIncompatible)).To(Equal(1))
			Expect(recorder.DetectedEvent("Instance is covered by capacity reservation cr-1 and relabeled reserved, but a pod on it requires other labels; the node may drift or lose pods")).To(BeTrue())
		})
		It("should say the NodeClaim has no NodePool, rather than that it is gone, when relabeling one without the label", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			nc = ExpectExists(ctx, env.Client, nc)
			delete(nc.Labels, karpv1.NodePoolLabelKey)
			ExpectApplied(ctx, env.Client, nc)
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			Expect(recorder.DetectedEvent("Instance is covered by capacity reservation cr-1 and relabeled reserved, but it has no NodePool; the node may drift or lose pods")).To(BeTrue())
		})
		It("should converge labels when the migration gate is turned off while a migration is pending", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			// A restart with the gate off drops the in-memory ledger and stops the migration controller.
			awsEnv.CapacityReservationProvider.SetPendingMigrations(nil)
			gateOff := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacityMigration: lo.ToPtr(false)}}))
			cover(id)
			ExpectSingletonReconciled(gateOff, capacityTypeController)
			expectReserved(nc, node, "cr-1")
			// The slot stays held until EC2's next sync counts the instance.
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(1))
		})
		DescribeTable("should relabel a finished migration again after a stale snapshot demotes it",
			func(gate bool) {
				gctx := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacityMigration: lo.ToPtr(gate)}}))
				finish := func() {
					if gate {
						ExpectSingletonReconciled(ctx, controller)
					} else {
						ExpectSingletonReconciled(gctx, capacityTypeController)
					}
				}
				nc, node, id := makeNode()
				ExpectSingletonReconciled(ctx, controller)
				cover(id)
				finish()
				expectReserved(nc, node, "cr-1")
				// A lagging DescribeInstances briefly shows the instance uncovered.
				inst := ec2Instance(id)
				inst.CapacityReservationId = nil
				awsEnv.EC2API.Instances.Store(id, inst)
				ExpectSingletonReconciled(gctx, capacityTypeController)
				expectOnDemand(nc, node)
				cover(id)
				finish()
				expectReserved(nc, node, "cr-1")
				Expect(modifyCalls()).To(Equal(1))
			},
			Entry("by the migration controller", true),
			Entry("by the capacitytype controller with the gate off", false),
		)
		It("should not promote coverage by a reservation the EC2NodeClass doesn't select", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			other := reservation("cr-2", 2)
			setReservations([]ec2types.CapacityReservation{cr, other}, other)
			cover(id)
			ExpectSingletonReconciled(ctx, capacityTypeController)
			expectOnDemand(nc, node)
		})
		It("should relabel every Node for a NodeClaim with duplicate Nodes", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			dup := coretest.NodeClaimLinkedNode(nc)
			ExpectApplied(ctx, env.Client, dup)
			cover(id)
			ExpectSingletonReconciled(ctx, controller)
			expectReserved(nc, node, "cr-1")
			expectReserved(nc, dup, "cr-1")
		})
		It("should keep capacitytype from demoting a flip made after its instance snapshot", func() {
			nc, node, id := makeNode()
			ExpectSingletonReconciled(ctx, controller)
			ct := capacitytype.NewController(env.Client, listThenRun{CloudProvider: cloudProvider, hook: func() {
				cover(id)
				// Completes the migration in between, Node first as complete() does.
				ExpectSingletonReconciled(ctx, controller)
			}}, awsEnv.CapacityReservationProvider)
			ExpectSingletonReconciled(ctx, ct)
			expectReserved(nc, node, "cr-1")
			ExpectSingletonReconciled(ctx, capacityTypeController)
			expectReserved(nc, node, "cr-1")
		})
	})

	Context("EC2 errors", func() {
		var cr ec2types.CapacityReservation
		BeforeEach(func() {
			cr = reservation("cr-1", 5)
			setReservations([]ec2types.CapacityReservation{cr}, cr)
		})
		It("should remove the annotation and back off on a client error", func() {
			nc, node, _ := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "InvalidParameterCombination", Message: "not allowed", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.FailedCalls()).To(Equal(1))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			Expect(recorder.Calls(migration.EventReasonFailed)).To(Equal(1))
			expectOnDemand(nc, node)
			Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(5))

			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			awsEnv.Clock.Step(6 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(1))
		})
		It("should count a client error as client_error", func() {
			makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "InvalidParameterCombination"}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			ExpectMetricCounterValue(migration.MigrationAttemptsTotal, 1, map[string]string{"result": "client_error", "case": "on_demand"})
		})
		It("should treat an expired request signature as a definite rejection", func() {
			nc, _, _ := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "RequestExpired", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
			ExpectMetricCounterValue(migration.MigrationAttemptsTotal, 1, map[string]string{"result": "client_error", "case": "on_demand"})
		})
		DescribeTable("should keep the annotation and hold the slot when the outcome is unknown",
			func(err error, applied bool) {
				cr = reservation("cr-1", 1)
				setReservations([]ec2types.CapacityReservation{cr}, cr)
				makeNode()
				makeNode()
				awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(err, fake.MaxCalls(1))
				ExpectSingletonReconciled(ctx, controller)
				// The pass ends after the ambiguous call, so the second node isn't tried.
				Expect(modifyCalls()).To(Equal(1))
				ExpectMetricCounterValue(migration.MigrationAttemptsTotal, 1, map[string]string{"result": "error", "case": "on_demand"})
				ncs := &karpv1.NodeClaimList{}
				Expect(env.Client.List(ctx, ncs)).To(Succeed())
				tried := lo.Filter(ncs.Items, func(nc karpv1.NodeClaim, _ int) bool {
					return nc.Annotations[v1.AnnotationCapacityReservationMigrationTarget] == "cr-1"
				})
				Expect(tried).To(HaveLen(1))
				Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(0))
				id := lo.Must(utils.ParseInstanceID(tried[0].Status.ProviderID))
				if applied {
					// EC2 applied it; DescribeInstances shows it a little later.
					inst := ec2Instance(id)
					inst.CapacityReservationSpecification = &ec2types.CapacityReservationSpecificationResponse{
						CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr("cr-1")},
					}
					awsEnv.EC2API.Instances.Store(id, inst)
				}

				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(1))
				awsEnv.Clock.Step(6 * time.Minute)
				ExpectSingletonReconciled(ctx, controller)
				// Applied: tracked as pending with no second call. Not applied: cleared and re-issued once.
				Expect(modifyCalls()).To(Equal(lo.Ternary(applied, 1, 2)))
				Expect(awsEnv.CapacityReservationProvider.GetAvailableInstanceCount("cr-1")).To(Equal(0))
				Expect(*ec2Instance(id).CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-1"))
			},
			Entry("timeout that was applied", errors.New("context deadline exceeded"), true),
			Entry("timeout that was not applied", errors.New("context deadline exceeded"), false),
			Entry("EC2 InternalError that was applied", &smithy.GenericAPIError{Code: "InternalError"}, true),
			Entry("EC2 Unavailable that was not applied", &smithy.GenericAPIError{Code: "Unavailable"}, false),
		)
		It("should not map a missing reservation to a missing NodeClaim", func() {
			nc, _, _ := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "InvalidCapacityReservationId.NotFound", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(recorder.Calls(migration.EventReasonFailed)).To(Equal(1))
			ExpectExists(ctx, env.Client, nc)
		})
		It("should end the pass early on throttling without backing off", func() {
			makeNode()
			makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "RequestLimitExceeded", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.FailedCalls()).To(Equal(1))
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(0))
			// Neither is backed off, so the next pass migrates both.
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(2))
		})
		It("should back off a reservation once two instances are denied", func() {
			for range 3 {
				makeNode()
			}
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}, fake.MaxCalls(2))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			ExpectMetricCounterValue(migration.MigrationAttemptsTotal, 2, map[string]string{"result": "unauthorized", "case": "on_demand"})
			ExpectMetricGaugeValue(migration.MigrationIneligible, 1, map[string]string{"reason": "unauthorized"})
			awsEnv.Clock.Step(time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			ExpectMetricGaugeValue(migration.MigrationIneligible, 3, map[string]string{"reason": "unauthorized"})
			awsEnv.Clock.Step(5 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(3))
		})
		It("should keep migrating other instances into a reservation one instance is denied", func() {
			nodes := lo.Times(3, func(int) string { _, _, id := makeNode(); return id })
			order := migrationOrder()
			Expect(order).To(ConsistOf(nodes))
			calls := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, deniedInstances{awsEnv.InstanceProvider, sets.New(order[0]), &calls}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			ExpectSingletonReconciled(ctx, c)
			Expect(calls).To(Equal(1))
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(2))
			Expect(ec2Instance(order[1]).CapacityReservationSpecification.CapacityReservationTarget).ToNot(BeNil())
			Expect(ec2Instance(order[2]).CapacityReservationSpecification.CapacityReservationTarget).ToNot(BeNil())
			// Only the denied instance backs off.
			awsEnv.Clock.Step(time.Minute)
			ExpectSingletonReconciled(ctx, c)
			Expect(calls).To(Equal(1))
			awsEnv.Clock.Step(5 * time.Minute)
			ExpectSingletonReconciled(ctx, c)
			Expect(calls).To(Equal(2))
		})
		It("should still migrate an allowed instance behind instances that keep being denied", func() {
			for range 3 {
				makeNode()
			}
			order := migrationOrder()
			calls := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, deniedInstances{awsEnv.InstanceProvider, sets.New(order[0], order[1]), &calls}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			for range 15 {
				ExpectSingletonReconciled(ctx, c)
				awsEnv.Clock.Step(time.Minute)
			}
			Expect(ec2Instance(order[2]).CapacityReservationSpecification.CapacityReservationTarget).ToNot(BeNil())
			Expect(calls).To(Equal(4))
		})
		It("should keep using other reservations while one is unauthorized", func() {
			denied, allowed := reservation("cr-denied", 5), reservation("cr-allowed", 2)
			setReservations([]ec2types.CapacityReservation{denied, allowed}, denied, allowed)
			makeNode()
			makeNode()
			deniedCalls := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, deniedTarget{awsEnv.InstanceProvider, "cr-denied", &deniedCalls}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			// Both instances are denied before the reservation backs off, then both move into the other one.
			ExpectSingletonReconciled(ctx, c)
			Expect(deniedCalls).To(Equal(2))
			Expect(modifyCalls()).To(Equal(0))
			awsEnv.Clock.Step(time.Minute)
			ExpectSingletonReconciled(ctx, c)
			Expect(deniedCalls).To(Equal(2))
			Expect(modifyCalls()).To(Equal(2))
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.CalledWithInput.ForEach(func(in *ec2.ModifyInstanceCapacityReservationAttributesInput) {
				Expect(*in.CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId).To(Equal("cr-allowed"))
			})
		})
		It("should back off a denied instance exponentially up to an hour", func() {
			makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}, fake.MaxCalls(6))
			for i, delay := range []time.Duration{5, 10, 20, 40, 60, 60} {
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(i + 1))
				awsEnv.Clock.Step((delay - 1) * time.Minute)
				ExpectSingletonReconciled(ctx, controller)
				Expect(modifyCalls()).To(Equal(i + 1))
				awsEnv.Clock.Step(2 * time.Minute)
			}
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(1))

			makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(8))
			awsEnv.Clock.Step(4 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(8))
			awsEnv.Clock.Step(2 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(2))
		})
		It("should skip a node whose Modify was never attempted, without a failure or attempt", func() {
			makeNode()
			makeNode()
			failed := false
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, notAttemptedOnce{awsEnv.InstanceProvider, &failed}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			ExpectSingletonReconciled(ctx, c)
			Expect(failed).To(BeTrue())
			Expect(modifyCalls()).To(Equal(1))
			Expect(recorder.Calls(migration.EventReasonFailed)).To(Equal(0))
			ExpectMetricCounterValue(migration.MigrationAttemptsTotal, 1, map[string]string{"result": "success", "case": "on_demand"})
			_, found := FindMetricWithLabelValues("karpenter_capacity_reservation_migration_attempts_total", map[string]string{"result": "error"})
			Expect(found).To(BeFalse())
			ncs := &karpv1.NodeClaimList{}
			Expect(env.Client.List(ctx, ncs)).To(Succeed())
			Expect(lo.CountBy(ncs.Items, func(nc karpv1.NodeClaim) bool {
				_, ok := nc.Annotations[v1.AnnotationCapacityReservationMigrationTarget]
				return ok
			})).To(Equal(1))
		})
		It("should end the pass when a Modify was never attempted because of throttling", func() {
			makeNode()
			makeNode()
			calls := 0
			c := migration.NewController(awsEnv.Clock, env.Client, cloudProvider, throttledBeforeModify{awsEnv.InstanceProvider, &calls}, awsEnv.CapacityReservationProvider, awsEnv.ZonalShiftProvider, recorder)
			ExpectSingletonReconciled(ctx, c)
			Expect(calls).To(Equal(1))
			ncs := &karpv1.NodeClaimList{}
			Expect(env.Client.List(ctx, ncs)).To(Succeed())
			Expect(lo.CountBy(ncs.Items, func(nc karpv1.NodeClaim) bool {
				_, ok := nc.Annotations[v1.AnnotationCapacityReservationMigrationTarget]
				return ok
			})).To(Equal(0))
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(2))
		})
		It("should forget an instance's unauthorized backoff once it has been idle past the cap", func() {
			_, node, _ := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "UnauthorizedOperation", Fault: smithy.FaultClient}, fake.MaxCalls(2))
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			// The node is ineligible for over an hour after its 5m backoff ends.
			node = ExpectExists(ctx, env.Client, node)
			node.Spec.Taints = append(node.Spec.Taints, karpv1.DisruptedNoScheduleTaint)
			ExpectApplied(ctx, env.Client, node)
			awsEnv.Clock.Step(66 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(1))
			node.Spec.Taints = nil
			ExpectApplied(ctx, env.Client, node)
			ExpectSingletonReconciled(ctx, controller)
			Expect(modifyCalls()).To(Equal(2))
			// The second denial starts over at 5m instead of doubling to 10m.
			awsEnv.Clock.Step(6 * time.Minute)
			ExpectSingletonReconciled(ctx, controller)
			Expect(awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.SuccessfulCalls()).To(Equal(1))
		})
		It("should skip a node whose instance is gone", func() {
			nc, _, _ := makeNode()
			awsEnv.EC2API.ModifyInstanceCapacityReservationAttributesBehavior.Error.Set(&smithy.GenericAPIError{Code: "InvalidInstanceID.NotFound", Fault: smithy.FaultClient}, fake.MaxCalls(1))
			ExpectSingletonReconciled(ctx, controller)
			Expect(recorder.Calls(migration.EventReasonFailed)).To(Equal(0))
			Expect(ExpectExists(ctx, env.Client, nc).Annotations).ToNot(HaveKey(v1.AnnotationCapacityReservationMigrationTarget))
		})
	})
	It("should not drift a reserved NodeClaim InstanceTypeNotFound when its reservation leaves the EC2NodeClass before demotion", func() {
		nodePool.Spec.Template.Spec.NodeClassRef = &karpv1.NodeClassReference{Group: object.GVK(nodeClass).Group, Kind: object.GVK(nodeClass).Kind, Name: nodeClass.Name}
		ExpectApplied(ctx, env.Client, nodePool)
		Expect(awsEnv.InstanceTypesProvider.UpdateInstanceTypes(ctx)).To(Succeed())
		Expect(awsEnv.InstanceTypesProvider.UpdateInstanceTypeOfferings(ctx)).To(Succeed())
		nodeClass.Spec.CapacityReservationSelectorTerms = nil
		nodeClass.Status.CapacityReservations = nil
		ExpectApplied(ctx, env.Client, nodeClass)
		nc, _, _ := makeNode(nodeOpts{instance: func(i *ec2types.Instance) {
			i.SubnetId = lo.ToPtr("subnet-test1")
			i.SecurityGroups = []ec2types.GroupIdentifier{{GroupId: lo.ToPtr("sg-test1")}, {GroupId: lo.ToPtr("sg-test2")}, {GroupId: lo.ToPtr("sg-test3")}}
		}, nodeClaim: func(nc *karpv1.NodeClaim) {
			nc.Status.ImageID = "ami-test1"
			nc.Labels = lo.Assign(nc.Labels, map[string]string{
				karpv1.CapacityTypeLabelKey:              karpv1.CapacityTypeReserved,
				corecloudprovider.ReservationIDLabel:     "cr-1",
				v1.LabelCapacityReservationType:          string(v1.CapacityReservationTypeDefault),
				v1.LabelCapacityReservationInterruptible: "false",
			})
		}})
		awsEnv.Clock.Step(2 * time.Hour)
		_, err := disruption.NewController(awsEnv.Clock, env.Client, cloudProvider).Reconcile(ctx, ExpectExists(ctx, env.Client, nc))
		Expect(err).ToNot(HaveOccurred())
		drifted := ExpectExists(ctx, env.Client, nc).StatusConditions().Get(karpv1.ConditionTypeDrifted)
		if drifted != nil {
			Expect(drifted.Reason).ToNot(Equal("InstanceTypeNotFound"))
		}
	})
})
