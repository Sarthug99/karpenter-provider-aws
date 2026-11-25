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
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	"github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/arczonalshift"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instance"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

const (
	maxModifyCallsPerPass = 10
	conflictRequeue       = 5 * time.Second
	stallThreshold        = 30 * time.Minute
	// Coverage can take up to stallThreshold, so a reservation ending sooner than this would barely hold the node.
	minReservationRemaining = time.Hour
	initialBackoff          = 5 * time.Minute
	maxBackoff              = time.Hour
	// DescribeInstances can lag a Modify, so a missing target isn't trusted until this long after the call.
	modifyVisibilityGrace = 5 * time.Minute
)

// serverErrorCodes are EC2 faults after which a Modify may still have been applied.
var serverErrorCodes = sets.New("InternalError", "InternalFailure", "ServiceUnavailable", "Unavailable", "RequestTimeout")

// Enabled reports whether the controller should run; it needs core ReservedCapacity for reserved offerings and labels.
func Enabled(ctx context.Context) bool {
	return options.FromContext(ctx).FeatureGates.ReservedCapacityMigration && coreoptions.FromContext(ctx).FeatureGates.ReservedCapacity
}

type backoffEntry struct {
	next  time.Time
	delay time.Duration
}

// Controller moves running on-demand or orphaned nodes into a capacity reservation their EC2NodeClass selects, in place.
type Controller struct {
	clk              clock.Clock
	kubeClient       client.Client
	cloudProvider    cloudprovider.CloudProvider
	instanceProvider instance.Provider
	crProvider       capacityreservation.Provider
	zonalShift       arczonalshift.Provider
	recorder         events.Recorder

	backoff      map[string]backoffEntry
	cases        map[string]string           // instance ID -> migration case, for metrics only
	denied       map[slot]backoffEntry       // instance in a reservation -> backoff after EC2 denied that move
	deniedBy     map[string]sets.Set[string] // reservation ID -> instances denied since its last backoff or success
	unauthorized map[string]time.Time        // reservation ID -> end of its backoff
	covered      sets.Set[slot]              // instances marked migrated in a reservation without completing
	uncovered    map[string]time.Time        // instance ID -> start of the last pass that saw its target uncovered
}

// slot is an instance in a reservation.
type slot struct {
	instanceID    string
	reservationID string
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, instanceProvider instance.Provider,
	crProvider capacityreservation.Provider, zonalShift arczonalshift.Provider, recorder events.Recorder) *Controller {
	return &Controller{
		clk:              clk,
		kubeClient:       kubeClient,
		cloudProvider:    cloudProvider,
		instanceProvider: instanceProvider,
		crProvider:       crProvider,
		zonalShift:       zonalShift,
		recorder:         recorder,
		backoff:          map[string]backoffEntry{},
		cases:            map[string]string{},
		denied:           map[slot]backoffEntry{},
		deniedBy:         map[string]sets.Set[string]{},
		unauthorized:     map[string]time.Time{},
		covered:          sets.New[slot](),
		uncovered:        map[string]time.Time{},
	}
}

func (*Controller) Name() string {
	return "capacityreservation.migration"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

// pass memoizes API reads for one reconcile.
type pass struct {
	start       time.Time
	nodeClasses map[string]*v1.EC2NodeClass
	nodePools   map[string]*karpv1.NodePool
	raw         map[string]map[string]*ec2types.CapacityReservation // nodeclass name -> reservation id -> reservation
	active      map[string]bool
	failed      map[string]error // reservation query -> error, so a failing query isn't repeated within the pass
	pending     map[string]sets.Set[string]
	launching   map[string]int // reservation ID -> reserved NodeClaims not yet launched
	ineligible  map[string]int
	stalled     int
}

type candidate struct {
	nodeClaim    *karpv1.NodeClaim
	instance     *instance.Instance
	caseLabel    string
	reservations []string
	endTimes     map[string]time.Time // reservation ID -> end, zero if it never ends
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	nodeClaims, err := nodeclaimutils.ListManaged(ctx, c.kubeClient, c.cloudProvider)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing nodeclaims")
		return reconciler.Result{RequeueAfter: time.Minute}, nil
	}
	idle, err := c.idle(ctx, nodeClaims)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing ec2nodeclasses")
		return reconciler.Result{RequeueAfter: time.Minute}, nil
	}
	if idle {
		// Skip the DescribeInstances sweep, which would find nothing to move or hold.
		c.crProvider.SetPendingMigrations(nil)
		c.publishGauges(&pass{})
		return reconciler.Result{RequeueAfter: time.Minute}, nil
	}
	start := c.clk.Now()
	instances, err := c.instanceProvider.List(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing instances")
		return reconciler.Result{RequeueAfter: time.Minute}, nil
	}
	byID := lo.KeyBy(instances, func(i *instance.Instance) string { return i.ID })
	p := &pass{
		start:       start,
		nodeClasses: map[string]*v1.EC2NodeClass{},
		nodePools:   map[string]*karpv1.NodePool{},
		raw:         map[string]map[string]*ec2types.CapacityReservation{},
		active:      map[string]bool{},
		failed:      map[string]error{},
		pending:     map[string]sets.Set[string]{},
		launching:   map[string]int{},
		ineligible:  map[string]int{},
	}
	c.prune(byID)

	var errs []error
	var unclaimed []*candidate
	for _, nc := range nodeClaims {
		id, err := utils.ParseInstanceID(nc.Status.ProviderID)
		if err != nil {
			p.holdLaunch(nc)
			continue
		}
		inst, ok := byID[id]
		target := trackedTarget(nc)
		if ok && target != "" && inst.CapacityReservationState.CoveringID != target {
			c.uncovered[id] = p.start
		}
		// A draining or briefly unlisted instance keeps its EC2 target, so its slot stays held.
		if !nc.DeletionTimestamp.IsZero() {
			if ok {
				c.holdOrCovered(p, target, inst)
			}
			continue
		}
		if !ok {
			p.hold(target, id)
			continue
		}
		free, err := c.reconcileTracked(ctx, p, nc, inst)
		if err != nil {
			errs = append(errs, serrors.Wrap(err, "NodeClaim", klog.KObj(nc)))
			// Stay conservative: an error must not release a slot this instance may hold.
			c.holdOrCovered(p, target, inst)
			c.holdOrCovered(p, inst.CapacityReservationState.TargetID, inst)
			continue
		}
		if free {
			unclaimed = append(unclaimed, &candidate{nodeClaim: nc, instance: inst})
		}
	}
	// Publish the ledger before selection so the scheduler and our budget both see in-flight migrations.
	c.crProvider.SetPendingMigrations(p.pending)

	var candidates []*candidate
	for _, u := range unclaimed {
		cand, err := c.eligible(ctx, p, u.nodeClaim, u.instance)
		if err != nil {
			errs = append(errs, serrors.Wrap(err, "NodeClaim", klog.KObj(u.nodeClaim)))
			continue
		}
		if cand != nil {
			candidates = append(candidates, cand)
		}
	}
	// Orphaned nodes first since they're at drift risk, then oldest first.
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].caseLabel != candidates[j].caseLabel {
			return candidates[i].caseLabel == caseOrphaned
		}
		ti, tj := candidates[i].nodeClaim.CreationTimestamp, candidates[j].nodeClaim.CreationTimestamp
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		return candidates[i].nodeClaim.Name < candidates[j].nodeClaim.Name
	})
	calls, migrateErrs := c.migrate(ctx, p, candidates)
	errs = lo.Compact(append(errs, migrateErrs...))
	c.publishGauges(p)

	for _, err := range errs {
		log.FromContext(ctx).Error(err, "failed reconciling capacity reservation migration")
	}
	// Errors keep the regular cadence; a conflict-only pass with no Modify calls retries soon without a fresh call budget.
	if len(errs) != 0 && calls == 0 && lo.EveryBy(errs, func(err error) bool { return errors.IsConflict(err) }) {
		return reconciler.Result{RequeueAfter: conflictRequeue}, nil
	}
	return reconciler.Result{RequeueAfter: time.Minute}, nil
}

// idle reports whether there is nothing to move or hold: no tracked migration and no EC2NodeClass with a reservation.
func (c *Controller) idle(ctx context.Context, nodeClaims []*karpv1.NodeClaim) (bool, error) {
	if lo.SomeBy(nodeClaims, func(nc *karpv1.NodeClaim) bool { return trackedTarget(nc) != "" }) {
		return false, nil
	}
	nodeClasses := &v1.EC2NodeClassList{}
	if err := c.kubeClient.List(ctx, nodeClasses); err != nil {
		return false, err
	}
	return lo.NoneBy(nodeClasses.Items, func(nodeClass v1.EC2NodeClass) bool { return len(nodeClass.Status.CapacityReservations) != 0 }), nil
}

// reconcileTracked advances a NodeClaim that already has a target, and reports whether it is free for a new migration.
//
//nolint:gocyclo
func (c *Controller) reconcileTracked(ctx context.Context, p *pass, nc *karpv1.NodeClaim, inst *instance.Instance) (bool, error) {
	state := inst.CapacityReservationState
	target := trackedTarget(nc)
	if target == "" {
		// Adopt an instance re-targeted but not yet recorded, e.g. a Modify that timed out client-side but succeeded.
		if nc.Labels[karpv1.CapacityTypeLabelKey] != karpv1.CapacityTypeOnDemand || state.TargetID == "" {
			return true, nil
		}
		if state.CoveringID != "" && state.CoveringID != state.TargetID {
			return true, nil
		}
		ok, err := c.relabelable(ctx, p, nc, state.TargetID)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, c.holdUncovered(ctx, p, nc, inst)
		}
		if err := c.setTarget(ctx, nc, state.TargetID); err != nil {
			return false, err
		}
		target = state.TargetID
	}
	if state.TargetID != target {
		if c.withinGrace(nc) {
			p.hold(target, inst.ID)
			return false, nil
		}
		if state.TargetID == "" {
			// The Modify call was never made or failed, so the instance is free to be re-evaluated.
			return true, c.clearTarget(ctx, nc)
		}
		// The instance is the source of truth for out-of-band changes, but only reservations we'd pick get our labels.
		ok, err := c.relabelable(ctx, p, nc, state.TargetID)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, multierr.Combine(c.holdUncovered(ctx, p, nc, inst), c.clearTarget(ctx, nc))
		}
		if err := c.setTarget(ctx, nc, state.TargetID); err != nil {
			return false, err
		}
		target = state.TargetID
	}
	if state.CoveringID == target {
		return false, c.complete(ctx, p, nc, inst, target)
	}
	active, err := c.isActive(ctx, p, nc, target)
	if err != nil {
		return false, err
	}
	if !active {
		// The target died while pending; this is now the orphaned case.
		return true, c.clearTarget(ctx, nc)
	}
	raw, ok := nc.Annotations[v1.AnnotationCapacityReservationMigrationRequestedAt]
	if !ok {
		// Only a completed migration's marker lacks requested-at, and its slot is already in EC2's count.
		return false, nil
	}
	if requestedAt, err := time.Parse(time.RFC3339, raw); err == nil {
		if pendingFor := c.clk.Since(requestedAt); pendingFor > stallThreshold {
			// EC2 covers a re-target as soon as a slot is free, so a hold this old only keeps a slot from launches.
			p.stalled++
			c.recorder.Publish(stalledEvent(nc, target, pendingFor))
			return false, nil
		}
	}
	p.hold(target, inst.ID)
	return false, nil
}

// trackedTarget returns the NodeClaim's migration target unless it is a completed migration's marker that still matches its labels.
func trackedTarget(nc *karpv1.NodeClaim) string {
	target := nc.Annotations[v1.AnnotationCapacityReservationMigrationTarget]
	if nc.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeReserved && nc.Labels[cloudprovider.ReservationIDLabel] == target {
		return ""
	}
	return target
}

// holdUncovered holds a slot for an instance we won't relabel whose live target will still cover it once a slot frees.
func (c *Controller) holdUncovered(ctx context.Context, p *pass, nc *karpv1.NodeClaim, inst *instance.Instance) error {
	state := inst.CapacityReservationState
	if state.TargetID == "" || state.CoveringID != "" {
		return nil
	}
	active, err := c.isActive(ctx, p, nc, state.TargetID)
	if active {
		p.hold(state.TargetID, inst.ID)
	}
	return err
}

func (c *Controller) withinGrace(nc *karpv1.NodeClaim) bool {
	requestedAt, err := time.Parse(time.RFC3339, nc.Annotations[v1.AnnotationCapacityReservationMigrationRequestedAt])
	return err == nil && c.clk.Since(requestedAt) < modifyVisibilityGrace
}

// relabelable reports whether labeling the NodeClaim reserved in id would be accurate and break neither its NodePool nor its pods.
func (c *Controller) relabelable(ctx context.Context, p *pass, nc *karpv1.NodeClaim, id string) (bool, error) {
	nodeClass, err := c.nodeClass(ctx, p, nc)
	if err != nil || nodeClass == nil || !adoptable(nodeClass, id) {
		return false, err
	}
	if active, err := c.isActive(ctx, p, nc, id); err != nil || !active {
		return false, err
	}
	nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nc)
	if err != nil {
		return false, err
	}
	ids, _, err := c.filterCompatible(ctx, p, nc, nodes, []string{id})
	return len(ids) != 0, err
}

// complete relabels a covered node reserved. The Nodes go first so a failed NodeClaim patch retries both.
//
//nolint:gocyclo
func (c *Controller) complete(ctx context.Context, p *pass, nc *karpv1.NodeClaim, inst *instance.Instance, target string) error {
	nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nc)
	if err != nil {
		return err
	}
	nodeClass, err := c.nodeClass(ctx, p, nc)
	if err != nil {
		return err
	}
	if nodeClass == nil || !adoptable(nodeClass, target) {
		// Like an open ODCR match, an unselected reservation stays on-demand; adoption relabels it if it is selected again.
		requestedAt := nc.Annotations[v1.AnnotationCapacityReservationMigrationRequestedAt]
		if err := c.clearTarget(ctx, nc); err != nil {
			return err
		}
		if requestedAt != "" {
			c.crProvider.MarkMigrated(target, inst.ID, c.uncovered[inst.ID])
		}
		delete(c.cases, inst.ID)
		delete(c.uncovered, inst.ID)
		c.recorder.Publish(notRelabeledEvent(nc, target, lo.Ternary(nodeClass == nil, "the EC2NodeClass is gone", "the reservation is no longer selected by the EC2NodeClass")))
		return nil
	}
	// The move can't be undone, so the labels follow EC2 even if the NodePool or a pod now rejects them.
	ids, why, err := c.filterCompatible(ctx, p, nc, nodes, []string{target})
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		why = lo.Ternary(why == "" && nc.Labels[karpv1.NodePoolLabelKey] == "", whyNoNodePool, why)
		c.recorder.Publish(incompatibleEvent(nc, target, incompatibleReasons[why]))
	}
	labels := reservedLabels(target)
	for _, node := range nodes {
		if !node.DeletionTimestamp.IsZero() || node.Labels[karpv1.NodeRegisteredLabelKey] != "true" {
			continue
		}
		stored := node.DeepCopy()
		node.Labels = lo.Assign(node.Labels, labels)
		if err := c.kubeClient.Patch(ctx, node, client.MergeFrom(stored)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("patching node, %w", err)
		}
	}
	stored := nc.DeepCopy()
	nc.Labels = lo.Assign(nc.Labels, labels)
	requestedAt := nc.Annotations[v1.AnnotationCapacityReservationMigrationRequestedAt]
	// The target stays as a marker, so a demotion from a stale DescribeInstances is relabeled once coverage shows again.
	delete(nc.Annotations, v1.AnnotationCapacityReservationMigrationRequestedAt)
	if err := c.kubeClient.Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return client.IgnoreNotFound(fmt.Errorf("patching nodeclaim, %w", err))
	}
	if t, err := time.Parse(time.RFC3339, requestedAt); err == nil {
		// EC2's count now includes this instance, but our cache won't until its next sync.
		c.crProvider.MarkMigrated(target, inst.ID, c.uncovered[inst.ID])
		MigrationCoverageDurationSeconds.Observe(c.clk.Since(t).Seconds(), map[string]string{})
		MigrationCompletedTotal.Inc(map[string]string{caseLabel.Name: lo.ValueOr(c.cases, inst.ID, caseOnDemand)})
	}
	delete(c.cases, inst.ID)
	delete(c.uncovered, inst.ID)
	c.recorder.Publish(completedEvent(nc, target))
	log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc), "capacity-reservation-id", target).Info("migrated node into capacity reservation")
	return nil
}

const whyNoNodePool = "no_nodepool"

var incompatibleReasons = map[string]string{
	"":                         "its NodePool is gone",
	whyNoNodePool:              "it has no NodePool",
	reasonNodePoolIncompatible: "its NodePool no longer allows it",
	reasonPodIncompatible:      "a pod on it requires other labels",
}

// adoptable reports whether id is a reservation the controller could have chosen, so the labels it writes are accurate.
func adoptable(nodeClass *v1.EC2NodeClass, id string) bool {
	return lo.ContainsBy(nodeClass.Status.CapacityReservations, func(cr v1.CapacityReservation) bool {
		return cr.ID == id && cr.ReservationType == v1.CapacityReservationTypeDefault && !cr.Interruptible
	})
}

func reservedLabels(reservationID string) map[string]string {
	return map[string]string{
		karpv1.CapacityTypeLabelKey:              karpv1.CapacityTypeReserved,
		cloudprovider.ReservationIDLabel:         reservationID,
		v1.LabelCapacityReservationType:          string(v1.CapacityReservationTypeDefault),
		v1.LabelCapacityReservationInterruptible: strconv.FormatBool(false),
	}
}

// eligible returns the NodeClaim as a migration candidate with its compatible reservations, or nil if it can't move.
//
//nolint:gocyclo
func (c *Controller) eligible(ctx context.Context, p *pass, nc *karpv1.NodeClaim, inst *instance.Instance) (*candidate, error) {
	if nc.Labels[karpv1.CapacityTypeLabelKey] != karpv1.CapacityTypeOnDemand {
		return nil, nil
	}
	if !nc.StatusConditions().Get(karpv1.ConditionTypeLaunched).IsTrue() ||
		!nc.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() ||
		!nc.StatusConditions().Get(karpv1.ConditionTypeInitialized).IsTrue() {
		return nil, nil
	}
	if b, ok := c.backoff[inst.ID]; ok && c.clk.Now().Before(b.next) {
		return nil, nil
	}
	state := inst.CapacityReservationState
	if inst.State != ec2types.InstanceStateNameRunning || inst.CapacityType != karpv1.CapacityTypeOnDemand ||
		state.Lifecycle == ec2types.InstanceLifecycleTypeCapacityBlock || state.Lifecycle == ec2types.InstanceLifecycleTypeInterruptibleCapacityReservation ||
		state.Lifecycle == ec2types.InstanceLifecycleTypeSpot || state.CoveringID != "" {
		return nil, nil
	}
	// Modify is refused in a shifted zone, so don't annotate and fail every pass.
	if inst.ZoneID != "" && c.zonalShift.IsZonalShifted(ctx, inst.ZoneID) {
		p.ineligible[reasonZonalShift]++
		return nil, nil
	}
	var migrationCase string
	switch {
	case state.TargetID == "" && state.Preference == ec2types.CapacityReservationPreferenceNone:
		migrationCase = caseOnDemand
	case state.TargetID != "":
		active, err := c.isActive(ctx, p, nc, state.TargetID)
		if err != nil {
			return nil, err
		}
		if active {
			return nil, nil
		}
		migrationCase = caseOrphaned
	case state.Preference == ec2types.CapacityReservationPreferenceOpen:
		p.ineligible[reasonPreferenceOpen]++
		return nil, nil
	default:
		return nil, nil
	}

	nodeClass, err := c.nodeClass(ctx, p, nc)
	if err != nil || nodeClass == nil {
		return nil, err
	}
	reservations, endTimes, err := c.compatibleReservations(ctx, p, nodeClass, inst, state.TargetID)
	if err != nil {
		return nil, err
	}
	if len(reservations) == 0 {
		p.ineligible[reasonNoCompatibleCR]++
		return nil, nil
	}
	node, err := nodeclaimutils.NodeForNodeClaim(ctx, c.kubeClient, nc)
	if err != nil {
		return nil, client.IgnoreNotFound(nodeclaimutils.IgnoreNodeNotFoundError(nodeclaimutils.IgnoreDuplicateNodeError(err)))
	}
	if !node.DeletionTimestamp.IsZero() || lo.ContainsBy(node.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&karpv1.DisruptedNoScheduleTaint) }) {
		return nil, nil
	}
	reservations, reason, err := c.filterCompatible(ctx, p, nc, []*corev1.Node{node}, reservations)
	if err != nil {
		return nil, err
	}
	if len(reservations) == 0 {
		if reason != "" {
			p.ineligible[reason]++
		}
		return nil, nil
	}
	return &candidate{nodeClaim: nc, instance: inst, caseLabel: migrationCase, reservations: reservations, endTimes: endTimes}, nil
}

// filterCompatible keeps the reservations whose labels neither the NodePool nor any pod on the nodes rejects, else returns why.
func (c *Controller) filterCompatible(ctx context.Context, p *pass, nc *karpv1.NodeClaim, nodes []*corev1.Node, ids []string) ([]string, string, error) {
	nodePool, err := c.nodePool(ctx, p, nc)
	if err != nil || nodePool == nil {
		return nil, "", err
	}
	nodePoolReqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodePool.Spec.Template.Spec.Requirements...)
	// Same check as core requirements drift, so relabeling can't drift the node.
	ids = lo.Filter(ids, func(id string, _ int) bool {
		return scheduling.NewLabelRequirements(lo.Assign(nc.Labels, reservedLabels(id))).Compatible(nodePoolReqs) == nil
	})
	if len(ids) == 0 {
		return nil, reasonNodePoolIncompatible, nil
	}
	for _, node := range nodes {
		pods, err := nodeutils.GetPods(ctx, c.kubeClient, node.Name)
		if err != nil {
			return nil, "", err
		}
		// The DaemonSet controller deletes pods whose required affinity stops matching, so they count too.
		pods = lo.Filter(pods, func(pod *corev1.Pod, _ int) bool { return !podutils.IsOwnedByNode(pod) && !podutils.IsTerminal(pod) })
		ids = lo.Filter(ids, func(id string, _ int) bool {
			synthetic := &corev1.Node{ObjectMeta: node.ObjectMeta}
			synthetic.Labels = lo.Assign(node.Labels, reservedLabels(id))
			return lo.EveryBy(pods, func(pod *corev1.Pod) bool {
				ok, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(synthetic)
				return err == nil && ok
			})
		})
		if len(ids) == 0 {
			return nil, reasonPodIncompatible, nil
		}
	}
	return ids, "", nil
}

// compatibleReservations returns the NodeClass's reservations the instance can be re-targeted at, ignoring capacity,
// with their end times.
//
//nolint:gocyclo
func (c *Controller) compatibleReservations(ctx context.Context, p *pass, nodeClass *v1.EC2NodeClass, inst *instance.Instance, source string) ([]string, map[string]time.Time, error) {
	raw, err := c.selected(ctx, p, nodeClass)
	if err != nil {
		return nil, nil, err
	}
	// An unknown platform could pick a reservation EC2 won't match, and the move can't be undone.
	if inst.CapacityReservationState.Platform == "" {
		return nil, nil, nil
	}
	endTimes := map[string]time.Time{}
	ids := lo.FilterMap(nodeClass.Status.CapacityReservations, func(cr v1.CapacityReservation, _ int) (string, bool) {
		if cr.ReservationType != v1.CapacityReservationTypeDefault || cr.Interruptible || cr.State != v1.CapacityReservationStateActive ||
			cr.InstanceType != string(inst.Type) || cr.AvailabilityZone != inst.Zone || cr.ID == source {
			return "", false
		}
		r, ok := raw[cr.ID]
		// EC2 doesn't validate placement group or Outpost on Modify, so such a target might never cover the instance.
		if !ok || r.State != ec2types.CapacityReservationStateActive || r.PlacementGroupArn != nil || r.OutpostArn != nil ||
			string(r.InstancePlatform) != inst.CapacityReservationState.Platform ||
			string(lo.Ternary(r.Tenancy == "", ec2types.CapacityReservationTenancyDefault, r.Tenancy)) != inst.Tenancy {
			return "", false
		}
		if end := lo.FromPtr(r.EndDate); !end.IsZero() {
			if end.Sub(c.clk.Now()) < minReservationRemaining {
				return "", false
			}
			endTimes[cr.ID] = end
		}
		return cr.ID, true
	})
	return ids, endTimes, nil
}

// selected returns the active reservations the NodeClass's selector terms match, keyed by ID.
func (c *Controller) selected(ctx context.Context, p *pass, nodeClass *v1.EC2NodeClass) (map[string]*ec2types.CapacityReservation, error) {
	if raw, ok := p.raw[nodeClass.Name]; ok {
		return raw, nil
	}
	key := "nodeclass/" + nodeClass.Name
	if err, ok := p.failed[key]; ok {
		return nil, err
	}
	reservations, err := c.crProvider.List(ctx, nodeClass.Spec.CapacityReservationSelectorTerms...)
	if err != nil {
		p.failed[key] = fmt.Errorf("listing capacity reservations, %w", err)
		return nil, p.failed[key]
	}
	raw := lo.KeyBy(reservations, func(r *ec2types.CapacityReservation) string { return lo.FromPtr(r.CapacityReservationId) })
	p.raw[nodeClass.Name] = raw
	return raw, nil
}

// preferred orders reservations that never end first, then by latest end, then by most free slots, then by ID.
func preferred(a, b string, endTimes map[string]time.Time, budgets map[string]int) bool {
	ea, eb := endTimes[a], endTimes[b]
	if !ea.Equal(eb) {
		return ea.IsZero() || (!eb.IsZero() && ea.After(eb))
	}
	if budgets[a] != budgets[b] {
		return budgets[a] > budgets[b]
	}
	return a < b
}

//nolint:gocyclo
func (c *Controller) migrate(ctx context.Context, p *pass, candidates []*candidate) (int, []error) {
	var errs []error
	calls := 0
	for _, cand := range candidates {
		if calls >= maxModifyCallsPerPass {
			break
		}
		// The ledger-adjusted count already excludes slots claimed earlier in this pass.
		budgets := lo.SliceToMap(cand.reservations, func(id string) (string, int) {
			return id, c.crProvider.GetAvailableInstanceCount(id) - p.launching[id]
		})
		reservations := lo.Filter(cand.reservations, func(id string, _ int) bool { return budgets[id] > 0 })
		if len(reservations) == 0 {
			p.ineligible[reasonNoCapacity]++
			continue
		}
		nc, inst := cand.nodeClaim, cand.instance
		reservations = lo.Reject(reservations, func(id string, _ int) bool {
			return c.clk.Now().Before(c.unauthorized[id]) || c.clk.Now().Before(c.denied[slot{instanceID: inst.ID, reservationID: id}].next)
		})
		if len(reservations) == 0 {
			p.ineligible[reasonUnauthorized]++
			continue
		}
		target := lo.MinBy(reservations, func(a, b string) bool { return preferred(a, b, cand.endTimes, budgets) })
		// Record intent first so a crash after the call still leaves durable state for the ledger.
		if err := c.setTarget(ctx, nc, target); err != nil {
			errs = append(errs, client.IgnoreNotFound(serrors.Wrap(err, "NodeClaim", klog.KObj(nc))))
			continue
		}
		err := c.instanceProvider.ModifyCapacityReservationTarget(ctx, inst.ID, target)
		if stderrors.Is(err, instance.ErrModifyNotAttempted) {
			// Nothing reached EC2, so skip the instance this pass without counting an attempt.
			log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc)).V(1).Info("skipping capacity reservation migration", "error", err.Error())
			if clearErr := c.clearTarget(ctx, nc); clearErr != nil {
				errs = append(errs, serrors.Wrap(clearErr, "NodeClaim", klog.KObj(nc)))
			}
			if awserrors.IsRateLimitedError(err) {
				return calls, errs
			}
			continue
		}
		calls++
		if err == nil || ambiguous(err) {
			c.uncovered[inst.ID] = p.start
		}
		if err == nil {
			p.hold(target, inst.ID)
			c.crProvider.SetPendingMigrations(p.pending)
			delete(c.backoff, inst.ID)
			delete(c.denied, slot{instanceID: inst.ID, reservationID: target})
			delete(c.deniedBy, target)
			delete(c.unauthorized, target)
			c.cases[inst.ID] = cand.caseLabel
			MigrationAttemptsTotal.Inc(map[string]string{resultLabel.Name: resultSuccess, caseLabel.Name: cand.caseLabel})
			c.recorder.Publish(startedEvent(nc, target))
			log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc), "capacity-reservation-id", target).Info("re-targeted instance at capacity reservation")
			continue
		}
		if ambiguous(err) {
			// EC2 may have applied it: keep the annotation and hold, and let a later DescribeInstances decide.
			p.hold(target, inst.ID)
			c.crProvider.SetPendingMigrations(p.pending)
			MigrationAttemptsTotal.Inc(map[string]string{resultLabel.Name: resultError, caseLabel.Name: cand.caseLabel})
			c.backOff(inst.ID)
			c.recorder.Publish(failedEvent(nc, target, err))
			log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc), "capacity-reservation-id", target).Error(err, "failed re-targeting instance at capacity reservation, outcome unknown")
			return calls, errs
		}
		if clearErr := c.clearTarget(ctx, nc); clearErr != nil {
			errs = append(errs, serrors.Wrap(clearErr, "NodeClaim", klog.KObj(nc)))
		}
		switch {
		case cloudprovider.IsNodeClaimNotFoundError(err):
			continue
		case awserrors.IsUnauthorizedOperationError(err):
			MigrationAttemptsTotal.Inc(map[string]string{resultLabel.Name: resultUnauthorized, caseLabel.Name: cand.caseLabel})
			c.deny(inst.ID, target)
			log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc), "capacity-reservation-id", target).Error(err,
				"not authorized for ec2:ModifyInstanceCapacityReservationAttributes, skipping capacity reservation for this instance")
		case awserrors.IsRateLimitedError(err):
			MigrationAttemptsTotal.Inc(map[string]string{resultLabel.Name: resultThrottled, caseLabel.Name: cand.caseLabel})
			return calls, errs
		default:
			MigrationAttemptsTotal.Inc(map[string]string{resultLabel.Name: resultClientError, caseLabel.Name: cand.caseLabel})
			c.backOff(inst.ID)
			c.recorder.Publish(failedEvent(nc, target, err))
			log.FromContext(ctx).WithValues("NodeClaim", client.ObjectKeyFromObject(nc), "capacity-reservation-id", target).Error(err, "failed re-targeting instance at capacity reservation")
		}
	}
	return calls, errs
}

// ambiguous reports whether a failed Modify may still have been applied by EC2.
func ambiguous(err error) bool {
	if cloudprovider.IsNodeClaimNotFoundError(err) ||
		awserrors.IsUnauthorizedOperationError(err) || awserrors.IsRateLimitedError(err) {
		return false
	}
	apiErr, ok := lo.ErrorsAs[smithy.APIError](err)
	if !ok {
		// A transport error (timeout, reset, canceled context) carries no EC2 verdict, so the call may have landed.
		return true
	}
	if resp, ok := lo.ErrorsAs[interface {
		error
		HTTPStatusCode() int
	}](err); ok && resp.HTTPStatusCode() >= 500 {
		return true
	}
	return serverErrorCodes.Has(apiErr.ErrorCode())
}

// holdLaunch keeps migration off slots the scheduler gave an un-launched NodeClaim; Create only counts it once launched.
func (p *pass) holdLaunch(nc *karpv1.NodeClaim) {
	if !nc.DeletionTimestamp.IsZero() {
		return
	}
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nc.Spec.Requirements...)
	if !reqs.Has(karpv1.CapacityTypeLabelKey) || !reqs.Has(cloudprovider.ReservationIDLabel) ||
		!reqs.Get(karpv1.CapacityTypeLabelKey).Has(karpv1.CapacityTypeReserved) || reqs.Get(karpv1.CapacityTypeLabelKey).Len() != 1 {
		return
	}
	// The launch may land in any of the offered reservations, so hold one slot in each.
	for _, id := range reqs.Get(cloudprovider.ReservationIDLabel).Values() {
		p.launching[id]++
	}
}

// holdOrCovered holds the instance's slot in target, unless EC2 already counts it there; then it is held only until the next sync.
func (c *Controller) holdOrCovered(p *pass, target string, inst *instance.Instance) {
	if target == "" || inst.CapacityReservationState.CoveringID != target {
		p.hold(target, inst.ID)
		return
	}
	if s := (slot{instanceID: inst.ID, reservationID: target}); !c.covered.Has(s) {
		c.crProvider.MarkMigrated(target, inst.ID, c.uncovered[inst.ID])
		c.covered.Insert(s)
	}
}

func (p *pass) hold(target, instanceID string) {
	if target == "" {
		return
	}
	if p.pending[target] == nil {
		p.pending[target] = sets.New[string]()
	}
	p.pending[target].Insert(instanceID)
}

// deny backs off the instance from the reservation, and the whole reservation once a second instance is denied,
// since the shipped policy scopes the permission by instance tags.
func (c *Controller) deny(instanceID, reservationID string) {
	s := slot{instanceID: instanceID, reservationID: reservationID}
	c.denied[s] = c.nextBackoff(c.denied[s])
	if c.deniedBy[reservationID] == nil {
		c.deniedBy[reservationID] = sets.New[string]()
	}
	c.deniedBy[reservationID].Insert(instanceID)
	if c.deniedBy[reservationID].Len() >= 2 {
		// Fixed, so per-instance backoff outgrows it and repeatedly denied instances stop crowding out the rest.
		c.unauthorized[reservationID] = c.clk.Now().Add(initialBackoff)
		delete(c.deniedBy, reservationID)
	}
}

func (c *Controller) backOff(instanceID string) {
	c.backoff[instanceID] = c.nextBackoff(c.backoff[instanceID])
}

func (c *Controller) nextBackoff(b backoffEntry) backoffEntry {
	delay := lo.Ternary(b.delay == 0, initialBackoff, min(b.delay*2, maxBackoff))
	return backoffEntry{next: c.clk.Now().Add(delay), delay: delay}
}

func (c *Controller) prune(instances map[string]*instance.Instance) {
	for id := range c.backoff {
		if _, ok := instances[id]; !ok {
			delete(c.backoff, id)
		}
	}
	for id := range c.cases {
		if _, ok := instances[id]; !ok {
			delete(c.cases, id)
		}
	}
	for s := range c.covered {
		if _, ok := instances[s.instanceID]; !ok {
			c.covered.Delete(s)
		}
	}
	for id := range c.uncovered {
		if _, ok := instances[id]; !ok {
			delete(c.uncovered, id)
		}
	}
	c.pruneDenials(instances)
}

func (c *Controller) pruneDenials(instances map[string]*instance.Instance) {
	for s, b := range c.denied {
		if _, ok := instances[s.instanceID]; !ok || c.clk.Since(b.next) > maxBackoff {
			delete(c.denied, s)
		}
	}
	for id, instanceIDs := range c.deniedBy {
		for instanceID := range instanceIDs {
			if _, ok := c.denied[slot{instanceID: instanceID, reservationID: id}]; !ok {
				instanceIDs.Delete(instanceID)
			}
		}
		if instanceIDs.Len() == 0 {
			delete(c.deniedBy, id)
		}
	}
	for id, until := range c.unauthorized {
		if !c.clk.Now().Before(until) {
			delete(c.unauthorized, id)
		}
	}
}

func (c *Controller) setTarget(ctx context.Context, nc *karpv1.NodeClaim, target string) error {
	stored := nc.DeepCopy()
	nc.Annotations = lo.Assign(nc.Annotations, map[string]string{
		v1.AnnotationCapacityReservationMigrationTarget:      target,
		v1.AnnotationCapacityReservationMigrationRequestedAt: c.clk.Now().UTC().Format(time.RFC3339),
	})
	if err := c.kubeClient.Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("annotating nodeclaim, %w", err)
	}
	return nil
}

func (c *Controller) clearTarget(ctx context.Context, nc *karpv1.NodeClaim) error {
	stored := nc.DeepCopy()
	delete(nc.Annotations, v1.AnnotationCapacityReservationMigrationTarget)
	delete(nc.Annotations, v1.AnnotationCapacityReservationMigrationRequestedAt)
	if err := c.kubeClient.Patch(ctx, nc, client.MergeFrom(stored)); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("removing nodeclaim annotations, %w", err)
	}
	return nil
}

// isActive reports whether a reservation is still active; List returns active, unexpired reservations only.
func (c *Controller) isActive(ctx context.Context, p *pass, nc *karpv1.NodeClaim, id string) (bool, error) {
	if active, ok := p.active[id]; ok {
		return active, nil
	}
	// The NodeClass's selector query is usually cached, so check it before a by-ID call.
	nodeClass, err := c.nodeClass(ctx, p, nc)
	if err != nil {
		return false, err
	}
	if nodeClass != nil {
		raw, err := c.selected(ctx, p, nodeClass)
		if err != nil {
			return false, err
		}
		if _, ok := raw[id]; ok {
			p.active[id] = true
			return true, nil
		}
	}
	key := "id/" + id
	if err, ok := p.failed[key]; ok {
		return false, err
	}
	reservations, err := c.crProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: id})
	if err != nil {
		p.failed[key] = fmt.Errorf("getting capacity reservation, %w", err)
		return false, p.failed[key]
	}
	p.active[id] = lo.ContainsBy(reservations, func(r *ec2types.CapacityReservation) bool { return lo.FromPtr(r.CapacityReservationId) == id })
	return p.active[id], nil
}

func (c *Controller) nodeClass(ctx context.Context, p *pass, nc *karpv1.NodeClaim) (*v1.EC2NodeClass, error) {
	name := nc.Spec.NodeClassRef.Name
	if nodeClass, ok := p.nodeClasses[name]; ok {
		return nodeClass, nil
	}
	nodeClass := &v1.EC2NodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodeClass); err != nil {
		if errors.IsNotFound(err) {
			p.nodeClasses[name] = nil
			return nil, nil
		}
		return nil, fmt.Errorf("getting ec2nodeclass, %w", err)
	}
	p.nodeClasses[name] = nodeClass
	return nodeClass, nil
}

func (c *Controller) nodePool(ctx context.Context, p *pass, nc *karpv1.NodeClaim) (*karpv1.NodePool, error) {
	name, ok := nc.Labels[karpv1.NodePoolLabelKey]
	if !ok {
		return nil, nil
	}
	if nodePool, ok := p.nodePools[name]; ok {
		return nodePool, nil
	}
	nodePool := &karpv1.NodePool{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodePool); err != nil {
		if errors.IsNotFound(err) {
			p.nodePools[name] = nil
			return nil, nil
		}
		return nil, fmt.Errorf("getting nodepool, %w", err)
	}
	p.nodePools[name] = nodePool
	return nodePool, nil
}

func (c *Controller) publishGauges(p *pass) {
	MigrationPending.Reset()
	for id, instances := range p.pending {
		MigrationPending.Set(float64(instances.Len()), map[string]string{reservationIDLabel.Name: id})
	}
	MigrationIneligible.Reset()
	for _, reason := range reasonLabel.Values {
		MigrationIneligible.Set(float64(p.ineligible[reason.Name]), map[string]string{reasonLabel.Name: reason.Name})
	}
	MigrationStalled.Set(float64(p.stalled), map[string]string{})
}
