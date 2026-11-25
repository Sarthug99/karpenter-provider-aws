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

package capacitytype

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

type Controller struct {
	cp         cloudprovider.CloudProvider
	kubeClient client.Client
	crProvider capacityreservation.Provider
}

func NewController(kubeClient client.Client, cp cloudprovider.CloudProvider, crProvider capacityreservation.Provider) *Controller {
	return &Controller{
		cp:         cp,
		kubeClient: kubeClient,
		crProvider: crProvider,
	}
}

func (*Controller) Name() string {
	return "capacityreservation.capacitytype"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	// List NodeClaims before instances so a reserved label written after an older instance snapshot is never demoted.
	ncs := &karpv1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, ncs); err != nil {
		return reconciler.Result{}, fmt.Errorf("listing nodeclaims, %w", err)
	}
	cpNodeClaims, err := c.cp.List(ctx)
	if err != nil {
		return reconciler.Result{}, fmt.Errorf("listing instance types, %w", err)
	}
	providerIDsToCPNodeClaims := lo.SliceToMap(cpNodeClaims, func(nc *karpv1.NodeClaim) (string, *karpv1.NodeClaim) {
		return nc.Status.ProviderID, nc
	})
	updatedNodeClaims := sets.New[string]()
	nodeClasses := map[string]*v1.EC2NodeClass{}
	var errs []error
	for i := range ncs.Items {
		cpNC, ok := providerIDsToCPNodeClaims[ncs.Items[i].Status.ProviderID]
		if !ok {
			continue
		}
		var updated bool
		var err error
		if cpNC.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeReserved {
			updated, err = c.promote(ctx, nodeClasses, cpNC, &ncs.Items[i])
		} else {
			updated, err = c.syncCapacityType(ctx, cpNC.Labels[karpv1.CapacityTypeLabelKey], &ncs.Items[i])
		}
		if err != nil {
			errs = append(errs, err)
		}
		if updated {
			updatedNodeClaims.Insert(ncs.Items[i].Name)
		}
	}
	if len(updatedNodeClaims) != 0 {
		log.FromContext(ctx).WithValues("NodeClaims", lo.Map(updatedNodeClaims.UnsortedList(), func(name string, _ int) klog.ObjectRef {
			return klog.KRef("", name)
		})).V(1).Info("updated capacity type for nodeclaims")
	}
	if len(errs) != 0 {
		if lo.EveryBy(errs, func(err error) bool { return errors.IsConflict(err) }) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, multierr.Combine(errs...)
	}
	return reconciler.Result{RequeueAfter: time.Minute}, nil
}

var reservedLabelKeys = append(cloudprovider.ReservedCapacityLabels.UnsortedList(), karpv1.CapacityTypeLabelKey)

// promote finishes migrations whose controller is off and syncs a reserved NodeClaim's Nodes; other coverage stays on-demand.
//
//nolint:gocyclo
func (c *Controller) promote(ctx context.Context, nodeClasses map[string]*v1.EC2NodeClass, cpNC, nc *karpv1.NodeClaim) (bool, error) {
	id := cpNC.Labels[cloudprovider.ReservationIDLabel]
	if !nc.DeletionTimestamp.IsZero() || id == "" {
		return false, nil
	}
	if nc.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeReserved && nc.Labels[cloudprovider.ReservationIDLabel] == id {
		return c.relabelNodes(ctx, nc, lo.PickByKeys(nc.Labels, reservedLabelKeys))
	}
	if nc.Labels[karpv1.CapacityTypeLabelKey] != karpv1.CapacityTypeOnDemand || nc.Spec.NodeClassRef == nil ||
		nc.Annotations[v1.AnnotationCapacityReservationMigrationTarget] != id || options.FromContext(ctx).FeatureGates.ReservedCapacityMigration {
		return false, nil
	}
	nodeClass, ok := nodeClasses[nc.Spec.NodeClassRef.Name]
	if !ok {
		nodeClass = &v1.EC2NodeClass{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nc.Spec.NodeClassRef.Name}, nodeClass); err != nil {
			if !errors.IsNotFound(err) {
				return false, serrors.Wrap(fmt.Errorf("getting ec2nodeclass, %w", err), "EC2NodeClass", klog.KRef("", nc.Spec.NodeClassRef.Name))
			}
			nodeClass = nil
		}
		nodeClasses[nc.Spec.NodeClassRef.Name] = nodeClass
	}
	if nodeClass == nil || !lo.ContainsBy(nodeClass.Status.CapacityReservations, func(cr v1.CapacityReservation) bool { return cr.ID == id }) {
		return false, nil
	}
	labels := lo.PickByKeys(cpNC.Labels, reservedLabelKeys)
	stored := nc.DeepCopy()
	nc.Labels = lo.Assign(nc.Labels, labels)
	_, first := nc.Annotations[v1.AnnotationCapacityReservationMigrationRequestedAt]
	// The target stays as a marker, so a demotion from a stale DescribeInstances is promoted again.
	delete(nc.Annotations, v1.AnnotationCapacityReservationMigrationRequestedAt)
	if err := c.kubeClient.Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("patching nodeclaim, %w", err), "NodeClaim", klog.KObj(nc)))
	}
	// EC2's count now includes this instance, but our cache won't until its next sync.
	if instanceID, err := utils.ParseInstanceID(nc.Status.ProviderID); err == nil && first {
		c.crProvider.MarkMigrated(id, instanceID, time.Time{})
	}
	// After the NodeClaim, so a failed Node patch is retried by the reserved NodeClaim path above.
	_, err := c.relabelNodes(ctx, nc, labels)
	return true, err
}

// relabelNodes sets labels on the NodeClaim's registered Nodes that don't already carry them.
func (c *Controller) relabelNodes(ctx context.Context, nc *karpv1.NodeClaim, labels map[string]string) (bool, error) {
	nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nc)
	if err != nil {
		return false, serrors.Wrap(fmt.Errorf("listing nodes for nodeclaim, %w", err), "NodeClaim", klog.KObj(nc))
	}
	updated := false
	for _, n := range nodes {
		if !n.DeletionTimestamp.IsZero() || n.Labels[karpv1.NodeRegisteredLabelKey] != "true" ||
			lo.EveryBy(lo.Entries(labels), func(e lo.Entry[string, string]) bool { return n.Labels[e.Key] == e.Value }) {
			continue
		}
		stored := n.DeepCopy()
		n.Labels = lo.Assign(n.Labels, labels)
		if err := c.kubeClient.Patch(ctx, n, client.MergeFrom(stored)); client.IgnoreNotFound(err) != nil {
			return false, serrors.Wrap(fmt.Errorf("patching node, %w", err), "Node", klog.KObj(n))
		}
		updated = true
	}
	return updated, nil
}

// syncCapacityType will update the capacity type for the given NodeClaim. This accounts for the fact that capacity
// reservations will expire, demoting NodeClaims with capacity type "reserved" to "on-demand".
//
//nolint:gocyclo
func (c *Controller) syncCapacityType(ctx context.Context, capacityType string, nc *karpv1.NodeClaim) (bool, error) {
	// We won't be able to sync deleting NodeClaims, and there's no real need to either as they're already draining.
	if !nc.DeletionTimestamp.IsZero() {
		return false, nil
	}

	if capacityType != karpv1.CapacityTypeOnDemand {
		return false, nil
	}
	updated := false
	if nc.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeReserved {
		stored := nc.DeepCopy()
		demote(nc.Labels)
		if err := c.kubeClient.Patch(ctx, nc, client.MergeFrom(stored)); client.IgnoreNotFound(err) != nil {
			return false, serrors.Wrap(fmt.Errorf("patching nodeclaim, %w", err), "NodeClaim", klog.KObj(nc))
		}
		updated = true
	}

	// If the reservation expired before the NodeClaim became registered, there may not be a Node on the cluster. Note
	// that there should never be duplicate Nodes for a given NodeClaim, but handling this user-induced error is more
	// straightforward than handling the duplicate error.
	nodes, err := nodeclaimutils.AllNodesForNodeClaim(ctx, c.kubeClient, nc)
	if err != nil {
		return false, serrors.Wrap(fmt.Errorf("listing nodes for nodeclaim, %w", err), "NodeClaim", klog.KObj(nc))
	}
	for _, n := range nodes {
		if !n.DeletionTimestamp.IsZero() {
			continue
		}
		// Skip Nodes which haven't been registered since we still may not have synced labels. We'll get it on the next
		// iteration.
		if n.Labels[karpv1.NodeRegisteredLabelKey] != "true" {
			continue
		}
		if n.Labels[karpv1.CapacityTypeLabelKey] != karpv1.CapacityTypeReserved {
			continue
		}
		// Nodes follow their NodeClaim, so don't demote one whose NodeClaim was relabeled after our instance snapshot.
		if !updated {
			if fresh, err := c.freshlyReserved(ctx, nc); err != nil || fresh {
				return false, err
			}
		}
		stored := n.DeepCopy()
		demote(n.Labels)
		if err := c.kubeClient.Patch(ctx, n, client.MergeFrom(stored)); client.IgnoreNotFound(err) != nil {
			return false, serrors.Wrap(fmt.Errorf("patching node, %w", err), "Node", klog.KObj(n))
		}
		updated = true
	}
	return updated, nil
}

func (c *Controller) freshlyReserved(ctx context.Context, nc *karpv1.NodeClaim) (bool, error) {
	fresh := &karpv1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(nc), fresh); err != nil {
		return false, client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("getting nodeclaim, %w", err), "NodeClaim", klog.KObj(nc)))
	}
	return fresh.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeReserved, nil
}

func demote(labels map[string]string) {
	labels[karpv1.CapacityTypeLabelKey] = karpv1.CapacityTypeOnDemand
	for label := range cloudprovider.ReservedCapacityLabels {
		delete(labels, label)
	}
}
