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
	"context"
	"fmt"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/providers/amifamily"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instance"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

const (
	AMIDrift                 cloudprovider.DriftReason = "AMIDrift"
	SubnetDrift              cloudprovider.DriftReason = "SubnetDrift"
	SecurityGroupDrift       cloudprovider.DriftReason = "SecurityGroupDrift"
	CapacityReservationDrift cloudprovider.DriftReason = "CapacityReservationDrift"
	PlacementGroupDrift      cloudprovider.DriftReason = "PlacementGroupDrift"
	NodeClassDrift           cloudprovider.DriftReason = "NodeClassDrift"
)

func (c *CloudProvider) isNodeClassDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim, nodeClass *v1.EC2NodeClass) (cloudprovider.DriftReason, error) {
	// First check if the node class is statically drifted to save on API calls.
	if drifted := c.areStaticFieldsDrifted(nodeClaim, nodeClass); drifted != "" {
		return drifted, nil
	}
	amiDrifted, err := c.isAMIDrifted(ctx, nodeClaim, nodeClass)
	if err != nil {
		return "", fmt.Errorf("calculating ami drift, %w", err)
	}
	if amiDrifted != "" {
		return amiDrifted, nil
	}
	instance, err := c.getInstance(ctx, nodeClaim.Status.ProviderID)
	if err != nil {
		return "", err
	}
	securitygroupDrifted, err := c.areSecurityGroupsDrifted(instance, nodeClass)
	if err != nil {
		return "", fmt.Errorf("calculating securitygroup drift, %w", err)
	}
	subnetDrifted, err := c.isSubnetDrifted(instance, nodeClass)
	if err != nil {
		return "", fmt.Errorf("calculating subnet drift, %w", err)
	}
	capacityReservationsDrifted, err := c.isCapacityReservationDrifted(ctx, nodeClaim, instance, nodeClass)
	if err != nil {
		return "", fmt.Errorf("calculating capacity reservation drift, %w", err)
	}
	placementGroupDrifted := c.isPlacementGroupDrifted(ctx, nodeClaim, nodeClass)
	drifted := lo.FindOrElse([]cloudprovider.DriftReason{
		securitygroupDrifted,
		subnetDrifted,
		capacityReservationsDrifted,
		placementGroupDrifted,
	}, "", func(i cloudprovider.DriftReason) bool {
		return string(i) != ""
	})
	return drifted, nil
}

func (c *CloudProvider) isAMIDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim, nodeClass *v1.EC2NodeClass) (cloudprovider.DriftReason, error) {
	instanceTypeName := nodeClaim.Labels[corev1.LabelInstanceTypeStable]
	nodeInstanceType, err := c.instanceTypeProvider.Get(ctx, nodeClass, ec2types.InstanceType(instanceTypeName))
	if err != nil {
		return "", serrors.Wrap(fmt.Errorf("getting node instance type, %w", err), "instance-type", instanceTypeName)
	}
	if len(nodeClass.Status.AMIs) == 0 {
		return "", fmt.Errorf("no amis exist given constraints")
	}
	// Should never happen as ImageID is resolved when setting Launched status condition
	if nodeClaim.Status.ImageID == "" {
		return "", fmt.Errorf("no ami ID found in nodeClaim status")
	}
	mappedAMIs := amifamily.MapToInstanceTypes([]*cloudprovider.InstanceType{nodeInstanceType}, nodeClass.Status.AMIs)
	if !lo.Contains(lo.Keys(mappedAMIs), nodeClaim.Status.ImageID) {
		return AMIDrift, nil
	}
	return "", nil
}

// Checks if the security groups are drifted, by comparing the subnet returned from the subnetProvider
// to the ec2 instance subnets
func (c *CloudProvider) isSubnetDrifted(instance *instance.Instance, nodeClass *v1.EC2NodeClass) (cloudprovider.DriftReason, error) {
	// subnets need to be found to check for drift
	if len(nodeClass.Status.Subnets) == 0 {
		return "", fmt.Errorf("no subnets are discovered")
	}

	_, found := lo.Find(nodeClass.Status.Subnets, func(subnet v1.Subnet) bool {
		return subnet.ID == instance.SubnetID
	})

	if !found {
		return SubnetDrift, nil
	}
	return "", nil
}

// Checks if the security groups are drifted, by comparing the security groups returned from the SecurityGroupProvider
// to the ec2 instance security groups
func (c *CloudProvider) areSecurityGroupsDrifted(ec2Instance *instance.Instance, nodeClass *v1.EC2NodeClass) (cloudprovider.DriftReason, error) {
	securityGroupIds := sets.New(lo.Map(nodeClass.Status.SecurityGroups, func(sg v1.SecurityGroup, _ int) string { return sg.ID })...)
	if len(securityGroupIds) == 0 {
		return "", fmt.Errorf("no security groups are present in the status")
	}

	if !securityGroupIds.Equal(sets.New(ec2Instance.SecurityGroupIDs...)) {
		return SecurityGroupDrift, nil
	}
	return "", nil
}

// Checks if capacity reservations are drifted, by comparing the capacity reservations persisted to the NodeClass to
// the instance's capacity reservation. When stale instance information isn't useful enough to determine drift, we
// skip the cache and fallback to the EC2 API.
// NOTE: We handle drift dynamically for capacity reservations rather than relying on the offerings inducing drift since
// a reserved instance may fall back to on-demand. Relying on offerings could result in drift occurring before fallback
// would cancel it out.
func (c *CloudProvider) isCapacityReservationDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim, i *instance.Instance, nodeClass *v1.EC2NodeClass) (cloudprovider.DriftReason, error) {
	capacityReservationIDs := sets.New(lo.Map(nodeClass.Status.CapacityReservations, func(cr v1.CapacityReservation, _ int) string { return cr.ID })...)
	drifted := func(i *instance.Instance) bool {
		if i.CapacityReservationDetails == nil || capacityReservationIDs.Has(i.CapacityReservationDetails.ID) {
			return false
		}
		// A node re-targeted in place and not yet labeled reserved counts as on-demand; a demoted launched node still drifts.
		if nodeClaim.Labels[karpv1.CapacityTypeLabelKey] == karpv1.CapacityTypeOnDemand &&
			nodeClaim.Annotations[v1.AnnotationCapacityReservationMigrationTarget] == i.CapacityReservationDetails.ID {
			return false
		}
		return i.CapacityReservationState.Preference == ec2types.CapacityReservationPreferenceCapacityReservationsOnly ||
			nodeClaim.Labels[cloudprovider.ReservationIDLabel] == i.CapacityReservationDetails.ID
	}
	if !drifted(i) {
		return "", nil
	}
	// skip the instance type cache to get up-to-date instance info
	i, err := c.getInstance(ctx, nodeClaim.Status.ProviderID, instance.SkipCache)
	if err != nil {
		return "", err
	}
	if !drifted(i) {
		return "", nil
	}
	if ended, err := c.retargetedPastEndDate(ctx, nodeClaim, i); err != nil || ended {
		return "", err
	}
	return CapacityReservationDrift, nil
}

// retargetedPastEndDate reports whether a re-targeted node's reservation passed its EndDate; it moves again once EC2 ends it.
func (c *CloudProvider) retargetedPastEndDate(ctx context.Context, nodeClaim *karpv1.NodeClaim, i *instance.Instance) (bool, error) {
	if i.CapacityReservationState.Preference == ec2types.CapacityReservationPreferenceCapacityReservationsOnly &&
		nodeClaim.Annotations[v1.AnnotationCapacityReservationMigrationTarget] != i.CapacityReservationDetails.ID {
		return false, nil
	}
	reservations, err := c.capacityReservationProvider.List(ctx, v1.CapacityReservationSelectorTerm{ID: i.CapacityReservationDetails.ID})
	if err != nil {
		return false, fmt.Errorf("getting capacity reservation, %w", err)
	}
	return len(reservations) == 0, nil
}

// isPlacementGroupDrifted checks if the node's placement group ID label no longer matches the EC2NodeClass's
// resolved placement group. This covers scenarios where placementGroupSelector was added, removed, or changed.
func (c *CloudProvider) isPlacementGroupDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim, nodeClass *v1.EC2NodeClass) cloudprovider.DriftReason {
	nodeClaimPGID := nodeClaim.Labels[v1.LabelPlacementGroupID]
	var nodeClassPGID string
	if pg, _ := c.placementGroupProvider.Get(ctx, nodeClass); pg != nil {
		nodeClassPGID = pg.ID
	}
	if nodeClaimPGID != nodeClassPGID {
		return PlacementGroupDrift
	}
	return ""
}

func (c *CloudProvider) areStaticFieldsDrifted(nodeClaim *karpv1.NodeClaim, nodeClass *v1.EC2NodeClass) cloudprovider.DriftReason {
	nodeClassHash, foundNodeClassHash := nodeClass.Annotations[v1.AnnotationEC2NodeClassHash]
	nodeClassHashVersion, foundNodeClassHashVersion := nodeClass.Annotations[v1.AnnotationEC2NodeClassHashVersion]
	nodeClaimHash, foundNodeClaimHash := nodeClaim.Annotations[v1.AnnotationEC2NodeClassHash]
	nodeClaimHashVersion, foundNodeClaimHashVersion := nodeClaim.Annotations[v1.AnnotationEC2NodeClassHashVersion]

	if !foundNodeClassHash || !foundNodeClaimHash || !foundNodeClassHashVersion || !foundNodeClaimHashVersion {
		return ""
	}
	// validate that the hash version for the EC2NodeClass is the same as the NodeClaim before evaluating for static drift
	if nodeClassHashVersion != nodeClaimHashVersion {
		return ""
	}
	return lo.Ternary(nodeClassHash != nodeClaimHash, NodeClassDrift, "")
}

func (c *CloudProvider) getInstance(ctx context.Context, providerID string, options ...instance.Options) (*instance.Instance, error) {
	// Get InstanceID to fetch from EC2
	instanceID, err := utils.ParseInstanceID(providerID)
	if err != nil {
		return nil, err
	}
	instance, err := c.instanceProvider.Get(ctx, instanceID, options...)
	if err != nil {
		return nil, fmt.Errorf("getting instance, %w", err)
	}
	return instance, nil
}
