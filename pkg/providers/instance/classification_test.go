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

package instance

import (
	"context"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	coretest "sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("capacityTypeFromInstance (rollback classification)", func() {
	gated := func(on bool) context.Context {
		return karpopts.ToContext(context.Background(), coretest.Options(coretest.OptionsFields{FeatureGates: coretest.FeatureGates{ReservedCapacity: lo.ToPtr(on)}}))
	}
	inst := func(covering *string, preference ec2types.CapacityReservationPreference, target *string) ec2types.Instance {
		i := ec2types.Instance{CapacityReservationId: covering, CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{CapacityReservationPreference: preference}}
		if target != nil {
			i.CapacityReservationSpecification.CapacityReservationTarget = &ec2types.CapacityReservationTargetResponse{CapacityReservationId: target}
		}
		return i
	}
	DescribeTable("should classify",
		func(ctx context.Context, i ec2types.Instance, expected string) {
			Expect(capacityTypeFromInstance(ctx, i)).To(Equal(expected))
		},
		Entry("an in-place migrated instance covered by its target as reserved", gated(true), inst(lo.ToPtr("cr-1"), "", lo.ToPtr("cr-1")), karpv1.CapacityTypeReserved),
		Entry("a target not yet covering as on-demand", gated(true), inst(nil, "", lo.ToPtr("cr-1")), karpv1.CapacityTypeOnDemand),
		Entry("coverage by another reservation than the target as on-demand", gated(true), inst(lo.ToPtr("cr-2"), "", lo.ToPtr("cr-1")), karpv1.CapacityTypeOnDemand),
		Entry("an open-ODCR match as on-demand", gated(true), inst(lo.ToPtr("cr-1"), ec2types.CapacityReservationPreferenceOpen, nil), karpv1.CapacityTypeOnDemand),
		Entry("a capacity-reservations-only launch as reserved", gated(true), inst(lo.ToPtr("cr-1"), ec2types.CapacityReservationPreferenceCapacityReservationsOnly, nil), karpv1.CapacityTypeReserved),
		Entry("a migrated instance as on-demand with ReservedCapacity off", gated(false), inst(lo.ToPtr("cr-1"), "", lo.ToPtr("cr-1")), karpv1.CapacityTypeOnDemand),
		Entry("a covered instance without a specification as on-demand", gated(true), ec2types.Instance{CapacityReservationId: lo.ToPtr("cr-1")}, karpv1.CapacityTypeOnDemand),
	)
})
