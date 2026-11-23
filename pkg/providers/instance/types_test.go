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
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	awsoptions "github.com/aws/karpenter-provider-aws/pkg/operator/options"
)

var _ = Describe("capacityTypeFromInstance", func() {
	gatedCtx := func(migration bool) context.Context {
		c := coreoptions.ToContext(context.Background(), coretest.Options(coretest.OptionsFields{FeatureGates: coretest.FeatureGates{ReservedCapacity: lo.ToPtr(true)}}))
		return awsoptions.ToContext(c, &awsoptions.Options{FeatureGates: awsoptions.FeatureGates{ReservedCapacityMigration: migration}})
	}
	targeted := func(target string, covering *string) ec2types.Instance {
		return ec2types.Instance{
			CapacityReservationId: covering,
			CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{
				CapacityReservationTarget: &ec2types.CapacityReservationTargetResponse{CapacityReservationId: lo.ToPtr(target)},
			},
		}
	}
	It("should treat a re-targeted, covered instance as reserved whether or not the migration gate is on", func() {
		Expect(capacityTypeFromInstance(gatedCtx(true), targeted("cr-1", lo.ToPtr("cr-1")))).To(Equal(karpv1.CapacityTypeReserved))
		// Migration is one-way; turning the gate off must not demote nodes EC2 still covers.
		Expect(capacityTypeFromInstance(gatedCtx(false), targeted("cr-1", lo.ToPtr("cr-1")))).To(Equal(karpv1.CapacityTypeReserved))
	})
	It("should treat a re-targeted instance that isn't covered yet as on-demand", func() {
		Expect(capacityTypeFromInstance(gatedCtx(true), targeted("cr-1", nil))).To(Equal(karpv1.CapacityTypeOnDemand))
		Expect(capacityTypeFromInstance(gatedCtx(true), targeted("cr-2", lo.ToPtr("cr-1")))).To(Equal(karpv1.CapacityTypeOnDemand))
	})
	It("should keep treating open-ODCR instances as on-demand", func() {
		i := ec2types.Instance{
			CapacityReservationId:            lo.ToPtr("cr-1"),
			CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{CapacityReservationPreference: ec2types.CapacityReservationPreferenceOpen},
		}
		Expect(capacityTypeFromInstance(gatedCtx(true), i)).To(Equal(karpv1.CapacityTypeOnDemand))
	})
	It("should keep treating capacity-reservations-only instances as reserved", func() {
		i := ec2types.Instance{
			CapacityReservationId:            lo.ToPtr("cr-1"),
			CapacityReservationSpecification: &ec2types.CapacityReservationSpecificationResponse{CapacityReservationPreference: ec2types.CapacityReservationPreferenceCapacityReservationsOnly},
		}
		Expect(capacityTypeFromInstance(gatedCtx(false), i)).To(Equal(karpv1.CapacityTypeReserved))
	})
	It("should tolerate a missing reservation specification", func() {
		Expect(capacityTypeFromInstance(gatedCtx(true), ec2types.Instance{CapacityReservationId: lo.ToPtr("cr-1")})).To(Equal(karpv1.CapacityTypeOnDemand))
	})
	It("should record the raw reservation state", func() {
		i := targeted("cr-1", lo.ToPtr("cr-1"))
		i.PlatformDetails = lo.ToPtr("Linux/UNIX")
		i.InstanceLifecycle = ec2types.InstanceLifecycleTypeCapacityBlock
		Expect(capacityReservationStateFromInstance(i)).To(Equal(CapacityReservationState{
			TargetID: "cr-1", CoveringID: "cr-1", Platform: "Linux/UNIX", Lifecycle: ec2types.InstanceLifecycleTypeCapacityBlock,
		}))
	})
})
