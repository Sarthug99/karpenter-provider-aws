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

package capacityreservation

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/patrickmn/go-cache"
	clock "k8s.io/utils/clock/testing"
)

var _ = Describe("availabilityCache", func() {
	It("should keep a migrated hold when the sync's API call was issued before coverage", func() {
		clk := clock.NewFakeClock(time.Now())
		c := &availabilityCache{availabilityCache: cache.New(time.Hour, time.Minute), clk: clk}
		requestedAt := clk.Now()
		clk.Step(time.Second)
		// Coverage lands while DescribeCapacityReservations is in flight, so its count may predate it.
		c.MarkMigrated("cr-1", "i-1")
		clk.Step(time.Second)
		c.syncAvailability(map[string]int{"cr-1": 3}, requestedAt)
		Expect(c.GetAvailableInstanceCount("cr-1")).To(Equal(2))

		c.syncAvailability(map[string]int{"cr-1": 2}, clk.Now())
		Expect(c.GetAvailableInstanceCount("cr-1")).To(Equal(2))
	})
	It("should expire migrated holds for reservations no sync returns", func() {
		clk := clock.NewFakeClock(time.Now())
		c := &availabilityCache{availabilityCache: cache.New(time.Hour, time.Minute), clk: clk}
		c.MarkMigrated("cr-unselected", "i-1")
		clk.Step(terminatedInstanceTTL + time.Second)
		c.syncAvailability(map[string]int{"cr-1": 2}, clk.Now())
		Expect(c.migrated).To(BeEmpty())
	})
	It("should drop migrated holds when the count is forced from EC2", func() {
		clk := clock.NewFakeClock(time.Now())
		c := &availabilityCache{availabilityCache: cache.New(time.Hour, time.Minute), terminationCache: cache.New(time.Hour, time.Minute), clk: clk}
		c.syncAvailability(map[string]int{"cr-1": 2}, clk.Now())
		c.MarkMigrated("cr-1", "i-1")
		// A launch hit ReservationCapacityExceeded, then a reserved node terminated.
		c.SetAvailableInstanceCount("cr-1", 0)
		c.MarkTerminated("cr-1", "i-2")
		Expect(c.GetAvailableInstanceCount("cr-1")).To(Equal(1))
	})
})
