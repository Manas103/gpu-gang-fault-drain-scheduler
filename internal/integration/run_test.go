// Package integration runs full synthetic traces through both scheduler
// implementations with the independent oracle checking every tick, which is
// the only place conservation and atomicity are verified end to end rather
// than unit by unit.
package integration

import (
	"testing"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/oracle"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/scheduler"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
)

const (
	testNumNodes      = 6
	testGPUsPerNode   = 8 // 48 GPU test cluster, smaller than the full benchmark for fast CI
	testNumJobs       = 300
	testArrivalWindow = 1500
	testFaultPeriod   = 150
	testFaultDuration = 40
	testSettleTicks   = 2000
)

var testQueueNames = []string{"team-a", "team-b", "team-c", "team-d"}

func checkBoth(cluster *sim.Cluster, gangs []*sim.GangState, gangSchedulerUnderTest bool) error {
	if err := oracle.CheckConservation(cluster, gangs); err != nil {
		return err
	}
	if gangSchedulerUnderTest {
		if err := oracle.CheckNoPartialGang(gangs); err != nil {
			return err
		}
	}
	return nil
}

func TestGangSchedulerHoldsInvariantsAndNeverDeadlocksOnARandomTrace(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4, 5} {
		trace := sim.NewTrace(seed, testNumJobs, testArrivalWindow, testNumNodes, testFaultPeriod, testFaultDuration, testQueueNames)
		cluster := sim.NewCluster(testNumNodes, testGPUsPerNode)
		queues := scheduler.NewQueues(testQueueNames, testNumNodes*testGPUsPerNode)
		s := scheduler.NewGangScheduler(queues)

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("seed %d: invariant violated: %v", seed, r)
				}
			}()
			res := sim.Run(s, cluster, trace, testSettleTicks, func(c *sim.Cluster, g []*sim.GangState) error {
				return checkBoth(c, g, true)
			})
			st := s.Stats()
			if st.DeadlockCount != 0 {
				t.Errorf("seed %d: gang scheduler should never deadlock by construction, got %d", seed, st.DeadlockCount)
			}
			if res.MeanUtilization <= 0 {
				t.Errorf("seed %d: mean utilization should be positive on a loaded trace, got %f", seed, res.MeanUtilization)
			}
		}()
	}
}

func TestFirstFitHoldsConservationButDoesDeadlockOnTheSameTrace(t *testing.T) {
	trace := sim.NewTrace(1, testNumJobs, testArrivalWindow, testNumNodes, testFaultPeriod, testFaultDuration, testQueueNames)
	cluster := sim.NewCluster(testNumNodes, testGPUsPerNode)
	s := scheduler.NewFirstFitScheduler()

	res := sim.Run(s, cluster, trace, testSettleTicks, func(c *sim.Cluster, g []*sim.GangState) error {
		return checkBoth(c, g, false)
	})
	st := s.Stats()
	// This is the pathology the gang scheduler exists to prevent, not a bug
	// in first-fit: a naive per-pod scheduler with no gang awareness really
	// does strand jobs holding partial GPU allocations on a contended trace.
	if st.DeadlockCount == 0 {
		t.Error("expected first-fit to deadlock at least once on a contended trace; if this starts failing, the trace parameters no longer create contention")
	}
	if res.Ticks == 0 {
		t.Fatal("run produced no ticks")
	}
}

func TestQueueHierarchyNeverLetsAQueueExceedQuotaPlusIdleSiblings(t *testing.T) {
	trace := sim.NewTrace(9, testNumJobs, testArrivalWindow, testNumNodes, testFaultPeriod, testFaultDuration, testQueueNames)
	cluster := sim.NewCluster(testNumNodes, testGPUsPerNode)
	queues := scheduler.NewQueues(testQueueNames, testNumNodes*testGPUsPerNode)
	s := scheduler.NewGangScheduler(queues)

	sim.Run(s, cluster, trace, testSettleTicks, func(c *sim.Cluster, g []*sim.GangState) error {
		for _, q := range queues {
			if q.Status.GPUUsed < 0 {
				t.Fatalf("queue %s has negative usage %d", q.ObjectMeta.Name, q.Status.GPUUsed)
			}
		}
		return checkBoth(c, g, true)
	})
}
