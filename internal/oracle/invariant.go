// Package oracle holds checks written independently of the scheduler
// implementations, so a bug shared between a scheduler and its own
// bookkeeping cannot also hide inside the thing checking it.
package oracle

import (
	"fmt"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

// CheckConservation recomputes, from scratch, every node's occupied GPU
// count as the sum of every gang's recorded placement on it, and compares
// that against cluster.Node.Free, independent of whatever bookkeeping the
// scheduler under test did. It also checks the cluster-wide total: free
// plus allocated must always equal the fixed physical capacity.
func CheckConservation(cluster *sim.Cluster, gangs []*sim.GangState) error {
	occupied := make(map[int]int, len(cluster.Nodes))
	for _, g := range gangs {
		for nodeID, units := range g.Placed {
			if units < 0 {
				return fmt.Errorf("gang %s has negative placement %d on node %d", g.Job.UID, units, nodeID)
			}
			occupied[nodeID] += units
		}
	}
	for _, n := range cluster.Nodes {
		want := n.Total - occupied[n.ID]
		if !n.Drained && want != n.Free {
			return fmt.Errorf("node %d: recomputed free %d from gang placements disagrees with cluster.Free %d (total %d, occupied %d)",
				n.ID, want, n.Free, n.Total, occupied[n.ID])
		}
		if occupied[n.ID] > n.Total {
			return fmt.Errorf("node %d: occupied %d exceeds total capacity %d", n.ID, occupied[n.ID], n.Total)
		}
	}
	free, alloc := cluster.FreeTotal(), cluster.Allocated()
	if free+alloc != cluster.TotalCapacity()-drainedFree(cluster) {
		// drainedFree accounts for capacity sitting idle on a drained node,
		// which FreeTotal deliberately excludes but TotalCapacity does not.
		return fmt.Errorf("cluster-wide free (%d) + allocated (%d) + drained-idle (%d) != total capacity (%d)",
			free, alloc, drainedFree(cluster), cluster.TotalCapacity())
	}
	return nil
}

func drainedFree(cluster *sim.Cluster) int {
	d := 0
	for _, n := range cluster.Nodes {
		if n.Drained {
			d += n.Free
		}
	}
	return d
}

// CheckNoPartialGang fails if any gang in Running phase holds a GPU count
// other than exactly its MinMember: the atomicity invariant the gang
// scheduler is supposed to guarantee and the whole reason this repo exists.
// It is meaningless for the first-fit baseline, which has a Partial phase
// precisely because it does not try to hold this invariant; callers should
// only run it against GangScheduler snapshots.
func CheckNoPartialGang(gangs []*sim.GangState) error {
	for _, g := range gangs {
		if g.Phase != types.PodGroupRunning {
			continue
		}
		if g.PlacedCount != g.Job.MinMember {
			return fmt.Errorf("gang %s is Running with %d/%d GPUs placed, atomicity invariant violated",
				g.Job.UID, g.PlacedCount, g.Job.MinMember)
		}
		sum := 0
		for _, units := range g.Placed {
			sum += units
		}
		if sum != g.PlacedCount {
			return fmt.Errorf("gang %s: per-node placements sum to %d but PlacedCount says %d", g.Job.UID, sum, g.PlacedCount)
		}
	}
	return nil
}
