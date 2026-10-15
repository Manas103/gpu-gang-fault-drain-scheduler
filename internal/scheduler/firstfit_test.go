package scheduler

import (
	"testing"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

func TestFirstFitLeavesAPartialGangHoldingResourcesForever(t *testing.T) {
	cluster := sim.NewCluster(1, 4)
	s := NewFirstFitScheduler()
	s.Submit(sim.Job{UID: "too-big", MinMember: 6, DurationTicks: 10, QueueName: "q"})

	for tick := 0; tick < 50; tick++ {
		s.Tick(tick, cluster)
	}
	g := s.Snapshot()[0]
	if g.Phase != types.PodGroupPartial {
		t.Fatalf("a 6-unit gang on a 4-GPU cluster can never complete; want Partial, got %s", g.Phase)
	}
	if g.PlacedCount != 4 {
		t.Fatalf("want all 4 available GPUs grabbed and held, got %d", g.PlacedCount)
	}
	if cluster.FreeTotal() != 0 {
		t.Fatalf("the held GPUs are never released; want 0 free, got %d", cluster.FreeTotal())
	}
	if s.Stats().DeadlockCount != 1 {
		t.Fatalf("want 1 deadlocked gang, got %d", s.Stats().DeadlockCount)
	}
}

func TestFirstFitSecondJobCanStillGetScrapsAheadOfAStuckFirstJob(t *testing.T) {
	// This is the fragmentation behavior that distinguishes first-fit from
	// the gang scheduler: because first-fit has no concept of a gang, a
	// later, smaller job can and does receive units while an earlier,
	// larger job is still stuck Partial, interleaving unrelated jobs.
	cluster := sim.NewCluster(1, 4)
	s := NewFirstFitScheduler()
	s.Submit(sim.Job{UID: "big", MinMember: 6, DurationTicks: 10, QueueName: "q"})
	s.Tick(0, cluster) // big grabs all 4, still short by 2, stuck Partial

	s.Submit(sim.Job{UID: "small", MinMember: 1, DurationTicks: 10, QueueName: "q"})
	s.Tick(1, cluster) // no free capacity left for small either: both now stuck

	big, small := s.Snapshot()[0], s.Snapshot()[1]
	if big.Phase != types.PodGroupPartial || small.Phase != types.PodGroupPending {
		t.Fatalf("got big=%s small=%s", big.Phase, small.Phase)
	}
}

func TestFirstFitFaultOnlyReleasesTheFaultedNodesShare(t *testing.T) {
	cluster := sim.NewCluster(2, 4)
	s := NewFirstFitScheduler()
	s.Submit(sim.Job{UID: "spans-two-nodes", MinMember: 6, DurationTicks: 100, QueueName: "q"})
	s.Tick(0, cluster) // 4 on node 0, 2 on node 1, Running

	if s.Snapshot()[0].Phase != types.PodGroupRunning {
		t.Fatal("setup failed: job did not reach Running")
	}
	s.HandleFault(5, 0, cluster)
	cluster.Drain(0, 50)

	g := s.Snapshot()[0]
	if g.Phase != types.PodGroupPartial {
		t.Fatalf("losing node 0 should knock the job back to Partial, got %s", g.Phase)
	}
	if g.PlacedCount != 2 {
		t.Fatalf("the job should still be holding its 2 units on the healthy node 1, got %d", g.PlacedCount)
	}
	if _, stillThere := g.Placed[1]; !stillThere {
		t.Fatal("first-fit must leave the healthy node's placement untouched: it never looks at a job's other nodes")
	}
}
