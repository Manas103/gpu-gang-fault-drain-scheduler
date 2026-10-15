package scheduler

import (
	"testing"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

func TestGangSchedulerNeverPartiallyAdmits(t *testing.T) {
	cluster := sim.NewCluster(1, 4) // 4 GPUs total
	queues := NewQueues([]string{"q"}, 4)
	s := NewGangScheduler(queues)
	s.Submit(sim.Job{UID: "too-big", MinMember: 5, DurationTicks: 10, QueueName: "q"})

	for tick := 0; tick < 20; tick++ {
		s.Tick(tick, cluster)
	}
	st := s.Stats()
	if st.RunningAtEnd != 0 || st.PendingAtEnd != 1 {
		t.Fatalf("a gang that can never fit should stay Pending forever, got running=%d pending=%d", st.RunningAtEnd, st.PendingAtEnd)
	}
	for _, g := range s.Snapshot() {
		if g.PlacedCount != 0 {
			t.Fatalf("an inadmissible gang must hold 0 GPUs, held %d", g.PlacedCount)
		}
	}
}

func TestGangSchedulerAdmitsAtomicallyWhenItFits(t *testing.T) {
	cluster := sim.NewCluster(2, 4) // 8 GPUs total
	queues := map[string]*types.Queue{"q": mkQueue("q", 8, 0)}
	s := NewGangScheduler(queues)
	s.Submit(sim.Job{UID: "fits", MinMember: 6, DurationTicks: 10, QueueName: "q"})

	s.Tick(0, cluster)
	st := s.Stats()
	if st.RunningAtEnd != 1 {
		t.Fatalf("a gang that fits should be admitted on the first tick it can, got running=%d", st.RunningAtEnd)
	}
	if cluster.FreeTotal() != 2 {
		t.Fatalf("want 2 GPUs free after admitting a 6-unit gang into 8, got %d", cluster.FreeTotal())
	}
}

func TestGangSchedulerOverQuotaReclaim(t *testing.T) {
	cluster := sim.NewCluster(1, 20)
	queues := map[string]*types.Queue{
		"a": mkQueue("a", 8, 0),
		"b": mkQueue("b", 8, 0), // idle, a will borrow from it
	}
	s := NewGangScheduler(queues)
	s.Submit(sim.Job{UID: "over-quota", MinMember: 12, DurationTicks: 10, QueueName: "a"}) // over a's own 8-unit quota

	s.Tick(0, cluster)
	if s.Stats().RunningAtEnd != 1 {
		t.Fatal("a 12-unit request from an 8-quota queue should be admitted by borrowing idle capacity from an idle sibling")
	}
}

func TestGangSchedulerOverQuotaBlockedWhenSiblingBusy(t *testing.T) {
	cluster := sim.NewCluster(1, 20)
	queues := map[string]*types.Queue{
		"a": mkQueue("a", 8, 0),
		"b": mkQueue("b", 8, 8), // fully busy, nothing to lend
	}
	s := NewGangScheduler(queues)
	s.Submit(sim.Job{UID: "over-quota", MinMember: 12, DurationTicks: 10, QueueName: "a"})

	s.Tick(0, cluster)
	if s.Stats().RunningAtEnd != 0 {
		t.Fatal("an over-quota request must be blocked once siblings are no longer idle, even with physical capacity free")
	}
}

func TestGangSchedulerFaultRequeuesWholeGangWithNoPartialWork(t *testing.T) {
	cluster := sim.NewCluster(2, 4) // node 0 and node 1, 4 GPUs each
	queues := map[string]*types.Queue{"q": mkQueue("q", 8, 0)}
	s := NewGangScheduler(queues)
	s.Submit(sim.Job{UID: "spans-two-nodes", MinMember: 6, DurationTicks: 100, QueueName: "q"})
	s.Tick(0, cluster) // fills node 0 (4) and part of node 1 (2)

	if s.Stats().RunningAtEnd != 1 {
		t.Fatal("setup failed: gang did not reach Running")
	}
	before := s.Snapshot()[0].Placed
	if before[0] != 4 || before[1] != 2 {
		t.Fatalf("setup failed: expected placement across both nodes, got %v", before)
	}

	s.HandleFault(5, 0, cluster) // node 0 goes down; the gang only had a *direct* hit there
	cluster.Drain(0, 50)

	st := s.Stats()
	if st.RunningAtEnd != 0 || st.PendingAtEnd != 1 || st.Requeued != 1 {
		t.Fatalf("expected the whole gang requeued, got running=%d pending=%d requeued=%d", st.RunningAtEnd, st.PendingAtEnd, st.Requeued)
	}
	g := s.Snapshot()[0]
	if g.PlacedCount != 0 || len(g.Placed) != 0 {
		t.Fatalf("a faulted gang must release every node it held, including the healthy one, got placed=%v count=%d", g.Placed, g.PlacedCount)
	}
	if cluster.Nodes[1].Free != 4 {
		t.Fatalf("node 1 was healthy and must be fully freed by the whole-gang requeue, got free=%d", cluster.Nodes[1].Free)
	}
	if g.RequeueCount != 1 {
		t.Fatalf("want RequeueCount 1, got %d", g.RequeueCount)
	}
}
