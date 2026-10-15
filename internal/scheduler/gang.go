package scheduler

import (
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

// GangScheduler admits a job's entire pod group atomically, or not at all:
// it never leaves a gang holding a strict subset of the GPUs it needs. A
// fault-drain controller watches for a simulated fatal Xid class on a node
// and, for every gang with any placement there, releases the whole gang
// (every node, not just the faulted one) and requeues it with priority,
// exactly once per pending slot so no partial work survives a fault.
type GangScheduler struct {
	queues   map[string]*types.Queue
	pending  []*sim.GangState // FIFO by arrival, backfill-style: a blocked head does not block the rest
	running  []*sim.GangState
	done     int
	requeued int
}

func NewGangScheduler(queues map[string]*types.Queue) *GangScheduler {
	return &GangScheduler{queues: queues}
}

func (s *GangScheduler) Name() string { return "gang" }

func (s *GangScheduler) Submit(job sim.Job) {
	s.pending = append(s.pending, &sim.GangState{Job: job, Phase: types.PodGroupPending})
}

func (s *GangScheduler) Tick(tick int, cluster *sim.Cluster) {
	// 1. Release gangs whose duration has elapsed.
	still := s.running[:0]
	for _, g := range s.running {
		if tick >= g.StartTick+g.Job.DurationTicks {
			cluster.ReleaseUnits(g.Job.UID, g.Placed)
			s.queues[g.Job.QueueName].Status.GPUUsed -= g.PlacedCount
			// See the equivalent fix in firstfit.go: clear stale placement
			// bookkeeping on completion rather than relying on Snapshot()
			// to filter it out.
			g.Placed = map[int]int{}
			g.PlacedCount = 0
			g.Phase = types.PodGroupCompleted
			s.done++
		} else {
			still = append(still, g)
		}
	}
	s.running = still

	// 2. Attempt admission over pending, in arrival order, skipping (not
	// blocking on) anything that does not currently fit.
	leftover := s.pending[:0]
	for _, g := range s.pending {
		need := g.Job.MinMember
		q := s.queues[g.Job.QueueName]
		free := cluster.FreeTotal()
		if free >= need && CanAdmit(q, s.queues, need, free) {
			placed := cluster.PlaceUnits(g.Job.UID, need)
			g.Placed = placed
			g.PlacedCount = need
			g.Phase = types.PodGroupRunning
			g.StartTick = tick
			q.Status.GPUUsed += need
			s.running = append(s.running, g)
		} else {
			leftover = append(leftover, g)
		}
	}
	s.pending = leftover
}

// HandleFault must be called before the cluster records the node as
// drained, so placements on it are still visible. Every affected gang is
// released in full, across every node it occupied, and pushed back to the
// front of pending so it gets first shot at the next tick's admission pass.
func (s *GangScheduler) HandleFault(tick, nodeID int, cluster *sim.Cluster) {
	still := s.running[:0]
	var requeue []*sim.GangState
	for _, g := range s.running {
		if g.Placed[nodeID] > 0 {
			cluster.ReleaseUnits(g.Job.UID, g.Placed)
			s.queues[g.Job.QueueName].Status.GPUUsed -= g.PlacedCount
			g.Placed = nil
			g.PlacedCount = 0
			g.Phase = types.PodGroupPending
			g.RequeueCount++
			s.requeued++
			requeue = append(requeue, g)
		} else {
			still = append(still, g)
		}
	}
	s.running = still
	s.pending = append(requeue, s.pending...)
}

func (s *GangScheduler) Stats() sim.Stats {
	// By construction a running gang here always holds exactly its full
	// requirement; this loop is the live form of the oracle invariant in
	// internal/oracle, kept here too so Stats is self-checking rather than
	// trusting the invariant held without ever looking again.
	deadlock := 0
	for _, g := range s.running {
		if g.PlacedCount != g.Job.MinMember {
			deadlock++
		}
	}
	return sim.Stats{
		Completed:     s.done,
		Requeued:      s.requeued,
		DeadlockCount: deadlock,
		PendingAtEnd:  len(s.pending),
		RunningAtEnd:  len(s.running),
	}
}

func (s *GangScheduler) Snapshot() []*sim.GangState {
	all := make([]*sim.GangState, 0, len(s.pending)+len(s.running))
	all = append(all, s.pending...)
	all = append(all, s.running...)
	return all
}
