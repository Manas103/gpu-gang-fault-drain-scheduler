package scheduler

import (
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

// FirstFitScheduler is the naive baseline: it has no concept of a gang. Each
// tick it walks every unfinished job in arrival order and gives it whatever
// free GPUs exist, one unit at a time, regardless of whether that leaves the
// job's siblings still unplaced. A job only starts its duration timer, and
// so can ever finish, once every one of its units has eventually been
// placed; until then the units it already holds sit idle, unable to do
// useful work and unable to be reclaimed, which is exactly the
// resource-fragmentation deadlock gang scheduling exists to prevent. A node
// fault removes only the units that happened to be on that node: this
// scheduler has no way to know the rest of the job's units should be
// released too, so they are not.
type FirstFitScheduler struct {
	jobs []*sim.GangState // arrival order, never reordered
	done int
}

func NewFirstFitScheduler() *FirstFitScheduler {
	return &FirstFitScheduler{}
}

func (s *FirstFitScheduler) Name() string { return "first-fit" }

func (s *FirstFitScheduler) Submit(job sim.Job) {
	s.jobs = append(s.jobs, &sim.GangState{Job: job, Phase: types.PodGroupPending, Placed: map[int]int{}})
}

func (s *FirstFitScheduler) Tick(tick int, cluster *sim.Cluster) {
	for _, g := range s.jobs {
		if g.Phase == types.PodGroupCompleted {
			continue
		}
		if g.Phase == types.PodGroupRunning {
			if tick >= g.StartTick+g.Job.DurationTicks {
				cluster.ReleaseUnits(g.Job.UID, g.Placed)
				// Clear the now-stale placement map: Snapshot() returns every
				// job ever submitted including completed ones, and a
				// conservation check that summed a completed job's old
				// placements would double-count GPUs that were already
				// released. Found by the integration test's independent
				// oracle, not by inspection.
				g.Placed = map[int]int{}
				g.PlacedCount = 0
				g.Phase = types.PodGroupCompleted
				s.done++
			}
			continue
		}
		remaining := g.Job.MinMember - g.PlacedCount
		avail := cluster.FreeTotal()
		if avail == 0 || remaining == 0 {
			continue
		}
		take := remaining
		if take > avail {
			take = avail
		}
		placed := cluster.PlaceUnits(g.Job.UID, take)
		for nodeID, units := range placed {
			g.Placed[nodeID] += units
		}
		g.PlacedCount += take
		if g.PlacedCount == g.Job.MinMember {
			g.Phase = types.PodGroupRunning
			g.StartTick = tick
		} else {
			g.Phase = types.PodGroupPartial
		}
	}
}

// HandleFault removes only the units this scheduler happened to place on
// the faulted node. It never looks at a job's other placements: that is
// the behavior gap this baseline exists to illustrate.
func (s *FirstFitScheduler) HandleFault(tick, nodeID int, cluster *sim.Cluster) {
	for _, g := range s.jobs {
		units, ok := g.Placed[nodeID]
		if !ok || units == 0 {
			continue
		}
		delete(g.Placed, nodeID)
		delete(cluster.Nodes[nodeID].Placements, g.Job.UID)
		g.PlacedCount -= units
		if g.Phase == types.PodGroupRunning {
			// A job that had fully completed its placement loses progress
			// the instant any of its nodes is lost: it goes back to
			// collecting units one at a time, same as a fresh partial job.
			g.Phase = types.PodGroupPartial
		}
	}
}

// Stats' PartialAtEnd/DeadlockCount are only meaningful once the trace has
// run well past its arrival window with no new jobs arriving: a job that
// is merely Partial mid-trace may still get its remaining units later.
func (s *FirstFitScheduler) Stats() sim.Stats {
	st := sim.Stats{Completed: s.done}
	for _, g := range s.jobs {
		switch g.Phase {
		case types.PodGroupPending:
			st.PendingAtEnd++
		case types.PodGroupPartial:
			st.PartialAtEnd++
			st.DeadlockCount++
		case types.PodGroupRunning:
			st.RunningAtEnd++
		}
	}
	return st
}

func (s *FirstFitScheduler) Snapshot() []*sim.GangState { return s.jobs }
