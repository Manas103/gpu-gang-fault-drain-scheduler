package sim

import "github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"

// GangState is the runtime record for one arrived job, shared by every
// scheduler implementation so the simulation runner and the independent
// oracle can both inspect it the same way regardless of admission policy.
type GangState struct {
	Job          Job
	Phase        types.PodGroupPhase
	Placed       map[int]int // nodeID -> units of this gang held there
	PlacedCount  int
	StartTick    int
	RequeueCount int
}

// Stats summarizes one scheduler's run at the end of a trace.
type Stats struct {
	Completed     int
	Requeued      int // whole-gang fault requeues (gang scheduler only, 0 for first-fit)
	DeadlockCount int
	PendingAtEnd  int
	PartialAtEnd  int // stuck holding some but not all required GPUs (first-fit only)
	RunningAtEnd  int
}

// Scheduler is the interface Run drives identically for every admission
// policy under test, so the comparison is fair: same trace, same fault
// timeline, same cluster size, different scheduling code.
type Scheduler interface {
	Name() string
	Submit(job Job)
	Tick(tick int, cluster *Cluster)
	HandleFault(tick, nodeID int, cluster *Cluster)
	Stats() Stats
	Snapshot() []*GangState
}
