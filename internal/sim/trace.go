package sim

import (
	"fmt"
	"math/rand"
)

// Job is one arriving gang scheduling request, independent of any scheduler.
type Job struct {
	UID           string
	ArrivalTick   int
	MinMember     int // total GPUs the gang needs, one per pod
	DurationTicks int
	QueueName     string
}

// Fault is one scripted node outage, independent of any scheduler, so that
// every scheduler under test sees the exact same fault timeline.
type Fault struct {
	Tick        int
	NodeID      int
	RepairDelay int
}

// Trace is a fixed, reproducible workload: jobs and faults, generated once
// from a seed and replayed identically against every scheduler under test.
type Trace struct {
	Jobs    []Job
	Faults  []Fault
	Queues  []string
	NumJobs int
}

// gangSizeBuckets mirrors a realistic mixed-size job population: mostly small
// jobs (single GPU to a small multi-GPU job) with a long tail of large
// multi-node distributed training gangs.
var gangSizeBuckets = []struct {
	size   int
	weight int
}{
	{1, 35}, {2, 20}, {4, 18}, {8, 14}, {16, 8}, {24, 3}, {32, 2},
}

func pickGangSize(r *rand.Rand) int {
	total := 0
	for _, b := range gangSizeBuckets {
		total += b.weight
	}
	roll := r.Intn(total)
	for _, b := range gangSizeBuckets {
		if roll < b.weight {
			return b.size
		}
		roll -= b.weight
	}
	return gangSizeBuckets[0].size
}

// NewTrace generates numJobs mixed-size gangs arriving over arrivalWindow
// ticks across the given queues, plus a fault every faultPeriod ticks on
// average, each draining for faultDuration ticks, all from one seed so the
// same call always reproduces the same trace.
func NewTrace(seed int64, numJobs, arrivalWindow, numNodes, faultPeriod, faultDuration int, queues []string) *Trace {
	r := rand.New(rand.NewSource(seed))
	t := &Trace{Queues: queues, NumJobs: numJobs}

	arrivals := make([]int, numJobs)
	for i := range arrivals {
		arrivals[i] = r.Intn(arrivalWindow)
	}
	// Sort arrivals so the trace is naturally time-ordered (simple insertion
	// sort; numJobs here is small enough that O(n^2) is not a concern and it
	// avoids pulling in "sort" purely for a trace generator).
	for i := 1; i < len(arrivals); i++ {
		for j := i; j > 0 && arrivals[j-1] > arrivals[j]; j-- {
			arrivals[j-1], arrivals[j] = arrivals[j], arrivals[j-1]
		}
	}

	for i := 0; i < numJobs; i++ {
		t.Jobs = append(t.Jobs, Job{
			UID:           fmt.Sprintf("job-%05d", i),
			ArrivalTick:   arrivals[i],
			MinMember:     pickGangSize(r),
			DurationTicks: 20 + r.Intn(180),
			QueueName:     queues[r.Intn(len(queues))],
		})
	}

	for tick := 0; tick < arrivalWindow; tick += faultPeriod {
		jitter := r.Intn(faultPeriod)
		t.Faults = append(t.Faults, Fault{
			Tick:        tick + jitter,
			NodeID:      r.Intn(numNodes),
			RepairDelay: faultDuration,
		})
	}
	return t
}
