package sim

import "github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"

// InvariantFunc lets Run call out to an independent checker after every
// tick. It is passed both the cluster and every gang the scheduler is
// currently tracking, since a conservation check needs to recompute
// occupancy from the gangs' own placements, independent of the scheduler's
// internal bookkeeping.
type InvariantFunc func(cluster *Cluster, gangs []*GangState) error

// Result is what one full run of a scheduler against a trace produced.
// Two utilization series are kept because they disagree in an informative
// way (see README "Findings"): RawUtilization counts every GPU the
// scheduler is holding, including one held by a job that is stuck Partial
// and not making progress. EffectiveUtilization counts only GPUs held by a
// gang in its Running phase, i.e. genuinely doing work. For the gang
// scheduler the two are identical by construction (nothing is ever held
// outside Running); for first-fit they are not, and the gap between them
// is the wasted-GPU cost of admitting a job without its whole gang.
type Result struct {
	Ticks                    int
	UtilizationSamples       []float64
	MeanUtilization          float64
	PeakUtilization          float64
	EffectiveUtilSamples     []float64
	MeanEffectiveUtilization float64
	PeakEffectiveUtilization float64
}

// Run drives scheduler s through every arrival and fault in trace, for
// settleTicks beyond the trace's own arrival window so that anything still
// unfinished at the end genuinely cannot make further progress, not merely
// that the trace ran out of time. checkInvariant, if non-nil, is called
// after every tick and panics the run on the first violation, since a
// violated invariant invalidates every measurement that follows it.
func Run(s Scheduler, cluster *Cluster, trace *Trace, settleTicks int, checkInvariant InvariantFunc) Result {
	totalTicks := trace.arrivalWindow() + settleTicks

	arrivalsByTick := make(map[int][]Job)
	for _, j := range trace.Jobs {
		arrivalsByTick[j.ArrivalTick] = append(arrivalsByTick[j.ArrivalTick], j)
	}
	faultsByTick := make(map[int][]Fault)
	for _, f := range trace.Faults {
		faultsByTick[f.Tick] = append(faultsByTick[f.Tick], f)
	}

	capacity := cluster.TotalCapacity()
	res := Result{
		UtilizationSamples:   make([]float64, 0, totalTicks),
		EffectiveUtilSamples: make([]float64, 0, totalTicks),
	}

	for tick := 0; tick < totalTicks; tick++ {
		for _, j := range arrivalsByTick[tick] {
			s.Submit(j)
		}
		for _, f := range faultsByTick[tick] {
			s.HandleFault(tick, f.NodeID, cluster)
			cluster.Drain(f.NodeID, tick+f.RepairDelay)
		}
		cluster.Repair(tick)
		s.Tick(tick, cluster)

		snapshot := s.Snapshot()
		if checkInvariant != nil {
			if err := checkInvariant(cluster, snapshot); err != nil {
				panic(err)
			}
		}

		u := float64(cluster.Allocated()) / float64(capacity)
		res.UtilizationSamples = append(res.UtilizationSamples, u)
		if u > res.PeakUtilization {
			res.PeakUtilization = u
		}

		effective := 0
		for _, g := range snapshot {
			if g.Phase == types.PodGroupRunning {
				effective += g.PlacedCount
			}
		}
		eu := float64(effective) / float64(capacity)
		res.EffectiveUtilSamples = append(res.EffectiveUtilSamples, eu)
		if eu > res.PeakEffectiveUtilization {
			res.PeakEffectiveUtilization = eu
		}
	}

	sum, effSum := 0.0, 0.0
	for _, u := range res.UtilizationSamples {
		sum += u
	}
	for _, u := range res.EffectiveUtilSamples {
		effSum += u
	}
	res.Ticks = totalTicks
	res.MeanUtilization = sum / float64(len(res.UtilizationSamples))
	res.MeanEffectiveUtilization = effSum / float64(len(res.EffectiveUtilSamples))
	return res
}

func (t *Trace) arrivalWindow() int {
	max := 0
	for _, j := range t.Jobs {
		if j.ArrivalTick > max {
			max = j.ArrivalTick
		}
	}
	return max
}
