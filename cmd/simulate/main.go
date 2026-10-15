// Command simulate runs the fixed 2,000-job benchmark trace through both the
// gang scheduler and the naive first-fit baseline and prints a comparison.
// This is the exact command docs/benchmark_output.txt was captured from.
package main

import (
	"flag"
	"fmt"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/oracle"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/scheduler"
	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
)

const (
	numNodes      = 18
	gpusPerNode   = 8 // 144 GPU simulated cluster
	numJobs       = 2000
	arrivalWindow = 8000
	faultPeriod   = 100
	faultDuration = 120
	settleTicks   = 3000
	seed          = int64(20270715)
)

var queueNames = []string{"vision", "nlp", "rl", "sim-infra"}

func main() {
	seedFlag := flag.Int64("seed", seed, "trace seed")
	flag.Parse()

	trace := sim.NewTrace(*seedFlag, numJobs, arrivalWindow, numNodes, faultPeriod, faultDuration, queueNames)
	fmt.Printf("trace: %d jobs, arrival window %d ticks, cluster %dx%d=%d GPUs, %d faults\n",
		len(trace.Jobs), arrivalWindow, numNodes, gpusPerNode, numNodes*gpusPerNode, len(trace.Faults))
	fmt.Println()

	runGang(trace)
	fmt.Println()
	runFirstFit(trace)
}

func runGang(trace *sim.Trace) {
	cluster := sim.NewCluster(numNodes, gpusPerNode)
	queues := scheduler.NewQueues(queueNames, numNodes*gpusPerNode)
	s := scheduler.NewGangScheduler(queues)

	res := sim.Run(s, cluster, trace, settleTicks, func(c *sim.Cluster, g []*sim.GangState) error {
		if err := oracle.CheckConservation(c, g); err != nil {
			return err
		}
		return oracle.CheckNoPartialGang(g)
	})
	st := s.Stats()
	fmt.Println("== gang scheduler + fault-drain controller ==")
	printStats(st, res)
}

func runFirstFit(trace *sim.Trace) {
	cluster := sim.NewCluster(numNodes, gpusPerNode)
	s := scheduler.NewFirstFitScheduler()

	res := sim.Run(s, cluster, trace, settleTicks, func(c *sim.Cluster, g []*sim.GangState) error {
		return oracle.CheckConservation(c, g)
	})
	st := s.Stats()
	fmt.Println("== first-fit baseline (no gang awareness) ==")
	printStats(st, res)
}

func printStats(st sim.Stats, res sim.Result) {
	fmt.Printf("  completed:            %d\n", st.Completed)
	fmt.Printf("  whole-gang requeues:  %d\n", st.Requeued)
	fmt.Printf("  deadlocked gangs:     %d\n", st.DeadlockCount)
	fmt.Printf("  pending at end:       %d\n", st.PendingAtEnd)
	fmt.Printf("  partial at end:       %d\n", st.PartialAtEnd)
	fmt.Printf("  running at end:       %d\n", st.RunningAtEnd)
	fmt.Printf("  mean raw allocation:  %.2f%% (includes GPUs held by a stuck Partial gang, if any)\n", res.MeanUtilization*100)
	fmt.Printf("  peak raw allocation:  %.2f%%\n", res.PeakUtilization*100)
	fmt.Printf("  mean effective alloc: %.2f%% (GPUs held by a fully-placed, Running gang only)\n", res.MeanEffectiveUtilization*100)
	fmt.Printf("  peak effective alloc: %.2f%%\n", res.PeakEffectiveUtilization*100)
	fmt.Printf("  simulated ticks:      %d\n", res.Ticks)
}
