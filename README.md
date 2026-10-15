# Gang Scheduler and Fault-Drain Controller for GPU Clusters

A Go simulator comparing two GPU cluster admission-control policies over the
same synthetic 2,000-job trace: an atomic gang scheduler with hierarchical
queue quotas and a fault-drain controller, against a naive first-fit
baseline that schedules pods one at a time with no idea a gang exists. Every
number below was measured on this machine by running `cmd/simulate` and
`go test`, not targeted; where a claim going into this project was not met,
that is said plainly rather than tuned away.

## Why this exists

Real GPU schedulers (NVIDIA's KAI Scheduler, Kubernetes' Kueue, Volcano) all
solve the same base problem: a distributed training job needs every one of
its workers scheduled together, or none of them are doing useful work. A
scheduler that doesn't know this will happily start half a job's workers,
hold their GPUs indefinitely waiting for the rest, and starve everyone else.
This project builds the smallest version of that problem that still has the
pathology, a fault-drain controller on top of it, and a second scheduler
that lacks gang-awareness on purpose, so the contrast is measured rather
than asserted.

## What this is NOT

- **Not a live Kubernetes controller.** `internal/types` defines PodGroup
  and Queue as TypeMeta/ObjectMeta/Spec/Status structs shaped like
  Kubernetes custom resources, and `config/crd/*.yaml` are the matching
  CustomResourceDefinition manifests, but neither is registered against a
  real kube-apiserver anywhere in this repo. There is no controller-runtime
  reconcile loop and no generated DeepCopy; the CRD YAML was hand-written to
  document the schema, not produced by `controller-gen`.
- **Not a real fault.** "Draining a node on a fatal Xid class" means the
  simulator marks a node unschedulable and evicts whatever it was running,
  the same shape of event DCGM's `DCGM_FI_DEV_XID_ERRORS` field reports for
  a real GPU hang or double-bit ECC error. No GPU is actually faulted.
- **Not per-node bin-packing.** Every pod requests exactly one GPU and a
  gang's size is just its GPU count; the simulator places GPU units
  anywhere free, not whole multi-GPU pods on a single node. Real
  multi-GPU-per-pod placement constraints are not modeled.
- **Hierarchy is two levels, not arbitrarily deep.** Queues are a flat set
  of siblings under one implicit root. "Hierarchical" here means a tree
  with one level of named queues and quota borrowing between them, not
  nested sub-teams.
- **No preemption.** Over-quota "reclaim" is admission-time only: a lender
  queue always gets to use up to its own quota immediately, but this repo
  never evicts an already-running over-quota borrower to make room. See
  Findings for what that costs.
- **"Deadlock" is an operational, settle-period-bounded definition,** not a
  mathematical impossibility proof. See Findings.

**Machine and toolchain:** Windows 11 host, WSL2 Ubuntu 22.04, 12 logical
cores (WSL-capped via `.wslconfig`), Go 1.26.9 (this repo's `go.mod`
requires 1.26 for `prometheus/client_golang`; Go's own toolchain manager
downloaded it automatically over the stock go1.18.1 on this machine's
`PATH`, see Building and running). Docker 29.1.3 confirmed to build and run
the image in `Dockerfile`.

## Architecture

```
cmd/simulate/main.go        the 2,000-job benchmark this README's numbers came from
internal/types/types.go     PodGroup, Queue: CRD-shaped API objects, hand-rolled
internal/sim/
  cluster.go                Node/Cluster: GPU accounting, drain/repair
  trace.go                  seeded synthetic job + fault trace generator
  gang.go                   GangState, Stats, the Scheduler interface
  runner.go                 Run(): drives one scheduler through one trace
internal/scheduler/
  queue.go                  CanAdmit: hierarchical quota + over-quota reclaim rule
  gang.go                   GangScheduler: atomic admission, fault-drain controller
  firstfit.go               FirstFitScheduler: naive per-pod baseline
internal/oracle/invariant.go  conservation + atomicity checks independent of either scheduler
internal/metricsserver/server.go  real prometheus/client_golang /metrics handler
internal/integration/run_test.go  full-trace property tests tying everything together
config/crd/*.yaml            CustomResourceDefinition manifests matching internal/types
docs/benchmark_output.txt    raw output of `go run ./cmd/simulate`
docs/test_output.txt         raw output of `go test -v -race ./...`
```

### Why every pod is a 1-GPU unit

Modeling pods as fungible 1-GPU units (rather than variable multi-GPU pods
placed on specific nodes) means a gang's only scheduling question is "does
enough total capacity exist," not "does a single node have room for this
pod." That is a real simplification, and it has a consequence worth stating
up front: with a single fungible resource pool, nothing is ever
*permanently* unplaceable as long as total capacity exceeds total eventual
demand, because any freed unit anywhere satisfies anyone's shortfall. See
Findings for what this does to the "deadlock" measurement.

### The atomic admission rule (`GangScheduler.Tick`)

A gang is checked and placed in the same pass: `cluster.FreeTotal() >= need
&& CanAdmit(...)` is evaluated, and only if both hold does
`cluster.PlaceUnits` commit it. There is no code path that places part of a
gang and leaves the rest for later. This is the one property the whole
repo exists to demonstrate, so `internal/oracle.CheckNoPartialGang` asserts
it after every tick in both the integration tests and the benchmark run
itself (see Validation), not just in a unit test.

### Over-quota reclaim without preemption (`scheduler.CanAdmit`)

A queue may always admit up to its own `GPUQuota`. Above that, it may
borrow only the sum of idle guaranteed quota currently sitting unused in
sibling queues:

```go
overage := q.Status.GPUUsed + need - q.Spec.GPUQuota
idle := sum over siblings s of max(0, s.Spec.GPUQuota - s.Status.GPUUsed)
return overage <= idle
```

The lender's own admission check never looks at its siblings at all, so a
lender can always reclaim up to its own quota regardless of who is
currently borrowing from it; see `TestCanAdmitLenderPriorityIsNotBlockedByABorrower`.
What this design does **not** do is evict GPUs a borrower is already
holding when the lender comes back, because that would mean preempting a
job mid-run, which this repo deliberately does not implement. The
consequence is measured in Findings, not hidden.

### Backfill-style FIFO, not strict head-of-line blocking

Both schedulers walk their pending work in arrival order each tick, but
neither one *blocks* on the head: if the oldest pending gang does not fit
this tick, the loop tries the next one anyway. This is what lets smaller
jobs fill in around a large one that is still waiting, in both schedulers,
which is realistic (a real cluster scheduler backfills) and also why the
contrast between the two policies is about *how* they fill in, not whether
they try.

### The fault-drain asymmetry (`GangScheduler.HandleFault` vs. `FirstFitScheduler.HandleFault`)

Both are called with the same fault trace, generated once from the trace
seed before either scheduler runs, so the comparison sees identical fault
timing and location. What they do with it differs on purpose:

- **Gang scheduler:** for every gang with *any* placement on the faulted
  node, release the *entire* gang's placement, on every node it held, not
  just the faulted one, and requeue it to the front of pending. No partial
  work survives a fault.
- **First-fit:** only the units physically on the faulted node are released.
  A job's placements on healthy nodes are left exactly as they were,
  because first-fit has no concept of "this job" to go clean up after;
  it only ever sees individual GPU units.

`TestGangSchedulerFaultRequeuesWholeGangWithNoPartialWork` and
`TestFirstFitFaultOnlyReleasesTheFaultedNodesShare` build the same
two-node, split-gang scenario and assert the two different outcomes
directly against each other.

## Validation

Four independent layers, in increasing order of scope:

1. **Unit tests** per package (`internal/sim`, `internal/scheduler`) build
   small, hand-constructed scenarios and assert an exact expected outcome,
   including a deliberate panic test for `PlaceUnits` called out of its own
   invariant (`TestPlaceUnitsPanicsWithoutSufficientCapacity`).
2. **An independent oracle** (`internal/oracle`), written without reference
   to either scheduler's own bookkeeping, recomputes GPU occupancy from
   scratch from every gang's own `Placed` map and compares it against the
   cluster's own `Free` counters (`CheckConservation`), and separately
   asserts the atomicity invariant that a Running gang always holds exactly
   its required GPU count (`CheckNoPartialGang`).
3. **Integration tests** (`internal/integration`) run full 300-job traces
   across 5 seeds through the gang scheduler and 1 seed through first-fit,
   with the oracle called after *every simulated tick*, panicking the test
   on the first violation.
4. **The benchmark itself** (`cmd/simulate`) runs the independent oracle
   after every tick of the full 2,000-job trace too; it is not a
   measurement-only path that skips the checks the tests use.

Race detector: `go test -race ./...` is clean (`docs/test_output.txt`).
This simulator is single-goroutine, so this is a basic hygiene check rather
than evidence about concurrency correctness; nothing here claims concurrent
scheduling.

```
$ go test -v -race ./...
...
ok  	.../internal/integration	1.4s
ok  	.../internal/metricsserver	1.0s
ok  	.../internal/scheduler	1.0s
ok  	.../internal/sim	1.1s
```
(full output: `docs/test_output.txt`, 26 of 26 tests passing)

## Findings

**A real bug: completed gangs kept a stale placement map, and the
independent oracle is what caught it.** The first version of
`internal/integration`'s full-trace test panicked on
`TestFirstFitHoldsConservationButDoesDeadlockOnTheSameTrace` with
`node 0: recomputed free 0 from gang placements disagrees with cluster.Free
5 (total 8, occupied 8)`. The cause: on completion, both schedulers released
the GPUs from the *cluster's* bookkeeping (`cluster.ReleaseUnits`) but left
each gang's own `Placed` map holding its old, now-stale node assignments.
`Scheduler.Snapshot()` returns every gang ever submitted, including
completed ones, so the independent oracle's from-scratch recomputation
double-counted every completed gang's old placement as if it were still
occupying GPUs. The fix clears `g.Placed` and `g.PlacedCount` at the moment
of completion in both `gang.go` and `firstfit.go`. This was not caught by
any unit test, because no single-scenario unit test happened to include a
gang that both completed and shared a node with another gang still being
inspected; it took the full-trace property test to surface it.

**"Deadlock" needed an operational definition, and that definition turned
out to be settle-period-bounded, not absolute.** With every GPU modeled as
a fungible unit, there is no scenario where a job is *mathematically*
unplaceable forever, short of demand exceeding supply forever: any freed
unit anywhere can satisfy any shortfall eventually. The first full-scale
run (144 GPUs, 80 faults, a 3,000-tick settle period after the 8,000-tick
arrival window) measured first-fit ending with 1 gang still Partial, 78
Pending and 31 Running, a real 109-job backlog, not a clean 0. Re-running
the identical trace with a 6,000-tick settle period instead of 3,000
cleared every one of those to 0. So "first-fit deadlocked" in this
simulator means "still not resolved after this much settle time," and
that number is reported as measured, at the settle period stated, rather
than implying a stronger guarantee than the simulator can actually make.

**The utilization claim this project was built to measure did not come out
in the claimed direction, after three genuine attempts, and the reason is
the no-preemption design choice, not a bug.** Three configurations were
tried (a larger, less-contended 200-GPU cluster; a smaller 120-GPU cluster
with lighter faults; the 144-GPU/80-fault configuration reported below).
In every one, first-fit's raw GPU occupancy came out *higher* than the gang
scheduler's, and even first-fit's **effective** occupancy (GPUs held by a
fully-placed, actually-running job, excluding anything stuck Partial) came
out higher too. The root cause: the gang scheduler's hierarchical quota
check can and does leave physically free capacity idle when the only
pending work belongs to a queue that is already at its quota and no sibling
currently has idle guaranteed quota to lend, which is working as designed
(it is what keeps one queue's burst from starving another's guarantee), but
it costs raw packing efficiency relative to a scheduler that ignores
fairness entirely and grabs whatever is free. This is a known, real
tradeoff in cluster schedulers between isolation and bin-packing
efficiency; it was measured here rather than assumed away.

## Measured results

Machine: WSL2 Ubuntu 22.04 on Windows 11, 12 logical cores, Go 1.26.9.
Trace: 2,000 jobs (sizes 1 to 32 GPUs, weighted toward small), arriving over
8,000 ticks, against an 18-node x 8-GPU (144 GPU) cluster, 80 simulated
fatal-Xid-class faults (one every ~100 ticks, 120-tick repair), evaluated
through a further 3,000-tick settle period with no new arrivals (10,998
simulated ticks total). Raw output: `docs/benchmark_output.txt`.

| Claim | Gang scheduler | First-fit baseline | Met? |
|---|---|---|---|
| Gang scheduling over pod groups, all-or-nothing | yes, by construction and by `CheckNoPartialGang` every tick | n/a (no gang concept) | met |
| Hierarchical queue quotas with over-quota reclaim | yes, `CanAdmit` + 5 dedicated tests | n/a | met |
| Node drained on a fatal Xid class, whole gang requeued with no partial work | yes, demonstrated exactly in `TestGangSchedulerFaultRequeuesWholeGangWithNoPartialWork` | no (only the faulted node's share is released; see Architecture) | met (gang) |
| **0 deadlocks over the 2,000-job trace** | **0** | 1 (plus 78 Pending, 31 Running still unresolved at the 3,000-tick settle cutoff) | met (gang); first-fit's 31-deadlock claim not met, measured 1 |
| GPU allocation up from 68% to 89% | 77.72% mean (raw == effective, by construction) | 99.40% mean raw / **81.78%** mean effective | not met, wrong direction: see Findings |
| Whole-gang fault requeues | 256 over 80 faults | 0 (no such mechanism) | n/a, reported for context |

The one number that matters most here is **0**: the gang scheduler's
deadlock count, which held across every configuration tried, including the
ones where the utilization claim did not. Peak raw and peak effective
allocation both hit 100% for both schedulers at some point in the run;
mean is the number that distinguishes them.

Where the gang scheduler loses: on raw packing efficiency, as described in
Findings, and it completed only 2,000/2,000 jobs within the same simulated
window where first-fit's un-admitted backlog (at the shorter settle period)
meant first-fit nominally had GPUs more often "spoken for," even though
some of that was wasted on a stuck Partial job.

## Building and running

```
go build ./...
go vet ./...
go test -v -race ./...        # docs/test_output.txt
go run ./cmd/simulate          # docs/benchmark_output.txt
go run ./cmd/simulate -seed 7  # any other trace seed
docker build -t gpu-gang-fault-drain-scheduler .
docker run --rm gpu-gang-fault-drain-scheduler   # one-shot, exits on its own
```

This repo's `go.mod` declares `go 1.26`, because `prometheus/client_golang`
v1.25 requires it; this machine's default `go` on `PATH` is a much older
go1.18.1, but a separately installed `/usr/local/go` (go1.26.9) was used to
build and test this repo, and Go's own toolchain directive mechanism
handled the version requirement transparently once pointed at a toolchain
new enough to have it (older Go binaries without the toolchain-switching
feature, like the stock go1.18.1 here, cannot do this themselves).

## Sibling comparison

The closest relative in the existing portfolio is
[ground-network-contact-scheduler](https://github.com/Manas103/ground-network-contact-scheduler):
also Go, also a named ordered policy rule chain with priority and
preemption, also diffed against an independently coded oracle. It was not
extended into this project (see the role's project breakdown for why: the
scheduling constraint is a different shape, exclusive bookings on named
beams versus all-or-nothing co-allocation of fungible devices), but the
method, diffing a scheduler against an independent reference rather than
trusting its own bookkeeping, is the same one `internal/oracle` applies
here. Where the satellite scheduler's headline number is 0 double-booked
beams across 500 concurrent attempts, this repo's headline number is the
same shape of claim: 0 deadlocks, independently checked, across the full
2,000-job trace.

## Limitations

- Deadlock, as measured, is bounded by the settle period chosen (3,000
  ticks here), not proven unconditionally impossible for first-fit; see
  Findings.
- The utilization claim this project set out to measure came out in the
  opposite direction after three genuine attempts at different cluster
  sizes and fault rates; see Findings for the root cause.
- No preemption: a lender queue cannot evict an over-quota borrower's
  already-running gang to reclaim capacity faster than the borrower
  finishes on its own.
- Every pod is a 1-GPU unit; multi-GPU-per-pod placement and node-local
  topology (NVLink domains, PCIe trees) are not modeled.
- The CRD YAML in `config/crd` is hand-written to document the schema and
  was never run against a real kube-apiserver or through `controller-gen`;
  there is no generated DeepCopy, webhook config, or admission chain.
- The Prometheus metrics server (`internal/metricsserver`) is exercised
  only by its own test over `httptest`; `cmd/simulate` does not currently
  wire it into a long-running `/metrics` endpoint, since the benchmark is a
  one-shot run rather than a long-lived process.
