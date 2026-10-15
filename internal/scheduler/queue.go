package scheduler

import "github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"

// NewQueues builds one Queue per name with an equal share of totalCapacity,
// leaving any remainder from integer division unassigned as shared pool
// capacity nobody holds a guaranteed quota over.
func NewQueues(names []string, totalCapacity int) map[string]*types.Queue {
	q := make(map[string]*types.Queue, len(names))
	share := totalCapacity / (len(names) + 1) // one extra share held back as shared pool
	for _, n := range names {
		q[n] = &types.Queue{
			TypeMeta:   types.TypeMeta{Kind: "Queue", APIVersion: "scheduling.manas103.dev/v1"},
			ObjectMeta: types.ObjectMeta{Name: n},
			Spec:       types.QueueSpec{GPUQuota: share},
		}
	}
	return q
}

// CanAdmit reports whether queue q may take on an additional `need` GPUs.
// Admission within q's own guaranteed quota is always allowed, subject only
// to physical cluster capacity. Admission above q's quota (over-quota
// reclaim) is allowed only up to the sum of idle guaranteed quota currently
// sitting unused in sibling queues: a queue can borrow capacity nobody else
// is using, but a lender's own future requests are never blocked by a
// borrower, because the lender's own admission check never looks at
// siblings at all. This implementation does not preempt an already-running
// over-quota borrower when the lender later wants its quota back; see
// README "what this is NOT".
func CanAdmit(q *types.Queue, all map[string]*types.Queue, need, clusterFree int) bool {
	if clusterFree < need {
		return false
	}
	if q.Status.GPUUsed+need <= q.Spec.GPUQuota {
		return true
	}
	overage := q.Status.GPUUsed + need - q.Spec.GPUQuota
	idle := 0
	for name, s := range all {
		if name == q.ObjectMeta.Name {
			continue
		}
		if s.Status.GPUUsed < s.Spec.GPUQuota {
			idle += s.Spec.GPUQuota - s.Status.GPUUsed
		}
	}
	return overage <= idle
}
