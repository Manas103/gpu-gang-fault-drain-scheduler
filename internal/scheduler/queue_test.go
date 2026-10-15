package scheduler

import (
	"testing"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/types"
)

func mkQueue(name string, quota, used int) *types.Queue {
	return &types.Queue{
		ObjectMeta: types.ObjectMeta{Name: name},
		Spec:       types.QueueSpec{GPUQuota: quota},
		Status:     types.QueueStatus{GPUUsed: used},
	}
}

func TestCanAdmitWithinOwnQuotaAlwaysAllowed(t *testing.T) {
	a := mkQueue("a", 10, 5)
	b := mkQueue("b", 10, 10) // sibling fully busy, irrelevant for a within-quota request
	all := map[string]*types.Queue{"a": a, "b": b}
	if !CanAdmit(a, all, 5, 100) {
		t.Fatal("a request that stays within the queue's own quota must always be admitted when cluster capacity exists")
	}
}

func TestCanAdmitOverQuotaAllowedWhenSiblingIdle(t *testing.T) {
	a := mkQueue("a", 10, 10) // already at its own quota
	b := mkQueue("b", 10, 2)  // 8 idle
	all := map[string]*types.Queue{"a": a, "b": b}
	if !CanAdmit(a, all, 6, 100) {
		t.Fatal("a 6-unit over-quota request should be admitted: sibling b has 8 idle guaranteed units to lend")
	}
}

func TestCanAdmitOverQuotaBlockedWhenNoSiblingIdle(t *testing.T) {
	a := mkQueue("a", 10, 10)
	b := mkQueue("b", 10, 10) // nothing idle to lend
	all := map[string]*types.Queue{"a": a, "b": b}
	if CanAdmit(a, all, 1, 100) {
		t.Fatal("an over-quota request must be blocked when no sibling has idle guaranteed capacity to lend")
	}
}

func TestCanAdmitBlockedByClusterCapacityRegardlessOfQuota(t *testing.T) {
	a := mkQueue("a", 100, 0)
	all := map[string]*types.Queue{"a": a}
	if CanAdmit(a, all, 10, 5) {
		t.Fatal("a request must never be admitted past physical cluster free capacity, however generous its quota")
	}
}

func TestCanAdmitLenderPriorityIsNotBlockedByABorrower(t *testing.T) {
	// a borrowed idle capacity from b; b later asks for exactly its own
	// quota back. The lender's own admission check never looks at siblings
	// at all, so it is unaffected by what a is currently holding.
	a := mkQueue("a", 10, 16) // 10 own + 6 borrowed from b
	b := mkQueue("b", 10, 0)
	all := map[string]*types.Queue{"a": a, "b": b}
	if !CanAdmit(b, all, 10, 100) {
		t.Fatal("a queue must always be able to reclaim up to its own quota, independent of borrowers")
	}
}
