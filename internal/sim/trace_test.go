package sim

import "testing"

func TestNewTraceIsDeterministic(t *testing.T) {
	a := NewTrace(42, 500, 4000, 20, 150, 40, []string{"a", "b", "c"})
	b := NewTrace(42, 500, 4000, 20, 150, 40, []string{"a", "b", "c"})
	if len(a.Jobs) != len(b.Jobs) || len(a.Faults) != len(b.Faults) {
		t.Fatalf("same seed produced different trace sizes: %d/%d jobs, %d/%d faults",
			len(a.Jobs), len(b.Jobs), len(a.Faults), len(b.Faults))
	}
	for i := range a.Jobs {
		if a.Jobs[i] != b.Jobs[i] {
			t.Fatalf("job %d differs between identically seeded traces: %+v vs %+v", i, a.Jobs[i], b.Jobs[i])
		}
	}
	for i := range a.Faults {
		if a.Faults[i] != b.Faults[i] {
			t.Fatalf("fault %d differs between identically seeded traces: %+v vs %+v", i, a.Faults[i], b.Faults[i])
		}
	}
}

func TestNewTraceDifferentSeedsDiffer(t *testing.T) {
	a := NewTrace(1, 500, 4000, 20, 150, 40, []string{"a", "b", "c"})
	b := NewTrace(2, 500, 4000, 20, 150, 40, []string{"a", "b", "c"})
	same := true
	for i := range a.Jobs {
		if a.Jobs[i] != b.Jobs[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds produced an identical trace, RNG is not actually being used")
	}
}

func TestNewTraceArrivalsAreSorted(t *testing.T) {
	tr := NewTrace(7, 1000, 4000, 20, 150, 40, []string{"a"})
	for i := 1; i < len(tr.Jobs); i++ {
		if tr.Jobs[i].ArrivalTick < tr.Jobs[i-1].ArrivalTick {
			t.Fatalf("arrivals not sorted at index %d: %d before %d", i, tr.Jobs[i-1].ArrivalTick, tr.Jobs[i].ArrivalTick)
		}
	}
}

func TestNewTraceGangSizesAreFromTheDeclaredBuckets(t *testing.T) {
	allowed := map[int]bool{1: true, 2: true, 4: true, 8: true, 16: true, 24: true, 32: true}
	tr := NewTrace(3, 2000, 4000, 20, 150, 40, []string{"a", "b"})
	for _, j := range tr.Jobs {
		if !allowed[j.MinMember] {
			t.Fatalf("job %s has gang size %d, outside the declared buckets", j.UID, j.MinMember)
		}
	}
}
