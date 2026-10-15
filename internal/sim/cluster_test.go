package sim

import "testing"

func TestPlaceAndReleaseConserveCapacity(t *testing.T) {
	c := NewCluster(4, 8) // 32 GPUs total
	if c.FreeTotal() != 32 {
		t.Fatalf("want 32 free, got %d", c.FreeTotal())
	}
	placement := c.PlaceUnits("gang-a", 20)
	if c.FreeTotal() != 12 {
		t.Fatalf("want 12 free after placing 20, got %d", c.FreeTotal())
	}
	sum := 0
	for _, units := range placement {
		sum += units
	}
	if sum != 20 {
		t.Fatalf("placement sums to %d, want 20", sum)
	}
	c.ReleaseUnits("gang-a", placement)
	if c.FreeTotal() != 32 {
		t.Fatalf("want 32 free after release, got %d", c.FreeTotal())
	}
}

func TestPlaceUnitsSpreadsAcrossNodesInIDOrder(t *testing.T) {
	c := NewCluster(3, 4) // nodes of 4 GPUs each
	placement := c.PlaceUnits("gang-a", 6)
	if placement[0] != 4 || placement[1] != 2 {
		t.Fatalf("want node 0 full (4) then node 1 partial (2), got %v", placement)
	}
	if _, ok := placement[2]; ok {
		t.Fatalf("node 2 should not have been touched, got %v", placement)
	}
}

func TestPlaceUnitsPanicsWithoutSufficientCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic when requesting more units than are free")
		}
	}()
	c := NewCluster(1, 2)
	c.PlaceUnits("gang-a", 3)
}

func TestDrainExcludesNodeFromFreeTotalUntilRepaired(t *testing.T) {
	c := NewCluster(2, 4)
	c.Drain(0, 10)
	if c.FreeTotal() != 4 {
		t.Fatalf("want 4 free with node 0 drained, got %d", c.FreeTotal())
	}
	c.Repair(9)
	if c.FreeTotal() != 4 {
		t.Fatalf("node should not repair before its RepairTick, got %d free", c.FreeTotal())
	}
	c.Repair(10)
	if c.FreeTotal() != 8 {
		t.Fatalf("want 8 free once node 0 is repaired, got %d", c.FreeTotal())
	}
}

func TestAllocatedPlusFreeEqualsCapacity(t *testing.T) {
	c := NewCluster(5, 8)
	c.PlaceUnits("gang-a", 17)
	if c.Allocated()+c.FreeTotal() != c.TotalCapacity() {
		t.Fatalf("allocated %d + free %d != capacity %d", c.Allocated(), c.FreeTotal(), c.TotalCapacity())
	}
}
