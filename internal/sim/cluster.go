// Package sim implements the discrete-tick cluster model, synthetic job trace
// and the simulation runner shared by every scheduler implementation.
package sim

// Node is one simulated GPU host.
type Node struct {
	ID         int
	Total      int
	Free       int
	Drained    bool
	RepairTick int // tick at which a drained node becomes schedulable again
	Placements map[string]int // gang UID -> GPUs of this gang held on this node
}

// NewNode builds a healthy node with Total GPUs all free.
func NewNode(id, total int) *Node {
	return &Node{ID: id, Total: total, Free: total, Placements: map[string]int{}}
}

// Cluster is a fixed set of nodes.
type Cluster struct {
	Nodes []*Node
}

// NewCluster builds numNodes identical nodes of gpusPerNode each.
func NewCluster(numNodes, gpusPerNode int) *Cluster {
	c := &Cluster{Nodes: make([]*Node, numNodes)}
	for i := 0; i < numNodes; i++ {
		c.Nodes[i] = NewNode(i, gpusPerNode)
	}
	return c
}

// FreeTotal is the cluster-wide free GPU count across healthy nodes.
func (c *Cluster) FreeTotal() int {
	total := 0
	for _, n := range c.Nodes {
		if !n.Drained {
			total += n.Free
		}
	}
	return total
}

// TotalCapacity is the cluster's fixed physical GPU count, drained or not.
func (c *Cluster) TotalCapacity() int {
	total := 0
	for _, n := range c.Nodes {
		total += n.Total
	}
	return total
}

// Allocated is the number of GPUs currently held by any gang, cluster-wide.
func (c *Cluster) Allocated() int {
	alloc := 0
	for _, n := range c.Nodes {
		alloc += n.Total - n.Free
	}
	return alloc
}

// PlaceUnits places exactly need single-GPU units for gangUID across whichever
// healthy nodes have free capacity, scanning in node-ID order. It never places
// a partial amount: callers must confirm FreeTotal() >= need first. It returns
// the per-node placement it made so the caller can release it exactly later.
func (c *Cluster) PlaceUnits(gangUID string, need int) map[int]int {
	placed := map[int]int{}
	remaining := need
	for _, n := range c.Nodes {
		if remaining == 0 {
			break
		}
		if n.Drained || n.Free == 0 {
			continue
		}
		take := n.Free
		if take > remaining {
			take = remaining
		}
		n.Free -= take
		n.Placements[gangUID] += take
		placed[n.ID] += take
		remaining -= take
	}
	if remaining != 0 {
		panic("sim: PlaceUnits called without sufficient free capacity, caller invariant violated")
	}
	return placed
}

// ReleaseUnits reverses a prior PlaceUnits call for the given per-node placement.
func (c *Cluster) ReleaseUnits(gangUID string, placement map[int]int) {
	for nodeID, units := range placement {
		n := c.Nodes[nodeID]
		n.Free += units
		n.Placements[gangUID] -= units
		if n.Placements[gangUID] <= 0 {
			delete(n.Placements, gangUID)
		}
	}
}

// Drain marks a node unschedulable until repairAt, simulating a fatal Xid-class
// fault (DCGM's XID error field; a real GPU hang or ECC double-bit error would
// surface as one). It does not by itself evict existing placements; callers
// decide how affected gangs respond (see scheduler.FaultResponse).
func (c *Cluster) Drain(nodeID, repairAt int) {
	n := c.Nodes[nodeID]
	n.Drained = true
	n.RepairTick = repairAt
}

// Repair heals every drained node whose RepairTick has arrived, restoring its
// full free capacity (any placements still recorded on it were already
// evicted by the fault handler before Repair is ever reachable for that node).
func (c *Cluster) Repair(tick int) {
	for _, n := range c.Nodes {
		if n.Drained && tick >= n.RepairTick {
			n.Drained = false
			n.Free = n.Total
		}
	}
}
