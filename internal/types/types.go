// Package types defines the PodGroup and Queue API objects. They are shaped like
// Kubernetes custom resources (TypeMeta/ObjectMeta/Spec/Status) but are plain Go
// structs, not registered against a live kube-apiserver; see config/crd for the
// matching CustomResourceDefinition manifests and the README for why.
package types

// TypeMeta mirrors the Kubernetes convention for identifying an API object's kind.
type TypeMeta struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
}

// ObjectMeta mirrors the subset of metav1.ObjectMeta this simulator needs.
type ObjectMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// PodGroupPhase is the lifecycle state of a gang.
type PodGroupPhase string

const (
	PodGroupPending   PodGroupPhase = "Pending"
	PodGroupPartial   PodGroupPhase = "Partial"   // first-fit only: some but not all pods placed
	PodGroupRunning   PodGroupPhase = "Running"   // the entire gang is placed and executing
	PodGroupCompleted PodGroupPhase = "Completed"
)

// PodGroupSpec is the desired state of a gang: a group of pods that must all be
// scheduled together before any of them is considered to be doing useful work.
// Every pod requests exactly one GPU; a gang's total GPU requirement is MinMember.
// Real multi-GPU-per-pod jobs are not modeled, see README "what this is NOT".
type PodGroupSpec struct {
	MinMember     int    `json:"minMember"`
	QueueName     string `json:"queueName"`
	DurationTicks int    `json:"durationTicks"`
}

// PodGroupStatus is the observed state of a gang.
type PodGroupStatus struct {
	Phase        PodGroupPhase `json:"phase"`
	PlacedPods   int           `json:"placedPods"`
	NodePods     map[int]int   `json:"nodePods"` // nodeID -> pods of this gang placed there
	StartTick    int           `json:"startTick"`
	RequeueCount int           `json:"requeueCount"`
}

// PodGroup is a gang scheduling request, CRD-shaped.
type PodGroup struct {
	TypeMeta
	ObjectMeta
	Spec   PodGroupSpec
	Status PodGroupStatus
}

// QueueSpec is the desired state of a scheduling queue. Queues are a flat set of
// children under one implicit root, which is as much hierarchy ("hierarchical
// queue quotas") as this simulator models; see README for the simplification.
type QueueSpec struct {
	GPUQuota int `json:"gpuQuota"`
}

// QueueStatus is the observed state of a queue.
type QueueStatus struct {
	GPUUsed int `json:"gpuUsed"`
}

// Queue is a hierarchical GPU quota bucket, CRD-shaped.
type Queue struct {
	TypeMeta
	ObjectMeta
	Spec   QueueSpec
	Status QueueStatus
}
