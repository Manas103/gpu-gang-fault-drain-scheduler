// Package metricsserver exposes scheduler run statistics in the Prometheus
// text exposition format, via the real client_golang library rather than a
// hand-rolled formatter, so a Prometheus server can actually scrape it.
package metricsserver

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
)

// Metrics owns its own registry rather than the global default one, so
// multiple instances (one per scheduler under comparison, or one per test)
// never collide.
type Metrics struct {
	registry        *prometheus.Registry
	completedJobs   *prometheus.GaugeVec
	requeuedGangs   *prometheus.GaugeVec
	deadlockedGangs *prometheus.GaugeVec
	meanUtilization *prometheus.GaugeVec
	peakUtilization *prometheus.GaugeVec
	nodesDrained    *prometheus.CounterVec
}

// New builds a Metrics instance with every gauge registered and zeroed.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	gauge := func(name, help string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, []string{"scheduler"})
		reg.MustRegister(g)
		return g
	}
	m := &Metrics{
		registry:        reg,
		completedJobs:   gauge("gang_scheduler_completed_jobs", "Gangs that ran to completion."),
		requeuedGangs:   gauge("gang_scheduler_requeued_gangs_total", "Whole-gang fault requeues observed so far."),
		deadlockedGangs: gauge("gang_scheduler_deadlocked_gangs", "Gangs stuck holding a partial allocation forever."),
		meanUtilization: gauge("gang_scheduler_mean_gpu_allocation_ratio", "Mean fraction of cluster GPUs allocated over the run."),
		peakUtilization: gauge("gang_scheduler_peak_gpu_allocation_ratio", "Peak fraction of cluster GPUs allocated over the run."),
		nodesDrained: func() *prometheus.CounterVec {
			c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gang_scheduler_nodes_drained_total", Help: "Simulated fatal Xid-class faults applied."}, []string{"scheduler"})
			reg.MustRegister(c)
			return c
		}(),
	}
	return m
}

// RecordRun publishes one scheduler's final Stats and utilization summary
// under its own "scheduler" label, so the gang and first-fit runs can be
// scraped side by side from the same /metrics endpoint.
func (m *Metrics) RecordRun(schedulerName string, st sim.Stats, res sim.Result) {
	m.completedJobs.WithLabelValues(schedulerName).Set(float64(st.Completed))
	m.requeuedGangs.WithLabelValues(schedulerName).Set(float64(st.Requeued))
	m.deadlockedGangs.WithLabelValues(schedulerName).Set(float64(st.DeadlockCount))
	m.meanUtilization.WithLabelValues(schedulerName).Set(res.MeanUtilization)
	m.peakUtilization.WithLabelValues(schedulerName).Set(res.PeakUtilization)
}

// RecordFault increments the drain counter for one scheduler's run.
func (m *Metrics) RecordFault(schedulerName string) {
	m.nodesDrained.WithLabelValues(schedulerName).Inc()
}

// Handler serves this instance's registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
