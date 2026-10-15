package metricsserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Manas103/gpu-gang-fault-drain-scheduler/internal/sim"
)

func TestHandlerServesPrometheusTextFormat(t *testing.T) {
	m := New()
	m.RecordRun("gang", sim.Stats{Completed: 10, Requeued: 2, DeadlockCount: 0}, sim.Result{MeanUtilization: 0.75, PeakUtilization: 0.95})
	m.RecordRun("first-fit", sim.Stats{Completed: 4, DeadlockCount: 3}, sim.Result{MeanUtilization: 0.4, PeakUtilization: 0.6})
	m.RecordFault("gang")

	// httptest.NewServer binds an ephemeral port chosen by the OS (port 0)
	// and this test stops it itself before returning; see BUILDER.md 3a.
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)

	for _, want := range []string{
		`gang_scheduler_completed_jobs{scheduler="gang"} 10`,
		`gang_scheduler_deadlocked_gangs{scheduler="first-fit"} 3`,
		`gang_scheduler_requeued_gangs_total{scheduler="gang"} 2`,
		`gang_scheduler_nodes_drained_total{scheduler="gang"} 1`,
		`gang_scheduler_mean_gpu_allocation_ratio{scheduler="gang"} 0.75`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("expected /metrics output to contain %q, got:\n%s", want, text)
		}
	}
}
