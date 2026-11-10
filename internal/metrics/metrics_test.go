package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ronnitchopra/dispatch/internal/metrics"
)

func TestMetricsHandler_ReturnsPrometheusFormat(t *testing.T) {
	// Increment a counter so there is at least one named metric in the output.
	metrics.TasksSubmittedTotal.WithLabelValues("accepted").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	promhttp.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "# HELP") {
		t.Error("expected Prometheus text format with # HELP lines")
	}
	if !strings.Contains(body, "tasks_submitted_total") {
		t.Error("expected tasks_submitted_total in metrics output")
	}
}
