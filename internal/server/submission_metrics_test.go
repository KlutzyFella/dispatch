package server_test

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/state"
)

var submissionOutcomes = []string{"accepted", "pending", "rejected", "error"}

// submissionCounts reads tasks_submitted_total from the default registry, the
// one the metrics endpoint serves.
func submissionCounts(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "tasks_submitted_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "status" {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	return counts
}

// Every SubmitTask response must be counted exactly once, under the label of
// the response the client received. Previously a retained intent whose first
// publish failed counted as "rejected" (and later "accepted" on retry), an
// identical pending duplicate counted nothing, and legacy/store-read failures
// counted nothing. Not parallel: the counter is process-global.
func TestSubmitTask_CountsEachResponseOnce(t *testing.T) {
	failing := &mockProducer{err: errors.New("broker unavailable")}
	pendingStore := newMockStore()
	legacyStore := newMockStore()
	if err := legacyStore.SaveTask(context.Background(), state.TaskRecord{ID: "legacy-metric", TaskType: "simulation"}); err != nil {
		t.Fatal(err)
	}
	brokenStore := newMockStore()
	brokenStore.getErr = errors.New("redis unavailable")
	confirmed := newTestServerOK(t)
	request := func(key, taskType string) *taskv1.SubmitTaskRequest {
		return &taskv1.SubmitTaskRequest{Task: &taskv1.Task{IdempotencyKey: key, Type: taskType}}
	}

	steps := []struct {
		name    string
		submit  func() error
		code    codes.Code
		outcome string
	}{
		{"new task published", func() error {
			_, err := confirmed.SubmitTask(context.Background(), request("confirmed-metric", "simulation"))
			return err
		}, codes.OK, "accepted"},
		{"confirmed duplicate", func() error {
			_, err := confirmed.SubmitTask(context.Background(), request("confirmed-metric", "simulation"))
			return err
		}, codes.OK, "accepted"},
		{"publish failed, intent retained", func() error {
			_, err := newTestServer(t, failing, pendingStore).SubmitTask(context.Background(), request("pending-metric", "simulation"))
			return err
		}, codes.Unavailable, "pending"},
		{"duplicate while pending", func() error {
			_, err := newTestServer(t, failing, pendingStore).SubmitTask(context.Background(), request("pending-metric", "simulation"))
			return err
		}, codes.Unavailable, "pending"},
		{"empty type", func() error {
			_, err := confirmed.SubmitTask(context.Background(), request("", ""))
			return err
		}, codes.InvalidArgument, "rejected"},
		{"legacy record", func() error {
			_, err := newTestServer(t, &mockProducer{}, legacyStore).SubmitTask(context.Background(), request("legacy-metric", "simulation"))
			return err
		}, codes.FailedPrecondition, "rejected"},
		{"state read failure", func() error {
			_, err := newTestServer(t, &mockProducer{}, brokenStore).SubmitTask(context.Background(), request("broken-metric", "simulation"))
			return err
		}, codes.Internal, "error"},
	}
	for _, step := range steps {
		before := submissionCounts(t)
		err := step.submit()
		if status.Code(err) != step.code {
			t.Fatalf("%s: setup returned %v; want %v", step.name, err, step.code)
		}
		after := submissionCounts(t)
		total := 0.0
		for _, outcome := range submissionOutcomes {
			total += after[outcome] - before[outcome]
		}
		if delta := after[step.outcome] - before[step.outcome]; delta != 1 || total != 1 {
			t.Errorf("%s: %s +%v, all outcomes +%v; want exactly one %q count (before=%v after=%v)",
				step.name, step.outcome, delta, total, step.outcome, before, after)
		}
	}
}
