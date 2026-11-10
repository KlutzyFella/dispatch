// Package metrics defines shared Prometheus metrics for the dispatch system.
// Metrics are registered automatically via promauto on package import.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TasksSubmittedTotal counts SubmitTask responses, not distinct tasks: client
// retries are counted again. The "status" label is "accepted" (ACCEPTED),
// "pending" (Unavailable; the intent is retained for recovery), "rejected"
// (InvalidArgument or FailedPrecondition), or "error" (any other failure).
var TasksSubmittedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "tasks_submitted_total",
		Help: "SubmitTask responses by outcome: accepted, pending, rejected, or error.",
	},
	[]string{"status"},
)

// TasksCompletedTotal counts tasks that completed successfully.
var TasksCompletedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "tasks_completed_total",
		Help: "Total number of tasks completed successfully.",
	},
)

// TasksFailedTotal counts tasks that failed (including DLQ sends).
var TasksFailedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "tasks_failed_total",
		Help: "Total number of tasks that failed execution.",
	},
)

// TaskExecutionDuration records task execution time in seconds.
var TaskExecutionDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "task_execution_duration_seconds",
		Help:    "Duration of task execution in seconds.",
		Buckets: prometheus.DefBuckets,
	},
)
