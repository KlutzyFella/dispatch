// Tests are written first per TDD workflow.
// Run `make generate` before `make test` to produce the required proto bindings.
package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
)

// ── Mock Producer ─────────────────────────────────────────────────────────────

type mockProducer struct {
	err       error
	calls     int
	published []publishedMsg
}

type publishedMsg struct {
	taskID string
	task   *taskv1.Task
}

func (m *mockProducer) Publish(_ context.Context, taskID string, task *taskv1.Task) error {
	m.calls++
	m.published = append(m.published, publishedMsg{taskID: taskID, task: task})
	return m.err
}

func (m *mockProducer) Close() error { return nil }

var _ broker.Producer = (*mockProducer)(nil)

// ── Mock TaskStore ─────────────────────────────────────────────────────────────

type mockTaskStore struct {
	tasks     map[string]state.TaskRecord
	saveErr   error
	getErr    error
	updateErr error
	deleteErr error
}

func newMockStore() *mockTaskStore {
	return &mockTaskStore{tasks: make(map[string]state.TaskRecord)}
}

func (m *mockTaskStore) SaveTask(_ context.Context, record state.TaskRecord) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.tasks[record.ID] = record
	return nil
}

func (m *mockTaskStore) GetTask(_ context.Context, taskID string) (state.TaskRecord, error) {
	if m.getErr != nil {
		return state.TaskRecord{}, m.getErr
	}
	r, ok := m.tasks[taskID]
	if !ok {
		return state.TaskRecord{}, state.ErrTaskNotFound
	}
	return r, nil
}

func (m *mockTaskStore) UpdateProgress(_ context.Context, taskID string, s, p int32) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	r, ok := m.tasks[taskID]
	if !ok {
		return state.ErrTaskNotFound
	}
	r.Status = s
	r.Progress = p
	r.UpdatedAt = time.Now()
	m.tasks[taskID] = r
	return nil
}

func (m *mockTaskStore) DeleteTask(_ context.Context, taskID string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.tasks, taskID)
	return nil
}

var _ state.TaskStore = (*mockTaskStore)(nil)

// ── Test helpers ──────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, p broker.Producer, s state.TaskStore) *server.DispatchServer {
	t.Helper()
	return server.New(slog.New(slog.NewTextHandler(io.Discard, nil)), p, s)
}

func newTestServerOK(t *testing.T) *server.DispatchServer {
	t.Helper()
	return newTestServer(t, &mockProducer{}, newMockStore())
}

func submitTask(t *testing.T, srv *server.DispatchServer, taskType string) string {
	t.Helper()
	resp, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{Type: taskType},
	})
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	return resp.GetTaskId()
}

// fakeProgressStream is a test double for DispatchService_ReportProgressServer.
type fakeProgressStream struct {
	taskv1.DispatchService_ReportProgressServer

	ctx     context.Context
	updates []*taskv1.ProgressUpdate
	idx     int
	acks    []*taskv1.ProgressAck
}

func (f *fakeProgressStream) Context() context.Context { return f.ctx }

func (f *fakeProgressStream) Recv() (*taskv1.ProgressUpdate, error) {
	if f.idx >= len(f.updates) {
		return nil, io.EOF
	}
	u := f.updates[f.idx]
	f.idx++
	return u, nil
}

func (f *fakeProgressStream) Send(ack *taskv1.ProgressAck) error {
	f.acks = append(f.acks, ack)
	return nil
}

func newStream(ctx context.Context, updates ...*taskv1.ProgressUpdate) *fakeProgressStream {
	return &fakeProgressStream{ctx: ctx, updates: updates}
}

// ── SubmitTask ────────────────────────────────────────────────────────────────

func TestSubmitTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        *taskv1.SubmitTaskRequest
		wantCode   codes.Code
		wantStatus taskv1.SubmitStatus
		wantTaskID string
	}{
		{
			name: "valid task with no idempotency key generates server UUID",
			req: &taskv1.SubmitTaskRequest{
				Task: &taskv1.Task{
					Type:     "email_dispatch",
					Priority: taskv1.Priority_PRIORITY_HIGH,
				},
			},
			wantCode:   codes.OK,
			wantStatus: taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED,
		},
		{
			name: "valid task preserves provided idempotency key as task_id",
			req: &taskv1.SubmitTaskRequest{
				Task: &taskv1.Task{
					IdempotencyKey: "client-supplied-key-abc",
					Type:           "video_transcode",
				},
			},
			wantCode:   codes.OK,
			wantStatus: taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED,
			wantTaskID: "client-supplied-key-abc",
		},
		{
			name:     "nil task returns InvalidArgument",
			req:      &taskv1.SubmitTaskRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "empty type returns InvalidArgument",
			req: &taskv1.SubmitTaskRequest{
				Task: &taskv1.Task{Type: ""},
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "negative max_retries returns InvalidArgument",
			req: &taskv1.SubmitTaskRequest{
				Task: &taskv1.Task{
					Type:       "email_dispatch",
					MaxRetries: -1,
				},
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServerOK(t)
			resp, err := srv.SubmitTask(context.Background(), tc.req)

			if tc.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tc.wantCode)
				}
				if got := status.Code(err); got != tc.wantCode {
					t.Errorf("error code = %v, want %v", got, tc.wantCode)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.GetStatus() != tc.wantStatus {
				t.Errorf("status = %v, want %v", resp.GetStatus(), tc.wantStatus)
			}
			if tc.wantTaskID != "" && resp.GetTaskId() != tc.wantTaskID {
				t.Errorf("task_id = %q, want %q", resp.GetTaskId(), tc.wantTaskID)
			}
			if tc.wantTaskID == "" && resp.GetTaskId() == "" {
				t.Error("task_id must not be empty when no idempotency key is provided")
			}
		})
	}
}

func TestSubmitTask_Idempotency(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	req := &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{
			IdempotencyKey: "idem-key-xyz",
			Type:           "email_dispatch",
		},
	}

	resp1, err := srv.SubmitTask(context.Background(), req)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	resp2, err := srv.SubmitTask(context.Background(), req)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}

	if resp1.GetTaskId() != resp2.GetTaskId() {
		t.Errorf("idempotency violated: first=%q second=%q", resp1.GetTaskId(), resp2.GetTaskId())
	}
}

// ── Producer integration tests ────────────────────────────────────────────────

func TestSubmitTask_PublishSuccess(t *testing.T) {
	t.Parallel()

	prod := &mockProducer{}
	srv := newTestServer(t, prod, newMockStore())

	resp, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{
			IdempotencyKey: "pub-success-key",
			Type:           "email_dispatch",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStatus() != taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED {
		t.Errorf("status = %v, want ACCEPTED", resp.GetStatus())
	}
	if prod.calls != 1 {
		t.Errorf("Publish called %d times, want 1", prod.calls)
	}
	if prod.published[0].taskID != "pub-success-key" {
		t.Errorf("published taskID = %q, want %q", prod.published[0].taskID, "pub-success-key")
	}
}

func TestSubmitTask_PublishFailure(t *testing.T) {
	t.Parallel()

	prod := &mockProducer{err: errors.New("kafka unavailable")}
	srv := newTestServer(t, prod, newMockStore())

	_, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{
			IdempotencyKey: "pub-fail-key",
			Type:           "email_dispatch",
		},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("error code = %v, want Internal", got)
	}

	// Verify rollback: submitting the same key again must NOT be treated as
	// idempotent — the task was rolled back — and must call Publish a second time.
	prod.err = nil
	resp2, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{
			IdempotencyKey: "pub-fail-key",
			Type:           "email_dispatch",
		},
	})
	if err != nil {
		t.Fatalf("re-submit after rollback: %v", err)
	}
	if resp2.GetStatus() != taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED {
		t.Errorf("re-submit status = %v, want ACCEPTED", resp2.GetStatus())
	}
	if prod.calls != 2 {
		t.Errorf("Publish called %d times total, want 2", prod.calls)
	}
}

func TestSubmitTask_IdempotentResubmitSkipsPublish(t *testing.T) {
	t.Parallel()

	prod := &mockProducer{}
	srv := newTestServer(t, prod, newMockStore())

	req := &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{
			IdempotencyKey: "idem-pub-key",
			Type:           "video_transcode",
		},
	}

	if _, err := srv.SubmitTask(context.Background(), req); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if _, err := srv.SubmitTask(context.Background(), req); err != nil {
		t.Fatalf("second submit: %v", err)
	}

	if prod.calls != 1 {
		t.Errorf("Publish called %d times, want exactly 1 (idempotent resubmit must skip publish)", prod.calls)
	}
}

func TestSubmitTask_StoreSaveFailure(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	store.saveErr = errors.New("redis unavailable")
	srv := newTestServer(t, &mockProducer{}, store)

	_, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{Type: "email_dispatch"},
	})
	if err == nil {
		t.Fatal("expected error when store.SaveTask fails, got nil")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("error code = %v, want Internal", got)
	}
}

// ── ReportProgress ────────────────────────────────────────────────────────────

func TestReportProgress_ValidUpdate(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	taskID := submitTask(t, srv, "video_transcode")

	stream := newStream(context.Background(), &taskv1.ProgressUpdate{
		WorkerId: "worker-1",
		TaskId:   taskID,
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 42,
	})

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}

	if len(stream.acks) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(stream.acks))
	}
	if !stream.acks[0].GetRecorded() {
		t.Error("expected recorded=true for valid update")
	}
}

func TestReportProgress_UnknownTask(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	stream := newStream(context.Background(), &taskv1.ProgressUpdate{
		WorkerId: "worker-1",
		TaskId:   "nonexistent-task-id",
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 50,
	})

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	if len(stream.acks) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(stream.acks))
	}
	if stream.acks[0].GetRecorded() {
		t.Error("expected recorded=false for unknown task")
	}
}

func TestReportProgress_InvalidProgressValue(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	taskID := submitTask(t, srv, "email_dispatch")

	stream := newStream(context.Background(), &taskv1.ProgressUpdate{
		WorkerId: "worker-1",
		TaskId:   taskID,
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 150, // out of range
	})

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	if stream.acks[0].GetRecorded() {
		t.Error("expected recorded=false for progress > 100")
	}
}

func TestReportProgress_MultipleTasksOnOneStream(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	taskA := submitTask(t, srv, "task_type_a")
	taskB := submitTask(t, srv, "task_type_b")

	stream := newStream(context.Background(),
		&taskv1.ProgressUpdate{
			WorkerId: "worker-1",
			TaskId:   taskA,
			Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
			Progress: 25,
		},
		&taskv1.ProgressUpdate{
			WorkerId: "worker-1",
			TaskId:   taskB,
			Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
			Progress: 75,
		},
	)

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	if len(stream.acks) != 2 {
		t.Fatalf("expected 2 acks, got %d", len(stream.acks))
	}
	for i, ack := range stream.acks {
		if !ack.GetRecorded() {
			t.Errorf("ack[%d]: expected recorded=true", i)
		}
	}
}

func TestReportProgress_MixedValidAndInvalid(t *testing.T) {
	t.Parallel()

	srv := newTestServerOK(t)
	taskID := submitTask(t, srv, "batch_job")

	stream := newStream(context.Background(),
		// valid
		&taskv1.ProgressUpdate{
			WorkerId: "worker-1",
			TaskId:   taskID,
			Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
			Progress: 10,
		},
		// invalid: unknown task
		&taskv1.ProgressUpdate{
			WorkerId: "worker-1",
			TaskId:   "ghost-id",
			Progress: 10,
		},
		// valid
		&taskv1.ProgressUpdate{
			WorkerId: "worker-1",
			TaskId:   taskID,
			Status:   taskv1.TaskStatus_TASK_STATUS_COMPLETED,
			Progress: 100,
		},
	)

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}

	wantRecorded := []bool{true, false, true}
	if len(stream.acks) != len(wantRecorded) {
		t.Fatalf("expected %d acks, got %d", len(wantRecorded), len(stream.acks))
	}
	for i, want := range wantRecorded {
		if stream.acks[i].GetRecorded() != want {
			t.Errorf("ack[%d]: recorded=%v, want %v", i, stream.acks[i].GetRecorded(), want)
		}
	}
}

func TestReportProgress_StoreUpdateFailure(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	srv := newTestServer(t, &mockProducer{}, store)

	// Submit task so it exists.
	resp, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{
		Task: &taskv1.Task{Type: "email_dispatch"},
	})
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	// Now make UpdateProgress fail.
	store.updateErr = errors.New("redis write error")

	stream := newStream(context.Background(), &taskv1.ProgressUpdate{
		WorkerId: "worker-1",
		TaskId:   resp.GetTaskId(),
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 10,
	})

	if err := srv.ReportProgress(stream); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	// The update failed — ack should be recorded=false.
	if stream.acks[0].GetRecorded() {
		t.Error("expected recorded=false when store.UpdateProgress fails")
	}
}

func TestReportProgress_RejectsUnsupportedState(t *testing.T) {
	tests := []struct {
		name     string
		status   taskv1.TaskStatus
		progress int32
		accepted bool
	}{
		{name: "unspecified"},
		{name: "unknown", status: taskv1.TaskStatus(999)},
		{name: "negative_status", status: taskv1.TaskStatus(-1)},
		{name: "pending_is_not_worker_progress", status: taskv1.TaskStatus_TASK_STATUS_PENDING},
		{name: "cancellation_is_not_implemented", status: taskv1.TaskStatus_TASK_STATUS_CANCELLED},
		{name: "completed_at_zero", status: taskv1.TaskStatus_TASK_STATUS_COMPLETED},
		{name: "completed_at_99", status: taskv1.TaskStatus_TASK_STATUS_COMPLETED, progress: 99},
		{name: "running", status: taskv1.TaskStatus_TASK_STATUS_RUNNING, progress: 42, accepted: true},
		{name: "failed", status: taskv1.TaskStatus_TASK_STATUS_FAILED, accepted: true},
		{name: "completed", status: taskv1.TaskStatus_TASK_STATUS_COMPLETED, progress: 100, accepted: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newMockStore()
			srv := newTestServer(t, &mockProducer{}, store)
			id := submitTask(t, srv, "simulation")
			before := store.tasks[id]
			stream := newStream(context.Background(), &taskv1.ProgressUpdate{
				WorkerId: "test-worker", TaskId: id, Status: tc.status, Progress: tc.progress,
			})
			if err := srv.ReportProgress(stream); err != nil {
				t.Fatal(err)
			}
			if len(stream.acks) != 1 || stream.acks[0].GetRecorded() != tc.accepted {
				t.Errorf("acknowledgment=%v, want recorded=%v", stream.acks, tc.accepted)
			}
			after := store.tasks[id]
			if !tc.accepted && after != before {
				t.Errorf("invalid progress changed task state: before=%+v after=%+v", before, after)
			}
			if tc.accepted && (after.Status != int32(tc.status) || after.Progress != tc.progress) {
				t.Errorf("valid progress was not stored: %+v", after)
			}
		})
	}
}
