package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// ── Mock implementations ───────────────────────────────────────────────────────

// mockReader implements messageReader for testing Consumer.Run.
type mockReader struct {
	messages []kafka.Message
	idx      int
	fetchErr error
	closeErr error
	onCommit func() // called after each CommitMessages, if non-nil
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if ctx.Err() != nil {
		return kafka.Message{}, ctx.Err()
	}
	if m.fetchErr != nil {
		return kafka.Message{}, m.fetchErr
	}
	if m.idx >= len(m.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := m.messages[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	if m.onCommit != nil {
		m.onCommit()
	}
	return nil
}

func (m *mockReader) Close() error { return m.closeErr }

// mockSender implements ProgressSender for testing.
type mockSender struct {
	updates   []*taskv1.ProgressUpdate
	ack       *taskv1.ProgressAck
	err       error
	errOnCall int // 0 = all calls fail; n = only call n fails
	callCount int
	onSend    func()
}

func (m *mockSender) SendUpdate(u *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error) {
	m.callCount++
	m.updates = append(m.updates, u)
	if m.onSend != nil {
		m.onSend()
	}
	if m.err != nil && (m.errOnCall == 0 || m.callCount == m.errOnCall) {
		return nil, m.err
	}
	if m.ack != nil {
		return m.ack, nil
	}
	return &taskv1.ProgressAck{Recorded: true}, nil
}

// mockDLQPublisher implements DLQPublisher for testing.
type mockDLQPublisher struct {
	published []string // task IDs published
	err       error
}

func (m *mockDLQPublisher) Publish(_ context.Context, taskID string, _ *taskv1.Task) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, taskID)
	return nil
}

// ── Helpers ────────────────────────────────────────────────────────────────────

// newTestConsumer builds a minimal Consumer for unit tests.
// failureDecider defaults to never-fail.
func newTestConsumer(sender ProgressSender) *Consumer {
	return &Consumer{
		reporter:       sender,
		failureDecider: func() bool { return false },
		workerID:       "test-worker",
		logger:         slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
}

func marshalTask(t *testing.T, task *taskv1.Task) []byte {
	t.Helper()
	b, err := proto.Marshal(task)
	if err != nil {
		t.Fatalf("marshalling task: %v", err)
	}
	return b
}

// ── processMessage: happy path ─────────────────────────────────────────────────

func TestProcessMessage_ValidTask(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	c := newTestConsumer(sender)

	task := &taskv1.Task{Type: "email", Priority: taskv1.Priority_PRIORITY_HIGH}
	payload := marshalTask(t, task)

	if err := c.processMessage(context.Background(), []byte("task-abc"), payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sender.callCount != 2 {
		t.Fatalf("expected 2 SendUpdate calls (RUNNING + COMPLETED), got %d", sender.callCount)
	}

	running := sender.updates[0]
	if running.GetStatus() != taskv1.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("first update should be RUNNING, got %s", running.GetStatus())
	}
	if running.GetProgress() != 0 {
		t.Errorf("RUNNING progress should be 0, got %d", running.GetProgress())
	}
	if running.GetTaskId() != "task-abc" {
		t.Errorf("expected task_id 'task-abc', got %q", running.GetTaskId())
	}
	if running.GetWorkerId() != "test-worker" {
		t.Errorf("expected worker_id 'test-worker', got %q", running.GetWorkerId())
	}

	completed := sender.updates[1]
	if completed.GetStatus() != taskv1.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("second update should be COMPLETED, got %s", completed.GetStatus())
	}
	if completed.GetProgress() != 100 {
		t.Errorf("COMPLETED progress should be 100, got %d", completed.GetProgress())
	}
}

func TestProcessMessage_CorruptProtobuf(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	c := newTestConsumer(sender)

	if err := c.processMessage(context.Background(), []byte("task-bad"), []byte("not-protobuf")); err == nil {
		t.Fatal("expected error for corrupt protobuf, got nil")
	}
	if sender.callCount != 0 {
		t.Errorf("SendUpdate should not be called for corrupt message, got %d calls", sender.callCount)
	}
}

func TestProcessMessage_ContextCancelledDuringWork(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	c := newTestConsumer(sender)

	task := &taskv1.Task{Type: "slow"}
	payload := marshalTask(t, task)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := c.processMessage(ctx, []byte("task-slow"), payload)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	if sender.callCount != 1 {
		t.Errorf("expected 1 SendUpdate call (RUNNING only), got %d", sender.callCount)
	}
	if sender.updates[0].GetStatus() != taskv1.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("expected RUNNING status, got %s", sender.updates[0].GetStatus())
	}
}

func TestProcessMessage_ReporterFailsOnRunning(t *testing.T) {
	t.Parallel()

	sender := &mockSender{
		err:       errors.New("stream broken"),
		errOnCall: 1,
	}
	c := newTestConsumer(sender)

	task := &taskv1.Task{Type: "email"}
	payload := marshalTask(t, task)

	if err := c.processMessage(context.Background(), []byte("task-x"), payload); err == nil {
		t.Fatal("expected error when reporter fails, got nil")
	}

	if sender.callCount != 1 {
		t.Errorf("expected 1 SendUpdate call, got %d", sender.callCount)
	}
}

// ── processMessage: failure / DLQ ─────────────────────────────────────────────

func TestProcessMessage_FailureWithRetriesRemaining(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	dlq := &mockDLQPublisher{}
	c := newTestConsumer(sender)
	c.dlqPublisher = dlq
	c.failureDecider = func() bool { return true } // always fail

	task := &taskv1.Task{Type: "email", MaxRetries: 2}
	payload := marshalTask(t, task)

	err := c.processMessage(context.Background(), []byte("task-retry"), payload)
	if err == nil {
		t.Fatal("expected error so offset is not committed, got nil")
	}

	// Should have sent RUNNING then FAILED.
	if sender.callCount != 2 {
		t.Fatalf("expected 2 SendUpdate calls (RUNNING + FAILED), got %d", sender.callCount)
	}
	if sender.updates[1].GetStatus() != taskv1.TaskStatus_TASK_STATUS_FAILED {
		t.Errorf("second update should be FAILED, got %s", sender.updates[1].GetStatus())
	}

	// DLQ must NOT be called — retries remain.
	if len(dlq.published) != 0 {
		t.Errorf("DLQ should not be called when retries remain, got %d calls", len(dlq.published))
	}
}

func TestProcessMessage_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	dlq := &mockDLQPublisher{}
	c := newTestConsumer(sender)
	c.dlqPublisher = dlq
	c.failureDecider = func() bool { return true }

	// MaxRetries=0 → after one failure, MaxRetries becomes -1 → send to DLQ.
	task := &taskv1.Task{Type: "email", MaxRetries: 0}
	payload := marshalTask(t, task)

	err := c.processMessage(context.Background(), []byte("task-dlq"), payload)
	if err != nil {
		t.Fatalf("expected nil (offset should be committed after DLQ), got %v", err)
	}

	// FAILED status must have been reported.
	if sender.callCount < 2 {
		t.Fatalf("expected at least 2 SendUpdate calls, got %d", sender.callCount)
	}
	hasFailed := false
	for _, u := range sender.updates {
		if u.GetStatus() == taskv1.TaskStatus_TASK_STATUS_FAILED {
			hasFailed = true
		}
	}
	if !hasFailed {
		t.Error("expected FAILED status to be sent before DLQ publish")
	}

	// DLQ must have received the task.
	if len(dlq.published) != 1 || dlq.published[0] != "task-dlq" {
		t.Errorf("expected DLQ publish for 'task-dlq', got %v", dlq.published)
	}
}

func TestProcessMessage_FailureNoDLQPublisher(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	c := newTestConsumer(sender)
	c.dlqPublisher = nil // no DLQ configured
	c.failureDecider = func() bool { return true }

	task := &taskv1.Task{Type: "email", MaxRetries: 0}
	payload := marshalTask(t, task)

	// Must not panic; just logs.
	err := c.processMessage(context.Background(), []byte("task-nodlq"), payload)
	if err != nil {
		t.Fatalf("expected nil when DLQ publisher is absent, got %v", err)
	}
}

// ── NewConsumer / Run / Close ──────────────────────────────────────────────────

func TestNewConsumer_InitialisesFields(t *testing.T) {
	t.Parallel()

	sender := &mockSender{}
	dlq := &mockDLQPublisher{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	c := NewConsumer("localhost:9092", "tasks.pending", "dispatch-workers", "w-1",
		sender, dlq, logger)
	if c == nil {
		t.Fatal("expected non-nil Consumer")
	}
	if c.workerID != "w-1" {
		t.Errorf("expected workerID 'w-1', got %q", c.workerID)
	}
	if c.reader == nil {
		t.Error("expected non-nil kafka.Reader")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}
}

func TestConsumer_Close(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(&mockSender{})
	c.reader = &mockReader{}
	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConsumer_Close_Error(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(&mockSender{})
	c.reader = &mockReader{closeErr: errors.New("close failed")}
	if err := c.Close(); err == nil {
		t.Fatal("expected error from reader.Close, got nil")
	}
}

func TestConsumer_Run_ContextAlreadyCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := newTestConsumer(&mockSender{})
	c.reader = &mockReader{}

	if err := c.Run(ctx); err != nil {
		t.Fatalf("expected nil on context cancel, got %v", err)
	}
}

func TestConsumer_Run_ProcessesOneMessage(t *testing.T) {
	t.Parallel()

	task := &taskv1.Task{Type: "batch"}
	payload, err := proto.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	committed := make(chan struct{}, 1)
	reader := &mockReader{
		messages: []kafka.Message{{Key: []byte("task-run-1"), Value: payload}},
		onCommit: func() { committed <- struct{}{} },
	}

	sender := &mockSender{}
	c := newTestConsumer(sender)
	c.reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	select {
	case <-committed:
		cancel()
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message to be committed")
	}

	if err := <-runDone; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sender.callCount != 2 {
		t.Errorf("expected 2 SendUpdate calls, got %d", sender.callCount)
	}
}

func TestConsumer_Run_FetchError(t *testing.T) {
	t.Parallel()

	reader := &mockReader{fetchErr: errors.New("broker unavailable")}
	c := newTestConsumer(&mockSender{})
	c.reader = reader

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("expected error from fetch failure, got nil")
	}
}

func TestConsumer_Run_ProcessErrorStopsBeforeNextFetch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	commits := 0
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("failed"), Value: []byte("not-protobuf")},
			{Key: []byte("healthy"), Value: marshalTask(t, &taskv1.Task{Type: "simulation"})},
		},
		onCommit: func() { commits++; cancel() },
	}
	c := newTestConsumer(&mockSender{})
	c.reader = reader
	if err := c.Run(ctx); err == nil {
		t.Error("Run must return a processing error instead of advancing to later work")
	}
	if reader.idx != 1 || commits != 0 {
		t.Errorf("fetched %d messages and committed %d; want one fetch and no commits", reader.idx, commits)
	}
}

func TestConsumer_Run_CancelledDuringProcessing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	commits := 0
	reader := &mockReader{
		messages: []kafka.Message{{Key: []byte("interrupted"), Value: marshalTask(t, &taskv1.Task{Type: "simulation"})}},
		onCommit: func() { commits++ },
	}
	// Cancel from inside processing, after RUNNING is sent, rather than racing
	// startup against a short timer that could cancel before the first fetch.
	sender := &mockSender{onSend: cancel}
	c := newTestConsumer(sender)
	c.reader = reader
	if err := c.Run(ctx); err != nil {
		t.Errorf("context cancellation should shut down cleanly, got %v", err)
	}
	if commits != 0 {
		t.Errorf("committed interrupted work %d times", commits)
	}
	if sender.callCount != 1 || sender.updates[0].GetStatus() != taskv1.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("expected interruption after RUNNING, got updates %v", sender.updates)
	}
}
