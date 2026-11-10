package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
)

// noopConn implements connCloser for testing.
type noopConn struct{ closeErr error }

func (n *noopConn) Close() error { return n.closeErr }

// mockStream implements taskv1.DispatchService_ReportProgressClient for testing.
type mockStream struct {
	grpc.ClientStream
	sent   []*taskv1.ProgressUpdate
	acks   []*taskv1.ProgressAck
	ackIdx int

	closeSendCalled bool
	sendErr         error
	recvErr         error
}

func (m *mockStream) Send(u *taskv1.ProgressUpdate) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, u)
	return nil
}

func (m *mockStream) Recv() (*taskv1.ProgressAck, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if m.ackIdx >= len(m.acks) {
		return nil, io.EOF
	}
	ack := m.acks[m.ackIdx]
	m.ackIdx++
	return ack, nil
}

func (m *mockStream) CloseSend() error {
	m.closeSendCalled = true
	return nil
}

func TestProgressReporter_SendUpdate(t *testing.T) {
	t.Parallel()

	stream := &mockStream{
		acks: []*taskv1.ProgressAck{{Recorded: true}},
	}

	reporter := &ProgressReporter{
		stream: stream,
	}

	update := &taskv1.ProgressUpdate{
		TaskId:   "task-1",
		WorkerId: "worker-1",
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 0,
	}

	ack, err := reporter.SendUpdate(update)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ack.GetRecorded() {
		t.Error("expected ack.Recorded to be true")
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(stream.sent))
	}
	if stream.sent[0].GetTaskId() != "task-1" {
		t.Errorf("expected task_id 'task-1', got %q", stream.sent[0].GetTaskId())
	}
}

func TestProgressReporter_SendUpdate_SendError(t *testing.T) {
	t.Parallel()

	stream := &mockStream{
		sendErr: io.ErrClosedPipe,
	}

	reporter := &ProgressReporter{stream: stream}
	_, err := reporter.SendUpdate(&taskv1.ProgressUpdate{})
	if err == nil {
		t.Fatal("expected error from failed Send, got nil")
	}
}

func TestProgressReporter_SendUpdate_RecvError(t *testing.T) {
	t.Parallel()

	stream := &mockStream{
		recvErr: io.ErrUnexpectedEOF,
	}

	reporter := &ProgressReporter{stream: stream}
	_, err := reporter.SendUpdate(&taskv1.ProgressUpdate{})
	if err == nil {
		t.Fatal("expected error from failed Recv, got nil")
	}
}

func TestProgressReporter_Close_CallsCloseSend(t *testing.T) {
	t.Parallel()

	stream := &mockStream{}
	conn := &noopConn{}

	reporter := &ProgressReporter{stream: stream, conn: conn}
	if err := reporter.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stream.closeSendCalled {
		t.Error("expected CloseSend to have been called")
	}
}

func TestProgressReporter_Close_ConnError(t *testing.T) {
	t.Parallel()

	stream := &mockStream{}
	conn := &noopConn{closeErr: io.ErrClosedPipe}

	reporter := &ProgressReporter{stream: stream, conn: conn}
	if err := reporter.Close(); err == nil {
		t.Fatal("expected error from conn.Close, got nil")
	}
}

// The worker opens its progress stream at startup so that an unreachable
// dispatch server fails startup instead of the first task. The connection is
// refused on a local port that was just closed; no external network is used.
func TestNewProgressReporter_UnreachableServerFailsFast(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	reporter, err := NewProgressReporter(ctx, addr, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if reporter != nil {
		reporter.Close()
	}
	if status.Code(errors.Unwrap(err)) != codes.Unavailable {
		t.Fatalf("unreachable dispatch server returned %v; want an Unavailable stream error", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("startup took %v to report an unreachable server", elapsed)
	}
}
