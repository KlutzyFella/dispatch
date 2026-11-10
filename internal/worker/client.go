// Package worker implements the Kafka consumer and gRPC client for the dispatch worker.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
)

// progressAckTimeout bounds one update's send and acknowledgment. The server
// performs two Redis commands per update, each bounded by go-redis's default
// 3s read timeout, so a healthy but slow round trip fits well inside it.
const progressAckTimeout = 10 * time.Second

// connCloser abstracts *grpc.ClientConn so tests can substitute a no-op closer.
type connCloser interface {
	Close() error
}

// ProgressReporter manages a long-lived bidirectional gRPC stream to the
// DispatchServer and provides a simple SendUpdate method for progress reporting.
type ProgressReporter struct {
	conn   connCloser
	stream taskv1.DispatchService_ReportProgressClient
	logger *slog.Logger
	// cancel aborts the stream when an update exceeds ackTimeout.
	cancel     context.CancelFunc
	ackTimeout time.Duration
	// inflight tracks an exchange abandoned at its deadline. gRPC forbids
	// CloseSend or another Send concurrently with a pending Send.
	inflight sync.WaitGroup
}

// NewProgressReporter dials the DispatchServer at addr, opens the ReportProgress
// bidi stream, and returns a ready-to-use ProgressReporter.
func NewProgressReporter(ctx context.Context, addr string, logger *slog.Logger) (*ProgressReporter, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialling dispatch server at %s: %w", addr, err)
	}

	client := taskv1.NewDispatchServiceClient(conn)

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.ReportProgress(streamCtx)
	if err != nil {
		cancel()
		conn.Close()
		return nil, fmt.Errorf("opening ReportProgress stream: %w", err)
	}

	logger.Info("gRPC stream opened", slog.String("server", addr))

	return &ProgressReporter{
		conn:       conn,
		stream:     stream,
		logger:     logger,
		cancel:     cancel,
		ackTimeout: progressAckTimeout,
	}, nil
}

// SendUpdate sends a ProgressUpdate to the server and waits for the ProgressAck.
// This is a synchronous send/recv pair matching the server's 1:1 ack pattern.
// A silent peer produces no transport error, so the exchange is bounded: past
// the deadline the stream is aborted and the reporter is no longer usable.
func (r *ProgressReporter) SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error) {
	type result struct {
		ack *taskv1.ProgressAck
		err error
	}
	r.inflight.Wait()
	done := make(chan result, 1)
	r.inflight.Add(1)
	go func() {
		defer r.inflight.Done()
		if err := r.stream.Send(update); err != nil {
			done <- result{err: fmt.Errorf("sending progress update: %w", err)}
			return
		}
		ack, err := r.stream.Recv()
		if err != nil {
			done <- result{err: fmt.Errorf("receiving progress ack: %w", err)}
			return
		}
		done <- result{ack: ack}
	}()

	timeout := r.ackTimeout
	if timeout <= 0 {
		timeout = progressAckTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-done:
		return res.ack, res.err
	case <-timer.C:
		if r.cancel != nil {
			r.cancel() // unblocks the pending Send/Recv
		}
		return nil, fmt.Errorf("no progress acknowledgment within %v; stream aborted", timeout)
	}
}

// Close signals the end of the stream to the server and closes the connection.
func (r *ProgressReporter) Close() error {
	if r.cancel != nil {
		defer r.cancel()
	}
	r.inflight.Wait()
	if err := r.stream.CloseSend(); err != nil {
		r.logger.Warn("error closing stream", slog.String("error", err.Error()))
	}

	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("closing gRPC connection: %w", err)
	}

	return nil
}
