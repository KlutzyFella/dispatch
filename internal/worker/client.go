// Package worker implements the Kafka consumer and gRPC client for the dispatch worker.
package worker

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
)

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
}

// NewProgressReporter dials the DispatchServer at addr, opens the ReportProgress
// bidi stream, and returns a ready-to-use ProgressReporter.
func NewProgressReporter(ctx context.Context, addr string, logger *slog.Logger) (*ProgressReporter, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialling dispatch server at %s: %w", addr, err)
	}

	client := taskv1.NewDispatchServiceClient(conn)

	stream, err := client.ReportProgress(ctx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("opening ReportProgress stream: %w", err)
	}

	logger.Info("gRPC stream opened", slog.String("server", addr))

	return &ProgressReporter{
		conn:   conn,
		stream: stream,
		logger: logger,
	}, nil
}

// SendUpdate sends a ProgressUpdate to the server and waits for the ProgressAck.
// This is a synchronous send/recv pair matching the server's 1:1 ack pattern.
func (r *ProgressReporter) SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error) {
	if err := r.stream.Send(update); err != nil {
		return nil, fmt.Errorf("sending progress update: %w", err)
	}

	ack, err := r.stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receiving progress ack: %w", err)
	}

	return ack, nil
}

// Close signals the end of the stream to the server and closes the connection.
func (r *ProgressReporter) Close() error {
	if err := r.stream.CloseSend(); err != nil {
		r.logger.Warn("error closing stream", slog.String("error", err.Error()))
	}

	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("closing gRPC connection: %w", err)
	}

	return nil
}
