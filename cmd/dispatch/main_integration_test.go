//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestDispatch_ShutdownWithProgressStream(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	for _, held := range []bool{true, false} {
		name := "client_closes_stream"
		if held {
			name = "client_holds_stream_open"
		}
		t.Run(name, func(t *testing.T) {
			redis := testutil.RedisClient(t)
			port, metricsPort := testutil.UnusedTCPPort(t), testutil.UnusedTCPPort(t)
			process := testutil.StartMainProcess(t, map[string]string{
				"PORT": port, "METRICS_PORT": metricsPort,
				"REDIS_ADDR": redis.Options().Addr, "KAFKA_BROKER": "127.0.0.1:1",
			})
			conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			readyCtx, stopReady := context.WithTimeout(context.Background(), 8*time.Second)
			defer stopReady()
			health, err := grpc_health_v1.NewHealthClient(conn).Check(readyCtx,
				&grpc_health_v1.HealthCheckRequest{Service: serviceName}, grpc.WaitForReady(true))
			if err != nil || health.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
				t.Fatalf("dispatch did not become ready: response=%v error=%v", health, err)
			}
			streamCtx, stopStream := context.WithCancel(context.Background())
			defer stopStream()
			stream, err := taskv1.NewDispatchServiceClient(conn).ReportProgress(streamCtx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&taskv1.ProgressUpdate{
				TaskId: "shutdown-test-" + uuid.NewString(), WorkerId: "shutdown-test",
				Status: taskv1.TaskStatus_TASK_STATUS_RUNNING,
			}); err != nil {
				t.Fatal(err)
			}
			if ack, err := stream.Recv(); err != nil || ack.GetRecorded() {
				t.Fatalf("progress stream was not established against an unknown task: ack=%v error=%v", ack, err)
			}
			if !held {
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("closed progress stream did not finish cleanly: %v", err)
				}
			}
			if err := process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			limit := 8 * time.Second
			if !held {
				limit = 3 * time.Second
			}
			exitErr, exited := process.Wait(limit)
			if !exited {
				t.Errorf("dispatch did not exit within the shutdown observation window while held_stream=%v", held)
				// Release the original unbounded GracefulStop so the regression itself
				// does not leave the old process hanging after it has failed.
				stopStream()
				exitErr, exited = process.Wait(5 * time.Second)
			}
			if !exited || exitErr != nil {
				t.Fatalf("dispatch shutdown failed: exited=%v error=%v", exited, exitErr)
			}
			forced := strings.Contains(process.Logs(t), "forcing gRPC shutdown")
			if forced != held {
				t.Errorf("forced shutdown=%v, want %v", forced, held)
			}
			for _, released := range []string{port, metricsPort} {
				listener, err := net.Listen("tcp", "127.0.0.1:"+released)
				if err != nil {
					t.Fatalf("shutdown did not release port %s: %v", released, err)
				}
				listener.Close()
			}
		})
	}
}
