//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"strconv"
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

func TestDispatch_FailsWhenMetricsPortOccupied(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	redis := testutil.RedisClient(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	metricsPort := strconv.Itoa(occupied.Addr().(*net.TCPAddr).Port)
	port := testutil.UnusedTCPPort(t)
	process := testutil.StartMainProcess(t, map[string]string{
		"PORT": port, "METRICS_PORT": metricsPort,
		"REDIS_ADDR": redis.Options().Addr, "KAFKA_BROKER": "127.0.0.1:1",
	})
	exitErr, exited := process.Wait(3 * time.Second)
	if !exited {
		t.Error("dispatch kept running after its metrics listener failed to bind")
		if err := process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		exitErr, exited = process.Wait(8 * time.Second)
	}
	var exit *exec.ExitError
	if !exited || !errors.As(exitErr, &exit) || exit.ExitCode() != 1 {
		t.Errorf("startup failure must exit 1, got exited=%v error=%v", exited, exitErr)
	}
	logs := process.Logs(t)
	if !strings.Contains(logs, "starting metrics server") || !strings.Contains(logs, metricsPort) {
		t.Errorf("startup error did not identify metrics and its occupied port:\n%s", logs)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("failed startup left gRPC port %s occupied: %v", port, err)
	}
	listener.Close()
}
