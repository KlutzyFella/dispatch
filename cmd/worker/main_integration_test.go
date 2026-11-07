//go:build integration

package main

import (
	"errors"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"google.golang.org/grpc"
)

// startupProgressServer accepts the worker's real streaming connection while
// the subprocess reaches metrics startup. No task execution is involved.
type startupProgressServer struct {
	taskv1.UnimplementedDispatchServiceServer
}

func (*startupProgressServer) ReportProgress(stream taskv1.DispatchService_ReportProgressServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestWorker_FailsWhenMetricsPortOccupied(t *testing.T) {
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	taskv1.RegisterDispatchServiceServer(grpcServer, &startupProgressServer{})
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	process := testutil.StartMainProcess(t, map[string]string{
		"METRICS_PORT": metricsPort, "REDIS_ADDR": redis.Options().Addr,
		"GRPC_SERVER_ADDR": listener.Addr().String(), "KAFKA_BROKER": "127.0.0.1:1",
	})
	exitErr, exited := process.Wait(3 * time.Second)
	if !exited {
		t.Error("worker kept running after its metrics listener failed to bind")
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
	if !strings.Contains(logs, "gRPC stream opened") || !strings.Contains(logs, "starting metrics server") || !strings.Contains(logs, metricsPort) {
		t.Errorf("startup did not reach the metrics bind error with its port:\n%s", logs)
	}
}

func TestWorker_RejectsUnavailableKafka(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	redis := testutil.RedisClient(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	taskv1.RegisterDispatchServiceServer(grpcServer, &startupProgressServer{})
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	process := testutil.StartMainProcess(t, map[string]string{
		"METRICS_PORT": testutil.UnusedTCPPort(t), "REDIS_ADDR": redis.Options().Addr,
		"GRPC_SERVER_ADDR": listener.Addr().String(), "KAFKA_BROKER": "127.0.0.1:1",
	})
	exitErr, exited := process.Wait(3 * time.Second)
	if !exited {
		t.Error("worker started despite unavailable Kafka")
		if err := process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		exitErr, exited = process.Wait(8 * time.Second)
	}
	var exit *exec.ExitError
	if !exited || !errors.As(exitErr, &exit) || exit.ExitCode() != 1 {
		t.Errorf("Kafka readiness failure must exit 1: exited=%v error=%v", exited, exitErr)
	}
	logs := process.Logs(t)
	if !strings.Contains(logs, "checking Kafka readiness") || !strings.Contains(logs, "127.0.0.1:1") {
		t.Errorf("startup did not identify the unavailable broker:\n%s", logs)
	}
}
