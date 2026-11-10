//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ronnitchopra/dispatch/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// redisOutageProxy forwards to the real Redis until cut; cutting it closes the
// listener and every forwarded connection, so dispatch sees what it would see
// if the Redis server went down: reset connections and refused reconnects.
type redisOutageProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func startRedisOutageProxy(t *testing.T, target string) *redisOutageProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &redisOutageProxy{ln: ln}
	t.Cleanup(p.cut)
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go func() { io.Copy(upstream, client); upstream.Close() }()
			go func() { io.Copy(client, upstream); client.Close() }()
		}
	}()
	return p
}

func (p *redisOutageProxy) addr() string { return p.ln.Addr().String() }

func (p *redisOutageProxy) cut() {
	p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

// Losing Redis after startup must stop dispatch with a diagnosable error and
// release its ports, rather than leave a gRPC API that looks healthy while
// recovery and admission cannot work. This is the fail-fast policy: dispatch
// does not ride out a Redis outage longer than go-redis's own retries, and it
// relies on a supervisor to restart it once Redis is back.
func TestDispatch_ExitsWhenRedisGoesAway(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	addr, topic := testutil.KafkaTopic(t)
	redis := testutil.RedisClient(t)
	proxy := startRedisOutageProxy(t, redis.Options().Addr)
	port := testutil.UnusedTCPPort(t)
	process := testutil.StartMainProcess(t, map[string]string{"KAFKA_BROKER": addr, "KAFKA_TOPIC": topic,
		"REDIS_ADDR": proxy.addr(), "PORT": port, "METRICS_PORT": testutil.UnusedTCPPort(t)})
	conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: serviceName}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("dispatch never became ready through the Redis proxy: %v\n%s", err, process.Logs(t))
	}

	proxy.cut()
	start := time.Now()
	exitErr, exited := process.Wait(10 * time.Second)
	if !exited {
		t.Error("dispatch kept running with Redis gone")
		process.Signal(syscall.SIGTERM)
		exitErr, exited = process.Wait(8 * time.Second)
	}
	var exit *exec.ExitError
	logs := process.Logs(t)
	if !exited || !errors.As(exitErr, &exit) || exit.ExitCode() != 1 || !strings.Contains(logs, "publication recovery") {
		t.Fatalf("Redis loss was not fatal and diagnosable: exited=%v error=%v logs=%s", exited, exitErr, logs)
	}
	t.Logf("dispatch exited 1 %v after Redis went away", time.Since(start).Round(10*time.Millisecond))
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("gRPC port %s still held after exit: %v", port, err)
	}
	listener.Close()
}
