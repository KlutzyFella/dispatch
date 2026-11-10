//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"google.golang.org/grpc"
)

type unusedProducer struct{}

func (unusedProducer) Publish(context.Context, string, *taskv1.Task) error { return nil }
func (unusedProducer) Close() error                                        { return nil }

// blackholeProxy forwards TCP until cut, then silently discards bytes in both
// directions while keeping connections open: no RST or FIN reaches either side,
// as with a frozen peer or a network path that drops packets.
func blackholeProxy(t *testing.T, target string) (string, *atomic.Bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		cut   atomic.Bool
		mu    sync.Mutex
		conns []net.Conn
	)
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 && !cut.Load() {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
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
			mu.Lock()
			conns = append(conns, client, upstream)
			mu.Unlock()
			go pipe(upstream, client)
			go pipe(client, upstream)
		}
	}()
	return ln.Addr().String(), &cut
}

// TestProgressReporter_BlackholedStreamFails reproduces a worker hanging on a
// progress acknowledgment that never arrives. Before the per-update deadline,
// SendUpdate blocked until the worker's lifecycle context was cancelled, so a
// worker could hold its partition indefinitely without making progress.
func TestProgressReporter_BlackholedStreamFails(t *testing.T) {
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "blackhole-" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id) })
	if err := store.SaveTask(context.Background(), state.TaskRecord{ID: id, TaskType: "simulation", Status: 1}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	taskv1.RegisterDispatchServiceServer(grpcServer, server.New(slog.New(slog.NewTextHandler(io.Discard, nil)), unusedProducer{}, store, "unused"))
	go grpcServer.Serve(ln)
	t.Cleanup(grpcServer.Stop)

	addr, cut := blackholeProxy(t, ln.Addr().String())
	lifecycle, cancel := context.WithCancel(context.Background()) // no deadline, as in cmd/worker
	defer cancel()
	reporter, err := NewProgressReporter(lifecycle, addr, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reporter.Close() })
	update := &taskv1.ProgressUpdate{TaskId: id, WorkerId: "worker", Status: taskv1.TaskStatus_TASK_STATUS_RUNNING}
	if ack, err := reporter.SendUpdate(update); err != nil || !ack.GetRecorded() {
		t.Fatalf("control: update over the healthy path failed: ack=%v error=%v", ack, err)
	}

	cut.Store(true)
	result := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := reporter.SendUpdate(update)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "acknowledgment") {
			t.Fatalf("blackholed update returned %v; want an acknowledgment deadline error", err)
		}
		t.Logf("blackholed update failed after %v: %v", time.Since(start).Round(time.Millisecond), err)
	case <-time.After(20 * time.Second):
		t.Fatalf("SendUpdate still blocked %v after the dispatch peer went silent", time.Since(start).Round(time.Second))
	}
}
