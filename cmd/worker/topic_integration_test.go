//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"syscall"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

// topicProgressServer observes real consumption, then holds the ACK so random
// simulated execution cannot influence this configuration regression.
type topicProgressServer struct {
	taskv1.UnimplementedDispatchServiceServer
	started chan *taskv1.ProgressUpdate
}

func (s *topicProgressServer) ReportProgress(stream taskv1.DispatchService_ReportProgressServer) error {
	update, err := stream.Recv()
	if err != nil {
		return err
	}
	s.started <- update
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestWorker_ConsumesConfiguredTopic(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	addr, topic := testutil.KafkaTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The old worker subscribes to the shared default topic. Refuse this test
	// against a broker with unrelated work there, rather than consume it.
	conn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	partitions, err := conn.ReadPartitions()
	if err != nil {
		t.Fatal(err)
	}
	for _, partition := range partitions {
		if partition.Topic != defaultKafkaTopic {
			continue
		}
		leader, err := kafka.DialLeader(ctx, "tcp", addr, partition.Topic, partition.ID)
		if err != nil {
			t.Fatal(err)
		}
		leader.SetDeadline(deadline)
		first, last, err := leader.ReadOffsets()
		leader.Close()
		if err != nil || first != last {
			t.Fatalf("configuration regression requires an isolated broker with no default-topic work: first=%d last=%d error=%v", first, last, err)
		}
	}
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
	if err := store.SaveTask(ctx, state.TaskRecord{ID: topic, TaskType: "simulation", MaxRetries: 1,
		Status: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	producer := broker.NewKafkaProducer(addr, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer producer.Close()
	if err := testutil.RetryKafkaMetadata(ctx, func() error {
		return producer.Publish(ctx, topic, &taskv1.Task{Type: "simulation", MaxRetries: 1})
	}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	progress := &topicProgressServer{started: make(chan *taskv1.ProgressUpdate, 1)}
	grpcServer := grpc.NewServer()
	taskv1.RegisterDispatchServiceServer(grpcServer, progress)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	process := testutil.StartMainProcess(t, map[string]string{
		"KAFKA_BROKER": addr, "KAFKA_TOPIC": topic, "KAFKA_GROUP_ID": topic,
		"REDIS_ADDR": client.Options().Addr, "GRPC_SERVER_ADDR": listener.Addr().String(),
		"METRICS_PORT": testutil.UnusedTCPPort(t),
	})
	select {
	case update := <-progress.started:
		if update.GetTaskId() != topic || update.GetStatus() != taskv1.TaskStatus_TASK_STATUS_RUNNING {
			t.Errorf("worker consumed unexpected work: %v", update)
		}
		if count, err := client.HGet(ctx, "task:"+topic, "attempts").Int64(); err != nil || count != 1 {
			t.Errorf("configured task was not started: attempts=%d error=%v", count, err)
		}
	case <-time.After(12 * time.Second):
		t.Error("worker ignored KAFKA_TOPIC and did not consume its configured queue")
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// kafka-go's default fetch wait is ten seconds; allow it to finish during
	// cleanup. The regression above concerns topic selection, not shutdown speed.
	if err, exited := process.Wait(15 * time.Second); !exited || err != nil {
		t.Fatalf("worker did not stop cleanly: exited=%v error=%v", exited, err)
	}
}
