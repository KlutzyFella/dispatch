//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

type pausedPublication struct {
	client   *redis.Client
	producer broker.Producer
	afterAck bool
}

func (p *pausedPublication) Publish(ctx context.Context, id string, task *taskv1.Task) error {
	if p.afterAck {
		if err := testutil.RetryKafkaMetadata(ctx, func() error { return p.producer.Publish(ctx, id, task) }); err != nil {
			return err
		}
	}
	if err := p.client.Set(ctx, "publication-pause:"+id, "ready", 0).Err(); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
func (p *pausedPublication) Close() error { return p.producer.Close() }

func recoveryTask(id string) *taskv1.Task {
	return &taskv1.Task{IdempotencyKey: id, Type: "original", Payload: []byte("original retained payload"), MaxRetries: 2,
		Metadata: map[string]string{"source": "crash-regression"}}
}

func TestDispatch_RecoversPublicationAfterCrash(t *testing.T) {
	if testutil.MainProcessChild() {
		if os.Getenv("PUBLICATION_TEST_MODE") == "creator" {
			client := testutil.RedisClient(t)
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			producer := &pausedPublication{client: client,
				producer: broker.NewKafkaProducer(os.Getenv("KAFKA_BROKER"), os.Getenv("KAFKA_TOPIC"), logger),
				afterAck: os.Getenv("PUBLICATION_TEST_AFTER_ACK") == "1"}
			defer producer.Close()
			srv := server.New(logger, producer, state.NewRedisTaskStore(client), os.Getenv("KAFKA_TOPIC"))
			_, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{Task: recoveryTask(os.Getenv("PUBLICATION_TEST_ID"))})
			t.Fatalf("creator should have been killed at its publication boundary, got %v", err)
		}
		main()
		return
	}
	for _, afterAck := range []bool{false, true} {
		name := "before_kafka_write"
		if afterAck {
			name = "after_ack_before_confirmation"
		}
		t.Run(name, func(t *testing.T) {
			addr, topic := testutil.KafkaTopic(t)
			client := testutil.RedisClient(t)
			t.Cleanup(func() {
				client.Del(context.Background(), "task:"+topic, "publication:"+topic, "outbox:"+topic, "publication-pause:"+topic)
			})
			env := map[string]string{"KAFKA_BROKER": addr, "KAFKA_TOPIC": topic, "REDIS_ADDR": client.Options().Addr,
				"PUBLICATION_TEST_MODE": "creator", "PUBLICATION_TEST_ID": topic}
			if afterAck {
				env["PUBLICATION_TEST_AFTER_ACK"] = "1"
			}
			creator := testutil.StartMainProcess(t, env)
			pauseCtx, stopPause := context.WithTimeout(context.Background(), 8*time.Second)
			defer stopPause()
			for {
				if value, err := client.Get(pauseCtx, "publication-pause:"+topic).Result(); err == nil && value == "ready" {
					break
				}
				select {
				case <-pauseCtx.Done():
					t.Fatal("creator never reached the durable publication boundary")
				case <-time.After(20 * time.Millisecond):
				}
			}
			if err := creator.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			if err, exited := creator.Wait(3 * time.Second); !exited || err == nil {
				t.Fatalf("creator was not killed: exited=%v error=%v", exited, err)
			}
			reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{addr}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
			defer reader.Close()
			if afterAck {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				first, err := reader.ReadMessage(ctx)
				cancel()
				if err != nil || first.Offset != 0 {
					t.Fatalf("first broker acknowledgment was not real: message=%+v error=%v", first, err)
				}
			}
			port := testutil.UnusedTCPPort(t)
			restarted := testutil.StartMainProcess(t, map[string]string{
				"KAFKA_BROKER": addr, "KAFKA_TOPIC": topic, "REDIS_ADDR": client.Options().Addr,
				"PORT": port, "METRICS_PORT": testutil.UnusedTCPPort(t), "PUBLICATION_TEST_MODE": "recovery",
			})
			conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			readyCtx, stopReady := context.WithTimeout(context.Background(), 8*time.Second)
			defer stopReady()
			if _, err := grpc_health_v1.NewHealthClient(conn).Check(readyCtx, &grpc_health_v1.HealthCheckRequest{Service: serviceName}, grpc.WaitForReady(true)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 22*time.Second)
			defer cancel()
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				t.Fatalf("retained publication was not recovered after restart: %v", err)
			}
			var task taskv1.Task
			if err := proto.Unmarshal(message.Value, &task); err != nil {
				t.Fatal(err)
			}
			if string(message.Key) != topic || !proto.Equal(&task, recoveryTask(topic)) {
				t.Fatalf("recovery changed the original task: key=%q task=%v", message.Key, &task)
			}
			// Kafka delivery can precede the Redis confirmation; wait for that state,
			// then prove a duplicate RPC is accepted without replacing its payload.
			for {
				if value, err := client.HGet(ctx, "task:"+topic, "publication_state").Result(); err == nil && value == "published" {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("publication was delivered but not confirmed")
				case <-time.After(20 * time.Millisecond):
				}
			}
			response, err := taskv1.NewDispatchServiceClient(conn).SubmitTask(ctx, &taskv1.SubmitTaskRequest{Task: &taskv1.Task{IdempotencyKey: topic, Type: "replacement"}})
			if err != nil || response.GetStatus() != taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED {
				t.Fatalf("confirmed duplicate was not accepted: response=%v error=%v", response, err)
			}
			leader, err := kafka.DialLeader(ctx, "tcp", addr, topic, 0)
			if err != nil {
				t.Fatal(err)
			}
			deadline, _ := ctx.Deadline()
			leader.SetDeadline(deadline)
			end, endErr := leader.ReadLastOffset()
			leader.Close()
			want := int64(1)
			if afterAck {
				want = 2
			}
			if endErr != nil || end != want {
				t.Fatalf("unexpected recovery deliveries: records=%d want=%d error=%v", end, want, endErr)
			}
			t.Logf("crash after_ack=%v recovered original task; Kafka records=%d", afterAck, end)
			if err := restarted.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err, exited := restarted.Wait(8 * time.Second); !exited || err != nil {
				t.Fatalf("recovery server did not stop cleanly: exited=%v error=%v", exited, err)
			}
			if n, err := client.Exists(context.Background(), "publication:"+topic).Result(); err != nil || n != 0 {
				t.Fatalf("confirmed intent was not removed: exists=%d error=%v", n, err)
			}
		})
	}
}

func TestDispatch_FailsIfPublicationRecoveryStoreBreaks(t *testing.T) {
	if testutil.MainProcessChild() {
		main()
		return
	}
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	t.Cleanup(func() { client.Del(context.Background(), "outbox:"+topic) })
	port := testutil.UnusedTCPPort(t)
	process := testutil.StartMainProcess(t, map[string]string{"KAFKA_BROKER": addr, "KAFKA_TOPIC": topic,
		"REDIS_ADDR": client.Options().Addr, "PORT": port, "METRICS_PORT": testutil.UnusedTCPPort(t)})
	conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: serviceName}, grpc.WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "outbox:"+topic, "invalid index", 0).Err(); err != nil {
		t.Fatal(err)
	}
	exitErr, exited := process.Wait(5 * time.Second)
	if !exited {
		t.Error("dispatch kept serving after publication recovery failed")
		process.Signal(syscall.SIGTERM)
		exitErr, exited = process.Wait(8 * time.Second)
	}
	var exit *exec.ExitError
	if !exited || !errors.As(exitErr, &exit) || exit.ExitCode() != 1 || !strings.Contains(process.Logs(t), "publication recovery") {
		t.Fatalf("recovery failure was not fatal/diagnosable: exited=%v error=%v logs=%s", exited, exitErr, process.Logs(t))
	}
}
