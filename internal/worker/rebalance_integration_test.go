//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// storeSender stands in for the dispatch server: it records each update in the
// real Redis store and acknowledges it. hold, if set, blocks the first RUNNING
// update until released, keeping that worker mid-task.
type storeSender struct {
	store   *state.RedisTaskStore
	running chan struct{}
	hold    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	updates []taskv1.TaskStatus
}

func (s *storeSender) SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error) {
	s.mu.Lock()
	s.updates = append(s.updates, update.GetStatus())
	s.mu.Unlock()
	if update.GetStatus() == taskv1.TaskStatus_TASK_STATUS_RUNNING {
		s.once.Do(func() {
			close(s.running)
			if s.hold != nil {
				<-s.hold
			}
		})
	}
	err := s.store.UpdateProgress(context.Background(), update.GetTaskId(), int32(update.GetStatus()), update.GetProgress())
	return &taskv1.ProgressAck{Recorded: err == nil}, nil
}

func (s *storeSender) statuses() []taskv1.TaskStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]taskv1.TaskStatus(nil), s.updates...)
}

func groupMembers(ctx context.Context, addr, group string) (string, int, error) {
	client := &kafka.Client{Addr: kafka.TCP(addr)}
	resp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{group}})
	if err != nil {
		return "", 0, err
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Error != nil {
		return "", 0, resp.Groups[0].Error
	}
	return resp.Groups[0].GroupState, len(resp.Groups[0].Members), nil
}

// TestConsumer_RebalanceHandsOffInterruptedWork covers a live consumer-group
// handoff rather than a sequential restart. Worker A holds a fetched message
// mid-task while worker B joins the same group; A then shuts down. The
// uncommitted message must reach B through the broker's rebalance, finish
// once, count both started attempts, and be committed exactly past itself.
func TestConsumer_RebalanceHandsOffInterruptedWork(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
	if err := store.SaveTask(context.Background(), state.TaskRecord{ID: topic, TaskType: "simulation", MaxRetries: 1,
		Status: int32(taskv1.TaskStatus_TASK_STATUS_PENDING)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	payload, err := proto.Marshal(&taskv1.Task{Type: "simulation", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	writer := &kafka.Writer{Addr: kafka.TCP(addr), Topic: topic, RequiredAcks: kafka.RequireAll}
	if err := testutil.RetryKafkaMetadata(ctx, func() error {
		return writer.WriteMessages(ctx, kafka.Message{Key: []byte(topic), Value: payload})
	}); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	group := "handoff-" + topic
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func(name string, sender *storeSender) (*Consumer, context.CancelFunc, chan error) {
		c := NewConsumer(addr, topic, group, name, sender, store, nil, logger)
		c.failureDecider = func() bool { return false }
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Run(runCtx) }()
		return c, stop, done
	}

	senderA := &storeSender{store: store, running: make(chan struct{}), hold: make(chan struct{})}
	consumerA, stopA, doneA := start("worker-a", senderA)
	select {
	case <-senderA.running:
	case <-ctx.Done():
		t.Fatal("worker A never started the task")
	}

	senderB := &storeSender{store: store, running: make(chan struct{})}
	consumerB, stopB, doneB := start("worker-b", senderB)
	defer func() {
		stopB()
		<-doneB
		consumerB.Close()
	}()
	for {
		state, members, err := groupMembers(ctx, addr, group)
		if err == nil && state == "Stable" && members == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("group never stabilised with both workers: state=%q members=%d error=%v", state, members, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	bTookOverEarly := false
	select {
	case <-senderB.running:
		bTookOverEarly = true // the first rebalance already moved the partition
	default:
	}

	// A shuts down mid-task: interrupted work is not committed, and leaving the
	// group triggers the rebalance that gives B the partition.
	stopA()
	close(senderA.hold)
	if err := <-doneA; err != nil {
		t.Fatalf("worker A shutdown reported an error: %v", err)
	}
	if err := consumerA.Close(); err != nil {
		t.Fatalf("closing worker A: %v", err)
	}

	for {
		record, err := store.GetTask(ctx, topic)
		if err == nil && record.Status == int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED) && committedGroupOffset(t, addr, topic, group) == 1 {
			break
		}
		select {
		case err := <-doneB:
			t.Fatalf("worker B stopped before finishing the handed-off task: %v", err)
		case <-ctx.Done():
			t.Fatalf("handed-off task never completed and committed: record=%+v error=%v updatesA=%v updatesB=%v",
				record, err, senderA.statuses(), senderB.statuses())
		case <-time.After(100 * time.Millisecond):
		}
	}

	attempts, err := client.HGet(ctx, "task:"+topic, "attempts").Int64()
	if err != nil || attempts != 2 {
		t.Errorf("started attempts = %d (error %v); want A's interrupted start plus B's", attempts, err)
	}
	completed := 0
	for _, status := range senderB.statuses() {
		if status == taskv1.TaskStatus_TASK_STATUS_COMPLETED {
			completed++
		}
	}
	if completed != 1 {
		t.Errorf("worker B completed the task %d times; want once (updates %v)", completed, senderB.statuses())
	}
	for _, status := range senderA.statuses() {
		if status == taskv1.TaskStatus_TASK_STATUS_COMPLETED {
			t.Errorf("interrupted worker A reported completion: %v", senderA.statuses())
		}
	}
	t.Logf("handoff complete: B took the partition at first rebalance=%v; A updates=%v B updates=%v attempts=%d committed=1",
		bTookOverEarly, senderA.statuses(), senderB.statuses(), attempts)
}
