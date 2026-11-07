package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/metrics"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/worker"
)

const (
	defaultKafkaBroker  = "localhost:9092"
	defaultKafkaGroupID = "dispatch-workers"
	defaultKafkaTopic   = "tasks.pending"
	defaultDLQTopic     = "tasks.dead_letter"
	defaultGRPCAddr     = "localhost:50051"
	defaultRedisAddr    = "localhost:6379"
	defaultMetricsPort  = "9091"

	envKafkaBroker  = "KAFKA_BROKER"
	envKafkaTopic   = "KAFKA_TOPIC"
	envKafkaGroupID = "KAFKA_GROUP_ID"
	envGRPCAddr     = "GRPC_SERVER_ADDR"
	envRedisAddr    = "REDIS_ADDR"
	envMetricsPort  = "METRICS_PORT"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("worker exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

// run wires up the worker and returns on SIGINT/SIGTERM or a service error.
func run(logger *slog.Logger) error {
	kafkaBroker := envOrDefault(envKafkaBroker, defaultKafkaBroker)
	kafkaTopic := envOrDefault(envKafkaTopic, defaultKafkaTopic)
	groupID := envOrDefault(envKafkaGroupID, defaultKafkaGroupID)
	grpcAddr := envOrDefault(envGRPCAddr, defaultGRPCAddr)
	redisAddr := envOrDefault(envRedisAddr, defaultRedisAddr)

	workerID := uuid.NewString()

	logger.Info("starting worker",
		slog.String("worker_id", workerID),
		slog.String("kafka_broker", kafkaBroker),
		slog.String("kafka_topic", kafkaTopic),
		slog.String("group_id", groupID),
		slog.String("grpc_addr", grpcAddr),
		slog.String("redis_addr", redisAddr),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Redis persists started-attempt counts across worker restarts.
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Error("redis client close error", slog.String("error", err.Error()))
		}
	}()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
	}
	attemptStore := state.NewRedisTaskStore(redisClient)

	// ── gRPC progress reporter ─────────────────────────────────────────────────
	reporter, err := worker.NewProgressReporter(ctx, grpcAddr, logger)
	if err != nil {
		return fmt.Errorf("initialising gRPC reporter: %w", err)
	}
	defer func() {
		if err := reporter.Close(); err != nil {
			logger.Error("reporter close error", slog.String("error", err.Error()))
		}
	}()

	// ── DLQ producer ──────────────────────────────────────────────────────────
	dlqProducer := broker.NewKafkaProducer(kafkaBroker, defaultDLQTopic, logger)
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			logger.Error("dlq producer close error", slog.String("error", err.Error()))
		}
	}()

	// ── Metrics HTTP server ────────────────────────────────────────────────────
	metricsPort := envOrDefault(envMetricsPort, defaultMetricsPort)
	if err := metrics.StartServer(ctx, ":"+metricsPort, logger); err != nil {
		return fmt.Errorf("starting metrics server: %w", err)
	}
	if err := broker.CheckReady(ctx, kafkaBroker); err != nil {
		return err
	}

	// ── Kafka consumer ─────────────────────────────────────────────────────────
	consumer := worker.NewConsumer(
		kafkaBroker, kafkaTopic, groupID, workerID,
		reporter, attemptStore, dlqProducer,
		logger,
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("consumer close error", slog.String("error", err.Error()))
		}
	}()

	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}

	logger.Info("worker shutdown complete", slog.String("worker_id", workerID))
	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
