package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/metrics"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
)

const (
	defaultPort        = "50051"
	defaultKafkaBroker = "localhost:9092"
	defaultKafkaTopic  = "tasks.pending"
	defaultRedisAddr   = "localhost:6379"
	defaultMetricsPort = "9090"
	portEnvVar         = "PORT"
	kafkaBrokerEnvVar  = "KAFKA_BROKER"
	kafkaTopicEnvVar   = "KAFKA_TOPIC"
	redisAddrEnvVar    = "REDIS_ADDR"
	metricsPortEnvVar  = "METRICS_PORT"
	serviceName        = "dispatch.v1.DispatchService"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := os.Getenv(portEnvVar)
	if port == "" {
		port = defaultPort
	}

	if err := run(logger, port); err != nil {
		logger.Error("server exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

// run wires up and starts the gRPC server, blocking until SIGINT/SIGTERM.
func run(logger *slog.Logger, port string) error {
	kafkaBroker := envOrDefault(kafkaBrokerEnvVar, defaultKafkaBroker)
	kafkaTopic := envOrDefault(kafkaTopicEnvVar, defaultKafkaTopic)
	redisAddr := envOrDefault(redisAddrEnvVar, defaultRedisAddr)

	// ── Redis ──────────────────────────────────────────────────────────────────
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Error("redis client close error", slog.String("error", err.Error()))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
	}
	logger.Info("redis connected", slog.String("addr", redisAddr))

	taskStore := state.NewRedisTaskStore(redisClient)

	// ── Kafka producer ─────────────────────────────────────────────────────────
	producer := broker.NewKafkaProducer(kafkaBroker, kafkaTopic, logger)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("kafka producer close error", slog.String("error", err.Error()))
		}
	}()

	logger.Info("kafka producer initialised",
		slog.String("broker", kafkaBroker),
		slog.String("topic", kafkaTopic),
	)

	// ── Metrics HTTP server ────────────────────────────────────────────────────
	metricsPort := envOrDefault(metricsPortEnvVar, defaultMetricsPort)
	metrics.StartServer(ctx, ":"+metricsPort, logger)

	// ── gRPC server ────────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("listening on :%s: %w", port, err)
	}

	grpcSrv := grpc.NewServer()

	dispatchSrv := server.New(logger, producer, taskStore)
	taskv1.RegisterDispatchServiceServer(grpcSrv, dispatchSrv)

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_SERVING)

	reflection.Register(grpcSrv)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("dispatch gRPC server listening",
			slog.String("port", port),
			slog.String("service", serviceName),
		)
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received — draining connections")
		grpcSrv.GracefulStop()
		return nil
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
