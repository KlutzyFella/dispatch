package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// CheckReady requires a Kafka protocol response with registered brokers before
// startup proceeds. It neither publishes probe records nor provisions topics.
// The ten-second deadline is a per-probe policy, not a latency measurement.
func CheckReady(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(addr), Transport: transport}
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{}})
	if err != nil {
		return fmt.Errorf("checking Kafka readiness at %s: %w", addr, err)
	}
	if len(metadata.Brokers) == 0 {
		return fmt.Errorf("checking Kafka readiness at %s: no registered brokers", addr)
	}
	return nil
}
