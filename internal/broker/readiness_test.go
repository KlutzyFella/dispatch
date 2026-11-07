package broker_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ronnitchopra/dispatch/internal/broker"
)

func TestCheckReady_DeadlineBoundsSilentBroker(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Bound even a broken probe without relying on the Go test timeout.
		fallback := time.AfterFunc(time.Second, func() { conn.Close() })
		defer fallback.Stop()
		io.Copy(io.Discard, conn)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = broker.CheckReady(ctx, listener.Addr().String())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a TCP connection without a Kafka response must respect the deadline, got %v", err)
	}
	listener.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("silent broker fixture did not close")
	}
}
