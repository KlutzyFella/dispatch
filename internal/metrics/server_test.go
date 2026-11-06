package metrics_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ronnitchopra/dispatch/internal/metrics"
)

func TestStartServer_RejectsUnbindableAddress(t *testing.T) {
	t.Run("invalid_port", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := metrics.StartServer(ctx, "127.0.0.1:70000", slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err == nil || !strings.Contains(err.Error(), "70000") {
			t.Fatalf("invalid metrics address must return a diagnostic, got %v", err)
		}
	})
	t.Run("occupied_port", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err = metrics.StartServer(ctx, listener.Addr().String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("metrics startup must preserve the bind error, got %v", err)
		}
	})
}

func TestStartServer_ServesMetricsAndReleasesPort(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := metrics.StartServer(ctx, addr, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	defer client.CloseIdleConnections()
	readyCtx, stopReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopReady()
	for {
		request, err := http.NewRequestWithContext(readyCtx, http.MethodGet, "http://"+addr+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "# HELP") {
				t.Fatalf("metrics endpoint failed: status=%d error=%v body=%s", response.StatusCode, readErr, body)
			}
			break
		}
		select {
		case <-readyCtx.Done():
			t.Fatalf("metrics endpoint never became ready: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			listener.Close()
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("metrics shutdown did not release %s: %v", addr, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
