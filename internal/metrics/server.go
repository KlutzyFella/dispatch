package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartServer starts an HTTP server that exposes the default Prometheus
// registry at /metrics on the given addr (e.g. ":9090").
// Binding is synchronous: startup errors are returned before any goroutine is
// started. Later serving errors are logged. Cancellation shuts down the server.
func StartServer(ctx context.Context, addr string, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening for metrics on %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", slog.String("error", err.Error()))
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics server shutdown error", slog.String("error", err.Error()))
		}
		logger.Info("metrics HTTP server stopped", slog.String("addr", addr))
	}()

	logger.Info("metrics HTTP server listening", slog.String("addr", listener.Addr().String()))
	return nil
}
