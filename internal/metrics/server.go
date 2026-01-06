package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartServer starts an HTTP server that exposes the default Prometheus
// registry at /metrics on the given addr (e.g. ":9090").
// It shuts down gracefully when ctx is cancelled.
// Errors from ListenAndServe (other than http.ErrServerClosed) are returned
// via the errCh channel that callers can ignore.
func StartServer(ctx context.Context, addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Info("metrics HTTP server listening", slog.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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

	logger.Info("metrics server started", slog.String("addr", fmt.Sprintf("http://localhost%s/metrics", addr)))
}
