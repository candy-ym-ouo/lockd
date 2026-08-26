package main

import (
	"context"
	"errors"
	"io/fs"
	"lockd/internal/api"
	"lockd/internal/config"
	core "lockd/internal/lock"
	"lockd/internal/logger"
	"lockd/internal/metrics"
	"lockd/web"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}
	log := logger.New(os.Stdout, cfg.LogLevel)
	registry := core.NewRegistry(cfg.Namespaces, cfg.NamespaceQuota, cfg.DefaultTTL)
	metricSet := &metrics.Metrics{}
	service := core.NewService(registry, core.NewBus(), metricSet, cfg.ForceToken)
	rootFS, err := fs.Sub(web.Files(), ".")
	if err != nil {
		log.Error("web_assets", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	handler := api.New(service, log, rootFS).WithRequestTracking(cfg.TrackRequests).Handler()
	server := &http.Server{
		Addr:    cfg.Addr,
		Handler: handler,
		// FIX: HTTP header protection must not change with the lease scan interval.
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go service.RunExpirer(ctx, cfg.ExpireInterval)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown", map[string]any{"error": err.Error()})
		}
	}()
	log.Info("server_started", map[string]any{"addr": cfg.Addr})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server_failed", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	log.Info("server_stopped", nil)
}
