package main

import (
	"context"
	"errors"
	"io"
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
	var logOutput io.Writer = os.Stdout
	if cfg.DisableLogging {
		// DisabledWriter yields a safe no-op sink; the logger and all log
		// call sites remain safe even when logging is disabled, so health
		// checks and request handling are unaffected.
		logOutput = logger.DisabledWriter()
	}
	log := logger.New(logOutput, cfg.LogLevel)
	registry := core.NewRegistry(cfg.Namespaces, cfg.NamespaceQuota, cfg.DefaultTTL)
	metricSet := &metrics.Metrics{}
	service := core.NewService(registry, core.NewBus(), metricSet, cfg.ForceToken)
	rootFS, err := fs.Sub(web.Files(), ".")
	if err != nil {
		log.Error("web_assets", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	handler := api.New(service, log, rootFS).Handler()
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
