// recovery scans Postgres for jobs with expired visibility leases and
// returns them to the pending pool so another worker can pick them up.
package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayush-algosoft/queueforge/internal/config"
	"github.com/ayush-algosoft/queueforge/internal/logging"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/recovery"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
)

func main() {
	cfg, err := config.Load("recovery")
	if err != nil {
		panic(err)
	}
	logger := logging.New("recovery", cfg.LogLevel)
	logger.Info().Str("env", cfg.Env).Msg("starting recovery")

	rootCtx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	repo, err := postgres.New(rootCtx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		logger.Fatal().Err(err).Msg("connect postgres")
	}
	defer repo.Close()

	if cfg.Postgres.MigrateOnStart {
		if err := repo.Migrate(rootCtx); err != nil {
			logger.Fatal().Err(err).Msg("apply migrations")
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info().Str("addr", cfg.MetricsAddr).Msg("metrics listening")
		_ = metricsSrv.ListenAndServe()
	}()

	r := &recovery.Recovery{
		Repo:         repo,
		ScanInterval: cfg.Recovery.ScanInterval,
		BatchSize:    cfg.Recovery.BatchSize,
		Logger:       logger,
	}
	if err := r.Run(rootCtx); err != nil {
		logger.Error().Err(err).Msg("recovery exited with error")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = metricsSrv.Shutdown(shutdownCtx)
	logger.Info().Msg("recovery stopped")
}
