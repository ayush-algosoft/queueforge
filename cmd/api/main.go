// api is the HTTP job-submission service. It is the only public entry point
// into QueueForge; clients POST jobs here and the rest of the system flows
// from Postgres + Kafka.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/queueforge/queueforge/internal/api"
	"github.com/queueforge/queueforge/internal/config"
	"github.com/queueforge/queueforge/internal/kafka"
	"github.com/queueforge/queueforge/internal/logging"
	"github.com/queueforge/queueforge/internal/storage/postgres"
	"github.com/queueforge/queueforge/internal/storage/redisrepo"
)

func main() {
	cfg, err := config.Load("api")
	if err != nil {
		panic(err)
	}
	logger := logging.New("api", cfg.LogLevel)
	logger.Info().Str("env", cfg.Env).Msg("starting api")

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
		logger.Info().Msg("migrations applied")
	}

	rdb, err := redisrepo.New(rootCtx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		logger.Fatal().Err(err).Msg("connect redis")
	}
	defer rdb.Close()

	topology := kafka.Topology{Prefix: cfg.Kafka.TopicPrefix}
	if err := kafka.EnsureTopics(rootCtx, cfg.Kafka.Brokers, cfg.Kafka.ClientID, topology, 3, 1); err != nil {
		// Topic creation failures are not always fatal — in some environments
		// topics are pre-created by an admin. Log and continue; producing
		// will surface a real error if topics genuinely don't exist.
		logger.Warn().Err(err).Msg("ensure topics")
	}

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ClientID, topology)
	if err != nil {
		logger.Fatal().Err(err).Msg("kafka producer")
	}
	defer producer.Close(context.Background())

	srv := &api.Server{
		Repo:            repo,
		Redis:           rdb,
		Producer:        producer,
		Logger:          logger,
		DedupTTL:        24 * time.Hour,
		GlobalRateLimit: 0, // disabled by default; raise via wrapper if needed
	}

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", cfg.HTTPAddr).Msg("http listening")
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-rootCtx.Done():
		logger.Info().Msg("shutdown signal received")
	case err := <-serverErr:
		logger.Error().Err(err).Msg("http server failed")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn().Err(err).Msg("http shutdown")
	}
	os.Exit(0)
}
