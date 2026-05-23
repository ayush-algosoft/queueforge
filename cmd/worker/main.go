// worker consumes priority topics and executes job handlers.
package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/ayush-algosoft/queueforge/internal/config"
	"github.com/ayush-algosoft/queueforge/internal/handlers"
	"github.com/ayush-algosoft/queueforge/internal/kafka"
	"github.com/ayush-algosoft/queueforge/internal/logging"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
	"github.com/ayush-algosoft/queueforge/internal/storage/redisrepo"
	"github.com/ayush-algosoft/queueforge/internal/worker"
)

func main() {
	cfg, err := config.Load("worker")
	if err != nil {
		panic(err)
	}
	logger := logging.New("worker", cfg.LogLevel)
	logger.Info().Str("env", cfg.Env).Msg("starting worker")

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

	rdb, err := redisrepo.New(rootCtx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		logger.Fatal().Err(err).Msg("connect redis")
	}
	defer rdb.Close()

	topology := kafka.Topology{Prefix: cfg.Kafka.TopicPrefix}
	topics := topology.PrioritiesToTopics(cfg.Worker.Priorities)
	if len(topics) == 0 {
		logger.Fatal().Msg("no valid priority topics configured")
	}
	logger.Info().Strs("topics", topics).Msg("subscribing")

	consumer, err := kafka.NewConsumer(kafka.ConsumerOptions{
		Brokers:  cfg.Kafka.Brokers,
		ClientID: cfg.Kafka.ClientID,
		GroupID:  "queueforge.worker",
		Topics:   topics,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("kafka consumer")
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ClientID+".producer", topology)
	if err != nil {
		logger.Fatal().Err(err).Msg("kafka producer")
	}
	defer producer.Close(context.Background())

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

	w := worker.New(worker.Worker{
		ID:                "worker-" + uuid.NewString()[:8],
		Consumer:          consumer,
		Producer:          producer,
		Repo:              repo,
		Redis:             rdb,
		Registry:          handlers.NewRegistry(),
		Concurrency:       cfg.Worker.Concurrency,
		VisibilityTimeout: cfg.Worker.VisibilityTimeout,
		HeartbeatInterval: cfg.Worker.HeartbeatInterval,
		HandlerTimeout:    cfg.Worker.HandlerTimeout,
		ShutdownGrace:     cfg.Worker.ShutdownGrace,
		Logger:            logger,
	})

	if err := w.Run(rootCtx); err != nil {
		logger.Error().Err(err).Msg("worker exited with error")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = metricsSrv.Shutdown(shutdownCtx)
	logger.Info().Msg("worker stopped")
}
