// scheduler is the long-running service that promotes due jobs from
// Postgres into Kafka.
package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/ayush-algosoft/queueforge/internal/config"
	"github.com/ayush-algosoft/queueforge/internal/kafka"
	"github.com/ayush-algosoft/queueforge/internal/logging"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/scheduler"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
)

func main() {
	cfg, err := config.Load("scheduler")
	if err != nil {
		panic(err)
	}
	logger := logging.New("scheduler", cfg.LogLevel)
	logger.Info().Str("env", cfg.Env).Msg("starting scheduler")

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

	topology := kafka.Topology{Prefix: cfg.Kafka.TopicPrefix}
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ClientID, topology)
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

	go publishGauges(rootCtx, repo, logger)

	sch := &scheduler.Scheduler{
		Repo:         repo,
		Producer:     producer,
		PollInterval: cfg.Scheduler.PollInterval,
		BatchSize:    cfg.Scheduler.BatchSize,
		Logger:       logger,
	}
	if err := sch.Run(rootCtx); err != nil {
		logger.Error().Err(err).Msg("scheduler exited with error")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = metricsSrv.Shutdown(shutdownCtx)
	logger.Info().Msg("scheduler stopped")
}

// publishGauges keeps queue-depth and oldest-pending-age Prometheus gauges
// current. The aggregate queries are cheap (indexed group-bys); a five-second
// cadence is fine for monitoring at any plausible scale.
func publishGauges(ctx context.Context, repo *postgres.Repo, logger zerolog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			stats, err := repo.Stats(ctx)
			if err != nil {
				logger.Warn().Err(err).Msg("stats query failed")
				continue
			}
			for _, s := range stats {
				metrics.QueueDepth.WithLabelValues(s.Queue, s.Priority, s.State).Set(float64(s.Count))
			}
			ages, err := repo.OldestPendingAges(ctx)
			if err != nil {
				continue
			}
			for p, age := range ages {
				metrics.OldestPendingSeconds.WithLabelValues(p).Set(age)
			}
		}
	}
}
