// Package config centralises environment-driven configuration for every
// QueueForge binary. Each service reads only the sections it needs but the
// shape is shared so docker-compose can pass a single set of env vars.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env           string // development | production
	ServiceName   string // api | scheduler | worker | recovery
	HTTPAddr      string
	MetricsAddr   string
	LogLevel      string

	Postgres PostgresConfig
	Redis    RedisConfig
	Kafka    KafkaConfig

	Scheduler SchedulerConfig
	Worker    WorkerConfig
	Recovery  RecoveryConfig
}

type PostgresConfig struct {
	DSN            string
	MaxConns       int32
	MigrateOnStart bool
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type KafkaConfig struct {
	Brokers     []string
	ClientID    string
	TopicPrefix string // e.g. "qf" -> qf.jobs.p0, qf.jobs.retry, qf.jobs.dlq
}

type SchedulerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

type WorkerConfig struct {
	Concurrency        int
	Priorities         []string      // which priority topics this worker consumes, e.g. ["P0","P1"]
	VisibilityTimeout  time.Duration // lease length
	HeartbeatInterval  time.Duration
	HandlerTimeout     time.Duration
	ShutdownGrace      time.Duration
}

type RecoveryConfig struct {
	ScanInterval time.Duration
	BatchSize    int
}

// Load reads configuration for a named service from the environment.
func Load(service string) (*Config, error) {
	cfg := &Config{
		Env:         getenv("QF_ENV", "development"),
		ServiceName: service,
		HTTPAddr:    getenv("QF_HTTP_ADDR", ":8080"),
		MetricsAddr: getenv("QF_METRICS_ADDR", ":9090"),
		LogLevel:    getenv("QF_LOG_LEVEL", "info"),
		Postgres: PostgresConfig{
			DSN:            getenv("QF_POSTGRES_DSN", "postgres://queueforge:queueforge@localhost:5432/queueforge?sslmode=disable"),
			MaxConns:       int32(getenvInt("QF_POSTGRES_MAX_CONNS", 10)),
			MigrateOnStart: getenvBool("QF_POSTGRES_MIGRATE", true),
		},
		Redis: RedisConfig{
			Addr:     getenv("QF_REDIS_ADDR", "localhost:6379"),
			Password: getenv("QF_REDIS_PASSWORD", ""),
			DB:       getenvInt("QF_REDIS_DB", 0),
		},
		Kafka: KafkaConfig{
			Brokers:     strings.Split(getenv("QF_KAFKA_BROKERS", "localhost:9092"), ","),
			ClientID:    getenv("QF_KAFKA_CLIENT_ID", "queueforge-"+service),
			TopicPrefix: getenv("QF_KAFKA_TOPIC_PREFIX", "qf"),
		},
		Scheduler: SchedulerConfig{
			PollInterval: getenvDuration("QF_SCHEDULER_POLL_INTERVAL", time.Second),
			BatchSize:    getenvInt("QF_SCHEDULER_BATCH_SIZE", 200),
		},
		Worker: WorkerConfig{
			Concurrency:       getenvInt("QF_WORKER_CONCURRENCY", 8),
			Priorities:        strings.Split(getenv("QF_WORKER_PRIORITIES", "P0,P1,P2,P3"), ","),
			VisibilityTimeout: getenvDuration("QF_WORKER_VISIBILITY_TIMEOUT", 60*time.Second),
			HeartbeatInterval: getenvDuration("QF_WORKER_HEARTBEAT_INTERVAL", 15*time.Second),
			HandlerTimeout:    getenvDuration("QF_WORKER_HANDLER_TIMEOUT", 5*time.Minute),
			ShutdownGrace:     getenvDuration("QF_WORKER_SHUTDOWN_GRACE", 30*time.Second),
		},
		Recovery: RecoveryConfig{
			ScanInterval: getenvDuration("QF_RECOVERY_SCAN_INTERVAL", 10*time.Second),
			BatchSize:    getenvInt("QF_RECOVERY_BATCH_SIZE", 200),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.Postgres.DSN == "" {
		return errors.New("QF_POSTGRES_DSN is required")
	}
	if len(c.Kafka.Brokers) == 0 || c.Kafka.Brokers[0] == "" {
		return errors.New("QF_KAFKA_BROKERS is required")
	}
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("invalid int for %s: %v", key, err))
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("invalid bool for %s: %v", key, err))
	}
	return b
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("invalid duration for %s: %v", key, err))
	}
	return d
}
