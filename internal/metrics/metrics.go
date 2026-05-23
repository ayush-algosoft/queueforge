// Package metrics owns the Prometheus collectors shared across services.
//
// We register a single, process-wide set of collectors so metric names are
// stable regardless of which binary emits them, with a "service" label
// distinguishing the source on the dashboard side.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Registry = prometheus.NewRegistry()

	JobsSubmitted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "queueforge_jobs_submitted_total",
		Help: "Jobs accepted by the API, by queue and priority.",
	}, []string{"queue", "priority", "outcome"}) // outcome: accepted|deduplicated|rate_limited|rejected

	JobsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "queueforge_jobs_processed_total",
		Help: "Jobs completed by workers, by queue/priority/outcome.",
	}, []string{"queue", "priority", "outcome"}) // outcome: succeeded|retried|dead_lettered|failed

	JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "queueforge_job_duration_seconds",
		Help:    "Worker handler execution duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	}, []string{"queue", "priority"})

	SchedulerPromoted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "queueforge_scheduler_promoted_total",
		Help: "Scheduled jobs promoted from Postgres into Kafka.",
	}, []string{"priority"})

	RecoveryReclaimed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queueforge_recovery_reclaimed_total",
		Help: "Jobs whose stale visibility lease was reclaimed.",
	})

	QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueforge_queue_depth",
		Help: "Approximate number of jobs in a given non-terminal state.",
	}, []string{"queue", "priority", "state"})

	OldestPendingSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueforge_oldest_pending_age_seconds",
		Help: "Age in seconds of the oldest pending job per priority.",
	}, []string{"priority"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		JobsSubmitted,
		JobsProcessed,
		JobDuration,
		SchedulerPromoted,
		RecoveryReclaimed,
		QueueDepth,
		OldestPendingSeconds,
	)
}

// Handler returns the http.Handler that exposes /metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}
