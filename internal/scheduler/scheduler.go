// Package scheduler promotes due jobs from Postgres into Kafka.
//
// Two responsibilities:
//
//  1. Initial promotion: jobs that the API created with delaySeconds/runAt
//     in the future. They sit in state='pending' with a future run_at; the
//     scheduler scans, finds those whose run_at has elapsed, publishes them
//     to the appropriate Kafka priority topic, and transitions them to
//     state='queued'.
//
//  2. Retry promotion: when a worker fails a job and the retry policy still
//     has attempts remaining, the worker writes the job back to 'pending'
//     with run_at = now + backoff. The same scan loop handles those.
//
// Multiple scheduler replicas can run concurrently — FetchDue uses
// SELECT … FOR UPDATE SKIP LOCKED so each replica claims a disjoint batch.
package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/ayush-algosoft/queueforge/internal/kafka"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
)

// Scheduler is the long-running service that drives promotion.
type Scheduler struct {
	Repo         *postgres.Repo
	Producer     *kafka.Producer
	PollInterval time.Duration
	BatchSize    int
	Logger       zerolog.Logger
}

// Run blocks until ctx is cancelled, scanning every PollInterval. If a scan
// returns a full batch, the next scan runs immediately to drain any backlog.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.PollInterval <= 0 {
		s.PollInterval = time.Second
	}
	if s.BatchSize <= 0 {
		s.BatchSize = 200
	}

	s.Logger.Info().
		Dur("poll", s.PollInterval).
		Int("batch", s.BatchSize).
		Msg("scheduler running")

	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	for {
		// Drain in a tight loop so a large backlog promoted at once doesn't
		// have to wait through many poll intervals.
		for {
			n, err := s.tick(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				s.Logger.Error().Err(err).Msg("scheduler tick failed")
				break
			}
			if n < s.BatchSize {
				break
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// tick fetches one batch of due jobs and publishes them. Returns the number
// of jobs processed (whether published or left for the next pass).
func (s *Scheduler) tick(ctx context.Context) (int, error) {
	jobs, err := s.Repo.FetchDue(ctx, s.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	for _, j := range jobs {
		if err := s.Producer.PublishJob(ctx, j); err != nil {
			// Leave the job in 'pending' — next scan will pick it up.
			s.Logger.Warn().Err(err).Str("job_id", j.ID).Msg("publish failed; retrying next tick")
			continue
		}
		if err := s.Repo.MarkQueued(ctx, j.ID); err != nil {
			// Already published but DB update failed. Worker may see the
			// message and try to claim — claim will fail (state still pending),
			// it will be redelivered and the second time around the state will
			// already be queued or the next tick will fix it. At-least-once.
			s.Logger.Warn().Err(err).Str("job_id", j.ID).Msg("mark queued failed")
			continue
		}
		metrics.SchedulerPromoted.WithLabelValues(string(j.Priority)).Inc()
	}
	return len(jobs), nil
}
