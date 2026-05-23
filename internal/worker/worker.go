// Package worker implements the QueueForge worker runtime.
//
// One worker process can subscribe to one or more priority topics. Inside
// the process, a single Kafka consumer feeds a bounded pool of goroutines
// that:
//
//  1. Claim the job in Postgres (visibility lease taken).
//  2. Start a heartbeat that extends the lease while the handler runs.
//  3. Invoke the registered handler.
//  4. On success → mark succeeded.
//     On error  → if attempts remain, schedule a future retry; otherwise
//                 dead-letter (Postgres state + DLQ topic record).
//  5. Commit the Kafka offset only after the DB transition is durable.
//
// The "commit-after-durability" ordering is what gives at-least-once
// semantics: if the worker dies before commit, the message will be
// redelivered, the Postgres state will say 'running' with a now-stale lease,
// and recovery will reclaim it for another attempt.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ayush-algosoft/queueforge/internal/handlers"
	"github.com/ayush-algosoft/queueforge/internal/job"
	"github.com/ayush-algosoft/queueforge/internal/kafka"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
	"github.com/ayush-algosoft/queueforge/internal/storage/redisrepo"
)

// Worker is the long-running process that drains priority topics.
type Worker struct {
	ID                string
	Consumer          *kafka.Consumer
	Producer          *kafka.Producer
	Repo              *postgres.Repo
	Redis             *redisrepo.Client
	Registry          *handlers.Registry
	Concurrency       int
	VisibilityTimeout time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	ShutdownGrace     time.Duration
	Logger            zerolog.Logger
}

// New builds a Worker with sensible defaults filled in.
func New(w Worker) *Worker {
	if w.ID == "" {
		w.ID = "worker-" + uuid.NewString()
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 8
	}
	if w.VisibilityTimeout <= 0 {
		w.VisibilityTimeout = 60 * time.Second
	}
	if w.HeartbeatInterval <= 0 {
		w.HeartbeatInterval = w.VisibilityTimeout / 3
	}
	if w.HandlerTimeout <= 0 {
		w.HandlerTimeout = 5 * time.Minute
	}
	if w.ShutdownGrace <= 0 {
		w.ShutdownGrace = 30 * time.Second
	}
	return &w
}

// Run polls Kafka and dispatches messages to a fixed-size worker pool.
// Returns when ctx is cancelled and all in-flight handlers finish (bounded
// by ShutdownGrace).
func (w *Worker) Run(ctx context.Context) error {
	w.Logger.Info().
		Str("worker_id", w.ID).
		Int("concurrency", w.Concurrency).
		Msg("worker starting")

	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup

	for {
		if ctx.Err() != nil {
			break
		}

		msgs, err := w.Consumer.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			w.Logger.Warn().Err(err).Msg("poll error; backing off")
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
			continue
		}

		for _, m := range msgs {
			m := m
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.handle(ctx, m)
			}()
		}

		// Allow the consumer group to rebalance once we've dispatched the
		// whole batch. We don't wait for in-flight handlers — they will
		// continue running and their offsets will commit when they finish.
		w.Consumer.AllowRebalance()
	}

	// Graceful drain: bounded by ShutdownGrace.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		w.Logger.Info().Msg("worker drained cleanly")
	case <-time.After(w.ShutdownGrace):
		w.Logger.Warn().Msg("worker shutdown grace exceeded")
	}
	return nil
}

// handle processes a single Kafka message end-to-end.
func (w *Worker) handle(parent context.Context, m kafka.Message) {
	env := m.Envelope
	log := w.Logger.With().
		Str("job_id", env.JobID).
		Str("queue", env.Queue).
		Str("job_type", env.JobType).
		Str("priority", string(env.Priority)).
		Logger()

	// Each DB call uses its own background-rooted context with a short
	// per-call timeout. We deliberately do NOT use one function-scoped
	// context: a long-running handler would let it expire before the
	// terminal Succeeded/MarkPendingRetry call.
	dbCall := func(timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), timeout)
	}

	claimCtx, claimCancel := dbCall(30 * time.Second)
	claim, err := w.Repo.Claim(claimCtx, env.JobID, w.ID, w.VisibilityTimeout)
	claimCancel()
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			// Job is missing, cancelled, terminal, or another worker won.
			// Commit the offset to avoid endless redelivery.
			log.Debug().Msg("nothing to claim; committing offset")
			w.commit(m.Record)
			return
		}
		log.Error().Err(err).Msg("claim failed")
		// Do not commit — let the message be redelivered.
		return
	}

	j := claim.Job

	// Heartbeat loop extends the lease periodically while the handler runs.
	hbCtx, stopHB := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				if err := w.Repo.ExtendLease(ctx, j.ID, w.ID, w.VisibilityTimeout); err != nil {
					log.Warn().Err(err).Msg("heartbeat extend failed")
				}
				c()
			}
		}
	}()

	handlerCtx, handlerCancel := context.WithTimeout(parent, w.HandlerTimeout)
	defer handlerCancel()

	h := w.Registry.Lookup(env.JobType)
	start := time.Now()
	var (
		result handlers.Result
		hErr   error
	)
	if h == nil {
		hErr = fmt.Errorf("no handler registered for jobType %q", env.JobType)
	} else {
		result, hErr = w.invokeWithRecover(handlerCtx, h, env)
	}
	stopHB()
	<-hbDone

	metrics.JobDuration.WithLabelValues(env.Queue, string(env.Priority)).Observe(time.Since(start).Seconds())

	if hErr == nil {
		ctx, cancel := dbCall(30 * time.Second)
		err := w.Repo.Succeeded(ctx, j.ID, w.ID, result.Output)
		cancel()
		if err != nil {
			log.Error().Err(err).Msg("mark succeeded failed; will retry via redelivery")
			return
		}
		metrics.JobsProcessed.WithLabelValues(env.Queue, string(env.Priority), "succeeded").Inc()
		w.dedupRelease(j)
		w.commit(m.Record)
		return
	}

	// Handler failed. Decide retry vs DLQ.
	log = log.With().Err(hErr).Logger()

	if j.Attempts < j.RetryPolicy.MaxAttempts {
		delay := job.NextRetryDelay(j.RetryPolicy, j.Attempts)
		runAt := time.Now().Add(delay)
		ctx, cancel := dbCall(30 * time.Second)
		err := w.Repo.MarkPendingRetry(ctx, j.ID, w.ID, runAt, hErr.Error())
		cancel()
		if err != nil {
			log.Error().Err(err).Msg("mark retry failed; will retry via redelivery")
			return
		}
		metrics.JobsProcessed.WithLabelValues(env.Queue, string(env.Priority), "retried").Inc()
		log.Info().Dur("retry_in", delay).Int("attempts", j.Attempts).Msg("scheduled retry")
		w.commit(m.Record)
		return
	}

	// Retries exhausted — dead-letter both in DB and on the DLQ topic.
	ctx, cancel := dbCall(30 * time.Second)
	dlErr := w.Repo.DeadLetter(ctx, j.ID, w.ID, hErr.Error())
	cancel()
	if dlErr != nil {
		log.Error().Err(dlErr).Msg("dead-letter mark failed; will retry via redelivery")
		return
	}
	pubCtx, pubCancel := dbCall(15 * time.Second)
	if err := w.Producer.PublishDLQ(pubCtx, j, hErr.Error()); err != nil {
		// DB already marked dead_lettered; DLQ topic is best-effort.
		log.Warn().Err(err).Msg("dlq publish failed; row still marked dead_lettered")
	}
	pubCancel()
	metrics.JobsProcessed.WithLabelValues(env.Queue, string(env.Priority), "dead_lettered").Inc()
	w.dedupRelease(j)
	w.commit(m.Record)
}

// invokeWithRecover protects the worker pool from a handler panic.
func (w *Worker) invokeWithRecover(ctx context.Context, h handlers.Handler, env kafka.Envelope) (res handlers.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, env)
}

// commit commits the Kafka offset for a single record. Failures are logged
// but not surfaced — uncommitted offsets simply mean the record is replayed
// after the worker restarts, which is safe.
func (w *Worker) commit(rec *kgo.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Consumer.CommitRecords(ctx, rec); err != nil {
		w.Logger.Warn().Err(err).Msg("commit offset failed")
	}
}

// dedupRelease drops the Redis dedup key once the job reaches a terminal
// state, so a later submission with the same key (after a TTL) is accepted.
// Best-effort — the index in Postgres only enforces non-terminal uniqueness.
func (w *Worker) dedupRelease(j *job.Job) {
	if j.DedupKey == nil || w.Redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Redis.DedupRelease(ctx, j.Queue, *j.DedupKey)
}
