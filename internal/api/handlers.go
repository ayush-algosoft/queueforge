// Package api implements the HTTP surface for QueueForge: submission,
// inspection, cancellation, and queue statistics. Handlers are thin —
// validation, deduplication, and rate-limiting only; durable logic lives
// in the storage layer.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/ayush-algosoft/queueforge/internal/job"
	"github.com/ayush-algosoft/queueforge/internal/kafka"
	"github.com/ayush-algosoft/queueforge/internal/metrics"
	"github.com/ayush-algosoft/queueforge/internal/storage/postgres"
	"github.com/ayush-algosoft/queueforge/internal/storage/redisrepo"
)

// Server bundles the dependencies handlers need.
type Server struct {
	Repo            *postgres.Repo
	Redis           *redisrepo.Client
	Producer        *kafka.Producer
	Logger          zerolog.Logger
	DedupTTL        time.Duration
	GlobalRateLimit int64         // 0 disables; otherwise N per window
	RateWindow      time.Duration // window for global rate limit
}

// Router builds the chi router with all routes mounted.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(s.recoverer)
	r.Use(s.requestLogger)

	r.Get("/health", s.health)
	r.Get("/readyz", s.ready)
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
	})

	r.Route("/v1", func(r chi.Router) {
		r.Post("/jobs", s.submitJob)
		r.Get("/jobs", s.listJobs)
		r.Get("/jobs/{id}", s.getJob)
		r.Post("/jobs/{id}/cancel", s.cancelJob)
		r.Get("/queues/stats", s.queueStats)
	})

	return r
}

// SubmitRequest is the public shape accepted by POST /v1/jobs.
type SubmitRequest struct {
	Queue            string          `json:"queue"`
	JobType          string          `json:"jobType"`
	Priority         job.Priority    `json:"priority"`
	Payload          json.RawMessage `json:"payload"`
	DelaySeconds     int             `json:"delaySeconds,omitempty"`
	RunAt            *time.Time      `json:"runAt,omitempty"`
	DedupKey         string          `json:"deduplicationKey,omitempty"`
	DedupMode        job.DedupMode   `json:"deduplicationMode,omitempty"`
	RetryPolicy      *job.RetryPolicy `json:"retryPolicy,omitempty"`
}

type submitResponse struct {
	JobID  string `json:"jobId"`
	State  string `json:"state"`
	RunAt  string `json:"runAt"`
	Existing bool `json:"existing,omitempty"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.Queue == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", job.ErrInvalidQueue.Error())
		return
	}
	if req.JobType == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", job.ErrInvalidJobType.Error())
		return
	}
	if req.Priority == "" {
		req.Priority = job.PriorityP2
	}
	if !job.ValidPriority(req.Priority) {
		writeError(w, http.StatusBadRequest, "invalid_request", job.ErrInvalidPriority.Error())
		return
	}
	if req.DedupMode == "" {
		req.DedupMode = job.DedupReject
	}

	// Global API-level rate limit. Per-queue / per-tenant limits could be
	// added the same way using a key like "queue:"+req.Queue.
	if s.GlobalRateLimit > 0 {
		allowed, retry, err := s.Redis.RateLimit(ctx, "api:global", s.GlobalRateLimit, s.rateWindow())
		if err == nil && !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds())+1))
			metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "rate_limited").Inc()
			writeError(w, http.StatusTooManyRequests, "rate_limited",
				fmt.Sprintf("api rate limit exceeded; retry after %s", retry))
			return
		}
	}

	// Pre-check dedup against Redis. Authoritative check happens on the
	// Postgres unique index — Redis is only a fast path.
	if req.DedupKey != "" {
		ok, err := s.Redis.DedupReserve(ctx, req.Queue, req.DedupKey, s.dedupTTL())
		if err != nil {
			s.Logger.Warn().Err(err).Msg("redis dedup unavailable; falling back to postgres")
		}
		if err == nil && !ok {
			// Already reserved. Behaviour depends on the caller's mode.
			if req.DedupMode == job.DedupReturnExisting {
				existing, findErr := s.Repo.FindActiveByDedupKey(ctx, req.Queue, req.DedupKey)
				if findErr == nil {
					metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "deduplicated").Inc()
					writeJSON(w, http.StatusOK, submitResponse{
						JobID:    existing.ID,
						State:    string(existing.State),
						RunAt:    existing.RunAt.Format(time.RFC3339),
						Existing: true,
					})
					return
				}
				// Redis says duplicate but Postgres can't find it — most
				// likely a stale Redis key from a job that already completed.
				// Fall through to attempt the insert; if Postgres also
				// rejects, we'll return a clean 409 below.
			} else {
				metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "deduplicated").Inc()
				writeError(w, http.StatusConflict, "duplicate",
					"a job with this deduplication key is already in flight")
				return
			}
		}
	}

	runAt := time.Now()
	if req.RunAt != nil {
		runAt = *req.RunAt
	} else if req.DelaySeconds > 0 {
		runAt = runAt.Add(time.Duration(req.DelaySeconds) * time.Second)
	}

	retry := job.RetryPolicy{
		MaxAttempts:  3,
		Backoff:      job.BackoffExponential,
		InitialDelay: job.Duration(30 * time.Second),
		MaxDelay:     job.Duration(10 * time.Minute),
	}
	if req.RetryPolicy != nil {
		retry = *req.RetryPolicy
	}

	j := &job.Job{
		Queue:       req.Queue,
		JobType:     req.JobType,
		Priority:    req.Priority,
		State:       job.StatePending,
		Payload:     req.Payload,
		DedupMode:   req.DedupMode,
		RetryPolicy: retry,
		RunAt:       runAt,
	}
	if req.DedupKey != "" {
		j.DedupKey = &req.DedupKey
	}

	if err := s.Repo.Insert(ctx, j); err != nil {
		if errors.Is(err, postgres.ErrDuplicate) {
			// Race with another submitter — Postgres unique index caught it.
			if req.DedupMode == job.DedupReturnExisting {
				if existing, findErr := s.Repo.FindActiveByDedupKey(ctx, req.Queue, req.DedupKey); findErr == nil {
					metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "deduplicated").Inc()
					writeJSON(w, http.StatusOK, submitResponse{
						JobID:    existing.ID,
						State:    string(existing.State),
						RunAt:    existing.RunAt.Format(time.RFC3339),
						Existing: true,
					})
					return
				}
			}
			metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "deduplicated").Inc()
			writeError(w, http.StatusConflict, "duplicate",
				"a job with this deduplication key already exists")
			return
		}
		s.Logger.Error().Err(err).Msg("insert job failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "could not persist job")
		return
	}

	// Immediate jobs go straight to Kafka so workers don't wait on a
	// scheduler tick. Delayed jobs stay 'pending' until the scheduler picks
	// them up at runAt.
	if !runAt.After(time.Now()) {
		if err := s.Producer.PublishJob(ctx, j); err != nil {
			// We've already inserted; leave it in 'pending' and let the
			// scheduler retry the publish on its next pass.
			s.Logger.Warn().Err(err).Str("job_id", j.ID).Msg("publish failed; scheduler will retry")
		} else {
			if err := s.Repo.MarkQueued(ctx, j.ID); err != nil {
				s.Logger.Warn().Err(err).Str("job_id", j.ID).Msg("mark queued failed")
			} else {
				j.State = job.StateQueued
			}
		}
	}

	metrics.JobsSubmitted.WithLabelValues(req.Queue, string(req.Priority), "accepted").Inc()
	writeJSON(w, http.StatusAccepted, submitResponse{
		JobID: j.ID,
		State: string(j.State),
		RunAt: j.RunAt.Format(time.RFC3339),
	})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, err := s.Repo.Get(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		s.Logger.Error().Err(err).Msg("get job failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	state := r.URL.Query().Get("state")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.Repo.List(r.Context(), queue, state, limit)
	if err != nil {
		s.Logger.Error().Err(err).Msg("list jobs failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "list failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "count": len(jobs)})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.Repo.Cancel(r.Context(), id); err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			writeError(w, http.StatusConflict, "not_cancellable",
				"job is missing or already past pending/queued")
			return
		}
		s.Logger.Error().Err(err).Msg("cancel job failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "cancel failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.Repo.Stats(r.Context())
	if err != nil {
		s.Logger.Error().Err(err).Msg("queue stats failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "stats failed")
		return
	}
	ages, _ := s.Repo.OldestPendingAges(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"states":       stats,
		"oldestPending": ages,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.Repo.Pool().Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "db_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) dedupTTL() time.Duration {
	if s.DedupTTL > 0 {
		return s.DedupTTL
	}
	return 24 * time.Hour
}

func (s *Server) rateWindow() time.Duration {
	if s.RateWindow > 0 {
		return s.RateWindow
	}
	return time.Second
}

// --- helpers ---------------------------------------------------------------

type errorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Detail  string `json:"detail,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{
		Error:  http.StatusText(status),
		Code:   code,
		Detail: detail,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
