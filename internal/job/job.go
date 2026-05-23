// Package job defines the core domain model for QueueForge jobs.
//
// A Job moves through a finite state machine. Persistent state lives in
// PostgreSQL; the executable handoff between services flows through Kafka.
// The model in this file is the canonical representation shared by every
// service (api, scheduler, worker, recovery).
package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// State represents a position in the job lifecycle.
type State string

const (
	// StatePending: created, awaiting promotion to a Kafka topic.
	// Either it is delayed (run_at in the future) or it is queued for
	// immediate execution and the scheduler/API just produced it.
	StatePending State = "pending"

	// StateQueued: published to a Kafka topic, not yet claimed by a worker.
	StateQueued State = "queued"

	// StateRunning: a worker has claimed the job and holds a visibility lease.
	StateRunning State = "running"

	// StateSucceeded: terminal success.
	StateSucceeded State = "succeeded"

	// StateFailed: terminal failure after retries exhausted (also goes to DLQ).
	StateFailed State = "failed"

	// StateDeadLettered: moved into the DLQ topic; awaiting manual replay.
	StateDeadLettered State = "dead_lettered"

	// StateCancelled: explicitly cancelled before completion.
	StateCancelled State = "cancelled"
)

// Priority is a coarse classification mapped to dedicated Kafka topics.
// Higher priorities preempt lower ones at the worker scheduling layer.
type Priority string

const (
	PriorityP0 Priority = "P0" // critical: payments, fraud
	PriorityP1 Priority = "P1" // user-facing: notifications, webhooks
	PriorityP2 Priority = "P2" // background: sync, reports
	PriorityP3 Priority = "P3" // low: cleanup, analytics
)

// BackoffStrategy controls how retry delays grow.
type BackoffStrategy string

const (
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

// DedupMode controls how the API resolves a duplicate dedup key collision.
type DedupMode string

const (
	DedupReject        DedupMode = "reject"          // 409, refuse new job
	DedupReturnExisting DedupMode = "return_existing" // 200, hand back existing id
)

// RetryPolicy describes a job's retry behaviour. Zero-value MaxAttempts means
// the job is single-shot.
type RetryPolicy struct {
	MaxAttempts   int             `json:"maxAttempts"`
	Backoff       BackoffStrategy `json:"backoff"`
	InitialDelay  Duration        `json:"initialDelay"`
	MaxDelay      Duration        `json:"maxDelay"`
}

// Duration is a JSON-friendly time.Duration that accepts Go duration strings
// (e.g. "30s", "2m") on the wire.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		// also accept raw nanoseconds
		var n int64
		if err2 := json.Unmarshal(b, &n); err2 == nil {
			*d = Duration(n)
			return nil
		}
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// Job is the persistent representation of work in flight.
type Job struct {
	ID              string          `json:"id"`
	Queue           string          `json:"queue"`
	JobType         string          `json:"jobType"`
	Priority        Priority        `json:"priority"`
	State           State           `json:"state"`
	Payload         json.RawMessage `json:"payload"`
	DedupKey        *string         `json:"deduplicationKey,omitempty"`
	DedupMode       DedupMode       `json:"deduplicationMode,omitempty"`
	RetryPolicy     RetryPolicy     `json:"retryPolicy"`
	Attempts        int             `json:"attempts"`
	LastError       *string         `json:"lastError,omitempty"`
	RunAt           time.Time       `json:"runAt"`
	VisibilityUntil *time.Time      `json:"visibilityUntil,omitempty"`
	ClaimedBy       *string         `json:"claimedBy,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	CompletedAt     *time.Time      `json:"completedAt,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
}

// Validation errors surfaced as user input problems.
var (
	ErrInvalidQueue    = errors.New("queue is required")
	ErrInvalidJobType  = errors.New("jobType is required")
	ErrInvalidPriority = errors.New("priority must be one of P0,P1,P2,P3")
	ErrInvalidPayload  = errors.New("payload must be valid JSON")
	ErrInvalidRetry    = errors.New("retry policy is invalid")
)

// ValidPriority reports whether p is a defined priority bucket.
func ValidPriority(p Priority) bool {
	switch p {
	case PriorityP0, PriorityP1, PriorityP2, PriorityP3:
		return true
	}
	return false
}

// NextRetryDelay computes the delay before attempt n+1 for the given policy.
// n is the number of attempts that have already failed (1-based after the
// first failure). The result is clamped to the policy's MaxDelay when set.
func NextRetryDelay(p RetryPolicy, attempt int) time.Duration {
	initial := p.InitialDelay.Std()
	if initial <= 0 {
		initial = 5 * time.Second
	}
	var d time.Duration
	switch p.Backoff {
	case BackoffExponential:
		// 2^(attempt-1) * initial
		shift := attempt - 1
		if shift < 0 {
			shift = 0
		}
		if shift > 16 { // guard against overflow
			shift = 16
		}
		d = initial << shift
	default:
		d = initial
	}
	if max := p.MaxDelay.Std(); max > 0 && d > max {
		d = max
	}
	return d
}

// TerminalState reports whether the state has no outgoing transitions.
func TerminalState(s State) bool {
	switch s {
	case StateSucceeded, StateFailed, StateDeadLettered, StateCancelled:
		return true
	}
	return false
}
