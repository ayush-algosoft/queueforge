// Package handlers contains the demo job handlers the worker can execute.
//
// In a real deployment these would be implemented by the team that owns the
// jobType — typically as a separate binary linked against an SDK. We ship a
// small in-process registry so the system can be exercised end-to-end
// without any third-party dependencies.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ayush-algosoft/queueforge/internal/kafka"
)

// Result is returned by a handler on success. The bytes are persisted on the
// job row and surfaced via GET /v1/jobs/{id}.
type Result struct {
	Output json.RawMessage
}

// Handler executes a single job. Returning a non-nil error triggers retry
// logic; a nil error marks the job succeeded.
type Handler func(ctx context.Context, env kafka.Envelope) (Result, error)

// Registry maps jobType strings to handlers.
type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	r := &Registry{handlers: map[string]Handler{}}
	r.RegisterDefaults()
	return r
}

// Register installs a handler for the named jobType.
func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

// Lookup returns the handler for jobType, or nil if none is registered.
func (r *Registry) Lookup(jobType string) Handler {
	return r.handlers[jobType]
}

// RegisterDefaults wires up the small set of demo handlers shipped with
// QueueForge. They are deterministic enough to exercise the system but
// stochastic enough to demonstrate retries and the DLQ.
func (r *Registry) RegisterDefaults() {
	r.Register("noop", func(ctx context.Context, env kafka.Envelope) (Result, error) {
		return Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})

	r.Register("sleep", func(ctx context.Context, env kafka.Envelope) (Result, error) {
		var p struct {
			Milliseconds int `json:"ms"`
		}
		if len(env.Payload) > 0 {
			_ = json.Unmarshal(env.Payload, &p)
		}
		if p.Milliseconds <= 0 {
			p.Milliseconds = 50
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(time.Duration(p.Milliseconds) * time.Millisecond):
		}
		return Result{Output: json.RawMessage(fmt.Sprintf(`{"slept_ms":%d}`, p.Milliseconds))}, nil
	})

	// flaky fails with a configurable probability; useful to exercise retry
	// + DLQ paths from a load test without hand-crafting payloads.
	r.Register("flaky", func(ctx context.Context, env kafka.Envelope) (Result, error) {
		var p struct {
			FailRate float64 `json:"failRate"`
		}
		if len(env.Payload) > 0 {
			_ = json.Unmarshal(env.Payload, &p)
		}
		if p.FailRate <= 0 {
			p.FailRate = 0.3
		}
		if rand.Float64() < p.FailRate {
			return Result{}, errors.New("flaky handler: simulated transient error")
		}
		return Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})

	// always_fail terminates with a non-retryable-looking error. Combined
	// with a maxAttempts=2 retry policy this drives a job into the DLQ on
	// the second attempt — handy for smoke-testing the DLQ flow.
	r.Register("always_fail", func(ctx context.Context, env kafka.Envelope) (Result, error) {
		return Result{}, errors.New("always_fail handler: deliberate failure")
	})
}
