package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/queueforge/queueforge/internal/kafka"
)

func TestRegistry_Defaults(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"noop", "sleep", "flaky", "always_fail"} {
		assert.NotNil(t, r.Lookup(name), "expected default handler %q", name)
	}
	assert.Nil(t, r.Lookup("missing"))
}

func TestNoopHandler_Succeeds(t *testing.T) {
	r := NewRegistry()
	h := r.Lookup("noop")
	require.NotNil(t, h)
	res, err := h(context.Background(), kafka.Envelope{JobType: "noop"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(res.Output))
}

func TestAlwaysFailHandler_Errors(t *testing.T) {
	r := NewRegistry()
	h := r.Lookup("always_fail")
	require.NotNil(t, h)
	_, err := h(context.Background(), kafka.Envelope{JobType: "always_fail"})
	assert.Error(t, err)
}

func TestSleepHandler_HonoursCancellation(t *testing.T) {
	r := NewRegistry()
	h := r.Lookup("sleep")
	require.NotNil(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Even with ms=0 (defaulted to 50), the immediate cancellation should
	// be observed.
	_, err := h(ctx, kafka.Envelope{
		JobType: "sleep",
		Payload: json.RawMessage(`{"ms":5000}`),
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRegistry_RegisterOverrides(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("custom", func(ctx context.Context, env kafka.Envelope) (Result, error) {
		called = true
		return Result{}, nil
	})
	h := r.Lookup("custom")
	require.NotNil(t, h)
	_, _ = h(context.Background(), kafka.Envelope{})
	assert.True(t, called)
}
