package job

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextRetryDelay_Fixed(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:  5,
		Backoff:      BackoffFixed,
		InitialDelay: Duration(30 * time.Second),
	}
	for attempt := 1; attempt <= 4; attempt++ {
		assert.Equal(t, 30*time.Second, NextRetryDelay(p, attempt),
			"fixed backoff should ignore the attempt number")
	}
}

func TestNextRetryDelay_Exponential(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:  6,
		Backoff:      BackoffExponential,
		InitialDelay: Duration(time.Second),
	}
	assert.Equal(t, 1*time.Second, NextRetryDelay(p, 1))
	assert.Equal(t, 2*time.Second, NextRetryDelay(p, 2))
	assert.Equal(t, 4*time.Second, NextRetryDelay(p, 3))
	assert.Equal(t, 8*time.Second, NextRetryDelay(p, 4))
}

func TestNextRetryDelay_ClampsToMax(t *testing.T) {
	p := RetryPolicy{
		Backoff:      BackoffExponential,
		InitialDelay: Duration(time.Second),
		MaxDelay:     Duration(5 * time.Second),
	}
	// Attempt 10 would be 512s without clamp.
	assert.Equal(t, 5*time.Second, NextRetryDelay(p, 10))
}

func TestNextRetryDelay_DefaultsWhenZero(t *testing.T) {
	// Zero policy must still return a non-zero, finite delay.
	d := NextRetryDelay(RetryPolicy{}, 1)
	assert.Greater(t, d, time.Duration(0))
}

func TestValidPriority(t *testing.T) {
	for _, p := range []Priority{PriorityP0, PriorityP1, PriorityP2, PriorityP3} {
		assert.True(t, ValidPriority(p))
	}
	assert.False(t, ValidPriority(""))
	assert.False(t, ValidPriority("P9"))
	assert.False(t, ValidPriority("p1"))
}

func TestTerminalState(t *testing.T) {
	for _, s := range []State{StateSucceeded, StateFailed, StateDeadLettered, StateCancelled} {
		assert.True(t, TerminalState(s), "expected %q to be terminal", s)
	}
	for _, s := range []State{StatePending, StateQueued, StateRunning} {
		assert.False(t, TerminalState(s), "expected %q to be non-terminal", s)
	}
}

func TestDuration_JSONRoundTrip(t *testing.T) {
	type wrap struct {
		D Duration `json:"d"`
	}
	cases := []struct {
		in   string
		want time.Duration
	}{
		{`{"d":"30s"}`, 30 * time.Second},
		{`{"d":"2m"}`, 2 * time.Minute},
		{`{"d":1500000000}`, 1500 * time.Millisecond}, // raw nanoseconds
	}
	for _, c := range cases {
		var w wrap
		require.NoError(t, json.Unmarshal([]byte(c.in), &w))
		assert.Equal(t, c.want, w.D.Std(), "input: %s", c.in)
	}

	// Marshal emits the human-readable form.
	w := wrap{D: Duration(45 * time.Second)}
	b, err := json.Marshal(w)
	require.NoError(t, err)
	assert.JSONEq(t, `{"d":"45s"}`, string(b))
}

func TestDuration_RejectsGarbage(t *testing.T) {
	var d Duration
	err := json.Unmarshal([]byte(`"not-a-duration"`), &d)
	assert.Error(t, err)
}
