// Integration tests against a running docker-compose stack.
//
// Skipped by default. Run with:
//
//	QF_INTEGRATION=1 go test ./tests/integration -count=1 -v
//
// Assumes `docker compose up -d` is already running locally (or in CI).
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const apiBase = "http://localhost:8080"

func skipIfNotIntegration(t *testing.T) {
	if os.Getenv("QF_INTEGRATION") != "1" {
		t.Skip("set QF_INTEGRATION=1 to run integration tests")
	}
}

func waitFor(t *testing.T, fn func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestSubmitAndComplete: a noop job should complete to 'succeeded'.
func TestSubmitAndComplete(t *testing.T) {
	skipIfNotIntegration(t)

	id := submit(t, map[string]any{
		"queue":    "default",
		"jobType":  "noop",
		"priority": "P1",
		"payload":  map[string]any{},
	})

	waitFor(t, func() bool {
		j := fetch(t, id)
		return j["state"] == "succeeded"
	}, 15*time.Second, "noop job to succeed")
}

// TestRetryAndDLQ: a deliberately-failing job with maxAttempts=2 should end
// up dead-lettered after 2 attempts.
func TestRetryAndDLQ(t *testing.T) {
	skipIfNotIntegration(t)

	id := submit(t, map[string]any{
		"queue":   "default",
		"jobType": "always_fail",
		"priority": "P2",
		"retryPolicy": map[string]any{
			"maxAttempts":  2,
			"backoff":      "fixed",
			"initialDelay": "500ms",
		},
		"payload": map[string]any{},
	})

	waitFor(t, func() bool {
		j := fetch(t, id)
		return j["state"] == "dead_lettered"
	}, 30*time.Second, "always_fail job to dead-letter")

	final := fetch(t, id)
	require.EqualValues(t, "dead_lettered", final["state"])
	require.InDelta(t, 2.0, final["attempts"], 0.0001,
		"maxAttempts=2 means two total claim+execute attempts before DLQ")
}

// TestDeduplication: two submissions with the same key and mode=reject must
// return 202 then 409.
func TestDeduplication(t *testing.T) {
	skipIfNotIntegration(t)

	key := fmt.Sprintf("dedup-%d", time.Now().UnixNano())
	body := map[string]any{
		"queue":             "default",
		"jobType":           "sleep",
		"priority":          "P3",
		"deduplicationKey":  key,
		"deduplicationMode": "reject",
		"payload":           map[string]any{"ms": 500},
	}

	// First submission accepted.
	resp1 := post(t, "/v1/jobs", body)
	require.Equal(t, http.StatusAccepted, resp1.StatusCode)
	resp1.Body.Close()

	// Second submission rejected.
	resp2 := post(t, "/v1/jobs", body)
	require.Equal(t, http.StatusConflict, resp2.StatusCode)
	resp2.Body.Close()
}

// TestDelayedJob: a job with delaySeconds=2 must not be in a running/done
// state immediately after submission.
func TestDelayedJob(t *testing.T) {
	skipIfNotIntegration(t)

	id := submit(t, map[string]any{
		"queue":        "default",
		"jobType":      "noop",
		"priority":     "P2",
		"delaySeconds": 2,
		"payload":      map[string]any{},
	})

	j := fetch(t, id)
	require.Contains(t, []string{"pending"}, j["state"],
		"expected delayed job to remain pending immediately after submit")

	waitFor(t, func() bool {
		j := fetch(t, id)
		return j["state"] == "succeeded"
	}, 30*time.Second, "delayed job to succeed after delay")
}

// --- helpers ---------------------------------------------------------------

func submit(t *testing.T, body map[string]any) string {
	resp := post(t, "/v1/jobs", body)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	id, _ := out["jobId"].(string)
	require.NotEmpty(t, id)
	return id
}

func fetch(t *testing.T, id string) map[string]any {
	resp, err := http.Get(apiBase + "/v1/jobs/" + id)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func post(t *testing.T, path string, body any) *http.Response {
	b, _ := json.Marshal(body)
	resp, err := http.Post(apiBase+path, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	return resp
}
