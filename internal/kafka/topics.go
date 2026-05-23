// Package kafka centralises Kafka client wiring and topic naming.
//
// Topology:
//
//	<prefix>.jobs.p0          one topic per priority bucket — workers consume
//	<prefix>.jobs.p1          these directly. Partitions parallelise within a
//	<prefix>.jobs.p2          priority and the consumer group balances them
//	<prefix>.jobs.p3          across worker pods.
//
//	<prefix>.jobs.dlq         dead-letter topic. Failed jobs after retries
//	                          land here and the recovery service can replay
//	                          from it on operator instruction.
//
// Retries are *not* implemented with separate timed retry topics in this
// build — instead, the worker writes the job back to Postgres with a future
// run_at and the scheduler re-promotes it when it becomes due. That gives
// arbitrary back-off precision without needing one topic per delay class.
package kafka

import (
	"strings"

	"github.com/queueforge/queueforge/internal/job"
)

// Topology generates fully-qualified topic names for a given prefix.
type Topology struct {
	Prefix string
}

// Priority returns the Kafka topic for a given job priority.
func (t Topology) Priority(p job.Priority) string {
	return t.Prefix + ".jobs." + strings.ToLower(string(p))
}

// DLQ returns the dead-letter topic name.
func (t Topology) DLQ() string {
	return t.Prefix + ".jobs.dlq"
}

// AllPriorityTopics returns every priority topic.
func (t Topology) AllPriorityTopics() []string {
	return []string{
		t.Priority(job.PriorityP0),
		t.Priority(job.PriorityP1),
		t.Priority(job.PriorityP2),
		t.Priority(job.PriorityP3),
	}
}

// PrioritiesToTopics maps a list of priority strings to their topics, skipping
// invalid entries (so an operator typo in QF_WORKER_PRIORITIES doesn't crash
// the worker).
func (t Topology) PrioritiesToTopics(priorities []string) []string {
	out := make([]string, 0, len(priorities))
	for _, p := range priorities {
		pri := job.Priority(strings.TrimSpace(p))
		if !job.ValidPriority(pri) {
			continue
		}
		out = append(out, t.Priority(pri))
	}
	return out
}
