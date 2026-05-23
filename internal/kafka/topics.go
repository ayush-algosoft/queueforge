// Package kafka centralises Kafka client wiring and topic naming.
//
// Topology:
//
//	<prefix>.jobs.p0..p3    one topic per priority bucket; workers consume
//	                        these directly. Partitions parallelise within a
//	                        priority; the consumer group balances them.
//	<prefix>.jobs.dlq       dead-letter topic for jobs whose retries are
//	                        exhausted.
//
// Retries are time-shifted in Postgres (worker writes the row back to
// pending with a future run_at) rather than via separate timed retry
// topics. That gives arbitrary back-off precision without an explosion of
// topics or consumer configs.
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
