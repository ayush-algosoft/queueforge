package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ayush-algosoft/queueforge/internal/job"
)

func TestTopology_PriorityNames(t *testing.T) {
	tp := Topology{Prefix: "qf"}
	assert.Equal(t, "qf.jobs.p0", tp.Priority(job.PriorityP0))
	assert.Equal(t, "qf.jobs.p1", tp.Priority(job.PriorityP1))
	assert.Equal(t, "qf.jobs.p2", tp.Priority(job.PriorityP2))
	assert.Equal(t, "qf.jobs.p3", tp.Priority(job.PriorityP3))
	assert.Equal(t, "qf.jobs.dlq", tp.DLQ())
}

func TestTopology_AllPriorityTopics(t *testing.T) {
	tp := Topology{Prefix: "qf"}
	want := []string{"qf.jobs.p0", "qf.jobs.p1", "qf.jobs.p2", "qf.jobs.p3"}
	assert.Equal(t, want, tp.AllPriorityTopics())
}

func TestTopology_PrioritiesToTopics_SkipsInvalid(t *testing.T) {
	tp := Topology{Prefix: "qf"}
	got := tp.PrioritiesToTopics([]string{"P0", "BOGUS", " P2 ", ""})
	assert.Equal(t, []string{"qf.jobs.p0", "qf.jobs.p2"}, got)
}
