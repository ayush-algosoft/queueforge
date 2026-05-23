package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/queueforge/queueforge/internal/job"
)

// Envelope is the payload we put on Kafka topics. It carries enough
// information for a worker to start processing without re-reading Postgres
// (the worker still updates Postgres on completion).
type Envelope struct {
	JobID    string          `json:"jobId"`
	Queue    string          `json:"queue"`
	JobType  string          `json:"jobType"`
	Priority job.Priority    `json:"priority"`
	Attempts int             `json:"attempts"`
	Payload  json.RawMessage `json:"payload"`
}

// Producer publishes job envelopes onto Kafka topics.
type Producer struct {
	cli      *kgo.Client
	topology Topology
}

// NewProducer builds an idempotent franz-go producer.
func NewProducer(brokers []string, clientID string, topology Topology) (*Producer, error) {
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cli: cli, topology: topology}, nil
}

// Close flushes pending records and shuts the client down.
func (p *Producer) Close(ctx context.Context) error {
	if err := p.cli.Flush(ctx); err != nil {
		return err
	}
	p.cli.Close()
	return nil
}

// PublishJob writes a job envelope to its priority topic. Returns once the
// broker has acknowledged the record on all in-sync replicas.
func (p *Producer) PublishJob(ctx context.Context, j *job.Job) error {
	env := Envelope{
		JobID:    j.ID,
		Queue:    j.Queue,
		JobType:  j.JobType,
		Priority: j.Priority,
		Attempts: j.Attempts,
		Payload:  j.Payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	topic := p.topology.Priority(j.Priority)
	rec := &kgo.Record{
		Topic: topic,
		// Key partitions by queue so jobs for the same logical queue land on
		// the same partition. That gives within-queue ordering when a single
		// worker is consuming the partition. Cross-queue parallelism still
		// works because different queues hash to different partitions.
		Key:   []byte(j.Queue),
		Value: body,
	}
	return p.cli.ProduceSync(ctx, rec).FirstErr()
}

// PublishDLQ writes a failed job's envelope to the DLQ topic with the failure
// reason attached as a header for operator visibility.
func (p *Producer) PublishDLQ(ctx context.Context, j *job.Job, reason string) error {
	env := Envelope{
		JobID:    j.ID,
		Queue:    j.Queue,
		JobType:  j.JobType,
		Priority: j.Priority,
		Attempts: j.Attempts,
		Payload:  j.Payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	rec := &kgo.Record{
		Topic: p.topology.DLQ(),
		Key:   []byte(j.Queue),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: "qf-reason", Value: []byte(reason)},
			{Key: "qf-job-type", Value: []byte(j.JobType)},
			{Key: "qf-attempts", Value: []byte(fmt.Sprintf("%d", j.Attempts))},
		},
	}
	return p.cli.ProduceSync(ctx, rec).FirstErr()
}
