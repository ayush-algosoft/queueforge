package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer wraps franz-go for the worker side. It polls a set of priority
// topics in a single consumer group and exposes them as decoded Envelopes.
//
// Offset semantics: we use the group's automatic offset management, but
// commit *manually* — only after the worker has finished the database
// transition for the message. That gives at-least-once delivery with a clean
// crash-recovery story: an uncommitted offset means "process this again",
// and the dedup/visibility logic above handles the duplicate.
type Consumer struct {
	cli *kgo.Client
}

// ConsumerOptions configures the consumer.
type ConsumerOptions struct {
	Brokers     []string
	ClientID    string
	GroupID     string
	Topics      []string
	SessionTime time.Duration // optional override; default 45s
}

// NewConsumer builds a manual-commit consumer for the given topics.
func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if opts.SessionTime == 0 {
		opts.SessionTime = 45 * time.Second
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID(opts.ClientID),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.SessionTimeout(opts.SessionTime),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cli: cli}, nil
}

// Message couples a decoded envelope with its underlying Kafka record so the
// caller can commit the offset after work completes.
type Message struct {
	Envelope Envelope
	Record   *kgo.Record
}

// Poll fetches a batch of messages. It returns when records arrive, the
// context is cancelled, or the broker times out the long-poll.
//
// AllowRebalance must be called between Poll cycles when no work is in
// flight so franz-go can perform group rebalancing — we call it implicitly
// at the end of each batch.
func (c *Consumer) Poll(ctx context.Context) ([]Message, error) {
	fetches := c.cli.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		// surface only the first; franz-go's fetch errors are usually
		// transient (e.g. metadata reload) and the client recovers itself
		return nil, errs[0].Err
	}
	var out []Message
	fetches.EachRecord(func(r *kgo.Record) {
		var env Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			// Skip malformed records but commit them anyway — leaving them
			// uncommitted would re-deliver forever. The job ID is unknown so
			// there is no Postgres row to update.
			return
		}
		out = append(out, Message{Envelope: env, Record: r})
	})
	return out, nil
}

// CommitRecords commits offsets for the supplied records. Safe to call with
// records from a single Poll batch — franz-go merges per-partition offsets.
func (c *Consumer) CommitRecords(ctx context.Context, recs ...*kgo.Record) error {
	return c.cli.CommitRecords(ctx, recs...)
}

// AllowRebalance releases the rebalance lock taken by BlockRebalanceOnPoll.
func (c *Consumer) AllowRebalance() { c.cli.AllowRebalance() }

// Close shuts the consumer client down, leaving the group cleanly.
func (c *Consumer) Close() { c.cli.Close() }
