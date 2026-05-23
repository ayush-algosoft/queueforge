package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates the priority + DLQ topics if they don't already exist.
// Idempotent — running it multiple times is harmless.
func EnsureTopics(ctx context.Context, brokers []string, clientID string, topology Topology, partitions int32, replication int16) error {
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
	)
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer cli.Close()
	adm := kadm.NewClient(cli)

	wantTopics := append(topology.AllPriorityTopics(), topology.DLQ())

	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(deadline, partitions, replication, nil, wantTopics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, t := range resp.Sorted() {
		if t.Err == nil {
			continue
		}
		// "Topic already exists" is the goal of EnsureTopics — not an error.
		if strings.Contains(strings.ToLower(t.Err.Error()), "already exists") {
			continue
		}
		// Other errors (e.g. invalid replication when only one broker is up)
		// are real and worth surfacing.
		return fmt.Errorf("create topic %s: %w", t.Topic, t.Err)
	}
	return nil
}

// ErrNoBrokers is returned when admin operations are attempted with an empty
// broker list — caught early so the user gets a clear message rather than a
// franz-go internal panic.
var ErrNoBrokers = errors.New("kafka: no brokers configured")
