package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates the priority + DLQ topics if they don't already
// exist. Idempotent: "topic already exists" is treated as success.
func EnsureTopics(ctx context.Context, brokers []string, clientID string, topology Topology, partitions int32, replication int16) error {
	cli, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID(clientID))
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
		if t.Err == nil || strings.Contains(strings.ToLower(t.Err.Error()), "already exists") {
			continue
		}
		return fmt.Errorf("create topic %s: %w", t.Topic, t.Err)
	}
	return nil
}
