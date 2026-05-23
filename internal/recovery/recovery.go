// Package recovery owns the janitor responsibilities:
//
//   * Reclaiming jobs whose visibility lease has expired (a worker died
//     mid-execution and never heartbeated the lease forward).
//   * Surfacing the recovered count as a metric so operators can alert on it.
//
// Reclaimed jobs go back to state='pending' with run_at=now, so the
// scheduler will republish them on its next pass. Attempts counter is
// preserved — the next claim will still increment it, and the retry policy
// will eventually decide the job has exhausted its attempts.
package recovery

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/queueforge/queueforge/internal/metrics"
	"github.com/queueforge/queueforge/internal/storage/postgres"
)

type Recovery struct {
	Repo         *postgres.Repo
	ScanInterval time.Duration
	BatchSize    int
	Logger       zerolog.Logger
}

func (r *Recovery) Run(ctx context.Context) error {
	if r.ScanInterval <= 0 {
		r.ScanInterval = 10 * time.Second
	}
	if r.BatchSize <= 0 {
		r.BatchSize = 200
	}
	r.Logger.Info().
		Dur("interval", r.ScanInterval).
		Int("batch", r.BatchSize).
		Msg("recovery running")

	t := time.NewTicker(r.ScanInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			n, err := r.Repo.ReclaimExpiredLeases(ctx, r.BatchSize)
			if err != nil {
				r.Logger.Warn().Err(err).Msg("reclaim scan failed")
				continue
			}
			if n > 0 {
				metrics.RecoveryReclaimed.Add(float64(n))
				r.Logger.Info().Int("reclaimed", n).Msg("reclaimed expired leases")
			}
		}
	}
}
