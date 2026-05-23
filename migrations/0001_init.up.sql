-- QueueForge core schema.
--
-- Design notes:
-- * `jobs` is the authoritative store. Kafka is the executable handoff channel,
--   but state of record lives here so we can answer "what is the state of job X?"
--   even after a Kafka topic is compacted or replayed.
-- * Visibility leases (claimed_by + visibility_until) let us recover from
--   worker crashes by a janitor that scans for expired leases.
-- * The unique partial index on (queue, dedup_key) enforces deduplication
--   atomically at the database level — Redis is only a fast pre-check.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue           TEXT        NOT NULL,
    job_type        TEXT        NOT NULL,
    priority        TEXT        NOT NULL CHECK (priority IN ('P0','P1','P2','P3')),
    state           TEXT        NOT NULL CHECK (state IN (
                        'pending','queued','running','succeeded',
                        'failed','dead_lettered','cancelled')),
    payload         JSONB       NOT NULL,
    dedup_key       TEXT,
    dedup_mode      TEXT        NOT NULL DEFAULT 'reject',
    retry_policy    JSONB       NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    last_error      TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    visibility_until TIMESTAMPTZ,
    claimed_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    result          JSONB
);

-- Drives the scheduler's "what is due now?" scan.
CREATE INDEX IF NOT EXISTS jobs_pending_runat_idx
    ON jobs (run_at)
    WHERE state = 'pending';

-- Drives the janitor's "what lease has expired?" scan.
CREATE INDEX IF NOT EXISTS jobs_visibility_idx
    ON jobs (visibility_until)
    WHERE state = 'running';

-- For dashboard queries and the queue-stats API.
CREATE INDEX IF NOT EXISTS jobs_queue_state_idx
    ON jobs (queue, priority, state);

-- Deduplication: only one *non-terminal* job per (queue, dedup_key).
-- Terminal jobs are excluded so a later submission with the same key after
-- success/failure is allowed (caller can opt in via dedup_mode semantics).
CREATE UNIQUE INDEX IF NOT EXISTS jobs_dedup_active_uidx
    ON jobs (queue, dedup_key)
    WHERE dedup_key IS NOT NULL
      AND state IN ('pending','queued','running');

-- Materialised view-style log of state changes for auditing / debugging.
CREATE TABLE IF NOT EXISTS job_events (
    id          BIGSERIAL   PRIMARY KEY,
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    from_state  TEXT,
    to_state    TEXT        NOT NULL,
    actor       TEXT,
    detail      JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, occurred_at DESC);
