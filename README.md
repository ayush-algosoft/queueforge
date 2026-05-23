# QueueForge

QueueForge is a Kafka-backed distributed job queue. Services submit jobs
over HTTP; the platform persists them in PostgreSQL, schedules delayed
and retry execution, dispatches them to a fleet of workers through
partitioned Kafka topics, and recovers from worker crashes via
lease-based visibility timeouts.

Four small Go services share a single repository, a single configuration
surface, and a single observability story.

```
 client ─► API ─► Postgres (state of record)
                │
                └─► Kafka priority topics ─► Worker ─► Postgres
                       ▲                        │
                       │   re-publish due/retry │ marks succeeded, schedules
                Scheduler ◄─── Postgres         │ retry, or dead-letters
                                                ▼
                                           DLQ topic
 Recovery ─── scans Postgres for expired visibility leases
```

## Design rationale

**Postgres is the state of record; Kafka is the executable handoff
channel.** A Kafka topic alone cannot answer "what is the state of job
X?" — it is a log, not a key/value store. We keep the authoritative job
row in PostgreSQL and use Kafka strictly for delivery. This gives us
durable lookups by ID, atomic dedup via a unique partial index, and the
ability to reclaim crashed jobs by scanning for expired leases. The cost
is one extra DB write per state transition.

**Retries are time-shifted in Postgres, not via timed retry topics.** When
a worker fails a job, it writes the row back to `state = 'pending'` with
`run_at = now + backoff`. The scheduler republishes when due. This gives
arbitrary delay precision and a single consumer config per priority,
rather than one topic per delay class.

**Priority is implemented as separate topics, not as an in-process
queue.** Each worker can be configured to consume any subset of `p0..p3`,
which lets you run dedicated worker pools for critical work
(`QF_WORKER_PRIORITIES=P0`) alongside general-purpose pools.

**At-least-once with a deduplication safety net.** The worker commits the
Kafka offset only after the database transition is durable; a crash
before commit causes one redelivery, and the next `Claim` either
succeeds (a fresh attempt) or no-ops (another worker won). For
business-level idempotency, callers pass a `deduplicationKey`. A unique
partial index on `(queue, dedup_key) WHERE state IN
('pending','queued','running')` enforces it atomically.

**Visibility timeouts handle worker crashes.** A claim takes a 60s lease
(default). The worker heartbeats it forward while the handler runs. If
the worker dies, the lease expires and recovery returns the row to
`pending`. `attempts` still increments, so a permanently-broken job
eventually dead-letters.

## Repository layout

```
cmd/
  api/         REST submission API
  scheduler/   Promotes due jobs from Postgres to Kafka
  worker/      Executes job handlers
  recovery/    Reclaims expired visibility leases

internal/
  api/         HTTP handlers and middleware
  config/      Single source of env-driven configuration
  handlers/    Demo job handlers (noop, sleep, flaky, always_fail)
  job/         Domain types (state, priority, retry policy)
  kafka/       franz-go wiring + topic naming
  logging/
  metrics/
  recovery/
  scheduler/
  storage/
    postgres/  Repository + embedded migrations
    redisrepo/ Deduplication + rate limiter
  worker/

deployments/
  docker/      Multi-stage Dockerfile shared by every binary
  grafana/     Provisioned datasource + overview dashboard
  prometheus/  Scrape config

tests/integration/  HTTP-level smoke tests against a running stack
docs/architecture.md
```

## Running locally

Requirements: Docker, Docker Compose v2.

From the project root:

```bash
docker compose up -d --build
curl -s http://localhost:8080/readyz
```

Submit a job:

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","jobType":"noop","priority":"P1","payload":{}}'
```

Fetch its state:

```bash
curl -s http://localhost:8080/v1/jobs/<jobId>
```

### Demo job types

| jobType       | Behaviour                                                      |
|---------------|----------------------------------------------------------------|
| `noop`        | Returns immediately.                                           |
| `sleep`       | Sleeps `payload.ms` milliseconds.                              |
| `flaky`       | Fails with probability `payload.failRate` (default 30%).       |
| `always_fail` | Always errors; pair with `maxAttempts=2` to exercise the DLQ.  |

### Observability

| Endpoint                          | Purpose                                                |
|-----------------------------------|--------------------------------------------------------|
| `http://localhost:8080/metrics`   | API metrics (mounted on the same listener as the API). |
| `http://localhost:9091/metrics`   | Scheduler metrics.                                     |
| `http://localhost:9192/metrics`   | Worker metrics.                                        |
| `http://localhost:9093/metrics`   | Recovery metrics.                                      |
| `http://localhost:9094`           | Prometheus UI.                                         |
| `http://localhost:3000`           | Grafana (anonymous admin). Dashboard is provisioned.   |

### Tear down

```bash
docker compose down -v
```

## API

### `POST /v1/jobs`

```json
{
  "queue": "notifications",
  "jobType": "send_email",
  "priority": "P1",
  "payload": { "userId": "123" },
  "delaySeconds": 30,
  "deduplicationKey": "welcome_email_user_123",
  "deduplicationMode": "reject",
  "retryPolicy": {
    "maxAttempts": 5,
    "backoff": "exponential",
    "initialDelay": "30s",
    "maxDelay": "10m"
  }
}
```

Responses:

| Status | Meaning                                                                          |
|--------|----------------------------------------------------------------------------------|
| `202`  | New job persisted.                                                               |
| `200`  | Duplicate detected, `deduplicationMode: return_existing` returned the prior id.  |
| `409`  | Duplicate detected, `deduplicationMode: reject` (the default).                   |
| `429`  | Global rate limit exceeded.                                                      |
| `400`  | Invalid request payload.                                                         |

### Other endpoints

| Method | Path                       | Notes                                                   |
|--------|----------------------------|---------------------------------------------------------|
| `GET`  | `/v1/jobs/{id}`            | Full job row.                                           |
| `GET`  | `/v1/jobs?queue=&state=`   | Recent jobs, newest first; `limit` defaults to 50.      |
| `POST` | `/v1/jobs/{id}/cancel`     | Cancels a job still in `pending` or `queued`.           |
| `GET`  | `/v1/queues/stats`         | Counts by `(queue, priority, state)` + oldest-pending ages. |
| `GET`  | `/health`                  | Liveness.                                               |
| `GET`  | `/readyz`                  | Readiness (pings the DB).                               |

## Configuration

Every service reads the same environment variables; defaults match the
Docker Compose stack.

| Variable                          | Default                                              |
|-----------------------------------|------------------------------------------------------|
| `QF_ENV`                          | `development`                                        |
| `QF_LOG_LEVEL`                    | `info`                                               |
| `QF_HTTP_ADDR`                    | `:8080` (API only)                                   |
| `QF_METRICS_ADDR`                 | `:9090` (overridden per service in compose)          |
| `QF_POSTGRES_DSN`                 | `postgres://queueforge:queueforge@localhost:5432/queueforge?sslmode=disable` |
| `QF_POSTGRES_MAX_CONNS`           | `10`                                                 |
| `QF_POSTGRES_MIGRATE`             | `true`                                               |
| `QF_REDIS_ADDR`                   | `localhost:6379`                                     |
| `QF_KAFKA_BROKERS`                | `localhost:9092` (comma-separated for multi-broker)  |
| `QF_KAFKA_TOPIC_PREFIX`           | `qf` (yields `qf.jobs.p0`, …, `qf.jobs.dlq`)         |
| `QF_SCHEDULER_POLL_INTERVAL`      | `1s`                                                 |
| `QF_SCHEDULER_BATCH_SIZE`         | `200`                                                |
| `QF_WORKER_CONCURRENCY`           | `8`                                                  |
| `QF_WORKER_PRIORITIES`            | `P0,P1,P2,P3`                                        |
| `QF_WORKER_VISIBILITY_TIMEOUT`    | `60s`                                                |
| `QF_WORKER_HEARTBEAT_INTERVAL`    | `15s` (keep below 1/3 of visibility timeout)         |
| `QF_WORKER_HANDLER_TIMEOUT`       | `5m`                                                 |
| `QF_RECOVERY_SCAN_INTERVAL`       | `10s`                                                |

## Tests

```bash
# Unit tests (no infrastructure required).
go test ./internal/...

# Integration tests against a running stack.
docker compose up -d --build
QF_INTEGRATION=1 go test ./tests/integration -v -count=1
```

The integration test exercises submission + completion, retry +
dead-letter, deduplication, and delayed execution. See
`verified_local_run.md` for captured outputs from a real local run.

## Operations

* **Adding a job type.** Implement a `handlers.Handler` and register it
  via `Registry.Register(jobType, fn)` in `cmd/worker/main.go`. No
  protocol changes.
* **Scaling workers.** Increase `QF_WORKER_CONCURRENCY` or run more
  worker containers. Kafka rebalances partitions across the consumer
  group. For priority-aware scaling, run separate pools with different
  `QF_WORKER_PRIORITIES` subsets.
* **Topics.** Created on API startup by `internal/kafka.EnsureTopics`
  (3 partitions, replication factor 1). Tune for multi-broker
  deployments in `cmd/api/main.go`.
* **Migrations.** Embedded in `internal/storage/postgres/migrations`.
  Any binary started with `QF_POSTGRES_MIGRATE=true` will apply them.
* **DLQ replay.** The DLQ is a regular Kafka topic; replay tooling is
  left to operators (read DLQ → republish to the original priority
  topic, mark Postgres row pending).

See `docs/architecture.md` for the full job lifecycle, service
responsibilities, and failure decision table.

## License

MIT — see `LICENSE`.
