# QueueForge

QueueForge is a Kafka-backed distributed job queue. Services submit jobs over
HTTP; the platform persists them in PostgreSQL, schedules delayed/retry
execution, dispatches them to a fleet of workers through partitioned Kafka
topics, and recovers from worker crashes via lease-based visibility timeouts.

It is implemented as four small Go services that share a single repository,
a single configuration surface, and a single observability story.

```
┌──────────┐   POST /v1/jobs   ┌─────┐
│ Producer │ ────────────────► │ API │ ─┐
└──────────┘                   └──┬──┘  │  insert
                                  │     ▼
                                  │  ┌──────────┐
                                  │  │ Postgres │  (state of record)
                                  │  └────┬─────┘
                                  │       │ fetch-due, mark-queued
                                  │       ▼
                                  │  ┌──────────────┐
                                  └─►│  Scheduler   │── publish ──┐
                                     └──────────────┘             │
                                                                  ▼
                                                      ┌────────────────────┐
                                                      │ Kafka priority     │
                                                      │ topics (p0..p3,    │
                                                      │ dlq)               │
                                                      └────┬───────────────┘
                                                           │ consume
                                                           ▼
                                       ┌──────────────────────────┐
                                       │        Worker            │
                                       │ claim → run → succeed /  │
                                       │  retry-as-pending /      │
                                       │  dead-letter             │
                                       └────────────┬─────────────┘
                                                    │ stale-lease scan
                                                    ▼
                                              ┌────────────┐
                                              │  Recovery  │
                                              └────────────┘
```

---

## Why these choices

### Postgres is the state of record; Kafka is the executable handoff channel

A Kafka topic alone cannot answer "what is the state of job X?" — it is a
log, not a key/value store. We keep the authoritative job row in PostgreSQL
and use Kafka strictly for delivery. This split lets us:

* Look any job up by ID, regardless of how long ago it ran.
* Enforce deduplication via a unique partial index, atomically and durably.
* Reclaim crashed jobs by scanning for expired visibility leases — Kafka has
  no equivalent.

The trade-off is one extra DB write per state transition. In return we get
operational properties that a Kafka-only design cannot match.

### Retries are time-shifted in Postgres, not in Kafka

Some Kafka job-queue designs use a separate topic per retry delay class
(retry-30s, retry-2m, retry-10m, …). That works but has two problems:
delays are quantised to topic granularity, and the worker fleet must
consume from every retry topic.

Instead, when a worker fails a job it writes the row back to `state =
'pending'` with `run_at = now + backoff`. The scheduler picks it up on the
next due-jobs scan and republishes to the original priority topic. Result:
arbitrary delay precision, and only one consumer config per priority.

### Priority is implemented as separate topics, not as a queue-level field

A worker process can be configured to consume any subset of `p0..p3`.
Dedicated "payments-only" worker pools subscribe to `p0`; general-purpose
pools subscribe to all four. The Kafka consumer group handles partition
balancing inside a priority; the priority-to-pool mapping handles ordering
between priorities. No in-process priority queue is needed.

### At-least-once, with deduplication as the safety net

The worker commits its Kafka offset **after** the database transition is
durable. A crash before commit → the message is redelivered, the
`Claim` UPDATE either succeeds (a fresh attempt) or returns "not claimable"
(another worker won), and the duplicate offset commits without re-running
the handler.

For business-level idempotency, callers pass a `deduplicationKey`. A
unique partial index on `(queue, dedup_key) WHERE state IN
('pending','queued','running')` guarantees that only one non-terminal job
exists per key, regardless of how many concurrent submissions arrive.

### Visibility timeouts, not orphan detection

When a worker claims a job, it gets a lease (default 60s). It heartbeats
the lease forward while the handler runs. If the worker process dies, the
lease expires and the recovery service flips the row back to `pending`. The
job runs again — and the `attempts` counter still increments, so a
permanently-failing job eventually dead-letters.

---

## Repository layout

```
cmd/
  api/         REST API service
  scheduler/   Promotes due jobs from Postgres to Kafka
  worker/      Executes job handlers
  recovery/    Reclaims expired visibility leases

internal/
  config/      Single source of env-driven configuration
  job/         Domain types (state, priority, retry policy)
  api/         HTTP handlers and middleware
  scheduler/
  worker/
  recovery/
  handlers/    Demo job handlers (noop, sleep, flaky, always_fail)
  kafka/       franz-go wiring + topic naming
  storage/
    postgres/  Repository + embedded migrations
    redisrepo/ Dedup + token-bucket rate limiter
  logging/
  metrics/     Prometheus collectors

deployments/
  docker/      Multi-stage Dockerfile shared by every binary
  prometheus/  Scrape config
  grafana/     Provisioned datasource + dashboard

migrations/    SQL migrations (also embedded into the postgres package)
tests/integration/  HTTP-level smoke tests against a running stack
```

---

## Running locally

Requirements: Docker + Docker Compose v2.

```bash
docker compose up -d --build
```

The first build downloads dependencies and compiles all four binaries from
the single multi-stage Dockerfile (`deployments/docker/Dockerfile`,
parameterised by `SERVICE`). After build, the stack comes up in roughly 20
seconds.

Verify everything is healthy:

```bash
curl -s http://localhost:8080/readyz
docker compose ps
```

### Submit a job

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "queue":    "default",
    "jobType":  "noop",
    "priority": "P1",
    "payload":  {}
  }'
```

Returns:

```json
{ "jobId":"...", "state":"queued", "runAt":"..." }
```

Fetch its final state:

```bash
curl -s http://localhost:8080/v1/jobs/<jobId>
```

### Other supported jobTypes (shipped for end-to-end exercise)

* `noop` – returns immediately.
* `sleep` – sleeps `payload.ms` milliseconds.
* `flaky` – fails with probability `payload.failRate` (default 30%).
* `always_fail` – always returns an error; combined with `maxAttempts=2`
  this drives the DLQ flow.

### Observability

* API metrics: `http://localhost:9090/metrics`
* Scheduler / Worker / Recovery metrics: `:9091`, `:9192`, `:9093`
* Prometheus: `http://localhost:9094`
* Grafana (anonymous, admin role): `http://localhost:3000`
  – dashboard "QueueForge Overview" is provisioned automatically.

### Tear down

```bash
docker compose down -v
```

---

## API

### `POST /v1/jobs`

Request body:

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

* `202 Accepted` — new job persisted.
* `200 OK` — duplicate, `deduplicationMode: return_existing` returned the
  existing job id.
* `409 Conflict` — duplicate, `deduplicationMode: reject` (default).
* `429 Too Many Requests` — global rate limit exceeded.
* `400 Bad Request` — invalid payload.

### Other endpoints

* `GET /v1/jobs/{id}` — full job state.
* `GET /v1/jobs?queue=&state=&limit=` — recent jobs.
* `POST /v1/jobs/{id}/cancel` — cancel a job still in `pending`/`queued`.
* `GET /v1/queues/stats` — counts by `(queue, priority, state)` and the
  oldest-pending age per priority.
* `GET /health`, `GET /readyz`, `GET /metrics`.

---

## Configuration

All services read environment variables; defaults match `docker-compose.yml`.

| Variable                          | Default                                              | Notes                                        |
|-----------------------------------|------------------------------------------------------|----------------------------------------------|
| `QF_ENV`                          | `development`                                        |                                              |
| `QF_LOG_LEVEL`                    | `info`                                               | trace / debug / info / warn / error          |
| `QF_HTTP_ADDR`                    | `:8080`                                              | API only                                     |
| `QF_METRICS_ADDR`                 | `:9090`                                              | per-service Prometheus endpoint              |
| `QF_POSTGRES_DSN`                 | `postgres://queueforge:queueforge@localhost:5432/…`  |                                              |
| `QF_POSTGRES_MAX_CONNS`           | `10`                                                 |                                              |
| `QF_POSTGRES_MIGRATE`             | `true`                                               | apply embedded migrations on start           |
| `QF_REDIS_ADDR`                   | `localhost:6379`                                     |                                              |
| `QF_KAFKA_BROKERS`                | `localhost:9092`                                     | comma-separated                              |
| `QF_KAFKA_TOPIC_PREFIX`           | `qf`                                                 | yields `qf.jobs.p0`, …, `qf.jobs.dlq`        |
| `QF_SCHEDULER_POLL_INTERVAL`      | `1s`                                                 |                                              |
| `QF_SCHEDULER_BATCH_SIZE`         | `200`                                                |                                              |
| `QF_WORKER_CONCURRENCY`           | `8`                                                  |                                              |
| `QF_WORKER_PRIORITIES`            | `P0,P1,P2,P3`                                        | which priority topics this worker consumes   |
| `QF_WORKER_VISIBILITY_TIMEOUT`    | `60s`                                                | lease length                                 |
| `QF_WORKER_HEARTBEAT_INTERVAL`    | `15s`                                                | should be < `1/3 × visibility timeout`       |
| `QF_WORKER_HANDLER_TIMEOUT`       | `5m`                                                 | hard limit on handler execution              |
| `QF_RECOVERY_SCAN_INTERVAL`       | `10s`                                                |                                              |

---

## Tests

```bash
# Unit tests — no infrastructure required.
go test ./internal/...

# Integration smoke tests — assume `docker compose up -d` is running.
QF_INTEGRATION=1 go test ./tests/integration -v -count=1
```

The integration test exercises:

* Submission + successful completion.
* Retry policy + dead-lettering after attempts exhausted.
* Deduplication (`409` on second submission with the same key).
* Delayed job execution.

---

## Operational notes

* **Adding a new job type.** Implement a `handlers.Handler`, register it via
  `Registry.Register(jobType, fn)` in `cmd/worker/main.go` (or fork the
  registry into a separate package). No protocol changes required.
* **Scaling workers.** Increase `QF_WORKER_CONCURRENCY` or run more worker
  containers. Kafka rebalances partitions across the consumer group
  automatically. For priority-aware scaling, run separate pools that
  subscribe to different `QF_WORKER_PRIORITIES` subsets.
* **Topics.** Created on startup if missing
  (`internal/kafka.EnsureTopics`). Partition count is 3 by default; tune in
  `cmd/api/main.go` if you operate with more brokers.
* **Migrations.** SQL files live in `migrations/`. They are also embedded
  into `internal/storage/postgres` so any binary with
  `QF_POSTGRES_MIGRATE=true` will apply them on start.
* **Failures during publish.** If the API or scheduler publishes a job
  successfully but the corresponding `MarkQueued` UPDATE fails, the row
  remains `pending`. The `Claim` UPDATE accepts `state IN
  ('pending','queued')` so the worker can still process the message; the
  next scheduler scan will eventually fix the row's state too.
* **DLQ replay.** The DLQ is a regular Kafka topic. A small replay tool
  (read DLQ → republish to the original priority topic, mark Postgres row
  pending) is left as an operator concern. The dashboards expose
  dead-letter counts so operators know when to look.

---

## License

MIT. See `LICENSE`.
