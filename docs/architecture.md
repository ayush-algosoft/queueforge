# QueueForge — Architecture

This document covers the job lifecycle, service contracts, and failure
handling in more detail than the project `README.md`. Read that first.

## Job lifecycle

```
                       ┌──────────┐
                       │ pending  │◄──────────────┐
                       └────┬─────┘               │
              scheduler publishes                 │
                       to Kafka                   │
                            │                     │
                            ▼                     │ worker retry
                       ┌──────────┐                │  (run_at += backoff)
                       │  queued  │                │
                       └────┬─────┘                │
                worker claims                      │
                            │                     │
                            ▼                     │
                       ┌──────────┐                │
                       │ running  │────────────────┘
                       └────┬─────┘
       ┌────────────────────┼────────────────────────┐
       │                    │                        │
       ▼                    ▼                        ▼
 ┌───────────┐      ┌──────────────┐         ┌──────────────────┐
 │ succeeded │      │ dead_letter…  │         │ visibility-lease │
 └───────────┘      └──────────────┘         │  expired → back  │
                                              │     to pending   │
                                              └──────────────────┘
```

* **pending** — durably stored, not yet on Kafka.
* **queued** — published to a priority topic, not yet claimed.
* **running** — claimed by a worker; visibility lease active.
* **succeeded** / **failed** / **dead_lettered** / **cancelled** — terminal.

State transitions are guarded by a CHECK constraint in PostgreSQL and by
predicates on each UPDATE statement (e.g. `Claim` only matches `state IN
('pending','queued')`).

## Service responsibilities

| Service    | Reads                                  | Writes                                                                                    |
|------------|----------------------------------------|-------------------------------------------------------------------------------------------|
| API        | Redis (dedup, rate limit)              | Postgres (insert), Kafka (immediate publish if due)                                       |
| Scheduler  | Postgres (`FetchDue`)                  | Kafka (publish), Postgres (`MarkQueued`)                                                  |
| Worker     | Kafka (priority topics)                | Postgres (`Claim`, `ExtendLease`, terminal transitions), Kafka (DLQ), Redis (dedup release) |
| Recovery   | Postgres (expired-lease scan)          | Postgres (`running` → `pending`)                                                          |

Every service exposes `/metrics`, scraped by Prometheus with the
`component` label coming from the scrape config.

## Concurrency

* **Multiple scheduler replicas.** `FetchDue` uses `SELECT ... FOR
  UPDATE SKIP LOCKED` inside a transaction; replicas claim disjoint
  batches without coordination.
* **Multiple worker replicas.** Standard Kafka consumer group —
  partitions balance automatically. Within a process, a fixed-size
  goroutine pool bounds parallelism.
* **Multiple recovery replicas.** `ReclaimExpiredLeases` is a CTE with
  `FOR UPDATE SKIP LOCKED`; same semantics as the scheduler.

## Delivery guarantees

* **API → Postgres → Kafka.** The API inserts (durable) before
  publishing. If publish fails, the row stays `pending`; the scheduler
  republishes on its next pass.
* **Kafka → Worker → Postgres.** The worker performs the DB transition
  *before* committing the Kafka offset. A crash between transition and
  commit causes one redelivery and one no-op `Claim`.
* **At-least-once with idempotency hatch.** Handlers that produce
  external side effects must be idempotent or use the stable job ID as
  an operation key. The deduplication index prevents enqueuing the same
  business-key job twice; it does not, by itself, prevent the same job
  from running its handler twice.

## Failure decision table

| Where                                       | Failure                       | Outcome                                                                          |
|---------------------------------------------|-------------------------------|----------------------------------------------------------------------------------|
| API publishes immediate job                 | Kafka unreachable             | Row stays `pending`; scheduler retries at next tick                              |
| Scheduler publishes then `MarkQueued` fails | DB blip                       | Row stays `pending`, message is on Kafka; worker accepts `Claim` on `pending`    |
| Worker crashes mid-handler                  | OOM, SIGKILL, network split   | Lease expires → recovery returns row to `pending` → another worker retries       |
| Handler returns error                       | Transient                     | Retry policy applies; `attempts` increments, `run_at` pushed forward             |
| Handler keeps failing                       | Permanent                     | After `maxAttempts`, row → `dead_lettered`, envelope → DLQ topic                  |
| DLQ publish fails after DB mark             | Kafka degradation             | Row remains `dead_lettered`; DLQ topic write is best-effort                      |

## Out of scope

* **A separate retry topic per delay class.** Postgres-based retry is
  simpler and gives arbitrary precision; the cost is one DB write per
  retry, which is well within target scale.
* **Cross-region replication.** Single-region deployment.
* **First-party dashboard UI.** Grafana with the provisioned dashboard
  is the operator surface.
* **Multi-tenant SaaS control plane.** Tenant-scoped rate limits are
  scaffolded (`internal/storage/redisrepo.RateLimit`) but not wired
  into a tenancy model.
