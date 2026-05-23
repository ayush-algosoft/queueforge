# QueueForge — Architecture

This document expands on the lifecycle of a single job and the contract
between services. It assumes you have read the project `README.md`.

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
 │ succeeded │      │ dead_letter… │         │ visibility-lease │
 └───────────┘      └──────────────┘         │  expired → back  │
                                              │     to pending   │
                                              └──────────────────┘
```

* **pending** — durably stored, not yet on Kafka.
* **queued** — published to a priority topic, not yet claimed.
* **running** — claimed by a worker; visibility lease active.
* **succeeded** / **failed** / **dead_lettered** / **cancelled** — terminal.

State transitions are guarded by both a CHECK constraint in PostgreSQL
(invalid string values are rejected) and the predicate on each UPDATE
statement (e.g. `Claim` only matches `state IN ('pending','queued')`).

## Service responsibilities

| Service    | Reads from                                      | Writes to                                                                  |
|------------|--------------------------------------------------|----------------------------------------------------------------------------|
| API        | Redis (dedup pre-check, rate limit)              | Postgres (insert), Kafka (immediate publish if `run_at <= now`)            |
| Scheduler  | Postgres (`FetchDue`)                            | Kafka (publish), Postgres (`MarkQueued`)                                   |
| Worker     | Kafka (priority topics)                          | Postgres (`Claim`, `ExtendLease`, `Succeeded`/`MarkPendingRetry`/`DeadLetter`), Kafka (DLQ on terminal failure), Redis (dedup release on terminal state) |
| Recovery   | Postgres (`ReclaimExpiredLeases`)                | Postgres (transition `running` → `pending`)                                |

Every service exposes `/metrics`, and they all share the same Prometheus
registry layout, so the dashboard can aggregate per-component metrics with
the `component` label coming from the scrape config.

## Concurrency model

* **Multiple scheduler replicas.** `FetchDue` uses `SELECT ... FOR UPDATE
  SKIP LOCKED` inside a transaction, so two schedulers running side-by-side
  will claim disjoint batches without coordination.
* **Multiple worker replicas.** Standard Kafka consumer group — partitions
  are balanced automatically. Within a process, a fixed-size goroutine pool
  bounds parallelism per worker.
* **Multiple recovery replicas.** `ReclaimExpiredLeases` is a CTE with
  `FOR UPDATE SKIP LOCKED`; same semantics as the scheduler.

## Delivery guarantees

* **API → Postgres → Kafka:** the API inserts the row (durable) before
  publishing. If publish fails, the row stays `pending`; the scheduler
  retries publication on its next pass.
* **Kafka → Worker → Postgres:** the worker performs the DB transition
  *before* it commits the Kafka offset. A crash between transition and
  commit causes one redelivery and one no-op `Claim` (which returns
  "already terminal/claimed").
* **At-least-once with idempotency hatch.** Handlers that perform
  external side effects (charging a card, sending an email) must be
  idempotent or use the job's stable ID as the operation key. The
  deduplication index ensures a *new* job with the same business key won't
  be enqueued twice, but it does not by itself prevent a single job from
  running its handler twice.

## Failure handling decision table

| Where                                      | Failure                          | Outcome                                                                                          |
|--------------------------------------------|----------------------------------|--------------------------------------------------------------------------------------------------|
| API publishes immediate job to Kafka       | Kafka unreachable                | Row stays `pending`; scheduler picks it up at next tick                                          |
| Scheduler publishes, then `MarkQueued` fails | DB blip                        | Row stays `pending`, message is on Kafka; worker accepts `Claim` on `pending` rows               |
| Worker crashes mid-handler                 | OOM, kill -9, network partition  | Lease expires → recovery flips row to `pending` → scheduler republishes → another worker retries |
| Handler returns error                      | Transient                        | Retry policy applies; `attempts` increments, `run_at` pushed forward                              |
| Handler keeps failing                      | Permanent                        | After `maxAttempts`, row → `dead_lettered`, envelope → DLQ topic                                  |
| DLQ publish fails after DB mark            | Kafka degradation               | Row remains `dead_lettered`; operator can re-publish from DB                                     |

## What is intentionally out of scope

* **A separate retry topic per delay class.** Postgres-based retry is
  simpler, gives arbitrary precision, and removes the need to size N retry
  topics. The trade-off is that retries hit Postgres write throughput — not
  a problem at the scales this design targets (low thousands/second).
* **Cross-region replication.** Active-active Kafka and Postgres are out of
  scope. The system assumes a single regional deployment.
* **A first-party dashboard UI.** Grafana is good enough and ships with
  the stack. The instructions explicitly de-prioritise frontend polish.
* **A managed control plane / multi-tenant SaaS layer.** The platform is
  an internal infrastructure component, not a SaaS product. Tenant-scoped
  rate limits and per-queue settings are scaffolded (`internal/storage/
  redisrepo.RateLimit`) but not wired into a tenancy model.
