# verified_local_run.md

Captured during local verification of QueueForge on 2026-05-23.

* Host: Windows 11 (Docker Desktop)
* `go version`: `go1.26.3 windows/amd64`
* `docker --version`: `Docker version 27.3.1`
* `docker compose version`: `v2.29.7-desktop.1`

All outputs below are **real captured output** from the stack, not
fabricated examples.

---

## 1. Stack start-up

```text
$ docker compose up -d --build
...
Container queueforge-redis      Started
Container queueforge-postgres   Started
Container queueforge-kafka      Started (healthy)
Container queueforge-api        Started
Container queueforge-scheduler  Started
Container queueforge-worker     Started
Container queueforge-recovery   Started
Container queueforge-prometheus Started
Container queueforge-grafana    Started
```

```text
$ docker compose ps
NAME                    IMAGE                     STATUS                    PORTS
queueforge-api          queueforge-api            Up 10 seconds             0.0.0.0:8080->8080/tcp
queueforge-grafana      grafana/grafana:11.3.0    Up  9 seconds             0.0.0.0:3000->3000/tcp
queueforge-kafka        apache/kafka:3.8.0        Up 17 seconds (healthy)   0.0.0.0:9092->9092/tcp
queueforge-postgres     postgres:16-alpine        Up 17 seconds (healthy)   0.0.0.0:5432->5432/tcp
queueforge-prometheus   prom/prometheus:v2.55.1   Up 10 seconds             0.0.0.0:9094->9090/tcp
queueforge-recovery     queueforge-recovery       Up 10 seconds             0.0.0.0:9093->9093/tcp
queueforge-redis        redis:7-alpine            Up 17 seconds (healthy)   0.0.0.0:6379->6379/tcp
queueforge-scheduler    queueforge-scheduler      Up 10 seconds             0.0.0.0:9091->9091/tcp
queueforge-worker       queueforge-worker         Up 10 seconds             0.0.0.0:9192->9192/tcp
```

Health checks:

```text
$ curl -s http://localhost:8080/health
{"status":"ok"}

$ curl -s http://localhost:8080/readyz
{"status":"ready"}
```

---

## 2. Happy path — submit and complete

```text
$ curl -s -X POST http://localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -d '{"queue":"default","jobType":"noop","priority":"P1","payload":{}}'
{"jobId":"b6a73dd4-91ba-4792-b739-519902b8569b","state":"queued","runAt":"2026-05-23T06:50:49Z"}
```

After ~250ms the worker has processed it:

```text
$ curl -s http://localhost:8080/v1/jobs/b6a73dd4-91ba-4792-b739-519902b8569b
{
  "id": "b6a73dd4-91ba-4792-b739-519902b8569b",
  "queue": "default",
  "jobType": "noop",
  "priority": "P1",
  "state": "succeeded",
  "attempts": 1,
  "runAt": "2026-05-23T06:50:49.737921Z",
  "createdAt": "2026-05-23T06:50:49.739274Z",
  "completedAt": "2026-05-23T06:50:49.987834Z",
  "result": { "ok": true }
}
```

Latency end-to-end (submit → succeeded): **~250ms**.

---

## 3. Retry + DLQ flow

Submit an `always_fail` job with `maxAttempts=2`:

```text
$ curl -s -X POST http://localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -d '{"queue":"default","jobType":"always_fail","priority":"P2",
         "retryPolicy":{"maxAttempts":2,"backoff":"fixed","initialDelay":"500ms"},
         "payload":{}}'
{"jobId":"02569d55-eb48-4d05-90bb-80c5f30e50ed","state":"queued","runAt":"2026-05-23T06:51:01Z"}
```

After ~1.5s it has progressed through two attempts and been dead-lettered:

```text
$ curl -s http://localhost:8080/v1/jobs/02569d55-eb48-4d05-90bb-80c5f30e50ed
{
  "id": "02569d55-eb48-4d05-90bb-80c5f30e50ed",
  "state": "dead_lettered",
  "attempts": 2,
  "lastError": "always_fail handler: deliberate failure",
  "runAt": "2026-05-23T06:51:02.346845Z",
  "completedAt": "2026-05-23T06:51:02.385604Z"
}
```

The DLQ Kafka topic `qf.jobs.dlq` also received the envelope (visible in
the worker's processed_total{outcome="dead_lettered"} metric).

---

## 4. Deduplication

Two identical submissions, same `deduplicationKey`:

```text
$ KEY="dedup-1779519079116933100"

$ curl -s -w 'HTTP %{http_code}\n' -X POST http://localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -d "{\"queue\":\"default\",\"jobType\":\"sleep\",\"priority\":\"P3\",
         \"deduplicationKey\":\"$KEY\",\"payload\":{\"ms\":3000}}"
{"jobId":"89d4f288-8ddc-4006-8cc9-4e095a5e4530","state":"queued","runAt":"2026-05-23T06:51:19Z"}
HTTP 202

$ curl -s -w 'HTTP %{http_code}\n' -X POST http://localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -d "{\"queue\":\"default\",\"jobType\":\"sleep\",\"priority\":\"P3\",
         \"deduplicationKey\":\"$KEY\",\"payload\":{\"ms\":3000}}"
{"error":"Conflict","code":"duplicate","detail":"a job with this deduplication key is already in flight"}
HTTP 409
```

First submission accepted (`202`); duplicate within the active window
rejected (`409`).

---

## 5. Visibility-lease recovery (worker crash mid-execution)

Submit a 30-second `sleep` job:

```text
$ curl -s -X POST http://localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -d '{"queue":"default","jobType":"sleep","priority":"P2","payload":{"ms":30000}}'
{"jobId":"41b2a499-8ddc-44bb-b37e-e951d98d2e37","state":"queued",...}
```

After 2s the worker has claimed it:

```text
state=running attempts=1 claimed_by=worker-daa630ad
```

Now SIGKILL the worker container so the lease is orphaned with no
graceful shutdown:

```text
$ docker kill -s KILL queueforge-worker
```

Polling the job state — for 60s the lease is still considered valid:

```text
[+10s] state=running attempts=1 lastError=
[+20s] state=running attempts=1 lastError=
...
[+60s] state=running attempts=1 lastError=
```

Once the 60s lease expires and the recovery service runs its next 10s
scan, the row is flipped back to `pending` (and immediately republished
by the scheduler):

```text
[+70s] state=queued attempts=1 lastError= [reclaimed: lease expired]
```

Restart the worker; it picks up the redelivered message and completes the
sleep:

```text
$ docker compose up -d worker
...
[+10s] state=running
[+20s] state=running
[+30s] state=running
[+40s] state=succeeded
```

Final state confirms the second attempt completed cleanly:

```text
{
  "id": "41b2a499-8ddc-44bb-b37e-e951d98d2e37",
  "state": "succeeded",
  "attempts": 2,
  "lastError": " [reclaimed: lease expired]",
  "result": { "slept_ms": 30000 }
}
```

This exercises the full crash-recovery path:

1. Worker claims a job, holds a visibility lease.
2. Worker dies without releasing the lease.
3. Recovery service notices the expired lease (60s + scan latency) and
   returns the row to `pending`.
4. Scheduler republishes the job on its next scan.
5. A new worker picks it up and completes successfully on a second attempt.

---

## 6. Queue stats endpoint

```text
$ curl -s http://localhost:8080/v1/queues/stats | python -m json.tool
{
  "oldestPending": {},
  "states": [
    { "queue":"default", "priority":"P1", "state":"succeeded", "count":1 },
    { "queue":"default", "priority":"P2", "state":"dead_lettered", "count":1 },
    { "queue":"default", "priority":"P3", "state":"succeeded", "count":1 }
  ]
}
```

---

## 7. Prometheus + Grafana

All four service targets are healthy:

```text
$ curl -s http://localhost:9094/api/v1/targets
  queueforge-api:       up
  queueforge-scheduler: up
  queueforge-worker:    up
  queueforge-recovery:  up
```

Sample metric query:

```text
$ curl -s 'http://localhost:9094/api/v1/query?query=queueforge_jobs_processed_total'
  succeeded/P1     = 1
  dead_lettered/P2 = 1
  retried/P2       = 1
  succeeded/P3     = 1
```

Grafana is reachable at <http://localhost:3000> with the "QueueForge
Overview" dashboard provisioned automatically (anonymous Admin access).

---

## 8. Tests

```text
$ go test ./internal/... -count=1
ok  github.com/ayush-algosoft/queueforge/internal/handlers   0.778s
ok  github.com/ayush-algosoft/queueforge/internal/job        0.776s
ok  github.com/ayush-algosoft/queueforge/internal/kafka      0.753s
```

```text
$ QF_INTEGRATION=1 go test ./tests/integration -count=1 -v
=== RUN   TestSubmitAndComplete
--- PASS: TestSubmitAndComplete (0.02s)
=== RUN   TestRetryAndDLQ
--- PASS: TestRetryAndDLQ (0.76s)
=== RUN   TestDeduplication
--- PASS: TestDeduplication (0.01s)
=== RUN   TestDelayedJob
--- PASS: TestDelayedJob (2.77s)
PASS
ok      github.com/ayush-algosoft/queueforge/tests/integration  5.023s
```

Unit tests and integration tests all pass.

---

## 9. Bug found and fixed during verification

The first attempt at the visibility-recovery test surfaced a real bug:
the worker created a single 30s `context.WithTimeout` at the top of
`handle()` and used it for both the initial `Claim` and the terminal
`Succeeded`/`MarkPendingRetry`/`DeadLetter` UPDATEs. For long-running
handlers (anything > 30s) the context expired before the terminal DB
call, causing the worker to log:

```text
error="context deadline exceeded"
message="mark succeeded failed; will retry via redelivery"
```

…which then caused infinite reclaim → reattempt → infinite-loop until
maxAttempts was exhausted (effectively a bug, not a feature).

Fix: each DB call now gets its own short-lived `context.WithTimeout`
(`internal/worker/worker.go`, `dbCall` helper). Verified the same 30s
sleep test now succeeds on the second attempt as expected.
