# How to run QueueForge

A step-by-step walkthrough to bring up the whole stack, submit a job,
and verify everything works. No prior context needed — just follow the
steps in order.

---

## 1. Install prerequisites

You need **only one tool** to run the stack:

- **Docker Desktop** (Windows/macOS) or **Docker Engine + Docker Compose
  v2** (Linux).
  Verify it's installed and running:

  ```bash
  docker --version
  docker compose version
  ```

  Both commands should print a version. If `docker` errors with
  "Cannot connect to the Docker daemon", start Docker Desktop first.

The following are **optional** — only needed if you want to run the test
suite from your host machine:

- **Go 1.23 or newer** (`go version`)
- **curl** (almost always preinstalled; on Windows it ships with Git Bash and PowerShell)
- **Python 3** (only used to pretty-print JSON in some commands; you can
  skip the `| python -m json.tool` part if you don't have it)

---

## 2. Open a terminal in the project root

Open a terminal (PowerShell, Git Bash, Terminal.app, etc.) and `cd` into
the project root — the directory that contains `docker-compose.yml`,
`go.mod`, `README.md`, etc.

Verify you're in the right place:

```bash
ls
```

You should see `docker-compose.yml`, `cmd/`, `internal/`, `deployments/`,
and the rest of the project files.

---

## 3. Make sure the required ports are free

The stack publishes these host ports:

| Port  | Used by                       |
|-------|-------------------------------|
| 3000  | Grafana UI                    |
| 5432  | PostgreSQL                    |
| 6379  | Redis                         |
| 8080  | API (HTTP + `/metrics`)       |
| 9091  | Scheduler metrics             |
| 9092  | Kafka (host listener)         |
| 9093  | Recovery metrics              |
| 9094  | Prometheus UI                 |
| 9192  | Worker metrics                |

If any of those ports is already in use on your machine, stop whatever
is using it before continuing. On Windows you can check with:

```bash
netstat -ano | grep -E ":(3000|5432|6379|8080|909[1-4]|9092|9192) "
```

---

## 4. Bring the stack up

From the project root:

```bash
docker compose up -d --build
```

What happens:

- Docker downloads the base images (Postgres, Redis, Kafka, Prometheus,
  Grafana) — this can take a few minutes the **first** time only.
- The four QueueForge service images are built from the single
  `deployments/docker/Dockerfile`. This also takes a couple of minutes
  the first time.
- All nine containers start.

When the command finishes, you should see a line like:

```
Container queueforge-grafana      Started
```

---

## 5. Wait for the stack to become healthy

The infrastructure containers (Kafka, Postgres, Redis) have built-in
health checks. The QueueForge services depend on them. Wait until the
API reports ready:

```bash
curl -s http://localhost:8080/readyz
```

Expected output:

```json
{"status":"ready"}
```

If you get a connection error or `{"status":"db_unavailable"}`, wait
5–10 seconds and try again — infrastructure is still warming up.

Check that every container is up:

```bash
docker compose ps
```

All nine containers should show `Up` in the STATUS column. Kafka,
Postgres and Redis should also show `(healthy)`.

---

## 6. Submit your first job

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","jobType":"noop","priority":"P1","payload":{}}'
```

Expected output (with a different `jobId`):

```json
{"jobId":"4f8e...","state":"queued","runAt":"2026-05-23T07:00:00Z"}
```

Copy the `jobId` — you'll use it in the next step.

---

## 7. Verify the job completed

```bash
curl -s http://localhost:8080/v1/jobs/<paste-jobId-here>
```

Within a second, the response's `state` field should be `succeeded` and
you should see a `result` of `{"ok":true}`:

```json
{
  "id": "4f8e...",
  "state": "succeeded",
  "attempts": 1,
  "result": { "ok": true }
}
```

If you still see `pending` or `queued`, wait another second and try
again — but it should be effectively instant.

---

## 8. (Optional) Try the other demo job types

A delayed job (runs 5 seconds after submission):

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","jobType":"noop","priority":"P2","delaySeconds":5,"payload":{}}'
```

A job that always fails (drives the dead-letter queue):

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","jobType":"always_fail","priority":"P2","retryPolicy":{"maxAttempts":2,"backoff":"fixed","initialDelay":"500ms"},"payload":{}}'
```

After ~1 second, fetch it — `state` will be `dead_lettered`.

A duplicate-rejected job:

```bash
# Run the same command twice. The second submission returns HTTP 409.
curl -i -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","jobType":"noop","priority":"P3","deduplicationKey":"hello-once","payload":{}}'
```

---

## 9. View metrics and dashboards

**Prometheus** (raw metrics + ad-hoc PromQL):

Open <http://localhost:9094> in a browser. All four QueueForge targets
should be **UP** under `Status → Targets`.

**Grafana** (provisioned dashboard):

Open <http://localhost:3000>. Anonymous Admin access is enabled — no
login required. Navigate to:

> Dashboards → QueueForge → **QueueForge Overview**

You'll see panels for submission rate, processing rate, dead-letter
count, queue depth, oldest pending age, handler duration percentiles,
and outcome rate.

---

## 10. View queue statistics via the API

```bash
curl -s http://localhost:8080/v1/queues/stats
```

You'll get counts grouped by `(queue, priority, state)` and the
oldest-pending age per priority.

---

## 11. (Optional) Run the test suites

**Unit tests** (no infrastructure required):

```bash
go test ./internal/... -count=1
```

Expected: three `ok` lines, zero `FAIL`.

**Integration tests** (require the docker stack from step 4 to be
running):

```bash
QF_INTEGRATION=1 go test ./tests/integration -v -count=1
```

On Windows PowerShell, set the env var differently:

```powershell
$env:QF_INTEGRATION="1"; go test ./tests/integration -v -count=1
```

Expected: all four tests (`TestSubmitAndComplete`, `TestRetryAndDLQ`,
`TestDeduplication`, `TestDelayedJob`) print `--- PASS`.

---

## 12. View logs (if something looks wrong)

Tail all logs:

```bash
docker compose logs -f --tail=100
```

Tail one service:

```bash
docker compose logs -f api
docker compose logs -f worker
docker compose logs -f scheduler
docker compose logs -f recovery
```

Press `Ctrl+C` to stop tailing.

---

## 13. Shut everything down

When you're done:

```bash
docker compose down
```

To also delete the database and Kafka volumes (so the next `up` starts
with an empty Postgres and a fresh Kafka log):

```bash
docker compose down -v
```

---

## Common problems

**`port is already allocated`** during `docker compose up`
A host port from the table in step 3 is taken. Stop the conflicting
process or change the mapping in `docker-compose.yml`.

**`{"status":"db_unavailable"}` from `/readyz`**
Postgres is still starting. Wait 10 seconds and retry. If it persists,
check `docker compose logs postgres`.

**`failed to resolve reference "docker.io/..."`**
Network/DNS issue pulling an image. Check your internet connection and
that Docker Desktop can reach Docker Hub.

**Worker exits immediately**
Almost always a Kafka connectivity issue. Check
`docker compose logs kafka` for a healthy startup, then
`docker compose logs worker` for the connection error.

**Integration tests fail with `connection refused`**
The stack isn't up, or the API isn't ready yet. Run
`curl http://localhost:8080/readyz` first.

---

That's it. If `/readyz` returns `ready` and a `noop` job reaches
`succeeded`, the system is working end-to-end.
