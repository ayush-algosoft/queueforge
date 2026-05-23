# QueueForge developer Makefile.

.PHONY: help build test test-race test-int up down logs ps psql redis tidy fmt vet

help:
	@echo "Targets:"
	@echo "  build       - go build ./..."
	@echo "  test        - unit tests"
	@echo "  test-race   - unit tests with -race"
	@echo "  test-int    - integration tests (requires \`make up\`)"
	@echo "  up          - docker compose up -d --build"
	@echo "  down        - docker compose down -v"
	@echo "  logs        - tail logs for all services"
	@echo "  ps          - docker compose ps"
	@echo "  psql        - open psql against the local postgres"
	@echo "  redis       - open redis-cli against the local redis"

build:
	go build ./...

test:
	go test ./internal/... -count=1

test-race:
	go test ./internal/... -count=1 -race

test-int:
	QF_INTEGRATION=1 go test ./tests/integration -count=1 -v

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

psql:
	docker compose exec postgres psql -U queueforge -d queueforge

redis:
	docker compose exec redis redis-cli

tidy:
	go mod tidy

fmt:
	gofmt -s -w .

vet:
	go vet ./...
