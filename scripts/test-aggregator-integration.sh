#!/usr/bin/env bash
# Uses disposable infrastructure and example credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
project="atlas-b-test-$$"
override=$(mktemp "$repo/.compose-test.XXXXXX.yaml")
cat > "$override" <<'YAML'
services:
  postgres:
    networks: [canonical, auth-store, test-host]
    ports:
      - target: 5432
        published: "0"
        host_ip: 127.0.0.1
  rabbitmq:
    ports:
      - target: 5672
        published: "0"
        host_ip: 127.0.0.1
networks:
  test-host:
YAML
compose=(docker compose --env-file "$repo/.env.example" -p "$project" -f "$repo/docker-compose.yml" -f "$override")
cleanup() { "${compose[@]}" down -v >/dev/null; rm -f "$override"; }
trap cleanup EXIT
"${compose[@]}" up -d --wait postgres rabbitmq
pg_port=$("${compose[@]}" port postgres 5432)
amqp_port=$("${compose[@]}" port rabbitmq 5672)
# Values come from the committed harmless test example, never from the user's .env.
set -a
source .env.example
set +a
export TEST_DATABASE_URL="postgres://aggregator:${CANONICAL_DB_PASSWORD}@${pg_port}/canonical?sslmode=disable"
export TEST_AUTH_CANONICAL_URL="postgres://auth:${AUTH_DB_PASSWORD}@${pg_port}/canonical?sslmode=disable"
export AUTH_TEST_DATABASE_URL="postgres://auth:${AUTH_DB_PASSWORD}@${pg_port}/auth?sslmode=disable"
export TEST_RABBITMQ_URL="amqp://${RABBITMQ_USER}:${RABBITMQ_PASSWORD}@${amqp_port}/"
export CONSUMER_TEST_AMQP_URL="$TEST_RABBITMQ_URL"
(cd services/bnpb/aggregator
go test -race -count=1 -timeout=5m -run Integration -v ./internal/aggregator)
(cd services/bnpb/auth
go test -race -count=1 -timeout=5m -run Integration -v ./internal/auth)
(cd consumers/consumer-a
go test -race -count=1 -timeout=5m -run Integration -v ./internal/consumer)
(cd consumers/consumer-b
go test -race -count=1 -timeout=5m -run Integration -v ./internal/consumer)
