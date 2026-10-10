#!/usr/bin/env bash
# Runs every *Integration test against disposable stores with freshly generated credentials.
set -euo pipefail
cd "$(dirname "$0")/.."

env_file=.env.test
compose=(docker compose --env-file "$env_file" -p atlas-integration -f compose.yaml -f compose.test.yaml)

./scripts/env.sh "$env_file" >/dev/null
trap '"${compose[@]}" down -v >/dev/null; rm -f "$env_file"' EXIT

"${compose[@]}" up -d --wait postgres rabbitmq bmkg-db pvmbg-db
pg=$("${compose[@]}" port postgres 5432)
amqp=$("${compose[@]}" port rabbitmq 5672)
bmkg=$("${compose[@]}" port bmkg-db 5432)
pvmbg=$("${compose[@]}" port pvmbg-db 5432)

set -a
source "$env_file"
set +a

export TEST_DATABASE_URL="postgres://aggregator:$CANONICAL_DB_PASSWORD@$pg/canonical?sslmode=disable"
export TEST_AUTH_CANONICAL_URL="postgres://auth:$AUTH_DB_PASSWORD@$pg/canonical?sslmode=disable"
export AUTH_TEST_DATABASE_URL="postgres://auth:$AUTH_DB_PASSWORD@$pg/auth?sslmode=disable"
export TEST_RABBITMQ_URL="amqp://atlas:$RABBITMQ_PASSWORD@$amqp/"
export CONSUMER_TEST_AMQP_URL="$TEST_RABBITMQ_URL"
export BMKG_TEST_DATABASE_URL="postgres://bmkg:$BMKG_DB_PASSWORD@$bmkg/bmkg?sslmode=disable"
export PVMBG_TEST_DATABASE_URL="postgres://pvmbg:$PVMBG_DB_PASSWORD@$pvmbg/pvmbg?sslmode=disable"

modules=(
  services/bmkg
  services/pvmbg
  services/bnpb/aggregator
  services/bnpb/auth
  consumers/consumer-a
  consumers/consumer-b
)

for module in "${modules[@]}"; do
  (cd "$module" && go test -race -count=1 -timeout=5m -run Integration -v ./internal/...)
done
