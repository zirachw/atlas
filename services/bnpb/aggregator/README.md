# BNPB Aggregator

Start the service using the [root README](../../../README.md). Configure source URLs, credentials, polling, and timeouts in the root `.env` using `.env.example`.

## API

Run from the repository root in Bash.

```sh
set -a
source .env
set +a

curl "http://localhost:${AGGREGATOR_PORT:-8083}/health"
curl -H "Authorization: Bearer $AGGREGATOR_INTERNAL_TOKEN" \
  "http://localhost:${AGGREGATOR_PORT:-8083}/internal/hazards?source=BMKG&limit=20"
```

`GET /internal/hazards` supports `source`, `hazard_type`, `severity`, inclusive `since=<RFC3339>`, and `limit` (1–500, default 100). Responses contain `data` and per-source freshness in `sources`.

## Checks

From the repository root.

```sh
make ci
make integration
```
