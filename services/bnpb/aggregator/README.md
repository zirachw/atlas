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

Run from the repository root:

```sh
make ci
make integration
make e2e
make e2e-p2
```

Full-stack tests cover ingestion, schema changes, protected reads, outage recovery, fanout and restart persistence. Results are saved under [evidence/](evidence/).

Persistence and publication are separate. A crash after commit can leave a stored event unpublished; the integration test verifies this M1 limitation.
