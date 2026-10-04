# Atlas

Disaster coordination services for BMKG, PVMBG, and BNPB. M1 runs each component in its own container. Auth owns its session database, the Aggregator owns the canonical store, and clients retrieve hazard data through the Client API.

## Run

Install Docker with Compose, Go 1.27.1, GNU Make, and Bash. On Windows, use WSL. Copy and edit the local configuration:

```sh
cp .env.example .env
```

Generate an Ed25519 key pair and put both PEM files into `.env` as base64. The private key belongs only to Auth; the public key is used by Client API.

```sh
openssl genpkey -algorithm Ed25519 -out .auth-private.pem
openssl pkey -in .auth-private.pem -pubout -out .auth-public.pem
auth_private_b64=$(base64 < .auth-private.pem | tr -d '\n')
auth_public_b64=$(base64 < .auth-public.pem | tr -d '\n')
sed -i "s|^AUTH_ED25519_PRIVATE_KEY_B64=.*|AUTH_ED25519_PRIVATE_KEY_B64=$auth_private_b64|" .env
sed -i "s|^AUTH_ED25519_PUBLIC_KEY_B64=.*|AUTH_ED25519_PUBLIC_KEY_B64=$auth_public_b64|" .env
```

Start all services:

```sh
make dev                 # foreground
# or
make start               # background
curl http://localhost:8080/health
curl http://localhost:8084/health
make logs
make stop                # preserves database, broker, and consumer state volumes
```

The source mocks listen on `8081` and `8082`; the Client API listens on `8080`; Auth listens on `8084`. All host bindings use loopback. The Aggregator API is reachable only from the Compose network.

Set source credentials and behavior, client credentials, database and broker passwords, the Auth validation credential, and ports in `.env`. The three downstream client IDs and secrets must be distinct. Access tokens default to 60 seconds; refresh tokens default to 7 days. Never commit `.env` or the generated private key.

## Configuration

Every variable lives in `.env`, copied from `.env.example`. Compose passes each service only the values it needs.

| Variable | Used by | Meaning | Example default |
|---|---|---|---|
| `POSTGRES_ADMIN_PASSWORD` | PostgreSQL | Superuser password, used only to create the two owner roles. | required |
| `CANONICAL_DB_PASSWORD` | PostgreSQL, Aggregator | Password of the `aggregator` role that owns the Canonical Store. | required |
| `AUTH_DB_PASSWORD` | PostgreSQL, Auth | Password of the `auth` role that owns the Auth Store. | required |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | RabbitMQ, Aggregator, consumers | Broker credentials. | required |
| `BMKG_API_KEY` | BMKG, Aggregator | BMKG key sent in `X-BMKG-Key`. | required |
| `PVMBG_TOKEN` | PVMBG, Aggregator | PVMBG bearer token shaped `pvmbg.<key-id>.<secret>`, with a secret of at least 16 URL-safe characters. | required |
| `AGGREGATOR_INTERNAL_TOKEN` | Aggregator, Client API | Credential for the Aggregator's internal read API. | required |
| `MEDIA_CLIENT_ID`, `MEDIA_CLIENT_SECRET` | Auth | Media identity, summary fields only. | required |
| `FIELD_TEAM_CLIENT_ID`, `FIELD_TEAM_CLIENT_SECRET` | Auth | Field Team identity, all fields. | required |
| `INTERNAL_OPS_CLIENT_ID`, `INTERNAL_OPS_CLIENT_SECRET` | Auth | BNPB Internal Ops identity, all fields. | required |
| `AUTH_ED25519_PRIVATE_KEY_B64` | Auth | Base64 PKCS#8 PEM key that signs access tokens. | required |
| `AUTH_ED25519_PUBLIC_KEY_B64` | Client API | Base64 public key PEM that verifies access tokens. | required |
| `AUTH_VALIDATE_TOKEN` | Auth, Client API | Credential for `POST /auth/validate`. | required |
| `ACCESS_TOKEN_TTL`, `REFRESH_TOKEN_TTL` | Auth | Token lifetimes. | `60s`, `168h` |
| `BMKG_PORT`, `PVMBG_PORT`, `CLIENT_API_PORT`, `AUTH_PORT` | Compose | Loopback host ports. | `8081`, `8082`, `8080`, `8084` |
| `BMKG_RESPONSE_DELAY` | BMKG | Fixed response delay, 50ms to 150ms. | `100ms` |
| `PVMBG_MIN_RESPONSE_DELAY`, `PVMBG_MAX_RESPONSE_DELAY` | PVMBG | Random response delay range, within 500ms to 3s. | `500ms`, `3s` |
| `SOURCE_EVENT_INTERVAL` | BMKG, PVMBG | Time between generated events, 1s to 10s. | `10s` |
| `POLL_INTERVAL` | Aggregator | Time between polls of each source. | `3s` |
| `SOURCE_TIMEOUT`, `SOURCE_RETRIES`, `RETRY_BACKOFF` | Aggregator | Source call deadline, retry count, and first retry wait. | `4s`, `2`, `200ms` |
| `QUERY_TIMEOUT`, `QUERY_CAPACITY`, `QUERY_POOL_SIZE`, `INGEST_POOL_SIZE` | Aggregator | Read deadline, concurrent read limit, and database pool sizes. | `2s`, `64`, `8`, `2` |
| `PUBLISH_TIMEOUT`, `PUBLISH_RETRIES`, `PUBLISH_CAPACITY` | Aggregator | Broker confirm deadline, retry count, and pending publication buffer. | `3s`, `2`, `256` |
| `CLIENT_API_OUTBOUND_TIMEOUT` | Client API | Deadline for calls to Auth and the Aggregator. | `3s` |
| `CONSUMER_A_NAME`, `CONSUMER_A_QUEUE`, and the B and C equivalents | Consumers | Consumer name and durable queue. | `consumer-a`, `hazard.events.consumer-a` |

## Source mocks

BMKG (`8081`) and PVMBG (`8082`) are passive. Each starts with 24 deterministic seed records and generates one new record every `SOURCE_EVENT_INTERVAL`. Data lives in memory and resets when a mock restarts. The frozen contract details, such as the inclusive `since` boundary, are in `docs/architecture.md` section 3.

Load the example credentials into the shell first.

```sh
set -a; source .env; set +a
curl -H "X-BMKG-Key: $BMKG_API_KEY" 'http://localhost:8081/seismic-events?since=2026-09-29T12:00:00Z'
curl -H "X-BMKG-Key: $BMKG_API_KEY" http://localhost:8081/tsunami-warnings
curl -H "Authorization: Bearer $PVMBG_TOKEN" http://localhost:8082/volcanic-reports
curl http://localhost:8082/health      # shows the current schema version
```

### P1. Live schema change

Start the stack and let the Aggregator ingest the original schema. Then switch PVMBG to schema version 2 without restarting anything. Reports created after the switch carry `confidence_level`, and the Aggregator stores it in `attributes`. Earlier reports keep their original shape.

```sh
curl -X POST http://localhost:8082/admin/schema-version -d '{"version":2}'
curl -X POST http://localhost:8082/admin/schema-version -d '{"version":1}'   # stop adding the field
```

### P2. Slow source and outage

For the three-second delay, set `PVMBG_MIN_RESPONSE_DELAY=3s` and `PVMBG_MAX_RESPONSE_DELAY=3s` in `.env`, then recreate only PVMBG. Other containers keep running.

```sh
docker compose up -d pvmbg
```

Switch the PVMBG outage on and off at any time. Mode `error` returns 503 at once. Mode `hang` sends no response, like a dead host. Reports keep accumulating during the outage, and the Aggregator catches up after recovery.

```sh
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":true,"mode":"error"}'
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":true,"mode":"hang"}'
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":false}'
```

### P3. Separate agency credentials

Each mock rejects the other agency's credential.

```sh
curl -i -H "X-BMKG-Key: $PVMBG_TOKEN" http://localhost:8081/seismic-events                 # 403
curl -i -H "Authorization: Bearer $BMKG_API_KEY" http://localhost:8082/volcanic-reports    # 401
```

### Source smoke test

The script checks credentials, `since` filtering, delays, the schema switch, and both outage modes against the running mocks. It takes about 15 seconds and leaves PVMBG on schema version 1 without an outage.

```sh
./scripts/smoke-sources.sh
```

### P2. Load test

Install [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/), start the stack, and set the three-second PVMBG delay as described above. The test runs 40 BMKG-only and 20 volcanic virtual users for 90 seconds. Each user keeps its own connection, so 60 users hold 60 parallel connections. Each user also refreshes its access token before the 60-second TTL ends, so token expiry does not show up as errors.

```sh
set -a; source .env; set +a
CLIENT_ID=$INTERNAL_OPS_CLIENT_ID CLIENT_SECRET=$INTERNAL_OPS_CLIENT_SECRET k6 run loadtest/p2.js
```

The summary reports throughput, p50, p95, and p99 per request kind. It counts unexpected errors and 429 rejections separately. To confirm the parallel connections, count them on Linux while the test runs.

```sh
ss -tn state established '( sport = :8080 )' | tail -n +2 | wc -l
```

| Variable | Default | Meaning |
|---|---|---|
| `BMKG_VUS`, `VOLCANIC_VUS` | `40`, `20` | Virtual users per request kind. |
| `DURATION` | `90s` | Sustained test length. |
| `THINK_TIME` | `0` | Seconds each user waits between requests. |
| `BASE_URL`, `AUTH_URL` | `http://localhost:8080`, `http://localhost:8084` | Client API and Auth addresses. |
| `SUMMARY_JSON` | unset | File path for the full k6 summary. |

## Client API and Auth

Obtain Media credentials from `.env` and log in:

```sh
curl -s http://localhost:8084/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"media-demo","client_secret":"example-media-secret"}'
```

Use the returned access token to request summary data:

```sh
curl -H 'Authorization: Bearer <access-token>' \
  'http://localhost:8080/hazards?source=BMKG&limit=20'
```

Media receives summary fields only. An explicit raw request is rejected with `403`:

```sh
curl -i -H 'Authorization: Bearer <media-access-token>' \
  'http://localhost:8080/hazards?view=raw'
```

Field Team and Internal Ops credentials can request `view=raw`. When an access token expires, refresh it without logging in again:

```sh
curl -s http://localhost:8084/auth/refresh \
  -H 'Content-Type: application/json' \
  -d '{"refresh_token":"<refresh-token>"}'
```

Refresh rotates the refresh token and increments the session token version. Reusing the previous refresh token or access token fails. `GET /hazards` also accepts the Aggregator filters `source`, `hazard_type`, `severity`, `since`, and `limit`; it returns per-source freshness metadata in `sources`.

## Event consumer demonstration

Both consumers have separate durable queues and separate persistent idempotency files. Stop Consumer A while source events continue, and confirm Consumer B continues processing:

```sh
docker compose stop consumer-a
docker compose logs -f consumer-b aggregator
docker compose start consumer-a
docker compose logs -f consumer-a
```

To add a third subscriber without changing Aggregator code, start the optional Consumer C demo profile. It uses the Consumer B implementation with its own queue and persistent state volume. New queues receive events published after they are bound.

```sh
docker compose --profile consumer-c-demo up -d consumer-c-demo
docker compose logs -f consumer-c-demo
docker compose --profile consumer-c-demo stop consumer-c-demo
```

## Checks

```sh
make setup
make ci
make integration         # uses disposable PostgreSQL and RabbitMQ
```

See the [Aggregator README](services/bnpb/aggregator/README.md) for the internal API contract and examples.
