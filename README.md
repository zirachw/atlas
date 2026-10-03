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
