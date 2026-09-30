# Atlas

Disaster coordination for BMKG, PVMBG, and BNPB.

## Run

Install Docker with Compose. For local checks, install Go 1.27.1, GNU Make, and Bash. On Windows, use WSL.

From the repository root, copy the configuration and edit its values.

```sh
cp .env.example .env
```

Start the services.

```sh
make dev                 # foreground
# or
make start               # background
```

Compose currently runs Aggregator, PostgreSQL, and RabbitMQ. BMKG/PVMBG mocks are not included yet, so hazard data will be unavailable until they are connected. Set their URLs and credentials in `.env`. Aggregator uses port 8083 by default, configurable through `AGGREGATOR_PORT`.

```sh
curl http://localhost:8083/health
make logs
make stop                # preserves stored data
```

Rebuild Aggregator independently.

```sh
docker compose up -d --build --no-deps aggregator
```

## Checks

```sh
make setup
make ci
```
