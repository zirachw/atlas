#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=canonical_password="$CANONICAL_DB_PASSWORD" --set=auth_password="$AUTH_DB_PASSWORD" <<'SQL'
CREATE ROLE aggregator LOGIN PASSWORD :'canonical_password';
CREATE ROLE auth LOGIN PASSWORD :'auth_password';

CREATE DATABASE canonical OWNER aggregator;
CREATE DATABASE auth OWNER auth;

REVOKE CONNECT ON DATABASE canonical FROM PUBLIC;
REVOKE CONNECT ON DATABASE auth FROM PUBLIC;
GRANT CONNECT ON DATABASE canonical TO aggregator;
GRANT CONNECT ON DATABASE auth TO auth;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname canonical <<'SQL'
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO aggregator;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname auth <<'SQL'
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO auth;
SQL
