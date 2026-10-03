CREATE TABLE IF NOT EXISTS sessions (
    session_id uuid PRIMARY KEY,
    client_id text NOT NULL,
    token_version bigint NOT NULL CHECK (token_version > 0),
    refresh_token_hash bytea NOT NULL,
    refresh_expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sessions_client_id ON sessions (client_id);
