CREATE TABLE IF NOT EXISTS hazard_events (
 hazard_id text PRIMARY KEY,
 source text NOT NULL CHECK (source IN ('BMKG','PVMBG')),
 source_ref_id text NOT NULL,
 hazard_type text NOT NULL CHECK (hazard_type IN ('SEISMIC','VOLCANIC')),
 severity text NOT NULL CHECK (severity IN ('NORMAL','WASPADA','SIAGA','AWAS')),
 area_name text NOT NULL,
 latitude double precision NOT NULL,
 longitude double precision NOT NULL,
 occurred_at timestamptz NOT NULL,
 ingested_at timestamptz NOT NULL,
 attributes jsonb NOT NULL CHECK (jsonb_typeof(attributes) = 'object'),
 UNIQUE (source, source_ref_id)
);
-- Existing rows represent the first published revision.
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0);
CREATE INDEX IF NOT EXISTS hazard_source_time ON hazard_events (source, occurred_at DESC, hazard_id);
CREATE INDEX IF NOT EXISTS hazard_time ON hazard_events (occurred_at DESC, hazard_id);
CREATE TABLE IF NOT EXISTS source_progress (
 source text PRIMARY KEY CHECK (source IN ('BMKG','PVMBG')),
 checkpoint timestamptz,
 last_success_at timestamptz,
 stale_since timestamptz
);
INSERT INTO source_progress(source) VALUES ('BMKG'),('PVMBG') ON CONFLICT DO NOTHING;
