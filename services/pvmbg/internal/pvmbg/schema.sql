CREATE TABLE IF NOT EXISTS volcanic_reports (
 report_id text PRIMARY KEY,
 volcano_id text NOT NULL,
 alert_level text NOT NULL CHECK (alert_level IN ('Normal','Waspada','Siaga','Awas')),
 eruption_count_24h integer NOT NULL CHECK (eruption_count_24h >= 0),
 ash_column_height_m double precision NOT NULL CHECK (ash_column_height_m >= 0),
 reported_at timestamptz NOT NULL,
 confidence_level double precision CHECK (confidence_level BETWEEN 0 AND 1)
);
CREATE INDEX IF NOT EXISTS volcanic_reports_time ON volcanic_reports (reported_at, report_id);
CREATE TABLE IF NOT EXISTS schema_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 version integer NOT NULL DEFAULT 1 CHECK (version IN (1, 2))
);
INSERT INTO schema_state DEFAULT VALUES ON CONFLICT DO NOTHING;
