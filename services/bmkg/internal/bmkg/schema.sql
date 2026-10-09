CREATE TABLE IF NOT EXISTS seismic_events (
 event_id text PRIMARY KEY,
 magnitude double precision NOT NULL CHECK (magnitude >= 0),
 depth_km double precision NOT NULL CHECK (depth_km >= 0),
 epicenter_lat double precision NOT NULL CHECK (epicenter_lat BETWEEN -90 AND 90),
 epicenter_lon double precision NOT NULL CHECK (epicenter_lon BETWEEN -180 AND 180),
 region_name text NOT NULL,
 occurred_at timestamptz NOT NULL,
 potential_tsunami boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS seismic_events_time ON seismic_events (occurred_at, event_id);
CREATE TABLE IF NOT EXISTS tsunami_warnings (
 warning_id text PRIMARY KEY,
 related_event_id text NOT NULL REFERENCES seismic_events (event_id),
 threat_level text NOT NULL CHECK (threat_level IN ('Waspada','Siaga','Awas')),
 affected_zones text[] NOT NULL,
 estimated_arrival timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS tsunami_warnings_event ON tsunami_warnings (related_event_id);
