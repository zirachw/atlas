package bmkg

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Add(ctx context.Context, event SeismicEvent, warnings ...TsunamiWarning) error
	Events(ctx context.Context, since *time.Time) ([]SeismicEvent, error)
	Warnings(ctx context.Context, since *time.Time) ([]TsunamiWarning, error)
	Health(ctx context.Context) error
}

type PostgresStore struct{ pool *pgxpool.Pool }

const MinSeedRecords = 20

//go:embed schema.sql
var schema string

//go:embed seed/*.json
var seed embed.FS

func OpenStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid BMKG database URL")
	}

	cfg.MaxConns = 8
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("BMKG database connection failed")
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("BMKG database unavailable")
	}

	if _, err = pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, errors.New("BMKG schema setup failed")
	}

	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() { s.pool.Close() }

func (s *PostgresStore) Health(ctx context.Context) error { return s.pool.Ping(ctx) }

// Add ignores records whose IDs are already stored, so seeding on every start is idempotent.
func (s *PostgresStore) Add(ctx context.Context, event SeismicEvent, warnings ...TsunamiWarning) error {
	if err := validate(event, warnings); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx, `
		INSERT INTO seismic_events (
			event_id, magnitude, depth_km, epicenter_lat,
			epicenter_lon, region_name, occurred_at, potential_tsunami
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.Magnitude, event.DepthKM, event.EpicenterLat,
		event.EpicenterLon, event.RegionName, event.OccurredAt, event.PotentialTsunami,
	); err != nil {
		return err
	}

	for _, w := range warnings {
		if _, err = tx.Exec(ctx, `
			INSERT INTO tsunami_warnings (warning_id, related_event_id, threat_level, affected_zones, estimated_arrival)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (warning_id) DO NOTHING`,
			w.WarningID, w.RelatedEventID, w.ThreatLevel, w.AffectedZones, w.EstimatedArrival,
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// Events includes an event whose occurred_at equals since.
func (s *PostgresStore) Events(ctx context.Context, since *time.Time) ([]SeismicEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, magnitude, depth_km, epicenter_lat, epicenter_lon, region_name, occurred_at, potential_tsunami
		FROM seismic_events
		WHERE $1::timestamptz IS NULL OR occurred_at >= $1
		ORDER BY occurred_at, event_id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []SeismicEvent{}

	for rows.Next() {
		var e SeismicEvent
		if err := rows.Scan(
			&e.EventID, &e.Magnitude, &e.DepthKM, &e.EpicenterLat,
			&e.EpicenterLon, &e.RegionName, &e.OccurredAt, &e.PotentialTsunami,
		); err != nil {
			return nil, err
		}

		e.OccurredAt = e.OccurredAt.UTC()
		out = append(out, e)
	}

	return out, rows.Err()
}

// Warnings have no timestamp, so since applies to the related event's occurred_at.
func (s *PostgresStore) Warnings(ctx context.Context, since *time.Time) ([]TsunamiWarning, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.warning_id, w.related_event_id, w.threat_level, w.affected_zones, w.estimated_arrival
		FROM tsunami_warnings w
		JOIN seismic_events e ON e.event_id = w.related_event_id
		WHERE $1::timestamptz IS NULL OR e.occurred_at >= $1
		ORDER BY e.occurred_at, w.warning_id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []TsunamiWarning{}

	for rows.Next() {
		var w TsunamiWarning
		if err := rows.Scan(
			&w.WarningID, &w.RelatedEventID, &w.ThreatLevel, &w.AffectedZones, &w.EstimatedArrival,
		); err != nil {
			return nil, err
		}

		w.EstimatedArrival = w.EstimatedArrival.UTC()
		out = append(out, w)
	}

	return out, rows.Err()
}

func seedRecords() ([]SeismicEvent, map[string][]TsunamiWarning, error) {
	var (
		events   []SeismicEvent
		warnings []TsunamiWarning
	)

	files := map[string]any{
		"seed/seismic-events.json":   &events,
		"seed/tsunami-warnings.json": &warnings,
	}
	for name, dst := range files {
		raw, err := seed.ReadFile(name)
		if err != nil {
			return nil, nil, err
		}

		if err := json.Unmarshal(raw, dst); err != nil {
			return nil, nil, fmt.Errorf("%s: %w", name, err)
		}
	}

	if len(events) < MinSeedRecords {
		return nil, nil, fmt.Errorf("seed has %d seismic events, need at least %d", len(events), MinSeedRecords)
	}

	byEvent := map[string][]TsunamiWarning{}
	for _, w := range warnings {
		byEvent[w.RelatedEventID] = append(byEvent[w.RelatedEventID], w)
	}

	return events, byEvent, nil
}

// LoadSeed stores the embedded seed records, so every start serves at least MinSeedRecords historical events.
func LoadSeed(ctx context.Context, store Store) error {
	events, warnings, err := seedRecords()
	if err != nil {
		return err
	}

	for _, e := range events {
		if err := store.Add(ctx, e, warnings[e.EventID]...); err != nil {
			return fmt.Errorf("seed %s: %w", e.EventID, err)
		}
	}

	return nil
}
