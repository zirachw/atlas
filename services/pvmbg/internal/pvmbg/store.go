package pvmbg

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

//go:embed fixtures/volcanic-reports.json
var seedReports []byte

const MinSeedRecords = 20

type Store interface {
	Add(ctx context.Context, r VolcanicReport) error
	Reports(ctx context.Context, since *time.Time) ([]VolcanicReport, error)
	SchemaVersion(ctx context.Context) (int, error)
	SetSchemaVersion(ctx context.Context, version int) error
	Health(ctx context.Context) error
}

type PostgresStore struct{ pool *pgxpool.Pool }

func OpenStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid PVMBG database URL")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("PVMBG database connection failed")
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("PVMBG database unavailable")
	}
	if _, err = pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, errors.New("PVMBG schema setup failed")
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close()                           { s.pool.Close() }
func (s *PostgresStore) Health(ctx context.Context) error { return s.pool.Ping(ctx) }

// Add ignores reports whose IDs are already stored, so seeding on every start is idempotent.
func (s *PostgresStore) Add(ctx context.Context, r VolcanicReport) error {
	if err := validate(r); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO volcanic_reports (report_id,volcano_id,alert_level,eruption_count_24h,ash_column_height_m,reported_at,confidence_level) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (report_id) DO NOTHING`,
		r.ReportID, r.VolcanoID, r.AlertLevel, r.EruptionCount24h, r.AshColumnHeightM, r.ReportedAt, r.ConfidenceLevel)
	return err
}

// Reports includes a report whose reported_at equals since.
func (s *PostgresStore) Reports(ctx context.Context, since *time.Time) ([]VolcanicReport, error) {
	rows, err := s.pool.Query(ctx, `SELECT report_id,volcano_id,alert_level,eruption_count_24h,ash_column_height_m,reported_at,confidence_level FROM volcanic_reports WHERE $1::timestamptz IS NULL OR reported_at >= $1 ORDER BY reported_at, report_id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []VolcanicReport{}
	for rows.Next() {
		var r VolcanicReport
		if err := rows.Scan(&r.ReportID, &r.VolcanoID, &r.AlertLevel, &r.EruptionCount24h, &r.AshColumnHeightM, &r.ReportedAt, &r.ConfidenceLevel); err != nil {
			return nil, err
		}
		r.ReportedAt = r.ReportedAt.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, `SELECT version FROM schema_state`).Scan(&v)
	return v, err
}

func (s *PostgresStore) SetSchemaVersion(ctx context.Context, version int) error {
	_, err := s.pool.Exec(ctx, `UPDATE schema_state SET version=$1`, version)
	return err
}

func seedRecords() ([]VolcanicReport, error) {
	var reports []VolcanicReport
	if err := json.Unmarshal(seedReports, &reports); err != nil {
		return nil, fmt.Errorf("volcanic seed: %w", err)
	}
	if len(reports) < MinSeedRecords {
		return nil, fmt.Errorf("seed has %d reports, need at least %d", len(reports), MinSeedRecords)
	}
	return reports, nil
}

// LoadFixtures stores the embedded seed reports, so every start serves at least MinSeedRecords historical reports.
func LoadFixtures(ctx context.Context, store Store) error {
	reports, err := seedRecords()
	if err != nil {
		return err
	}
	for _, r := range reports {
		if err := store.Add(ctx, r); err != nil {
			return fmt.Errorf("seed %s: %w", r.ReportID, err)
		}
	}
	return nil
}
