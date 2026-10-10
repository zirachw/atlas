package aggregator

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ingestion and reads have separate pools so polling cannot consume read capacity.
type Store struct{ ingest, query *pgxpool.Pool }

const columns = `hazard_id, source, source_ref_id, hazard_type, severity, area_name,
	latitude, longitude, occurred_at, ingested_at, attributes, revision`

//go:embed schema.sql
var schema string

func openPool(ctx context.Context, url string, size int32) (*pgxpool.Pool, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}

	c.MaxConns = size
	c.ConnConfig.ConnectTimeout = 2 * time.Second

	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
}

func OpenStore(ctx context.Context, url string, ingestSize, querySize int32) (*Store, error) {
	i, err := openPool(ctx, url, ingestSize)
	if err != nil {
		return nil, err
	}

	q, err := openPool(ctx, url, querySize)
	if err != nil {
		i.Close()
		return nil, err
	}

	s := &Store{i, q}
	if _, err = i.Exec(ctx, schema); err != nil {
		s.Close()
		return nil, err
	}
	// Persisted data survives restart, but freshness needs a new successful poll.
	const markStale = `UPDATE source_progress SET stale_since = COALESCE(stale_since, last_success_at, now())`
	if _, err = i.Exec(ctx, markStale); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

func (s *Store) Close() {
	s.ingest.Close()
	s.query.Close()
}

func (s *Store) Health(ctx context.Context) error { return s.query.Ping(ctx) }

func (s *Store) Checkpoint(ctx context.Context, source string) (*time.Time, error) {
	var t *time.Time

	err := s.ingest.QueryRow(ctx, `SELECT checkpoint FROM source_progress WHERE source = $1`, source).Scan(&t)

	return t, err
}

func (s *Store) Failure(ctx context.Context, source string, now time.Time) error {
	_, err := s.ingest.Exec(ctx, `
		UPDATE source_progress
		SET stale_since = COALESCE(stale_since, $2)
		WHERE source = $1`, source, now)

	return err
}

func scanEvent(row pgx.Row) (HazardEvent, error) {
	var (
		h   HazardEvent
		raw []byte
	)

	err := row.Scan(
		&h.HazardID,
		&h.Source,
		&h.SourceRefID,
		&h.HazardType,
		&h.Severity,
		&h.AreaName,
		&h.Latitude,
		&h.Longitude,
		&h.OccurredAt,
		&h.IngestedAt,
		&raw,
		&h.Revision,
	)
	if err == nil {
		err = json.Unmarshal(raw, &h.Attributes)
	}

	return h, err
}

func (s *Store) WarningEvents(ctx context.Context, refs []string) ([]HazardEvent, error) {
	rows, err := s.ingest.Query(ctx, `
		SELECT `+columns+`
		FROM hazard_events
		WHERE source = 'BMKG' AND source_ref_id = ANY($1)`, refs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []HazardEvent{}

	for rows.Next() {
		h, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, h)
	}

	return out, rows.Err()
}

func (s *Store) Save(ctx context.Context, source string, events []HazardEvent, checkpoint *time.Time, now time.Time) (
	[]HazardEvent,
	error,
) {
	tx, err := s.ingest.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	changed := []HazardEvent{}

	for _, h := range events {
		raw, err := json.Marshal(h.Attributes)
		if err != nil {
			return nil, err
		}
		// Compare persisted values atomically; ingestion time and IDs remain stable on updates.
		stored, err := scanEvent(tx.QueryRow(ctx, `
			INSERT INTO hazard_events (`+columns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1)
			ON CONFLICT (source, source_ref_id) DO UPDATE SET
				severity    = EXCLUDED.severity,
				area_name   = EXCLUDED.area_name,
				latitude    = EXCLUDED.latitude,
				longitude   = EXCLUDED.longitude,
				occurred_at = EXCLUDED.occurred_at,
				attributes  = EXCLUDED.attributes,
				revision    = hazard_events.revision + 1
			WHERE (hazard_events.severity, hazard_events.area_name, hazard_events.latitude,
				hazard_events.longitude, hazard_events.occurred_at, hazard_events.attributes)
			IS DISTINCT FROM (EXCLUDED.severity, EXCLUDED.area_name, EXCLUDED.latitude,
				EXCLUDED.longitude, EXCLUDED.occurred_at, EXCLUDED.attributes)
			RETURNING `+columns,
			h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
			h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw),
		))
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}

		if err != nil {
			return nil, err
		}

		changed = append(changed, stored)
	}

	_, err = tx.Exec(ctx, `
		UPDATE source_progress
		SET checkpoint = CASE WHEN $2::timestamptz IS NULL THEN checkpoint
				ELSE GREATEST(checkpoint, $2::timestamptz) END,
			last_success_at = $3,
			stale_since = NULL
		WHERE source = $1`, source, checkpoint, now)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return changed, nil
}

func (s *Store) Read(ctx context.Context, q Query) (Response, error) {
	out := Response{Data: []HazardEvent{}, Sources: []Freshness{}}
	// Snapshot data and freshness together so a concurrent ingestion cannot mix outcomes.
	tx, err := s.query.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT `+columns+`
		FROM hazard_events
		WHERE ($1 = '' OR source = $1)
			AND ($2 = '' OR hazard_type = $2)
			AND ($3 = '' OR severity = $3)
			AND ($4::timestamptz IS NULL OR occurred_at >= $4)
		ORDER BY occurred_at DESC, hazard_id
		LIMIT $5`, q.Source, q.HazardType, q.Severity, q.Since, q.Limit)
	if err != nil {
		return out, err
	}

	for rows.Next() {
		h, err := scanEvent(rows)
		if err != nil {
			rows.Close()
			return out, err
		}

		out.Data = append(out.Data, h)
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return out, err
	}

	for _, source := range sources(q) {
		f := Freshness{Source: source}

		var has bool

		err = tx.QueryRow(ctx, `
			SELECT last_success_at, stale_since, EXISTS (SELECT 1 FROM hazard_events WHERE source = $1)
			FROM source_progress
			WHERE source = $1`, source).Scan(&f.LastSuccessAt, &f.StaleSince, &has)
		if err != nil {
			return out, fmt.Errorf("freshness query: %w", err)
		}

		f.Status = "healthy"
		if f.LastSuccessAt == nil || f.StaleSince != nil {
			f.Status = "stale"
			if !has {
				f.Status = "unavailable"
			}
		}

		out.Sources = append(out.Sources, f)
	}

	return out, tx.Commit(ctx)
}
