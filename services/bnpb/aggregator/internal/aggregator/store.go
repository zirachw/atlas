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

//go:embed schema.sql
var schema string

// Ingestion and reads have separate pools so polling cannot consume read capacity.
type Store struct{ ingest, query *pgxpool.Pool }

func openPool(ctx context.Context, url string, size int32) (*pgxpool.Pool, error) {
	c, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, errors.New("invalid database configuration")
	}
	c.MaxConns = size
	c.ConnConfig.ConnectTimeout = 2 * time.Second
	p, e := pgxpool.NewWithConfig(ctx, c)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, e
	}
	return p, nil
}
func OpenStore(ctx context.Context, url string, ingestSize, querySize int32) (*Store, error) {
	i, e := openPool(ctx, url, ingestSize)
	if e != nil {
		return nil, e
	}
	q, e := openPool(ctx, url, querySize)
	if e != nil {
		i.Close()
		return nil, e
	}
	s := &Store{i, q}
	if _, e = i.Exec(ctx, schema); e != nil {
		s.Close()
		return nil, e
	}
	// Persisted data survives restart, but freshness needs a new successful poll.
	if _, e = i.Exec(ctx, "UPDATE source_progress SET stale_since=COALESCE(stale_since,last_success_at,now())"); e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Close()                           { s.ingest.Close(); s.query.Close() }
func (s *Store) Health(ctx context.Context) error { return s.query.Ping(ctx) }
func (s *Store) Checkpoint(ctx context.Context, source string) (*time.Time, error) {
	var t *time.Time
	e := s.ingest.QueryRow(ctx, "SELECT checkpoint FROM source_progress WHERE source=$1", source).Scan(&t)
	return t, e
}
func (s *Store) Failure(ctx context.Context, source string, now time.Time) error {
	_, e := s.ingest.Exec(ctx, "UPDATE source_progress SET stale_since=COALESCE(stale_since,$2) WHERE source=$1", source, now)
	return e
}

const columns = "hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes"

func scanEvent(row pgx.Row) (HazardEvent, error) {
	var h HazardEvent
	var raw []byte
	e := row.Scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity, &h.AreaName, &h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &raw)
	if e == nil {
		e = json.Unmarshal(raw, &h.Attributes)
	}
	return h, e
}
func (s *Store) WarningCandidates(ctx context.Context, max int) ([]HazardEvent, error) {
	rows, e := s.ingest.Query(ctx, "SELECT "+columns+" FROM hazard_events WHERE source='BMKG' AND attributes->>'potential_tsunami'='true' ORDER BY hazard_id LIMIT $1", max+1)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []HazardEvent{}
	for rows.Next() {
		h, e := scanEvent(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, h)
	}
	if len(out) > max {
		return nil, errors.New("warning reconciliation bound exceeded")
	}
	return out, rows.Err()
}
func (s *Store) Save(ctx context.Context, source string, events []HazardEvent, checkpoint *time.Time, now time.Time) ([]HazardEvent, error) {
	tx, e := s.ingest.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	created := []HazardEvent{}
	for _, h := range events {
		raw, e := json.Marshal(h.Attributes)
		if e != nil {
			return nil, e
		}
		var id string
		e = tx.QueryRow(ctx, "INSERT INTO hazard_events ("+columns+") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (source,source_ref_id) DO NOTHING RETURNING hazard_id", h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw)).Scan(&id)
		if e == nil {
			created = append(created, h)
		} else if errors.Is(e, pgx.ErrNoRows) {
			_, e = tx.Exec(ctx, "UPDATE hazard_events SET severity=$3,area_name=$4,latitude=$5,longitude=$6,occurred_at=$7,attributes=$8 WHERE source=$1 AND source_ref_id=$2", h.Source, h.SourceRefID, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, string(raw))
		}
		if e != nil {
			return nil, e
		}
	}
	_, e = tx.Exec(ctx, "UPDATE source_progress SET checkpoint=CASE WHEN $2::timestamptz IS NULL THEN checkpoint ELSE GREATEST(checkpoint,$2::timestamptz) END,last_success_at=$3,stale_since=NULL WHERE source=$1", source, checkpoint, now)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return created, nil
}
func (s *Store) Read(ctx context.Context, q Query) (Response, error) {
	out := Response{Data: []HazardEvent{}, Sources: []Freshness{}}
	// Snapshot data and freshness together so a concurrent ingestion cannot mix outcomes.
	tx, e := s.query.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, "SELECT "+columns+" FROM hazard_events WHERE ($1='' OR source=$1) AND ($2='' OR hazard_type=$2) AND ($3='' OR severity=$3) AND ($4::timestamptz IS NULL OR occurred_at >= $4) ORDER BY occurred_at DESC,hazard_id LIMIT $5", q.Source, q.HazardType, q.Severity, q.Since, q.Limit)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		h, e := scanEvent(rows)
		if e != nil {
			rows.Close()
			return out, e
		}
		out.Data = append(out.Data, h)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	for _, source := range sources(q) {
		f := Freshness{Source: source}
		var has bool
		e = tx.QueryRow(ctx, "SELECT last_success_at,stale_since,EXISTS(SELECT 1 FROM hazard_events WHERE source=$1) FROM source_progress WHERE source=$1", source).Scan(&f.LastSuccessAt, &f.StaleSince, &has)
		if e != nil {
			return out, fmt.Errorf("freshness query: %w", e)
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
