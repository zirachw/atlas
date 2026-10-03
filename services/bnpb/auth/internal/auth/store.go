package auth

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var ErrInvalidRefresh = errors.New("invalid refresh token")

type SessionStore interface {
	Create(context.Context, string, string, []byte, time.Time) error
	Rotate(context.Context, string, []byte, []byte, time.Time) (string, int64, error)
	Validate(context.Context, string, int64) (bool, error)
	Health(context.Context) error
}

type Store struct{ pool *pgxpool.Pool }

func OpenStore(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid auth database URL")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("auth database connection failed")
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("auth database unavailable")
	}
	if _, err = pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, errors.New("auth session schema setup failed")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                           { s.pool.Close() }
func (s *Store) Health(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Create(ctx context.Context, sessionID, clientID string, hash []byte, expires time.Time) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO sessions(session_id,client_id,token_version,refresh_token_hash,refresh_expires_at) VALUES($1,$2,1,$3,$4)`, sessionID, clientID, hash, expires)
	return err
}

func (s *Store) Rotate(ctx context.Context, sessionID string, oldHash, newHash []byte, expires time.Time) (string, int64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback(ctx)
	var clientID string
	var version int64
	var storedHash []byte
	var revokedAt *time.Time
	var refreshExpires time.Time
	err = tx.QueryRow(ctx, `SELECT client_id,token_version,refresh_token_hash,refresh_expires_at,revoked_at FROM sessions WHERE session_id=$1 FOR UPDATE`, sessionID).Scan(&clientID, &version, &storedHash, &refreshExpires, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (revokedAt != nil || !refreshExpires.After(time.Now()) || len(storedHash) != len(oldHash) || subtle.ConstantTimeCompare(storedHash, oldHash) != 1) {
		return "", 0, ErrInvalidRefresh
	}
	if err != nil {
		return "", 0, err
	}
	version++
	_, err = tx.Exec(ctx, `UPDATE sessions SET token_version=$2,refresh_token_hash=$3,refresh_expires_at=$4 WHERE session_id=$1`, sessionID, version, newHash, expires)
	if err != nil {
		return "", 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", 0, err
	}
	return clientID, version, nil
}

func (s *Store) Validate(ctx context.Context, sessionID string, version int64) (bool, error) {
	var active bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id=$1 AND token_version=$2 AND revoked_at IS NULL AND refresh_expires_at > now())`, sessionID, version).Scan(&active)
	return active, err
}
