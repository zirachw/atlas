package auth

import (
	"context"
	"crypto/sha256"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationSessionSurvivesAuthStoreRestart(t *testing.T) {
	url := os.Getenv("AUTH_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AUTH_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := OpenStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}

	session := newSessionID()
	oldHash := sha256.Sum256([]byte("integration-old-refresh"))
	newHash := sha256.Sum256([]byte("integration-new-refresh"))

	expires := time.Now().Add(time.Hour)
	if err := store.Create(ctx, session, "integration-client", oldHash[:], expires); err != nil {
		store.Close()
		t.Fatal(err)
	}

	store.Close()

	store, err = OpenStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if active, err := store.Validate(ctx, session, 1); err != nil || !active {
		t.Fatalf("session did not survive store reopen: active=%v err=%v", active, err)
	}

	client, version, err := store.Rotate(ctx, session, oldHash[:], newHash[:], expires)
	if err != nil || client != "integration-client" || version != 2 {
		t.Fatalf("rotate client=%q version=%d err=%v", client, version, err)
	}

	store.Close()

	store, err = OpenStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if active, err := store.Validate(ctx, session, 1); err != nil || active {
		t.Fatalf("old version active=%v err=%v", active, err)
	}

	if active, err := store.Validate(ctx, session, 2); err != nil || !active {
		t.Fatalf("new version active=%v err=%v", active, err)
	}

	if _, _, err := store.Rotate(ctx, session, oldHash[:], newHash[:], expires); err != ErrInvalidRefresh {
		t.Fatalf("old refresh replay error=%v", err)
	}

	_, _ = store.pool.Exec(ctx, `DELETE FROM sessions WHERE session_id=$1`, session)
}

func TestIntegrationConcurrentRefreshRevocationAndExpiry(t *testing.T) {
	url := os.Getenv("AUTH_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AUTH_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := OpenStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := newSessionID()
	old, replacement := sha256.Sum256([]byte("old")), sha256.Sum256([]byte("replacement"))

	expires := time.Now().Add(time.Hour)
	if err := s.Create(ctx, id, "integration-race", old[:], expires); err != nil {
		t.Fatal(err)
	}

	defer s.pool.Exec(context.Background(), `DELETE FROM sessions WHERE session_id=$1`, id)

	var (
		successes atomic.Int32
		wg        sync.WaitGroup
	)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, version, err := s.Rotate(ctx, id, old[:], replacement[:], expires)
			if err == nil {
				if version != 2 {
					t.Errorf("version=%d", version)
				}

				successes.Add(1)
			} else if err != ErrInvalidRefresh {
				t.Errorf("rotate=%v", err)
			}
		}()
	}

	wg.Wait()

	if successes.Load() != 1 {
		t.Fatalf("successful rotations=%d", successes.Load())
	}

	if active, err := s.Validate(ctx, id, 1); err != nil || active {
		t.Fatalf("old version active=%v err=%v", active, err)
	}
	// The losers presented the rotated-out token, so the session is revoked for every holder.
	if active, err := s.Validate(ctx, id, 2); err != nil || active {
		t.Fatalf("revoked active=%v err=%v", active, err)
	}

	if _, _, err := s.Rotate(ctx, id, replacement[:], old[:], expires); err != ErrInvalidRefresh {
		t.Fatalf("revoked refresh=%v", err)
	}

	const expire = `UPDATE sessions SET revoked_at = NULL, refresh_expires_at = now() - interval '1 second' WHERE session_id = $1`
	if _, err := s.pool.Exec(ctx, expire, id); err != nil {
		t.Fatal(err)
	}

	if active, err := s.Validate(ctx, id, 2); err != nil || active {
		t.Fatalf("expired session active=%v err=%v", active, err)
	}

	if _, _, err := s.Rotate(ctx, id, replacement[:], old[:], expires); err != ErrInvalidRefresh {
		t.Fatalf("expired refresh=%v", err)
	}
}
