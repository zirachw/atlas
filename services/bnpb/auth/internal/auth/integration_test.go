package auth

import (
	"context"
	"crypto/sha256"
	"os"
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
	session, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
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
