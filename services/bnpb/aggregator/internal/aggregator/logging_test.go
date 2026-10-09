package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func assertDatabaseLog(t *testing.T, raw, operation, id, reason string) {
	t.Helper()
	for _, line := range strings.Split(raw, "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) != nil || entry["msg"] != "database_call" || entry["operation"] != operation {
			continue
		}
		if entry["service"] != "aggregator" || entry["dependency"] != "canonical_store" || entry["correlation_id"] != id || entry["reason"] != reason {
			t.Fatalf("incorrect database log: %v", entry)
		}
		if latency, ok := entry["latency_ms"].(float64); !ok || latency < 0 {
			t.Fatalf("missing or invalid latency: %v", entry)
		}
		return
	}
	t.Fatalf("missing %s database log in %s", operation, raw)
}

func TestAPIDatabaseCallsLogLatencyAndSafeFailures(t *testing.T) {
	for _, failed := range []bool{false, true} {
		var logs bytes.Buffer
		store := &readStub{result: Response{Data: []HazardEvent{}, Sources: []Freshness{}}}
		reason := ""
		if failed {
			store.err = errors.New("postgres://secret:password@host")
			reason = "store_error"
		}
		api := &API{Store: store, Token: "test-internal", Timeout: time.Second, Capacity: make(chan struct{}, 1), Log: slog.New(slog.NewJSONHandler(&logs, nil))}
		handler := api.Handler()
		for path, operation := range map[string]string{"/health": "health", "/internal/hazards": "read"} {
			logs.Reset()
			response := request(handler, path, "test-internal")
			want := http.StatusOK
			if failed {
				want = http.StatusServiceUnavailable
			}
			if response.Code != want {
				t.Fatalf("%s status %d, want %d", path, response.Code, want)
			}
			assertDatabaseLog(t, logs.String(), operation, "test-read", reason)
			if strings.Contains(logs.String(), "password") {
				t.Fatal("database credentials leaked into logs")
			}
		}
	}
}

func TestPollDatabaseCallsLogLatency(t *testing.T) {
	raw := fixture(t, "volcanic-v1.json")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(raw)
	}))
	defer server.Close()
	var logs bytes.Buffer
	store := newMemoryStore()
	poller := newPoller("PVMBG", server.URL, store, make(chan Publication, 4))
	poller.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := poller.Poll(context.Background(), "database-poll"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"checkpoint", "save"} {
		assertDatabaseLog(t, logs.String(), operation, "database-poll", "")
	}
	logs.Reset()
	store.failSave = true
	if err := poller.Poll(context.Background(), "database-failure"); err == nil {
		t.Fatal("expected save failure")
	}
	assertDatabaseLog(t, logs.String(), "save", "database-failure", "store_error")
}
