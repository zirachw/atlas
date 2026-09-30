package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryStore struct {
	mu         sync.Mutex
	checkpoint map[string]*time.Time
	events     map[string]HazardEvent
	commits    map[string]int
	failures   map[string]int
	failSave   bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{checkpoint: map[string]*time.Time{}, events: map[string]HazardEvent{}, commits: map[string]int{}, failures: map[string]int{}}
}
func (s *memoryStore) Checkpoint(_ context.Context, source string) (*time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint[source], nil
}
func (s *memoryStore) WarningCandidates(context.Context, int) ([]HazardEvent, error) { return nil, nil }
func (s *memoryStore) Failure(_ context.Context, source string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[source]++
	return nil
}
func (s *memoryStore) Save(_ context.Context, source string, events []HazardEvent, checkpoint *time.Time, _ time.Time) ([]HazardEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSave {
		return nil, errors.New("save failure")
	}
	created := []HazardEvent{}
	for _, h := range events {
		if _, ok := s.events[h.HazardID]; !ok {
			created = append(created, h)
		}
		s.events[h.HazardID] = h
	}
	s.checkpoint[source] = checkpoint
	s.commits[source]++
	return created, nil
}
func newPoller(source, base string, store IngestStore, pending chan Publication) *Poller {
	return &Poller{Source: source, BaseURL: base, Credential: "test-" + source, Interval: 10 * time.Millisecond, Timeout: 100 * time.Millisecond, Backoff: time.Millisecond, Retries: 1, Client: &http.Client{}, Store: store, Pending: pending, Log: testLog()}
}
func TestPollingCredentialsCheckpointAndCommitFailure(t *testing.T) {
	raw := fixture(t, "volcanic-v1.json")
	var since string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-PVMBG" || r.Header.Get("X-BMKG-Key") != "" || r.Header.Get("X-Correlation-ID") != "poll-id" {
			t.Error("credential or correlation mismatch")
		}
		since = r.URL.Query().Get("since")
		_ = json.NewEncoder(w).Encode(raw)
	}))
	defer server.Close()
	store := newMemoryStore()
	pending := make(chan Publication, 4)
	p := newPoller("PVMBG", server.URL, store, pending)
	if e := p.Poll(context.Background(), "poll-id"); e != nil {
		t.Fatal(e)
	}
	if len(pending) != 1 {
		t.Fatal("missing publication")
	}
	if e := p.Poll(context.Background(), "poll-id"); e != nil {
		t.Fatal(e)
	}
	if len(pending) != 1 || since != "2026-09-30T00:00:00Z" {
		t.Fatal("overlap or checkpoint failed", since)
	}
	store.failSave = true
	raw = fixture(t, "volcanic-v2.json")
	if e := p.Poll(context.Background(), "poll-id"); e == nil {
		t.Fatal("save failed silently")
	}
	if len(pending) != 1 || store.checkpoint["PVMBG"].Format(time.RFC3339) != "2026-09-30T00:00:00Z" {
		t.Fatal("publication or cursor preceded commit")
	}
}
func TestIndependentPollingDuringSlowSourceAndOutage(t *testing.T) {
	seismic := fixture(t, "seismic.json")
	volcanic := fixture(t, "volcanic-v1.json")
	var outage atomic.Bool
	outage.Store(true)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if outage.Load() {
			w.WriteHeader(503)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(80 * time.Millisecond):
		}
		_ = json.NewEncoder(w).Encode(volcanic)
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-BMKG-Key") != "test-BMKG" || r.Header.Get("Authorization") != "" {
			t.Error("BMKG credentials")
		}
		if r.URL.Path == "/tsunami-warnings" {
			w.Write([]byte("[]"))
			return
		}
		_ = json.NewEncoder(w).Encode(seismic)
	}))
	defer fast.Close()
	store := newMemoryStore()
	pending := make(chan Publication, 16)
	a := newPoller("BMKG", fast.URL, store, pending)
	b := newPoller("PVMBG", slow.URL, store, pending)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, p := range []*Poller{a, b} {
		wg.Add(1)
		go func(p *Poller) { defer wg.Done(); p.Run(ctx) }(p)
	}
	defer func() { cancel(); wg.Wait() }()
	waitFor(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.failures["PVMBG"] > 0 && store.commits["BMKG"] >= 3
	})
	outage.Store(false)
	waitFor(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.commits["PVMBG"] > 0 && store.commits["BMKG"] >= 6
	})
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func TestSourceBoundsRetriesAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		status, attempts int
	}{{"auth", "", 401, 1}, {"outage", "", 503, 2}, {"malformed", "{}", 200, 1}, {"null", "null", 200, 1}, {"oversize", string(make([]byte, MaxSourceBytes+1)), 200, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer s.Close()
			p := newPoller("PVMBG", s.URL, newMemoryStore(), make(chan Publication, 1))
			if _, e := p.fetch(context.Background(), "/volcanic-reports", nil, "id"); e == nil {
				t.Fatal("accepted failure")
			}
			if int(calls.Load()) != tc.attempts {
				t.Fatal(calls.Load())
			}
		})
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	p := newPoller("PVMBG", slow.URL, newMemoryStore(), make(chan Publication, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, e := p.fetch(ctx, "/volcanic-reports", nil, "id"); e == nil || time.Since(start) > time.Second {
		t.Fatal("cancellation failed")
	}
}
