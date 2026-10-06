package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryStore struct {
	mu             sync.Mutex
	checkpoint     map[string]*time.Time
	events         map[string]HazardEvent
	commits        map[string]int
	failures       map[string]int
	failSave       bool
	warningLookups [][]string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{checkpoint: map[string]*time.Time{}, events: map[string]HazardEvent{}, commits: map[string]int{}, failures: map[string]int{}}
}
func (s *memoryStore) Checkpoint(_ context.Context, source string) (*time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint[source], nil
}
func (s *memoryStore) WarningEvents(_ context.Context, refs []string) ([]HazardEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warningLookups = append(s.warningLookups, refs)
	out := []HazardEvent{}
	for _, h := range s.events {
		if slices.Contains(refs, h.SourceRefID) {
			out = append(out, h)
		}
	}
	return out, nil
}
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

func TestSourceCallLogsDependencyAndSafeFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte("private source payload"))
	}))
	defer s.Close()
	var output bytes.Buffer
	p := newPoller("PVMBG", s.URL, newMemoryStore(), make(chan Publication, 1))
	p.Log = slog.New(slog.NewJSONHandler(&output, nil))
	if _, _, err := p.fetchOnce(context.Background(), "/volcanic-reports", nil, "poll-log"); err == nil {
		t.Fatal("accepted source failure")
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["dependency"] != "PVMBG" || entry["correlation_id"] != "poll-log" || entry["status"] != float64(503) || entry["reason"] != "source status 503" || entry["latency_ms"] == nil {
		t.Fatal(entry)
	}
	if strings.Contains(output.String(), p.Credential) || strings.Contains(output.String(), "private source payload") {
		t.Fatal("dependency log leaked credentials or payload")
	}
}
func TestPollRejectsBadRecordsWithoutStallingSource(t *testing.T) {
	valid := fixture(t, "volcanic-v1.json")[0]
	unknown := clone(valid)
	unknown["report_id"] = json.RawMessage(`"report-bromo"`)
	unknown["volcano_id"] = json.RawMessage(`"bromo"`)
	unknown["reported_at"] = json.RawMessage(`"2026-09-30T00:00:30Z"`)
	missingID := clone(valid)
	delete(missingID, "report_id")
	missingID["reported_at"] = json.RawMessage(`"2026-09-30T00:00:20Z"`)
	invalid := fixture(t, "invalid-volcanic.json")[0]
	invalid["report_id"] = json.RawMessage(`"report-invalid"`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]Object{valid, unknown, missingID, invalid, valid})
	}))
	defer server.Close()
	var output bytes.Buffer
	store := newMemoryStore()
	p := newPoller("PVMBG", server.URL, store, make(chan Publication, 4))
	p.Log = slog.New(slog.NewJSONHandler(&output, nil))
	if e := p.Poll(context.Background(), "poll-bad"); e != nil {
		t.Fatal("bad records failed the whole poll:", e)
	}
	if len(store.events) != 1 || store.failures["PVMBG"] != 0 {
		t.Fatal("valid record not committed", store.events)
	}
	// The newest record is rejected, yet the checkpoint moves past it.
	if store.checkpoint["PVMBG"].Format(time.RFC3339) != "2026-09-30T00:00:30Z" {
		t.Fatal("checkpoint stalled at", store.checkpoint["PVMBG"])
	}
	if got := strings.Count(output.String(), `"msg":"record_rejected"`); got != 4 {
		t.Fatalf("logged %d rejections, want 4: %s", got, output.String())
	}
	for _, want := range []string{`"source_ref_id":"report-bromo"`, `"reason":"unknown volcano"`, `"reason":"duplicate source ID"`, `"correlation_id":"poll-bad"`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("rejection log misses %s", want)
		}
	}
}
func TestWarningsUseLookbackAndLoadOnlyReferencedEvents(t *testing.T) {
	seismic := fixture(t, "seismic.json")
	good := fixture(t, "warnings.json")[0]
	bad := clone(good)
	bad["warning_id"] = json.RawMessage(`"warning-bad"`)
	bad["threat_level"] = json.RawMessage(`"Bahaya"`)
	var mu sync.Mutex
	var warningSince []string
	served, warnings := seismic, []Object{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/tsunami-warnings" {
			warningSince = append(warningSince, r.URL.Query().Get("since"))
			_ = json.NewEncoder(w).Encode(warnings)
			return
		}
		_ = json.NewEncoder(w).Encode(served)
	}))
	defer server.Close()
	store := newMemoryStore()
	p := newPoller("BMKG", server.URL, store, make(chan Publication, 16))
	if e := p.Poll(context.Background(), "first"); e != nil {
		t.Fatal(e)
	}
	if len(store.warningLookups) != 0 {
		t.Fatal("looked up stored events without any warning", store.warningLookups)
	}
	// A late warning for an already stored event, beside an invalid one and a duplicate.
	mu.Lock()
	served, warnings = []Object{}, []Object{good, bad, good}
	mu.Unlock()
	if e := p.Poll(context.Background(), "late"); e != nil {
		t.Fatal("an invalid warning failed the poll:", e)
	}
	if !slices.Equal(warningSince, []string{"", "2026-09-29T23:00:04Z"}) {
		t.Fatal("warnings must be fetched from one lookback before the checkpoint", warningSince)
	}
	if len(store.warningLookups) != 1 || !slices.Equal(store.warningLookups[0], []string{"quake-5"}) {
		t.Fatal("must load only events referenced by valid warnings", store.warningLookups)
	}
	if h := store.events[hazardID("BMKG", "quake-5")]; h.Severity != "AWAS" {
		t.Fatal("late warning not applied", h.Severity)
	}
}
