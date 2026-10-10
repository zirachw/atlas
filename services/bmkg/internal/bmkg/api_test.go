package bmkg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// memoryStore stands in for PostgresStore. SQL filtering and ordering are covered by the store integration test.
type memoryStore struct {
	mu       sync.Mutex
	events   []SeismicEvent
	warnings []TsunamiWarning
	since    []*time.Time
	err      error
}

const (
	testKey    = "test-bmkg"
	pvmbgToken = "pvmbg.demo.example-pvmbg-secret-only"
)

var authorized = map[string]string{"X-BMKG-Key": testKey}

var (
	eventFields = []string{
		"depth_km", "epicenter_lat", "epicenter_lon", "event_id",
		"magnitude", "occurred_at", "potential_tsunami", "region_name",
	}
	warningFields = []string{"affected_zones", "estimated_arrival", "related_event_id", "threat_level", "warning_id"}
)

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func (m *memoryStore) Add(_ context.Context, e SeismicEvent, ws ...TsunamiWarning) error {
	if err := validate(e, ws); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.events = append(m.events, e)
	m.warnings = append(m.warnings, ws...)

	return nil
}

func (m *memoryStore) Events(_ context.Context, since *time.Time) ([]SeismicEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.since = append(m.since, since)

	return slices.Clone(m.events), m.err
}

func (m *memoryStore) Warnings(_ context.Context, since *time.Time) ([]TsunamiWarning, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.since = append(m.since, since)

	return slices.Clone(m.warnings), m.err
}

func (m *memoryStore) Health(context.Context) error { return m.err }

func (m *memoryStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.events)
}

func seededStore(t *testing.T) *memoryStore {
	t.Helper()

	m := &memoryStore{}
	if err := LoadSeed(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	return m
}

func testAPI(t *testing.T, delay time.Duration) *API {
	t.Helper()
	return &API{Store: seededStore(t), APIKey: testKey, Delay: delay, Log: testLog()}
}

func request(h http.Handler, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	return w
}

func decodeList(t *testing.T, w *httptest.ResponseRecorder) []map[string]any {
	t.Helper()

	var out []map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &out) != nil {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}

	return out
}

func TestCredentialDomains(t *testing.T) {
	h := testAPI(t, 0).Handler()

	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"missing", nil, http.StatusUnauthorized},
		{"empty key", map[string]string{"X-BMKG-Key": ""}, http.StatusUnauthorized},
		{"pvmbg bearer token", map[string]string{"Authorization": "Bearer " + pvmbgToken}, http.StatusUnauthorized},
		{"pvmbg token as key", map[string]string{"X-BMKG-Key": pvmbgToken}, http.StatusForbidden},
		{"key prefix", map[string]string{"X-BMKG-Key": testKey[:4]}, http.StatusForbidden},
		{"key with suffix", map[string]string{"X-BMKG-Key": testKey + "x"}, http.StatusForbidden},
		{"valid", authorized, http.StatusOK},
	} {
		for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
			if w := request(h, http.MethodGet, path, tc.headers); w.Code != tc.want {
				t.Fatalf("%s %s status %d, want %d", tc.name, path, w.Code, tc.want)
			}
		}
	}

	if w := request(h, http.MethodGet, "/health", nil); w.Code != http.StatusOK {
		t.Fatalf("health status %d", w.Code)
	}
}

func TestResponsesMatchSpecFieldsAndTypes(t *testing.T) {
	h := testAPI(t, 0).Handler()
	events := decodeList(t, request(h, http.MethodGet, "/seismic-events", authorized))
	ids := map[string]bool{}

	for _, e := range events {
		if got := slices.Sorted(maps.Keys(e)); !slices.Equal(got, eventFields) {
			t.Fatalf("seismic fields %v", got)
		}

		id, ok1 := e["event_id"].(string)
		_, ok2 := e["magnitude"].(float64)
		_, ok3 := e["depth_km"].(float64)
		_, ok4 := e["epicenter_lat"].(float64)
		_, ok5 := e["epicenter_lon"].(float64)
		_, ok6 := e["region_name"].(string)

		_, ok7 := e["potential_tsunami"].(bool)
		if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 || !ok7 || !isTimestamp(e["occurred_at"]) {
			t.Fatalf("seismic types %v", e)
		}

		ids[id] = e["potential_tsunami"].(bool)
	}

	for _, w := range decodeList(t, request(h, http.MethodGet, "/tsunami-warnings", authorized)) {
		if got := slices.Sorted(maps.Keys(w)); !slices.Equal(got, warningFields) {
			t.Fatalf("warning fields %v", got)
		}

		_, ok1 := w["warning_id"].(string)
		related, _ := w["related_event_id"].(string)
		level, _ := w["threat_level"].(string)

		zones, ok2 := w["affected_zones"].([]any)
		if !ok1 || !ok2 || !threatLevels[level] || !isTimestamp(w["estimated_arrival"]) {
			t.Fatalf("warning types %v", w)
		}

		for _, z := range zones {
			if _, ok := z.(string); !ok {
				t.Fatalf("affected_zones must hold strings: %v", zones)
			}
		}

		if potential, known := ids[related]; !known || !potential {
			t.Fatalf("warning relates to %q, which is not a stored potential tsunami", related)
		}
	}
}

func isTimestamp(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}

	_, err := time.Parse(time.RFC3339, s)

	return err == nil
}

func TestSinceIsParsedAndPassedToStore(t *testing.T) {
	a := testAPI(t, 0)
	store := a.Store.(*memoryStore)
	h := a.Handler()
	want := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	// "+" must be percent-encoded in a query, or it decodes to a space.
	for _, query := range []string{
		"",
		"since=2026-09-29T12:00:00Z",
		"since=" + url.QueryEscape("2026-09-29T19:00:00+07:00"),
		"since=2026-09-29T12:00:00.000000000Z",
	} {
		for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
			store.since = nil

			if w := request(h, http.MethodGet, path+"?"+query, authorized); w.Code != http.StatusOK {
				t.Fatalf("%s?%s status %d", path, query, w.Code)
			}

			got := store.since[0]
			if query == "" && got != nil || query != "" && (got == nil || !got.Equal(want)) {
				t.Fatalf("%s?%s passed since %v", path, query, got)
			}
		}
	}

	for _, query := range []string{
		"since=",
		"since=yesterday",
		"since=2026-09-29",
		"since=2026-09-29T00:00:00",
		"since=2026-09-29T00:00:00+07:00",
		"since=2026-09-29T00:00:00Z&since=2026-09-30T00:00:00Z",
	} {
		store.since = nil

		w := request(h, http.MethodGet, "/seismic-events?"+query, authorized)
		if w.Code != http.StatusBadRequest || len(store.since) != 0 {
			t.Fatalf("?%s status %d, store called %d times", query, w.Code, len(store.since))
		}
	}
}

func TestStoreFailureIsUnavailableAndLogged(t *testing.T) {
	var logs bytes.Buffer

	a := testAPI(t, 0)
	a.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	a.Store.(*memoryStore).err = errors.New("connection refused")
	h := a.Handler()

	headers := map[string]string{"X-BMKG-Key": testKey, correlationHeader: "poll-db"}
	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		w := request(h, http.MethodGet, path, headers)
		if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "store_unavailable") {
			t.Fatalf("%s with a failed store status %d body %s", path, w.Code, w.Body.String())
		}
	}

	if w := request(h, http.MethodGet, "/health", nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("health with a failed store status %d", w.Code)
	}

	for _, want := range []string{
		`"msg":"database_call"`,
		`"dependency":"bmkg_store"`,
		`"correlation_id":"poll-db"`,
		`"latency_ms":`,
		`"reason":"store_error"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("database call log misses %s", want)
		}
	}

	if strings.Contains(logs.String(), "connection refused") {
		t.Fatal("database error details leaked into the log")
	}
}

func TestDelayAppliesOnlyToAuthorizedDataRequests(t *testing.T) {
	const delay = 60 * time.Millisecond

	h := testAPI(t, delay).Handler()

	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		start := time.Now()
		if w := request(h, http.MethodGet, path, authorized); w.Code != http.StatusOK || time.Since(start) < delay {
			t.Fatalf("%s status %d after %s", path, w.Code, time.Since(start))
		}
	}

	for _, tc := range []struct {
		path    string
		headers map[string]string
	}{{"/health", nil}, {"/seismic-events", nil}} {
		start := time.Now()

		request(h, http.MethodGet, tc.path, tc.headers)

		if time.Since(start) >= delay {
			t.Fatalf("%s waited for the delay", tc.path)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/seismic-events", nil)
	r.Header.Set("X-BMKG-Key", testKey)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
		t.Fatal("a client that left during the delay still got a response")
	}
}

func TestRoutesAndCorrelation(t *testing.T) {
	h := testAPI(t, 0).Handler()
	if w := request(h, http.MethodPost, "/seismic-events", authorized); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status %d", w.Code)
	}

	if w := request(h, http.MethodGet, "/earthquakes", authorized); w.Code != http.StatusNotFound {
		t.Fatalf("unknown route status %d", w.Code)
	}

	headers := map[string]string{"X-BMKG-Key": testKey, correlationHeader: "poll-1"}

	w := request(h, http.MethodGet, "/seismic-events", headers)
	if w.Header().Get(correlationHeader) != "poll-1" {
		t.Fatal("valid correlation ID not echoed")
	}

	for _, id := range []string{"", "has space", strings.Repeat("a", 129)} {
		w := request(h, http.MethodGet, "/health", map[string]string{correlationHeader: id})

		got := w.Header().Get(correlationHeader)
		if got == id || !validCorrelation(got) {
			t.Fatalf("correlation ID %q became %q", id, got)
		}
	}
}

func TestUnknownRoutesAndMethodsAnswerJSON(t *testing.T) {
	h := testAPI(t, 0).Handler()

	for _, tc := range []struct {
		method, path string
		status       int
		code         string
	}{
		{http.MethodPost, "/seismic-events", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/missing", http.StatusNotFound, "not_found"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
			t.Fatalf("%s %s status %d body %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
