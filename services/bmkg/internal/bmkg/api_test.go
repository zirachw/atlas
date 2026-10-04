package bmkg

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

const testKey = "test-bmkg"

var authorized = map[string]string{"X-BMKG-Key": testKey}

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func testAPI(t *testing.T, delay time.Duration) *API {
	t.Helper()
	store, err := LoadSeed()
	if err != nil {
		t.Fatal(err)
	}
	return &API{Store: store, APIKey: testKey, Delay: delay, Log: testLog()}
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
		{"pvmbg bearer token", map[string]string{"Authorization": "Bearer pvmbg.demo.example-pvmbg-secret-only"}, http.StatusUnauthorized},
		{"pvmbg token as key", map[string]string{"X-BMKG-Key": "pvmbg.demo.example-pvmbg-secret-only"}, http.StatusForbidden},
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
		if got := slices.Sorted(maps.Keys(e)); !slices.Equal(got, []string{"depth_km", "epicenter_lat", "epicenter_lon", "event_id", "magnitude", "occurred_at", "potential_tsunami", "region_name"}) {
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
		if got := slices.Sorted(maps.Keys(w)); !slices.Equal(got, []string{"affected_zones", "estimated_arrival", "related_event_id", "threat_level", "warning_id"}) {
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

func TestSinceQuery(t *testing.T) {
	a := testAPI(t, 0)
	h := a.Handler()
	all := a.Store.Events(nil)
	pivot := all[5].OccurredAt
	count := func(path string) int {
		return len(decodeList(t, request(h, http.MethodGet, path, authorized)))
	}
	if n := count("/seismic-events"); n != len(all) {
		t.Fatalf("without since: %d of %d", n, len(all))
	}
	inclusive := count("/seismic-events?since=" + pivot.Format(time.RFC3339))
	if inclusive != len(all)-5 {
		t.Fatalf("since at a record's time returned %d, want %d", inclusive, len(all)-5)
	}
	// "+" must be percent-encoded in a query, or it decodes to a space.
	offset := url.QueryEscape(pivot.In(time.FixedZone("WIB", 7*3600)).Format(time.RFC3339))
	if n := count("/seismic-events?since=" + offset); n != inclusive {
		t.Fatalf("offset since returned %d, want %d", n, inclusive)
	}
	if n := count("/seismic-events?since=" + pivot.Add(time.Nanosecond).Format(time.RFC3339Nano)); n != inclusive-1 {
		t.Fatalf("since one nanosecond later returned %d, want %d", n, inclusive-1)
	}
	if w := request(h, http.MethodGet, "/seismic-events?since=2999-01-01T00:00:00Z", authorized); strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("empty result body %s", w.Body.String())
	}
	if n := count("/tsunami-warnings?since=" + pivot.Format(time.RFC3339)); n == 0 || n >= len(a.Store.Warnings(nil)) {
		t.Fatalf("warnings since pivot: %d", n)
	}
	for _, query := range []string{"since=", "since=yesterday", "since=2026-09-29", "since=2026-09-29T00:00:00", "since=2026-09-29T00:00:00+07:00", "since=2026-09-29T00:00:00Z&since=2026-09-30T00:00:00Z"} {
		for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
			if w := request(h, http.MethodGet, path+"?"+query, authorized); w.Code != http.StatusBadRequest {
				t.Fatalf("%s?%s status %d", path, query, w.Code)
			}
		}
	}
}

func TestNewEventsAppearAfterCheckpoint(t *testing.T) {
	a := testAPI(t, 0)
	h := a.Handler()
	seed := a.Store.Events(nil)
	checkpoint := seed[len(seed)-1].OccurredAt
	gen := NewGenerator(Seed, time.Now())
	for range 3 {
		event, warnings := gen.Next(time.Now())
		if err := a.Store.Add(event, warnings...); err != nil {
			t.Fatal(err)
		}
	}
	events := decodeList(t, request(h, http.MethodGet, "/seismic-events?since="+checkpoint.Format(time.RFC3339Nano), authorized))
	if len(events) != 4 || events[0]["event_id"] != seed[len(seed)-1].EventID {
		t.Fatalf("since the checkpoint returned %d events", len(events))
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
	if w := request(h, http.MethodGet, "/seismic-events", map[string]string{"X-BMKG-Key": testKey, correlationHeader: "poll-1"}); w.Header().Get(correlationHeader) != "poll-1" {
		t.Fatal("valid correlation ID not echoed")
	}
	for _, id := range []string{"", "has space", strings.Repeat("a", 129)} {
		got := request(h, http.MethodGet, "/health", map[string]string{correlationHeader: id}).Header().Get(correlationHeader)
		if got == id || !validCorrelation(got) {
			t.Fatalf("correlation ID %q became %q", id, got)
		}
	}
}
