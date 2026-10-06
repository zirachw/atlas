package pvmbg

import (
	"bytes"
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

var authorized = map[string]string{"Authorization": "Bearer " + testToken}

var v1Fields = []string{"alert_level", "ash_column_height_m", "eruption_count_24h", "report_id", "reported_at", "volcano_id"}

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func testAPI(t *testing.T, delay time.Duration) *API {
	t.Helper()
	store, err := LoadSeed()
	if err != nil {
		t.Fatal(err)
	}
	return &API{Store: store, Controls: NewControls(), Token: testToken, Delay: func() time.Duration { return delay }, MaxHang: 5 * time.Second, Log: testLog()}
}

func request(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func decodeReports(t *testing.T, h http.Handler, path string) []map[string]json.RawMessage {
	t.Helper()
	w := request(h, http.MethodGet, path, "", authorized)
	var out []map[string]json.RawMessage
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &out) != nil {
		t.Fatalf("GET %s status %d body %s", path, w.Code, w.Body.String())
	}
	return out
}

func generate(t *testing.T, a *API, gen *Generator) VolcanicReport {
	t.Helper()
	r := gen.Next(time.Now(), a.Controls.SchemaVersion())
	if err := a.Store.Add(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCredentialDomains(t *testing.T) {
	h := testAPI(t, 0).Handler()
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"missing", nil, http.StatusUnauthorized},
		{"bmkg header", map[string]string{"X-BMKG-Key": "test-bmkg"}, http.StatusUnauthorized},
		{"bmkg key as bearer", map[string]string{"Authorization": "Bearer test-bmkg"}, http.StatusUnauthorized},
		{"empty bearer", map[string]string{"Authorization": "Bearer "}, http.StatusUnauthorized},
		{"other scheme", map[string]string{"Authorization": "Basic " + testToken}, http.StatusUnauthorized},
		{"token without scheme", map[string]string{"Authorization": testToken}, http.StatusUnauthorized},
		{"token with trailing space", map[string]string{"Authorization": "Bearer " + testToken + " "}, http.StatusUnauthorized},
		{"wrong secret", map[string]string{"Authorization": "Bearer pvmbg.demo.another-secret-value"}, http.StatusForbidden},
		{"wrong key ID", map[string]string{"Authorization": "Bearer pvmbg.other.test-pvmbg-secret-value"}, http.StatusForbidden},
		{"valid", authorized, http.StatusOK},
	} {
		if w := request(h, http.MethodGet, "/volcanic-reports", "", tc.headers); w.Code != tc.want {
			t.Fatalf("%s status %d, want %d", tc.name, w.Code, tc.want)
		}
	}
	if w := request(h, http.MethodGet, "/health", "", nil); w.Code != http.StatusOK {
		t.Fatalf("health status %d", w.Code)
	}
}

func TestResponsesMatchSpecFieldsAndTypes(t *testing.T) {
	h := testAPI(t, 0).Handler()
	for _, r := range decodeReports(t, h, "/volcanic-reports") {
		if got := slices.Sorted(maps.Keys(r)); !slices.Equal(got, v1Fields) {
			t.Fatalf("schema version 1 fields %v", got)
		}
		var id, volcano, level, reported string
		var ash float64
		var count json.Number
		d := json.NewDecoder(bytes.NewReader(r["eruption_count_24h"]))
		d.UseNumber()
		if json.Unmarshal(r["report_id"], &id) != nil || json.Unmarshal(r["volcano_id"], &volcano) != nil || json.Unmarshal(r["alert_level"], &level) != nil || json.Unmarshal(r["ash_column_height_m"], &ash) != nil || json.Unmarshal(r["reported_at"], &reported) != nil || d.Decode(&count) != nil {
			t.Fatalf("report types %v", r)
		}
		if _, err := count.Int64(); err != nil {
			t.Fatalf("eruption_count_24h must be an integer, got %s", count)
		}
		if _, err := time.Parse(time.RFC3339, reported); err != nil || !slices.Contains(alertLevels, level) {
			t.Fatalf("reported_at %q or alert_level %q invalid", reported, level)
		}
	}
}

func TestSinceQuery(t *testing.T) {
	a := testAPI(t, 0)
	h := a.Handler()
	all := a.Store.Reports(nil)
	pivot := all[7].ReportedAt
	count := func(path string) int { return len(decodeReports(t, h, path)) }
	if n := count("/volcanic-reports"); n != len(all) {
		t.Fatalf("without since: %d of %d", n, len(all))
	}
	inclusive := count("/volcanic-reports?since=" + pivot.Format(time.RFC3339))
	if inclusive != len(all)-7 {
		t.Fatalf("since at a report's time returned %d, want %d", inclusive, len(all)-7)
	}
	offset := url.QueryEscape(pivot.In(time.FixedZone("WIB", 7*3600)).Format(time.RFC3339))
	if n := count("/volcanic-reports?since=" + offset); n != inclusive {
		t.Fatalf("offset since returned %d, want %d", n, inclusive)
	}
	if n := count("/volcanic-reports?since=" + pivot.Add(time.Nanosecond).Format(time.RFC3339Nano)); n != inclusive-1 {
		t.Fatalf("since one nanosecond later returned %d, want %d", n, inclusive-1)
	}
	if w := request(h, http.MethodGet, "/volcanic-reports?since=2999-01-01T00:00:00Z", "", authorized); strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("empty result body %s", w.Body.String())
	}
	for _, query := range []string{"since=", "since=yesterday", "since=2026-09-29", "since=2026-09-29T00:00:00", "since=2026-09-29T00:00:00+07:00", "since=2026-09-29T00:00:00Z&since=2026-09-30T00:00:00Z"} {
		if w := request(h, http.MethodGet, "/volcanic-reports?"+query, "", authorized); w.Code != http.StatusBadRequest {
			t.Fatalf("?%s status %d", query, w.Code)
		}
	}
}

// P1 starts on the old schema, switches at runtime, and keeps serving valid reports without a restart.
func TestSchemaChangeAtRuntime(t *testing.T) {
	a := testAPI(t, 0)
	h := a.Handler()
	gen := NewGenerator(Seed, time.Now())
	before := generate(t, a, gen)
	if w := request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"version":2}` {
		t.Fatalf("switch to version 2: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(request(h, http.MethodGet, "/health", "", nil).Body.String(), `"schema_version":2`) {
		t.Fatal("health does not show the schema version")
	}
	after := generate(t, a, gen)
	// The inclusive since refetches the pre-switch report, so it must keep its original shape.
	reports := decodeReports(t, h, "/volcanic-reports?since="+before.ReportedAt.Format(time.RFC3339Nano))
	byID := map[string]map[string]json.RawMessage{}
	for _, r := range reports {
		var id string
		_ = json.Unmarshal(r["report_id"], &id)
		byID[id] = r
	}
	if got := slices.Sorted(maps.Keys(byID[before.ReportID])); !slices.Equal(got, v1Fields) {
		t.Fatalf("pre-switch report fields changed to %v", got)
	}
	var confidence float64
	if err := json.Unmarshal(byID[after.ReportID]["confidence_level"], &confidence); err != nil || confidence < 0 || confidence > 1 {
		t.Fatalf("post-switch confidence_level %s", byID[after.ReportID]["confidence_level"])
	}
	request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil)
	if w := request(h, http.MethodPost, "/admin/schema-version", `{"version":1}`, nil); w.Code != http.StatusOK {
		t.Fatalf("switch back to version 1 status %d", w.Code)
	}
	if generate(t, a, gen).ConfidenceLevel != nil {
		t.Fatal("version 1 still adds confidence_level")
	}
	for _, body := range []string{``, `{}`, `null`, `{"version":0}`, `{"version":3}`, `{"version":"2"}`, `{"version":2.5}`, `{"version":2,"extra":1}`, `{"version":2}{"version":1}`, `{"version":2,"pad":"` + strings.Repeat("x", 2048) + `"}`} {
		if w := request(h, http.MethodPost, "/admin/schema-version", body, nil); w.Code != http.StatusBadRequest {
			t.Fatalf("body %.40q status %d", body, w.Code)
		}
	}
	if a.Controls.SchemaVersion() != 1 {
		t.Fatal("a rejected body changed the schema version")
	}
}

func TestOutageErrorModeAndRecovery(t *testing.T) {
	a := testAPI(t, time.Hour)
	h := a.Handler()
	for _, body := range []string{`{}`, `{"mode":"error"}`, `{"enabled":"yes"}`, `{"enabled":true,"mode":"slow"}`} {
		if w := request(h, http.MethodPost, "/admin/outage", body, nil); w.Code != http.StatusBadRequest {
			t.Fatalf("body %q status %d", body, w.Code)
		}
	}
	if w := request(h, http.MethodPost, "/admin/outage", `{"enabled":true}`, nil); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"enabled":true,"mode":"error"}` {
		t.Fatalf("enable outage: %d %s", w.Code, w.Body.String())
	}
	// The one-hour delay proves the outage answers at once, before authentication and delay.
	for _, headers := range []map[string]string{authorized, nil} {
		start := time.Now()
		if w := request(h, http.MethodGet, "/volcanic-reports", "", headers); w.Code != http.StatusServiceUnavailable || time.Since(start) > time.Second {
			t.Fatalf("during outage status %d after %s", w.Code, time.Since(start))
		}
	}
	if w := request(h, http.MethodGet, "/health", "", nil); w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"mode":"error"`) {
		t.Fatalf("health during outage: %d %s", w.Code, w.Body.String())
	}
	if w := request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil); w.Code != http.StatusOK {
		t.Fatal("admin routes must keep working during an outage")
	}
	generated := generate(t, a, NewGenerator(Seed, time.Now()))
	if w := request(h, http.MethodPost, "/admin/outage", `{"enabled":false,"mode":"hang"}`, nil); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"enabled":false}` {
		t.Fatalf("disable outage: %d %s", w.Code, w.Body.String())
	}
	a.Delay = func() time.Duration { return 0 }
	reports := decodeReports(t, h, "/volcanic-reports?since="+generated.ReportedAt.Format(time.RFC3339Nano))
	if len(reports) != 1 {
		t.Fatal("a report generated during the outage is missing after recovery")
	}
	if w := request(h, http.MethodGet, "/health", "", nil); w.Code != http.StatusOK {
		t.Fatalf("health after recovery status %d", w.Code)
	}
}

func TestOutageHangModeSendsNoResponse(t *testing.T) {
	a := testAPI(t, 0)
	a.MaxHang = 300 * time.Millisecond
	server := httptest.NewServer(a.Handler())
	defer server.Close()
	post := func(body string) {
		resp, err := http.Post(server.URL+"/admin/outage", "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatal("outage control failed", err)
		}
		resp.Body.Close()
	}
	get := func(timeout time.Duration) (*http.Response, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		r, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/volcanic-reports", nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := server.Client().Do(r)
		if err == nil {
			resp.Body.Close()
		}
		return resp, err
	}
	post(`{"enabled":true,"mode":"hang"}`)
	// The client gives up first, as the Aggregator does at its source timeout.
	if resp, err := get(100 * time.Millisecond); err == nil {
		t.Fatalf("hang mode answered with status %d", resp.StatusCode)
	}
	// A client that waits longer is dropped at MaxHang, still without a response.
	start := time.Now()
	if resp, err := get(5 * time.Second); err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("hang mode resp=%v err=%v after %s", resp, err, time.Since(start))
	}
	post(`{"enabled":true,"mode":"error"}`)
	if resp, err := get(time.Second); err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("switching hang to error mode did not apply at once", err)
	}
	post(`{"enabled":false}`)
	if resp, err := get(time.Second); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatal("recovery did not apply at once", err)
	}
}

func TestDelayAppliesOnlyToAuthorizedReads(t *testing.T) {
	const delay = 80 * time.Millisecond
	h := testAPI(t, delay).Handler()
	start := time.Now()
	if w := request(h, http.MethodGet, "/volcanic-reports", "", authorized); w.Code != http.StatusOK || time.Since(start) < delay {
		t.Fatalf("status %d after %s", w.Code, time.Since(start))
	}
	for _, tc := range []struct{ method, path, body string }{{http.MethodGet, "/health", ""}, {http.MethodGet, "/volcanic-reports", ""}, {http.MethodPost, "/admin/schema-version", `{"version":1}`}, {http.MethodPost, "/admin/outage", `{"enabled":false}`}} {
		start := time.Now()
		request(h, tc.method, tc.path, tc.body, nil)
		if time.Since(start) >= delay {
			t.Fatalf("%s %s waited for the delay", tc.method, tc.path)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/volcanic-reports", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
		t.Fatal("a client that left during the delay still got a response")
	}
}

func TestRoutesAndCorrelation(t *testing.T) {
	h := testAPI(t, 0).Handler()
	for _, tc := range []struct{ method, path string }{{http.MethodPost, "/volcanic-reports"}, {http.MethodGet, "/admin/schema-version"}, {http.MethodGet, "/admin/outage"}} {
		if w := request(h, tc.method, tc.path, "", authorized); w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s status %d", tc.method, tc.path, w.Code)
		}
	}
	if w := request(h, http.MethodGet, "/volcanoes", "", authorized); w.Code != http.StatusNotFound {
		t.Fatalf("unknown route status %d", w.Code)
	}
	if w := request(h, http.MethodGet, "/volcanic-reports", "", map[string]string{"Authorization": "Bearer " + testToken, correlationHeader: "poll-1"}); w.Header().Get(correlationHeader) != "poll-1" {
		t.Fatal("valid correlation ID not echoed")
	}
	for _, id := range []string{"", "has space", strings.Repeat("a", 129)} {
		got := request(h, http.MethodGet, "/health", "", map[string]string{correlationHeader: id}).Header().Get(correlationHeader)
		if got == id || !validCorrelation(got) {
			t.Fatalf("correlation ID %q became %q", id, got)
		}
	}
}
