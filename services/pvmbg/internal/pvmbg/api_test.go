package pvmbg

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
	mu      sync.Mutex
	reports []VolcanicReport
	version int
	since   []*time.Time
	err     error
}

var authorized = map[string]string{"Authorization": "Bearer " + testToken}

var v1Fields = []string{
	"alert_level", "ash_column_height_m", "eruption_count_24h",
	"report_id", "reported_at", "volcano_id",
}

func bearer(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func (m *memoryStore) Add(_ context.Context, r VolcanicReport) error {
	if err := validate(r); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.reports = append(m.reports, r)

	return nil
}

func (m *memoryStore) Reports(_ context.Context, since *time.Time) ([]VolcanicReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.since = append(m.since, since)

	return slices.Clone(m.reports), m.err
}

func (m *memoryStore) SchemaVersion(context.Context) (int, error) { return m.version, m.err }

func (m *memoryStore) SetSchemaVersion(_ context.Context, v int) error {
	if m.err != nil {
		return m.err
	}

	m.version = v

	return nil
}

func (m *memoryStore) Health(context.Context) error { return m.err }

func (m *memoryStore) snapshot() []VolcanicReport {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.reports)
}

func (m *memoryStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.reports)
}

func testAPI(t *testing.T, delay time.Duration) *API {
	t.Helper()

	store := &memoryStore{version: 1}
	if err := LoadSeed(context.Background(), store); err != nil {
		t.Fatal(err)
	}

	return &API{
		Store:    store,
		Controls: NewControls(1),
		Token:    testToken,
		Delay:    func() time.Duration { return delay },
		MaxHang:  5 * time.Second,
		Log:      testLog(),
	}
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
	if err := a.Store.Add(context.Background(), r); err != nil {
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
		{"token with trailing space", bearer(testToken + " "), http.StatusUnauthorized},
		{"wrong secret", bearer("pvmbg.demo.another-secret-value"), http.StatusForbidden},
		{"wrong key ID", bearer("pvmbg.other.test-pvmbg-secret-value"), http.StatusForbidden},
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

		var (
			id, volcano, level, reported string
			ash                          float64
			count                        json.Number
		)

		d := json.NewDecoder(bytes.NewReader(r["eruption_count_24h"]))
		d.UseNumber()

		if json.Unmarshal(r["report_id"], &id) != nil ||
			json.Unmarshal(r["volcano_id"], &volcano) != nil ||
			json.Unmarshal(r["alert_level"], &level) != nil ||
			json.Unmarshal(r["ash_column_height_m"], &ash) != nil ||
			json.Unmarshal(r["reported_at"], &reported) != nil ||
			d.Decode(&count) != nil {
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
		store.since = nil

		if w := request(h, http.MethodGet, "/volcanic-reports?"+query, "", authorized); w.Code != http.StatusOK {
			t.Fatalf("?%s status %d", query, w.Code)
		}

		got := store.since[0]
		if query == "" && got != nil || query != "" && (got == nil || !got.Equal(want)) {
			t.Fatalf("?%s passed since %v", query, got)
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

		w := request(h, http.MethodGet, "/volcanic-reports?"+query, "", authorized)
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
	headers := bearer(testToken)
	headers[correlationHeader] = "poll-db"

	w := request(h, http.MethodGet, "/volcanic-reports", "", headers)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "store_unavailable") {
		t.Fatalf("read with a failed store status %d body %s", w.Code, w.Body.String())
	}

	w = request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil)
	if w.Code != http.StatusServiceUnavailable || a.Controls.SchemaVersion() != 1 {
		t.Fatalf("schema switch with a failed store status %d, version %d", w.Code, a.Controls.SchemaVersion())
	}

	if w := request(h, http.MethodGet, "/health", "", nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("health with a failed store status %d", w.Code)
	}

	for _, want := range []string{
		`"msg":"database_call"`,
		`"dependency":"pvmbg_store"`,
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

// P1 starts on the old schema, switches at runtime, and keeps serving valid reports without a restart.
func TestSchemaChangeAtRuntime(t *testing.T) {
	a := testAPI(t, 0)
	h := a.Handler()
	gen := NewGenerator(Seed, time.Now())
	before := generate(t, a, gen)

	w := request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"version":2}` {
		t.Fatalf("switch to version 2: %d %s", w.Code, w.Body.String())
	}

	if !strings.Contains(request(h, http.MethodGet, "/health", "", nil).Body.String(), `"schema_version":2`) {
		t.Fatal("health does not show the schema version")
	}

	if v, _ := a.Store.SchemaVersion(context.Background()); v != 2 {
		t.Fatalf("stored schema version %d, so a restart would lose the switch", v)
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

	err := json.Unmarshal(byID[after.ReportID]["confidence_level"], &confidence)
	if err != nil || confidence < 0 || confidence > 1 {
		t.Fatalf("post-switch confidence_level %s", byID[after.ReportID]["confidence_level"])
	}

	request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil)

	if w := request(h, http.MethodPost, "/admin/schema-version", `{"version":1}`, nil); w.Code != http.StatusOK {
		t.Fatalf("switch back to version 1 status %d", w.Code)
	}

	if generate(t, a, gen).ConfidenceLevel != nil {
		t.Fatal("version 1 still adds confidence_level")
	}

	for _, body := range []string{
		``,
		`{}`,
		`null`,
		`{"version":0}`,
		`{"version":3}`,
		`{"version":"2"}`,
		`{"version":2.5}`,
		`{"version":2,"extra":1}`,
		`{"version":2}{"version":1}`,
		`{"version":2,"pad":"` + strings.Repeat("x", 2048) + `"}`,
	} {
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

	w := request(h, http.MethodPost, "/admin/outage", `{"enabled":true}`, nil)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"enabled":true,"mode":"error"}` {
		t.Fatalf("enable outage: %d %s", w.Code, w.Body.String())
	}
	// The one-hour delay proves the outage answers at once, before authentication and delay.
	for _, headers := range []map[string]string{authorized, nil} {
		start := time.Now()

		w := request(h, http.MethodGet, "/volcanic-reports", "", headers)
		if w.Code != http.StatusServiceUnavailable || time.Since(start) > time.Second {
			t.Fatalf("during outage status %d after %s", w.Code, time.Since(start))
		}
	}

	w = request(h, http.MethodGet, "/health", "", nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"mode":"error"`) {
		t.Fatalf("health during outage: %d %s", w.Code, w.Body.String())
	}

	if w := request(h, http.MethodPost, "/admin/schema-version", `{"version":2}`, nil); w.Code != http.StatusOK {
		t.Fatal("admin routes must keep working during an outage")
	}

	generated := generate(t, a, NewGenerator(Seed, time.Now()))

	w = request(h, http.MethodPost, "/admin/outage", `{"enabled":false,"mode":"hang"}`, nil)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"enabled":false}` {
		t.Fatalf("disable outage: %d %s", w.Code, w.Body.String())
	}

	a.Delay = func() time.Duration { return 0 }

	served := false
	for _, r := range decodeReports(t, h, "/volcanic-reports") {
		served = served || string(r["report_id"]) == `"`+generated.ReportID+`"`
	}

	if !served {
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
	if w := request(h, http.MethodGet, "/volcanic-reports", "", authorized); w.Code != http.StatusOK ||
		time.Since(start) < delay {
		t.Fatalf("status %d after %s", w.Code, time.Since(start))
	}

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/health", ""},
		{http.MethodGet, "/volcanic-reports", ""},
		{http.MethodPost, "/admin/schema-version", `{"version":1}`},
		{http.MethodPost, "/admin/outage", `{"enabled":false}`},
	} {
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
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/volcanic-reports"},
		{http.MethodGet, "/admin/schema-version"},
		{http.MethodGet, "/admin/outage"},
	} {
		if w := request(h, tc.method, tc.path, "", authorized); w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s status %d", tc.method, tc.path, w.Code)
		}
	}

	if w := request(h, http.MethodGet, "/volcanoes", "", authorized); w.Code != http.StatusNotFound {
		t.Fatalf("unknown route status %d", w.Code)
	}

	headers := bearer(testToken)
	headers[correlationHeader] = "poll-1"

	w := request(h, http.MethodGet, "/volcanic-reports", "", headers)
	if w.Header().Get(correlationHeader) != "poll-1" {
		t.Fatal("valid correlation ID not echoed")
	}

	for _, id := range []string{"", "has space", strings.Repeat("a", 129)} {
		w := request(h, http.MethodGet, "/health", "", map[string]string{correlationHeader: id})

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
		{http.MethodPost, "/volcanic-reports", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/missing", http.StatusNotFound, "not_found"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
			t.Fatalf("%s %s status %d body %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
