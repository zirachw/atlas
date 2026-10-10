package clientapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testToken(t *testing.T, key ed25519.PrivateKey, scope string, expiry time.Time) string {
	t.Helper()

	claims := AccessClaims{
		Issuer:       "atlas-auth",
		Audience:     "atlas-client-api",
		Subject:      "client-1",
		ClientID:     "client-1",
		Scope:        scope,
		SessionID:    "00000000-0000-4000-8000-000000000001",
		TokenVersion: 1,
		IssuedAt:     time.Now().Add(-time.Second).Unix(),
		ExpiresAt:    expiry.Unix(),
		TokenID:      "jti-1",
	}
	header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)

	return unsigned + "." + enc.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
}

func testAPI(
	t *testing.T,
	authStatus int,
	authBody string,
	aggregatorStatus int,
	aggregatorBody string,
	handler func(*http.Request),
) (*API, ed25519.PrivateKey) {
	t.Helper()

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	a := &API{
		Config: Config{
			AuthURL:         "http://auth:8084",
			AggregatorURL:   "http://aggregator:8083",
			AggregatorToken: "agg-secret",
			ValidateToken:   "validate-secret",
			Issuer:          "atlas-auth",
			Audience:        "atlas-client-api",
			PublicKey:       public,
			OutboundTimeout: time.Second,
			RequestCapacity: defaultRequestCapacity,
		},
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	a.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if handler != nil {
			handler(r)
		}

		status, body := authStatus, authBody
		if r.URL.Host == "aggregator:8083" {
			status, body = aggregatorStatus, aggregatorBody
		}

		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})}

	return a, private
}

func invoke(a *API, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Correlation-ID", "test-client")

	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)

	return w
}

func TestSummaryFiltersRawFieldsAndPreservesFreshnessAndCorrelation(t *testing.T) {
	body := `{"data":[{"hazard_id":"id-1","source":"BMKG","source_ref_id":"raw-id","hazard_type":"SEISMIC","severity":"SIAGA","area_name":"North","latitude":-6.1,"longitude":106.8,"occurred_at":"2026-10-03T00:00:00Z","ingested_at":"2026-10-03T00:00:01Z","attributes":{"magnitude":6.2}}],"sources":[{"source":"BMKG","status":"healthy"},{"source":"PVMBG","status":"stale","stale_since":"2026-10-02T00:00:00Z"}]}`
	a, private := testAPI(t, http.StatusOK, `{"active":true}`, http.StatusOK, body, func(r *http.Request) {
		if r.Header.Get("X-Correlation-ID") != "test-client" {
			t.Errorf("correlation ID not forwarded: %q", r.Header.Get("X-Correlation-ID"))
		}

		if r.URL.Host == "auth:8084" {
			if r.URL.Path != "/auth/validate" || r.Header.Get("Authorization") != "Bearer validate-secret" {
				t.Errorf("bad Auth request: %s %s", r.Method, r.URL)
			}

			var request validateRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.SessionID == "" ||
				request.TokenVersion != 1 {
				t.Errorf("bad validate payload: %#v %v", request, err)
			}
		}

		if r.URL.Host == "aggregator:8083" {
			if r.Header.Get("Authorization") != "Bearer agg-secret" {
				t.Errorf("bad Aggregator credential")
			}

			if r.URL.Query().Get("source") != "BMKG" || r.URL.Query().Get("view") != "" {
				t.Errorf("bad forwarded query: %s", r.URL.RawQuery)
			}
		}
	})

	w := invoke(
		a,
		"/hazards?source=BMKG&view=summary",
		testToken(t, private, "hazards:summary", time.Now().Add(time.Minute)),
	)
	if w.Code != http.StatusOK || w.Header().Get("X-Correlation-ID") != "test-client" {
		t.Fatalf("status/header: %d %#v", w.Code, w.Header())
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	var events []map[string]json.RawMessage
	if err := json.Unmarshal(got["data"], &events); err != nil || len(events) != 1 {
		t.Fatalf("data: %s (%v)", got["data"], err)
	}

	if len(events[0]) != len(summaryFields) {
		t.Fatalf("summary fields: %#v", events[0])
	}

	for key := range summaryFields {
		if _, ok := events[0][key]; !ok {
			t.Fatalf("missing summary field %q", key)
		}
	}

	for _, key := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if _, ok := events[0][key]; ok {
			t.Fatalf("raw field %q leaked", key)
		}
	}

	var sources []map[string]any
	if err := json.Unmarshal(got["sources"], &sources); err != nil || len(sources) != 2 ||
		sources[1]["status"] != "stale" {
		t.Fatalf("freshness metadata lost: %s (%v)", got["sources"], err)
	}
}

func TestMediaRawRequestForbiddenAndRawClientGetsAllFields(t *testing.T) {
	body := `{"data":[{"hazard_id":"id-1","source":"PVMBG","source_ref_id":"raw-id","latitude":-6.1,"attributes":{"confidence_level":0.9}}],"sources":[]}`
	aggregatorCalls := 0
	a, private := testAPI(t, http.StatusOK, `{"active":true}`, http.StatusOK, body, func(r *http.Request) {
		if r.URL.Host == "aggregator:8083" {
			aggregatorCalls++
		}
	})

	media := invoke(a, "/hazards?view=raw", testToken(t, private, "hazards:summary", time.Now().Add(time.Minute)))
	if media.Code != http.StatusForbidden || aggregatorCalls != 0 {
		t.Fatalf("media raw access status=%d aggregator calls=%d", media.Code, aggregatorCalls)
	}

	field := invoke(a, "/hazards?view=raw", testToken(t, private, "hazards:raw", time.Now().Add(time.Minute)))
	if field.Code != http.StatusOK {
		t.Fatalf("raw client status %d: %s", field.Code, field.Body.String())
	}

	var out struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(field.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}

	if _, ok := out.Data[0]["source_ref_id"]; !ok {
		t.Fatal("raw field absent for authorized client")
	}

	if _, ok := out.Data[0]["attributes"]; !ok {
		t.Fatal("attributes absent for authorized client")
	}
}

func TestInvalidExpiredSupersededAndUnavailableAuth(t *testing.T) {
	a, private := testAPI(t, http.StatusOK, `{"active":false}`, http.StatusOK, `{"data":[],"sources":[]}`, nil)

	valid := testToken(t, private, "hazards:raw", time.Now().Add(time.Minute))
	if w := invoke(a, "/hazards", "malformed"); w.Code != http.StatusUnauthorized {
		t.Fatalf("malformed token status %d", w.Code)
	}

	expired := testToken(t, private, "hazards:raw", time.Now().Add(-time.Second))
	if w := invoke(a, "/hazards", expired); w.Code != http.StatusUnauthorized {
		t.Fatalf("expired token status %d", w.Code)
	}

	if w := invoke(a, "/hazards", valid); w.Code != http.StatusUnauthorized {
		t.Fatalf("superseded token status %d", w.Code)
	}

	authDown := `{"error":{"code":"auth_store_unavailable"}}`
	a, private = testAPI(t, http.StatusServiceUnavailable, authDown, http.StatusOK, `{"data":[],"sources":[]}`, nil)

	current := testToken(t, private, "hazards:raw", time.Now().Add(time.Minute))
	if w := invoke(a, "/hazards", current); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("auth outage status %d", w.Code)
	}
}

func TestQueryValidationAndHealth(t *testing.T) {
	a, private := testAPI(t, http.StatusOK, `{"active":true}`, http.StatusOK, `{"data":[],"sources":[]}`, nil)
	for _, query := range []string{
		"view=other",
		"view=raw&view=summary",
		"source=BMKG&hazard_type=VOLCANIC",
		"raw=true",
		"limit=501",
	} {
		token := testToken(t, private, "hazards:raw", time.Now().Add(time.Minute))
		if w := invoke(a, "/hazards?"+query, token); w.Code != http.StatusBadRequest {
			t.Errorf("query %q status %d", query, w.Code)
		}
	}

	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("health status %d", w.Code)
	}

	for _, tc := range []struct {
		method, path string
		status       int
		code         string
	}{
		{http.MethodPost, "/hazards", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/missing", http.StatusNotFound, "not_found"},
	} {
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
			t.Fatalf("%s %s status %d body %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
