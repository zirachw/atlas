package clientapi

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSummaryBoundaryRejectsMalformedEnvelopesAndStripsDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, body     string
		upstream, want int
	}{
		{"metadata", `{"data":[{"hazard_id":"h1","attributes":{"secret":1}}],"sources":[{"source":"BMKG","status":"healthy","latitude":1,"attributes":{"secret":1}}],"debug":{"source_ref_id":"raw-secret"}}`, 200, 200},
		{"missing sources", `{"data":[{"latitude":1}]}`, 200, 502},
		{"missing data", `{"sources":[]}`, 206, 502},
		{"null data", `{"data":null,"sources":[]}`, 200, 502},
		{"nested summary field", `{"data":[{"hazard_id":{"latitude":1}}],"sources":[]}`, 200, 502},
		{"nested source name", `{"data":[],"sources":[{"source":{"latitude":1},"status":"healthy"}]}`, 200, 502},
		{"bad timestamp", `{"data":[],"sources":[{"source":"BMKG","status":"healthy","last_success_at":{"latitude":1}}]}`, 200, 502},
		{"error diagnostics", `{"error":{"code":"raw-secret","message":"raw-secret","attributes":{"latitude":1}},"debug":"raw-secret"}`, 503, 503},
		{"partial", `{"data":[],"sources":[{"source":"PVMBG","status":"unavailable","attributes":{"secret":1}}]}`, 206, 206},
		{"unavailable envelope", `{"data":[],"sources":[{"source":"BMKG","status":"unavailable","attributes":{"secret":1}}]}`, 503, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, key := testAPI(t, 200, `{"active":true}`, tc.upstream, tc.body, nil)
			w := invoke(a, "/hazards", testToken(t, key, "hazards:summary", time.Now().Add(time.Minute)))
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			for _, forbidden := range []string{"latitude", "attributes", "raw-secret", "source_ref_id", "debug"} {
				if strings.Contains(w.Body.String(), forbidden) {
					t.Fatalf("leaked %s: %s", forbidden, w.Body)
				}
			}
		})
	}
}

func TestAuthInvalidResponsesAndTimeoutFailClosed(t *testing.T) {
	for _, body := range []string{`{}`, `{"active":true} trailing`, `{"active":"true"}`, strings.Repeat("x", 5000)} {
		a, key := testAPI(t, 200, body, 200, `{"data":[],"sources":[]}`, nil)
		if w := invoke(a, "/hazards", testToken(t, key, "hazards:raw", time.Now().Add(time.Minute))); w.Code != 503 {
			t.Fatalf("invalid Auth response=%d", w.Code)
		}
	}
	a, key := testAPI(t, 401, `{"error":{"code":"unauthorized"}}`, 200, `{"data":[],"sources":[]}`, nil)
	if w := invoke(a, "/hazards", testToken(t, key, "hazards:raw", time.Now().Add(time.Minute))); w.Code != 503 {
		t.Fatalf("internal credential failure=%d", w.Code)
	}
	a.Client.Timeout = 50 * time.Millisecond
	a.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
	started := time.Now()
	if w := invoke(a, "/hazards", testToken(t, key, "hazards:raw", time.Now().Add(time.Minute))); w.Code != 503 {
		t.Fatalf("timeout=%d", w.Code)
	}
	if time.Since(started) > time.Second {
		t.Fatal("Auth timeout was not bounded")
	}
}

func TestAdmissionLimitDoesNotCallDependenciesAndReleasesCapacity(t *testing.T) {
	a, key := testAPI(t, 200, `{"active":true}`, 200, `{"data":[],"sources":[]}`, nil)
	a.Config.RequestCapacity = 1
	entered, release := make(chan struct{}), make(chan struct{})
	a.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"data":[],"sources":[]}`
		if r.URL.Host == "auth:8084" {
			select {
			case entered <- struct{}{}:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			body = `{"active":true}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	h := a.Handler()
	token := testToken(t, key, "hazards:raw", time.Now().Add(time.Minute))
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/hazards", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- request() }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request never entered Auth")
	}
	w := request()
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("overload=%d %s", w.Code, w.Body)
	}
	close(release)
	if w = <-done; w.Code != 200 {
		t.Fatalf("first=%d", w.Code)
	}
	a.Client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })
	for i := 0; i < 2; i++ {
		if w = request(); w.Code != 503 {
			t.Fatalf("capacity leaked: %d", w.Code)
		}
	}
}

func TestUpstream429PreservesStatusAndSafeRetryAfter(t *testing.T) {
	for _, retry := range []string{"7", "bad-diagnostics"} {
		a, key := testAPI(t, 200, `{"active":true}`, 429, `{"error":{"latitude":1}}`, nil)
		original := a.Client.Transport
		a.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := original.RoundTrip(r)
			if r.URL.Host == "aggregator:8083" {
				resp.Header.Set("Retry-After", retry)
			}
			return resp, err
		})
		w := invoke(a, "/hazards", testToken(t, key, "hazards:summary", time.Now().Add(time.Minute)))
		want := retry
		if retry == "bad-diagnostics" {
			want = "1"
		}
		if w.Code != 429 || w.Header().Get("Retry-After") != want || strings.Contains(w.Body.String(), "latitude") {
			t.Fatalf("response=%d %v %s", w.Code, w.Header(), w.Body)
		}
	}
}
