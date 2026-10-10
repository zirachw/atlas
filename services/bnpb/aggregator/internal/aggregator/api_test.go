package aggregator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type readStub struct {
	result  Response
	err     error
	wait    bool
	entered chan struct{}
}

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func (s *readStub) Read(ctx context.Context, q Query) (Response, error) {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}

	if s.wait {
		<-ctx.Done()
		return Response{}, ctx.Err()
	}

	return s.result, s.err
}

func (s *readStub) Health(context.Context) error { return s.err }

func request(handler http.Handler, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}

	r.Header.Set("X-Correlation-ID", "test-read")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	return w
}

func TestAPIAuthenticationQueriesAndFreshness(t *testing.T) {
	store := &readStub{result: Response{
		Data:    []HazardEvent{},
		Sources: []Freshness{{Source: "BMKG", Status: "healthy"}, {Source: "PVMBG", Status: "stale"}},
	}}
	api := &API{Store: store, Token: "test-internal", Timeout: time.Second, Capacity: make(chan struct{}, 2), Log: testLog()}

	handler := api.Handler()
	for _, token := range []string{"", "test-bmkg", "test-pvmbg"} {
		w := request(handler, "/internal/hazards", token)
		if w.Code != 401 {
			t.Fatal(w.Code)
		}
	}

	for _, q := range []string{
		"source=other",
		"source=BMKG&source=BMKG",
		"limit=0",
		"limit=501",
		"severity=unknown",
		"since=bad",
		"raw=true",
		"source=BMKG&hazard_type=VOLCANIC",
		"limit=%xx",
		"source=BMKG;foo=bar",
	} {
		if w := request(handler, "/internal/hazards?"+q, "test-internal"); w.Code != 400 {
			t.Fatalf("query %s status %d", q, w.Code)
		}
	}

	w := request(handler, "/internal/hazards?source=BMKG&limit=20&since=2026-09-30T00:00:00Z", "test-internal")
	if w.Code != 200 || w.Header().Get("X-Correlation-ID") != "test-read" {
		t.Fatal(w)
	}

	store.result.Sources[1].Status = "unavailable"
	// One source down still answers 200, and the body names the missing source.
	w = request(handler, "/internal/hazards", "test-internal")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"unavailable"`) {
		t.Fatal(w.Code, w.Body.String())
	}

	store.result.Sources[0].Status = "unavailable"

	if w = request(handler, "/internal/hazards", "test-internal"); w.Code != 503 {
		t.Fatal(w.Code)
	}

	store.err = errors.New("postgres://secret:password@host")

	w = request(handler, "/internal/hazards", "test-internal")
	if w.Code != 503 || w.Body.String() != `{"error":{"code":"store_unavailable"}}`+"\n" {
		t.Fatal(w.Body.String())
	}

	if request(handler, "/health", "").Code != 503 {
		t.Fatal("health ignored database failure")
	}
}

func TestAPIAdmissionAndDeadline(t *testing.T) {
	s := &readStub{wait: true, entered: make(chan struct{}, 1)}
	a := &API{Store: s, Token: "test-internal", Timeout: 60 * time.Millisecond, Capacity: make(chan struct{}, 1), Log: testLog()}
	h := a.Handler()

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- request(h, "/internal/hazards", "test-internal") }()

	<-s.entered

	if w := request(h, "/internal/hazards", "test-internal"); w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatal(w)
	}

	select {
	case w := <-done:
		if w.Code != 503 {
			t.Fatal(w.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("query failed to cancel")
	}

	if len(a.Capacity) != 0 {
		t.Fatal("admission slot leaked")
	}
}

func TestAPIRouteMethodsAndCorrelation(t *testing.T) {
	a := &API{
		Store:    &readStub{},
		Token:    "test-internal",
		Timeout:  time.Second,
		Capacity: make(chan struct{}, 1),
		Log:      testLog(),
	}
	h := a.Handler()

	for _, tc := range []struct {
		method, path string
		status       int
	}{{"GET", "/health", 200}, {"POST", "/health", 405}, {"POST", "/internal/hazards", 405}, {"GET", "/missing", 404}} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("X-Correlation-ID", "invalid id")

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != tc.status || !validCorrelation(w.Header().Get("X-Correlation-ID")) {
			t.Fatal(tc, w.Code, w.Header())
		}
	}
}

func TestUnknownRoutesAndMethodsAnswerJSON(t *testing.T) {
	a := &API{Store: &readStub{}, Token: "test-internal", Timeout: time.Second, Capacity: make(chan struct{}, 1), Log: testLog()}
	h := a.Handler()

	for _, tc := range []struct {
		method, path string
		status       int
		code         string
	}{
		{http.MethodPost, "/internal/hazards", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/missing", http.StatusNotFound, "not_found"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
			t.Fatalf("%s %s status %d body %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
