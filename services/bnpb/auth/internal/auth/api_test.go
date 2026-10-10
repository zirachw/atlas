package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type memorySession struct {
	clientID string
	version  int64
	hash     []byte
	expires  time.Time
}

type memoryStore struct {
	mu       sync.Mutex
	sessions map[string]memorySession
	fail     bool
}

func newMemoryStore() *memoryStore { return &memoryStore{sessions: map[string]memorySession{}} }

func (s *memoryStore) Create(_ context.Context, id, client string, hash []byte, expires time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail {
		return errors.New("db")
	}

	s.sessions[id] = memorySession{client, 1, append([]byte(nil), hash...), expires}

	return nil
}

func (s *memoryStore) Rotate(_ context.Context, id string, oldHash, newHash []byte, expires time.Time) (
	string,
	int64,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail {
		return "", 0, errors.New("db")
	}

	x, ok := s.sessions[id]
	if !ok || !x.expires.After(time.Now()) {
		return "", 0, ErrInvalidRefresh
	}

	if !bytes.Equal(x.hash, oldHash) {
		delete(s.sessions, id)
		return "", 0, ErrInvalidRefresh
	}

	x.version++

	x.hash = append([]byte(nil), newHash...)
	x.expires = expires
	s.sessions[id] = x

	return x.clientID, x.version, nil
}

func (s *memoryStore) Validate(_ context.Context, id string, version int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail {
		return false, errors.New("db")
	}

	x, ok := s.sessions[id]

	return ok && x.version == version && x.expires.After(time.Now()), nil
}

func (s *memoryStore) Health(context.Context) error {
	if s.fail {
		return errors.New("db")
	}

	return nil
}

func testAPI(t *testing.T, store SessionStore) *API {
	t.Helper()

	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	return &API{
		Store: store,
		Clients: []ClientCredential{
			{ID: "media", Secret: "media-secret", Scope: "hazards:summary"},
			{ID: "field", Secret: "field-secret", Scope: "hazards:raw"},
			{ID: "ops", Secret: "ops-secret", Scope: "hazards:raw"},
		},
		PrivateKey:    private,
		ValidateToken: "service-secret",
		Issuer:        "atlas-auth",
		Audience:      "atlas-client-api",
		AccessTTL:     time.Minute,
		RefreshTTL:    time.Hour,
		Log:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
}

func postJSON(h http.Handler, path, body, authorization string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Correlation-ID", "test-auth")

	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	return w
}

func decodeTokenResponse(t *testing.T, w *httptest.ResponseRecorder) tokenResponse {
	t.Helper()

	var out tokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}

	return out
}

func TestLoginScopesAndRefreshSupersedesOldSessionVersion(t *testing.T) {
	store, api := newMemoryStore(), (*API)(nil)
	api = testAPI(t, store)
	handler := api.Handler()
	media := postJSON(handler, "/auth/login", `{"client_id":"media","client_secret":"media-secret"}`, "")

	field := postJSON(handler, "/auth/login", `{"client_id":"field","client_secret":"field-secret"}`, "")
	if media.Code != http.StatusOK || field.Code != http.StatusOK {
		t.Fatalf("login status media=%d field=%d", media.Code, field.Code)
	}

	mr, fr := decodeTokenResponse(t, media), decodeTokenResponse(t, field)
	if mClaims := tokenClaims(t, mr.AccessToken); mClaims.Scope != "hazards:summary" {
		t.Fatalf("media scope %q", mClaims.Scope)
	}

	if fClaims := tokenClaims(t, fr.AccessToken); fClaims.Scope != "hazards:raw" {
		t.Fatalf("field scope %q", fClaims.Scope)
	}

	claims := tokenClaims(t, fr.AccessToken)
	validate := func(body []byte) *httptest.ResponseRecorder {
		return postJSON(handler, "/auth/validate", string(body), "Bearer service-secret")
	}

	validBody, _ := json.Marshal(validateRequest{claims.SessionID, claims.TokenVersion})
	if w := validate(validBody); w.Code != http.StatusOK || w.Body.String() != `{"active":true}`+"\n" {
		t.Fatalf("validate: %d %s", w.Code, w.Body.String())
	}

	oldRefresh := fr.RefreshToken

	refreshed := postJSON(handler, "/auth/refresh", `{"refresh_token":"`+oldRefresh+`"}`, "")
	if refreshed.Code != http.StatusOK {
		t.Fatalf("refresh status %d: %s", refreshed.Code, refreshed.Body.String())
	}

	nr := decodeTokenResponse(t, refreshed)

	newClaims := tokenClaims(t, nr.AccessToken)
	if newClaims.TokenVersion != claims.TokenVersion+1 || nr.RefreshToken == oldRefresh {
		t.Fatal("refresh did not rotate token and version")
	}

	validBody, _ = json.Marshal(validateRequest{claims.SessionID, claims.TokenVersion})
	if w := validate(validBody); w.Code != http.StatusUnauthorized {
		t.Fatalf("old access version status %d", w.Code)
	}

	validBody, _ = json.Marshal(validateRequest{newClaims.SessionID, newClaims.TokenVersion})
	if w := validate(validBody); w.Code != http.StatusOK {
		t.Fatalf("new access version status %d", w.Code)
	}

	replay := `{"refresh_token":"` + oldRefresh + `"}`
	if w := postJSON(handler, "/auth/refresh", replay, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("replayed refresh status %d", w.Code)
	}

	// Reusing the rotated-out refresh token revokes the session, so the newest token stops working too.
	if w := validate(validBody); w.Code != http.StatusUnauthorized {
		t.Fatalf("session after refresh reuse status %d", w.Code)
	}
}

func tokenClaims(t *testing.T, token string) AccessClaims {
	t.Helper()

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed JWT %q", token)
	}

	decoded, err := base64URLDecode(parts[1])
	if err != nil {
		t.Fatal(err)
	}

	var claims AccessClaims
	if err := json.Unmarshal(decoded, &claims); err != nil {
		t.Fatal(err)
	}

	return claims
}

func TestCredentialsAndValidateEndpointFailClosed(t *testing.T) {
	store, api := newMemoryStore(), (*API)(nil)
	api = testAPI(t, store)

	h := api.Handler()

	badLogin := `{"client_id":"media","client_secret":"wrong"}`
	if w := postJSON(h, "/auth/login", badLogin, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad login %d", w.Code)
	}

	badSession := `{"session_id":"s","token_version":1}`
	if w := postJSON(h, "/auth/validate", badSession, "Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad service credential %d", w.Code)
	}

	store.fail = true

	session := `{"session_id":"00000000-0000-4000-8000-000000000001","token_version":1}`
	if w := postJSON(h, "/auth/validate", session, "Bearer service-secret"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("store failure %d", w.Code)
	}
}

func TestConcurrentRefreshHasOneWinner(t *testing.T) {
	store, api := newMemoryStore(), (*API)(nil)
	api = testAPI(t, store)
	h := api.Handler()
	login := postJSON(h, "/auth/login", `{"client_id":"field","client_secret":"field-secret"}`, "")
	refresh := decodeTokenResponse(t, login).RefreshToken
	start := make(chan struct{})
	results := make(chan int, 2)

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start

			w := postJSON(h, "/auth/refresh", `{"refresh_token":"`+refresh+`"}`, "")
			results <- w.Code
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	toks := map[int]int{}
	for code := range results {
		toks[code]++
	}

	if toks[http.StatusOK] != 1 || toks[http.StatusUnauthorized] != 1 {
		t.Fatalf("refresh race statuses: %#v", toks)
	}
}

func base64URLDecode(v string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(v) }

func TestUnknownRoutesAndMethodsAnswerJSON(t *testing.T) {
	h := testAPI(t, newMemoryStore()).Handler()

	for _, tc := range []struct {
		method, path string
		status       int
		code         string
	}{
		{http.MethodGet, "/auth/login", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodPost, "/health", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/missing", http.StatusNotFound, "not_found"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
			t.Fatalf("%s %s status %d body %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
