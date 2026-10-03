package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type API struct {
	Store         SessionStore
	Clients       []ClientCredential
	PrivateKey    ed25519.PrivateKey
	ValidateToken string
	Issuer        string
	Audience      string
	AccessTTL     time.Duration
	RefreshTTL    time.Duration
	Log           *slog.Logger
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

type loginRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}
type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}
type validateRequest struct {
	SessionID    string `json:"session_id"`
	TokenVersion int64  `json:"token_version"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	return true
}

func refreshHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}
func (a *API) clientCredentials(id, secret string) (ClientCredential, bool) {
	var found ClientCredential
	matched := 0
	for _, client := range a.Clients {
		idOK := subtle.ConstantTimeCompare([]byte(id), []byte(client.ID))
		secretOK := subtle.ConstantTimeCompare([]byte(secret), []byte(client.Secret))
		if idOK&secretOK == 1 {
			found, matched = client, 1
		}
	}
	return found, matched == 1
}
func (a *API) issue(client ClientCredential, sessionID string, version int64, refresh string, now time.Time) (tokenResponse, error) {
	claims, err := newClaims(a.Issuer, a.Audience, client, sessionID, version, now, a.AccessTTL)
	if err != nil {
		return tokenResponse{}, err
	}
	token, err := signAccessToken(a.PrivateKey, claims)
	if err != nil {
		return tokenResponse{}, err
	}
	return tokenResponse{AccessToken: token, TokenType: "Bearer", ExpiresIn: int64(a.AccessTTL.Seconds()), RefreshToken: refresh}, nil
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var in loginRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.ClientID == "" || in.ClientSecret == "" {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	client, ok := a.clientCredentials(in.ClientID, in.ClientSecret)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid_credentials")
		return
	}
	sessionID, err := newSessionID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token_issue_failed")
		return
	}
	refresh, err := newRefreshToken(sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token_issue_failed")
		return
	}
	now := time.Now().UTC()
	response, err := a.issue(client, sessionID, 1, refresh, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token_issue_failed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err = a.Store.Create(ctx, sessionID, client.ID, refreshHash(refresh), now.Add(a.RefreshTTL))
	a.logDB(r, "create_session", start, err)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "auth_store_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) refresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var in refreshRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	parts := strings.Split(in.RefreshToken, ".")
	if len(parts) != 2 {
		writeError(w, http.StatusUnauthorized, "invalid_refresh_token")
		return
	}
	newRefresh, err := newRefreshToken(parts[0])
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token_issue_failed")
		return
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	clientID, version, err := a.Store.Rotate(ctx, parts[0], refreshHash(in.RefreshToken), refreshHash(newRefresh), now.Add(a.RefreshTTL))
	a.logDB(r, "rotate_session", start, err)
	if errors.Is(err, ErrInvalidRefresh) {
		writeError(w, http.StatusUnauthorized, "invalid_refresh_token")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "auth_store_unavailable")
		return
	}
	client, ok := a.clientByID(clientID)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "auth_configuration_invalid")
		return
	}
	response, err := a.issue(client, parts[0], version, newRefresh, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token_issue_failed")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) clientByID(id string) (ClientCredential, bool) {
	for _, client := range a.Clients {
		if client.ID == id {
			return client, true
		}
	}
	return ClientCredential{}, false
}

func bearerValue(header string) (string, bool) {
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	return token, token != "" && !strings.ContainsAny(token, " \t\r\n")
}

func (a *API) validate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	provided, ok := bearerValue(r.Header.Get("Authorization"))
	if !ok || subtle.ConstantTimeCompare([]byte(provided), []byte(a.ValidateToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var in validateRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.SessionID == "" || in.TokenVersion < 1 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	active, err := a.Store.Validate(ctx, in.SessionID, in.TokenVersion)
	a.logDB(r, "validate_session", start, err)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "auth_store_unavailable")
		return
	}
	if !active {
		writeError(w, http.StatusUnauthorized, "inactive_session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"active": true})
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := a.Store.Health(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "auth_store_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) logDB(r *http.Request, operation string, start time.Time, err error) {
	if a.Log == nil {
		return
	}
	reason := ""
	if err != nil {
		reason = "store_error"
	}
	a.Log.Info("database_call", "service", "auth", "dependency", "auth_store", "operation", operation, "correlation_id", r.Header.Get("X-Correlation-ID"), "latency_ms", time.Since(start).Milliseconds(), "reason", reason)
}

func validCorrelation(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-_.", c)) {
			return false
		}
	}
	return true
}
func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "correlation-unavailable"
	}
	return hex.EncodeToString(b[:])
}
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", a.login)
	mux.HandleFunc("/auth/refresh", a.refresh)
	mux.HandleFunc("/auth/validate", a.validate)
	mux.HandleFunc("/health", a.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if !validCorrelation(id) {
			id = newCorrelationID()
		}
		r.Header.Set("X-Correlation-ID", id)
		w.Header().Set("X-Correlation-ID", id)
		start := time.Now()
		status := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(status, r)
		if a.Log != nil {
			a.Log.Info("request", "service", "auth", "correlation_id", id, "method", r.Method, "path", r.URL.Path, "status", status.status, "latency_ms", time.Since(start).Milliseconds())
		}
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
