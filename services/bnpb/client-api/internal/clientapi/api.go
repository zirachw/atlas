package clientapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var summaryFields = map[string]bool{
	"hazard_id": true, "source": true, "hazard_type": true, "severity": true,
	"area_name": true, "occurred_at": true, "ingested_at": true,
}

type API struct {
	Config Config
	Client *http.Client
	Log    *slog.Logger
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
func bearerValue(header string) (string, bool) {
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	return token, token != "" && !strings.ContainsAny(token, " \t\r\n")
}
func correlationValid(id string) bool {
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

func allowedFilters(raw string) (url.Values, string, error) {
	parsed, err := url.ParseQuery(raw)
	if err != nil {
		return nil, "", err
	}
	out := url.Values{}
	view := parsed.Get("view")
	if view == "" {
		view = "summary"
	}
	if view != "summary" && view != "raw" {
		return nil, "", errors.New("invalid view")
	}
	allowed := map[string]bool{"source": true, "hazard_type": true, "severity": true, "since": true, "limit": true, "view": true}
	for key, values := range parsed {
		if !allowed[key] || len(values) != 1 {
			return nil, "", errors.New("invalid filter")
		}
		value := values[0]
		switch key {
		case "source":
			if value != "BMKG" && value != "PVMBG" {
				return nil, "", errors.New("invalid source")
			}
		case "hazard_type":
			if value != "SEISMIC" && value != "VOLCANIC" {
				return nil, "", errors.New("invalid hazard type")
			}
		case "severity":
			if value != "NORMAL" && value != "WASPADA" && value != "SIAGA" && value != "AWAS" {
				return nil, "", errors.New("invalid severity")
			}
		case "since":
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return nil, "", errors.New("invalid since")
			}
		case "limit":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 500 {
				return nil, "", errors.New("invalid limit")
			}
		case "view":
			if value != "summary" && value != "raw" {
				return nil, "", errors.New("invalid view")
			}
		}
		if key != "view" {
			out.Set(key, value)
		}
	}
	if out.Get("source") == "BMKG" && out.Get("hazard_type") == "VOLCANIC" || out.Get("source") == "PVMBG" && out.Get("hazard_type") == "SEISMIC" {
		return nil, "", errors.New("contradictory filters")
	}
	return out, view, nil
}

func (a *API) outbound(ctx context.Context, method, target string, headers map[string]string, body io.Reader, id, dependency string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Correlation-ID", id)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	start := time.Now()
	resp, err := a.Client.Do(req)
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	reason := ""
	if err != nil {
		reason = "network_error"
	}
	path := ""
	if parsed, parseErr := url.Parse(target); parseErr == nil {
		path = parsed.Path
	}
	if a.Log != nil {
		a.Log.Info("outbound_call", "service", "client-api", "dependency", dependency, "correlation_id", id, "method", method, "path", path, "status", status, "latency_ms", time.Since(start).Milliseconds(), "reason", reason)
	}
	return resp, err
}

func (a *API) validateCurrent(ctx context.Context, claims AccessClaims, id string) (int, string) {
	body, _ := json.Marshal(validateRequest{SessionID: claims.SessionID, TokenVersion: claims.TokenVersion})
	resp, err := a.outbound(ctx, http.MethodPost, a.Config.AuthURL+"/auth/validate", map[string]string{
		"Authorization": "Bearer " + a.Config.ValidateToken, "Content-Type": "application/json",
	}, strings.NewReader(string(body)), id, "auth")
	if err != nil {
		return http.StatusServiceUnavailable, "auth_unavailable"
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return http.StatusUnauthorized, "invalid_token"
	}
	if resp.StatusCode != http.StatusOK {
		return http.StatusServiceUnavailable, "auth_unavailable"
	}
	var result struct {
		Active bool `json:"active"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&result) != nil {
		return http.StatusServiceUnavailable, "auth_invalid_response"
	}
	if !result.Active {
		return http.StatusUnauthorized, "invalid_token"
	}
	return http.StatusOK, ""
}

func summaryData(raw json.RawMessage) (json.RawMessage, error) {
	var events []json.RawMessage
	if err := json.Unmarshal(raw, &events); err != nil {
		return nil, err
	}
	filtered := make([]map[string]json.RawMessage, 0, len(events))
	for _, eventRaw := range events {
		var event map[string]json.RawMessage
		if err := json.Unmarshal(eventRaw, &event); err != nil {
			return nil, err
		}
		out := make(map[string]json.RawMessage, len(summaryFields))
		for key, value := range event {
			if summaryFields[key] {
				out[key] = value
			}
		}
		filtered = append(filtered, out)
	}
	return json.Marshal(filtered)
}

func (a *API) hazards(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	jwt, ok := bearerValue(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	claims, err := verifyAccessToken(jwt, a.Config.PublicKey, a.Config.Issuer, a.Config.Audience, time.Now())
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_token")
		return
	}
	status, code := a.validateCurrent(r.Context(), claims, r.Header.Get("X-Correlation-ID"))
	if status != http.StatusOK {
		writeError(w, status, code)
		return
	}
	filters, view, err := allowedFilters(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	if view == "raw" && claims.Scope != "hazards:raw" {
		writeError(w, http.StatusForbidden, "insufficient_scope")
		return
	}
	target := a.Config.AggregatorURL + "/internal/hazards"
	if encoded := filters.Encode(); encoded != "" {
		target += "?" + encoded
	}
	resp, err := a.outbound(r.Context(), http.MethodGet, target, map[string]string{
		"Authorization": "Bearer " + a.Config.AggregatorToken,
	}, nil, r.Header.Get("X-Correlation-ID"), "aggregator")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "aggregator_unavailable")
		return
	}
	defer resp.Body.Close()
	const maxBody = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
		return
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusServiceUnavailable {
		if resp.StatusCode >= 500 {
			writeError(w, http.StatusServiceUnavailable, "aggregator_unavailable")
		} else {
			writeError(w, http.StatusBadGateway, "aggregator_error")
		}
		return
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
		return
	}
	data, hasData := envelope["data"]
	_, hasSources := envelope["sources"]
	if hasData && hasSources {
		if view == "summary" {
			filtered, err := summaryData(data)
			if err != nil {
				writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
				return
			}
			envelope["data"] = filtered
		}
		body, err = json.Marshal(envelope)
		if err != nil {
			writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(append(body, '\n'))
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/hazards", a.hazards)
	mux.HandleFunc("/health", a.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if !correlationValid(id) {
			id = newCorrelationID()
		}
		r.Header.Set("X-Correlation-ID", id)
		w.Header().Set("X-Correlation-ID", id)
		start := time.Now()
		logged := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(logged, r)
		if a.Log != nil {
			a.Log.Info("request", "service", "client-api", "correlation_id", id, "method", r.Method, "path", r.URL.Path, "status", logged.status, "latency_ms", time.Since(start).Milliseconds())
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
