package aggregator

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

type ReadStore interface {
	Read(context.Context, Query) (Response, error)
	Health(context.Context) error
}

type API struct {
	Store    ReadStore
	Token    string
	Timeout  time.Duration
	Capacity chan struct{}
	Log      *slog.Logger
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func correlationID() string {
	var b [16]byte

	_, _ = rand.Read(b[:])

	return hex.EncodeToString(b[:])
}

func validCorrelation(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}

	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("-_.", c)) {
			return false
		}
	}

	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func parseQuery(r *http.Request) (Query, error) {
	q := Query{Limit: 100}

	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, err
	}

	for k, values := range params {
		if len(values) != 1 {
			return q, errors.New("repeated filter")
		}

		v := values[0]

		switch k {
		case "source":
			if v != "BMKG" && v != "PVMBG" {
				return q, errors.New("source")
			}

			q.Source = v
		case "hazard_type":
			if v != "SEISMIC" && v != "VOLCANIC" {
				return q, errors.New("hazard type")
			}

			q.HazardType = v
		case "severity":
			if v != "NORMAL" && v != "WASPADA" && v != "SIAGA" && v != "AWAS" {
				return q, errors.New("severity")
			}

			q.Severity = v
		case "since":
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return q, err
			}

			q.Since = &t
		case "limit":
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 500 {
				return q, errors.New("limit")
			}

			q.Limit = n
		default:
			return q, errors.New("unknown filter")
		}
	}

	if q.Source == "BMKG" && q.HazardType == "VOLCANIC" || q.Source == "PVMBG" && q.HazardType == "SEISMIC" {
		return q, errors.New("contradictory filters")
	}

	return q, nil
}

func (a *API) Handler() http.Handler {
	mux := chi.NewRouter()
	mux.Use(a.logRequest)
	mux.NotFound(func(w http.ResponseWriter, _ *http.Request) { writeError(w, http.StatusNotFound, "not_found") })
	mux.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	})
	mux.Get("/health", a.health)
	mux.Get("/internal/hazards", a.hazards)

	return mux
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()

	start := time.Now()
	err := a.Store.Health(ctx)
	logDatabaseCall(a.Log, r.Header.Get("X-Correlation-ID"), "health", start, err)

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) hazards(w http.ResponseWriter, r *http.Request) {
	credential, isBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if a.Token == "" || !isBearer || subtle.ConstantTimeCompare([]byte(credential), []byte(a.Token)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	q, err := parseQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}

	select {
	case a.Capacity <- struct{}{}:
		defer func() { <-a.Capacity }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "capacity_exceeded")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()

	start := time.Now()
	result, err := a.Store.Read(ctx, q)
	logDatabaseCall(a.Log, r.Header.Get("X-Correlation-ID"), "read", start, err)

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}

	unavailable := 0

	for _, s := range result.Sources {
		if s.Status == "unavailable" {
			unavailable++
		}
	}
	// A partly unavailable answer stays 200, and sources[].status tells the client which source is missing.
	status := http.StatusOK
	if unavailable > 0 && unavailable == len(result.Sources) {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, result)
}

func (a *API) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if !validCorrelation(id) {
			id = correlationID()
		}

		r.Header.Set("X-Correlation-ID", id)
		w.Header().Set("X-Correlation-ID", id)

		start := time.Now()
		logged := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(logged, r)
		a.Log.Info(
			"request",
			"correlation_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"status", logged.status,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
