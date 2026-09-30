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

func correlationID() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func validCorrelation(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-_.", c)) {
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
		var e error
		switch k {
		case "source":
			if v != "BMKG" && v != "PVMBG" {
				e = errors.New("source")
			}
			q.Source = v
		case "hazard_type":
			if v != "SEISMIC" && v != "VOLCANIC" {
				e = errors.New("hazard type")
			}
			q.HazardType = v
		case "severity":
			if v != "NORMAL" && v != "WASPADA" && v != "SIAGA" && v != "AWAS" {
				e = errors.New("severity")
			}
			q.Severity = v
		case "since":
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				e = err
			}
			q.Since = &t
		case "limit":
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 500 {
				e = errors.New("limit")
			}
			q.Limit = n
		default:
			e = errors.New("unknown filter")
		}
		if e != nil {
			return q, e
		}
	}
	if q.Source == "BMKG" && q.HazardType == "VOLCANIC" || q.Source == "PVMBG" && q.HazardType == "SEISMIC" {
		return q, errors.New("contradictory filters")
	}
	return q, nil
}
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
		defer cancel()
		if a.Store.Health(ctx) != nil {
			writeError(w, 503, "store_unavailable")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /internal/hazards", func(w http.ResponseWriter, r *http.Request) {
		credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if a.Token == "" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(credential), []byte(a.Token)) != 1 {
			writeError(w, 401, "unauthorized")
			return
		}
		q, e := parseQuery(r)
		if e != nil {
			writeError(w, 400, "invalid_query")
			return
		}
		select {
		case a.Capacity <- struct{}{}:
			defer func() { <-a.Capacity }()
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, 429, "capacity_exceeded")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
		defer cancel()
		result, e := a.Store.Read(ctx, q)
		if e != nil {
			writeError(w, 503, "store_unavailable")
			return
		}
		unavailable := 0
		for _, s := range result.Sources {
			if s.Status == "unavailable" {
				unavailable++
			}
		}
		status := 200
		if unavailable > 0 {
			status = 206
			if unavailable == len(result.Sources) {
				status = 503
			}
		}
		writeJSON(w, status, result)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if !validCorrelation(id) {
			id = correlationID()
		}
		r.Header.Set("X-Correlation-ID", id)
		w.Header().Set("X-Correlation-ID", id)
		start := time.Now()
		logged := &statusWriter{ResponseWriter: w, status: 200}
		mux.ServeHTTP(logged, r)
		a.Log.Info("request", "service", "aggregator", "correlation_id", id, "method", r.Method, "path", r.URL.Path, "status", logged.status, "latency_ms", time.Since(start).Milliseconds())
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
