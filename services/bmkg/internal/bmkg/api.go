package bmkg

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

const correlationHeader = "X-Correlation-ID"

type API struct {
	Store  Store
	APIKey string
	Delay  time.Duration
	Log    *slog.Logger
}

func (a *API) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(a.logRequest)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := a.Store.Health(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "store_unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Group(func(r chi.Router) {
		r.Use(a.authenticate, a.delay)
		r.Get("/seismic-events", read(a, "read_seismic_events", a.Store.Events))
		r.Get("/tsunami-warnings", read(a, "read_tsunami_warnings", a.Store.Warnings))
	})
	return r
}

func read[T any](a *API, operation string, query func(context.Context, *time.Time) ([]T, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		since, ok := parseSince(w, r)
		if !ok {
			return
		}
		start := time.Now()
		records, err := query(r.Context(), since)
		a.logDB(r, operation, start, err)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "store_unavailable")
			return
		}
		writeJSON(w, http.StatusOK, records)
	}
}

func (a *API) logDB(r *http.Request, operation string, start time.Time, err error) {
	reason := ""
	if err != nil {
		reason = "store_error"
	}
	a.Log.Info("database_call", "service", "bmkg", "dependency", "bmkg_store", "operation", operation, "correlation_id", r.Header.Get(correlationHeader), "latency_ms", time.Since(start).Milliseconds(), "reason", reason)
}

// No X-BMKG-Key (e.g. a PVMBG bearer token) gets 401. A wrong key gets 403.
func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-BMKG-Key")
		switch {
		case key == "":
			writeError(w, http.StatusUnauthorized, "missing_credentials")
		case subtle.ConstantTimeCompare([]byte(key), []byte(a.APIKey)) != 1:
			writeError(w, http.StatusForbidden, "invalid_credentials")
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// A client that disconnects during the delay gets no response.
func (a *API) delay(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sleep(r.Context(), a.Delay) {
			next.ServeHTTP(w, r)
		}
	})
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func parseSince(w http.ResponseWriter, r *http.Request) (*time.Time, bool) {
	values := r.URL.Query()["since"]
	if len(values) == 0 {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339Nano, values[0])
	if len(values) > 1 || err != nil {
		writeError(w, http.StatusBadRequest, "invalid_since")
		return nil, false
	}
	return &t, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func (a *API) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if !validCorrelation(id) {
			id = newCorrelationID()
		}
		r.Header.Set(correlationHeader, id)
		w.Header().Set(correlationHeader, id)
		start := time.Now()
		logged := &statusWriter{ResponseWriter: w}
		defer func() {
			a.Log.Info("request", "service", "bmkg", "correlation_id", id, "method", r.Method, "path", r.URL.Path, "status", logged.status, "latency_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(logged, r)
	})
}

func validCorrelation(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	return strings.IndexFunc(id, func(c rune) bool {
		return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.", c))
	}) < 0
}

func newCorrelationID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// status stays 0 if nothing is written, e.g. the client left during the delay.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}
