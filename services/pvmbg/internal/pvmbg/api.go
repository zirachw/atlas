package pvmbg

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

type API struct {
	Store    Store
	Controls *Controls
	Token    string
	Delay    func() time.Duration
	MaxHang  time.Duration
	Log      *slog.Logger
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

const correlationHeader = "X-Correlation-ID"

// Bounds a hang-mode request whose client never times out.
const DefaultMaxHang = time.Minute

func (a *API) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(a.logRequest)
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) { writeError(w, http.StatusNotFound, "not_found") })
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	})
	r.Get("/health", a.health)
	r.Post("/admin/schema-version", a.setSchemaVersion)
	r.Post("/admin/outage", a.setOutage)
	r.With(a.outage, a.authenticate, a.delay).Get("/volcanic-reports", a.reports)

	return r
}

func (a *API) reports(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}

	start := time.Now()
	reports, err := a.Store.Reports(r.Context(), since)
	a.logDB(r, "read_volcanic_reports", start, err)

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}

	writeJSON(w, http.StatusOK, reports)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if o := a.Controls.Outage(); o.Enabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "outage", "outage": o})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	start := time.Now()
	err := a.Store.Health(ctx)
	a.logDB(r, "health", start, err)

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "schema_version": a.Controls.SchemaVersion()})
}

func (a *API) setSchemaVersion(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Version *int `json:"version"`
	}
	if !decodeBody(w, r, &in) {
		return
	}

	if in.Version == nil || *in.Version != 1 && *in.Version != 2 {
		writeError(w, http.StatusBadRequest, "version_must_be_1_or_2")
		return
	}

	start := time.Now()
	err := a.Store.SetSchemaVersion(r.Context(), *in.Version)
	a.logDB(r, "set_schema_version", start, err)

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}

	a.Controls.SetSchemaVersion(*in.Version)
	a.Log.Info("schema_version_changed",
		"correlation_id", r.Header.Get(correlationHeader),
		"schema_version", *in.Version,
	)
	writeJSON(w, http.StatusOK, map[string]int{"version": *in.Version})
}

func (a *API) setOutage(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Enabled *bool  `json:"enabled"`
		Mode    string `json:"mode"`
	}
	if !decodeBody(w, r, &in) {
		return
	}

	if in.Enabled == nil {
		writeError(w, http.StatusBadRequest, "enabled_is_required")
		return
	}

	o := Outage{Enabled: *in.Enabled}
	if o.Enabled {
		o.Mode = cmp.Or(in.Mode, OutageError)
		if o.Mode != OutageError && o.Mode != OutageHang {
			writeError(w, http.StatusBadRequest, "mode_must_be_error_or_hang")
			return
		}
	}

	a.Controls.SetOutage(o)
	a.Log.Info("outage_changed",
		"correlation_id", r.Header.Get(correlationHeader),
		"enabled", o.Enabled,
		"mode", o.Mode,
	)
	writeJSON(w, http.StatusOK, o)
}

func (a *API) logDB(r *http.Request, operation string, start time.Time, err error) {
	reason := ""
	if err != nil {
		reason = "store_error"
	}

	a.Log.Info("database_call",
		"dependency", "pvmbg_store",
		"operation", operation,
		"correlation_id", r.Header.Get(correlationHeader),
		"latency_ms", time.Since(start).Milliseconds(),
		"reason", reason,
	)
}

// Runs before authentication because a dead service answers nobody.
func (a *API) outage(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o := a.Controls.Outage()
		switch {
		case !o.Enabled:
			next.ServeHTTP(w, r)
		case o.Mode == OutageHang:
			sleep(r.Context(), a.MaxHang)
			// Closes the connection without a response, and net/http logs no stack trace for it.
			panic(http.ErrAbortHandler)
		default:
			writeError(w, http.StatusServiceUnavailable, "outage")
		}
	})
}

// Missing or malformed token (e.g. BMKG's key) gets 401. A well-formed wrong token gets 403.
func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")

		token, isBearer := strings.CutPrefix(header, "Bearer ")
		switch {
		case header == "":
			writeError(w, http.StatusUnauthorized, "missing_credentials")
		case !isBearer || !tokenFormat.MatchString(token):
			writeError(w, http.StatusUnauthorized, "invalid_token_format")
		case subtle.ConstantTimeCompare([]byte(token), []byte(a.Token)) != 1:
			writeError(w, http.StatusForbidden, "invalid_credentials")
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// A client that disconnects during the delay gets no response.
func (a *API) delay(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sleep(r.Context(), a.Delay()) {
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

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	d.DisallowUnknownFields()

	if d.Decode(dst) != nil || !errors.Is(d.Decode(&struct{}{}), io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_body")
		return false
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
			a.Log.Info("request",
				"correlation_id", id,
				"method", r.Method,
				"path", r.URL.Path,
				"status", logged.status,
				"latency_ms", time.Since(start).Milliseconds(),
			)
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

// status stays 0 if nothing is written, e.g. the client left or a hang-mode drop.

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
