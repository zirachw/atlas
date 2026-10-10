package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

type IngestStore interface {
	Checkpoint(context.Context, string) (*time.Time, error)
	WarningEvents(context.Context, []string) ([]HazardEvent, error)
	Save(context.Context, string, []HazardEvent, *time.Time, time.Time) ([]HazardEvent, error)
	Failure(context.Context, string, time.Time) error
}

type Publication struct {
	Event         HazardEvent
	CorrelationID string
}

type Poller struct {
	Source, BaseURL, Credential string
	Interval, Timeout, Backoff  time.Duration
	Retries                     int
	Client                      *http.Client
	Store                       IngestStore
	Pending                     chan<- Publication
	Log                         *slog.Logger
}

const (
	MaxRecords     = 1000
	MaxSourceBytes = 4 << 20
)

// Warnings for events up to this long before the checkpoint are fetched again, so late warnings still apply.
const WarningLookback = time.Hour

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *Poller) fetch(ctx context.Context, path string, since *time.Time, id string) ([]Object, error) {
	for attempt := 0; attempt <= p.Retries; attempt++ {
		if attempt > 0 && !pause(ctx, p.Backoff*time.Duration(1<<(attempt-1))) {
			return nil, ctx.Err()
		}

		records, status, err := p.fetchOnce(ctx, path, since, id)
		if err == nil {
			return records, nil
		}

		if status >= 400 && status < 500 && status != 429 {
			return nil, err
		}

		if status == http.StatusOK {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, errors.New("source attempts exhausted")
}

func (p *Poller) fetchOnce(ctx context.Context, path string, since *time.Time, id string) (
	records []Object,
	status int,
	err error,
) {
	u, err := url.Parse(p.BaseURL + path)
	if err != nil {
		return nil, 0, errors.New("invalid source URL")
	}

	if since != nil {
		q := u.Query()
		q.Set("since", since.UTC().Format(time.RFC3339Nano))
		u.RawQuery = q.Encode()
	}

	callCtx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, errors.New("invalid request")
	}

	req.Header.Set("X-Correlation-ID", id)

	if p.Source == "BMKG" {
		req.Header.Set("X-BMKG-Key", p.Credential)
	} else {
		req.Header.Set("Authorization", "Bearer "+p.Credential)
	}

	start := time.Now()

	defer func() {
		reason := ""
		if err != nil {
			reason = err.Error()
		}

		p.Log.Info(
			"source_call",
			"source", p.Source,
			"dependency", p.Source,
			"correlation_id", id,
			"method", "GET",
			"path", path,
			"status", status,
			"reason", reason,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	}()

	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, status, errors.New("source network failure")
	}
	defer resp.Body.Close()

	status = resp.StatusCode
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("source status %d", status)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxSourceBytes+1))
	if err != nil {
		return nil, status, errors.New("source read failure")
	}

	if len(data) > MaxSourceBytes {
		return nil, status, errors.New("source body bound exceeded")
	}

	if err = json.Unmarshal(data, &records); err != nil || records == nil {
		return nil, status, errors.New("invalid source array")
	}

	if len(records) > MaxRecords {
		return nil, status, errors.New("source record bound exceeded")
	}

	return records, status, nil
}

func (p *Poller) reject(id, ref, reason string) {
	p.Log.Warn("record_rejected", "source", p.Source, "correlation_id", id, "source_ref_id", ref, "reason", reason)
}

func (p *Poller) fetchWarnings(ctx context.Context, checkpoint *time.Time, id string) ([]Warning, error) {
	var since *time.Time

	if checkpoint != nil {
		t := checkpoint.Add(-WarningLookback)
		since = &t
	}

	raw, err := p.fetch(ctx, "/tsunami-warnings", since, id)
	if err != nil {
		return nil, err
	}

	warnings := []Warning{}
	seen := map[string]bool{}

	for _, o := range raw {
		ref, w, err := mapWarning(o)
		if err == nil && seen[ref] {
			err = errors.New("duplicate warning ID")
		}

		if err != nil {
			p.reject(id, ref, err.Error())
			continue
		}

		seen[ref] = true

		warnings = append(warnings, w)
	}

	return warnings, nil
}

func (p *Poller) Poll(ctx context.Context, id string) error {
	start := time.Now()
	checkpoint, err := p.Store.Checkpoint(ctx, p.Source)
	logDatabaseCall(p.Log, id, "checkpoint", start, err)

	if err != nil {
		return errors.New("checkpoint read failed")
	}

	path, idField, timeField := "/volcanic-reports", "report_id", "reported_at"
	if p.Source == "BMKG" {
		path, idField, timeField = "/seismic-events", "event_id", "occurred_at"
	}

	raw, err := p.fetch(ctx, path, checkpoint, id)
	if err != nil {
		return err
	}

	warnings := []Warning{}
	if p.Source == "BMKG" {
		if warnings, err = p.fetchWarnings(ctx, checkpoint, id); err != nil {
			return err
		}
	}

	now := time.Now().UTC()
	events := []HazardEvent{}
	seen := map[string]bool{}

	for _, o := range raw {
		// Rejected records still advance the checkpoint, so one bad record cannot stall the source.
		if t, err := timestamp(o, timeField); err == nil && (checkpoint == nil || t.After(*checkpoint)) {
			checkpoint = &t
		}

		var h HazardEvent
		if p.Source == "BMKG" {
			h, err = MapSeismic(o, warnings, now)
		} else {
			h, err = MapVolcanic(o, now)
		}

		if err == nil && seen[h.SourceRefID] {
			err = errors.New("duplicate source ID")
		}

		if err != nil {
			ref, _ := textField(o, idField)
			p.reject(id, ref, err.Error())

			continue
		}

		seen[h.SourceRefID] = true
		events = append(events, h)
	}

	refs := []string{}

	for _, w := range warnings {
		if !seen[w.RelatedID] {
			refs = append(refs, w.RelatedID)
		}
	}

	if len(refs) > 0 {
		start = time.Now()
		old, err := p.Store.WarningEvents(ctx, refs)
		logDatabaseCall(p.Log, id, "warning_events", start, err)

		if err != nil {
			return errors.New("warning lookup failed")
		}

		for _, h := range old {
			events = append(events, ApplyWarnings(h, warnings))
		}
	}

	start = time.Now()
	changed, err := p.Store.Save(ctx, p.Source, events, checkpoint, time.Now().UTC())
	logDatabaseCall(p.Log, id, "save", start, err)

	if err != nil {
		return errors.New("canonical commit failed")
	}

	for _, h := range changed {
		select {
		case p.Pending <- Publication{h, id}:
		default:
			p.Log.Error(
				"publication_dropped",
				"source", p.Source,
				"hazard_id", h.HazardID,
				"correlation_id", id,
				"reason", "queue_full",
			)
		}
	}

	p.Log.Info(
		"poll_committed",
		"source", p.Source,
		"correlation_id", id,
		"published_revisions", len(changed),
		"mapped", len(events),
		"checkpoint", checkpoint,
	)

	return nil
}

// Two fetches, events and warnings, each with every retry and its backoff, plus slack for the database.
func (p *Poller) pollBudget() time.Duration {
	fetch := p.Timeout*time.Duration(p.Retries+1) + p.Backoff*time.Duration(1<<p.Retries)

	return 2*fetch + 5*time.Second
}

func (p *Poller) Run(ctx context.Context) {
	for ctx.Err() == nil {
		id := correlationID()
		pollCtx, cancel := context.WithTimeout(ctx, p.pollBudget())
		err := p.Poll(pollCtx, id)

		cancel()

		if err != nil && ctx.Err() == nil {
			failureCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			start := time.Now()
			markErr := p.Store.Failure(failureCtx, p.Source, time.Now().UTC())
			logDatabaseCall(p.Log, id, "failure", start, markErr)
			stop()
			p.Log.Warn(
				"poll_failed",
				"source", p.Source,
				"correlation_id", id,
				"reason", err.Error(),
				"freshness_saved", markErr == nil,
			)
		}

		if !pause(ctx, p.Interval) {
			return
		}
	}
}
