package aggregator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Object map[string]json.RawMessage

type Volcano struct {
	Name                string
	Latitude, Longitude float64
}

type Warning struct {
	RelatedID string
	Severity  string
	Raw       Object
}

// Static demo references, not a complete national volcano catalogue.
var Volcanoes = map[string]Volcano{
	"merapi":   {"Merapi", -7.54, 110.44},
	"semeru":   {"Semeru", -8.11, 112.92},
	"krakatau": {"Krakatau", -6.10, 105.42},
}

// A tsunami warning has no Normal level, so only volcanic alerts map to NORMAL.
var (
	threatSeverity = map[string]string{"Waspada": "WASPADA", "Siaga": "SIAGA", "Awas": "AWAS"}
	alertSeverity  = map[string]string{"Normal": "NORMAL", "Waspada": "WASPADA", "Siaga": "SIAGA", "Awas": "AWAS"}
)

func required[T any](o Object, key string) (T, error) {
	var v T

	raw, ok := o[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return v, fmt.Errorf("required field %s", key)
	}

	if err := json.Unmarshal(raw, &v); err != nil {
		return v, fmt.Errorf("invalid field %s", key)
	}

	return v, nil
}

func textField(o Object, key string) (string, error) {
	v, err := required[string](o, key)
	if err == nil && strings.TrimSpace(v) == "" {
		err = fmt.Errorf("empty field %s", key)
	}

	return v, err
}

func number(o Object, key string, min, max float64) (float64, error) {
	v, err := required[float64](o, key)
	if err == nil && (math.IsNaN(v) || math.IsInf(v, 0) || v < min || v > max) {
		err = fmt.Errorf("out of range %s", key)
	}

	return v, err
}

func timestamp(o Object, key string) (time.Time, error) {
	s, err := textField(o, key)
	if err != nil {
		return time.Time{}, err
	}

	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return v, fmt.Errorf("invalid timestamp %s", key)
	}

	return v, nil
}

func extensions(o Object, mapped ...string) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(o))
	for k, v := range o {
		out[k] = v
	}

	for _, k := range mapped {
		delete(out, k)
	}

	return out
}

func severityOf(levels map[string]string, s string) (string, error) {
	if severity, ok := levels[s]; ok {
		return severity, nil
	}

	return "", fmt.Errorf("invalid alert or threat level")
}

func MapWarnings(records []Object) ([]Warning, error) {
	out := make([]Warning, 0, len(records))
	seen := map[string]bool{}

	for _, o := range records {
		id, w, err := mapWarning(o)
		if err != nil {
			return nil, err
		}

		if seen[id] {
			return nil, fmt.Errorf("duplicate warning ID")
		}

		seen[id] = true

		out = append(out, w)
	}

	return out, nil
}

func mapWarning(o Object) (string, Warning, error) {
	id, err := textField(o, "warning_id")
	if err != nil {
		return "", Warning{}, err
	}

	ref, err := textField(o, "related_event_id")
	if err != nil {
		return id, Warning{}, err
	}

	threat, err := textField(o, "threat_level")
	if err != nil {
		return id, Warning{}, err
	}

	severity, err := severityOf(threatSeverity, threat)
	if err != nil {
		return id, Warning{}, err
	}

	zones, err := required[[]string](o, "affected_zones")
	if err != nil {
		return id, Warning{}, err
	}

	for _, z := range zones {
		if strings.TrimSpace(z) == "" {
			return id, Warning{}, fmt.Errorf("invalid affected zones")
		}
	}

	if _, err = timestamp(o, "estimated_arrival"); err != nil {
		return id, Warning{}, err
	}

	return id, Warning{ref, severity, o}, nil
}

func ApplyWarnings(h HazardEvent, warnings []Warning) HazardEvent {
	var magnitude float64

	_ = json.Unmarshal(h.Attributes["magnitude"], &magnitude)

	h.Severity = "NORMAL"
	if magnitude >= 6.5 {
		h.Severity = "SIAGA"
	} else if magnitude >= 5 {
		h.Severity = "WASPADA"
	}

	var potential bool

	_ = json.Unmarshal(h.Attributes["potential_tsunami"], &potential)
	delete(h.Attributes, "tsunami_warnings")

	if !potential {
		return h
	}

	ranks := map[string]int{"WASPADA": 1, "SIAGA": 2, "AWAS": 3}
	selected := []Object{}
	rank := 0

	for _, w := range warnings {
		if w.RelatedID == h.SourceRefID {
			selected = append(selected, w.Raw)
			if ranks[w.Severity] > rank {
				h.Severity = w.Severity
				rank = ranks[w.Severity]
			}
		}
	}

	sort.Slice(
		selected,
		func(i, j int) bool { return string(selected[i]["warning_id"]) < string(selected[j]["warning_id"]) },
	)

	if len(selected) > 0 {
		h.Attributes["tsunami_warnings"], _ = json.Marshal(selected)
	}

	return h
}

func MapSeismic(o Object, warnings []Warning, now time.Time) (HazardEvent, error) {
	var h HazardEvent

	ref, err := textField(o, "event_id")
	if err != nil {
		return h, err
	}

	if _, err = number(o, "magnitude", 0, math.MaxFloat64); err != nil {
		return h, err
	}

	if _, err = number(o, "depth_km", 0, math.MaxFloat64); err != nil {
		return h, err
	}

	lat, err := number(o, "epicenter_lat", -90, 90)
	if err != nil {
		return h, err
	}

	lon, err := number(o, "epicenter_lon", -180, 180)
	if err != nil {
		return h, err
	}

	area, err := textField(o, "region_name")
	if err != nil {
		return h, err
	}

	when, err := timestamp(o, "occurred_at")
	if err != nil {
		return h, err
	}

	if _, err = required[bool](o, "potential_tsunami"); err != nil {
		return h, err
	}

	h = HazardEvent{
		HazardID:    hazardID("BMKG", ref),
		Source:      "BMKG",
		SourceRefID: ref,
		HazardType:  "SEISMIC",
		AreaName:    area,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  when,
		IngestedAt:  now,
		Attributes:  extensions(o, "event_id", "region_name", "epicenter_lat", "epicenter_lon", "occurred_at"),
		Revision:    1,
	}
	// Preserve a source field that collides with the generated warning join.
	if raw, ok := h.Attributes["tsunami_warnings"]; ok {
		key := "source.tsunami_warnings"
		for _, exists := h.Attributes[key]; exists; _, exists = h.Attributes[key] {
			key = "source." + key
		}

		h.Attributes[key] = raw
	}

	return ApplyWarnings(h, warnings), nil
}

func MapVolcanic(o Object, now time.Time) (HazardEvent, error) {
	var h HazardEvent

	ref, err := textField(o, "report_id")
	if err != nil {
		return h, err
	}

	volcano, err := textField(o, "volcano_id")
	if err != nil {
		return h, err
	}

	location, ok := Volcanoes[volcano]
	if !ok {
		return h, fmt.Errorf("unknown volcano")
	}

	alert, err := textField(o, "alert_level")
	if err != nil {
		return h, err
	}

	severity, err := severityOf(alertSeverity, alert)
	if err != nil {
		return h, err
	}

	count, err := required[int](o, "eruption_count_24h")
	if err != nil {
		return h, err
	}

	if count < 0 {
		return h, fmt.Errorf("negative eruption count")
	}

	if _, err = number(o, "ash_column_height_m", 0, math.MaxFloat64); err != nil {
		return h, err
	}

	when, err := timestamp(o, "reported_at")
	if err != nil {
		return h, err
	}

	if _, ok := o["confidence_level"]; ok {
		if _, err = number(o, "confidence_level", 0, 1); err != nil {
			return h, err
		}
	}

	h = HazardEvent{
		HazardID:    hazardID("PVMBG", ref),
		Source:      "PVMBG",
		SourceRefID: ref,
		HazardType:  "VOLCANIC",
		Severity:    severity,
		AreaName:    location.Name,
		Latitude:    location.Latitude,
		Longitude:   location.Longitude,
		OccurredAt:  when,
		IngestedAt:  now,
		Attributes:  extensions(o, "report_id", "reported_at", "alert_level"),
		Revision:    1,
	}

	return h, nil
}
