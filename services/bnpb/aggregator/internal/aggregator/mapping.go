package aggregator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type Object map[string]json.RawMessage

type Volcano struct {
	Name                string
	Latitude, Longitude float64
}

// Static demo references, not a complete national volcano catalogue.
var Volcanoes = map[string]Volcano{
	"merapi":   {"Merapi", -7.54, 110.44},
	"semeru":   {"Semeru", -8.11, 112.92},
	"krakatau": {"Krakatau", -6.10, 105.42},
}

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
	v, e := required[string](o, key)
	if e == nil && strings.TrimSpace(v) == "" {
		e = fmt.Errorf("empty field %s", key)
	}
	return v, e
}
func number(o Object, key string, min, max float64) (float64, error) {
	v, e := required[float64](o, key)
	if e == nil && (math.IsNaN(v) || math.IsInf(v, 0) || v < min || v > max) {
		e = fmt.Errorf("out of range %s", key)
	}
	return v, e
}
func timestamp(o Object, key string) (time.Time, error) {
	s, e := textField(o, key)
	if e != nil {
		return time.Time{}, e
	}
	v, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
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
func level(s string, normal bool) (string, error) {
	switch s {
	case "Normal":
		if normal {
			return "NORMAL", nil
		}
	case "Waspada":
		return "WASPADA", nil
	case "Siaga":
		return "SIAGA", nil
	case "Awas":
		return "AWAS", nil
	}
	return "", fmt.Errorf("invalid alert or threat level")
}

type Warning struct {
	RelatedID string
	Severity  string
	Raw       Object
}

func MapWarnings(records []Object) ([]Warning, error) {
	out := make([]Warning, 0, len(records))
	seen := map[string]bool{}
	for _, o := range records {
		id, w, e := mapWarning(o)
		if e != nil {
			return nil, e
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
	id, e := textField(o, "warning_id")
	if e != nil {
		return "", Warning{}, e
	}
	ref, e := textField(o, "related_event_id")
	if e != nil {
		return id, Warning{}, e
	}
	threat, e := textField(o, "threat_level")
	if e != nil {
		return id, Warning{}, e
	}
	severity, e := level(threat, false)
	if e != nil {
		return id, Warning{}, e
	}
	zones, e := required[[]string](o, "affected_zones")
	if e != nil {
		return id, Warning{}, e
	}
	for _, z := range zones {
		if strings.TrimSpace(z) == "" {
			return id, Warning{}, fmt.Errorf("invalid affected zones")
		}
	}
	if _, e = timestamp(o, "estimated_arrival"); e != nil {
		return id, Warning{}, e
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
	if len(selected) > 0 {
		h.Attributes["tsunami_warnings"], _ = json.Marshal(selected)
	}
	return h
}
func MapSeismic(o Object, warnings []Warning, now time.Time) (HazardEvent, error) {
	var h HazardEvent
	ref, e := textField(o, "event_id")
	if e != nil {
		return h, e
	}
	if _, e = number(o, "magnitude", 0, math.MaxFloat64); e != nil {
		return h, e
	}
	if _, e = number(o, "depth_km", 0, math.MaxFloat64); e != nil {
		return h, e
	}
	lat, e := number(o, "epicenter_lat", -90, 90)
	if e != nil {
		return h, e
	}
	lon, e := number(o, "epicenter_lon", -180, 180)
	if e != nil {
		return h, e
	}
	area, e := textField(o, "region_name")
	if e != nil {
		return h, e
	}
	when, e := timestamp(o, "occurred_at")
	if e != nil {
		return h, e
	}
	if _, e = required[bool](o, "potential_tsunami"); e != nil {
		return h, e
	}
	h = HazardEvent{hazardID("BMKG", ref), "BMKG", ref, "SEISMIC", "", area, lat, lon, when, now, extensions(o, "event_id", "region_name", "epicenter_lat", "epicenter_lon", "occurred_at")}
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
	ref, e := textField(o, "report_id")
	if e != nil {
		return h, e
	}
	volcano, e := textField(o, "volcano_id")
	if e != nil {
		return h, e
	}
	location, ok := Volcanoes[volcano]
	if !ok {
		return h, fmt.Errorf("unknown volcano")
	}
	alert, e := textField(o, "alert_level")
	if e != nil {
		return h, e
	}
	severity, e := level(alert, true)
	if e != nil {
		return h, e
	}
	count, e := required[int](o, "eruption_count_24h")
	if e != nil {
		return h, e
	}
	if count < 0 {
		return h, fmt.Errorf("negative eruption count")
	}
	if _, e = number(o, "ash_column_height_m", 0, math.MaxFloat64); e != nil {
		return h, e
	}
	when, e := timestamp(o, "reported_at")
	if e != nil {
		return h, e
	}
	if _, ok := o["confidence_level"]; ok {
		if _, e = number(o, "confidence_level", 0, 1); e != nil {
			return h, e
		}
	}
	h = HazardEvent{hazardID("PVMBG", ref), "PVMBG", ref, "VOLCANIC", severity, location.Name, location.Latitude, location.Longitude, when, now, extensions(o, "report_id", "reported_at", "alert_level")}
	return h, nil
}
