//go:build demo

package demo

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	severities = []string{"NORMAL", "WASPADA", "SIAGA", "AWAS"}
	hazardID   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func attributes(h map[string]any) map[string]any {
	a, _ := h["attributes"].(map[string]any)
	return a
}

// validEvent checks one HazardEvent against the canonical contract.
func validEvent(t *testing.T, h map[string]any) {
	t.Helper()
	source, _ := h["source"].(string)
	wantType := map[string]string{"BMKG": "SEISMIC", "PVMBG": "VOLCANIC"}[source]
	id, _ := h["hazard_id"].(string)
	severity, _ := h["severity"].(string)
	area, _ := h["area_name"].(string)
	_, lat := h["latitude"].(float64)
	_, lon := h["longitude"].(float64)
	switch {
	case wantType == "" || h["hazard_type"] != wantType:
		t.Errorf("source %q with hazard_type %v", source, h["hazard_type"])
	case !hazardID.MatchString(id) || h["source_ref_id"] == "":
		t.Errorf("invalid identity %v / %v", h["hazard_id"], h["source_ref_id"])
	case !slices.Contains(severities, severity) || area == "" || !lat || !lon:
		t.Errorf("invalid severity, area, or coordinates in %v", h)
	case attributes(h) == nil:
		t.Errorf("attributes missing in %v", h)
	}
	for _, field := range []string{"occurred_at", "ingested_at"} {
		if _, err := time.Parse(time.RFC3339Nano, toString(h[field])); err != nil {
			t.Errorf("%s %v is not ISO 8601", field, h[field])
		}
	}
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}

func checkMapping(t *testing.T, ev map[string]any) {
	s.ready(t)
	// Seed records have known inputs, so their mapped severity is exact.
	want := map[string]string{
		"bmkg-seed-001":  "SEISMIC NORMAL",  // magnitude 4.9, no warning
		"bmkg-seed-002":  "SEISMIC WASPADA", // magnitude 5.0, no warning
		"bmkg-seed-004":  "SEISMIC SIAGA",   // magnitude 6.5, no warning
		"bmkg-seed-006":  "SEISMIC AWAS",    // Waspada and Awas warnings
		"bmkg-seed-008":  "SEISMIC WASPADA", // magnitude 7.1, the Waspada warning wins
		"bmkg-seed-009":  "SEISMIC SIAGA",   // potential tsunami without warning
		"pvmbg-seed-001": "VOLCANIC NORMAL Merapi -7.54 110.44",
	}
	refs := []string{}
	for ref := range want {
		refs = append(refs, "'"+ref+"'")
	}
	rows := s.sql(t, "SELECT source_ref_id, hazard_type || ' ' || severity || CASE WHEN source='PVMBG' THEN ' ' || area_name || ' ' || latitude || ' ' || longitude ELSE '' END FROM hazard_events WHERE source_ref_id IN ("+strings.Join(refs, ",")+")")
	got := map[string]string{}
	for _, row := range strings.Split(rows, "\n") {
		if ref, mapped, ok := strings.Cut(row, "|"); ok {
			got[ref] = mapped
		}
	}
	for ref, mapped := range want {
		if got[ref] != mapped {
			t.Errorf("%s mapped to %q, want %q", ref, got[ref], mapped)
		}
	}
	if s.sql(t, "SELECT attributes ? 'tsunami_warnings' FROM hazard_events WHERE source_ref_id='bmkg-seed-006'") != "t" {
		t.Error("related warning fields missing from seismic attributes")
	}
	_, page := s.internal(t, "limit=200")
	seen := map[string]map[string]any{}
	for _, h := range page.Data {
		validEvent(t, h)
		seen[toString(h["source"])] = h
	}
	for source, keys := range map[string][]string{"BMKG": {"magnitude", "depth_km", "potential_tsunami"}, "PVMBG": {"eruption_count_24h", "ash_column_height_m"}} {
		if seen[source] == nil {
			t.Fatalf("no %s HazardEvent served", source)
		}
		for _, k := range keys {
			if _, ok := attributes(seen[source])[k]; !ok {
				t.Errorf("%s attributes miss %s", source, k)
			}
		}
	}
	ev["seed_mapping"] = got
	ev["sample"] = seen
}

func checkSchemaChange(t *testing.T, ev map[string]any) {
	s.ready(t)
	aggregator, pvmbg := s.containerID(t, "aggregator"), s.containerID(t, "pvmbg")
	since := time.Now().Add(-5 * time.Second)
	s.admin(t, "schema-version", `{"version":2}`)
	var fresh map[string]any
	eventually(t, 60*time.Second, "a PVMBG HazardEvent carrying confidence_level", func() bool {
		_, page := s.internal(t, "source=PVMBG&limit=50&since="+since.UTC().Format(time.RFC3339Nano))
		for _, h := range page.Data {
			if _, ok := attributes(h)["confidence_level"]; ok {
				fresh = h
				return true
			}
		}
		return false
	})
	if s.containerID(t, "aggregator") != aggregator || s.containerID(t, "pvmbg") != pvmbg {
		t.Error("a container restarted during the schema change")
	}
	validEvent(t, fresh)
	if _, ok := fresh["confidence_level"]; ok {
		t.Error("confidence_level leaked outside attributes")
	}
	if c, ok := attributes(fresh)["confidence_level"].(float64); !ok || c < 0 || c > 1 {
		t.Errorf("confidence_level %v is not a float in 0-1", attributes(fresh)["confidence_level"])
	}
	failed := s.logged(t, "aggregator", since, func(l map[string]any) bool { return l["msg"] == "poll_failed" && l["source"] == "PVMBG" })
	if len(failed) > 0 {
		t.Errorf("PVMBG polling failed after the schema change: %v", failed[0])
	}
	ev["containers_unchanged"] = []string{aggregator[:12], pvmbg[:12]}
	ev["new_event"] = fresh
}
