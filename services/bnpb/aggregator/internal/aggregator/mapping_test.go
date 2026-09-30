package aggregator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []Object {
	t.Helper()
	raw, e := os.ReadFile(filepath.Join("../../testdata", name))
	if e != nil {
		t.Fatal(e)
	}
	var out []Object
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatal(e)
	}
	return out
}
func clone(o Object) Object {
	out := Object{}
	for k, v := range o {
		out[k] = v
	}
	return out
}
func TestSeismicBoundariesAndWarnings(t *testing.T) {
	records := fixture(t, "seismic.json")
	warnings, e := MapWarnings(fixture(t, "warnings.json"))
	if e != nil {
		t.Fatal(e)
	}
	expected := []string{"NORMAL", "WASPADA", "WASPADA", "SIAGA", "AWAS"}
	for i, o := range records {
		h, e := MapSeismic(o, warnings, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if h.Severity != expected[i] {
			t.Fatalf("record %d severity %s", i, h.Severity)
		}
		if h.Source != "BMKG" || h.HazardType != "SEISMIC" || h.Latitude != -6 {
			t.Fatal(h)
		}
	}
	o := clone(records[4])
	o["magnitude"] = json.RawMessage("8.0")
	for _, s := range []string{"Waspada", "Siaga", "Awas"} {
		w := clone(fixture(t, "warnings.json")[0])
		w["threat_level"], _ = json.Marshal(s)
		ws, e := MapWarnings([]Object{w})
		if e != nil {
			t.Fatal(e)
		}
		h, e := MapSeismic(o, ws, time.Now())
		if e != nil || h.Severity != map[string]string{"Waspada": "WASPADA", "Siaga": "SIAGA", "Awas": "AWAS"}[s] {
			t.Fatal(h, e)
		}
	}
	low := clone(fixture(t, "warnings.json")[0])
	low["warning_id"] = json.RawMessage(`"warning-2"`)
	low["threat_level"] = json.RawMessage(`"Waspada"`)
	ws, e := MapWarnings([]Object{low, fixture(t, "warnings.json")[0]})
	if e != nil {
		t.Fatal(e)
	}
	h, e := MapSeismic(o, ws, time.Now())
	if e != nil || h.Severity != "AWAS" {
		t.Fatal(h, e)
	}
	var joined []Object
	if e = json.Unmarshal(h.Attributes["tsunami_warnings"], &joined); e != nil || len(joined) != 2 {
		t.Fatal(joined, e)
	}
	o["potential_tsunami"] = json.RawMessage("false")
	h, e = MapSeismic(o, ws, time.Now())
	if e != nil || h.Severity != "SIAGA" {
		t.Fatal(h, e)
	}
}
func TestVolcanicAdditiveValuesAndStableIdentity(t *testing.T) {
	old := fixture(t, "volcanic-v1.json")[0]
	newer := fixture(t, "volcanic-v2.json")[0]
	h, e := MapVolcanic(old, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	h2, e := MapVolcanic(newer, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if h.AreaName != "Merapi" || h.Severity != "SIAGA" || h.Latitude != -7.54 {
		t.Fatal(h)
	}
	if _, ok := h.Attributes["confidence_level"]; ok {
		t.Fatal("old record got new field")
	}
	if string(h2.Attributes["confidence_level"]) != "0.85" || string(h2.Attributes["observations"]) != string(newer["observations"]) {
		t.Fatal(h2.Attributes)
	}
	old["extra"] = json.RawMessage(`{"integer":9007199254740993,"nullable":null,"array":[true,"text"]}`)
	same, e := MapVolcanic(old, time.Now().Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if same.HazardID != h.HazardID || string(same.Attributes["extra"]) != string(old["extra"]) {
		t.Fatal("identity or JSON number changed")
	}
	for _, s := range []string{"Normal", "Waspada", "Siaga", "Awas"} {
		o := clone(old)
		o["alert_level"], _ = json.Marshal(s)
		v, e := MapVolcanic(o, time.Now())
		want, _ := level(s, true)
		if e != nil || v.Severity != want {
			t.Fatal(s, v, e)
		}
	}
}

func TestSeismicAdditiveWarningKeyCollision(t *testing.T) {
	o := clone(fixture(t, "seismic.json")[4])
	o["tsunami_warnings"] = json.RawMessage(`{"source_extension":9007199254740993}`)
	o["source.tsunami_warnings"] = json.RawMessage(`["existing extension"]`)
	warnings, err := MapWarnings(fixture(t, "warnings.json"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := MapSeismic(o, warnings, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if h.Severity != "AWAS" || string(h.Attributes["source.source.tsunami_warnings"]) != string(o["tsunami_warnings"]) || string(h.Attributes["source.tsunami_warnings"]) != string(o["source.tsunami_warnings"]) {
		t.Fatal("source extensions lost during warning join", h.Attributes)
	}
	var joined []Object
	if err := json.Unmarshal(h.Attributes["tsunami_warnings"], &joined); err != nil || len(joined) != 1 {
		t.Fatal("missing generated warning join", joined, err)
	}
	h = ApplyWarnings(h, nil)
	if _, exists := h.Attributes["tsunami_warnings"]; exists || h.Severity != "SIAGA" || string(h.Attributes["source.source.tsunami_warnings"]) != string(o["tsunami_warnings"]) {
		t.Fatal("warning reconciliation lost source extension", h.Attributes)
	}
}
func TestRequiredFieldsRejectMissingNullAndWrongType(t *testing.T) {
	cases := []struct {
		name      string
		record    Object
		mapRecord func(Object) error
		keys      []string
	}{
		{"seismic", fixture(t, "seismic.json")[0], func(o Object) error { _, e := MapSeismic(o, nil, time.Now()); return e }, []string{"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"}},
		{"volcanic", fixture(t, "volcanic-v1.json")[0], func(o Object) error { _, e := MapVolcanic(o, time.Now()); return e }, []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at"}},
		{"warning", fixture(t, "warnings.json")[0], func(o Object) error { _, e := MapWarnings([]Object{o}); return e }, []string{"warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"}},
	}
	for _, c := range cases {
		for _, key := range c.keys {
			for _, bad := range []string{"missing", "null", "{}"} {
				t.Run(c.name+"/"+key+"/"+bad, func(t *testing.T) {
					o := clone(c.record)
					if bad == "missing" {
						delete(o, key)
					} else {
						o[key] = json.RawMessage(bad)
					}
					if c.mapRecord(o) == nil {
						t.Fatal("accepted broken required field")
					}
				})
			}
		}
	}
}
func TestInvalidOptionalValuesAndVolcano(t *testing.T) {
	base := fixture(t, "volcanic-v1.json")[0]
	for key, values := range map[string][]string{"confidence_level": {"null", "-0.1", "1.01", `"0.8"`}, "eruption_count_24h": {"1.5", "-1"}, "volcano_id": {`"unknown"`}, "reported_at": {`"2026-09-30"`}, "alert_level": {`"Unknown"`}} {
		for _, bad := range values {
			o := clone(base)
			o[key] = json.RawMessage(bad)
			if _, e := MapVolcanic(o, time.Now()); e == nil {
				t.Fatalf("accepted %s=%s", key, bad)
			}
		}
	}
}
