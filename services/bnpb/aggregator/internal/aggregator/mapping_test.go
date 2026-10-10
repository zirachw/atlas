package aggregator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []Object {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("../../testdata", name))
	if err != nil {
		t.Fatal(err)
	}

	var out []Object
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
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

	warnings, err := MapWarnings(fixture(t, "warnings.json"))
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"NORMAL", "WASPADA", "WASPADA", "SIAGA", "AWAS"}

	for i, o := range records {
		h, err := MapSeismic(o, warnings, time.Now())
		if err != nil {
			t.Fatal(err)
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

		ws, err := MapWarnings([]Object{w})
		if err != nil {
			t.Fatal(err)
		}

		h, err := MapSeismic(o, ws, time.Now())
		if err != nil || h.Severity != map[string]string{"Waspada": "WASPADA", "Siaga": "SIAGA", "Awas": "AWAS"}[s] {
			t.Fatal(h, err)
		}
	}

	low := clone(fixture(t, "warnings.json")[0])
	low["warning_id"] = json.RawMessage(`"warning-2"`)
	low["threat_level"] = json.RawMessage(`"Waspada"`)

	ws, err := MapWarnings([]Object{low, fixture(t, "warnings.json")[0]})
	if err != nil {
		t.Fatal(err)
	}

	h, err := MapSeismic(o, ws, time.Now())
	if err != nil || h.Severity != "AWAS" {
		t.Fatal(h, err)
	}

	var joined []Object
	if err = json.Unmarshal(h.Attributes["tsunami_warnings"], &joined); err != nil || len(joined) != 2 {
		t.Fatal(joined, err)
	}

	o["potential_tsunami"] = json.RawMessage("false")

	h, err = MapSeismic(o, ws, time.Now())
	if err != nil || h.Severity != "SIAGA" {
		t.Fatal(h, err)
	}
}

func TestVolcanicAdditiveValuesAndStableIdentity(t *testing.T) {
	old := fixture(t, "volcanic-v1.json")[0]
	newer := fixture(t, "volcanic-v2.json")[0]

	h, err := MapVolcanic(old, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	h2, err := MapVolcanic(newer, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if h.AreaName != "Merapi" || h.Severity != "SIAGA" || h.Latitude != -7.54 {
		t.Fatal(h)
	}

	if _, ok := h.Attributes["confidence_level"]; ok {
		t.Fatal("old record got new field")
	}

	if string(h2.Attributes["confidence_level"]) != "0.85" ||
		string(h2.Attributes["observations"]) != string(newer["observations"]) {
		t.Fatal(h2.Attributes)
	}

	old["extra"] = json.RawMessage(`{"integer":9007199254740993,"nullable":null,"array":[true,"text"]}`)

	same, err := MapVolcanic(old, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if same.HazardID != h.HazardID || string(same.Attributes["extra"]) != string(old["extra"]) {
		t.Fatal("identity or JSON number changed")
	}

	for _, s := range []string{"Normal", "Waspada", "Siaga", "Awas"} {
		o := clone(old)
		o["alert_level"], _ = json.Marshal(s)
		v, err := MapVolcanic(o, time.Now())

		if err != nil || v.Severity != strings.ToUpper(s) {
			t.Fatal(s, v, err)
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

	if h.Severity != "AWAS" ||
		string(h.Attributes["source.source.tsunami_warnings"]) != string(o["tsunami_warnings"]) ||
		string(h.Attributes["source.tsunami_warnings"]) != string(o["source.tsunami_warnings"]) {
		t.Fatal("source extensions lost during warning join", h.Attributes)
	}

	var joined []Object
	if err := json.Unmarshal(h.Attributes["tsunami_warnings"], &joined); err != nil || len(joined) != 1 {
		t.Fatal("missing generated warning join", joined, err)
	}

	h = ApplyWarnings(h, nil)
	if _, exists := h.Attributes["tsunami_warnings"]; exists || h.Severity != "SIAGA" ||
		string(h.Attributes["source.source.tsunami_warnings"]) != string(o["tsunami_warnings"]) {
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
		{
			"seismic",
			fixture(t, "seismic.json")[0],
			func(o Object) error { _, err := MapSeismic(o, nil, time.Now()); return err },
			[]string{
				"event_id", "magnitude", "depth_km", "epicenter_lat",
				"epicenter_lon", "region_name", "occurred_at", "potential_tsunami",
			},
		},
		{
			"volcanic",
			fixture(t, "volcanic-v1.json")[0],
			func(o Object) error { _, err := MapVolcanic(o, time.Now()); return err },
			[]string{
				"report_id", "volcano_id", "alert_level",
				"eruption_count_24h", "ash_column_height_m", "reported_at",
			},
		},
		{
			"warning",
			fixture(t, "warnings.json")[0],
			func(o Object) error { _, err := MapWarnings([]Object{o}); return err },
			[]string{"warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"},
		},
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

	for key, values := range map[string][]string{
		"confidence_level":   {"null", "-0.1", "1.01", `"0.8"`},
		"eruption_count_24h": {"1.5", "-1"},
		"volcano_id":         {`"unknown"`},
		"reported_at":        {`"2026-09-30"`},
		"alert_level":        {`"Unknown"`},
	} {
		for _, bad := range values {
			o := clone(base)

			o[key] = json.RawMessage(bad)
			if _, err := MapVolcanic(o, time.Now()); err == nil {
				t.Fatalf("accepted %s=%s", key, bad)
			}
		}
	}
}
