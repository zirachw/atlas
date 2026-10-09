package pvmbg

import "testing"

func TestValidateRejectsInvalidReports(t *testing.T) {
	high, low, edge := 1.01, -0.01, 1.0
	if err := validate(VolcanicReport{ReportID: "r0", VolcanoID: "semeru", AlertLevel: "Awas", ConfidenceLevel: &edge}); err != nil {
		t.Fatal("confidence_level of exactly 1 rejected:", err)
	}
	for _, tc := range []struct {
		name   string
		report VolcanicReport
	}{
		{"missing ID", VolcanicReport{VolcanoID: "merapi", AlertLevel: "Normal"}},
		{"unknown volcano", VolcanicReport{ReportID: "r2", VolcanoID: "bromo", AlertLevel: "Normal"}},
		{"invalid alert level", VolcanicReport{ReportID: "r3", VolcanoID: "merapi", AlertLevel: "Bahaya"}},
		{"uppercase alert level", VolcanicReport{ReportID: "r4", VolcanoID: "merapi", AlertLevel: "AWAS"}},
		{"negative eruptions", VolcanicReport{ReportID: "r5", VolcanoID: "merapi", AlertLevel: "Siaga", EruptionCount24h: -1}},
		{"negative ash height", VolcanicReport{ReportID: "r6", VolcanoID: "merapi", AlertLevel: "Siaga", AshColumnHeightM: -1}},
		{"confidence above 1", VolcanicReport{ReportID: "r7", VolcanoID: "merapi", AlertLevel: "Siaga", ConfidenceLevel: &high}},
		{"confidence below 0", VolcanicReport{ReportID: "r8", VolcanoID: "merapi", AlertLevel: "Siaga", ConfidenceLevel: &low}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if validate(tc.report) == nil {
				t.Fatal("invalid report accepted")
			}
		})
	}
}
