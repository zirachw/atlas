package bmkg

import "testing"

func TestValidateRejectsContractViolations(t *testing.T) {
	risky := SeismicEvent{EventID: "e1", PotentialTsunami: true}

	warn := func(id, event, level string) TsunamiWarning {
		return TsunamiWarning{WarningID: id, RelatedEventID: event, ThreatLevel: level}
	}
	if err := validate(risky, []TsunamiWarning{warn("w1", "e1", "Siaga")}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		event    SeismicEvent
		warnings []TsunamiWarning
	}{
		{"missing event ID", SeismicEvent{}, nil},
		{"warning on calm event", SeismicEvent{EventID: "e2"}, []TsunamiWarning{warn("w2", "e2", "Awas")}},
		{"invalid threat level", risky, []TsunamiWarning{warn("w3", "e1", "Bahaya")}},
		{"lowercase threat level", risky, []TsunamiWarning{warn("w4", "e1", "awas")}},
		{"warning for another event", risky, []TsunamiWarning{warn("w5", "e9", "Awas")}},
		{"repeated warning ID", risky, []TsunamiWarning{warn("w6", "e1", "Awas"), warn("w6", "e1", "Siaga")}},
		{"missing warning ID", risky, []TsunamiWarning{warn("", "e1", "Awas")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if validate(tc.event, tc.warnings) == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
}
