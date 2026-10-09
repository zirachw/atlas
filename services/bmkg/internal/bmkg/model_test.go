package bmkg

import "testing"

func TestValidateRejectsContractViolations(t *testing.T) {
	risky := SeismicEvent{EventID: "e1", PotentialTsunami: true}
	if err := validate(risky, []TsunamiWarning{{WarningID: "w1", RelatedEventID: "e1", ThreatLevel: "Siaga"}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		event    SeismicEvent
		warnings []TsunamiWarning
	}{
		{"missing event ID", SeismicEvent{}, nil},
		{"warning on calm event", SeismicEvent{EventID: "e2"}, []TsunamiWarning{{WarningID: "w2", RelatedEventID: "e2", ThreatLevel: "Awas"}}},
		{"invalid threat level", risky, []TsunamiWarning{{WarningID: "w3", RelatedEventID: "e1", ThreatLevel: "Bahaya"}}},
		{"lowercase threat level", risky, []TsunamiWarning{{WarningID: "w4", RelatedEventID: "e1", ThreatLevel: "awas"}}},
		{"warning for another event", risky, []TsunamiWarning{{WarningID: "w5", RelatedEventID: "e9", ThreatLevel: "Awas"}}},
		{"repeated warning ID", risky, []TsunamiWarning{{WarningID: "w6", RelatedEventID: "e1", ThreatLevel: "Awas"}, {WarningID: "w6", RelatedEventID: "e1", ThreatLevel: "Siaga"}}},
		{"missing warning ID", risky, []TsunamiWarning{{RelatedEventID: "e1", ThreatLevel: "Awas"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if validate(tc.event, tc.warnings) == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
}
