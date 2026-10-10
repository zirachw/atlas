package bmkg

import (
	"errors"
	"fmt"
	"time"
)

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

var threatLevels = map[string]bool{"Waspada": true, "Siaga": true, "Awas": true}

// TsunamiWarning is issued only for events where potential_tsunami = true.
func validate(event SeismicEvent, warnings []TsunamiWarning) error {
	if event.EventID == "" {
		return errors.New("event_id is required")
	}

	seen := map[string]bool{}

	for _, w := range warnings {
		switch {
		case w.WarningID == "":
			return errors.New("warning_id is required")
		case seen[w.WarningID]:
			return fmt.Errorf("duplicate warning_id %s", w.WarningID)
		case w.RelatedEventID != event.EventID:
			return fmt.Errorf("warning %s does not relate to event %s", w.WarningID, event.EventID)
		case !event.PotentialTsunami:
			return fmt.Errorf("warning %s relates to event %s without potential_tsunami", w.WarningID, event.EventID)
		case !threatLevels[w.ThreatLevel]:
			return fmt.Errorf("warning %s has invalid threat_level %q", w.WarningID, w.ThreatLevel)
		}

		seen[w.WarningID] = true
	}

	return nil
}
