package pvmbg

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	// Set at creation under schema v2, never at serve time, so older reports keep their shape.
	ConfidenceLevel *float64 `json:"confidence_level,omitempty"`
}

var alertLevels = []string{"Normal", "Waspada", "Siaga", "Awas"}

// Must match the Aggregator's volcano table because it rejects reports with any other ID.
var volcanoIDs = []string{"merapi", "semeru", "krakatau"}

func validate(r VolcanicReport) error {
	switch {
	case r.ReportID == "":
		return errors.New("report_id is required")
	case !slices.Contains(volcanoIDs, r.VolcanoID):
		return fmt.Errorf("report %s has unknown volcano_id %q", r.ReportID, r.VolcanoID)
	case !slices.Contains(alertLevels, r.AlertLevel):
		return fmt.Errorf("report %s has invalid alert_level %q", r.ReportID, r.AlertLevel)
	case r.EruptionCount24h < 0 || r.AshColumnHeightM < 0:
		return fmt.Errorf("report %s has negative measurements", r.ReportID)
	case r.ConfidenceLevel != nil && (*r.ConfidenceLevel < 0 || *r.ConfidenceLevel > 1):
		return fmt.Errorf("report %s has confidence_level outside 0 to 1", r.ReportID)
	}

	return nil
}
