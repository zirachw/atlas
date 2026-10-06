package pvmbg

import "time"

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
