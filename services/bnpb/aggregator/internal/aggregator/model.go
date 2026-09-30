package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type HazardEvent struct {
	HazardID    string                     `json:"hazard_id"`
	Source      string                     `json:"source"`
	SourceRefID string                     `json:"source_ref_id"`
	HazardType  string                     `json:"hazard_type"`
	Severity    string                     `json:"severity"`
	AreaName    string                     `json:"area_name"`
	Latitude    float64                    `json:"latitude"`
	Longitude   float64                    `json:"longitude"`
	OccurredAt  time.Time                  `json:"occurred_at"`
	IngestedAt  time.Time                  `json:"ingested_at"`
	Attributes  map[string]json.RawMessage `json:"attributes"`
}

type Freshness struct {
	Source        string     `json:"source"`
	Status        string     `json:"status"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

type Query struct {
	Source, HazardType, Severity string
	Since                        *time.Time
	Limit                        int
}

type Response struct {
	Data    []HazardEvent `json:"data"`
	Sources []Freshness   `json:"sources"`
}

func hazardID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + "\x00" + ref))
	return hex.EncodeToString(sum[:])
}

func sources(q Query) []string {
	if q.Source != "" {
		return []string{q.Source}
	}
	if q.HazardType == "SEISMIC" {
		return []string{"BMKG"}
	}
	if q.HazardType == "VOLCANIC" {
		return []string{"PVMBG"}
	}
	return []string{"BMKG", "PVMBG"}
}
