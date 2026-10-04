package pvmbg

import (
	"cmp"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

//go:embed fixtures/volcanic-reports.json
var seedReports []byte

const MinSeedRecords = 20

type Store struct {
	mu      sync.RWMutex
	reports []VolcanicReport
	ids     map[string]bool
}

func NewStore() *Store { return &Store{ids: map[string]bool{}} }

func LoadSeed() (*Store, error) {
	var reports []VolcanicReport
	if err := json.Unmarshal(seedReports, &reports); err != nil {
		return nil, fmt.Errorf("volcanic seed: %w", err)
	}
	if len(reports) < MinSeedRecords {
		return nil, fmt.Errorf("seed has %d reports, need at least %d", len(reports), MinSeedRecords)
	}
	s := NewStore()
	for _, r := range reports {
		if err := s.Add(r); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Add(r VolcanicReport) error {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[r.ReportID] {
		return fmt.Errorf("duplicate report_id %s", r.ReportID)
	}
	s.ids[r.ReportID] = true
	s.reports = append(s.reports, r)
	return nil
}

// Reports includes a report whose reported_at equals since.
func (s *Store) Reports(since *time.Time) []VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []VolcanicReport{}
	for _, r := range s.reports {
		if since == nil || !r.ReportedAt.Before(*since) {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b VolcanicReport) int {
		return cmp.Or(a.ReportedAt.Compare(b.ReportedAt), cmp.Compare(a.ReportID, b.ReportID))
	})
	return out
}
