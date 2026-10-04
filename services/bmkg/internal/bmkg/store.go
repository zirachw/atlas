package bmkg

import (
	"cmp"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

//go:embed fixtures/*.json
var fixtures embed.FS

const MinSeedRecords = 20

type Store struct {
	mu       sync.RWMutex
	events   []SeismicEvent
	byID     map[string]SeismicEvent
	warnings []TsunamiWarning
	warnIDs  map[string]bool
}

func NewStore() *Store {
	return &Store{byID: map[string]SeismicEvent{}, warnIDs: map[string]bool{}}
}

func LoadSeed() (*Store, error) {
	var events []SeismicEvent
	var warnings []TsunamiWarning
	if err := readFixture("fixtures/seismic-events.json", &events); err != nil {
		return nil, err
	}
	if err := readFixture("fixtures/tsunami-warnings.json", &warnings); err != nil {
		return nil, err
	}
	if len(events) < MinSeedRecords {
		return nil, fmt.Errorf("seed has %d seismic events, need at least %d", len(events), MinSeedRecords)
	}
	s := NewStore()
	for _, e := range events {
		if err := s.Add(e); err != nil {
			return nil, err
		}
	}
	for _, w := range warnings {
		if err := s.AddWarning(w); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func readFixture(name string, dst any) error {
	raw, err := fixtures.ReadFile(name)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// Add stores nothing when the event or any of its warnings is invalid.
func (s *Store) Add(event SeismicEvent, warnings ...TsunamiWarning) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.EventID == "" {
		return errors.New("event_id is required")
	}
	if _, exists := s.byID[event.EventID]; exists {
		return fmt.Errorf("duplicate event_id %s", event.EventID)
	}
	batch := map[string]bool{}
	for _, w := range warnings {
		if err := s.checkWarning(w, event); err != nil {
			return err
		}
		if batch[w.WarningID] {
			return fmt.Errorf("duplicate warning_id %s", w.WarningID)
		}
		batch[w.WarningID] = true
	}
	s.events = append(s.events, event)
	s.byID[event.EventID] = event
	for _, w := range warnings {
		s.warnings = append(s.warnings, w)
		s.warnIDs[w.WarningID] = true
	}
	return nil
}

func (s *Store) AddWarning(w TsunamiWarning) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.byID[w.RelatedEventID]
	if !ok {
		return fmt.Errorf("warning %s references unknown event %s", w.WarningID, w.RelatedEventID)
	}
	if err := s.checkWarning(w, event); err != nil {
		return err
	}
	s.warnings = append(s.warnings, w)
	s.warnIDs[w.WarningID] = true
	return nil
}

func (s *Store) checkWarning(w TsunamiWarning, event SeismicEvent) error {
	switch {
	case w.WarningID == "":
		return errors.New("warning_id is required")
	case s.warnIDs[w.WarningID]:
		return fmt.Errorf("duplicate warning_id %s", w.WarningID)
	case w.RelatedEventID != event.EventID:
		return fmt.Errorf("warning %s does not relate to event %s", w.WarningID, event.EventID)
	case !event.PotentialTsunami:
		return fmt.Errorf("warning %s relates to event %s without potential_tsunami", w.WarningID, event.EventID)
	case !threatLevels[w.ThreatLevel]:
		return fmt.Errorf("warning %s has invalid threat_level %q", w.WarningID, w.ThreatLevel)
	}
	return nil
}

// Events includes an event whose occurred_at equals since.
func (s *Store) Events(since *time.Time) []SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SeismicEvent{}
	for _, e := range s.events {
		if since == nil || !e.OccurredAt.Before(*since) {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b SeismicEvent) int {
		return cmp.Or(a.OccurredAt.Compare(b.OccurredAt), cmp.Compare(a.EventID, b.EventID))
	})
	return out
}

// Warnings have no timestamp, so since applies to the related event's occurred_at.
func (s *Store) Warnings(since *time.Time) []TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()
	eventTime := func(w TsunamiWarning) time.Time { return s.byID[w.RelatedEventID].OccurredAt }
	out := []TsunamiWarning{}
	for _, w := range s.warnings {
		if since == nil || !eventTime(w).Before(*since) {
			out = append(out, w)
		}
	}
	slices.SortFunc(out, func(a, b TsunamiWarning) int {
		return cmp.Or(eventTime(a).Compare(eventTime(b)), cmp.Compare(a.WarningID, b.WarningID))
	})
	return out
}
