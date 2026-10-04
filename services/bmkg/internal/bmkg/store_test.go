package bmkg

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestSeedMeetsContract(t *testing.T) {
	store, err := LoadSeed()
	if err != nil {
		t.Fatal(err)
	}
	events := store.Events(nil)
	if len(events) < MinSeedRecords {
		t.Fatalf("seed has %d events", len(events))
	}
	magnitudes := map[float64]bool{}
	for _, e := range events {
		magnitudes[e.Magnitude] = true
	}
	for _, m := range []float64{4.9, 5.0, 6.49, 6.5} {
		if !magnitudes[m] {
			t.Fatalf("seed misses magnitude boundary %v", m)
		}
	}
	perEvent := map[string][]string{}
	for _, w := range store.Warnings(nil) {
		perEvent[w.RelatedEventID] = append(perEvent[w.RelatedEventID], w.ThreatLevel)
	}
	var several, unwarned, calm bool
	for _, e := range events {
		several = several || len(perEvent[e.EventID]) > 1
		unwarned = unwarned || e.PotentialTsunami && len(perEvent[e.EventID]) == 0
		calm = calm || !e.PotentialTsunami
	}
	if !several || !unwarned || !calm {
		t.Fatalf("seed needs several warnings on one event (%v), a potential tsunami without warning (%v), and events without potential tsunami (%v)", several, unwarned, calm)
	}
	levels := map[string]bool{}
	for _, ls := range perEvent {
		for _, l := range ls {
			levels[l] = true
		}
	}
	if len(levels) != len(threatLevels) {
		t.Fatalf("seed covers threat levels %v", levels)
	}
}

func TestStoreRejectsInvalidRecordsWithoutPartialWrites(t *testing.T) {
	store := NewStore()
	risky := SeismicEvent{EventID: "e1", PotentialTsunami: true}
	if err := store.Add(risky, TsunamiWarning{WarningID: "w1", RelatedEventID: "e1", ThreatLevel: "Siaga"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		event    SeismicEvent
		warnings []TsunamiWarning
	}{
		{"missing event ID", SeismicEvent{}, nil},
		{"duplicate event ID", SeismicEvent{EventID: "e1"}, nil},
		{"warning on calm event", SeismicEvent{EventID: "e2"}, []TsunamiWarning{{WarningID: "w2", RelatedEventID: "e2", ThreatLevel: "Awas"}}},
		{"invalid threat level", SeismicEvent{EventID: "e3", PotentialTsunami: true}, []TsunamiWarning{{WarningID: "w3", RelatedEventID: "e3", ThreatLevel: "Bahaya"}}},
		{"lowercase threat level", SeismicEvent{EventID: "e4", PotentialTsunami: true}, []TsunamiWarning{{WarningID: "w4", RelatedEventID: "e4", ThreatLevel: "awas"}}},
		{"warning for another event", SeismicEvent{EventID: "e5", PotentialTsunami: true}, []TsunamiWarning{{WarningID: "w5", RelatedEventID: "e1", ThreatLevel: "Awas"}}},
		{"stored warning ID", SeismicEvent{EventID: "e6", PotentialTsunami: true}, []TsunamiWarning{{WarningID: "w1", RelatedEventID: "e6", ThreatLevel: "Awas"}}},
		{"repeated warning ID in one batch", SeismicEvent{EventID: "e7", PotentialTsunami: true}, []TsunamiWarning{{WarningID: "w7", RelatedEventID: "e7", ThreatLevel: "Awas"}, {WarningID: "w7", RelatedEventID: "e7", ThreatLevel: "Siaga"}}},
		{"missing warning ID", SeismicEvent{EventID: "e8", PotentialTsunami: true}, []TsunamiWarning{{RelatedEventID: "e8", ThreatLevel: "Awas"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.Add(tc.event, tc.warnings...); err == nil {
				t.Fatal("invalid record accepted")
			}
			if len(store.Events(nil)) != 1 || len(store.Warnings(nil)) != 1 {
				t.Fatal("rejected add left a partial write")
			}
		})
	}
	if store.AddWarning(TsunamiWarning{WarningID: "w9", RelatedEventID: "missing", ThreatLevel: "Awas"}) == nil {
		t.Fatal("warning for unknown event accepted")
	}
}

func TestSinceIsInclusiveAndOrdered(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	// Added out of time order, with two events sharing a timestamp.
	for i, offset := range []int{3, 1, 2, 2, 0} {
		id := fmt.Sprintf("e%d", i)
		if err := store.Add(SeismicEvent{EventID: id, OccurredAt: base.Add(time.Duration(offset) * time.Minute), PotentialTsunami: true}, TsunamiWarning{WarningID: "w" + id, RelatedEventID: id, ThreatLevel: "Waspada"}); err != nil {
			t.Fatal(err)
		}
	}
	since := base.Add(2 * time.Minute)
	ids := func(events []SeismicEvent) (out []string) {
		for _, e := range events {
			out = append(out, e.EventID)
		}
		return out
	}
	if got := ids(store.Events(&since)); !slices.Equal(got, []string{"e2", "e3", "e0"}) {
		t.Fatalf("events since %s: %v", since, got)
	}
	if got := ids(store.Events(nil)); !slices.Equal(got, []string{"e4", "e1", "e2", "e3", "e0"}) {
		t.Fatalf("all events: %v", got)
	}
	var warned []string
	for _, w := range store.Warnings(&since) {
		warned = append(warned, w.RelatedEventID)
	}
	if !slices.Equal(warned, []string{"e2", "e3", "e0"}) {
		t.Fatalf("warnings since %s relate to %v", since, warned)
	}
	future := base.Add(time.Hour)
	if store.Events(&future) == nil || len(store.Events(&future)) != 0 || store.Warnings(&future) == nil {
		t.Fatal("an empty result must be an empty slice, which encodes as []")
	}
}

func TestStoreConcurrentAddAndRead(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup
	for writer := range 4 {
		wg.Go(func() {
			for i := range 50 {
				id := fmt.Sprintf("e%d-%d", writer, i)
				_ = store.Add(SeismicEvent{EventID: id, OccurredAt: time.Now(), PotentialTsunami: true}, TsunamiWarning{WarningID: "w" + id, RelatedEventID: id, ThreatLevel: "Siaga"})
			}
		})
		wg.Go(func() {
			for range 50 {
				store.Events(nil)
				store.Warnings(nil)
			}
		})
	}
	wg.Wait()
	if len(store.Events(nil)) != 200 || len(store.Warnings(nil)) != 200 {
		t.Fatalf("stored %d events and %d warnings", len(store.Events(nil)), len(store.Warnings(nil)))
	}
}
