package bmkg

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestSeedMeetsContract(t *testing.T) {
	events, warnings, err := seedRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < MinSeedRecords {
		t.Fatalf("seed has %d events", len(events))
	}
	magnitudes := map[float64]bool{}
	levels := map[string]bool{}
	var several, unwarned, calm bool
	for _, e := range events {
		magnitudes[e.Magnitude] = true
		if err := validate(e, warnings[e.EventID]); err != nil {
			t.Fatalf("seed event %s: %v", e.EventID, err)
		}
		for _, w := range warnings[e.EventID] {
			levels[w.ThreatLevel] = true
		}
		several = several || len(warnings[e.EventID]) > 1
		unwarned = unwarned || e.PotentialTsunami && len(warnings[e.EventID]) == 0
		calm = calm || !e.PotentialTsunami
	}
	for _, m := range []float64{4.9, 5.0, 6.49, 6.5} {
		if !magnitudes[m] {
			t.Fatalf("seed misses magnitude boundary %v", m)
		}
	}
	if !several || !unwarned || !calm || len(levels) != len(threatLevels) {
		t.Fatalf("seed needs several warnings on one event (%v), a potential tsunami without warning (%v), calm events (%v), and every threat level %v", several, unwarned, calm, levels)
	}
}

func integrationStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := os.Getenv("BMKG_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set BMKG_TEST_DATABASE_URL or run make integration")
	}
	s, err := OpenStore(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(context.Background(), "TRUNCATE tsunami_warnings, seismic_events"); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestIntegrationStorePersistsSeedAndFiltersSince(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	// Loading twice proves seeding on every start never duplicates records.
	for range 2 {
		if err := LoadFixtures(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	seed, seedWarnings, _ := seedRecords()
	all, err := s.Events(ctx, nil)
	if err != nil || len(all) != len(seed) {
		t.Fatalf("stored %d events after two seed loads, want %d: %v", len(all), len(seed), err)
	}
	if !slices.IsSortedFunc(all, func(a, b SeismicEvent) int { return a.OccurredAt.Compare(b.OccurredAt) }) {
		t.Fatal("events are not ordered by occurred_at")
	}
	pivot := all[5].OccurredAt
	since, _ := s.Events(ctx, &pivot)
	if len(since) != len(all)-5 || since[0].EventID != all[5].EventID {
		t.Fatalf("since at a stored time returned %d events starting at %s", len(since), since[0].EventID)
	}
	byEvent := map[string]time.Time{}
	for _, e := range all {
		byEvent[e.EventID] = e.OccurredAt
	}
	warnings, _ := s.Warnings(ctx, &pivot)
	for _, w := range warnings {
		if byEvent[w.RelatedEventID].Before(pivot) {
			t.Fatalf("warning %s relates to an event before since", w.WarningID)
		}
	}
	allWarnings, _ := s.Warnings(ctx, nil)
	total := 0
	for _, ws := range seedWarnings {
		total += len(ws)
	}
	if len(allWarnings) != total || len(warnings) == 0 || len(warnings) >= len(allWarnings) {
		t.Fatalf("warnings stored %d of %d, since kept %d", len(allWarnings), total, len(warnings))
	}
	future := time.Now().Add(time.Hour)
	if empty, err := s.Events(ctx, &future); err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("an empty result must be an empty slice, which encodes as []")
	}

	// A warning the database rejects rolls back its event too.
	broken := SeismicEvent{EventID: "atomic-1", Magnitude: 7, OccurredAt: time.Now(), PotentialTsunami: true}
	if err := s.Add(ctx, broken, TsunamiWarning{WarningID: "atomic-w1", RelatedEventID: "atomic-1", ThreatLevel: "Awas"}); err == nil {
		t.Fatal("a warning without affected_zones was stored")
	}
	if after, _ := s.Events(ctx, nil); len(after) != len(all) {
		t.Fatal("a rejected warning left its event stored")
	}

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			e := SeismicEvent{EventID: fmt.Sprintf("concurrent-%d", i), Magnitude: 4, RegionName: "Laut Banda", OccurredAt: time.Now()}
			if err := s.Add(ctx, e); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	// A reopened store is what a restarted mock sees.
	s.Close()
	reopened, err := OpenStore(ctx, os.Getenv("BMKG_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if after, _ := reopened.Events(ctx, nil); len(after) != len(all)+20 {
		t.Fatalf("restart kept %d events, want %d", len(after), len(all)+20)
	}
}
