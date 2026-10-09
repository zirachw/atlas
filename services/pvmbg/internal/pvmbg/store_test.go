package pvmbg

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
	reports, err := seedRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) < MinSeedRecords {
		t.Fatalf("seed has %d reports", len(reports))
	}
	levels, volcanoes := map[string]bool{}, map[string]bool{}
	for _, r := range reports {
		if err := validate(r); err != nil {
			t.Fatalf("seed report %s: %v", r.ReportID, err)
		}
		if r.ConfidenceLevel != nil {
			t.Fatalf("seed report %s has confidence_level before any schema change", r.ReportID)
		}
		levels[r.AlertLevel], volcanoes[r.VolcanoID] = true, true
	}
	if len(levels) != len(alertLevels) || len(volcanoes) != len(volcanoIDs) {
		t.Fatalf("seed covers alert levels %v and volcanoes %v", levels, volcanoes)
	}
}

func integrationStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := os.Getenv("PVMBG_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set PVMBG_TEST_DATABASE_URL or run make integration")
	}
	s, err := OpenStore(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(context.Background(), "TRUNCATE volcanic_reports; UPDATE schema_state SET version=1"); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestIntegrationStorePersistsReportsAndSchemaVersion(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	// Loading twice proves seeding on every start never duplicates records.
	for range 2 {
		if err := LoadFixtures(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	seed, _ := seedRecords()
	all, err := s.Reports(ctx, nil)
	if err != nil || len(all) != len(seed) {
		t.Fatalf("stored %d reports after two seed loads, want %d: %v", len(all), len(seed), err)
	}
	if !slices.IsSortedFunc(all, func(a, b VolcanicReport) int { return a.ReportedAt.Compare(b.ReportedAt) }) {
		t.Fatal("reports are not ordered by reported_at")
	}
	pivot := all[7].ReportedAt
	since, _ := s.Reports(ctx, &pivot)
	if len(since) != len(all)-7 || since[0].ReportID != all[7].ReportID {
		t.Fatalf("since at a stored time returned %d reports starting at %s", len(since), since[0].ReportID)
	}
	future := time.Now().Add(time.Hour)
	if empty, err := s.Reports(ctx, &future); err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("an empty result must be an empty slice, which encodes as []")
	}

	if err := s.SetSchemaVersion(ctx, 2); err != nil {
		t.Fatal(err)
	}
	confidence := 0.75
	v2 := VolcanicReport{ReportID: "v2-1", VolcanoID: "merapi", AlertLevel: "Siaga", EruptionCount24h: 3, AshColumnHeightM: 800, ReportedAt: time.Now().UTC().Truncate(time.Millisecond), ConfidenceLevel: &confidence}
	if err := s.Add(ctx, v2); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			r := VolcanicReport{ReportID: fmt.Sprintf("concurrent-%d", i), VolcanoID: "semeru", AlertLevel: "Normal", ReportedAt: time.Now()}
			if err := s.Add(ctx, r); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	// A reopened store is what a restarted mock sees.
	s.Close()
	reopened, err := OpenStore(ctx, os.Getenv("PVMBG_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if v, err := reopened.SchemaVersion(ctx); err != nil || v != 2 {
		t.Fatalf("restart lost the schema version, got %d: %v", v, err)
	}
	after, _ := reopened.Reports(ctx, nil)
	if len(after) != len(all)+21 {
		t.Fatalf("restart kept %d reports, want %d", len(after), len(all)+21)
	}
	shapes := map[bool]int{}
	for _, r := range after {
		shapes[r.ConfidenceLevel != nil]++
	}
	if shapes[true] != 1 || shapes[false] != len(all)+20 {
		t.Fatalf("stored shapes %v, want old and new reports side by side", shapes)
	}
}
