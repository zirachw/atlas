package pvmbg

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
	reports := store.Reports(nil)
	if len(reports) < MinSeedRecords {
		t.Fatalf("seed has %d reports", len(reports))
	}
	levels, volcanoes := map[string]bool{}, map[string]bool{}
	for _, r := range reports {
		if r.ConfidenceLevel != nil {
			t.Fatalf("seed report %s has confidence_level before any schema change", r.ReportID)
		}
		levels[r.AlertLevel], volcanoes[r.VolcanoID] = true, true
	}
	if len(levels) != len(alertLevels) || len(volcanoes) != len(volcanoIDs) {
		t.Fatalf("seed covers alert levels %v and volcanoes %v", levels, volcanoes)
	}
}

func TestStoreRejectsInvalidReports(t *testing.T) {
	store := NewStore()
	valid := VolcanicReport{ReportID: "r1", VolcanoID: "merapi", AlertLevel: "Siaga"}
	if err := store.Add(valid); err != nil {
		t.Fatal(err)
	}
	high, low, edge := 1.01, -0.01, 1.0
	for _, tc := range []struct {
		name   string
		report VolcanicReport
	}{
		{"missing ID", VolcanicReport{VolcanoID: "merapi", AlertLevel: "Normal"}},
		{"duplicate ID", valid},
		{"unknown volcano", VolcanicReport{ReportID: "r2", VolcanoID: "bromo", AlertLevel: "Normal"}},
		{"invalid alert level", VolcanicReport{ReportID: "r3", VolcanoID: "merapi", AlertLevel: "Bahaya"}},
		{"uppercase alert level", VolcanicReport{ReportID: "r4", VolcanoID: "merapi", AlertLevel: "AWAS"}},
		{"negative eruptions", VolcanicReport{ReportID: "r5", VolcanoID: "merapi", AlertLevel: "Siaga", EruptionCount24h: -1}},
		{"negative ash height", VolcanicReport{ReportID: "r6", VolcanoID: "merapi", AlertLevel: "Siaga", AshColumnHeightM: -1}},
		{"confidence above 1", VolcanicReport{ReportID: "r7", VolcanoID: "merapi", AlertLevel: "Siaga", ConfidenceLevel: &high}},
		{"confidence below 0", VolcanicReport{ReportID: "r8", VolcanoID: "merapi", AlertLevel: "Siaga", ConfidenceLevel: &low}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.Add(tc.report); err == nil {
				t.Fatal("invalid report accepted")
			}
		})
	}
	if err := store.Add(VolcanicReport{ReportID: "r9", VolcanoID: "semeru", AlertLevel: "Awas", ConfidenceLevel: &edge}); err != nil {
		t.Fatal("confidence_level of exactly 1 rejected:", err)
	}
	if len(store.Reports(nil)) != 2 {
		t.Fatalf("store holds %d reports", len(store.Reports(nil)))
	}
}

func TestSinceIsInclusiveAndOrdered(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	// Added out of time order, with two reports sharing a timestamp.
	for i, offset := range []int{3, 1, 2, 2, 0} {
		if err := store.Add(VolcanicReport{ReportID: fmt.Sprintf("r%d", i), VolcanoID: "merapi", AlertLevel: "Normal", ReportedAt: base.Add(time.Duration(offset) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(reports []VolcanicReport) (out []string) {
		for _, r := range reports {
			out = append(out, r.ReportID)
		}
		return out
	}
	since := base.Add(2 * time.Minute)
	if got := ids(store.Reports(&since)); !slices.Equal(got, []string{"r2", "r3", "r0"}) {
		t.Fatalf("reports since %s: %v", since, got)
	}
	if got := ids(store.Reports(nil)); !slices.Equal(got, []string{"r4", "r1", "r2", "r3", "r0"}) {
		t.Fatalf("all reports: %v", got)
	}
	future := base.Add(time.Hour)
	if got := store.Reports(&future); got == nil || len(got) != 0 {
		t.Fatal("an empty result must be an empty slice, which encodes as []")
	}
}

func TestStoreConcurrentAddAndRead(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup
	for writer := range 4 {
		wg.Go(func() {
			for i := range 50 {
				_ = store.Add(VolcanicReport{ReportID: fmt.Sprintf("r%d-%d", writer, i), VolcanoID: "krakatau", AlertLevel: "Waspada", ReportedAt: time.Now()})
			}
		})
		wg.Go(func() {
			for range 50 {
				store.Reports(nil)
			}
		})
	}
	wg.Wait()
	if n := len(store.Reports(nil)); n != 200 {
		t.Fatalf("stored %d reports", n)
	}
}
