package pvmbg

import (
	"context"
	"testing"
	"time"
)

func TestGeneratedReportsKeepContract(t *testing.T) {
	store := NewStore()
	gen := NewGenerator(Seed, time.Unix(1_700_000_000, 0))
	start := time.Now()
	levels := map[string]int{}
	for i := range 2000 {
		version := 1 + i%2
		now := start.Add(time.Duration(i) * time.Second)
		r := gen.Next(now, version)
		if err := store.Add(r); err != nil {
			t.Fatal(err)
		}
		if !r.ReportedAt.Equal(now.UTC().Truncate(time.Millisecond)) {
			t.Fatal("generated reports must use the current time, or the Aggregator checkpoint skips them")
		}
		if r.AlertLevel == "Normal" && (r.EruptionCount24h != 0 || r.AshColumnHeightM != 0) {
			t.Fatalf("Normal report with activity %+v", r)
		}
		switch {
		case version == 1 && r.ConfidenceLevel != nil:
			t.Fatal("schema version 1 report has confidence_level")
		case version == 2 && (r.ConfidenceLevel == nil || *r.ConfidenceLevel < 0 || *r.ConfidenceLevel > 1):
			t.Fatalf("schema version 2 report confidence_level %v", r.ConfidenceLevel)
		}
		levels[r.AlertLevel]++
	}
	if len(levels) != len(alertLevels) {
		t.Fatalf("generator covers alert levels %v", levels)
	}
}

func TestGeneratorIsReproducibleAndRestartSafe(t *testing.T) {
	now := time.Now()
	a, b := NewGenerator(Seed, time.Unix(100, 0)), NewGenerator(Seed, time.Unix(100, 0))
	for range 50 {
		if ra, rb := a.Next(now, 1), b.Next(now, 1); ra != rb {
			t.Fatal("same seed and boot time must give the same sequence")
		}
	}
	first := NewGenerator(Seed, time.Unix(100, 0)).Next(now, 1)
	restarted := NewGenerator(Seed, time.Unix(101, 0)).Next(now, 1)
	if first.ReportID == restarted.ReportID {
		t.Fatal("a restarted mock reused a report ID")
	}
}

func TestRunGeneratorFollowsSchemaAndRunsDuringOutage(t *testing.T) {
	store, controls := NewStore(), NewControls()
	controls.SetOutage(Outage{Enabled: true, Mode: OutageError})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunGenerator(ctx, store, controls, NewGenerator(Seed, time.Now()), 5*time.Millisecond, testLog())
		close(done)
	}()
	waitFor := func(cond func([]VolcanicReport) bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !cond(store.Reports(nil)) {
			if time.Now().After(deadline) {
				t.Fatal("generator did not produce the expected reports")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(func(r []VolcanicReport) bool { return len(r) >= 3 })
	controls.SetSchemaVersion(2)
	waitFor(func(r []VolcanicReport) bool { return r[len(r)-1].ConfidenceLevel != nil })
	cancel()
	<-done
	count := len(store.Reports(nil))
	time.Sleep(20 * time.Millisecond)
	if len(store.Reports(nil)) != count {
		t.Fatal("generator kept running after cancellation")
	}
}
