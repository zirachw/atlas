package bmkg

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestGeneratedEventsKeepContract(t *testing.T) {
	store := NewStore()
	gen := NewGenerator(Seed, time.Unix(1_700_000_000, 0))
	start := time.Now()
	var warned, unwarned int
	for i := range 2000 {
		event, warnings := gen.Next(start.Add(time.Duration(i) * time.Second))
		if err := store.Add(event, warnings...); err != nil {
			t.Fatal(err)
		}
		if event.Magnitude < 3 || event.Magnitude > 7.8 || event.DepthKM < 5 || event.DepthKM > 200 {
			t.Fatalf("out of range event %+v", event)
		}
		if event.EpicenterLat < -90 || event.EpicenterLat > 90 || event.EpicenterLon < -180 || event.EpicenterLon > 180 {
			t.Fatalf("invalid coordinates %+v", event)
		}
		if !slices.ContainsFunc(regions, func(r region) bool { return r.Name == event.RegionName }) {
			t.Fatalf("unknown region %q", event.RegionName)
		}
		if !event.OccurredAt.Equal(start.Add(time.Duration(i) * time.Second).UTC().Truncate(time.Millisecond)) {
			t.Fatal("generated events must use the current time, or the Aggregator checkpoint skips them")
		}
		for _, w := range warnings {
			if !w.EstimatedArrival.After(event.OccurredAt) || len(w.AffectedZones) == 0 {
				t.Fatalf("invalid warning %+v", w)
			}
		}
		switch {
		case len(warnings) > 0:
			warned++
		case event.PotentialTsunami:
			unwarned++
		}
	}
	if warned == 0 || unwarned == 0 {
		t.Fatalf("generator needs warned (%d) and unwarned (%d) potential tsunamis", warned, unwarned)
	}
}

func TestGeneratorIsReproducibleAndRestartSafe(t *testing.T) {
	now := time.Now()
	a, b := NewGenerator(Seed, time.Unix(100, 0)), NewGenerator(Seed, time.Unix(100, 0))
	for range 50 {
		ea, wa := a.Next(now)
		eb, wb := b.Next(now)
		if ea != eb || len(wa) != len(wb) {
			t.Fatal("same seed and boot time must give the same sequence")
		}
	}
	first, _ := NewGenerator(Seed, time.Unix(100, 0)).Next(now)
	restarted, _ := NewGenerator(Seed, time.Unix(101, 0)).Next(now)
	if first.EventID == restarted.EventID {
		t.Fatal("a restarted mock reused an event ID")
	}
}

func TestRunGeneratorAddsEventsUntilCancelled(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunGenerator(ctx, store, NewGenerator(Seed, time.Now()), 5*time.Millisecond, testLog())
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(store.Events(nil)) < 3 {
		if time.Now().After(deadline) {
			t.Fatal("generator added too few events")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	count := len(store.Events(nil))
	time.Sleep(20 * time.Millisecond)
	if len(store.Events(nil)) != count {
		t.Fatal("generator kept running after cancellation")
	}
}
