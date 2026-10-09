package bmkg

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"
)

type region struct {
	Name     string
	Lat, Lon float64
	Zones    []string
}

var regions = []region{
	{"Selatan Jawa", -9.52, 110.21, []string{"Pantai Selatan Yogyakarta", "Pantai Selatan Jawa Tengah"}},
	{"Laut Banda", -6.48, 129.51, []string{"Kepulauan Banda", "Pesisir Maluku Tengah"}},
	{"Sulawesi Tengah", -1.31, 120.12, []string{"Teluk Palu", "Pesisir Donggala"}},
	{"Selat Sunda", -6.93, 105.31, []string{"Pesisir Banten", "Pesisir Lampung Selatan"}},
	{"Kepulauan Mentawai", -2.52, 99.53, []string{"Kepulauan Mentawai", "Pesisir Sumatera Barat"}},
	{"Nusa Tenggara Barat", -8.87, 118.48, []string{"Pesisir Sumbawa", "Pesisir Lombok"}},
}

const Seed = 4031 // IF4031

// IDs embed the boot time because seq restarts at 1, so a restarted mock never reuses a stored ID.
type Generator struct {
	rng  *rand.Rand
	boot int64
	seq  int
}

func NewGenerator(seed uint64, boot time.Time) *Generator {
	return &Generator{rng: rand.New(rand.NewPCG(seed, seed)), boot: boot.Unix()}
}

func round(v float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(v*p) / p
}

func (g *Generator) Next(now time.Time) (SeismicEvent, []TsunamiWarning) {
	g.seq++
	r := regions[g.rng.IntN(len(regions))]
	occurred := now.UTC().Truncate(time.Millisecond)
	event := SeismicEvent{
		EventID:      fmt.Sprintf("bmkg-%d-%06d", g.boot, g.seq),
		Magnitude:    round(3.0+g.rng.Float64()*4.8, 1),
		DepthKM:      round(5+g.rng.Float64()*195, 1),
		EpicenterLat: round(r.Lat+(g.rng.Float64()-0.5), 2),
		EpicenterLon: round(r.Lon+(g.rng.Float64()-0.5), 2),
		RegionName:   r.Name,
		OccurredAt:   occurred,
	}
	event.PotentialTsunami = event.Magnitude >= 6.0 && event.DepthKM <= 70 && g.rng.Float64() < 0.8
	if !event.PotentialTsunami || g.rng.Float64() < 0.25 {
		return event, nil
	}
	level := "Waspada"
	switch {
	case event.Magnitude >= 7.2:
		level = "Awas"
	case event.Magnitude >= 6.5:
		level = "Siaga"
	}
	return event, []TsunamiWarning{{
		WarningID:        fmt.Sprintf("bmkg-tw-%d-%06d", g.boot, g.seq),
		RelatedEventID:   event.EventID,
		ThreatLevel:      level,
		AffectedZones:    r.Zones,
		EstimatedArrival: occurred.Add(time.Duration(15+g.rng.IntN(46)) * time.Minute),
	}}
}

func RunGenerator(ctx context.Context, store Store, gen *Generator, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			event, warnings := gen.Next(now)
			id := newCorrelationID()
			writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			start := time.Now()
			err := store.Add(writeCtx, event, warnings...)
			cancel()
			reason := ""
			if err != nil {
				reason = err.Error()
			}
			log.Info("database_call", "service", "bmkg", "dependency", "bmkg_store", "operation", "add_event", "correlation_id", id, "latency_ms", time.Since(start).Milliseconds(), "reason", reason)
			if err != nil {
				log.Error("event_generation_failed", "service", "bmkg", "correlation_id", id, "event_id", event.EventID, "reason", err.Error())
				continue
			}
			log.Info("event_generated", "service", "bmkg", "correlation_id", id, "event_id", event.EventID, "magnitude", event.Magnitude, "potential_tsunami", event.PotentialTsunami, "warnings", len(warnings))
		}
	}
}
