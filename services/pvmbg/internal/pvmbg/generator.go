package pvmbg

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"
)

// IDs embed the boot time because seq restarts at 1, so a restarted mock never reuses a stored ID.
type Generator struct {
	rng  *rand.Rand
	boot int64
	seq  int
}

const Seed = 4031 // IF4031

func NewGenerator(seed uint64, boot time.Time) *Generator {
	return &Generator{rng: rand.New(rand.NewPCG(seed, seed)), boot: boot.Unix()}
}

func round(v float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(v*p) / p
}

func (g *Generator) Next(now time.Time, schemaVersion int) VolcanicReport {
	g.seq++
	// Normal 40%, Waspada 30%, Siaga 20%, Awas 10%.
	level := [10]int{0, 0, 0, 0, 1, 1, 1, 2, 2, 3}[g.rng.IntN(10)]

	report := VolcanicReport{
		ReportID:   fmt.Sprintf("pvmbg-%d-%06d", g.boot, g.seq),
		VolcanoID:  volcanoIDs[g.rng.IntN(len(volcanoIDs))],
		AlertLevel: alertLevels[level],
		ReportedAt: now.UTC().Truncate(time.Millisecond),
	}
	if level > 0 {
		report.EruptionCount24h = g.rng.IntN(level * 15)
		report.AshColumnHeightM = round(g.rng.Float64()*float64(level)*1500, 1)
	}

	if schemaVersion >= 2 {
		confidence := round(0.5+g.rng.Float64()*0.5, 2)
		report.ConfidenceLevel = &confidence
	}

	return report
}

// Keeps running during an outage so the Aggregator has reports to catch up on.
func RunGenerator(
	ctx context.Context,
	store Store,
	controls *Controls,
	gen *Generator,
	interval time.Duration,
	log *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			report := gen.Next(now, controls.SchemaVersion())
			id := newCorrelationID()
			writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			start := time.Now()
			err := store.Add(writeCtx, report)

			cancel()

			reason := ""
			if err != nil {
				reason = "store_error"
			}

			log.Info("database_call",
				"dependency", "pvmbg_store",
				"operation", "add_report",
				"correlation_id", id,
				"latency_ms", time.Since(start).Milliseconds(),
				"reason", reason,
			)

			if err != nil {
				log.Error("report_generation_failed",
					"correlation_id", id,
					"report_id", report.ReportID,
					"reason", err.Error(),
				)

				continue
			}

			log.Info("report_generated",
				"correlation_id", id,
				"report_id", report.ReportID,
				"volcano_id", report.VolcanoID,
				"alert_level", report.AlertLevel,
				"confidence_level", report.ConfidenceLevel != nil,
			)
		}
	}
}
