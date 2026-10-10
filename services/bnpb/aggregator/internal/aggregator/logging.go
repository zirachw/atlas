package aggregator

import (
	"log/slog"
	"time"
)

func logDatabaseCall(log *slog.Logger, id, operation string, start time.Time, err error) {
	reason := ""
	if err != nil {
		reason = "store_error"
	}

	log.Info("database_call", "dependency", "canonical_store", "operation", operation,
		"correlation_id", id, "latency_ms", time.Since(start).Milliseconds(), "reason", reason)
}
