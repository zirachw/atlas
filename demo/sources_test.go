//go:build demo

package demo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func checkSourceContract(t *testing.T, ev map[string]any) {
	s.ready(t)

	type source struct {
		name, path, header, value, timeField string
		minDelay, maxDelay                   time.Duration
	}
	// Delay bounds allow 300ms of local network and container overhead.
	for _, src := range []source{
		{
			"BMKG",
			s.bmkg + "/seismic-events",
			"X-BMKG-Key",
			s.env["BMKG_API_KEY"],
			"occurred_at",
			50 * time.Millisecond,
			450 * time.Millisecond,
		},
		{
			"PVMBG",
			s.pvmbg + "/volcanic-reports",
			"Authorization",
			"Bearer " + s.env["PVMBG_TOKEN"],
			"reported_at",
			3 * time.Second,
			3300 * time.Millisecond,
		},
	} {
		headers := map[string]string{src.header: src.value}

		var sourceDuration time.Duration

		read := func(query string) ([]map[string]any, time.Duration) {
			started := time.Now().Add(-2 * time.Second)
			id := fmt.Sprintf("demo-source-%d", time.Now().UnixNano())
			headers["X-Correlation-ID"] = id
			status, raw, took := call(t, http.MethodGet, src.path+query, "", "", headers)

			var records []map[string]any
			if status != http.StatusOK || json.Unmarshal(raw, &records) != nil {
				t.Fatalf("%s%s status %d", src.name, query, status)
			}

			if query == "" {
				// Compare the delay on the source clock; host and VM clocks can differ.
				eventually(t, 5*time.Second, src.name+" request timing", func() bool {
					for _, line := range s.logged(t, strings.ToLower(src.name), started, func(l map[string]any) bool {
						return l["msg"] == "request" && l["correlation_id"] == id
					}) {
						if ms, ok := line["latency_ms"].(float64); ok {
							sourceDuration = time.Duration(ms * float64(time.Millisecond))
							return true
						}
					}

					return false
				})
			}

			return records, took
		}

		all, took := read("")
		if len(all) < 20 {
			t.Errorf("%s serves %d records, want at least 20 seeded", src.name, len(all))
		}

		if sourceDuration < src.minDelay || sourceDuration > src.maxDelay || took > src.maxDelay {
			t.Errorf(
				"%s source latency %s, client latency %s, want source %s-%s and client at most %s",
				src.name,
				sourceDuration,
				took,
				src.minDelay,
				src.maxDelay,
				src.maxDelay,
			)
		}

		pivot := toString(all[len(all)/2][src.timeField])

		since, _ := read("?since=" + pivot)
		if len(since) == 0 || toString(since[0][src.timeField]) != pivot {
			t.Errorf("%s since=%s does not start at the record with that time", src.name, pivot)
		}

		started := time.Now().UTC()

		eventually(t, 15*time.Second, src.name+" generating a new record", func() bool {
			fresh, _ := read("?since=" + started.Format(time.RFC3339Nano))
			return len(fresh) > 0
		})
		ev[src.name] = map[string]any{
			"records":                   len(all),
			"response_ms":               took.Milliseconds(),
			"source_response_ms":        sourceDuration.Milliseconds(),
			"new_record_within_seconds": time.Since(started).Seconds(),
		}
	}
}

func checkSourceRestart(t *testing.T, ev map[string]any) {
	s.ready(t)
	s.admin(t, "schema-version", `{"version":2}`)

	count := func() int {
		status, raw, _ := call(
			t,
			http.MethodGet,
			s.pvmbg+"/volcanic-reports",
			"",
			"",
			map[string]string{"Authorization": "Bearer " + s.env["PVMBG_TOKEN"]},
		)

		var reports []map[string]any
		if status != http.StatusOK || json.Unmarshal(raw, &reports) != nil {
			t.Fatalf("PVMBG read status %d", status)
		}

		return len(reports)
	}
	before := count()
	container := s.containerID(t, "pvmbg")
	s.mustCompose(t, "restart", "pvmbg")

	restarted := time.Now().UTC()
	// A restarted container can get a new ephemeral host port.
	if err := s.resolvePorts(); err != nil {
		t.Fatal(err)
	}

	var health struct {
		SchemaVersion int `json:"schema_version"`
	}

	eventually(t, 60*time.Second, "PVMBG healthy after restart", func() bool {
		status, raw, _ := call(t, http.MethodGet, s.pvmbg+"/health", "", "", nil)
		return status == http.StatusOK && json.Unmarshal(raw, &health) == nil
	})

	if s.containerID(t, "pvmbg") != container {
		t.Error("restart replaced the container instead of restarting it")
	}

	if after := count(); after < before {
		t.Errorf("history shrank from %d to %d reports across the restart", before, after)
	}

	if health.SchemaVersion != 2 {
		t.Errorf("schema version %d after restart, want 2", health.SchemaVersion)
	}

	eventually(t, 30*time.Second, "a post-restart report carrying confidence_level", func() bool {
		status, raw, _ := call(
			t,
			http.MethodGet,
			s.pvmbg+"/volcanic-reports?since="+restarted.Format(time.RFC3339Nano),
			"",
			"",
			map[string]string{"Authorization": "Bearer " + s.env["PVMBG_TOKEN"]},
		)

		var reports []map[string]any

		_ = json.Unmarshal(raw, &reports)

		return status == http.StatusOK && len(reports) > 0 && reports[len(reports)-1]["confidence_level"] != nil
	})

	ev["reports_before_and_after"] = []int{before, count()}
	ev["schema_version_after_restart"] = health.SchemaVersion
}
