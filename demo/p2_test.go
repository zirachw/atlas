//go:build demo

package demo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const k6Image = "grafana/k6:1.3.0"

func volcanicFreshness(t *testing.T, token string) (int, hazardPage, time.Duration) {
	t.Helper()
	status, raw, took := call(t, http.MethodGet, s.client+"/hazards?source=PVMBG&limit=20", token, "", nil)

	var page hazardPage

	_ = json.Unmarshal(raw, &page)

	return status, page, took
}

func isStale(page hazardPage) bool {
	return len(page.Sources) == 1 && page.Sources[0].Status == "stale" && page.Sources[0].StaleSince != nil
}

func checkOutage(t *testing.T, ev map[string]any) {
	s.ready(t)
	aggregator := s.containerID(t, "aggregator")
	since := time.Now().Add(-2 * time.Second)

	s.admin(t, "outage", `{"enabled":true,"mode":"error"}`)

	var stale hazardPage

	eventually(t, 60*time.Second, "volcanic reads marked stale", func() bool {
		status, page, _ := volcanicFreshness(t, s.token(t, "INTERNAL_OPS"))
		stale = page

		return status == http.StatusOK && isStale(page) && len(page.Data) > 0
	})
	status, raw, took := call(
		t,
		http.MethodGet,
		s.client+"/hazards?source=BMKG&limit=20",
		s.token(t, "INTERNAL_OPS"),
		"",
		nil,
	)

	var seismic hazardPage

	_ = json.Unmarshal(raw, &seismic)
	if status != http.StatusOK || len(seismic.Sources) != 1 || seismic.Sources[0].Status != "healthy" {
		t.Errorf("seismic read during outage status %d sources %v", status, seismic.Sources)
	}

	eventually(t, 30*time.Second, "BMKG polls committing during the outage", func() bool {
		committed := s.logged(t, "aggregator", since, func(l map[string]any) bool {
			return l["msg"] == "poll_committed" && l["source"] == "BMKG"
		})

		return len(committed) >= 2
	})

	recovering := time.Now()

	s.admin(t, "outage", `{"enabled":false}`)
	eventually(t, 60*time.Second, "volcanic reads healthy again", func() bool {
		_, page, _ := volcanicFreshness(t, s.token(t, "INTERNAL_OPS"))
		return len(page.Sources) == 1 && page.Sources[0].Status == "healthy"
	})

	if s.containerID(t, "aggregator") != aggregator {
		t.Error("the Aggregator restarted during recovery")
	}

	ev["stale_volcanic"] = map[string]any{"sources": stale.Sources, "stored_records": len(stale.Data)}
	ev["seismic_during_outage"] = map[string]any{"status": status, "ms": took.Milliseconds()}
	ev["recovery_seconds"] = time.Since(recovering).Seconds()
}

func checkHangingSource(t *testing.T, ev map[string]any) {
	s.ready(t)

	start := time.Now()

	s.admin(t, "outage", `{"enabled":true,"mode":"hang"}`)

	var slowest time.Duration

	eventually(t, 60*time.Second, "volcanic reads marked stale", func() bool {
		status, page, took := volcanicFreshness(t, s.token(t, "INTERNAL_OPS"))

		slowest = max(slowest, took)
		if status != http.StatusOK || took > 2*time.Second {
			t.Errorf("read during hang outage status %d after %s", status, took)
		}

		return isStale(page)
	})
	s.admin(t, "outage", `{"enabled":false}`)

	ev["stale_after_seconds"] = time.Since(start).Seconds()
	ev["slowest_read_ms"] = slowest.Milliseconds()
}

func checkExplicitUnavailable(t *testing.T, ev map[string]any) {
	s.ready(t)
	s.admin(t, "outage", `{"enabled":true,"mode":"error"}`)
	eventually(t, 60*time.Second, "volcanic reads marked stale", func() bool {
		_, page, _ := volcanicFreshness(t, s.token(t, "INTERNAL_OPS"))
		return isStale(page)
	})
	s.sql(t, "DELETE FROM hazard_events WHERE source='PVMBG'")
	token := s.token(t, "INTERNAL_OPS")

	got, pages := map[string]int{}, map[string]hazardPage{}
	for name, query := range map[string]string{
		"volcanic": "source=PVMBG",
		"mixed":    "limit=20",
		"seismic":  "source=BMKG",
	} {
		got[name], pages[name] = s.hazards(t, token, query, nil)
	}

	// A mixed answer stays 200, so the body must name the missing source.
	missing := slices.ContainsFunc(pages["mixed"].Sources, func(f freshness) bool {
		return f.Source == "PVMBG" && f.Status == "unavailable"
	})
	if got["volcanic"] != http.StatusServiceUnavailable || got["mixed"] != http.StatusOK || !missing ||
		got["seismic"] != http.StatusOK {
		t.Errorf("statuses %v, want volcanic 503, mixed 200 naming PVMBG unavailable, seismic 200", got)
	}
	// A cleared checkpoint makes the next poll ingest every stored report again, seeds included.
	s.sql(t, "UPDATE source_progress SET checkpoint=NULL WHERE source='PVMBG'")
	s.admin(t, "outage", `{"enabled":false}`)
	eventually(t, 60*time.Second, "volcanic data restored", func() bool {
		return s.sql(t, "SELECT count(*) > 0 FROM hazard_events WHERE source_ref_id='pvmbg-seed-001'") == "t"
	})

	ev["statuses"] = got
}

func checkSustainedLoad(t *testing.T, ev map[string]any) {
	s.ready(t)

	since := time.Now()
	metrics, samples := runK6(t, "sustained")

	p95 := metric(metrics, "http_req_duration{kind:bmkg}", "p(95)")
	if p95 >= 300 {
		t.Errorf("BMKG-only p95 %.1f ms, want below 300 ms", p95)
	}

	slow := s.logged(t, "aggregator", since, func(l map[string]any) bool {
		ms, _ := l["latency_ms"].(float64)
		return l["msg"] == "source_call" && l["source"] == "PVMBG" && ms >= 3000
	})
	if len(slow) == 0 {
		t.Error("no 3s PVMBG call happened during the load test")
	} else {
		ev["pvmbg_call_ms"] = slow[0]["latency_ms"]
	}

	span := longestConnectionSpan(samples, 50, 5*time.Second)
	if span < time.Minute {
		t.Errorf("longest observed interval with 50+ connections was %s, want at least 60s", span)
	}

	if rate := metric(metrics, "unexpected_errors", "rate"); rate >= 0.01 {
		t.Errorf("unexpected error rate %.4f, want below 0.01", rate)
	}

	if failures := metric(metrics, "auth_failures", "count"); failures > 0 {
		t.Errorf("%v auth failures distort the result", failures)
	}

	ev["bmkg_ms"] = map[string]float64{
		"p50": metric(metrics, "http_req_duration{kind:bmkg}", "med"),
		"p95": p95,
		"p99": metric(metrics, "http_req_duration{kind:bmkg}", "p(99)"),
	}
	ev["volcanic_ms"] = map[string]float64{
		"p50": metric(metrics, "http_req_duration{kind:volcanic}", "med"),
		"p95": metric(metrics, "http_req_duration{kind:volcanic}", "p(95)"),
		"p99": metric(metrics, "http_req_duration{kind:volcanic}", "p(99)"),
	}
	ev["requests_per_second"] = metric(metrics, "http_reqs{kind:bmkg}", "rate") + metric(
		metrics,
		"http_reqs{kind:volcanic}",
		"rate",
	)
	ev["unexpected_error_rate"] = metric(metrics, "unexpected_errors", "rate")
	ev["controlled_rejections"] = metric(metrics, "controlled_rejections", "count")
	ev["seconds_with_50_connections"] = span.Seconds()
	ev["connection_samples"] = samples
	ev["files"] = "p2-sustained.json, p2-sustained.txt, p2-sustained-connections.json"
}

func checkOverload(t *testing.T, ev map[string]any) {
	s.ready(t)
	metrics, _ := runK6(t, "burst")

	rejected := metric(metrics, "controlled_rejections", "count")
	if rejected == 0 {
		t.Error("overload produced no 429, so the admission limit was never reached")
	}

	if rate := metric(metrics, "unexpected_errors", "rate"); rate >= 0.01 {
		t.Errorf("unexpected error rate %.4f under overload, want below 0.01", rate)
	}

	ev["controlled_rejections"] = rejected
	ev["unexpected_error_rate"] = metric(metrics, "unexpected_errors", "rate")
	ev["accepted_requests"] = metric(metrics, "http_reqs{kind:bmkg}", "count") - rejected
	ev["files"] = "p2-burst.json, p2-burst.txt"
}

// runK6 runs loadtest/p2.js inside the Compose network and samples Client API connections every 2s.
func runK6(t *testing.T, scenario string) (map[string]any, []connectionSample) {
	t.Helper()

	if out, err := exec.Command("docker", "pull", "-q", k6Image).CombinedOutput(); err != nil {
		t.Fatalf("pull %s: %v %s", k6Image, err, out)
	}

	results := filepath.Join(s.evidence, "P2")
	if err := os.MkdirAll(results, 0o755); err != nil {
		t.Fatal(err)
	}

	args := []string{
		"run", "--rm", "--name", s.project + "-k6", "--network", s.project + "_auth-api",
		"-v", filepath.Join(s.root, "loadtest") + ":/scripts:ro", "-v", results + ":/results",
	}
	if runtime.GOOS != "windows" {
		args = append(args, "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()))
	}

	for k, v := range map[string]string{
		"SCENARIO": scenario, "BASE_URL": "http://client-api:8080", "AUTH_URL": "http://auth:8084",
		"CLIENT_ID": s.env["INTERNAL_OPS_CLIENT_ID"], "CLIENT_SECRET": s.env["INTERNAL_OPS_CLIENT_SECRET"],
		"SUMMARY_JSON": "/results/p2-" + scenario + ".json", "SUMMARY_TEXT": "/results/p2-" + scenario + ".txt",
	} {
		args = append(args, "-e", k+"="+v)
	}

	args = append(args, k6Image, "run", "--quiet", "/scripts/p2.js")
	cmd := exec.Command("docker", args...)

	var output strings.Builder

	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	samples := []connectionSample{}

	for {
		select {
		case err := <-done:
			// A failed k6 threshold exits 99, and the summary still holds the measurements.
			if err != nil && cmd.ProcessState.ExitCode() != 99 {
				t.Fatalf("k6 %s: %v %s", scenario, err, tail(output.String(), 2000))
			}

			raw, err := os.ReadFile(filepath.Join(results, "p2-"+scenario+".json"))
			if err != nil {
				t.Fatal(err)
			}

			var summary struct {
				Metrics map[string]any `json:"metrics"`
			}
			if err := json.Unmarshal(raw, &summary); err != nil {
				t.Fatal(err)
			}

			raw, err = json.MarshalIndent(samples, "", "  ")
			if err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(results, "p2-"+scenario+"-connections.json")
			if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}

			t.Log(tail(output.String(), 1500))

			return summary.Metrics, samples
		case <-time.After(2 * time.Second):
			connections := clientConnections(t)
			samples = append(samples, connectionSample{At: time.Now(), Connections: connections})
		}
	}
}

// clientConnections counts established sockets on Client API port 8080 (0x1F90).
func clientConnections(t *testing.T) int {
	out, err := s.compose("exec", "-T", "client-api", "cat", "/proc/net/tcp", "/proc/net/tcp6")
	if err != nil {
		return 0
	}

	count := 0

	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) > 3 && strings.HasSuffix(f[1], ":1F90") && f[3] == "01" {
			count++
		}
	}

	return count
}

func metric(metrics map[string]any, name, stat string) float64 {
	m, _ := metrics[name].(map[string]any)
	values, _ := m["values"].(map[string]any)
	v, _ := values[stat].(float64)

	return v
}
