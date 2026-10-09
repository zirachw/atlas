//go:build demo

package demo

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

type check struct {
	Name     string
	Criteria []string
	Logic    []string // unit and integration tests that cover the rules behind this check
	Load     bool
	Run      func(t *testing.T, ev map[string]any)
}

// Each check names the spec criteria it proves, so the report maps one to one onto the grading sheet.
var catalog = []check{
	{"R3 every service runs in its own container from one compose up", []string{"R3"}, nil, false, checkContainers},
	{"R1 services share no code", []string{"R1"}, nil, false, checkNoSharedCode},
	{"S1 source mocks keep their seed, event rate, delay, and since contract", []string{"S1"}, []string{"TestSeedMeetsContract", "TestSinceQuery", "TestDelayAppliesOnlyToAuthorizedDataRequests", "TestRandomDelayStaysWithinBounds", "TestRunGeneratorAddsEventsUntilCancelled"}, false, checkSourceContract},
	{"R4 a restarted source keeps its history and schema version", []string{"R4", "S1"}, []string{"TestIntegrationStorePersistsReportsAndSchemaVersion", "TestIntegrationStorePersistsSeedAndFiltersSince"}, false, checkSourceRestart},
	{"R7 every service answers its health check", []string{"R7"}, []string{"TestQueryValidationAndHealth", "TestRoutesAndCorrelation"}, false, checkHealth},
	{"P1.1 both sources map to canonical HazardEvents", []string{"P1.1"}, []string{"TestSeismicBoundariesAndWarnings", "TestVolcanicAdditiveValuesAndStableIdentity", "TestRequiredFieldsRejectMissingNullAndWrongType"}, false, checkMapping},
	{"P1.2-P1.4 schema change applies at runtime and stays valid", []string{"P1.2", "P1.3", "P1.4"}, []string{"TestSchemaChangeAtRuntime", "TestIntegrationLivePollingMappingFreshnessAndReads"}, false, checkSchemaChange},
	{"P2.1-P2.2 BMKG reads stay fast under sustained load", []string{"P2.1", "P2.2"}, []string{"TestIndependentPollingDuringSlowSourceAndOutage", "TestAPIAdmissionAndDeadline"}, true, checkSustainedLoad},
	{"P2.2 overload is rejected with counted 429 responses", []string{"P2.2"}, []string{"TestAdmissionLimitDoesNotCallDependenciesAndReleasesCapacity", "TestUpstream429PreservesStatusAndSafeRetryAfter"}, true, checkOverload},
	{"P2.3 outage serves stale volcanic data and fresh seismic data", []string{"P2.3", "R4"}, []string{"TestOutageErrorModeAndRecovery", "TestIntegrationLivePollingMappingFreshnessAndReads"}, false, checkOutage},
	{"P2.3 hanging source never makes reads hang", []string{"P2.3", "R4"}, []string{"TestOutageHangModeSendsNoResponse", "TestSourceBoundsRetriesAndCancellation"}, false, checkHangingSource},
	{"P3.1 agencies reject each other's credentials", []string{"P3.1", "R5"}, []string{"TestCredentialDomains"}, false, checkAgencyCredentials},
	{"P3.2 Media gets summary fields and raw requests get 403", []string{"P3.2"}, []string{"TestMediaRawRequestForbiddenAndRawClientGetsAllFields", "TestSummaryFiltersRawFieldsAndPreservesFreshnessAndCorrelation"}, false, checkMediaScope},
	{"P3.3 expired token refreshes without login and the old token is rejected", []string{"P3.3"}, []string{"TestNaturalExpiryThenRefreshWithoutLogin", "TestLoginScopesAndRefreshSupersedesOldSessionVersion", "TestIntegrationConcurrentRefreshRevocationAndExpiry"}, false, checkTokenRefresh},
	{"P3.4 no credentials are committed and .env.example lists every variable", []string{"P3.4", "R6"}, nil, false, checkNoCommittedSecrets},
	{"P4.2 old and new attribute shapes coexist without migration", []string{"P4.2"}, []string{"TestIntegrationStoreCommitRollbackRestartAndOwnership"}, false, checkAttributeShapes},
	{"P4.3 only the Aggregator reaches the Canonical Store and every store has one owner", []string{"P4.3", "R2"}, []string{"TestIntegrationStoreCommitRollbackRestartAndOwnership"}, false, checkStoreOwnership},
	{"P5.1-P5.2 events fan out to independent consumers", []string{"P5.1", "P5.2"}, []string{"TestIntegrationRabbitFanoutConfirmAndReconnect", "TestIntegrationIndependentQueuesAndRestartCatchup"}, false, checkFanout},
	{"P5.3 a stopped consumer blocks nothing and catches up", []string{"P5.3", "R4"}, []string{"TestIntegrationIndependentQueuesAndRestartCatchup", "TestIntegrationRedeliveryAfterPersistBeforeAck"}, false, checkConsumerOutage},
	{"P5.4 a new consumer joins without producer changes", []string{"P5.4"}, nil, false, checkNewConsumer},
	{"R7 correlation IDs cross every service in a flow", []string{"R7"}, []string{"TestAPIRouteMethodsAndCorrelation", "TestSourceCallLogsDependencyAndSafeFailure"}, false, checkCorrelation},
	{"R7 every outbound call logs its latency", []string{"R7"}, []string{"TestSourceCallLogsDependencyAndSafeFailure"}, false, checkOutboundLatency},
	{"P4.1 one service rebuilds while the others keep serving", []string{"P4.1", "R4"}, nil, false, checkIndependentRebuild},
	// Deletes stored volcanic rows, so it runs last.
	{"P2.3 failed source without stored data is explicitly unavailable", []string{"P2.3"}, []string{"TestAPIAuthenticationQueriesAndFreshness"}, false, checkExplicitUnavailable},
}

var criteria = []struct{ ID, Requirement string }{
	{"S1", "Mocks seed 20+ records, add 1+ per 10s, keep their delays, filter by since, and keep data across restarts"},
	{"P1.1", "Both sources map to HazardEvent by the mapping rules"},
	{"P1.2", "PVMBG schema change runs without restarting any service"},
	{"P1.3", "HazardEvents stay valid after the schema change"},
	{"P1.4", "Unknown fields follow the declared attributes policy"},
	{"P1.5", "Schema evolution mechanism and supported changes"},
	{"P2.1", "BMKG-only p95 below 300 ms while PVMBG takes 3 s"},
	{"P2.2", "50+ connections for 60+ s with full metrics, errors below 1%, 429 counted"},
	{"P2.3", "Outage serves stale data or explicit unavailability and recovers without restart"},
	{"P2.4", "Concurrency model and load test configuration"},
	{"P3.1", "Each agency rejects the other agency's credentials with 401 or 403"},
	{"P3.2", "Media gets summary fields and explicit raw requests get 401 or 403"},
	{"P3.3", "Expired Field Team token refreshes without login and the old token is rejected"},
	{"P3.4", "No hard-coded or committed credentials and .env.example exists"},
	{"P3.5", "Authentication mechanism and why it opens no new gap"},
	{"P4.1", "One service rebuilds and restarts while independent requests keep working"},
	{"P4.2", "Old and new attribute shapes stored side by side without migration"},
	{"P4.3", "Only the Aggregator accesses the Canonical Store"},
	{"P4.4", "Service boundaries and two storage alternatives compared"},
	{"P5.1", "New HazardEvents are published to a broker"},
	{"P5.2", "Two or more independent consumers receive the same stream"},
	{"P5.3", "A stopped consumer blocks neither producer nor others and catches up"},
	{"P5.4", "A new consumer joins without producer code changes"},
	{"P5.5", "Broker, delivery semantics, and idempotency"},
	{"R1", "Services talk only over the network and share no business logic"},
	{"R2", "Each storage has exactly one owning service"},
	{"R3", "Each service has its own container and one command starts all"},
	{"R4", "Services start and stop independently under real failures"},
	{"R5", "Credentials are valid in one agency only"},
	{"R6", "Configuration and secrets come from the environment"},
	{"R7", "Health checks, correlated structured logs, and outbound latency"},
}

type result struct {
	Name     string         `json:"name"`
	Criteria []string       `json:"criteria"`
	Status   string         `json:"status"`
	Seconds  float64        `json:"seconds"`
	Logic    []string       `json:"logic,omitempty"`
	Evidence map[string]any `json:"evidence,omitempty"`
}

var (
	s       *stack
	results = map[string]result{}
)

func TestMain(m *testing.M) {
	flag.Parse()
	started := time.Now()
	var err error
	if s, err = startStack(); err != nil {
		fmt.Fprintln(os.Stderr, "stack startup failed:", err)
		if s != nil {
			s.stop()
		}
		os.Exit(1)
	}
	code := m.Run()
	if err := writeReport(started); err != nil {
		fmt.Fprintln(os.Stderr, "report:", err)
	}
	s.stop()
	os.Exit(code)
}

func TestDemo(t *testing.T) {
	for _, c := range catalog {
		t.Run(c.Name, func(t *testing.T) {
			ev := map[string]any{}
			start := time.Now()
			defer func() {
				status := "pass"
				if t.Skipped() {
					status = "skip"
				} else if t.Failed() {
					status = "fail"
				}
				results[c.Name] = result{c.Name, c.Criteria, status, time.Since(start).Seconds(), c.Logic, ev}
			}()
			if c.Load && os.Getenv("DEMO_LOAD") != "1" {
				t.Skip("load check, set DEMO_LOAD=1")
			}
			c.Run(t, ev)
		})
	}
}

var marks = map[string]string{"pass": "✅ pass", "fail": "❌ fail", "skip": "⏭ skip", "": "not run"}

func writeReport(started time.Time) error {
	commit, _ := exec.Command("git", "-C", s.root, "rev-parse", "--short", "HEAD").Output()
	dirty, _ := exec.Command("git", "-C", s.root, "status", "--porcelain").Output()
	docker, _ := exec.Command("docker", "info", "--format", "{{.ServerVersion}} {{.NCPU}} {{.MemTotal}}").Output()
	run := flag.Lookup("test.run").Value.String()
	meta := map[string]any{
		"commit": strings.TrimSpace(string(commit)), "uncommitted_changes": len(strings.TrimSpace(string(dirty))) > 0,
		"started": started.UTC().Format(time.RFC3339), "finished": time.Now().UTC().Format(time.RFC3339),
		"host":   fmt.Sprintf("%s/%s, %d CPUs", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
		"docker": strings.TrimSpace(string(docker)), "settings": overrides, "filter": run,
	}
	ordered := []result{}
	for _, c := range catalog {
		ordered = append(ordered, results[c.Name])
	}
	raw, err := json.MarshalIndent(map[string]any{"run": meta, "checks": ordered}, "", "  ")
	if err != nil {
		return err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# M1 Demo\n\nCommit `%s`", meta["commit"])
	if meta["uncommitted_changes"] == true {
		b.WriteString(" with uncommitted changes")
	}
	fmt.Fprintf(&b, ". Run from %s to %s on %s. Docker `%s`.\n\n", meta["started"], meta["finished"], meta["host"], meta["docker"])
	b.WriteString("Settings within the specification are PVMBG fixed at 3s, polling every 3s, a new source event every 2s, and a 60s access token.\n\n")
	b.WriteString("## Checks\n\n| Check | Criteria | Result | Seconds | Logic also covered by |\n|---|---|---|---|---|\n")
	for _, c := range catalog {
		r := results[c.Name]
		fmt.Fprintf(&b, "| %s | %s | %s | %.1f | %s |\n", c.Name, strings.Join(c.Criteria, ", "), marks[r.Status], r.Seconds, strings.Join(c.Logic, ", "))
	}
	b.WriteString("\n## Specification Coverage\n\n| Criterion | Requirement | Result |\n|---|---|---|\n")
	for _, cr := range criteria {
		statuses := []string{}
		for _, c := range catalog {
			if slices.Contains(c.Criteria, cr.ID) {
				statuses = append(statuses, results[c.Name].Status)
			}
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", cr.ID, cr.Requirement, coverage(statuses))
	}
	b.WriteString("\nEvidence for each check is in `demo.json`.\n")

	dir := filepath.Join(s.root, "evidence")
	if run != "" && run != "TestDemo" {
		dir = s.dir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "demo.json"), append(raw, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Println("demo report:", filepath.Join(dir, "demo.md"))
	return os.WriteFile(filepath.Join(dir, "demo.md"), []byte(b.String()), 0o644)
}

func coverage(statuses []string) string {
	switch {
	case len(statuses) == 0:
		return "report section"
	case slices.Contains(statuses, "fail"):
		return marks["fail"]
	case slices.Contains(statuses, ""):
		return marks[""]
	case slices.Contains(statuses, "skip"):
		return marks["skip"]
	}
	return marks["pass"]
}
