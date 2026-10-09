//go:build demo

package demo

import (
	"encoding/json"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

const columnsSQL = "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name='hazard_events'"

func checkAttributeShapes(t *testing.T, ev map[string]any) {
	s.ready(t)
	columns := s.sql(t, columnsSQL)
	if !strings.Contains(columns, "attributes jsonb") {
		t.Fatalf("attributes is not a JSONB column: %s", columns)
	}
	count := func() string {
		return s.sql(t, "SELECT count(*) FILTER (WHERE attributes ? 'confidence_level') || ' ' || count(*) FILTER (WHERE NOT attributes ? 'confidence_level') FROM hazard_events WHERE source='PVMBG'")
	}
	before := count()
	s.admin(t, "schema-version", `{"version":2}`)
	eventually(t, 60*time.Second, "a stored report with confidence_level", func() bool { return count() != before && !strings.HasPrefix(count(), "0 ") })
	if after := s.sql(t, columnsSQL); after != columns {
		t.Errorf("table columns changed from %q to %q", columns, after)
	}
	_, page := s.internal(t, "source=PVMBG&limit=500")
	shapes := map[bool]int{}
	for _, h := range page.Data {
		_, hasConfidence := attributes(h)["confidence_level"]
		shapes[hasConfidence]++
	}
	if shapes[true] == 0 || shapes[false] == 0 {
		t.Errorf("served shapes %v, want both old and new", shapes)
	}
	ev["columns"] = columns
	ev["stored_with_and_without_confidence"] = count()
	ev["served_old_and_new"] = map[string]int{"old": shapes[false], "new": shapes[true]}
}

func checkStoreOwnership(t *testing.T, ev map[string]any) {
	denied := map[string]bool{}
	for _, tc := range []struct{ role, password, database string }{
		{"auth", s.env["AUTH_DB_PASSWORD"], "canonical"},
		{"aggregator", s.env["CANONICAL_DB_PASSWORD"], "auth"},
	} {
		_, err := s.compose("exec", "-T", "-e", "PGPASSWORD="+tc.password, "postgres", "psql", "-h", "127.0.0.1", "-U", tc.role, "-d", tc.database, "-c", "SELECT 1")
		denied[tc.role+" on "+tc.database] = err != nil && strings.Contains(err.Error(), "permission denied")
		if !denied[tc.role+" on "+tc.database] {
			t.Errorf("role %s reached database %s: %v", tc.role, tc.database, err)
		}
	}
	for _, service := range []string{"client-api", "auth", "consumer-a", "consumer-b"} {
		if env := s.mustCompose(t, "exec", "-T", service, "env"); strings.Contains(env, s.env["CANONICAL_DB_PASSWORD"]) {
			t.Errorf("%s holds Canonical Store credentials", service)
		}
	}
	// Every store sits on an internal network that only its owner and its database join.
	members := map[string][]string{}
	for network, want := range map[string][]string{
		"canonical": {"aggregator", "postgres"}, "auth-store": {"auth", "postgres"},
		"bmkg-store": {"bmkg", "bmkg-db"}, "pvmbg-store": {"pvmbg", "pvmbg-db"},
	} {
		out, err := exec.Command("docker", "network", "inspect", "-f", "{{range .Containers}}{{.Name}} {{end}}", s.project+"_"+network).Output()
		if err != nil {
			t.Fatalf("inspect network %s: %v", network, err)
		}
		for _, name := range strings.Fields(string(out)) {
			service := strings.TrimPrefix(name, s.project+"-")
			service = service[:strings.LastIndex(service, "-")]
			members[network] = append(members[network], service)
		}
		slices.Sort(members[network])
		slices.Sort(want)
		if !slices.Equal(members[network], want) {
			t.Errorf("network %s joined by %v, want only %v", network, members[network], want)
		}
	}
	if status, _ := s.hazards(t, s.token(t, "INTERNAL_OPS"), "limit=5", nil); status != http.StatusOK {
		t.Errorf("Client API read through the Aggregator status %d", status)
	}
	ev["denied"] = denied
	ev["store_networks"] = members
}

func checkIndependentRebuild(t *testing.T, ev map[string]any) {
	s.ready(t)
	cutoff := time.Now().UTC().Format(time.RFC3339Nano)
	storedSQL := "SELECT count(*) || ' ' || md5(string_agg(source_ref_id || hazard_id, ',' ORDER BY source_ref_id)) FROM hazard_events WHERE ingested_at < '" + cutoff + "'"
	stored := s.sql(t, storedSQL)
	checkpoints := s.sql(t, "SELECT source || '|' || checkpoint FROM source_progress ORDER BY source")
	peers := map[string]string{}
	for _, service := range []string{"postgres", "rabbitmq", "bmkg", "pvmbg", "auth", "client-api", "consumer-a", "consumer-b"} {
		peers[service] = s.containerID(t, service)
	}
	aggregator := s.containerID(t, "aggregator")
	done := make(chan error, 1)
	go func() {
		_, err := s.compose("up", "-d", "--no-deps", "--build", "--force-recreate", "--wait", "--wait-timeout", "180", "aggregator")
		done <- err
	}()
	// Requests that do not need the Aggregator must keep working while it is replaced.
	independent := map[string]string{"auth login": s.auth + "/auth/login", "bmkg": s.bmkg + "/seismic-events?since=2999-01-01T00:00:00Z"}
	login, _ := json.Marshal(map[string]string{"client_id": s.env["MEDIA_CLIENT_ID"], "client_secret": s.env["MEDIA_CLIENT_SECRET"]})
	samples, failures, aggregatorDown := 0, 0, 0
	for waiting := true; waiting; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			waiting = false
		case <-time.After(200 * time.Millisecond):
			samples++
			if status, _, _ := call(t, http.MethodPost, independent["auth login"], "", string(login), nil); status != http.StatusOK {
				failures++
			}
			if status, _, _ := call(t, http.MethodGet, independent["bmkg"], "", "", map[string]string{"X-BMKG-Key": s.env["BMKG_API_KEY"]}); status != http.StatusOK {
				failures++
			}
			if status, _, _ := call(t, http.MethodGet, s.agg+"/health", "", "", nil); status != http.StatusOK {
				aggregatorDown++
			}
		}
	}
	if err := s.resolvePorts(); err != nil {
		t.Fatal(err)
	}
	if failures > 0 {
		t.Errorf("%d independent requests failed during the rebuild", failures)
	}
	if aggregatorDown == 0 {
		t.Error("never observed the Aggregator down, so the rebuild was not exercised")
	}
	if s.containerID(t, "aggregator") == aggregator {
		t.Error("the Aggregator container was not replaced")
	}
	for service, id := range peers {
		if s.containerID(t, service) != id {
			t.Errorf("%s restarted during the Aggregator rebuild", service)
		}
	}
	s.ready(t)
	if after := s.sql(t, storedSQL); after != stored {
		t.Errorf("events stored before the rebuild changed from %q to %q", stored, after)
	}
	for _, row := range strings.Split(checkpoints, "\n") {
		source, checkpoint, _ := strings.Cut(row, "|")
		if kept := s.sql(t, "SELECT checkpoint >= '"+checkpoint+"' FROM source_progress WHERE source='"+source+"'"); kept != "t" {
			t.Errorf("%s checkpoint went back after the rebuild", source)
		}
	}
	ev["samples_during_rebuild"] = samples
	ev["independent_failures"] = failures
	ev["aggregator_down_samples"] = aggregatorDown
	ev["stored_before_rebuild"] = stored
	ev["checkpoints_before"] = checkpoints
}
