//go:build demo

package demo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var serviceDirs = map[string]string{
	"bmkg": "services/bmkg", "pvmbg": "services/pvmbg", "aggregator": "services/bnpb/aggregator",
	"auth": "services/bnpb/auth", "client-api": "services/bnpb/client-api",
	"consumer-a": "consumers/consumer-a", "consumer-b": "consumers/consumer-b",
}

func checkContainers(t *testing.T, ev map[string]any) {
	out := s.mustCompose(t, "ps", "--format", "json")
	seen := map[string]string{}
	ids := map[string]bool{}

	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var c struct{ Service, State, Health, ID string }
		if json.Unmarshal([]byte(line), &c) != nil {
			continue
		}

		seen[c.Service] = c.State + "/" + c.Health
		ids[c.ID] = true
	}

	for _, service := range append([]string{"postgres", "rabbitmq", "bmkg-db", "pvmbg-db"}, appServices...) {
		if seen[service] != "running/healthy" {
			t.Errorf("%s is %q, want running/healthy", service, seen[service])
		}
	}

	for service, dir := range serviceDirs {
		if _, err := os.Stat(filepath.Join(s.root, dir, "Dockerfile")); err != nil {
			t.Errorf("%s has no Dockerfile in %s", service, dir)
		}
	}

	if len(ids) < len(seen) {
		t.Error("services share a container")
	}

	ev["containers"] = seen
}

func checkNoSharedCode(t *testing.T, ev map[string]any) {
	modules := map[string]string{}

	err := filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() && (d.Name() == ".git" || d.Name() == "docs" || d.Name() == "evidence") {
			return filepath.SkipDir
		}

		if d.Name() == "go.mod" {
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			first, _, _ := strings.Cut(string(raw), "\n")
			modules[strings.TrimSpace(strings.TrimPrefix(first, "module"))] = filepath.Dir(path)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for module, dir := range modules {
		cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./...")
		cmd.Dir = dir

		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list in %s: %v", dir, err)
		}

		for _, imp := range strings.Fields(string(out)) {
			if strings.HasPrefix(imp, "atlas/") && !strings.HasPrefix(imp, module) {
				t.Errorf("%s imports %s from another service", module, imp)
			}
		}
	}

	ev["modules"] = len(modules)
}

func checkHealth(t *testing.T, ev map[string]any) {
	statuses := map[string]int{}

	for name, url := range map[string]string{
		"bmkg":       s.bmkg,
		"pvmbg":      s.pvmbg,
		"aggregator": s.agg,
		"auth":       s.auth,
		"client-api": s.client,
	} {
		status, _, _ := call(t, http.MethodGet, url+"/health", "", "", nil)

		statuses[name] = status
		if status != http.StatusOK {
			t.Errorf("%s health status %d", name, status)
		}
	}

	ev["health"] = statuses
	ev["consumers"] = "health checked by Docker, see R3"
}

func checkCorrelation(t *testing.T, ev map[string]any) {
	s.ready(t)

	since := time.Now().Add(-2 * time.Second)

	id := fmt.Sprintf("demo-%d", time.Now().UnixNano())

	headers := map[string]string{"X-Correlation-ID": id}
	if status, _ := s.hazards(t, s.token(t, "INTERNAL_OPS"), "source=BMKG&limit=5", headers); status != http.StatusOK {
		t.Fatalf("client read status %d", status)
	}

	byID := func(want string) func(map[string]any) bool {
		return func(l map[string]any) bool { return l["correlation_id"] == want }
	}
	for _, service := range []string{"client-api", "auth", "aggregator"} {
		if len(s.logged(t, service, since, byID(id))) == 0 {
			t.Errorf("client request %s missing from %s logs", id, service)
		}
	}

	var poll, publication string

	ids := func(lines []map[string]any) map[string]bool {
		out := map[string]bool{}
		for _, l := range lines {
			out[fmt.Sprint(l["correlation_id"])] = true
		}

		return out
	}

	eventually(t, 45*time.Second, "a polled BMKG call and a consumed publication", func() bool {
		atSource := ids(s.logs(t, "bmkg", since))

		consumed := ids(s.logged(t, "consumer-a", since, func(l map[string]any) bool {
			return l["msg"] == "event_processed"
		}))
		for _, l := range s.logs(t, "aggregator", since) {
			cid := fmt.Sprint(l["correlation_id"])
			switch {
			case l["msg"] == "source_call" && l["source"] == "BMKG" && atSource[cid]:
				poll = cid
			case l["msg"] == "broker_call" && l["confirmed"] == true && consumed[cid]:
				publication = cid
			}
		}

		return poll != "" && publication != ""
	})

	ev["client_flow"] = id + " seen in client-api, auth, aggregator"
	ev["poll_flow"] = poll + " seen in aggregator and bmkg"
	ev["event_flow"] = publication + " seen in aggregator and consumer-a"
}

func checkOutboundLatency(t *testing.T, ev map[string]any) {
	s.ready(t)

	since := time.Now().Add(-2 * time.Second)

	s.hazards(t, s.token(t, "INTERNAL_OPS"), "limit=5", nil)

	required := []struct{ service, msg, dependency string }{
		{"client-api", "outbound_call", "auth"},
		{"client-api", "outbound_call", "aggregator"},
		{"auth", "database_call", "auth_store"},
		{"aggregator", "source_call", "BMKG"},
		{"aggregator", "source_call", "PVMBG"},
		{"aggregator", "broker_call", "rabbitmq"},
		{"aggregator", "database_call", ""},
		{"consumer-a", "broker_ack", "rabbitmq"},
		{"consumer-b", "broker_ack", "rabbitmq"},
		{"bmkg", "database_call", "bmkg_store"},
		{"pvmbg", "database_call", "pvmbg_store"},
	}
	missing := map[string]bool{}
	deadline := time.Now().Add(45 * time.Second)

	for {
		clear(missing)

		logs := map[string][]map[string]any{}
		for _, r := range required {
			if _, ok := logs[r.service]; !ok {
				logs[r.service] = s.logs(t, r.service, since)
			}

			if !slices.ContainsFunc(logs[r.service], func(l map[string]any) bool {
				_, timed := l["latency_ms"].(float64)
				return l["msg"] == r.msg && timed && (r.dependency == "" || l["dependency"] == r.dependency)
			}) {
				missing[r.service+" "+r.msg+" "+r.dependency] = true
			}
		}

		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}

		time.Sleep(2 * time.Second)
	}

	ev["calls_checked"] = len(required)

	for call := range missing {
		t.Errorf("no latency_ms logged for %s", strings.TrimSpace(call))
	}
}
