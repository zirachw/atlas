//go:build demo

package demo

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

var summaryFields = []string{
	"area_name",
	"hazard_id",
	"hazard_type",
	"ingested_at",
	"occurred_at",
	"severity",
	"source",
}

func checkAgencyCredentials(t *testing.T, ev map[string]any) {
	bmkgKey, pvmbgToken := s.env["BMKG_API_KEY"], s.env["PVMBG_TOKEN"]
	got := map[string]int{}

	for _, tc := range []struct {
		name, url, header, value string
		want                     []int
	}{
		{"BMKG accepts its key", s.bmkg + "/seismic-events", "X-BMKG-Key", bmkgKey, []int{200}},
		{"BMKG rejects a missing key", s.bmkg + "/seismic-events", "", "", []int{401}},
		{
			"BMKG rejects the PVMBG token as bearer",
			s.bmkg + "/seismic-events",
			"Authorization",
			"Bearer " + pvmbgToken,
			[]int{401, 403},
		},
		{
			"BMKG rejects the PVMBG token as key",
			s.bmkg + "/tsunami-warnings",
			"X-BMKG-Key",
			pvmbgToken,
			[]int{401, 403},
		},
		{"PVMBG accepts its token", s.pvmbg + "/volcanic-reports", "Authorization", "Bearer " + pvmbgToken, []int{200}},
		{"PVMBG rejects a missing token", s.pvmbg + "/volcanic-reports", "", "", []int{401}},
		{
			"PVMBG rejects the BMKG key as bearer",
			s.pvmbg + "/volcanic-reports",
			"Authorization",
			"Bearer " + bmkgKey,
			[]int{401, 403},
		},
		{"PVMBG rejects the BMKG key header", s.pvmbg + "/volcanic-reports", "X-BMKG-Key", bmkgKey, []int{401, 403}},
		{
			"Aggregator rejects the BMKG key",
			s.agg + "/internal/hazards",
			"Authorization",
			"Bearer " + bmkgKey,
			[]int{401, 403},
		},
		{
			"Client API rejects the PVMBG token",
			s.client + "/hazards",
			"Authorization",
			"Bearer " + pvmbgToken,
			[]int{401, 403},
		},
	} {
		headers := map[string]string{}
		if tc.header != "" {
			headers[tc.header] = tc.value
		}

		status, _, _ := call(t, http.MethodGet, tc.url, "", "", headers)

		got[tc.name] = status
		if !slices.Contains(tc.want, status) {
			t.Errorf("%s: status %d, want %v", tc.name, status, tc.want)
		}
	}

	ev["statuses"] = got
}

func checkMediaScope(t *testing.T, ev map[string]any) {
	s.ready(t)
	media := s.token(t, "MEDIA")

	status, page := s.hazards(t, media, "limit=20", nil)
	if status != http.StatusOK || len(page.Data) == 0 {
		t.Fatalf("Media read status %d with %d records", status, len(page.Data))
	}

	for _, h := range page.Data {
		for field := range h {
			if !slices.Contains(summaryFields, field) {
				t.Fatalf("Media response leaked raw field %s", field)
			}
		}
	}

	rawStatus, rawBody, _ := call(t, http.MethodGet, s.client+"/hazards?view=raw", media, "", nil)
	if rawStatus != http.StatusUnauthorized && rawStatus != http.StatusForbidden {
		t.Errorf("Media raw request status %d, want 401 or 403", rawStatus)
	}

	if strings.Contains(string(rawBody), "latitude") || strings.Contains(string(rawBody), "attributes") {
		t.Error("raw fields leaked in the rejection body")
	}

	fieldStatus, field := s.hazards(t, s.token(t, "FIELD_TEAM"), "view=raw&limit=5", nil)
	if fieldStatus != http.StatusOK || len(field.Data) == 0 || field.Data[0]["latitude"] == nil ||
		field.Data[0]["attributes"] == nil {
		t.Errorf("Field Team raw read status %d without raw fields", fieldStatus)
	}

	ev["media_fields"] = summaryFields
	ev["media_raw_status"] = rawStatus
	ev["field_team_raw_status"] = fieldStatus
}

func checkTokenRefresh(t *testing.T, ev map[string]any) {
	s.ready(t)

	read := func(token string) int {
		status, _ := s.hazards(t, token, "view=raw&limit=1", nil)
		return status
	}
	refresh := func(token string) (int, session) {
		body, _ := json.Marshal(map[string]string{"refresh_token": token})
		status, raw, _ := call(t, http.MethodPost, s.auth+"/auth/refresh", "", string(body), nil)

		var out session

		_ = json.Unmarshal(raw, &out)

		return status, out
	}
	first := s.login(t, "FIELD_TEAM")
	got := map[string]int{"fresh token": read(first.Access)}
	t.Logf("waiting %ds for the access token to expire naturally", first.ExpiresIn+2)
	time.Sleep(time.Duration(first.ExpiresIn+2) * time.Second)
	got["expired token"] = read(first.Access)

	var second, third session

	got["refresh without login"], second = refresh(first.Refresh)
	got["refreshed token"] = read(second.Access)
	// Rotating again while second is inside its TTL shows the old token is rejected by version, not expiry.
	got["second refresh"], third = refresh(second.Refresh)
	got["superseded token"] = read(second.Access)
	got["latest token"] = read(third.Access)
	got["reused refresh token"], _ = refresh(first.Refresh)
	// A reused refresh token revokes the session, so even the latest token stops working.
	got["latest token after reuse"] = read(third.Access)

	want := map[string]int{
		"fresh token": 200, "expired token": 401, "refresh without login": 200, "refreshed token": 200,
		"second refresh": 200, "superseded token": 401, "latest token": 200, "reused refresh token": 401,
		"latest token after reuse": 401,
	}
	for step, status := range want {
		if got[step] != status {
			t.Errorf("%s status %d, want %d", step, got[step], status)
		}
	}

	ev["ttl_seconds"] = first.ExpiresIn
	ev["statuses"] = got
}

func checkNoCommittedSecrets(t *testing.T, ev map[string]any) {
	git := func(args ...string) string {
		out, err := exec.Command("git", append([]string{"-C", s.root}, args...)...).Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}

		return string(out)
	}
	if strings.TrimSpace(git("ls-files", ".env")) != "" {
		t.Error(".env is committed")
	}

	example, err := readEnv(filepath.Join(s.root, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}

	compose, err := os.ReadFile(filepath.Join(s.root, "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range regexp.MustCompile(`\$\{([A-Z0-9_]+)`).FindAllStringSubmatch(string(compose), -1) {
		if _, ok := example[m[1]]; !ok {
			t.Errorf("%s is used by Compose but missing from .env.example", m[1])
		}
	}

	secret := regexp.MustCompile(`PASSWORD|SECRET|TOKEN|_KEY`)
	scanned := 0

	for _, path := range strings.Fields(git("ls-files")) {
		if strings.HasSuffix(path, "_test.go") || slices.Contains([]string{".env.example", "README.md"}, path) ||
			strings.HasPrefix(path, "evidence/") {
			continue
		}

		raw, err := os.ReadFile(filepath.Join(s.root, path))
		if err != nil {
			continue
		}

		scanned++

		for key, value := range s.env {
			if secret.MatchString(key) && len(value) >= 8 && strings.Contains(string(raw), value) {
				t.Errorf("%s contains the value of %s", path, key)
			}
		}
	}

	ev["files_scanned"] = scanned
	ev["env_example_variables"] = len(example)
}
