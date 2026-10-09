//go:build demo

package demo

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Demo-speed settings within the spec: PVMBG pinned at 3s for P2, polling within the suggested 2-5s.
var overrides = map[string]string{
	"BMKG_PORT": "0", "PVMBG_PORT": "0", "CLIENT_API_PORT": "0", "AUTH_PORT": "0",
	"PVMBG_MIN_RESPONSE_DELAY": "3s", "PVMBG_MAX_RESPONSE_DELAY": "3s",
	"SOURCE_EVENT_INTERVAL": "2s", "POLL_INTERVAL": "3s", "ACCESS_TOKEN_TTL": "60s",
}

var appServices = []string{"bmkg", "pvmbg", "aggregator", "auth", "client-api", "consumer-a", "consumer-b"}

type stack struct {
	root, project, dir string
	env                map[string]string
	args               []string
	bmkg, pvmbg        string
	auth, client, agg  string
}

func startStack() (*stack, error) {
	root, err := filepath.Abs("..")
	if err != nil {
		return nil, err
	}
	s := &stack{root: root, project: cmp.Or(os.Getenv("DEMO_PROJECT"), "atlas-demo")}
	s.dir = filepath.Join(root, "evidence", "local", s.project)
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	envFile := filepath.Join(s.dir, "demo.env")
	// A reused stack must keep its signing keys, or Auth and Client API would be recreated.
	if s.env, err = readEnv(envFile); err != nil {
		if s.env, err = readEnv(filepath.Join(root, ".env.example")); err != nil {
			return nil, err
		}
		if err := addSigningKeys(s.env); err != nil {
			return nil, err
		}
		for k, v := range overrides {
			s.env[k] = v
		}
		if err := writeEnv(envFile, s.env); err != nil {
			return nil, err
		}
	}
	override := filepath.Join(s.dir, "ports.yaml")
	if err := os.WriteFile(override, []byte("services:\n  aggregator:\n    ports: ['127.0.0.1:0:8083']\n"), 0o600); err != nil {
		return nil, err
	}
	s.args = []string{"compose", "--env-file", envFile, "-p", s.project, "-f", filepath.Join(root, "docker-compose.yml"), "-f", override}
	// Consumers bind their queues before the Aggregator publishes anything.
	if _, err := s.compose("up", "-d", "--build", "--wait", "--wait-timeout", "300", "postgres", "rabbitmq", "bmkg", "pvmbg", "auth", "consumer-a", "consumer-b"); err != nil {
		return nil, err
	}
	if _, err := s.compose("up", "-d", "--wait", "--wait-timeout", "180", "aggregator", "client-api"); err != nil {
		return nil, err
	}
	return s, s.resolvePorts()
}

func (s *stack) resolvePorts() error {
	for _, p := range []struct {
		dst     *string
		service string
		port    int
	}{{&s.bmkg, "bmkg", 8081}, {&s.pvmbg, "pvmbg", 8082}, {&s.agg, "aggregator", 8083}, {&s.auth, "auth", 8084}, {&s.client, "client-api", 8080}} {
		out, err := s.compose("port", p.service, fmt.Sprint(p.port))
		if err != nil {
			return err
		}
		*p.dst = "http://" + strings.TrimSpace(out)
	}
	return nil
}

func (s *stack) stop() {
	if os.Getenv("DEMO_KEEP") == "1" {
		return
	}
	_, _ = s.compose("--profile", "consumer-c-demo", "down", "-v", "--remove-orphans")
}

func (s *stack) compose(args ...string) (string, error) {
	cmd := exec.Command("docker", append(slices.Clone(s.args), args...)...)
	cmd.Dir = s.root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, tail(stderr.String(), 2000))
	}
	return stdout.String(), nil
}

func (s *stack) mustCompose(t *testing.T, args ...string) string {
	t.Helper()
	out, err := s.compose(args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func (s *stack) containerID(t *testing.T, service string) string {
	t.Helper()
	return strings.TrimSpace(s.mustCompose(t, "ps", "-q", service))
}

func (s *stack) sql(t *testing.T, statement string) string {
	t.Helper()
	return strings.TrimSpace(s.mustCompose(t, "exec", "-T", "postgres", "psql", "-U", "atlas_admin", "-d", "canonical", "-Atc", statement))
}

// logs returns the JSON log lines a service wrote since the given time.
func (s *stack) logs(t *testing.T, service string, since time.Time) []map[string]any {
	t.Helper()
	out := s.mustCompose(t, "logs", "--no-log-prefix", "--no-color", "--since", since.UTC().Format(time.RFC3339Nano), service)
	var lines []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(out))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var line map[string]any
		if json.Unmarshal(scanner.Bytes(), &line) == nil {
			lines = append(lines, line)
		}
	}
	return lines
}

func (s *stack) logged(t *testing.T, service string, since time.Time, match func(map[string]any) bool) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range s.logs(t, service, since) {
		if match(line) {
			out = append(out, line)
		}
	}
	return out
}

func (s *stack) processed(t *testing.T, service string, since time.Time) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	for _, line := range s.logged(t, service, since, func(l map[string]any) bool { return l["msg"] == "event_processed" }) {
		ids[fmt.Sprint(line["hazard_id"])] = true
	}
	return ids
}

// ready resets the shared source controls, so every check starts from the same state.
func (s *stack) ready(t *testing.T) {
	t.Helper()
	s.admin(t, "outage", `{"enabled":false}`)
	s.admin(t, "schema-version", `{"version":1}`)
	s.mustCompose(t, "start", "consumer-a")
	eventually(t, 60*time.Second, "both sources healthy", func() bool {
		_, body := s.internal(t, "limit=1")
		healthy := 0
		for _, f := range body.Sources {
			if f.Status == "healthy" {
				healthy++
			}
		}
		return healthy == 2
	})
}

func (s *stack) admin(t *testing.T, control, body string) {
	t.Helper()
	if status, _, _ := call(t, http.MethodPost, s.pvmbg+"/admin/"+control, "", body, nil); status != http.StatusOK {
		t.Fatalf("PVMBG %s control status %d", control, status)
	}
}

type freshness struct {
	Source     string  `json:"source"`
	Status     string  `json:"status"`
	StaleSince *string `json:"stale_since"`
}

type hazardPage struct {
	Data    []map[string]any `json:"data"`
	Sources []freshness      `json:"sources"`
}

func (s *stack) internal(t *testing.T, query string) (int, hazardPage) {
	t.Helper()
	status, raw, _ := call(t, http.MethodGet, s.agg+"/internal/hazards?"+query, s.env["AGGREGATOR_INTERNAL_TOKEN"], "", nil)
	var page hazardPage
	_ = json.Unmarshal(raw, &page)
	return status, page
}

func (s *stack) hazards(t *testing.T, token, query string, headers map[string]string) (int, hazardPage) {
	t.Helper()
	status, raw, _ := call(t, http.MethodGet, s.client+"/hazards?"+query, token, "", headers)
	var page hazardPage
	_ = json.Unmarshal(raw, &page)
	return status, page
}

type session struct {
	Access    string `json:"access_token"`
	Refresh   string `json:"refresh_token"`
	ExpiresIn int    `json:"expires_in"`
}

// login takes the env prefix of a client identity, such as MEDIA or FIELD_TEAM.
func (s *stack) login(t *testing.T, identity string) session {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"client_id": s.env[identity+"_CLIENT_ID"], "client_secret": s.env[identity+"_CLIENT_SECRET"]})
	status, raw, _ := call(t, http.MethodPost, s.auth+"/auth/login", "", string(body), nil)
	var out session
	if status != http.StatusOK || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("%s login status %d", identity, status)
	}
	return out
}

var httpClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}

func call(t *testing.T, method, url, token, body string, headers map[string]string) (int, []byte, time.Duration) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, time.Since(start)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, time.Since(start)
}

func eventually(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(time.Second)
	}
}

func readEnv(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	env := map[string]string{}
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			env[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	if len(env) == 0 {
		return nil, errors.New("empty env file " + path)
	}
	return env, nil
}

func writeEnv(path string, env map[string]string) error {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(env)) {
		fmt.Fprintf(&b, "%s=%s\n", k, env[k])
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func addSigningKeys(env map[string]string) error {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return err
	}
	pkix, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		return err
	}
	env["AUTH_ED25519_PRIVATE_KEY_B64"] = base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}))
	env["AUTH_ED25519_PUBLIC_KEY_B64"] = base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pkix}))
	return nil
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

type cachedToken struct {
	value string
	at    time.Time
}

var tokens = map[string]cachedToken{}

// token reuses a login for 40s, safely inside the 60s access TTL.
func (s *stack) token(t *testing.T, identity string) string {
	t.Helper()
	if c, ok := tokens[identity]; ok && time.Since(c.at) < 40*time.Second {
		return c.value
	}
	tokens[identity] = cachedToken{s.login(t, identity).Access, time.Now()}
	return tokens[identity].value
}
