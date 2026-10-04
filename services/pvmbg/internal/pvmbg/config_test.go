package pvmbg

import (
	"testing"
	"time"
)

const testToken = "pvmbg.demo.test-pvmbg-secret-value"

func validTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PVMBG_TOKEN", testToken)
	for _, key := range []string{"HTTP_ADDR", "MIN_RESPONSE_DELAY", "MAX_RESPONSE_DELAY", "EVENT_INTERVAL"} {
		t.Setenv(key, "")
	}
}

func TestConfigDefaultsAndContractBounds(t *testing.T) {
	validTestEnv(t)
	c, err := ConfigFromEnv()
	if err != nil || c.Address != ":8082" || c.Token != testToken || c.MinDelay != 500*time.Millisecond || c.MaxDelay != 3*time.Second || c.EventInterval != 10*time.Second {
		t.Fatal(c, err)
	}
	for _, tc := range []struct{ min, max string }{{"3s", "3s"}, {"500ms", "500ms"}, {"1s", "2s"}} {
		t.Run("accept delay "+tc.min+"-"+tc.max, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv("MIN_RESPONSE_DELAY", tc.min)
			t.Setenv("MAX_RESPONSE_DELAY", tc.max)
			if _, err := ConfigFromEnv(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, tc := range []struct{ key, value string }{
		{"PVMBG_TOKEN", ""},
		{"PVMBG_TOKEN", "example-bmkg-only"},
		{"PVMBG_TOKEN", "pvmbg.demo.too-short"},
		{"PVMBG_TOKEN", "PVMBG.demo.test-pvmbg-secret-value"},
		{"PVMBG_TOKEN", "pvmbg..test-pvmbg-secret-value"},
		{"PVMBG_TOKEN", "pvmbg.Demo.test-pvmbg-secret-value"},
		{"PVMBG_TOKEN", "pvmbg.demo.test pvmbg secret value"},
		{"PVMBG_TOKEN", "pvmbg.demo.test-pvmbg-secret-value.extra"},
		{"MIN_RESPONSE_DELAY", "499ms"},
		{"MAX_RESPONSE_DELAY", "3001ms"},
		{"MAX_RESPONSE_DELAY", "slow"},
		{"EVENT_INTERVAL", "0s"},
		{"EVENT_INTERVAL", "11s"},
	} {
		t.Run("reject "+tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	validTestEnv(t)
	t.Setenv("MIN_RESPONSE_DELAY", "2s")
	t.Setenv("MAX_RESPONSE_DELAY", "1s")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("minimum above maximum accepted")
	}
}

func TestRandomDelayStaysWithinBounds(t *testing.T) {
	random := Config{MinDelay: 500 * time.Millisecond, MaxDelay: 3 * time.Second}.RandomDelay()
	seen := map[time.Duration]bool{}
	for range 1000 {
		d := random()
		if d < 500*time.Millisecond || d > 3*time.Second {
			t.Fatalf("delay %s outside 500ms-3s", d)
		}
		seen[d] = true
	}
	if len(seen) < 100 {
		t.Fatalf("delay does not fluctuate, only %d distinct values", len(seen))
	}
	fixed := Config{MinDelay: 3 * time.Second, MaxDelay: 3 * time.Second}.RandomDelay()
	for range 100 {
		if d := fixed(); d != 3*time.Second {
			t.Fatalf("fixed delay gave %s", d)
		}
	}
}
