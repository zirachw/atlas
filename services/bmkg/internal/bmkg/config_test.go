package bmkg

import (
	"testing"
	"time"
)

func validTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BMKG_API_KEY", "test-bmkg")
	t.Setenv("DATABASE_URL", "postgres://bmkg:test@localhost/bmkg")
	for _, key := range []string{"HTTP_ADDR", "RESPONSE_DELAY", "EVENT_INTERVAL"} {
		t.Setenv(key, "")
	}
}

func TestConfigDefaultsAndContractBounds(t *testing.T) {
	validTestEnv(t)
	c, err := ConfigFromEnv()
	if err != nil || c.Address != ":8081" || c.APIKey != "test-bmkg" || c.Delay != 100*time.Millisecond || c.EventInterval != 10*time.Second {
		t.Fatal(c, err)
	}
	for _, tc := range []struct{ key, value string }{{"RESPONSE_DELAY", "50ms"}, {"RESPONSE_DELAY", "150ms"}, {"EVENT_INTERVAL", "1s"}, {"EVENT_INTERVAL", "10s"}} {
		t.Run("accept "+tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := ConfigFromEnv(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, tc := range []struct{ key, value string }{{"DATABASE_URL", ""}, {"BMKG_API_KEY", ""}, {"BMKG_API_KEY", "key with space"}, {"RESPONSE_DELAY", "49ms"}, {"RESPONSE_DELAY", "151ms"}, {"RESPONSE_DELAY", "fast"}, {"EVENT_INTERVAL", "0s"}, {"EVENT_INTERVAL", "-1s"}, {"EVENT_INTERVAL", "11s"}} {
		t.Run("reject "+tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
