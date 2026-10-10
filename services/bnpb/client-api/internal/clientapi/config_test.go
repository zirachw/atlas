package clientapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
	"time"
)

func validTestEnv(t *testing.T) {
	t.Helper()

	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	der, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}

	key := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	for name, value := range map[string]string{
		"AUTH_BASE_URL":               "http://auth:8084",
		"AGGREGATOR_BASE_URL":         "http://aggregator:8083",
		"AGGREGATOR_INTERNAL_TOKEN":   "test-internal",
		"AUTH_VALIDATE_TOKEN":         "test-validate",
		"AUTH_ED25519_PUBLIC_KEY_B64": key,
	} {
		t.Setenv(name, value)
	}

	for _, name := range []string{
		"CLIENT_API_HTTP_ADDR",
		"CLIENT_API_OUTBOUND_TIMEOUT",
		"CLIENT_API_REQUEST_CAPACITY",
	} {
		t.Setenv(name, "")
	}
}

func TestConfigDefaultsAndRejection(t *testing.T) {
	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.Address != ":8080" || c.OutboundTimeout != 3*time.Second || c.RequestCapacity != 128 {
		t.Fatal(c, err)
	}

	for _, tc := range []struct{ key, value string }{
		{"AUTH_BASE_URL", ""},
		{"AGGREGATOR_BASE_URL", "http://user:pass@aggregator:8083"},
		{"AGGREGATOR_BASE_URL", "http://aggregator:8083/internal"},
		{"AUTH_ED25519_PUBLIC_KEY_B64", "not-a-key"},
		{"CLIENT_API_OUTBOUND_TIMEOUT", "500ms"},
		{"CLIENT_API_REQUEST_CAPACITY", "0"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
