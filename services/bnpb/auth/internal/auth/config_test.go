package auth

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

	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}

	key := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	for name, value := range map[string]string{
		"AUTH_DATABASE_URL":            "postgres://example:example@localhost/auth",
		"AUTH_VALIDATE_TOKEN":          "test-validate",
		"AUTH_ED25519_PRIVATE_KEY_B64": key,
		"MEDIA_CLIENT_ID":              "media",
		"MEDIA_CLIENT_SECRET":          "media-secret",
		"FIELD_TEAM_CLIENT_ID":         "field-team",
		"FIELD_TEAM_CLIENT_SECRET":     "field-secret",
		"INTERNAL_OPS_CLIENT_ID":       "internal-ops",
		"INTERNAL_OPS_CLIENT_SECRET":   "ops-secret",
	} {
		t.Setenv(name, value)
	}

	for _, name := range []string{"AUTH_HTTP_ADDR", "ACCESS_TOKEN_TTL", "REFRESH_TOKEN_TTL"} {
		t.Setenv(name, "")
	}
}

func TestConfigDefaultsAndRejection(t *testing.T) {
	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.Address != ":8084" || c.AccessTTL != 60*time.Second || c.RefreshTTL != 7*24*time.Hour {
		t.Fatal(c, err)
	}

	if len(c.Clients) != 3 || c.Clients[0].Scope != "hazards:summary" || c.Clients[1].Scope != "hazards:raw" {
		t.Fatalf("clients %+v", c.Clients)
	}

	for _, tc := range []struct{ key, value string }{
		{"AUTH_DATABASE_URL", ""},
		{"AUTH_VALIDATE_TOKEN", ""},
		{"AUTH_ED25519_PRIVATE_KEY_B64", "not-a-key"},
		{"MEDIA_CLIENT_SECRET", ""},
		{"FIELD_TEAM_CLIENT_ID", "media"},
		{"INTERNAL_OPS_CLIENT_SECRET", "media-secret"},
		{"ACCESS_TOKEN_TTL", "500ms"},
		{"REFRESH_TOKEN_TTL", "bad"},
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
