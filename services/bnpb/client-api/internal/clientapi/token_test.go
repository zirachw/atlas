package clientapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestJWTTrustAlgorithmAndRequiredClaims(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	base := AccessClaims{Issuer: "atlas-auth", Audience: "atlas-client-api", Subject: "media", ClientID: "media", Scope: "hazards:summary", SessionID: "00000000-0000-4000-8000-000000000001", TokenVersion: 1, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), TokenID: "jti"}
	encode := func(c AccessClaims, algorithm string) string {
		header, _ := json.Marshal(map[string]string{"alg": algorithm, "typ": "JWT"})
		payload, _ := json.Marshal(c)
		unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
		return unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(unsigned)))
	}
	if _, err := verifyAccessToken(encode(base, "EdDSA"), public, base.Issuer, base.Audience, now); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*AccessClaims)
	}{
		{"issuer", func(c *AccessClaims) { c.Issuer = "other" }},
		{"audience", func(c *AccessClaims) { c.Audience = "other" }},
		{"client mismatch", func(c *AccessClaims) { c.ClientID = "field" }},
		{"scope", func(c *AccessClaims) { c.Scope = "admin" }},
		{"session", func(c *AccessClaims) { c.SessionID = "" }},
		{"version", func(c *AccessClaims) { c.TokenVersion = 0 }},
		{"token ID", func(c *AccessClaims) { c.TokenID = "" }},
		{"issued at", func(c *AccessClaims) { c.IssuedAt = 0 }},
		{"future issue", func(c *AccessClaims) {
			c.IssuedAt = now.Add(2 * time.Minute).Unix()
			c.ExpiresAt = now.Add(3 * time.Minute).Unix()
		}},
		{"expiry", func(c *AccessClaims) { c.ExpiresAt = now.Unix() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if _, err := verifyAccessToken(encode(c, "EdDSA"), public, base.Issuer, base.Audience, now); err == nil {
				t.Fatal("bad claims accepted")
			}
		})
	}
	for _, alg := range []string{"none", "HS256", "RS256"} {
		if _, err := verifyAccessToken(encode(base, alg), public, base.Issuer, base.Audience, now); err == nil {
			t.Fatalf("accepted algorithm %s", alg)
		}
	}
	wrong, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := verifyAccessToken(encode(base, "EdDSA"), wrong, base.Issuer, base.Audience, now); err == nil {
		t.Fatal("untrusted signing key accepted")
	}
}
