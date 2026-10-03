package auth

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type ClientCredential struct {
	ID     string
	Secret string
	Scope  string
}

type Config struct {
	DatabaseURL, ValidateToken, Address, Issuer, Audience string
	PrivateKey                                            ed25519.PrivateKey
	Clients                                               []ClientCredential
	AccessTTL, RefreshTTL                                 time.Duration
}

func decodeBase64(raw string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(raw); err == nil {
		return b, nil
	}
	return base64.StdEncoding.DecodeString(raw)
}

func parsePrivateKey(raw string) (ed25519.PrivateKey, error) {
	encoded, err := decodeBase64(raw)
	if err != nil {
		return nil, errors.New("invalid signing key encoding")
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		return nil, errors.New("signing key must be a base64-encoded PKCS#8 PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid PKCS#8 signing key")
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("signing key must be Ed25519")
	}
	return key, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < time.Second || d > 365*24*time.Hour {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return d, nil
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		DatabaseURL:   os.Getenv("AUTH_DATABASE_URL"),
		ValidateToken: os.Getenv("AUTH_VALIDATE_TOKEN"),
		Address:       os.Getenv("AUTH_HTTP_ADDR"),
		Issuer:        "atlas-auth",
		Audience:      "atlas-client-api",
	}
	if c.DatabaseURL == "" || c.ValidateToken == "" {
		return c, errors.New("AUTH_DATABASE_URL and AUTH_VALIDATE_TOKEN are required")
	}
	key, err := parsePrivateKey(os.Getenv("AUTH_ED25519_PRIVATE_KEY_B64"))
	if err != nil {
		return c, err
	}
	c.PrivateKey = key
	if c.Address == "" {
		c.Address = ":8084"
	}
	c.AccessTTL, err = durationEnv("ACCESS_TOKEN_TTL", 60*time.Second)
	if err != nil {
		return c, err
	}
	c.RefreshTTL, err = durationEnv("REFRESH_TOKEN_TTL", 7*24*time.Hour)
	if err != nil {
		return c, err
	}
	c.Clients = []ClientCredential{
		{ID: os.Getenv("MEDIA_CLIENT_ID"), Secret: os.Getenv("MEDIA_CLIENT_SECRET"), Scope: "hazards:summary"},
		{ID: os.Getenv("FIELD_TEAM_CLIENT_ID"), Secret: os.Getenv("FIELD_TEAM_CLIENT_SECRET"), Scope: "hazards:raw"},
		{ID: os.Getenv("INTERNAL_OPS_CLIENT_ID"), Secret: os.Getenv("INTERNAL_OPS_CLIENT_SECRET"), Scope: "hazards:raw"},
	}
	seenID, seenSecret := map[string]bool{}, map[string]bool{}
	for _, client := range c.Clients {
		if client.ID == "" || client.Secret == "" || seenID[client.ID] || seenSecret[client.Secret] {
			return c, errors.New("three distinct client IDs and secrets are required")
		}
		seenID[client.ID], seenSecret[client.Secret] = true, true
	}
	return c, nil
}

func parsePositiveInt64(s string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 1 {
		return 0, errors.New("invalid positive integer")
	}
	return v, nil
}
