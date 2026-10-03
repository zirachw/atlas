package clientapi

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

type Config struct {
	AuthURL, AggregatorURL, AggregatorToken, ValidateToken, Address string
	Issuer, Audience                                                string
	PublicKey                                                       ed25519.PublicKey
	OutboundTimeout                                                 time.Duration
}

func decodeBase64(raw string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(raw); err == nil {
		return b, nil
	}
	return base64.StdEncoding.DecodeString(raw)
}
func parsePublicKey(raw string) (ed25519.PublicKey, error) {
	encoded, err := decodeBase64(raw)
	if err != nil {
		return nil, errors.New("invalid public key encoding")
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		return nil, errors.New("public key must be a base64-encoded SubjectPublicKeyInfo PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid SubjectPublicKeyInfo key")
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("verification key must be Ed25519")
	}
	return key, nil
}
func requireOrigin(name, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return fmt.Errorf("%s must be an HTTP origin without credentials", name)
	}
	return nil
}
func ConfigFromEnv() (Config, error) {
	c := Config{
		AuthURL: os.Getenv("AUTH_BASE_URL"), AggregatorURL: os.Getenv("AGGREGATOR_BASE_URL"),
		AggregatorToken: os.Getenv("AGGREGATOR_INTERNAL_TOKEN"), ValidateToken: os.Getenv("AUTH_VALIDATE_TOKEN"),
		Address: os.Getenv("CLIENT_API_HTTP_ADDR"), Issuer: "atlas-auth", Audience: "atlas-client-api",
	}
	for name, value := range map[string]string{"AUTH_BASE_URL": c.AuthURL, "AGGREGATOR_BASE_URL": c.AggregatorURL, "AGGREGATOR_INTERNAL_TOKEN": c.AggregatorToken, "AUTH_VALIDATE_TOKEN": c.ValidateToken} {
		if value == "" {
			return c, fmt.Errorf("%s is required", name)
		}
	}
	if err := requireOrigin("AUTH_BASE_URL", c.AuthURL); err != nil {
		return c, err
	}
	if err := requireOrigin("AGGREGATOR_BASE_URL", c.AggregatorURL); err != nil {
		return c, err
	}
	key, err := parsePublicKey(os.Getenv("AUTH_ED25519_PUBLIC_KEY_B64"))
	if err != nil {
		return c, err
	}
	c.PublicKey = key
	if c.Address == "" {
		c.Address = ":8080"
	}
	c.OutboundTimeout = 3 * time.Second
	if raw := os.Getenv("CLIENT_API_OUTBOUND_TIMEOUT"); raw != "" {
		c.OutboundTimeout, err = time.ParseDuration(raw)
		if err != nil || c.OutboundTimeout < time.Second || c.OutboundTimeout > time.Minute {
			return c, errors.New("invalid CLIENT_API_OUTBOUND_TIMEOUT")
		}
	}
	return c, nil
}
