package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type AccessClaims struct {
	Issuer       string `json:"iss"`
	Audience     string `json:"aud"`
	Subject      string `json:"sub"`
	ClientID     string `json:"client_id"`
	Scope        string `json:"scope"`
	SessionID    string `json:"session_id"`
	TokenVersion int64  `json:"token_version"`
	IssuedAt     int64  `json:"iat"`
	ExpiresAt    int64  `json:"exp"`
	TokenID      string `json:"jti"`
}

func validSessionID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil && len(raw) == 16
}

func validRefreshToken(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 2 || !validSessionID(parts[0]) {
		return false
	}
	secret, err := base64.RawURLEncoding.DecodeString(parts[1])
	return err == nil && len(secret) == 32
}

func newOpaqueToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func newRefreshToken(sessionID string) (string, error) {
	secret, err := newOpaqueToken()
	if err != nil {
		return "", err
	}
	return sessionID + "." + secret, nil
}

func newSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}

func signAccessToken(key ed25519.PrivateKey, claims AccessClaims) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	sig := ed25519.Sign(key, []byte(unsigned))
	return unsigned + "." + enc.EncodeToString(sig), nil
}

func newClaims(issuer, audience string, client ClientCredential, sessionID string, version int64, now time.Time, ttl time.Duration) (AccessClaims, error) {
	jti, err := newOpaqueToken()
	if err != nil {
		return AccessClaims{}, err
	}
	return AccessClaims{
		Issuer: issuer, Audience: audience, Subject: client.ID, ClientID: client.ID,
		Scope: client.Scope, SessionID: sessionID, TokenVersion: version,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix(), TokenID: jti,
	}, nil
}

func validateClaimValues(c AccessClaims, issuer, audience string, now time.Time) error {
	if c.Issuer != issuer || c.Audience != audience || c.Subject == "" || c.ClientID == "" || c.Subject != c.ClientID || c.SessionID == "" || c.TokenVersion < 1 || c.TokenID == "" {
		return errors.New("invalid token claims")
	}
	if c.Scope != "hazards:summary" && c.Scope != "hazards:raw" {
		return errors.New("invalid token scope")
	}
	if c.IssuedAt <= 0 || c.ExpiresAt <= c.IssuedAt || now.Unix() >= c.ExpiresAt || c.IssuedAt > now.Add(time.Minute).Unix() {
		return errors.New("expired or invalid token time")
	}
	return nil
}
