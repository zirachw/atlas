package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	sessionID, secret, ok := strings.Cut(token, ".")
	if !ok || !validSessionID(sessionID) {
		return false
	}

	raw, err := base64.RawURLEncoding.DecodeString(secret)

	return err == nil && len(raw) == 32
}

func newOpaqueToken() string {
	var raw [32]byte

	_, _ = rand.Read(raw[:])

	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func newRefreshToken(sessionID string) string {
	return sessionID + "." + newOpaqueToken()
}

// A version 4 UUID, the format the sessions table stores as uuid.
func newSessionID() string {
	var raw [16]byte

	_, _ = rand.Read(raw[:])
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

func signAccessToken(key ed25519.PrivateKey, claims AccessClaims) string {
	header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT"})
	payload, _ := json.Marshal(claims)

	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	sig := ed25519.Sign(key, []byte(unsigned))

	return unsigned + "." + enc.EncodeToString(sig)
}

func newClaims(
	issuer, audience string,
	client ClientCredential,
	sessionID string,
	version int64,
	now time.Time,
	ttl time.Duration,
) AccessClaims {
	return AccessClaims{
		Issuer:       issuer,
		Audience:     audience,
		Subject:      client.ID,
		ClientID:     client.ID,
		Scope:        client.Scope,
		SessionID:    sessionID,
		TokenVersion: version,
		IssuedAt:     now.Unix(),
		ExpiresAt:    now.Add(ttl).Unix(),
		TokenID:      newOpaqueToken(),
	}
}
