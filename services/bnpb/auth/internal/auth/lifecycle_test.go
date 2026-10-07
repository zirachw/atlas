package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNaturalExpiryThenRefreshWithoutLogin(t *testing.T) {
	a := testAPI(t, newMemoryStore())
	a.AccessTTL = 2 * time.Second
	h := a.Handler()
	login := decodeTokenResponse(t, postJSON(h, "/auth/login", `{"client_id":"media","client_secret":"media-secret"}`, ""))
	decodeClaims := func(token string) AccessClaims {
		t.Helper()
		raw, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
		if err != nil {
			t.Fatal(err)
		}
		var c AccessClaims
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	old := decodeClaims(login.AccessToken)
	if err := validateClaimValues(old, a.Issuer, a.Audience, time.Now()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(time.Unix(old.ExpiresAt, 0)) + 20*time.Millisecond)
	if err := validateClaimValues(old, a.Issuer, a.Audience, time.Now()); err == nil {
		t.Fatal("naturally expired token accepted")
	}
	response := postJSON(h, "/auth/refresh", `{"refresh_token":"`+login.RefreshToken+`"}`, "")
	if response.Code != http.StatusOK {
		t.Fatalf("refresh=%d %s", response.Code, response.Body)
	}
	current := decodeClaims(decodeTokenResponse(t, response).AccessToken)
	if current.TokenVersion != old.TokenVersion+1 || current.SessionID != old.SessionID || validateClaimValues(current, a.Issuer, a.Audience, time.Now()) != nil {
		t.Fatalf("invalid replacement claims: %+v", current)
	}
}

func TestMalformedRefreshAndValidationIDsNeverReachDatabase(t *testing.T) {
	s := newMemoryStore()
	s.fail = true
	h := testAPI(t, s).Handler()
	for _, token := range []string{"not-a-uuid.secret", "00000000-0000-4000-8000-000000000001.short", ".secret", "too.many.parts"} {
		w := postJSON(h, "/auth/refresh", `{"refresh_token":"`+token+`"}`, "")
		if w.Code != 401 {
			t.Fatalf("malformed token=%q status=%d", token, w.Code)
		}
	}
	w := postJSON(h, "/auth/validate", `{"session_id":"not-a-uuid","token_version":1}`, "Bearer service-secret")
	if w.Code != 400 {
		t.Fatalf("malformed validation=%d", w.Code)
	}
}
