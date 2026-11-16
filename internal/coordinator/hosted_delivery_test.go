package coordinator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type hostedRoundTrip func(*http.Request) (*http.Response, error)

func (f hostedRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func hostedTestSubscription() hostedSubscription {
	return hostedSubscription{ID: "id", Generation: "generation", Owner: "owner", PrincipalID: "principal", Name: "name", URL: "https://example.com/hook", Secret: "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)), ExpiresAt: time.Now().Add(time.Hour)}
}

func TestHostedSigningAndChallenge(t *testing.T) {
	d, err := newHostedDelivery([]string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	sub := hostedTestSubscription()
	key, err := hostedKey(sub.Secret)
	if err != nil {
		t.Fatal(err)
	}
	var lastChallenge string
	d.client.Transport = hostedRoundTrip(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-MCP-Subscription-Id") != sub.ID || r.Header.Get("webhook-id") == "" || r.Header.Get("webhook-timestamp") == "" {
			t.Fatal("missing mandatory callback headers")
		}
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(r.Header.Get("webhook-id") + "." + r.Header.Get("webhook-timestamp") + "."))
		mac.Write(body)
		if r.Header.Get("webhook-signature") != "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
			t.Fatal("signature differs from exact bytes")
		}
		var challenge struct {
			Type      string `json:"type"`
			Challenge string `json:"challenge"`
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&challenge); err != nil || challenge.Type != "verification" {
			t.Fatalf("invalid verification body: %s", body)
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			t.Fatal("extra verification data")
		}
		if challenge.Challenge == lastChallenge || challenge.Challenge == "" {
			t.Fatal("challenge reused")
		}
		lastChallenge = challenge.Challenge
		response, err := json.Marshal(struct {
			Challenge string `json:"challenge"`
		}{challenge.Challenge})
		if err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(response))}, nil
	})
	if err := d.verify(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	if err := d.verify(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	d.client.Transport = hostedRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"challenge":"wrong"}`))}, nil
	})
	if err := d.verify(context.Background(), sub); err == nil {
		t.Fatal("accepted wrong challenge")
	}
}

func TestHostedDestinationSafety(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "100.64.0.1", "192.0.2.1", "::ffff:8.8.8.8", "::1", "2001:db8::1", "fc00::1"} {
		if hostedPublicIP(netip.MustParseAddr(ip)) {
			t.Fatalf("accepted %s", ip)
		}
	}
	if !hostedPublicIP(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("rejected public address")
	}
	for _, raw := range []string{"http://example.com", "https://example.com/?key=secret", "https://user@example.com", "https://127.0.0.1", "https://example.com:8443", "https://example.com/#fragment"} {
		if validHostedURL(raw) != nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := newHostedDelivery([]string{"*.example.com"}); err == nil {
		t.Fatal("accepted wildcard")
	}
}

func TestHostedStoreRestoreAndUnsafePaths(t *testing.T) {
	dir := filepath.Join(privateTempDir(t), "hosted")
	store, err := openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub := hostedTestSubscription()
	if err := store.put(sub); err != nil {
		t.Fatal(err)
	}
	restored, err := openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := restored.get(sub.ID)
	if err != nil || !ok || got.Secret != sub.Secret {
		t.Fatalf("restore failed: %v", err)
	}
	if err := restored.remove(sub.ID); err != nil {
		t.Fatal(err)
	}
	restored, err = openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := restored.get(sub.ID); err != nil || ok {
		t.Fatal("removal not persisted")
	}
	path := filepath.Join(dir, "subscriptions.json")
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := openHostedStore(dir); err == nil {
		t.Fatal("accepted unsafe file")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "missing"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := openHostedStore(dir); err == nil {
		t.Fatal("accepted symlink")
	}
}

func TestHostedRotationSignaturesAndPersistence(t *testing.T) {
	d, err := newHostedDelivery([]string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	sub := hostedTestSubscription()
	previousKey := bytes.Repeat([]byte{9}, 32)
	currentKey := bytes.Repeat([]byte{7}, 32)
	sub.PreviousSecret = "whsec_" + base64.StdEncoding.EncodeToString(previousKey)
	sub.PreviousSecretUntil = time.Now().Add(5 * time.Minute)
	dir := filepath.Join(privateTempDir(t), "hosted")
	store, err := openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.put(sub); err != nil {
		t.Fatal(err)
	}
	store, err = openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub, ok, err := store.get(sub.ID)
	if err != nil || !ok || sub.PreviousSecret != "whsec_"+base64.StdEncoding.EncodeToString(previousKey) || sub.PreviousSecretUntil.IsZero() {
		t.Fatal("rotation state not persisted")
	}
	wantPrevious := true
	d.client.Transport = hostedRoundTrip(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if r.Header.Get("X-MCP-Subscription-Id") != sub.ID || r.Header.Get("webhook-id") != "event-id" || string(body) != `{"event":"value"}` {
			t.Fatal("incorrect delivery")
		}
		signatures := strings.Split(r.Header.Get("webhook-signature"), " ")
		keys := [][]byte{currentKey}
		if wantPrevious {
			keys = append(keys, previousKey)
		}
		if len(signatures) != len(keys) {
			t.Fatalf("incorrect signature count: %v", signatures)
		}
		for i, key := range keys {
			mac := hmac.New(sha256.New, key)
			mac.Write([]byte("event-id." + r.Header.Get("webhook-timestamp") + "."))
			mac.Write(body)
			if signatures[i] != "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
				t.Fatal("invalid rotation signature")
			}
		}
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	if _, _, err := d.send(context.Background(), sub, "event-id", []byte(`{"event":"value"}`)); err != nil {
		t.Fatal(err)
	}
	wantPrevious = false
	sub.PreviousSecretUntil = time.Now().Add(-time.Second)
	if _, _, err := d.send(context.Background(), sub, "event-id", []byte(`{"event":"value"}`)); err != nil {
		t.Fatal(err)
	}
	sub.PreviousSecret = "invalid"
	if _, _, err := d.send(context.Background(), sub, "event-id", []byte(`{"event":"value"}`)); err == nil {
		t.Fatal("accepted invalid previous key")
	}
	if err := store.put(sub); err == nil {
		t.Fatal("stored invalid previous key")
	}
	sub.PreviousSecret = ""
	if err := store.put(sub); err == nil {
		t.Fatal("stored unpaired previous expiry")
	}
}

func TestHostedStoreRejectsDuplicateAndIdentityMismatch(t *testing.T) {
	dir := filepath.Join(privateTempDir(t), "hosted")
	store, err := openHostedStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub := hostedTestSubscription()
	if err := store.put(sub); err != nil {
		t.Fatal(err)
	}
	changed := sub
	changed.PrincipalID = "other"
	if err := store.put(changed); err == nil {
		t.Fatal("accepted identity mismatch")
	}
	changed = sub
	changed.Generation = "another-generation"
	if err := store.put(changed); err == nil {
		t.Fatal("accepted active generation replacement")
	}
	if err := store.remove(sub.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.put(changed); err != nil {
		t.Fatalf("rejected new generation after removal: %v", err)
	}
	missing := sub
	missing.Generation = ""
	if err := store.put(missing); err == nil {
		t.Fatal("accepted missing generation")
	}
	expired := sub
	expired.ExpiresAt = time.Now().Add(-time.Hour)
	data, err := json.Marshal([]hostedSubscription{expired, sub})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "subscriptions.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openHostedStore(dir); err == nil {
		t.Fatal("accepted duplicate expired identity")
	}
}
