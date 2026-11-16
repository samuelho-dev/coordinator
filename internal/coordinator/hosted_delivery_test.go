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
	return hostedSubscription{ID: "id", Owner: "owner", PrincipalID: "principal", Name: "name", URL: "https://example.com/hook", Secret: "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)), ExpiresAt: time.Now().Add(time.Hour)}
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
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(r.Header.Get("webhook-id") + "." + r.Header.Get("webhook-timestamp") + "."))
		mac.Write(body)
		if r.Header.Get("webhook-signature") != "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
			t.Fatal("signature differs from exact bytes")
		}
		var challenge struct {
			Challenge string `json:"challenge"`
		}
		if err := json.Unmarshal(body, &challenge); err != nil {
			t.Fatal(err)
		}
		if challenge.Challenge == lastChallenge || challenge.Challenge == "" {
			t.Fatal("challenge reused")
		}
		lastChallenge = challenge.Challenge
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
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
