package coordinator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestHostedEventsLifecycle(t *testing.T) {
	alice := wire.InspectorPrincipal{ID: "alice", Owner: "owner", Messaging: true}
	bob := wire.InspectorPrincipal{ID: "bob", Owner: "other", Messaging: true}
	r := &Registry{principals: map[string]wire.InspectorPrincipal{"alice": alice, "bob": bob}, oauth: &oauthAccess{bindings: map[string]wire.InspectorPrincipal{"alice": alice, "bob": bob}}}
	h, err := newHostedAccess(r, wire.InspectorHostedConfig{StateDirectory: filepath.Join(privateTempDir(t), "events"), CallbackHosts: []string{"example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	var events []wire.HostedEventEnvelope
	var signatures []string
	h.delivery.client.Transport = hostedRoundTrip(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Type      string `json:"type"`
			Challenge string `json:"challenge"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if req.Header.Get("X-MCP-Subscription-Id") == "" {
			t.Fatal("missing subscription header")
		}
		if envelope.Type == "verification" {
			b, _ := json.Marshal(map[string]string{"challenge": envelope.Challenge})
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(b))}, nil
		}
		var event wire.HostedEventEnvelope
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
		signatures = append(signatures, req.Header.Get("webhook-signature"))
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	s := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, &mcp.ServerOptions{HasTools: true})
	h.register(s)
	transport := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	server := httptest.NewServer(auth.RequireBearerToken(func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		p := alice
		if token == "bob" {
			p = bob
		}
		if token != "alice" && token != "bob" && token != "read" && token != "native" {
			return nil, auth.ErrInvalidToken
		}
		scopes := []string{readScope, messageScope}
		if token == "read" {
			scopes = []string{readScope}
		}
		return &auth.TokenInfo{UserID: p.ID, Scopes: scopes, Expiration: time.Now().Add(time.Hour), Extra: map[string]any{"principal": p, "oauth": token != "native"}}, nil
	}, nil)(transport))
	defer server.Close()
	meta := map[string]any{mcp.MetaKeyProtocolVersion: "2026-07-28", mcp.MetaKeyClientCapabilities: map[string]any{}, mcp.MetaKeyClientInfo: map[string]any{"name": "test", "version": "1"}}
	call := func(token, method string, params map[string]any) map[string]any {
		t.Helper()
		params["_meta"] = meta
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", "2026-07-28")
		req.Header.Set("Mcp-Method", method)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, method := range []string{"server/discover", "events/list"} {
		result := call("alice", method, map[string]any{})
		if result["error"] != nil || result["result"] == nil {
			t.Fatalf("%s: %v", method, result)
		}
		if method == "server/discover" {
			caps := result["result"].(map[string]any)["capabilities"].(map[string]any)
			if caps["events"] == nil || caps["tools"] == nil {
				t.Fatalf("missing capabilities: %v", caps)
			}
		}
	}
	for _, token := range []string{"read", "native"} {
		if got := call(token, "events/list", map[string]any{}); got["error"] == nil {
			t.Fatalf("unauthorized events/list: %v", got)
		}
	}
	secret := func(b byte) string { return "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)) }
	params := func(key string) map[string]any {
		return map[string]any{"name": hostedEventName, "arguments": map[string]any{"name": "Chat"}, "delivery": map[string]any{"mode": "webhook", "url": "https://example.com/callback", "secret": key}, "cursor": nil}
	}
	invalid := params(secret(1))
	invalid["arguments"] = map[string]any{"name": "Chat", "extra": true}
	if got := call("alice", "events/subscribe", invalid); got["error"] == nil {
		t.Fatalf("accepted unknown argument: %v", got)
	}
	verifiedTransport := h.delivery.client.Transport
	h.delivery.client.Transport = hostedRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"challenge":"wrong"}`))}, nil
	})
	failed := call("alice", "events/subscribe", params(secret(1)))
	if failed["error"] == nil {
		t.Fatalf("accepted invalid challenge: %v", failed)
	}
	records, err := h.subscriptions.list()
	if err != nil || len(records) != 0 {
		t.Fatal("failed challenge activated a subscription")
	}
	h.delivery.client.Transport = verifiedTransport
	first := call("alice", "events/subscribe", params(secret(1)))
	if first["error"] != nil {
		t.Fatalf("subscribe: %v", first)
	}
	id := first["result"].(map[string]any)["id"].(string)
	initial, ok, err := h.subscriptions.get(id)
	if err != nil || !ok || initial.Generation == "" {
		t.Fatal("missing initial generation")
	}
	second := call("alice", "events/subscribe", params(secret(2)))
	if second["error"] != nil || second["result"].(map[string]any)["id"] != id {
		t.Fatalf("refresh: %v", second)
	}
	restored, err := openHostedStore(h.subscriptions.directory)
	if err != nil {
		t.Fatal(err)
	}
	stored, ok, err := restored.get(id)
	if err != nil || !ok || stored.Generation != initial.Generation || stored.Secret != secret(2) || stored.PreviousSecret != secret(1) {
		t.Fatalf("rotation not persisted: %v", err)
	}
	if _, err := h.subscription(bob, id); err == nil {
		t.Fatal("cross-owner recipient visible")
	}
	msg := wire.Message{ID: "message-1", Sender: "sender", Text: "hello", CreatedAt: float64(time.Now().UnixMilli()), ExpiresAt: float64(time.Now().Add(time.Minute).UnixMilli()), Status: "dispatched"}
	if err := h.deliver(context.Background(), id, initial.Generation, msg); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventID != msg.ID || events[0].Data.Message.Text != msg.Text || events[0].Cursor != nil || len(strings.Fields(signatures[0])) != 2 {
		t.Fatal("incorrect event or rotation")
	}
	attempts := 0
	h.delivery.client.Transport = hostedRoundTrip(func(req *http.Request) (*http.Response, error) {
		attempts++
		if req.Header.Get("webhook-id") != msg.ID {
			t.Fatal("retry changed event ID")
		}
		status := http.StatusServiceUnavailable
		if attempts == 3 {
			status = http.StatusNoContent
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	if err := h.deliver(context.Background(), id, initial.Generation, msg); err != nil || attempts != 3 {
		t.Fatalf("retry count %d: %v", attempts, err)
	}
	attempts = 0
	h.delivery.client.Transport = hostedRoundTrip(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: http.StatusGone, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	if err := h.deliver(context.Background(), id, initial.Generation, msg); err == nil || attempts != 1 {
		t.Fatalf("permanent failure retry count %d: %v", attempts, err)
	}
	unsubscribe := func() map[string]any {
		return map[string]any{"name": hostedEventName, "arguments": map[string]any{"name": "Chat"}, "delivery": map[string]any{"mode": "webhook", "url": "https://example.com/callback"}}
	}
	if got := call("bob", "events/unsubscribe", unsubscribe()); got["error"] != nil {
		t.Fatalf("idempotent unsubscribe: %v", got)
	}
	if _, err := h.subscription(alice, id); err != nil {
		t.Fatal("other principal revoked subscription")
	}
	for range 2 {
		if got := call("alice", "events/unsubscribe", unsubscribe()); got["error"] != nil {
			t.Fatalf("unsubscribe: %v", got)
		}
	}
	if err := h.deliver(context.Background(), id, initial.Generation, msg); err == nil {
		t.Fatal("delivered after unsubscribe")
	}
	h.delivery.client.Transport = verifiedTransport
	if got := call("alice", "events/subscribe", params(secret(1))); got["error"] != nil || got["result"].(map[string]any)["id"] != id {
		t.Fatalf("resubscribe: %v", got)
	}
	recreated, ok, err := h.subscriptions.get(id)
	if err != nil || !ok || recreated.Generation == initial.Generation {
		t.Fatal("resubscribe did not create a new generation")
	}
	before := len(events)
	if err := h.deliver(context.Background(), id, initial.Generation, msg); err == nil || len(events) != before {
		t.Fatal("old generation delivered after resubscribe")
	}
	if err := h.deliver(context.Background(), id, recreated.Generation, msg); err != nil {
		t.Fatal(err)
	}
	h.subscriptions.mu.Lock()
	expired := recreated
	expired.ExpiresAt = time.Now().Add(-time.Second)
	h.subscriptions.subscriptions[id] = expired
	h.subscriptions.mu.Unlock()
	if got := call("alice", "events/subscribe", params(secret(1))); got["error"] != nil || got["result"].(map[string]any)["id"] != id {
		t.Fatalf("subscribe after expiry: %v", got)
	}
	afterExpiry, ok, err := h.subscriptions.get(id)
	if err != nil || !ok || afterExpiry.Generation == recreated.Generation {
		t.Fatal("expiry did not create a new generation")
	}
	before = len(events)
	if err := h.deliver(context.Background(), id, recreated.Generation, msg); err == nil || len(events) != before {
		t.Fatal("old generation delivered after expiry")
	}
	if err := h.deliver(context.Background(), id, afterExpiry.Generation, msg); err != nil {
		t.Fatal(err)
	}
}
