package coordinator

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestHostedRouteOAuthReplyAndRestart(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		enc := base64.RawURLEncoding.EncodeToString
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "rsa", "alg": "RS256", "use": "sig", "n": enc(key.N.Bytes()), "e": enc([]byte{1, 0, 1})}}})
	}))
	defer idp.Close()
	var handlerMu sync.RWMutex
	var handler http.Handler
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handlerMu.RLock()
		h := handler
		handlerMu.RUnlock()
		h.ServeHTTP(w, req)
	}))
	resource := "http://" + server.Listener.Addr().String() + "/mcp"
	hash := sha256.Sum256([]byte("native-secret-123456"))
	native := wire.InspectorPrincipal{ID: "native", Owner: "owner", TokenHash: hex.EncodeToString(hash[:]), Messaging: true}
	alice := wire.InspectorPrincipal{ID: "oauth", Owner: "owner", Messaging: true}
	peer := wire.InspectorPrincipal{ID: "peer", Owner: "owner", Messaging: true}
	other := wire.InspectorPrincipal{ID: "other", Owner: "other", Messaging: true}
	cfg := wire.InspectorConfig{Principals: []wire.InspectorPrincipal{native, alice, peer, other}, OAuth: &wire.InspectorOAuthConfig{Resource: resource, Issuer: idp.URL, JWKSURL: idp.URL + "/jwks", Bindings: []wire.InspectorOAuthBinding{{Subject: "alice", PrincipalID: alice.ID}, {Subject: "peer", PrincipalID: peer.ID}, {Subject: "other", PrincipalID: other.ID}}}, Hosted: &wire.InspectorHostedConfig{StateDirectory: filepath.Join(privateTempDir(t), "hosted-route"), CallbackHosts: []string{"example.com"}}}
	reg, err := NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { reg.Close() }()
	handler = Handler(reg)
	server.Start()
	defer server.Close()
	e := &oauthTestEnv{server: server, issuer: idp.URL, resource: resource, rsa: key}
	token := func(subject string) string {
		return e.token(t, jwt.SigningMethodRS256, key, "rsa", "at+jwt", map[string]any{"sub": subject})
	}
	aliceToken := token("alice")
	aliceSession := oauthSession(t, e, aliceToken)
	peerSession := oauthSession(t, e, token("peer"))
	otherSession := oauthSession(t, e, token("other"))

	delivered := make(chan wire.HostedEventEnvelope, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	reg.hosted.delivery.client.Transport = hostedRoundTrip(func(req *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var kind struct {
			Type      string `json:"type"`
			Challenge string `json:"challenge"`
		}
		if err := json.Unmarshal(b, &kind); err != nil {
			return nil, err
		}
		if kind.Type == "verification" {
			body, _ := json.Marshal(map[string]string{"challenge": kind.Challenge})
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		}
		var event wire.HostedEventEnvelope
		if err := json.Unmarshal(b, &event); err != nil {
			return nil, err
		}
		select {
		case delivered <- event:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		select {
		case <-release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	eventCall := func(method string, params map[string]any) map[string]any {
		t.Helper()
		params["_meta"] = map[string]any{mcp.MetaKeyProtocolVersion: "2026-07-28", mcp.MetaKeyClientCapabilities: map[string]any{}, mcp.MetaKeyClientInfo: map[string]any{"name": "test", "version": "1"}}
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequest(http.MethodPost, resource, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+aliceToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", "2026-07-28")
		req.Header.Set("Mcp-Method", method)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var result map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	delivery := map[string]any{"mode": "webhook", "url": "https://example.com/callback"}
	secret := "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	created := eventCall("events/subscribe", map[string]any{"name": hostedEventName, "arguments": map[string]any{"name": "Chat"}, "delivery": map[string]any{"mode": "webhook", "url": "https://example.com/callback", "secret": secret}, "cursor": nil})
	if created["error"] != nil {
		t.Fatalf("subscribe: %v", created)
	}
	id := created["result"].(map[string]any)["id"].(string)
	ref := wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "native-session"}
	ann := wire.ConnectorAnnounce{ConnectorID: "connector", Incarnation: "first", MachineID: ref.MachineID, Sessions: []wire.SessionPresence{{Ref: ref, ObservedAt: 1, Messaging: true}}, Receivers: []wire.SessionRef{ref}}
	if _, err := reg.Announce(native, ann); err != nil {
		t.Fatal(err)
	}
	caller := &wire.MessageCaller{ConnectorID: ann.ConnectorID, Incarnation: ann.Incarnation, Ref: ref}
	to := wire.RecipientRef{Kind: "subscription", SubscriptionID: id}
	sent, err := reg.SendMessage(native, wire.MessageSendInput{To: to, Text: "hello hosted", Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	var event wire.HostedEventEnvelope
	select {
	case event = <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("hosted worker did not deliver")
	}
	if event.EventID != sent.ID || event.Name != hostedEventName || event.Data.Message.ID != sent.ID || event.Data.Message.Sender != ref.NativeSessionID || event.Data.Message.From == nil || event.Data.Message.From.Key() != wire.SessionRecipient(ref).Key() || event.Data.Message.To == nil || event.Data.Message.To.Key() != to.Key() || event.Data.Message.Text != "hello hosted" {
		t.Fatalf("wrong event: %+v", event)
	}
	replyArgs := map[string]any{"replyId": sent.ID, "text": "answer"}
	oauthStatus(t, oauthCall(t, aliceSession, "message.reply", replyArgs), 409)
	oauthStatus(t, oauthCall(t, peerSession, "message.reply", replyArgs), 403)
	oauthStatus(t, oauthCall(t, otherSession, "message.reply", replyArgs), 404)
	unblock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		record, err := reg.GetMessage(native, wire.MessageGetInput{MessageID: sent.ID})
		if err == nil && record.Message.Status == "accepted" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback receipt not accepted: %+v %v", record, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Reconstruct the registry from durable state, then reattach the exact native incarnation.
	reg.Close()
	restarted, err := NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg = restarted
	handlerMu.Lock()
	handler = Handler(reg)
	handlerMu.Unlock()
	if _, err := reg.Announce(native, ann); err != nil {
		t.Fatal(err)
	}
	result := oauthCall(t, aliceSession, "message.reply", replyArgs)
	if result.IsError {
		t.Fatalf("account reply: %+v", result)
	}
	b, _ := json.Marshal(result.StructuredContent)
	var replied struct {
		Result wire.Message `json:"result"`
	}
	if err := json.Unmarshal(b, &replied); err != nil {
		t.Fatal(err)
	}
	if replied.Result.ID == "" || replied.Result.InReplyTo != sent.ID || replied.Result.Sender != alice.ID || replied.Result.From != nil || replied.Result.To == nil || replied.Result.To.Key() != wire.SessionRecipient(ref).Key() {
		t.Fatalf("incorrect account reply identity: %+v", replied.Result)
	}
	batch, err := reg.Poll(native, wire.ConnectorPoll{ConnectorID: ann.ConnectorID, Incarnation: ann.Incarnation})
	if err != nil || len(batch.Messages) != 1 || batch.Messages[0].ID != replied.Result.ID || batch.Messages[0].InReplyTo != sent.ID {
		t.Fatalf("native reply poll: %+v %v", batch, err)
	}

	stopped := eventCall("events/unsubscribe", map[string]any{"name": hostedEventName, "arguments": map[string]any{"name": "Chat"}, "delivery": delivery})
	if stopped["error"] != nil {
		t.Fatalf("unsubscribe: %v", stopped)
	}
	// Recreate the deterministic ID before any routing operation can prune the old route.
	reg.hosted.delivery.client.Transport = hostedRoundTrip(func(req *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var verification struct {
			Challenge string `json:"challenge"`
		}
		if err := json.Unmarshal(b, &verification); err != nil {
			return nil, err
		}
		body, _ := json.Marshal(map[string]string{"challenge": verification.Challenge})
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	recreated := eventCall("events/subscribe", map[string]any{"name": hostedEventName, "arguments": map[string]any{"name": "Chat"}, "delivery": map[string]any{"mode": "webhook", "url": "https://example.com/callback", "secret": secret}, "cursor": nil})
	if recreated["error"] != nil || recreated["result"].(map[string]any)["id"] != id {
		t.Fatalf("resubscribe: %v", recreated)
	}
	oauthStatus(t, oauthCall(t, aliceSession, "message.reply", replyArgs), 403)
}
