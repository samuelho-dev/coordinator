package coordinator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

type oauthTestEnv struct {
	server   *httptest.Server
	issuer   string
	resource string
	rsa      *rsa.PrivateKey
	ec       *ecdsa.PrivateKey
	ref      wire.SessionRef
}

func newOAuthTestEnv(t *testing.T) *oauthTestEnv {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.RawURLEncoding.EncodeToString
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{
			map[string]any{"kty": "RSA", "kid": "rsa", "alg": "RS256", "use": "sig", "n": b64(rsaKey.N.Bytes()), "e": b64([]byte{1, 0, 1})},
			map[string]any{"kty": "EC", "kid": "ec", "alg": "ES256", "use": "sig", "crv": "P-256", "x": b64(ecKey.X.FillBytes(make([]byte, 32))), "y": b64(ecKey.Y.FillBytes(make([]byte, 32)))},
		}})
	}))
	t.Cleanup(idp.Close)
	var handler http.Handler
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	resource := "http://" + server.Listener.Addr().String() + "/mcp"
	hash := sha256.Sum256([]byte("native-secret-123456"))
	native := wire.InspectorPrincipal{ID: "native", Owner: "owner", TokenHash: hex.EncodeToString(hash[:]), Messaging: true}
	principals := []wire.InspectorPrincipal{native, {ID: "oauth", Owner: "owner", Messaging: true}, {ID: "readonly", Owner: "owner"}, {ID: "other", Owner: "other", Messaging: true}, {ID: "peer", Owner: "owner", Messaging: true}}
	cfg := wire.InspectorConfig{Principals: principals, OAuth: &wire.InspectorOAuthConfig{Resource: resource, Issuer: idp.URL, JWKSURL: idp.URL + "/jwks", Bindings: []wire.InspectorOAuthBinding{{Subject: "alice", PrincipalID: "oauth"}, {Subject: "readonly", PrincipalID: "readonly"}, {Subject: "other", PrincipalID: "other"}, {Subject: "peer", PrincipalID: "peer"}}}}
	reg, err := NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	ref := wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "target"}
	ann := wire.ConnectorAnnounce{ConnectorID: "connector", Incarnation: "first", MachineID: "machine", Sessions: []wire.SessionPresence{{Ref: ref, ObservedAt: 1, Messaging: true}}, Receivers: []wire.SessionRef{ref}}
	if _, err := reg.Announce(native, ann); err != nil {
		t.Fatal(err)
	}
	handler = Handler(reg)
	server.Start()
	t.Cleanup(server.Close)
	return &oauthTestEnv{server: server, issuer: idp.URL, resource: resource, rsa: rsaKey, ec: ecKey, ref: ref}
}

func (e *oauthTestEnv) token(t *testing.T, method jwt.SigningMethod, key any, kid, typ string, changes map[string]any) string {
	t.Helper()
	claims := jwt.MapClaims{"iss": e.issuer, "aud": e.resource, "sub": "alice", "exp": time.Now().Add(time.Hour).Unix(), "scope": "coordinator:read coordinator:message", "padding": strings.Repeat("x", 200)}
	for k, v := range changes {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}
	token := jwt.NewWithClaims(method, claims)
	token.Header["typ"] = typ
	token.Header["kid"] = kid
	s, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func oauthRequest(t *testing.T, e *oauthTestEnv, method, path, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, e.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func oauthSession(t *testing.T, e *oauthTestEnv, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "oauth-test", Version: "1"}, nil)
	hc := &http.Client{Transport: &boundedTransport{base: http.DefaultTransport, token: token}}
	s, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: e.resource, HTTPClient: hc, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func oauthCall(t *testing.T, s *mcp.ClientSession, name string, args any) *mcp.CallToolResult {
	t.Helper()
	out, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func oauthStatus(t *testing.T, result *mcp.CallToolResult, want int) {
	t.Helper()
	b, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Error struct {
			Status int `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &v); err != nil || !result.IsError || v.Error.Status != want {
		t.Fatalf("expected tool error %d: %s (%v)", want, b, err)
	}
}

func TestOAuthMetadataAndAuthentication(t *testing.T) {
	e := newOAuthTestEnv(t)
	for _, path := range []string{"/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"} {
		r := oauthRequest(t, e, "GET", path, "", "")
		var metadata struct {
			Resource             string   `json:"resource"`
			AuthorizationServers []string `json:"authorization_servers"`
			Scopes               []string `json:"scopes_supported"`
		}
		if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil || r.StatusCode != 200 || metadata.Resource != e.resource || len(metadata.AuthorizationServers) != 1 || metadata.AuthorizationServers[0] != e.issuer || !strings.Contains(strings.Join(metadata.Scopes, " "), "coordinator:read") || !strings.Contains(strings.Join(metadata.Scopes, " "), "coordinator:message") {
			t.Fatalf("metadata %s: %+v status=%d err=%v", path, metadata, r.StatusCode, err)
		}
	}
	request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`
	r := oauthRequest(t, e, "POST", "/mcp", "", request)
	if r.StatusCode != 401 || !strings.Contains(r.Header.Get("WWW-Authenticate"), "resource_metadata=") {
		t.Fatalf("challenge: %d %q", r.StatusCode, r.Header.Get("WWW-Authenticate"))
	}
	valid := e.token(t, jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", nil)
	if len(valid) <= 512 {
		t.Fatal("test token must exceed native bearer limit")
	}
	s := oauthSession(t, e, valid)
	result := oauthCall(t, s, "session.list", map[string]any{"limit": 10})
	b, _ := json.Marshal(result.StructuredContent)
	var page struct {
		Result wire.SessionListOutput `json:"result"`
	}
	if err := json.Unmarshal(b, &page); err != nil || result.IsError || len(page.Result.Sessions) != 1 || page.Result.Sessions[0].Ref.Key() != e.ref.Key() {
		t.Fatalf("list: %s %v", b, err)
	}
	ec := e.token(t, jwt.SigningMethodES256, e.ec, "ec", "at+jwt", nil)
	if out := oauthCall(t, oauthSession(t, e, ec), "session.list", map[string]any{"limit": 10}); out.IsError {
		t.Fatalf("ES256 rejected: %+v", out)
	}
	other := e.token(t, jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"sub": "other"})
	otherList := oauthCall(t, oauthSession(t, e, other), "session.list", map[string]any{"limit": 10})
	b, _ = json.Marshal(otherList.StructuredContent)
	page = struct {
		Result wire.SessionListOutput `json:"result"`
	}{}
	if err := json.Unmarshal(b, &page); err != nil || otherList.IsError || len(page.Result.Sessions) != 0 {
		t.Fatalf("cross-owner list: %s %v", b, err)
	}
	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		method   jwt.SigningMethod
		key      any
		kid, typ string
		changes  map[string]any
		status   int
	}{
		{"issuer", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"iss": "https://wrong.example"}, 401},
		{"audience", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"aud": "https://wrong.example/mcp"}, 401},
		{"signature", jwt.SigningMethodRS256, wrongKey, "rsa", "at+jwt", nil, 401},
		{"missing exp", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"exp": nil}, 401},
		{"expired", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"exp": time.Now().Add(-time.Hour).Unix()}, 401},
		{"future nbf", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"nbf": time.Now().Add(time.Hour).Unix()}, 401},
		{"id token", jwt.SigningMethodRS256, e.rsa, "rsa", "JWT", nil, 401},
		{"subject", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"sub": "unknown"}, 401},
		{"unknown kid", jwt.SigningMethodRS256, e.rsa, "unknown", "at+jwt", nil, 401},
		{"non-string scope", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"scope": []string{"coordinator:read"}}, 401},
		{"HS256", jwt.SigningMethodHS256, []byte("secret"), "rsa", "at+jwt", nil, 401},
		{"none", jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, "rsa", "at+jwt", nil, 401},
		{"missing read", jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"scope": "coordinator:message"}, 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := e.token(t, tc.method, tc.key, tc.kid, tc.typ, tc.changes)
			r := oauthRequest(t, e, "POST", "/mcp", token, request)
			if r.StatusCode != tc.status {
				body, _ := io.ReadAll(r.Body)
				t.Fatalf("status %d want %d: %s", r.StatusCode, tc.status, body)
			}
		})
	}
}

func TestOAuthMessagingAndNativeIsolation(t *testing.T) {
	e := newOAuthTestEnv(t)
	token := func(sub, scope string) string {
		return e.token(t, jwt.SigningMethodRS256, e.rsa, "rsa", "at+jwt", map[string]any{"sub": sub, "scope": scope})
	}
	read := "coordinator:read"
	both := read + " coordinator:message"
	alice := oauthSession(t, e, token("alice", both))
	args := map[string]any{"to": e.ref, "text": "hello"}
	for _, tc := range []struct{ sub, scope string }{{"alice", read}, {"readonly", both}} {
		oauthStatus(t, oauthCall(t, oauthSession(t, e, token(tc.sub, tc.scope)), "message.send", args), 403)
	}
	sent := oauthCall(t, alice, "message.send", args)
	b, _ := json.Marshal(sent.StructuredContent)
	var message struct {
		Result wire.Message `json:"result"`
	}
	if err := json.Unmarshal(b, &message); err != nil || sent.IsError || message.Result.ID == "" {
		t.Fatalf("send: %s %v", b, err)
	}
	get := map[string]any{"messageId": message.Result.ID}
	got := oauthCall(t, oauthSession(t, e, token("alice", read)), "message.get", get)
	b, _ = json.Marshal(got.StructuredContent)
	var record struct {
		Result wire.MessageRecord `json:"result"`
	}
	if err := json.Unmarshal(b, &record); err != nil || got.IsError || record.Result.Message.ID != message.Result.ID {
		t.Fatalf("get: %s %v", b, err)
	}
	for _, sub := range []string{"peer", "other"} {
		out := oauthCall(t, oauthSession(t, e, token(sub, both)), "message.get", get)
		if !out.IsError {
			t.Fatalf("%s read another participant's message", sub)
		}
	}
	caller := map[string]any{"connectorId": "connector", "incarnation": "first", "ref": e.ref}
	withCaller := map[string]any{"to": e.ref, "text": "bad", "caller": caller}
	if out := oauthCall(t, alice, "message.send", withCaller); !out.IsError {
		t.Fatal("OAuth caller accepted")
	}
	if out := oauthCall(t, alice, "message.reply", map[string]any{"replyId": message.Result.ID, "text": "bad", "caller": caller}); !out.IsError {
		t.Fatal("OAuth reply accepted")
	}
	for _, path := range []string{"/connector/announce", "/connector/poll", "/healthz", "/readyz"} {
		method := "POST"
		if strings.HasSuffix(path, "z") {
			method = "GET"
		}
		r := oauthRequest(t, e, method, path, token("alice", both), "{}")
		if r.StatusCode < 400 {
			t.Fatalf("OAuth accessed %s: %d", path, r.StatusCode)
		}
	}
	native := oauthSession(t, e, "native-secret-123456")
	if out := oauthCall(t, native, "session.list", map[string]any{"limit": 10}); out.IsError {
		t.Fatalf("native list: %+v", out)
	}
	ann := wire.ConnectorAnnounce{ConnectorID: "another", Incarnation: "first", MachineID: "another", Sessions: []wire.SessionPresence{}}
	body, err := json.Marshal(ann)
	if err != nil {
		t.Fatal(err)
	}
	r := oauthRequest(t, e, "POST", "/connector/announce", "native-secret-123456", string(body))
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("native announce: %d %s", r.StatusCode, b)
	}
}

func TestOAuthRegistryConfig(t *testing.T) {
	h := sha256.Sum256([]byte("native-secret-123456"))
	valid := func() wire.InspectorConfig {
		return wire.InspectorConfig{Principals: []wire.InspectorPrincipal{{ID: "oauth", Owner: "owner"}, {ID: "native", Owner: "owner", TokenHash: hex.EncodeToString(h[:])}}, OAuth: &wire.InspectorOAuthConfig{Resource: "https://service.example/mcp", Issuer: "https://idp.example", JWKSURL: "https://idp.example/jwks", Bindings: []wire.InspectorOAuthBinding{{Subject: "alice", PrincipalID: "oauth"}}}}
	}
	cases := []struct {
		name   string
		mutate func(*wire.InspectorConfig)
	}{
		{"duplicate subject", func(c *wire.InspectorConfig) {
			c.OAuth.Bindings = append(c.OAuth.Bindings, wire.InspectorOAuthBinding{Subject: "alice", PrincipalID: "oauth"})
		}},
		{"token bearing binding", func(c *wire.InspectorConfig) { c.OAuth.Bindings[0].PrincipalID = "native" }},
		{"unbound principal", func(c *wire.InspectorConfig) {
			c.Principals = append(c.Principals, wire.InspectorPrincipal{ID: "unbound", Owner: "owner"})
		}},
		{"invalid URL", func(c *wire.InspectorConfig) { c.OAuth.Issuer = "http://evil.example" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.mutate(&cfg)
			reg, err := NewRegistry(cfg)
			if err == nil {
				reg.Close()
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
