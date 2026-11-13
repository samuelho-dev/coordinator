package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func fixture(t *testing.T) (*Registry, wire.InspectorPrincipal, wire.InspectorPrincipal, wire.ConnectorAnnounce) {
	t.Helper()
	hash := func(s string) string { v := sha256.Sum256([]byte(s)); return hex.EncodeToString(v[:]) }
	a := wire.InspectorPrincipal{ID: "a", Owner: "owner", TokenHash: hash("secret")}
	b := wire.InspectorPrincipal{ID: "b", Owner: "other", TokenHash: hash("other")}
	r, e := NewRegistry(wire.InspectorConfig{Principals: []wire.InspectorPrincipal{a, b}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(r.Close)
	in := wire.ConnectorAnnounce{ConnectorID: "connector", Incarnation: "first", MachineID: "machine", Sessions: []wire.SessionPresence{{Ref: wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "session"}, ObservedAt: 1}}}
	return r, a, b, in
}
func TestRegistryFencingAndOwnerIsolation(t *testing.T) {
	r, a, b, in := fixture(t)
	if _, e := r.Authorize("wrong"); e == nil {
		t.Fatal("wrong token accepted")
	}
	if _, e := r.Announce(a, in); e != nil {
		t.Fatal(e)
	}
	if page, e := r.List(b, wire.SessionListInput{Limit: 1}); e != nil || len(page.Sessions) != 0 {
		t.Fatalf("owner isolation: %+v %v", page, e)
	}
	bad := in
	bad.Incarnation = "second"
	if _, e := r.Announce(a, bad); e == nil {
		t.Fatal("live incarnation replaced")
	}
	bad = in
	bad.ConnectorID = "other"
	if _, e := r.Announce(a, bad); e == nil {
		t.Fatal("duplicate publisher accepted")
	}
	if _, e := r.Disconnect(a, wire.ConnectorDisconnect{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation}); e != nil {
		t.Fatal(e)
	}
	page, e := r.List(a, wire.SessionListInput{Limit: 1})
	if e != nil || page.Sessions[0].Connection.State != "stale" {
		t.Fatalf("stale: %+v %v", page, e)
	}
	bad = in
	bad.Incarnation = "second"
	if _, e = r.Announce(a, bad); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Announce(a, in); e == nil {
		t.Fatal("retired incarnation accepted")
	}
}
func TestInspectionCancellationAndReference(t *testing.T) {
	r, a, _, in := fixture(t)
	if _, e := r.Announce(a, in); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := r.Inspect(ctx, a, in.Sessions[0].Ref); done <- e }()
	var request wire.InspectionRequest
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		p, e := r.Poll(a, wire.ConnectorPoll{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Requests) > 0 {
			request = p.Requests[0]
			break
		}
		time.Sleep(time.Millisecond)
	}
	if request.RequestID == "" {
		t.Fatal("no request")
	}
	observation := wire.CreateInspection(in.Sessions[0], 1)
	observation.Presence.Ref.NativeSessionID = "wrong"
	raw, _ := json.Marshal(observation)
	if _, e := r.Complete(a, wire.ConnectorResult{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation, RequestID: request.RequestID, Observation: raw}); e == nil {
		t.Fatal("wrong ref accepted")
	}
	cancel()
	if e := <-done; e == nil {
		t.Fatal("cancellation ignored")
	}
	if _, e := r.Complete(a, wire.ConnectorResult{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation, RequestID: request.RequestID, Observation: raw}); e == nil {
		t.Fatal("late result accepted")
	}
}
func TestHTTPRejectsPrivateFields(t *testing.T) {
	r, _, _, _ := fixture(t)
	h := Handler(r)
	for _, tc := range []struct {
		path, token, origin, body string
		status                    int
	}{{"/livez", "", "", "", 200}, {"/readyz", "", "", "", 401}, {"/readyz", "secret", "https://attacker", "", 403}, {"/connector/announce", "secret", "", `{"connectorId":"c","incarnation":"i","machineId":"m","sessions":[],"transcript":"private"}`, 400}, {"/connector/announce", "secret", "", strings.Repeat("x", httpLimit+1), 413}} {
		method := "GET"
		if tc.body != "" {
			method = "POST"
		}
		req := httptest.NewRequest(method, tc.path, strings.NewReader(tc.body))
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "private") {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	_ = http.MethodGet
}
func TestMCPRealHTTP(t *testing.T) {
	r, a, _, in := fixture(t)
	if _, e := r.Announce(a, in); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(Handler(r))
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	hc := &http.Client{Transport: &boundedTransport{base: http.DefaultTransport, token: "secret"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, e := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: hc, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	result, e := session.CallTool(ctx, &mcp.CallToolParams{Name: "session.list", Arguments: map[string]any{"limit": 1}})
	if e != nil || result.IsError {
		t.Fatalf("list: %+v %v", result, e)
	}
	b, _ := json.Marshal(result.StructuredContent)
	var envelope struct {
		Result wire.SessionListOutput `json:"result"`
	}
	if e = json.Unmarshal(b, &envelope); e != nil || len(envelope.Result.Sessions) != 1 {
		t.Fatalf("result: %s %v", b, e)
	}
	result, e = session.CallTool(ctx, &mcp.CallToolParams{Name: "session.list", Arguments: map[string]any{"limit": 1, "transcript": "private"}})
	if e != nil || !result.IsError {
		t.Fatalf("private input accepted: %+v %v", result, e)
	}
	b, _ = json.Marshal(result)
	if strings.Contains(string(b), "private") {
		t.Fatal("input leaked")
	}
}
func TestMCPClientPreservesHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "private rejection", http.StatusUnauthorized)
	}))
	defer server.Close()
	client, e := NewClient(wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "valid-token-123456"})
	if e != nil {
		t.Fatal(e)
	}
	_, e = client.List(context.Background(), wire.SessionListInput{Limit: 1})
	h, ok := e.(*HTTPError)
	if !ok || h.Status != 401 || strings.Contains(e.Error(), "private") {
		t.Fatalf("unexpected error: %v", e)
	}
}
