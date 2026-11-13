package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestDeploymentDiscoveryAndExplicitTarget(t *testing.T) {
	const token = "valid-test-token-123456789"
	sha := strings.Repeat("a", 40)
	ref := map[string]any{"machineId": "machine-private", "harness": "omp", "nativeSessionId": "session-private", "nativeAgentId": "agent-private"}
	var mu sync.Mutex
	var calls []struct {
		name string
		args map[string]any
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "acceptance-test", Version: "1"}, nil)
	for _, name := range []string{"session.list", "session.inspect"} {
		name := name
		server.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object", "additionalProperties": true}}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args map[string]any
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return nil, err
			}
			mu.Lock()
			calls = append(calls, struct {
				name string
				args map[string]any
			}{name, args})
			mu.Unlock()
			var result any = map[string]any{"sessions": []any{map[string]any{"ref": ref}}, "nextCursor": nil}
			if name == "session.inspect" {
				result = map[string]any{"presence": map[string]any{"ref": ref}}
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}, StructuredContent: map[string]any{"result": result}}, nil
		})
	}
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/readyz" {
			w.Header().Set("X-Coordinator-Source-Sha", sha)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/mcp" {
			mcpHandler.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	tlsServer := httptest.NewTLSServer(handler)
	defer tlsServer.Close()
	roots := x509.NewCertPool()
	roots.AddCert(tlsServer.Certificate())
	oldTransport := http.DefaultTransport
	base := oldTransport.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	http.DefaultTransport = base
	t.Cleanup(func() { http.DefaultTransport = oldTransport; base.CloseIdleConnections() })
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	b, _ := json.Marshal(map[string]any{"inspectorUrl": tlsServer.URL, "apiToken": token})
	if err := os.WriteFile(config, b, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target.json")
	b, _ = json.Marshal(ref)
	if err := os.WriteFile(target, b, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COORDINATOR_API_URL", tlsServer.URL)
	t.Setenv("COORDINATOR_API_TOKEN", token)
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"deployment-smoke", "--config", config, "--source-sha", sha}
	if err := main1(); err != nil {
		t.Fatalf("default discovery: %v", err)
	}
	mu.Lock()
	count := len(calls)
	mu.Unlock()
	if count != 0 {
		t.Fatal("default discovery called a session tool")
	}
	os.Args = append(os.Args, "--safe-target", target)
	if err := main1(); err != nil {
		t.Fatalf("safe target: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || calls[0].name != "session.list" || calls[1].name != "session.inspect" {
		t.Fatalf("unexpected tool sequence")
	}
	if !reflect.DeepEqual(calls[0].args, map[string]any{"machineId": ref["machineId"], "limit": float64(100)}) || !reflect.DeepEqual(calls[1].args, map[string]any{"ref": ref}) {
		t.Fatal("unsafe or incorrect tool arguments")
	}
}
