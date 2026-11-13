package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestInitializePrivateNoOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := initialize([]string{"--directory", dir, "--owner", "alice"}); err != nil {
		t.Fatal(err)
	}
	configBytes, err := os.ReadFile(filepath.Join(dir, "server.json"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(filepath.Join(dir, "owner.token"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg wire.InspectorConfig
	if err = json.Unmarshal(configBytes, &cfg); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(string(token))))
	if len(cfg.Principals) != 1 || cfg.Principals[0].TokenHash != hex.EncodeToString(sum[:]) {
		t.Fatal("credential hash mismatch")
	}
	if info, err := os.Stat(filepath.Join(dir, "owner.token")); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("credential is not private")
	}
	if err := initialize([]string{"--directory", dir}); err == nil {
		t.Fatal("overwrote existing files")
	}
	after, err := os.ReadFile(filepath.Join(dir, "owner.token"))
	if err != nil || string(after) != string(token) {
		t.Fatal("existing credential changed")
	}
}
func TestExecutableResolution(t *testing.T) {
	path, err := executablePath()
	if err != nil || !canonicalPath(path) {
		t.Fatalf("invalid executable path: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatal("executable does not exist")
	}
}
func TestStageConfigRejectsEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input")
	if err := os.Mkdir(input, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(input, "inspector.json")); err != nil {
		t.Fatal(err)
	}
	if err := stageConfig([]string{"--input", input, "--destination", filepath.Join(root, "dest")}); err == nil {
		t.Fatal("accepted escaping symlink")
	}
}

func TestMessagingSetup(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "enabled"}[enabled], func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "server")
			args := []string{"--directory", dir}
			if enabled {
				args = append(args, "--messaging")
			}
			if err := initialize(args); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "server.json"))
			if err != nil {
				t.Fatal(err)
			}
			var cfg wire.InspectorConfig
			if err := json.Unmarshal(raw, &cfg); err != nil {
				t.Fatal(err)
			}
			if len(cfg.Principals) != 1 || cfg.Principals[0].Messaging != enabled {
				t.Fatalf("principal: %+v", cfg.Principals)
			}
			if err := initialize([]string{"--directory", dir, "--messaging"}); err == nil {
				t.Fatal("overwrote principal")
			}
			after, err := os.ReadFile(filepath.Join(dir, "server.json"))
			if err != nil || string(after) != string(raw) {
				t.Fatal("principal changed on overwrite")
			}
		})
	}
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	value, present := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(key, value)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func TestConnectMessagingOptIn(t *testing.T) {
	sum := sha256.Sum256([]byte("test-token-1234567890"))
	reg, err := coordinator.NewRegistry(wire.InspectorConfig{Principals: []wire.InspectorPrincipal{{ID: "owner", Owner: "owner", TokenHash: hex.EncodeToString(sum[:])}}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	server := httptest.NewServer(coordinator.Handler(reg))
	defer server.Close()
	t.Setenv("COORDINATOR_API_TOKEN", "test-token-1234567890")
	unsetEnv(t, "COORDINATOR_API_URL")
	for _, enabled := range []bool{false, true} {
		dir := filepath.Join(t.TempDir(), "client")
		config := filepath.Join(dir, "inspector.json")
		args := []string{"--endpoint", server.URL, "--config", config}
		if enabled {
			args = append(args, "--messaging")
		}
		if err := connect(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		var cfg wire.ConnectorConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Messaging != enabled || cfg.InspectorURL != server.URL || cfg.TokenFile != filepath.Join(dir, "inspector.token") || cfg.APIToken != "" {
			t.Fatalf("config: %+v", cfg)
		}
		if err := connect(context.Background(), args); err == nil {
			t.Fatal("overwrote config")
		}
		after, err := os.ReadFile(config)
		if err != nil || string(after) != string(raw) {
			t.Fatal("config changed on overwrite")
		}
	}
}

func TestMessageInputAndFailedCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "denied", http.StatusForbidden) }))
	defer server.Close()
	client, err := coordinator.NewClient(wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "test-token-1234567890"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ action, body string }{
		{"send", `{"to":{"machineId":"x","harness":"pi","nativeSessionId":"y"},"text":"ok","unexpected":true}`},
		{"send", `{"text":"ok"} trailing`},
		{"reply", `{"replyId":"id","text":"ok"}`},
		{"get", `{"messageId":"id","extra":1}`},
		{"get", `{"messageId":"id"}`},
		{"other", `{}`},
	} {
		if err := message(context.Background(), client, tc.action, strings.NewReader(tc.body)); err == nil {
			t.Fatalf("accepted %s %s", tc.action, tc.body)
		}
	}
	if err := run(context.Background(), []string{"message", "send", "message body"}); err == nil {
		t.Fatal("accepted body in argv")
	}
}

func TestInstallMessagingRequiresOptIn(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("installer requires nonroot owner")
	}
	root := t.TempDir()
	config := filepath.Join(root, "inspector.json")
	unsetEnv(t, "COORDINATOR_API_URL")
	unsetEnv(t, "COORDINATOR_API_TOKEN")
	for _, enabled := range []bool{false, true} {
		cfg := wire.ConnectorConfig{InspectorURL: "http://127.0.0.1:8787", APIToken: "test-token-1234567890", Messaging: enabled}
		if err := os.WriteFile(config, jsonBytes(cfg), 0600); err != nil {
			t.Fatal(err)
		}
		dest := filepath.Join(root, map[bool]string{false: "disabled", true: "enabled"}[enabled])
		err := install([]string{"--target", "claude", "--destination", dest, "--config", config, "--messaging"})
		if !enabled {
			if err == nil {
				t.Fatal("installed without opt-in")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dest, ".mcp.json"))
		if err != nil {
			t.Fatal(err)
		}
		var parsed struct {
			MCPServers map[string]struct {
				Args []string          `json:"args"`
				Env  map[string]string `json:"env"`
			} `json:"mcpServers"`
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatal(err)
		}
		server := parsed.MCPServers["coordinator"]
		if len(server.Args) != 1 || server.Args[0] != "claude-channel" || server.Env["COORDINATOR_CONNECTOR_CONFIG"] != config {
			t.Fatalf("bad channel config: %+v", server)
		}
	}
}
