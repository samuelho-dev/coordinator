package coordinator

import (
	"context"
	"strings"
	"testing"
)

func TestCaptureProjections(t *testing.T) {
	var h hookInput
	if err := readExternal(strings.NewReader(`{"session_id":"s","hook_event_name":"PermissionRequest","tool_name":"Bash","prompt":"secret"}`), &h); err != nil {
		t.Fatal(err)
	}
	r, err := normalizeHook("m", "claude", h, 100)
	if err != nil || r.Presence.ProgramStatus == nil || r.Presence.ProgramStatus.Kind != "permission" || r.Presence.Activity.Wait != "approval" {
		t.Fatalf("hook: %+v %v", r.Presence, err)
	}
	h.Name = "on_session_end"
	h.Extra = &hermesExtra{}
	r, err = normalizeHook("m", "hermes", h, 100)
	if err != nil || r.Presence.Activity.State != "idle" {
		t.Fatal(r, err)
	}
	var c claudeStatus
	if err = readExternal(strings.NewReader(`{"session_id":"s","model":{"id":"model"},"workspace":{"current_dir":"/work","project_dir":"/work"},"context_window":{"current_usage":{"input_tokens":-1}},"cost":{"total_cost_usd":0.25}}`), &c); err != nil {
		t.Fatal(err)
	}
	r, err = normalizeClaudeStatus("m", c, 100)
	if err != nil || r.Presence.Model.ID != "model" || r.Presence.Workspace.Project.Root != "/work" {
		t.Fatal(r, err)
	}
	if err = readExternal(strings.NewReader(`{"session_id":"s","model":null}`), &c); err == nil {
		t.Fatal("accepted null retained metadata")
	}
	if err = readExternal(strings.NewReader(strings.Repeat(" ", 65537)), &h); err == nil {
		t.Fatal("accepted oversized input")
	}
}
func TestHermesCaptureLifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MachineID: "machine", StateDirectory: privateTempDir(t)}
	send := func(body string) {
		t.Helper()
		if err := Capture(ctx, cfg, "hermes", strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	send(`{"session_id":"old","hook_event_name":"on_session_start","extra":{}}`)
	send(`{"session_id":"old","hook_event_name":"post_api_request","extra":{"usage":{"prompt_tokens":12,"completion_tokens":3}}}`)
	send(`{"session_id":"old","hook_event_name":"on_session_end","extra":{}}`)
	rows, err := CacheRead(ctx, cfg, nil)
	if err != nil || len(rows) != 1 || rows[0].Presence.Activity.State != "idle" {
		t.Fatal(rows, err)
	}
	send(`{"session_id":"new","hook_event_name":"on_session_start","extra":{}}`)
	send(`{"session_id":"new","hook_event_name":"on_session_finalize","extra":{"old_session_id":"old"}}`)
	send(`{"session_id":"old","hook_event_name":"post_api_request","extra":{"usage":{"input_tokens":20}}}`)
	rows, err = CacheRead(ctx, cfg, nil)
	if err != nil || len(rows) != 1 || rows[0].Presence.Ref.NativeSessionID != "new" {
		t.Fatal(rows, err)
	}
}
func TestClaudeCaptureSourceMetadata(t *testing.T) {
	t.Setenv("TERM_PROGRAM", "test-terminal")
	t.Setenv("TERM_PROGRAM_VERSION", "1.2")
	ctx := context.Background()
	cfg := Config{MachineID: "machine", StateDirectory: privateTempDir(t)}
	if err := CaptureClaudeStatus(ctx, cfg, strings.NewReader(`{"session_id":"claude-session"}`)); err != nil {
		t.Fatal(err)
	}
	rows, err := CacheRead(ctx, cfg, nil)
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	p := rows[0].Presence
	if p.Machine == nil || p.Machine.Name == "" || p.Machine.OS == nil || p.Machine.Resources == nil || p.Machine.Resources.Source == "" {
		t.Fatalf("missing source machine metadata: %+v", p.Machine)
	}
	if len(p.Processes) != 0 {
		t.Fatalf("claimed hook child as session process: %+v", p.Processes)
	}
	if p.Terminal == nil || p.Terminal.Name != "test-terminal" || p.Terminal.Version != "1.2" {
		t.Fatalf("missing terminal: %+v", p.Terminal)
	}
}
