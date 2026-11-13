package coordinator

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestNativeLifecycleParity(t *testing.T) {
	for _, harness := range []string{"pi", "omp"} {
		t.Run(harness, func(t *testing.T) {
			o, err := newNativeObserver("machine", os.Getpid(), harness)
			if err != nil {
				t.Fatal(err)
			}
			s := wire.NativeSnapshot{Harness: harness, NativeSessionID: "one", CWD: "/work"}
			send := func(e wire.NativeEvent) { t.Helper(); s.Event = &e; o.update(s, true); s.Event = nil }
			status := func(want, kind string) {
				t.Helper()
				p, err := o.presence()
				if err != nil {
					t.Fatal(err)
				}
				if want == "" {
					if p.ProgramStatus != nil {
						t.Fatalf("status = %+v", p.ProgramStatus)
					}
					return
				}
				if p.ProgramStatus == nil || p.ProgramStatus.State != want || p.ProgramStatus.Kind != kind {
					t.Fatalf("status = %+v, want %s %s", p.ProgramStatus, want, kind)
				}
			}
			send(wire.NativeEvent{Type: "agent_start"})
			status("working", "")
			send(wire.NativeEvent{Type: "tool_execution_start", ToolCallID: "a", ToolName: "read"})
			send(wire.NativeEvent{Type: "tool_execution_start", ToolCallID: "b", ToolName: "search"})
			p, _ := o.presence()
			if p.Activity.Wait != "tool" || p.Activity.CurrentToolName != "read" {
				t.Fatal(p.Activity)
			}
			send(wire.NativeEvent{Type: "tool_execution_end", ToolCallID: "a"})
			p, _ = o.presence()
			if p.Activity.CurrentToolName != "search" {
				t.Fatal(p.Activity)
			}
			if harness == "pi" {
				send(wire.NativeEvent{Type: "ui_prompt_start"})
				status("blocked", "question")
				send(wire.NativeEvent{Type: "tool_execution_start", ToolCallID: "c", ToolName: "read"})
				status("blocked", "question")
				p, _ = o.presence()
				if p.Activity.Wait != "user" {
					t.Fatal(p.Activity)
				}
				send(wire.NativeEvent{Type: "ui_prompt_end"})
				status("working", "")
			} else {
				send(wire.NativeEvent{Type: "ui_prompt_start"})
				status("working", "")
			}
			for _, tc := range []struct{ stop, outcome, want string }{{"stop", "completed", "done"}, {"error", "error", "error"}, {"aborted", "aborted", "idle"}, {"", "", ""}} {
				send(wire.NativeEvent{Type: "agent_start"})
				send(wire.NativeEvent{Type: "message_end", StopReason: tc.stop})
				send(wire.NativeEvent{Type: "agent_end"})
				if harness == "pi" {
					status("working", "")
					send(wire.NativeEvent{Type: "agent_before_settle", Outcome: tc.outcome})
					send(wire.NativeEvent{Type: "agent_settled"})
				}
				status(tc.want, "")
			}
			continuing := true
			send(wire.NativeEvent{Type: "agent_end", WillContinue: &continuing})
			if harness == "pi" {
				send(wire.NativeEvent{Type: "agent_settled", WillContinue: &continuing})
			}
			status("working", "")
			send(wire.NativeEvent{Type: "tool_execution_start", ToolCallID: "old", ToolName: "private"})
			s.NativeSessionID = "two"
			s.IsIdle = true
			send(wire.NativeEvent{Type: "session_switch"})
			p, _ = o.presence()
			status("idle", "")
			if p.Activity.CurrentToolName != "" || p.Activity.Wait != "" {
				t.Fatal(p.Activity)
			}
		})
	}
}

func TestNativeAsyncJobsParity(t *testing.T) {
	for _, harness := range []string{"pi", "omp"} {
		t.Run(harness, func(t *testing.T) {
			o, err := newNativeObserver("machine", os.Getpid(), harness)
			if err != nil {
				t.Fatal(err)
			}
			s := wire.NativeSnapshot{Harness: harness, NativeSessionID: "s", CWD: "/work", AsyncJobs: &wire.NativeAsyncJobs{Running: 2, Pending: 1}}
			o.update(s, true)
			r, err := o.inspect()
			if err != nil {
				t.Fatal(err)
			}
			if harness == "pi" {
				if r.Activity.ActiveAsyncJobCount.Status != "unavailable" || r.Activity.PendingAsyncWorkCount.Status != "unavailable" {
					t.Fatal(r.Activity)
				}
			} else {
				if r.Activity.ActiveAsyncJobCount.Value == nil || *r.Activity.ActiveAsyncJobCount.Value != 2 || !r.Activity.ActiveAsyncJobCount.Provenance.Derived || r.Activity.PendingAsyncWorkCount.Value == nil || *r.Activity.PendingAsyncWorkCount.Value != 1 || r.Activity.PendingAsyncWorkCount.Provenance.Derived {
					t.Fatal(r.Activity)
				}
			}
		})
	}
}

func TestCaptureTerminalMarkersParity(t *testing.T) {
	for _, tc := range []struct{ name, version, wantName, wantVersion string }{
		{"tern", "0.4.3", "tern", "0.4.3"}, {"future-terminal", "", "future-terminal", ""}, {"", "1.0", "", ""}, {"   ", "", "", ""}, {"name\nbad", "", "", ""}, {strings.Repeat("x", 4097), "", "", ""}, {"valid", "version\u0085bad", "", ""},
	} {
		t.Run(tc.name+tc.version, func(t *testing.T) {
			t.Setenv("TERM_PROGRAM", tc.name)
			t.Setenv("TERM_PROGRAM_VERSION", tc.version)
			t.Setenv("ORCA_TERMINAL_HANDLE", "private-pane")
			t.Setenv("REX_SESSION", "private-session")
			t.Setenv("TERM", "xterm-256color")
			var r wire.SessionInspection
			captureSource(context.Background(), &r)
			if tc.wantName == "" {
				if r.Presence.Terminal != nil {
					t.Fatal(r.Presence.Terminal)
				}
				return
			}
			if r.Presence.Terminal == nil || r.Presence.Terminal.Name != tc.wantName || r.Presence.Terminal.Version != tc.wantVersion {
				t.Fatal(r.Presence.Terminal)
			}
		})
	}
}
