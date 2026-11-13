package coordinator

import (
	"os"
	"testing"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestNativeObserverLifecycle(t *testing.T) {
	o, err := newNativeObserver("m", os.Getpid(), "pi")
	if err != nil {
		t.Fatal(err)
	}
	s := wire.NativeSnapshot{Harness: "pi", NativeSessionID: "s", CWD: "/tmp", Event: &wire.NativeEvent{Type: "agent_start"}}
	o.update(s, true)
	p, err := o.presence()
	if err != nil || p.ProgramStatus.State != "working" || p.LastActivityAt == nil {
		t.Fatal(p, err)
	}
	at := *p.LastActivityAt
	s.Event = nil
	s.IsIdle = true
	o.update(s, false)
	p, _ = o.presence()
	if *p.LastActivityAt != at {
		t.Fatal("getter refreshed activity")
	}
	r, err := o.inspect()
	if err != nil || r.Presence.Ref.NativeSessionID != "s" || *r.Presence.LastActivityAt != at {
		t.Fatal(r, err)
	}
	s.Event = &wire.NativeEvent{Type: "ui_prompt_start"}
	o.update(s, true)
	p, _ = o.presence()
	if p.ProgramStatus.State != "blocked" || p.Activity.Wait != "user" {
		t.Fatal(p)
	}
	s.Event = &wire.NativeEvent{Type: "ui_prompt_end"}
	o.update(s, true)
	s.Event = &wire.NativeEvent{Type: "agent_before_settle", Outcome: "completed"}
	o.update(s, true)
	s.Event = &wire.NativeEvent{Type: "agent_settled"}
	o.update(s, true)
	p, _ = o.presence()
	if p.ProgramStatus.State != "done" {
		t.Fatal(p)
	}
	s.NativeSessionID = "new"
	s.Event = nil
	o.update(s, false)
	r, err = o.inspect()
	if err != nil || r.Presence.Ref.NativeSessionID != "new" || r.Presence.LastActivityAt != nil || r.Presence.ProgramStatus.State != "idle" {
		t.Fatal("state leaked across sessions", r, err)
	}
}
