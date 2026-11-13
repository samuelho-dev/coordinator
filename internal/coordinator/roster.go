package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

type rosterBuffer struct{ bytes.Buffer }

func (b *rosterBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 65536 {
		return 0, errors.New("roster exceeds bound")
	}
	return b.Buffer.Write(p)
}
func claudeRoster(ctx context.Context, cfg Config, sampler *observe.Sampler, previous map[string]wire.SessionInspection) ([]wire.SessionPresence, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "claude", "agents", "--json")
	cmd.Stdin = nil
	cmd.Stderr = io.Discard
	buf := &rosterBuffer{}
	cmd.Stdout = buf
	if e := cmd.Run(); e != nil {
		return nil, e
	}
	var rows []struct {
		SessionID  string `json:"sessionId"`
		ID         string `json:"id"`
		PID        int    `json:"pid"`
		CWD        string `json:"cwd"`
		Kind       string `json:"kind"`
		Status     string `json:"status"`
		WaitingFor string `json:"waitingFor"`
	}
	if e := json.Unmarshal(buf.Bytes(), &rows); e != nil {
		return nil, e
	}
	if len(rows) > 256 {
		return nil, errors.New("roster exceeds bound")
	}
	out := make([]wire.SessionPresence, 0, len(rows))
	for _, row := range rows {
		id := row.SessionID
		if id == "" {
			id = row.ID
		}
		if id == "" || row.PID <= 0 {
			continue
		}
		identity, e := observe.Identify(ctx, row.PID)
		if e != nil {
			continue
		}
		resources := sampler.ReadMachine(ctx, row.PID)
		ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: "claude", NativeSessionID: id}
		old, exists := previous[ref.Key()]
		if exists && old.Presence.Machine != nil && old.Presence.Machine.Resources != nil {
			r := old.Presence.Machine.Resources
			bound := false
			for _, p := range old.Presence.Processes {
				if p.PID == row.PID && p.StartIdentity == identity.StartIdentity {
					bound = true
				}
			}
			if !bound && (r.Scope != resources.Scope || r.ScopeID == nil || resources.ScopeID == nil || *r.ScopeID != *resources.ScopeID) {
				continue
			}
		}
		p := wire.SessionPresence{Ref: ref, ObservedAt: float64(time.Now().UnixMilli()), Machine: &wire.SessionMachine{Resources: &resources}, Processes: []wire.ProcessIdentity{{PID: row.PID, StartIdentity: identity.StartIdentity, Ownership: "owned"}}, Activity: &wire.SessionActivity{State: "unknown"}}
		if row.Kind != "" {
			p.Agent = &wire.SessionAgent{Kind: row.Kind}
		}
		switch row.Status {
		case "busy":
			p.Activity.State = "working"
			p.ProgramStatus = &wire.ProgramStatus{State: "working"}
		case "idle":
			p.Activity.State = "idle"
			p.ProgramStatus = &wire.ProgramStatus{State: "idle"}
		case "waiting":
			p.Activity.State = "waiting"
		}
		if row.WaitingFor == "permission prompt" {
			p.Activity.Wait = "approval"
			p.Activity.Approval = "pending"
			p.ProgramStatus = &wire.ProgramStatus{State: "blocked", Kind: "permission"}
		}
		if row.CWD != "" {
			w := observe.Workspace(ctx, row.CWD, nil, nil)
			p.Workspace = &w
		}
		again, e := observe.Identify(ctx, row.PID)
		if e != nil || again.StartIdentity != identity.StartIdentity {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}
