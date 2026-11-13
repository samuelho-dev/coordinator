package coordinator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

type nativeObserver struct {
	mu                 sync.Mutex
	machineID, harness string
	process            *wire.ProcessIdentity
	current            *wire.NativeSnapshot
	lastActivity       *float64
	status, prompt     *wire.ProgramStatus
	stop               string
	tools              map[string]string
	toolOrder          []string
}

func clockMS() float64 { return float64(time.Now().UnixNano()) / 1e6 }
func newNativeObserver(machineID string, pid int, harness string) (*nativeObserver, error) {
	if machineID == "" || (harness != "pi" && harness != "omp") || pid <= 0 {
		return nil, errors.New("invalid native observer identity")
	}
	o := &nativeObserver{machineID: machineID, harness: harness, tools: map[string]string{}}
	if id, err := observe.Identify(context.Background(), pid); err == nil {
		if harness == "omp" {
			id.Ownership = "shared"
		} else {
			id.Ownership = "owned"
		}
		o.process = &id
	}
	return o, nil
}
func (o *nativeObserver) update(s wire.NativeSnapshot, active bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if s.Harness != o.harness || s.NativeSessionID == "" {
		return
	}
	if o.current == nil || o.current.NativeSessionID != s.NativeSessionID || o.current.NativeAgentID != s.NativeAgentID {
		o.lastActivity = nil
		o.status = nil
		o.prompt = nil
		o.stop = ""
		o.tools = map[string]string{}
		o.toolOrder = nil
	}
	now := clockMS()
	if active {
		o.lastActivity = &now
	}
	e := s.Event
	if e != nil {
		switch e.Type {
		case "tool_execution_start":
			if e.ToolCallID != "" && e.ToolName != "" {
				if _, ok := o.tools[e.ToolCallID]; !ok && len(o.toolOrder) < 128 {
					o.toolOrder = append(o.toolOrder, e.ToolCallID)
					o.tools[e.ToolCallID] = e.ToolName
				} else if ok {
					o.tools[e.ToolCallID] = e.ToolName
				}
			}
			o.status = &wire.ProgramStatus{State: "working"}
		case "tool_execution_end":
			delete(o.tools, e.ToolCallID)
			for i, id := range o.toolOrder {
				if id == e.ToolCallID {
					o.toolOrder = append(o.toolOrder[:i], o.toolOrder[i+1:]...)
					break
				}
			}
		case "agent_start", "turn_start":
			o.status = &wire.ProgramStatus{State: "working"}
			o.stop = ""
		case "ui_prompt_start":
			if o.harness == "pi" {
				o.prompt = &wire.ProgramStatus{State: "blocked", Kind: "question"}
			}
		case "ui_prompt_end":
			if o.harness == "pi" {
				o.prompt = nil
			}
		}
		if e.StopReason != "" {
			o.stop = e.StopReason
		}
		if e.Type == "agent_before_settle" && o.harness == "pi" {
			o.stop = e.Outcome
			if o.stop == "completed" {
				o.stop = "stop"
			}
		}
		if e.Type == "agent_end" {
			o.tools = map[string]string{}
			o.toolOrder = nil
		}
		if e.Type == "agent_settled" || (e.Type == "agent_end" && o.harness != "pi") {
			o.status = nil
			if e.WillContinue != nil && *e.WillContinue {
				o.status = &wire.ProgramStatus{State: "working"}
			} else {
				switch o.stop {
				case "aborted":
					o.status = &wire.ProgramStatus{State: "idle"}
				case "error":
					o.status = &wire.ProgramStatus{State: "error"}
				case "stop", "length":
					o.status = &wire.ProgramStatus{State: "done"}
				}
			}
		} else if e.Type == "agent_end" {
			o.status = &wire.ProgramStatus{State: "working"}
		}
	}
	if o.status == nil && (e == nil || e.Type == "session_start" || e.Type == "session_switch") {
		state := "working"
		if s.IsIdle {
			state = "idle"
		}
		o.status = &wire.ProgramStatus{State: state}
	}
	o.current = &s
}
func (o *nativeObserver) presenceLocked() (wire.SessionPresence, error) {
	if o.current == nil {
		return wire.SessionPresence{}, errors.New("native snapshot unavailable")
	}
	s := o.current
	p := wire.SessionPresence{Ref: wire.SessionRef{MachineID: o.machineID, Harness: o.harness, NativeSessionID: s.NativeSessionID, NativeAgentID: s.NativeAgentID}, ObservedAt: clockMS(), LastActivityAt: o.lastActivity, Workspace: &wire.SessionWorkspace{CWD: s.CWD}, Agent: s.Agent, Model: s.Model, Parent: s.Parent, HarnessVersion: s.HarnessVersion}
	if o.process != nil {
		p.Processes = []wire.ProcessIdentity{*o.process}
	}
	p.ProgramStatus = o.status
	if o.prompt != nil {
		p.ProgramStatus = o.prompt
	}
	state := "working"
	if s.IsIdle {
		state = "idle"
	}
	streaming := !s.IsIdle
	p.Activity = &wire.SessionActivity{State: state, Streaming: &streaming}
	for _, id := range o.toolOrder {
		if name, ok := o.tools[id]; ok {
			p.Activity.CurrentToolName = name
			p.Activity.Wait = "tool"
			break
		}
	}
	if o.prompt != nil {
		p.Activity.State = "waiting"
		p.Activity.Wait = "user"
	}
	return p, nil
}
func (o *nativeObserver) presence() (wire.SessionPresence, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.presenceLocked()
}
func provenance(source, scope, description string, at float64) wire.MetricProvenance {
	return wire.MetricProvenance{Source: source, Scope: scope, ScopeDescription: description, ObservedAt: &at}
}
func numberMetric(unit string, v *float64, p wire.MetricProvenance) wire.Metric {
	if v == nil {
		return wire.UnavailableMetric(unit, "Native counter not supplied", p)
	}
	return wire.NewMetric(unit, *v, p)
}
func (o *nativeObserver) inspect() (wire.SessionInspection, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	p, err := o.presenceLocked()
	if err != nil {
		return wire.SessionInspection{}, err
	}
	now := p.ObservedAt
	result := wire.CreateInspection(p, now)
	s := o.current
	if j := s.AsyncJobs; j != nil && o.harness == "omp" {
		jp := provenance("omp.ctx.getAsyncJobSnapshot.delivery.queued", "native-aggregate", "Current session pending deliveries including in-flight deliveries", now)
		result.Activity.PendingAsyncWorkCount = numberMetric("count", &j.Pending, jp)
		jp.Source = "omp.ctx.getAsyncJobSnapshot.running"
		jp.ScopeDescription = "Current session running async jobs"
		jp.Derived = true
		result.Activity.ActiveAsyncJobCount = numberMetric("count", &j.Running, jp)
	}
	return result, nil
}
