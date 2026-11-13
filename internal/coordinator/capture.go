package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"unicode"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
	"golang.org/x/sys/unix"
)

type hookInput struct {
	SessionID string       `json:"session_id"`
	Name      string       `json:"hook_event_name"`
	Source    string       `json:"source"`
	CWD       string       `json:"cwd"`
	Model     string       `json:"model"`
	ToModel   string       `json:"to_model"`
	ToolName  string       `json:"tool_name"`
	Profile   string       `json:"profile"`
	Extra     *hermesExtra `json:"extra"`
}
type basicHook struct {
	SessionID string `json:"session_id"`
	Name      string `json:"hook_event_name"`
	Source    string `json:"source"`
	CWD       string `json:"cwd"`
	Model     string `json:"model"`
	ToModel   string `json:"to_model"`
	ToolName  string `json:"tool_name"`
}
type hermesHook struct {
	SessionID string       `json:"session_id"`
	Name      string       `json:"hook_event_name"`
	CWD       string       `json:"cwd"`
	Profile   string       `json:"profile"`
	Extra     *hermesExtra `json:"extra"`
}
type hermesExtra struct {
	OldSessionID string `json:"old_session_id"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
}
type claudeStatus struct {
	SessionID string `json:"session_id"`
	Model     *struct {
		ID string `json:"id"`
	} `json:"model"`
	Workspace *struct {
		Current string `json:"current_dir"`
		Project string `json:"project_dir"`
	} `json:"workspace"`
}

func readExternal(in io.Reader, out any) error {
	b, err := io.ReadAll(io.LimitReader(in, 65537))
	if err != nil {
		return err
	}
	if len(b) > 65536 {
		return errors.New("input exceeds 64KiB")
	}
	if err = json.Unmarshal(b, out); err != nil {
		return errors.New("invalid input JSON")
	}
	if err = validateRetained(b, reflect.TypeOf(out).Elem(), ""); err != nil {
		return err
	}
	return validateExternal(reflect.ValueOf(out), "")
}
func validateRetained(raw []byte, t reflect.Type, name string) error {
	if t.Kind() == reflect.Pointer {
		if string(raw) == "null" {
			return errors.New("invalid null field")
		}
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return errors.New("invalid object")
		}
		for i := range t.NumField() {
			field := t.Field(i)
			key := field.Tag.Get("json")
			if value, ok := fields[key]; ok {
				if err := validateRetained(value, field.Type, key); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if t.Kind() == reflect.String {
		var value string
		if string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
			return errors.New("invalid string")
		}
		if value == "" && name != "cwd" && name != "current_dir" && name != "project_dir" && name != "session_id" {
			return errors.New("empty native identifier")
		}
	}
	return nil
}
func validateExternal(v reflect.Value, name string) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return validateExternal(v.Elem(), name)
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := validateExternal(v.Field(i), v.Type().Field(i).Tag.Get("json")); err != nil {
				return err
			}
		}
	case reflect.String:
		limit := 256
		if name == "cwd" || name == "current_dir" || name == "project_dir" {
			limit = 4096
		}
		if len(v.String()) > limit {
			return errors.New("input field exceeds limit")
		}
	}
	return nil
}
func validID(s string) bool { return len(s) > 0 && len(s) <= 256 }
func normalizeHook(machine, harness string, h hookInput, at float64) (wire.SessionInspection, error) {
	if harness == "hermes" && h.Name == "on_session_finalize" && h.Extra != nil && h.Extra.OldSessionID != "" {
		h.SessionID = h.Extra.OldSessionID
	}
	if !validID(h.SessionID) || !validID(h.Name) || (harness != "claude" && harness != "codex" && harness != "hermes") {
		return wire.SessionInspection{}, errors.New("invalid hook identity")
	}
	p := wire.SessionPresence{Ref: wire.SessionRef{MachineID: machine, Harness: harness, NativeSessionID: h.SessionID}, ObservedAt: at, LastActivityAt: &at, NativeUpdatedAt: &at, Activity: &wire.SessionActivity{State: "unknown"}}
	if h.CWD != "" {
		p.Workspace = &wire.SessionWorkspace{CWD: h.CWD}
	}
	if h.Model != "" || h.ToModel != "" {
		p.Model = &wire.SessionModel{ID: h.Model}
		if h.ToModel != "" {
			p.Model.ID = h.ToModel
		}
	}
	if harness == "hermes" {
		if h.Extra == nil {
			return wire.SessionInspection{}, errors.New("missing Hermes extra")
		}
		switch h.Name {
		case "on_session_start", "post_api_request":
		case "on_session_end":
			p.Activity.State = "idle"
		case "on_session_finalize":
			p.Activity.State = "stopped"
		default:
			return wire.SessionInspection{}, errors.New("invalid Hermes event")
		}
		if h.Name == "post_api_request" {
			p.NativeUpdatedAt = nil
		}
		if h.Profile != "" {
			p.Agent = &wire.SessionAgent{Profile: h.Profile}
		}
		if h.Extra.Model != "" || h.Extra.Provider != "" {
			p.Model = &wire.SessionModel{ID: h.Extra.Model, Provider: h.Extra.Provider}
		}
		return wire.CreateInspection(p, at), nil
	}
	switch {
	case h.Name == "PreCompact" || h.Source == "compact":
		p.Activity.State = "working"
		compacting := true
		p.Activity.Compacting = &compacting
	case h.Name == "SessionEnd":
		p.Activity.State = "stopped"
	case h.Name == "PermissionRequest":
		p.Activity.State = "waiting"
		p.Activity.Wait = "approval"
		p.Activity.Approval = "pending"
		p.Activity.CurrentToolName = h.ToolName
		p.ProgramStatus = &wire.ProgramStatus{State: "blocked", Kind: "permission"}
	case h.Name == "PreToolUse":
		p.Activity.State = "working"
		p.Activity.Wait = "tool"
		p.Activity.CurrentToolName = h.ToolName
	case h.Name == "Stop" || h.Name == "Interrupt":
		p.Activity.State = "idle"
	case h.Name == "UserPromptSubmit":
		p.Activity.State = "working"
		p.Activity.Wait = "api"
	}
	if p.ProgramStatus == nil {
		switch {
		case harness == "claude" && h.Name == "Elicitation":
			p.ProgramStatus = &wire.ProgramStatus{State: "blocked", Kind: "question"}
		case h.Name == "Interrupt":
			p.ProgramStatus = &wire.ProgramStatus{State: "idle"}
		case harness == "claude" && h.Name == "StopFailure":
			p.ProgramStatus = &wire.ProgramStatus{State: "error"}
		case harness == "claude" && h.Name == "Stop":
			p.ProgramStatus = &wire.ProgramStatus{State: "done"}
		case p.Activity.State == "working" || (harness == "claude" && (h.Name == "PostToolUse" || h.Name == "PostToolUseFailure" || h.Name == "ElicitationResult")):
			p.ProgramStatus = &wire.ProgramStatus{State: "working"}
		}
	}
	return wire.CreateInspection(p, at), nil
}
func normalizeClaudeStatus(machine string, d claudeStatus, at float64) (wire.SessionInspection, error) {
	if !validID(d.SessionID) {
		return wire.SessionInspection{}, errors.New("invalid session")
	}
	p := wire.SessionPresence{Ref: wire.SessionRef{MachineID: machine, Harness: "claude", NativeSessionID: d.SessionID}, ObservedAt: at}
	if d.Model != nil && d.Model.ID != "" {
		p.Model = &wire.SessionModel{ID: d.Model.ID}
	}
	if d.Workspace != nil && d.Workspace.Current != "" {
		p.Workspace = &wire.SessionWorkspace{CWD: d.Workspace.Current}
		if d.Workspace.Project != "" {
			p.Workspace.Project = &wire.SessionProject{Root: d.Workspace.Project, Source: "native"}
		}
	}
	return wire.CreateInspection(p, at), nil
}

// Capture metadata describes the hook process's machine scope, not session ownership.
func captureSource(ctx context.Context, r *wire.SessionInspection) {
	name, _ := os.Hostname()
	machine := &wire.SessionMachine{Name: name}
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		info := &wire.MachineOS{Platform: runtime.GOOS, Architecture: runtime.GOARCH}
		var u unix.Utsname
		if unix.Uname(&u) == nil {
			var b []byte
			for _, c := range u.Release {
				if c == 0 {
					break
				}
				b = append(b, byte(c))
			}
			info.Release = string(b)
		}
		machine.OS = info
	}
	resources := observe.NewSampler().ReadMachine(ctx, 0)
	machine.Resources = &resources
	r.Presence.Machine = machine
	terminalName, terminalVersion := os.Getenv("TERM_PROGRAM"), os.Getenv("TERM_PROGRAM_VERSION")
	valid := func(s string) bool {
		if len(s) > 4096 || strings.TrimSpace(s) == "" {
			return false
		}
		for _, c := range s {
			if unicode.IsControl(c) {
				return false
			}
		}
		return true
	}
	if valid(terminalName) && (terminalVersion == "" || valid(terminalVersion)) {
		r.Presence.Terminal = &wire.SessionTerminal{Name: terminalName, Version: terminalVersion}
	}
}
func Capture(ctx context.Context, cfg Config, harness string, in io.Reader) error {
	var h hookInput
	if harness == "hermes" {
		var v hermesHook
		if err := readExternal(in, &v); err != nil {
			return err
		}
		h = hookInput{SessionID: v.SessionID, Name: v.Name, CWD: v.CWD, Profile: v.Profile, Extra: v.Extra}
	} else {
		var v basicHook
		if err := readExternal(in, &v); err != nil {
			return err
		}
		h = hookInput{SessionID: v.SessionID, Name: v.Name, Source: v.Source, CWD: v.CWD, Model: v.Model, ToModel: v.ToModel, ToolName: v.ToolName}
	}
	r, err := normalizeHook(cfg.MachineID, harness, h, clockMS())
	if err != nil {
		return err
	}
	captureSource(ctx, &r)
	if harness != "hermes" {
		return CacheWrite(ctx, cfg, r)
	}
	id, idErr := observe.Identify(ctx, os.Getppid())
	if idErr == nil {
		id.Ownership = "shared"
	}
	if h.Name == "on_session_finalize" {
		return CacheFinalizeHermes(ctx, cfg, r.Presence.Ref, id)
	}
	if idErr == nil {
		r.Presence.Processes = []wire.ProcessIdentity{id}
	}
	if h.Name == "on_session_start" {
		return CacheStart(ctx, cfg, r)
	}
	return CacheWrite(ctx, cfg, r)
}
func CaptureClaudeStatus(ctx context.Context, cfg Config, in io.Reader) error {
	var d claudeStatus
	if err := readExternal(in, &d); err != nil {
		return err
	}
	r, err := normalizeClaudeStatus(cfg.MachineID, d, clockMS())
	if err != nil {
		return err
	}
	captureSource(ctx, &r)
	return CacheWrite(ctx, cfg, r)
}
