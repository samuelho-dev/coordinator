package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

type cacheRecord struct {
	Version    int                    `json:"version"`
	Kind       string                 `json:"kind"`
	ExpiresAt  float64                `json:"expiresAt"`
	Inspection wire.SessionInspection `json:"inspection"`
}

var cacheName = regexp.MustCompile(`^v6-(hermes-)?[a-f0-9]{64}\.json$`)

func cachePath(cfg Config, ref wire.SessionRef) string {
	sum := sha256.Sum256([]byte(ref.Key()))
	prefix := "v6-"
	if ref.Harness == "hermes" {
		prefix += "hermes-"
	}
	return filepath.Join(cfg.StateDirectory, "native-observations", fmt.Sprintf("%s%x.json", prefix, sum))
}
func nowMillis() float64 { return float64(time.Now().UnixMilli()) }
func lockCache(ctx context.Context, cfg Config) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(cfg.StateDirectory, "native-observations")
	if err := WritePrivate(dir, map[string][]byte{}); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, ".lock")
	fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), p)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	uid, ok := owner(info)
	if !info.Mode().IsRegular() || !ok || uid != uint32(os.Getuid()) || info.Mode().Perm() != 0600 {
		f.Close()
		return nil, errors.New("unsafe cache lock")
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			if err = ctx.Err(); err != nil {
				unlockCache(f)
				return nil, err
			}
			return f, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN && err != syscall.EINTR {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func unlockCache(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }
func scopeMatches(a, b *wire.MachineResources) bool {
	return a != nil && b != nil && a.ScopeID != nil && b.ScopeID != nil && a.Scope == b.Scope && *a.ScopeID == *b.ScopeID
}
func processAlive(ctx context.Context, p wire.ProcessIdentity) bool {
	id, err := observe.Identify(ctx, p.PID)
	return err == nil && id.StartIdentity == p.StartIdentity
}
func records(ctx context.Context, cfg Config, local *wire.MachineResources) (map[string]cacheRecord, error) {
	dir := filepath.Join(cfg.StateDirectory, "native-observations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := map[string]cacheRecord{}
	count := 0
	for _, entry := range entries {
		if !cacheName.MatchString(entry.Name()) {
			continue
		}
		count++
		if count > 1024 {
			return nil, errors.New("metadata cache exceeds bound")
		}
		path := filepath.Join(dir, entry.Name())
		b, e := ReadPrivate(path)
		if e != nil {
			continue
		}
		var r cacheRecord
		d := json.NewDecoder(bytes.NewReader(b))
		d.DisallowUnknownFields()
		if d.Decode(&r) != nil {
			continue
		}
		var extra any
		if d.Decode(&extra) != io.EOF || r.Version != 6 || r.Kind != "inspection" || r.Inspection.Presence.Ref.MachineID != cfg.MachineID || cachePath(cfg, r.Inspection.Presence.Ref) != path || wire.Validate(r.Inspection) != nil {
			continue
		}
		stopped := r.Inspection.Presence.Activity != nil && r.Inspection.Presence.Activity.State == "stopped"
		live := false
		if !stopped && r.Inspection.Presence.Ref.Harness == "hermes" && (local == nil || r.Inspection.Presence.Machine != nil && scopeMatches(r.Inspection.Presence.Machine.Resources, local)) {
			for _, p := range r.Inspection.Presence.Processes {
				if p.Ownership == "shared" && processAlive(ctx, p) {
					live = true
					break
				}
			}
		}
		// Non-start Hermes callbacks cannot create records, so expired tombstones are safe to remove.
		if stopped && r.Inspection.Presence.Ref.Harness != "hermes" || !live && r.ExpiresAt <= nowMillis() {
			if e = ctx.Err(); e != nil {
				return nil, e
			}
			if e = os.Remove(path); e != nil {
				return nil, e
			}
			continue
		}
		out[path] = r
	}
	return out, nil
}
func compatible(a, b wire.SessionPresence) bool {
	if len(a.Processes) == 0 || len(b.Processes) == 0 {
		return true
	}
	for _, x := range a.Processes {
		for _, y := range b.Processes {
			if x.PID == y.PID && x.StartIdentity == y.StartIdentity {
				return true
			}
		}
	}
	return false
}
func mergePresence(old, in wire.SessionPresence) wire.SessionPresence {
	if !compatible(old, in) {
		return in
	}
	p := in
	if p.HarnessVersion == "" {
		p.HarnessVersion = old.HarnessVersion
	}
	if p.Machine == nil {
		p.Machine = old.Machine
	}
	if p.Agent == nil {
		p.Agent = old.Agent
	} else if old.Agent != nil {
		a := *p.Agent
		if a.Kind == "" {
			a.Kind = old.Agent.Kind
		}
		if a.Name == "" {
			a.Name = old.Agent.Name
		}
		if a.Profile == "" {
			a.Profile = old.Agent.Profile
		}
		p.Agent = &a
	}
	if p.Model == nil {
		p.Model = old.Model
	} else if old.Model != nil && (p.Model.ID == "" || p.Model.ID == old.Model.ID) && (p.Model.Provider == "" || p.Model.Provider == old.Model.Provider) {
		m := *p.Model
		if m.ID == "" {
			m.ID = old.Model.ID
		}
		if m.Provider == "" {
			m.Provider = old.Model.Provider
		}
		p.Model = &m
	}
	if p.Workspace == nil {
		p.Workspace = old.Workspace
	} else if old.Workspace != nil && p.Workspace.CWD == old.Workspace.CWD {
		w := *p.Workspace
		if w.Git == nil || old.Workspace.Git != nil && old.Workspace.Git.ObservedAt > w.Git.ObservedAt {
			w.Git = old.Workspace.Git
		}
		if w.Project == nil {
			w.Project = old.Workspace.Project
		}
		p.Workspace = &w
	}
	if p.Connection == nil {
		p.Connection = old.Connection
	}
	if p.NativeUpdatedAt == nil {
		p.NativeUpdatedAt = old.NativeUpdatedAt
	}
	if p.StartedAt == nil {
		p.StartedAt = old.StartedAt
	}
	if p.LastActivityAt == nil {
		p.LastActivityAt = old.LastActivityAt
	}
	if p.Parent == nil {
		p.Parent = old.Parent
	}
	if p.Label == "" {
		p.Label = old.Label
	}
	if p.Terminal == nil {
		p.Terminal = old.Terminal
	}
	if p.ProgramStatus == nil && p.Activity == nil {
		p.ProgramStatus = old.ProgramStatus
	}
	if len(p.Processes) == 0 {
		p.Processes = old.Processes
	}
	if p.Resources == nil {
		p.Resources = old.Resources
	}
	if p.Activity == nil {
		p.Activity = old.Activity
	}
	return p
}

func writeRecord(path string, r cacheRecord) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > maxPrivateBytes {
		return errors.New("metadata cache exceeds bound")
	}
	id, err := newUUID()
	if err != nil {
		return err
	}
	tmp := path + "." + id
	if err = WritePrivate(filepath.Dir(path), map[string][]byte{tmp: b}); err != nil {
		return err
	}
	defer os.Remove(tmp)
	return os.Rename(tmp, path)
}
func cacheWrite(ctx context.Context, cfg Config, in wire.SessionInspection, start bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.Presence.Ref.MachineID != cfg.MachineID {
		return errors.New("wrong machine identity")
	}
	if err := wire.Validate(in); err != nil {
		return errors.New("invalid inspection")
	}
	lock, err := lockCache(ctx, cfg)
	if err != nil {
		return err
	}
	defer unlockCache(lock)
	rs, err := records(ctx, cfg, nil)
	if err != nil {
		return err
	}
	path := cachePath(cfg, in.Presence.Ref)
	old, exists := rs[path]
	if in.Presence.Ref.Harness == "hermes" && !start {
		if !exists {
			return nil
		}
		prior := old.Inspection.Presence.Processes
		if len(prior) > 0 && (len(in.Presence.Processes) == 0 || !compatible(old.Inspection.Presence, in.Presence)) {
			return nil
		}
	}
	if !exists {
		if _, e := os.Lstat(path); e == nil {
			return errors.New("unsupported cache input")
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if len(rs) >= 1024 {
			return errors.New("metadata cache exceeds bound")
		}
	} else if old.Inspection.Presence.Activity != nil && old.Inspection.Presence.Activity.State == "stopped" {
		if !start {
			return nil
		}
	} else if compatible(old.Inspection.Presence, in.Presence) {
		in.Presence = mergePresence(old.Inspection.Presence, in.Presence)
	}
	if workspace := in.Presence.Workspace; workspace != nil {
		var project *wire.SessionProject
		if cfg.Project != nil {
			project = &wire.SessionProject{Root: cfg.Project.Root, ID: cfg.Project.ID, Name: cfg.Project.Name, Source: "configured"}
		} else if workspace.Project != nil && workspace.Project.Source == "native" {
			project = workspace.Project
		}
		var previous *wire.SessionWorkspace
		if exists && compatible(old.Inspection.Presence, in.Presence) {
			previous = old.Inspection.Presence.Workspace
		}
		observed := observe.Workspace(ctx, workspace.CWD, previous, project)
		in.Presence.Workspace = &observed
	}
	if len(in.Presence.Processes) > 0 && processAlive(ctx, in.Presence.Processes[0]) {
		resources := observe.NewSampler().ReadMachine(ctx, in.Presence.Processes[0].PID)
		if processAlive(ctx, in.Presence.Processes[0]) {
			machine := wire.SessionMachine{}
			if in.Presence.Machine != nil {
				machine = *in.Presence.Machine
			}
			machine.Resources = &resources
			in.Presence.Machine = &machine
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return writeRecord(path, cacheRecord{6, "inspection", in.Presence.ObservedAt + 120000, in})
}
func CacheWrite(ctx context.Context, cfg Config, in wire.SessionInspection) error {
	return cacheWrite(ctx, cfg, in, false)
}
func CacheStart(ctx context.Context, cfg Config, in wire.SessionInspection) error {
	return cacheWrite(ctx, cfg, in, true)
}
func CacheFinalize(ctx context.Context, cfg Config, ref wire.SessionRef) error {
	return cacheFinalize(ctx, cfg, ref, nil)
}
func CacheFinalizeHermes(ctx context.Context, cfg Config, ref wire.SessionRef, process wire.ProcessIdentity) error {
	return cacheFinalize(ctx, cfg, ref, &process)
}
func cacheFinalize(ctx context.Context, cfg Config, ref wire.SessionRef, process *wire.ProcessIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ref.MachineID != cfg.MachineID {
		return errors.New("wrong machine identity")
	}
	lock, err := lockCache(ctx, cfg)
	if err != nil {
		return err
	}
	defer unlockCache(lock)
	rs, err := records(ctx, cfg, nil)
	if err != nil {
		return err
	}
	path := cachePath(cfg, ref)
	r, ok := rs[path]
	if process != nil && ok {
		prior := r.Inspection.Presence.Processes
		if len(prior) > 0 && !compatible(r.Inspection.Presence, wire.SessionPresence{Processes: []wire.ProcessIdentity{*process}}) {
			return nil
		}
	}
	if !ok {
		if _, e := os.Lstat(path); e == nil {
			return errors.New("unsupported cache input")
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if len(rs) >= 1024 {
			return errors.New("metadata cache exceeds bound")
		}
		at := nowMillis()
		r = cacheRecord{6, "inspection", at + 120000, wire.CreateInspection(wire.SessionPresence{Ref: ref, ObservedAt: at}, at)}
	}
	r.ExpiresAt = nowMillis() + 120000
	r.Inspection.Presence.Activity = &wire.SessionActivity{State: "stopped"}
	if err = ctx.Err(); err != nil {
		return err
	}
	return writeRecord(path, r)
}
func CacheRead(ctx context.Context, cfg Config, local *wire.MachineResources) ([]wire.SessionInspection, error) {
	lock, err := lockCache(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer unlockCache(lock)
	rs, err := records(ctx, cfg, local)
	if err != nil {
		return nil, err
	}
	out := make([]wire.SessionInspection, 0, len(rs))
	for _, r := range rs {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		p := r.Inspection.Presence
		if p.Activity != nil && p.Activity.State == "stopped" {
			continue
		}
		if local != nil && (p.Machine == nil || !scopeMatches(p.Machine.Resources, local)) {
			out = append(out, r.Inspection)
			continue
		}
		if len(p.Processes) > 0 {
			alive := make([]wire.ProcessIdentity, 0, len(p.Processes))
			for _, proc := range p.Processes {
				if processAlive(ctx, proc) {
					alive = append(alive, proc)
				}
			}
			if len(alive) == 0 {
				continue
			}
			p.Processes = alive
			if p.Ref.Harness == "hermes" {
				p.ObservedAt = nowMillis()
			}
			r.Inspection.Presence = p
		}
		out = append(out, r.Inspection)
	}
	return out, nil
}
