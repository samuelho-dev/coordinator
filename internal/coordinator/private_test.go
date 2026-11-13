package coordinator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestPrivateExclusiveAndSymlinks(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := WritePrivate(dir, map[string][]byte{"secret": []byte("original")}); err != nil {
		t.Fatal(err)
	}
	if err := WritePrivate(dir, map[string][]byte{"secret": []byte("replacement"), "other": []byte("x")}); err == nil {
		t.Fatal("overwrote secret")
	}
	b, err := ReadPrivate(filepath.Join(dir, "secret"))
	if err != nil || string(b) != "original" {
		t.Fatalf("secret changed: %q %v", b, err)
	}
	if _, err = os.Stat(filepath.Join(dir, "other")); !os.IsNotExist(err) {
		t.Fatalf("rollback: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err = os.Symlink(filepath.Join(dir, "secret"), link); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadPrivate(link); err == nil {
		t.Fatal("followed symlink")
	}
	if err = os.Chmod(filepath.Join(dir, "secret"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadPrivate(filepath.Join(dir, "secret")); err == nil {
		t.Fatal("accepted public secret")
	}
}
func TestWritePrivateConcurrentDirectoryCreation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state", "coordinator")
	const count = 64
	start := make(chan struct{})
	results := make(chan error, count)
	var ready sync.WaitGroup
	ready.Add(count)
	for i := range count {
		go func(i int) {
			ready.Done()
			<-start
			results <- WritePrivate(dir, map[string][]byte{strconv.Itoa(i): []byte("private")})
		}(i)
	}
	ready.Wait()
	close(start)
	for range count {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for i := range count {
		data, err := ReadPrivate(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil || string(data) != "private" {
			t.Fatalf("file %d: %q %v", i, data, err)
		}
	}
}

func TestIdentityAndTokenReread(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_STATE_HOME", base)
	a, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil || a.MachineID != b.MachineID {
		t.Fatalf("identity changed: %v", err)
	}
	dir := filepath.Join(base, "tokens")
	if err = WritePrivate(dir, map[string][]byte{"token": []byte("abcdefghijklmnop\n")}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "token")
	cfg := wire.ConnectorConfig{TokenFile: file}
	token, err := Token(cfg)
	if err != nil || token != "abcdefghijklmnop" {
		t.Fatalf("token: %v", err)
	}
	if err = os.WriteFile(file, []byte("qrstuvwxyz012345\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, err = Token(cfg)
	if err != nil || token != "qrstuvwxyz012345" {
		t.Fatalf("token not reread: %v", err)
	}
}
func TestHermesFinalizeBlocksLateCallbacks(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cfg, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := nowMillis()
	ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: "hermes", NativeSessionID: "session"}
	in := wire.CreateInspection(wire.SessionPresence{Ref: ref, ObservedAt: at}, at)
	if err = CacheStart(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	if err = CacheFinalize(ctx, cfg, ref); err != nil {
		t.Fatal(err)
	}
	if err = CacheWrite(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	rows, err := CacheRead(ctx, cfg, nil)
	if err != nil || len(rows) != 0 {
		t.Fatalf("late callback resurrected session: %d %v", len(rows), err)
	}
	if err = CacheStart(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	rows, err = CacheRead(ctx, cfg, nil)
	if err != nil || len(rows) != 1 {
		t.Fatalf("new start did not restore session: %d %v", len(rows), err)
	}
}

func TestMergePreservesMetadataAndProcessIdentity(t *testing.T) {
	old := wire.SessionPresence{Model: &wire.SessionModel{ID: "model"}, Processes: []wire.ProcessIdentity{{PID: 42, StartIdentity: "birth", Ownership: "shared"}}}
	in := mergePresence(old, wire.SessionPresence{})
	if in.Model == nil || in.Model.ID != "model" || len(in.Processes) != 1 || in.Processes[0].StartIdentity != "birth" {
		t.Fatal("ordinary hook erased metadata", in)
	}
	if compatible(old, wire.SessionPresence{Processes: []wire.ProcessIdentity{{PID: 42, StartIdentity: "reused", Ownership: "shared"}}}) {
		t.Fatal("reused pid accepted as same process")
	}
}
func TestHermesGuardsPriorBinding(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cfg, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := nowMillis()
	ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: "hermes", NativeSessionID: "guard"}
	p := wire.ProcessIdentity{PID: 1234567, StartIdentity: "birth", Ownership: "shared"}
	in := wire.CreateInspection(wire.SessionPresence{Ref: ref, ObservedAt: at, Processes: []wire.ProcessIdentity{p}}, at)
	if err = CacheWrite(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	rs, err := records(ctx, cfg, nil)
	if err != nil || len(rs) != 0 {
		t.Fatalf("callback created missing session: %v", err)
	}
	if err = CacheStart(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	wrong := p
	wrong.StartIdentity = "other"
	if err = CacheFinalizeHermes(ctx, cfg, ref, wrong); err != nil {
		t.Fatal(err)
	}
	rs, err = records(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs[cachePath(cfg, ref)].Inspection.Presence.Activity != nil {
		t.Fatal("foreign process finalized session")
	}
	if err = CacheFinalizeHermes(ctx, cfg, ref, p); err != nil {
		t.Fatal(err)
	}
	rs, err = records(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs[cachePath(cfg, ref)].Inspection.Presence.Activity == nil || rs[cachePath(cfg, ref)].Inspection.Presence.Activity.State != "stopped" {
		t.Fatal("matching process did not finalize")
	}
}
func TestCacheLockCancellationAndExpiredTombstone(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cfg, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lock, err := lockCache(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err = CacheRead(deadline, cfg, nil)
	unlockCache(lock)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock ignored deadline: %v", err)
	}
	at := nowMillis()
	ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: "hermes", NativeSessionID: "expired"}
	in := wire.CreateInspection(wire.SessionPresence{Ref: ref, ObservedAt: at, Activity: &wire.SessionActivity{State: "stopped"}}, at)
	path := cachePath(cfg, ref)
	if err = writeRecord(path, cacheRecord{6, "inspection", at - 1, in}); err != nil {
		t.Fatal(err)
	}
	if _, err = CacheRead(ctx, cfg, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("tombstone not expired: %v", err)
	}
	in.Presence.Activity = nil
	if err = CacheWrite(ctx, cfg, in); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("late callback resurrected expired tombstone")
	}
}
func TestCacheV6PreservesV5(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cfg, err := ResolveConfig(wire.ConnectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, harness := range []string{"omp", "hermes"} {
		at := nowMillis()
		ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: harness, NativeSessionID: "namespace"}
		in := wire.CreateInspection(wire.SessionPresence{Ref: ref, ObservedAt: at}, at)
		path := cachePath(cfg, ref)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		oldPath := filepath.Join(filepath.Dir(path), "v5-"+strings.TrimPrefix(filepath.Base(path), "v6-"))
		if err := writeRecord(oldPath, cacheRecord{5, "inspection", at - 1, in}); err != nil {
			t.Fatal(err)
		}
		old, err := os.ReadFile(oldPath)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := CacheRead(ctx, cfg, nil)
		if err != nil || len(rows) != 0 {
			t.Fatalf("%s: v5 cache read: %d %v", harness, len(rows), err)
		}
		if harness == "hermes" {
			if err := CacheStart(ctx, cfg, in); err != nil {
				t.Fatal(err)
			}
		}
		if err := CacheWrite(ctx, cfg, in); err != nil {
			t.Fatal(err)
		}
		rs, err := records(ctx, cfg, nil)
		if err != nil || rs[path].Version != 6 {
			t.Fatalf("%s: v6 record unavailable: %v", harness, err)
		}
		rows, err = CacheRead(ctx, cfg, nil)
		if err != nil || len(rows) != 1 || rows[0].Presence.Ref != ref {
			t.Fatalf("%s: v6 cache read: %d %v", harness, len(rows), err)
		}
		if err := CacheFinalize(ctx, cfg, ref); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(oldPath)
		if err != nil || string(got) != string(old) {
			t.Fatalf("%s: v5 record changed: %v", harness, err)
		}
	}
}

func TestEmptyEnvironmentTokenDoesNotFallBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := WritePrivate(dir, map[string][]byte{"config": []byte(`{"inspectorUrl":"http://localhost:3000","tokenFile":"/tmp/secret"}`)}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COORDINATOR_API_TOKEN", "")
	if _, err := LoadConfig(filepath.Join(dir, "config")); err == nil {
		t.Fatal("empty token override accepted")
	}
}
