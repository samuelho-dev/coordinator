package observe

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestSampleIdentityAndSharedOwnership(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("native process observation unavailable")
	}
	ctx := context.Background()
	root, err := Identify(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	root.Ownership = "owned"
	sample, err := NewSampler().Sample(ctx, []wire.ProcessIdentity{root, root})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range sample.Processes {
		if row.PID == root.PID && row.StartIdentity == root.StartIdentity {
			if row.Ownership != "shared" || row.RSSBytes == nil {
				t.Fatalf("shared root: %+v", row)
			}
			goto invalid
		}
	}
	t.Fatal("root missing")
invalid:
	root.StartIdentity += "-reused"
	sample, err = NewSampler().Sample(ctx, []wire.ProcessIdentity{root})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range sample.Processes {
		if row.Ownership == "owned" {
			t.Fatalf("invalid identity charged: %+v", row)
		}
	}
}
func TestDarwinElapsedCounter(t *testing.T) {
	value, err := parsePSTime("1-02:03:04.50")
	if err != nil || value != 93784500 {
		t.Fatalf("%v %v", value, err)
	}
}
func TestLimitedBufferPreservesPrefix(t *testing.T) {
	b := &limitedBuffer{limit: 5}
	n, err := b.Write([]byte("abcdef"))
	if err != nil || n != 6 || !b.truncated || b.String() != "abcde" {
		t.Fatalf("write: %d %v %v %q", n, err, b.truncated, b.String())
	}
}
func TestDarwinCappedRows(t *testing.T) {
	first := "10 1 Mon Jan  2 03:04:05 2006 00:01.00 42\n"
	second := "20 10 Mon Jan  2 03:04:06 2006 00:02.00 84\n"
	b := &limitedBuffer{limit: len(first) + len(second)/2}
	for _, part := range []string{first, second} {
		if n, err := b.Write([]byte(part)); err != nil || n != len(part) {
			t.Fatalf("write: %d %v", n, err)
		}
	}
	at := time.Unix(100, 0)
	rows, err := parseDarwinSnapshot(b.Bytes(), b.truncated, at)
	if err != nil || !b.truncated || len(rows) != 1 {
		t.Fatalf("truncated rows: %+v, %v", rows, err)
	}
	c := rows[10]
	if c.birth != "darwin:Mon Jan 2 03:04:05 2006" || c.cpu != 1000 || c.rss != 42*1024 || !c.at.Equal(at) {
		t.Fatalf("counter: %+v", c)
	}
	rows, err = parseDarwinSnapshot([]byte(first+second), false, at)
	if err != nil || len(rows) != 2 {
		t.Fatalf("complete rows: %+v, %v", rows, err)
	}
}

func TestAttributionCompetingIdentityAndIncompleteTree(t *testing.T) {
	now := time.Unix(100, 0)
	a := map[int]counter{
		10: {pid: 10, ppid: 1, birth: "parent", cpu: 10, rss: 100, at: now},
		20: {pid: 20, ppid: 10, birth: "current", cpu: 10, rss: 100, at: now},
		30: {pid: 30, ppid: 20, birth: "grandchild", cpu: 10, rss: 100, at: now},
		40: {pid: 40, ppid: 10, birth: "exited", cpu: 10, rss: 100, at: now},
	}
	b := map[int]counter{}
	for pid, c := range a {
		if pid != 40 {
			c.cpu += 10
			c.at = now.Add(time.Second)
			b[pid] = c
		}
	}
	b[50] = counter{pid: 50, ppid: 10, birth: "new", cpu: 10, rss: 100, at: now.Add(time.Second)}
	roots := []wire.ProcessIdentity{{PID: 10, StartIdentity: "parent", Ownership: "owned"}, {PID: 20, StartIdentity: "old", Ownership: "owned"}}
	valid := map[processKey]bool{{10, "parent"}: true, {20, "current"}: true}
	rows, partial := attributeProcesses(roots, valid, a, b)
	if !partial {
		t.Fatal("expected partial sample")
	}
	find := func(pid int, birth string) ProcessRow {
		for _, row := range rows {
			if row.PID == pid && row.StartIdentity == birth {
				return row
			}
		}
		t.Fatalf("missing %d %s", pid, birth)
		return ProcessRow{}
	}
	for _, pid := range []int{10, 20, 30} {
		birth := map[int]string{10: "parent", 20: "current", 30: "grandchild"}[pid]
		row := find(pid, birth)
		if row.Ownership != "owned" || row.RSSBytes == nil || row.SampleStartedAt != 100000 || row.SampledAt != 101000 || row.WindowMs != 1000 {
			t.Fatalf("stable process %d: %+v", pid, row)
		}
	}
	for _, item := range []struct {
		pid   int
		birth string
	}{{20, "old"}, {40, "exited"}, {50, "new"}} {
		row := find(item.pid, item.birth)
		if row.Ownership != "unknown" || row.RSSBytes != nil {
			t.Fatalf("incomplete process: %+v", row)
		}
	}
	roots = append(roots, wire.ProcessIdentity{PID: 20, StartIdentity: "current", Ownership: "owned"})
	rows, _ = attributeProcesses(roots, valid, a, b)
	if row := find(30, "grandchild"); row.Ownership != "shared" {
		t.Fatalf("overlapping roots: %+v", row)
	}
	roots = append(roots, wire.ProcessIdentity{PID: 10, StartIdentity: "parent", Ownership: "owned"})
	rows, _ = attributeProcesses(roots, valid, a, b)
	if row := find(30, "grandchild"); row.Ownership != "shared" {
		t.Fatalf("shared ancestor: %+v", row)
	}
	changed := b[20]
	changed.birth = "reused"
	b[20] = changed
	rows, _ = attributeProcesses(roots, valid, a, b)
	if row := find(30, "grandchild"); row.Ownership != "unknown" || row.RSSBytes != nil {
		t.Fatalf("unstable ancestor: %+v", row)
	}
}
