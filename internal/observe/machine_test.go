package observe

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestMachineIntervalResets(t *testing.T) {
	at := time.Now()
	id := "scope"
	prev := machineBaseline{identity: id, topology: "a", usage: 10, at: at}
	n := machineNative{usage: ptr(20), topology: "a"}
	if machineInterval(n, &id, at.Add(time.Second), prev, true) == nil {
		t.Fatal("valid interval unavailable")
	}
	n.usage = ptr(9)
	if machineInterval(n, &id, at.Add(time.Second), prev, true) != nil {
		t.Fatal("counter reset accepted")
	}
	n.usage = ptr(20)
	n.topology = "b"
	if machineInterval(n, &id, at.Add(time.Second), prev, true) != nil {
		t.Fatal("topology change accepted")
	}
	n.topology = "a"
	other := "other"
	if machineInterval(n, &other, at.Add(time.Second), prev, true) != nil {
		t.Fatal("scope change accepted")
	}
	if machineInterval(n, nil, at.Add(time.Second), prev, true) != nil {
		t.Fatal("unknown scope accepted")
	}
}
func TestRawResourcesWithoutScope(t *testing.T) {
	ctx := context.Background()
	root, err := Identify(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	root.Ownership = "owned"
	s := NewSampler()
	p := wire.SessionPresence{Ref: wire.SessionRef{MachineID: "m", Harness: "omp", NativeSessionID: "s"}, Processes: []wire.ProcessIdentity{root}}
	result, err := s.Enrich(ctx, []wire.SessionPresence{p})
	if err != nil {
		t.Fatal(err)
	}
	r := result[0].Resources
	if r == nil || r.CPUPercent.Status != "available" || r.RSSBytes.Status != "available" {
		t.Fatalf("raw observations missing: %+v", r)
	}
	if r.CPUCapacityPercent.Status != "unavailable" || r.RSSCapacityPercent.Status != "unavailable" {
		t.Fatal("capacity without scope")
	}
}
func TestChangedDenominatorAndSharedInspection(t *testing.T) {
	ctx := context.Background()
	root, err := Identify(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	root.Ownership = "shared"
	s := NewSampler()
	machine := s.ReadMachine(ctx, root.PID)
	p := wire.SessionPresence{Ref: wire.SessionRef{MachineID: "m", Harness: "omp", NativeSessionID: "s"}, Processes: []wire.ProcessIdentity{root}, Machine: &wire.SessionMachine{Resources: &machine}}
	if machine.CPU.CapacityCores.Value != nil {
		changed := *machine.CPU.CapacityCores.Value + 1
		machine.CPU.CapacityCores.Value = &changed
	}
	if machine.Memory.CapacityBytes.Value != nil {
		changed := *machine.Memory.CapacityBytes.Value + 1
		machine.Memory.CapacityBytes.Value = &changed
	}
	in := wire.CreateInspection(p, nowMS())
	out, err := s.Inspect(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	r := out.Presence.Resources
	if r.CPUPercent.Status != "available" || r.RSSBytes.Status != "available" {
		t.Fatal("raw measurements lost")
	}
	if r.CPUCapacityPercent.Status != "unavailable" || r.RSSCapacityPercent.Status != "unavailable" {
		t.Fatal("changed denominator accepted")
	}
	if out.Resources.OwnedProcessCount.Status != "unavailable" || out.Resources.OwnedCPUPercent.Status != "unavailable" || out.Resources.OwnedRSSBytes.Status != "unavailable" {
		t.Fatal("shared processes claimed as owned")
	}
	var rootMetric *wire.Metric
	for i := range out.Resources.Processes {
		if out.Resources.Processes[i].Process.PID == root.PID {
			rootMetric = &out.Resources.Processes[i].CPUPercent
			break
		}
	}
	if rootMetric == nil || rootMetric.Status != "available" {
		t.Fatal("root CPU observation missing")
	}
	window := rootMetric.Provenance.SampleWindow
	if window == nil || window.StartedAt <= 0 || window.EndedAt <= window.StartedAt || rootMetric.Provenance.ObservedAt == nil || *rootMetric.Provenance.ObservedAt != window.EndedAt {
		t.Fatalf("invalid process sample provenance: %+v", rootMetric.Provenance)
	}
	batch := out.Resources.OwnedCPUPercent.Provenance.SampleWindow
	if batch == nil || window.StartedAt < batch.StartedAt || window.EndedAt > batch.EndedAt {
		t.Fatalf("process window %+v outside batch %+v", window, batch)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var restored wire.SessionInspection
	if err = json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	var restoredWindow *wire.SampleWindow
	for _, row := range restored.Resources.Processes {
		if row.Process.PID == root.PID {
			restoredWindow = row.CPUPercent.Provenance.SampleWindow
			break
		}
	}
	if restoredWindow == nil || *restoredWindow != *window {
		t.Fatalf("sample provenance lost in snapshot: %+v", restoredWindow)
	}
}
