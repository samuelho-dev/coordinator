package observe

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

type machineNative struct {
	scope, source, identity, topology, cpuDefinition, memoryDefinition string
	usage, total, capacity, used, memory, available                    *float64
	at                                                                 time.Time
	partial                                                            bool
}
type machineBaseline struct {
	identity, topology string
	usage              float64
	total              *float64
	at                 time.Time
	wall               float64
}
type Sampler struct {
	mu        sync.Mutex
	baselines map[int]machineBaseline
}

func NewSampler() *Sampler   { return &Sampler{baselines: make(map[int]machineBaseline)} }
func ptr(v float64) *float64 { return &v }
func blank(source string) machineNative {
	return machineNative{scope: "unknown", source: source, partial: true, cpuDefinition: "Native CPU counters unavailable", memoryDefinition: "Native memory counters unavailable"}
}
func (s *Sampler) ReadMachine(ctx context.Context, pid int) wire.MachineResources {
	if pid == 0 {
		pid = os.Getpid()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	before, err := Identify(ctx, pid)
	n := blank("target-identity-unavailable")
	if err == nil && before.StartIdentity != "" {
		n = readNativeMachine(ctx, pid)
		after, e := Identify(ctx, pid)
		if e != nil || after.StartIdentity != before.StartIdentity {
			n = blank("target-identity-changed")
		}
	}
	n.topology += ":target:" + before.StartIdentity
	at := n.at
	if at.IsZero() {
		at = time.Now()
	}
	wall := nowMS()
	var scope *string
	if n.identity != "" {
		h := sha256.Sum256([]byte(n.identity))
		id := fmt.Sprintf("machine:%x", h)
		scope = &id
	}
	prev, ok := s.baselines[pid]
	usage := machineInterval(n, scope, at, prev, ok)
	if s.baselines == nil {
		s.baselines = make(map[int]machineBaseline)
	}
	if scope != nil && n.usage != nil {
		s.baselines[pid] = machineBaseline{*scope, n.topology, *n.usage, n.total, at, wall}
	} else {
		delete(s.baselines, pid)
	}
	provenance := func(desc string, derived bool) wire.MetricProvenance {
		return wire.MetricProvenance{Source: n.source, Scope: n.scope, ScopeDescription: desc, ObservedAt: &wall, NativeUpdatedAt: &wall, Derived: derived, Shared: true, Partial: n.partial}
	}
	value := func(unit string, v *float64, desc string, derived bool) wire.Metric {
		p := provenance(desc, derived)
		if v != nil && !math.IsNaN(*v) && !math.IsInf(*v, 0) && *v >= 0 && (unit != "bytes" || (*v <= 9007199254740991 && math.Trunc(*v) == *v)) {
			return wire.NewMetric(unit, *v, p)
		}
		return wire.UnavailableMetric(unit, "Native measurement unavailable or no defined value for this scope", p)
	}
	var cpuPct, memPct *float64
	if usage != nil && n.capacity != nil && *n.capacity > 0 {
		cpuPct = ptr(*usage / *n.capacity)
	}
	if n.used != nil && n.memory != nil && *n.memory > 0 {
		memPct = ptr(*n.used / *n.memory * 100)
	}
	result := wire.MachineResources{Scope: n.scope, ScopeID: scope, Source: n.source, ObservedAt: wall, CPU: wire.MachineCPU{UsagePercent: value("percent-one-core", usage, n.cpuDefinition, true), CapacityCores: value("cores", n.capacity, "Visible logical CPU/cpuset/quota bound; hidden ancestors unknown", false), UsedPercent: value("percent", cpuPct, n.cpuDefinition, true)}, Memory: wire.MachineMemory{UsedBytes: value("bytes", n.used, n.memoryDefinition, true), CapacityBytes: value("bytes", n.memory, n.memoryDefinition, false), AvailableBytes: value("bytes", n.available, "Native available memory", false), UsedPercent: value("percent", memPct, n.memoryDefinition, true)}}
	if usage != nil && wall >= prev.wall {
		w := &wire.SampleWindow{StartedAt: prev.wall, EndedAt: wall}
		result.CPU.UsagePercent.Provenance.SampleWindow = w
		result.CPU.UsedPercent.Provenance.SampleWindow = w
	}
	return result
}

func machineInterval(n machineNative, scope *string, at time.Time, prev machineBaseline, ok bool) *float64 {
	if scope == nil || n.usage == nil || !ok || prev.identity != *scope || prev.topology != n.topology || !at.After(prev.at) || *n.usage < prev.usage {
		return nil
	}
	delta := *n.usage - prev.usage
	if n.total != nil && prev.total != nil && *n.total > *prev.total && n.capacity != nil && delta <= *n.total-*prev.total {
		return ptr(delta / (*n.total - *prev.total) * *n.capacity * 100)
	}
	if n.total == nil && prev.total == nil {
		return ptr(delta / float64(at.Sub(prev.at)) * 100)
	}
	return nil
}
