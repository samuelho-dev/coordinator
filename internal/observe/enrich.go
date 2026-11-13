package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

type targetReading struct {
	before, after wire.MachineResources
	valid         bool
}

func rootKey(r wire.ProcessIdentity) string { return fmt.Sprintf("%d:%s", r.PID, r.StartIdentity) }
func processMetric(unit string, v *float64, p wire.MetricProvenance, reason string) wire.Metric {
	if v != nil && *v >= 0 && !math.IsNaN(*v) && !math.IsInf(*v, 0) && (unit != "bytes" || (*v <= 9007199254740991 && math.Trunc(*v) == *v)) {
		return wire.NewMetric(unit, *v, p)
	}
	return wire.UnavailableMetric(unit, reason, p)
}
func sameScope(a, b *wire.MachineResources) bool {
	return a != nil && b != nil && a.Scope != "unknown" && b.Scope == a.Scope && a.ScopeID != nil && b.ScopeID != nil && *a.ScopeID == *b.ScopeID
}
func (s *Sampler) collect(ctx context.Context, presences []wire.SessionPresence) ([]wire.SessionPresence, ProcessSample, [][]ProcessRow, error) {
	roots := map[string]wire.ProcessIdentity{}
	claims := map[string]map[string]bool{}
	boundaries := map[string]bool{}
	for _, p := range presences {
		for _, r := range p.Processes {
			k := rootKey(r)
			boundaries[k] = true
			if claims[k] == nil {
				claims[k] = map[string]bool{}
			}
			claims[k][p.Ref.Key()] = true
			if old, ok := roots[k]; !ok {
				roots[k] = r
			} else if old.Ownership != r.Ownership {
				old.Ownership = "unknown"
				roots[k] = old
			}
		}
	}
	var list []wire.ProcessIdentity
	readings := map[string]targetReading{}
	for k, r := range roots {
		if len(claims[k]) > 1 {
			r.Ownership = "shared"
		}
		id, e := Identify(ctx, r.PID)
		if e != nil || id.StartIdentity != r.StartIdentity {
			continue
		}
		before := s.ReadMachine(ctx, r.PID)
		readings[k] = targetReading{before: before}
		list = append(list, r)
	}
	sample, err := s.Sample(ctx, list)
	if err != nil {
		return nil, sample, nil, err
	}
	for _, r := range list {
		k := rootKey(r)
		tr := readings[k]
		id, e := Identify(ctx, r.PID)
		if e == nil && id.StartIdentity == r.StartIdentity {
			tr.after = s.ReadMachine(ctx, r.PID)
			tr.valid = true
		}
		readings[k] = tr
	}
	rows := map[int]ProcessRow{}
	for _, r := range sample.Processes {
		rows[r.PID] = r
	}
	selected := make([][]ProcessRow, len(presences))
	out := append([]wire.SessionPresence(nil), presences...)
	for i, p := range out {
		own := map[string]bool{}
		shared := false
		for _, r := range p.Processes {
			own[rootKey(r)] = true
			if r.Ownership == "shared" || len(claims[rootKey(r)]) > 1 {
				shared = true
			}
		}
		for _, row := range sample.Processes {
			cur := row
			seen := map[int]bool{}
			for !seen[cur.PID] {
				seen[cur.PID] = true
				k := rootKey(wire.ProcessIdentity{PID: cur.PID, StartIdentity: cur.StartIdentity})
				if boundaries[k] {
					if own[k] {
						selected[i] = append(selected[i], row)
					}
					break
				}
				next, ok := rows[cur.PPID]
				if !ok {
					break
				}
				cur = next
			}
		}
		valid := len(p.Processes) > 0
		unknown := false
		for _, r := range p.Processes {
			row, ok := rows[r.PID]
			tr, matched := readings[rootKey(r)]
			if !ok || row.StartIdentity != r.StartIdentity || row.UnavailableReason != "" || !matched || !tr.valid {
				valid = false
			}
			if r.Ownership == "unknown" {
				unknown = true
			}
		}
		for _, r := range selected[i] {
			if r.Ownership == "unknown" {
				unknown = true
			}
			if r.Ownership == "shared" {
				shared = true
			}
		}
		attribution := "owned"
		if shared {
			attribution = "shared"
		} else if !valid || unknown {
			attribution = "unknown"
		}
		observed := sample.SampledAt
		if observed == 0 {
			observed = nowMS()
		}
		prov := wire.MetricProvenance{Source: "native-process-sample", Scope: "process", ScopeDescription: "Session root and descendants excluding separately identified session trees; RSS sums resident sets, not unique physical RAM", ObservedAt: &observed, Derived: true, Shared: shared, Partial: !valid || sample.Partial || sample.Truncated}
		if attribution == "owned" {
			prov.Scope = "owned-processes"
		}
		if sample.SampledAt >= sample.SampleStartedAt {
			prov.SampleWindow = &wire.SampleWindow{StartedAt: sample.SampleStartedAt, EndedAt: sample.SampledAt}
		}
		var cpu, rss *float64
		if valid {
			for _, r := range selected[i] {
				if r.CPUPercent != nil {
					if cpu == nil {
						cpu = ptr(0)
					}
					*cpu += *r.CPUPercent
				}
				if r.RSSBytes != nil {
					if rss == nil {
						rss = ptr(0)
					}
					*rss += *r.RSSBytes
				}
			}
		}
		var source *wire.MachineResources
		if p.Machine != nil {
			source = p.Machine.Resources
		}
		denominator := func(cpu bool) *float64 {
			if !valid || source == nil || source.ScopeID == nil || source.Scope == "unknown" {
				return nil
			}
			pick := func(m wire.MachineResources) wire.Metric {
				if cpu {
					return m.CPU.CapacityCores
				}
				return m.Memory.CapacityBytes
			}
			initial := pick(*source)
			if initial.Status != "available" || initial.Value == nil || *initial.Value <= 0 || initial.Provenance.Stale {
				return nil
			}
			for _, r := range p.Processes {
				tr, ok := readings[rootKey(r)]
				if !ok || !tr.valid || !sameScope(source, &tr.before) || !sameScope(source, &tr.after) {
					return nil
				}
				for _, m := range []wire.MachineResources{tr.before, tr.after} {
					v := pick(m)
					if v.Status != "available" || v.Value == nil || v.Provenance.Stale || *v.Value != *initial.Value {
						return nil
					}
				}
			}
			return initial.Value
		}
		var cp, mp *float64
		if d := denominator(true); cpu != nil && d != nil {
			cp = ptr(*cpu / *d)
		}
		if d := denominator(false); rss != nil && d != nil {
			mp = ptr(100 * *rss / *d)
		}
		var scope *string
		if source != nil {
			scope = source.ScopeID
		}
		res := wire.SessionResources{ObservedAt: observed, MachineScopeID: scope, Attribution: attribution, CPUPercent: processMetric("percent-one-core", cpu, prov, "Process observation unavailable or identity changed"), RSSBytes: processMetric("bytes", rss, prov, "Process observation unavailable or identity changed"), CPUCapacityPercent: processMetric("percent", cp, prov, "CPU capacity unavailable, stale, or changed"), RSSCapacityPercent: processMetric("percent", mp, prov, "Memory capacity unavailable, stale, or changed")}
		out[i].Resources = &res
		out[i].Processes = append([]wire.ProcessIdentity(nil), p.Processes...)
		for j := range out[i].Processes {
			if len(claims[rootKey(out[i].Processes[j])]) > 1 {
				out[i].Processes[j].Ownership = "shared"
			}
		}
	}
	return out, sample, selected, nil
}
func (s *Sampler) Enrich(ctx context.Context, presences []wire.SessionPresence) ([]wire.SessionPresence, error) {
	out, _, _, err := s.collect(ctx, presences)
	return out, err
}
func (s *Sampler) Inspect(ctx context.Context, in wire.SessionInspection) (wire.SessionInspection, error) {
	enriched, sample, selected, err := s.collect(ctx, []wire.SessionPresence{in.Presence})
	if err != nil {
		return in, err
	}
	in.Presence = enriched[0]
	rows := selected[0]
	at := sample.SampledAt
	shared := in.Presence.Resources.Attribution != "owned"
	p := wire.MetricProvenance{Source: "native-process-sample", Scope: "process", ScopeDescription: "Process sample; RSS sums resident sets, not unique physical RAM", ObservedAt: &at, Derived: true, Shared: shared, Partial: sample.Partial || sample.Truncated}
	if sample.SampledAt >= sample.SampleStartedAt {
		p.SampleWindow = &wire.SampleWindow{StartedAt: sample.SampleStartedAt, EndedAt: sample.SampledAt}
	}
	in.Resources.Processes = []wire.ProcessObservation{}
	var cpu, rss *float64
	for _, r := range rows {
		rp := p
		rp.SampleWindow = nil
		rp.ObservedAt = nil
		if r.SampledAt > 0 {
			rp.ObservedAt = &r.SampledAt
		}
		if r.SampleStartedAt > 0 && r.SampledAt > r.SampleStartedAt && r.WindowMs > 0 {
			rp.SampleWindow = &wire.SampleWindow{StartedAt: r.SampleStartedAt, EndedAt: r.SampledAt}
		}
		rp.Shared = r.Ownership == "shared"
		rp.Partial = rp.Partial || r.UnavailableReason != ""
		reason := r.UnavailableReason
		if reason == "" {
			reason = "Native process counter unavailable"
		}
		in.Resources.Processes = append(in.Resources.Processes, wire.ProcessObservation{Process: wire.ProcessIdentity{PID: r.PID, StartIdentity: r.StartIdentity, Ownership: r.Ownership}, CPUPercent: processMetric("percent-one-core", r.CPUPercent, rp, reason), CPUTimeMs: processMetric("milliseconds", r.CPUTimeMs, rp, reason), RSSBytes: processMetric("bytes", r.RSSBytes, rp, reason)})
		if r.CPUPercent != nil {
			if cpu == nil {
				cpu = ptr(0)
			}
			*cpu += *r.CPUPercent
		}
		if r.RSSBytes != nil {
			if rss == nil {
				rss = ptr(0)
			}
			*rss += *r.RSSBytes
		}
	}
	var count *float64
	if !shared && !sample.Partial && !sample.Truncated {
		count = ptr(float64(len(rows)))
	}
	if shared {
		cpu = nil
		rss = nil
	}
	p.Scope = "owned-processes"
	p.Shared = shared
	p.Partial = p.Partial || shared
	reason := "Process ownership is shared, unknown, or incomplete"
	in.Resources.OwnedProcessCount = processMetric("count", count, p, reason)
	in.Resources.OwnedCPUPercent = processMetric("percent-one-core", cpu, p, reason)
	in.Resources.OwnedRSSBytes = processMetric("bytes", rss, p, reason)
	for len(in.Resources.Processes) > 0 {
		b, e := json.Marshal(in)
		if e == nil && len(b) <= wire.InspectionLimit && len(in.Resources.Processes) <= 128 {
			break
		}
		in.Resources.Processes = in.Resources.Processes[:len(in.Resources.Processes)-1]
		in.Resources.OwnedProcessCount.Provenance.Partial = true
		in.Resources.OwnedCPUPercent.Provenance.Partial = true
		in.Resources.OwnedRSSBytes.Provenance.Partial = true
	}
	b, e := json.Marshal(in)
	if e != nil {
		return in, e
	}
	if len(b) > wire.InspectionLimit {
		return in, fmt.Errorf("inspection exceeds %d bytes without process rows", wire.InspectionLimit)
	}
	return in, nil
}
