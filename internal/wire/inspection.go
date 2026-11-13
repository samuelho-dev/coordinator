package wire

func NewMetric(unit string, value float64, p MetricProvenance) Metric {
	return Metric{Status: "available", Value: &value, Unit: unit, Provenance: p}
}
func UnavailableMetric(unit, reason string, p MetricProvenance) Metric {
	return Metric{Status: "unavailable", Unit: unit, Reason: reason, Provenance: p}
}
func emptyMetric(unit string) Metric {
	return UnavailableMetric(unit, "Not provided by the native session", MetricProvenance{Source: "unavailable", Scope: "unknown", ScopeDescription: "No native measurement available"})
}
func CreateInspection(p SessionPresence, at float64) SessionInspection {
	count := emptyMetric("count")
	cpu := emptyMetric("percent-one-core")
	byt := emptyMetric("bytes")
	return SessionInspection{Presence: p, QueriedAt: at, Resources: ResourceObservation{[]ProcessObservation{}, count, cpu, byt}, Activity: ActivityObservation{count, count}}
}
func stale(m Metric) Metric { m.Provenance.Stale = true; return m }
func StaleMachineResources(r MachineResources) MachineResources {
	r.CPU.UsagePercent = stale(r.CPU.UsagePercent)
	r.CPU.CapacityCores = stale(r.CPU.CapacityCores)
	r.CPU.UsedPercent = stale(r.CPU.UsedPercent)
	r.Memory.UsedBytes = stale(r.Memory.UsedBytes)
	r.Memory.CapacityBytes = stale(r.Memory.CapacityBytes)
	r.Memory.AvailableBytes = stale(r.Memory.AvailableBytes)
	r.Memory.UsedPercent = stale(r.Memory.UsedPercent)
	return r
}
func StaleSessionResources(r SessionResources) SessionResources {
	r.CPUPercent = stale(r.CPUPercent)
	r.CPUCapacityPercent = stale(r.CPUCapacityPercent)
	r.RSSBytes = stale(r.RSSBytes)
	r.RSSCapacityPercent = stale(r.RSSCapacityPercent)
	return r
}
