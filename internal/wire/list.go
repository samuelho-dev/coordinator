package wire

import "encoding/json"

type ListMetric struct {
	Status    string   `json:"status"`
	Value     *float64 `json:"value"`
	Reason    string   `json:"reason,omitempty"`
	Estimated bool     `json:"estimated,omitempty"`
	Shared    bool     `json:"shared,omitempty"`
	Partial   bool     `json:"partial,omitempty"`
	Stale     bool     `json:"stale,omitempty"`
}
type ListMachineOS struct {
	Platform string `json:"platform"`
}
type ListMachineCPU struct {
	CapacityCores ListMetric `json:"capacityCores"`
	UsedPercent   ListMetric `json:"usedPercent"`
}
type ListMachineMemory struct {
	Accounting    string     `json:"accounting"`
	UsedBytes     ListMetric `json:"usedBytes"`
	CapacityBytes ListMetric `json:"capacityBytes"`
	UsedPercent   ListMetric `json:"usedPercent"`
}
type ListMachineResources struct {
	Scope      string            `json:"scope"`
	Source     string            `json:"source"`
	ObservedAt float64           `json:"observedAt"`
	CPU        ListMachineCPU    `json:"cpu"`
	Memory     ListMachineMemory `json:"memory"`
}
type ListMachine struct {
	MachineID string                `json:"machineId"`
	ScopeID   *string               `json:"scopeId"`
	Name      string                `json:"name,omitempty"`
	OS        *ListMachineOS        `json:"os,omitempty"`
	Resources *ListMachineResources `json:"resources,omitempty"`
}
type ListTerminal struct {
	Name string `json:"name"`
}
type ListActivity struct {
	State           string `json:"state"`
	Wait            string `json:"wait,omitempty"`
	Approval        string `json:"approval,omitempty"`
	CurrentToolName string `json:"currentToolName,omitempty"`
}
type ListSessionResources struct {
	ObservedAt         float64    `json:"observedAt"`
	MachineScopeID     *string    `json:"machineScopeId"`
	Attribution        string     `json:"attribution"`
	CPUCapacityPercent ListMetric `json:"cpuCapacityPercent"`
	RSSBytes           ListMetric `json:"rssBytes"`
	RSSCapacityPercent ListMetric `json:"rssCapacityPercent"`
}
type ListSession struct {
	Ref            SessionRef            `json:"ref"`
	ObservedAt     float64               `json:"observedAt"`
	Messaging      bool                  `json:"messaging,omitempty"`
	MachineScopeID *string               `json:"machineScopeId"`
	Agent          *SessionAgent         `json:"agent,omitempty"`
	Model          *SessionModel         `json:"model,omitempty"`
	Workspace      *SessionWorkspace     `json:"workspace,omitempty"`
	Connection     *SessionConnection    `json:"connection,omitempty"`
	StartedAt      *float64              `json:"startedAt,omitempty"`
	LastActivityAt *float64              `json:"lastActivityAt,omitempty"`
	Parent         *SessionRef           `json:"parent,omitempty"`
	Label          string                `json:"label,omitempty"`
	Terminal       *ListTerminal         `json:"terminal,omitempty"`
	ProgramStatus  *ProgramStatus        `json:"programStatus,omitempty"`
	Processes      []ProcessIdentity     `json:"processes,omitzero"`
	Resources      *ListSessionResources `json:"resources,omitempty"`
	Activity       *ListActivity         `json:"activity,omitempty"`
	nullable       map[string]bool
}

func listMetric(m Metric) ListMetric {
	return ListMetric{m.Status, m.Value, m.Reason, m.Provenance.Estimated, m.Provenance.Shared, m.Provenance.Partial, m.Provenance.Stale}
}
func ProjectListMachine(p SessionPresence) ListMachine {
	m := ListMachine{MachineID: p.Ref.MachineID}
	if p.Machine == nil {
		return m
	}
	m.Name = p.Machine.Name
	if p.Machine.OS != nil {
		m.OS = &ListMachineOS{p.Machine.OS.Platform}
	}
	if r := p.Machine.Resources; r != nil {
		m.ScopeID = r.ScopeID
		m.Resources = &ListMachineResources{r.Scope, r.Source, r.ObservedAt, ListMachineCPU{listMetric(r.CPU.CapacityCores), listMetric(r.CPU.UsedPercent)}, ListMachineMemory{r.Memory.UsedBytes.Provenance.ScopeDescription, listMetric(r.Memory.UsedBytes), listMetric(r.Memory.CapacityBytes), listMetric(r.Memory.UsedPercent)}}
	}
	return m
}
func ProjectListSession(p SessionPresence) ListSession {
	s := ListSession{Ref: p.Ref, ObservedAt: p.ObservedAt, Messaging: p.Messaging, Agent: p.Agent, Model: p.Model, Workspace: p.Workspace, Connection: p.Connection, StartedAt: p.StartedAt, LastActivityAt: p.LastActivityAt, Parent: p.Parent, Label: p.Label, ProgramStatus: p.ProgramStatus, Processes: p.Processes, nullable: p.nullable}
	if p.Machine != nil && p.Machine.Resources != nil {
		s.MachineScopeID = p.Machine.Resources.ScopeID
	}
	if p.Terminal != nil {
		s.Terminal = &ListTerminal{p.Terminal.Name}
	}
	if a := p.Activity; a != nil {
		s.Activity = &ListActivity{a.State, a.Wait, a.Approval, a.CurrentToolName}
	}
	if r := p.Resources; r != nil {
		s.Resources = &ListSessionResources{r.ObservedAt, r.MachineScopeID, r.Attribution, listMetric(r.CPUCapacityPercent), listMetric(r.RSSBytes), listMetric(r.RSSCapacityPercent)}
	}
	return s
}
func ListMachineKey(m ListMachine) string {
	b, _ := json.Marshal([]any{m.MachineID, m.ScopeID})
	return string(b)
}
func staleCount(r *ListMachineResources) int {
	n := 0
	for _, m := range []ListMetric{r.CPU.CapacityCores, r.CPU.UsedPercent, r.Memory.UsedBytes, r.Memory.CapacityBytes, r.Memory.UsedPercent} {
		if m.Stale {
			n++
		}
	}
	return n
}
func MergeListMachines(target map[string]ListMachine, machines []ListMachine) {
	for _, m := range machines {
		k := ListMachineKey(m)
		old, ok := target[k]
		if !ok || (m.Resources != nil && (old.Resources == nil || m.Resources.ObservedAt > old.Resources.ObservedAt || (m.Resources.ObservedAt == old.Resources.ObservedAt && staleCount(m.Resources) < staleCount(old.Resources)))) {
			target[k] = m
		}
	}
}
