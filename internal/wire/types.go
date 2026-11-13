package wire

import (
	"bytes"
	"encoding/json"
	"strings"
)

const InspectionLimit = 48 * 1024

type SessionRef struct {
	MachineID       string `json:"machineId"`
	Harness         string `json:"harness"`
	NativeSessionID string `json:"nativeSessionId"`
	NativeAgentID   string `json:"nativeAgentId,omitempty"`
}

func (r SessionRef) Key() string {
	var agent any
	if r.NativeAgentID != "" {
		agent = r.NativeAgentID
	}
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode([]any{r.MachineID, r.Harness, r.NativeSessionID, agent})
	s := strings.TrimSuffix(b.String(), "\n")
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '\\' {
			start := i
			for i < len(s) && s[i] == '\\' {
				i++
			}
			count := i - start
			if count%2 == 1 && i+5 <= len(s) && (s[i:i+5] == "u2028" || s[i:i+5] == "u2029") {
				out.WriteString(s[start : i-1])
				if s[i:i+5] == "u2028" {
					out.WriteRune('\u2028')
				} else {
					out.WriteRune('\u2029')
				}
				i += 5
			} else {
				out.WriteString(s[start:i])
			}
		} else {
			out.WriteByte(s[i])
			i++
		}
	}
	return out.String()
}

type ProcessIdentity struct {
	PID           int    `json:"pid"`
	StartIdentity string `json:"startIdentity"`
	Ownership     string `json:"ownership"`
}
type ProgramStatus struct {
	State string `json:"state"`
	Kind  string `json:"kind,omitempty"`
}
type SampleWindow struct {
	StartedAt float64 `json:"startedAt"`
	EndedAt   float64 `json:"endedAt"`
}
type MetricProvenance struct {
	Source           string        `json:"source"`
	Scope            string        `json:"scope"`
	ScopeDescription string        `json:"scopeDescription"`
	ObservedAt       *float64      `json:"observedAt"`
	NativeUpdatedAt  *float64      `json:"nativeUpdatedAt"`
	Estimated        bool          `json:"estimated"`
	Derived          bool          `json:"derived"`
	Shared           bool          `json:"shared"`
	Partial          bool          `json:"partial"`
	Stale            bool          `json:"stale"`
	SampleWindow     *SampleWindow `json:"sampleWindow,omitempty"`
}
type Metric struct {
	Status     string           `json:"status"`
	Value      *float64         `json:"value"`
	Unit       string           `json:"unit"`
	Reason     string           `json:"reason,omitempty"`
	Provenance MetricProvenance `json:"provenance"`
}
type MachineCPU struct {
	UsagePercent  Metric `json:"usagePercent"`
	CapacityCores Metric `json:"capacityCores"`
	UsedPercent   Metric `json:"usedPercent"`
}
type MachineMemory struct {
	UsedBytes      Metric `json:"usedBytes"`
	CapacityBytes  Metric `json:"capacityBytes"`
	AvailableBytes Metric `json:"availableBytes"`
	UsedPercent    Metric `json:"usedPercent"`
}
type MachineResources struct {
	Scope      string        `json:"scope"`
	ScopeID    *string       `json:"scopeId"`
	Source     string        `json:"source"`
	ObservedAt float64       `json:"observedAt"`
	CPU        MachineCPU    `json:"cpu"`
	Memory     MachineMemory `json:"memory"`
}
type SessionResources struct {
	ObservedAt         float64 `json:"observedAt"`
	MachineScopeID     *string `json:"machineScopeId"`
	Attribution        string  `json:"attribution"`
	CPUPercent         Metric  `json:"cpuPercent"`
	CPUCapacityPercent Metric  `json:"cpuCapacityPercent"`
	RSSBytes           Metric  `json:"rssBytes"`
	RSSCapacityPercent Metric  `json:"rssCapacityPercent"`
}
type MachineOS struct {
	Platform     string `json:"platform"`
	Release      string `json:"release,omitempty"`
	Architecture string `json:"architecture,omitempty"`
}
type SessionMachine struct {
	Name      string            `json:"name,omitempty"`
	Resources *MachineResources `json:"resources,omitempty"`
	OS        *MachineOS        `json:"os,omitempty"`
}
type SessionAgent struct {
	Kind    string `json:"kind,omitempty"`
	Name    string `json:"name,omitempty"`
	Profile string `json:"profile,omitempty"`
}
type SessionModel struct {
	Provider string `json:"provider,omitempty"`
	ID       string `json:"id,omitempty"`
}
type SessionProject struct {
	Root   string `json:"root"`
	Source string `json:"source"`
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
}
type SessionGit struct {
	State        string  `json:"state"`
	ObservedAt   float64 `json:"observedAt"`
	WorktreeRoot string  `json:"worktreeRoot,omitempty"`
	CommonDir    string  `json:"commonDir,omitempty"`
	Branch       *string `json:"branch,omitempty"`
	Head         *string `json:"head,omitempty"`
}

func (g SessionGit) MarshalJSON() ([]byte, error) {
	if g.State == "available" {
		return json.Marshal(struct {
			State        string  `json:"state"`
			ObservedAt   float64 `json:"observedAt"`
			WorktreeRoot string  `json:"worktreeRoot"`
			CommonDir    string  `json:"commonDir"`
			Branch       *string `json:"branch"`
			Head         *string `json:"head"`
		}{g.State, g.ObservedAt, g.WorktreeRoot, g.CommonDir, g.Branch, g.Head})
	}
	return json.Marshal(struct {
		State      string  `json:"state"`
		ObservedAt float64 `json:"observedAt"`
	}{g.State, g.ObservedAt})
}

type SessionWorkspace struct {
	CWD     string          `json:"cwd"`
	Git     *SessionGit     `json:"git,omitempty"`
	Project *SessionProject `json:"project,omitempty"`
}
type SessionTerminal struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}
type SessionConnection struct {
	State      string  `json:"state"`
	ReceivedAt float64 `json:"receivedAt"`
	LiveUntil  float64 `json:"liveUntil"`
}
type SessionActivity struct {
	State           string `json:"state"`
	Wait            string `json:"wait,omitempty"`
	Approval        string `json:"approval,omitempty"`
	Streaming       *bool  `json:"streaming,omitempty"`
	Compacting      *bool  `json:"compacting,omitempty"`
	CurrentToolName string `json:"currentToolName,omitempty"`
}
type SessionPresence struct {
	Ref             SessionRef         `json:"ref"`
	Messaging       bool               `json:"messaging,omitempty"`
	ObservedAt      float64            `json:"observedAt"`
	HarnessVersion  string             `json:"harnessVersion,omitempty"`
	Machine         *SessionMachine    `json:"machine,omitempty"`
	Agent           *SessionAgent      `json:"agent,omitempty"`
	Model           *SessionModel      `json:"model,omitempty"`
	Workspace       *SessionWorkspace  `json:"workspace,omitempty"`
	Connection      *SessionConnection `json:"connection,omitempty"`
	NativeUpdatedAt *float64           `json:"nativeUpdatedAt,omitempty"`
	StartedAt       *float64           `json:"startedAt,omitempty"`
	LastActivityAt  *float64           `json:"lastActivityAt,omitempty"`
	Parent          *SessionRef        `json:"parent,omitempty"`
	Label           string             `json:"label,omitempty"`
	Terminal        *SessionTerminal   `json:"terminal,omitempty"`
	ProgramStatus   *ProgramStatus     `json:"programStatus,omitempty"`
	Processes       []ProcessIdentity  `json:"processes,omitzero"`
	Resources       *SessionResources  `json:"resources,omitempty"`
	Activity        *SessionActivity   `json:"activity,omitempty"`
	nullable        map[string]bool
}
type ProcessObservation struct {
	Process    ProcessIdentity `json:"process"`
	CPUPercent Metric          `json:"cpuPercent"`
	CPUTimeMs  Metric          `json:"cpuTimeMs"`
	RSSBytes   Metric          `json:"rssBytes"`
}
type ResourceObservation struct {
	Processes         []ProcessObservation `json:"processes"`
	OwnedProcessCount Metric               `json:"ownedProcessCount"`
	OwnedCPUPercent   Metric               `json:"ownedCpuPercent"`
	OwnedRSSBytes     Metric               `json:"ownedRssBytes"`
}
type ActivityObservation struct {
	PendingAsyncWorkCount Metric `json:"pendingAsyncWorkCount"`
	ActiveAsyncJobCount   Metric `json:"activeAsyncJobCount"`
}
type SessionInspection struct {
	Presence  SessionPresence     `json:"presence"`
	QueriedAt float64             `json:"queriedAt"`
	Resources ResourceObservation `json:"resources"`
	Activity  ActivityObservation `json:"activity"`
}
type InspectorPrincipal struct {
	ID        string `json:"id"`
	Owner     string `json:"owner"`
	TokenHash string `json:"tokenHash"`
	Messaging bool   `json:"messaging,omitempty"`
}
type InspectorConfig struct {
	Host       string               `json:"host"`
	Port       int                  `json:"port"`
	Principals []InspectorPrincipal `json:"principals"`
}
type ConnectorConfig struct {
	InspectorURL string             `json:"inspectorUrl"`
	APIToken     string             `json:"apiToken,omitempty"`
	TokenFile    string             `json:"tokenFile,omitempty"`
	Messaging    bool               `json:"messaging,omitempty"`
	Project      *ConfiguredProject `json:"project,omitempty"`
}
type ConfiguredProject struct {
	Root string `json:"root"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}
type ConnectorAnnounce struct {
	ConnectorID string            `json:"connectorId"`
	Incarnation string            `json:"incarnation"`
	MachineID   string            `json:"machineId"`
	Receivers   []SessionRef      `json:"receivers,omitempty"`
	Sessions    []SessionPresence `json:"sessions"`
}
type ConnectorAccepted struct {
	Accepted bool `json:"accepted"`
}
type ConnectorPoll struct {
	ConnectorID string `json:"connectorId"`
	Incarnation string `json:"incarnation"`
}
type ConnectorDisconnect = ConnectorPoll
type InspectionRequest struct {
	RequestID string     `json:"requestId"`
	Ref       SessionRef `json:"ref"`
}
type ConnectorPollResult struct {
	Requests []InspectionRequest `json:"requests"`
	Messages []Message           `json:"messages,omitempty"`
}
type ObservationError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ConnectorResult struct {
	ConnectorID string          `json:"connectorId"`
	Incarnation string          `json:"incarnation"`
	RequestID   string          `json:"requestId"`
	Observation json.RawMessage `json:"observation"`
}
type SessionListInput struct {
	Cursor     string `json:"cursor,omitempty"`
	Limit      int    `json:"limit"`
	MachineID  string `json:"machineId,omitempty"`
	Repository string `json:"repository,omitempty"`
}
type SessionListOutput struct {
	Machines   []ListMachine `json:"machines"`
	Sessions   []ListSession `json:"sessions"`
	NextCursor *string       `json:"nextCursor"`
}
type SessionInspectInput struct {
	Ref SessionRef `json:"ref"`
}

type MessageCaller struct {
	ConnectorID string     `json:"connectorId"`
	Incarnation string     `json:"incarnation"`
	Ref         SessionRef `json:"ref"`
}
type MessageSendParams struct {
	To        SessionRef `json:"to"`
	Text      string     `json:"text"`
	RequestID string     `json:"requestId,omitempty"`
}
type MessageReplyParams struct {
	ReplyID   string `json:"replyId"`
	Text      string `json:"text"`
	RequestID string `json:"requestId,omitempty"`
}
type MessageSendInput struct {
	To        SessionRef     `json:"to"`
	Text      string         `json:"text"`
	RequestID string         `json:"requestId,omitempty"`
	Caller    *MessageCaller `json:"caller,omitempty"`
}
type MessageReplyInput struct {
	ReplyID   string         `json:"replyId"`
	Text      string         `json:"text"`
	RequestID string         `json:"requestId,omitempty"`
	Caller    *MessageCaller `json:"caller,omitempty"`
}
type MessageGetInput struct {
	MessageID string `json:"messageId"`
}
type Message struct {
	ID        string      `json:"id"`
	Sender    string      `json:"sender"`
	From      *SessionRef `json:"from,omitempty"`
	To        *SessionRef `json:"to,omitempty"`
	Text      string      `json:"text"`
	InReplyTo string      `json:"inReplyTo,omitempty"`
	CreatedAt float64     `json:"createdAt"`
	ExpiresAt float64     `json:"expiresAt"`
	Status    string      `json:"status"`
}
type MessageRecord struct {
	Message Message   `json:"message"`
	Replies []Message `json:"replies"`
}
type ConnectorMessageAck struct {
	ConnectorID string `json:"connectorId"`
	Incarnation string `json:"incarnation"`
	MessageID   string `json:"messageId"`
	Status      string `json:"status"`
}
