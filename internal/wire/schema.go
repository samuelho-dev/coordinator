package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"unicode"

	"github.com/google/jsonschema-go/jsonschema"
)

var schemaTypes = map[string]reflect.Type{
	"SessionRef": reflect.TypeFor[SessionRef](), "ProcessIdentity": reflect.TypeFor[ProcessIdentity](), "Metric": reflect.TypeFor[Metric](), "MetricProvenance": reflect.TypeFor[MetricProvenance](), "MachineResources": reflect.TypeFor[MachineResources](), "SessionResources": reflect.TypeFor[SessionResources](), "SessionPresence": reflect.TypeFor[SessionPresence](), "SessionInspection": reflect.TypeFor[SessionInspection](), "InspectorConfig": reflect.TypeFor[InspectorConfig](), "ConnectorConfig": reflect.TypeFor[ConnectorConfig](), "ConnectorAnnounce": reflect.TypeFor[ConnectorAnnounce](), "ConnectorAccepted": reflect.TypeFor[ConnectorAccepted](), "ConnectorPoll": reflect.TypeFor[ConnectorPoll](), "ConnectorDisconnect": reflect.TypeFor[ConnectorDisconnect](), "ConnectorPollResult": reflect.TypeFor[ConnectorPollResult](), "ConnectorResult": reflect.TypeFor[ConnectorResult](), "SessionListInput": reflect.TypeFor[SessionListInput](), "SessionListOutput": reflect.TypeFor[SessionListOutput](), "SessionInspectInput": reflect.TypeFor[SessionInspectInput](), "ObservationError": reflect.TypeFor[ObservationError](), "ListMachine": reflect.TypeFor[ListMachine](), "ListSession": reflect.TypeFor[ListSession](), "NativeSnapshot": reflect.TypeFor[NativeSnapshot](), "NativeEvent": reflect.TypeFor[NativeEvent](), "NativeMessage": reflect.TypeFor[NativeMessage](), "NativeNotice": reflect.TypeFor[NativeNotice](),
	"InspectorPrincipal": reflect.TypeFor[InspectorPrincipal](), "MessageCaller": reflect.TypeFor[MessageCaller](), "MessageSendParams": reflect.TypeFor[MessageSendParams](), "MessageReplyParams": reflect.TypeFor[MessageReplyParams](), "MessageSendInput": reflect.TypeFor[MessageSendInput](), "MessageReplyInput": reflect.TypeFor[MessageReplyInput](), "MessageGetInput": reflect.TypeFor[MessageGetInput](), "Message": reflect.TypeFor[Message](), "MessageRecord": reflect.TypeFor[MessageRecord](), "ConnectorMessageAck": reflect.TypeFor[ConnectorMessageAck](),
}

func f(v float64) *float64 { return &v }
func n(v int) *int         { return &v }
func enum(v ...string) []any {
	a := make([]any, len(v))
	for i := range v {
		a[i] = v[i]
	}
	return a
}
func constrain(s *jsonschema.Schema, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		if s.Items != nil {
			constrain(s.Items, t.Elem())
		}
		return
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := range t.NumField() {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		p := s.Properties[name]
		if p == nil {
			continue
		}
		ft := field.Type
		optional := strings.Contains(field.Tag.Get("json"), "omitempty")
		if optional && ft.Kind() == reflect.Pointer && name != "startedAt" && name != "lastActivityAt" && name != "nativeUpdatedAt" && !(t.Name() == "SessionGit" && (name == "branch" || name == "head")) {
			if len(p.Types) > 0 {
				p.Type = p.Types[len(p.Types)-1]
				p.Types = nil
			}
		}
		constrain(p, ft)
		if p.Type == "string" || (len(p.Types) > 0 && (ft.Kind() == reflect.Pointer && ft.Elem().Kind() == reflect.String)) {
			p.MinLength = n(1)
			p.MaxLength = n(256)
			switch name {
			case "name", "source", "scopeDescription", "reason", "message", "accounting":
				p.MaxLength = n(512)
			case "cwd", "root", "worktreeRoot", "commonDir", "tokenFile", "inspectorUrl", "repository", "version":
				p.MaxLength = n(4096)
			}
		}
		switch name {
		case "observedAt", "nativeUpdatedAt", "startedAt", "lastActivityAt", "receivedAt", "liveUntil", "queriedAt", "endedAt", "createdAt", "expiresAt":
			p.Minimum = f(0)
		case "pid":
			p.Minimum = f(1)
			p.Maximum = f(2147483647)
		case "limit":
			p.Minimum = f(1)
			p.Maximum = f(100)
		case "port":
			p.Minimum = f(0)
			p.Maximum = f(65535)
		case "tokenHash":
			p.Pattern = "^[a-f0-9]{64}$"
		case "apiToken":
			p.MinLength = n(16)
			p.MaxLength = n(4096)
			p.Pattern = "^[A-Za-z0-9._~+/-]+=*$"
		case "head":
			p.Pattern = "^([a-f0-9]{40}|[a-f0-9]{64})$"
		case "harness":
			if t.Name() == "NativeSnapshot" {
				p.Enum = enum("pi", "omp")
			} else {
				p.Enum = enum("pi", "omp", "hermes", "codex", "claude")
			}
		case "ownership", "attribution":
			p.Enum = enum("owned", "shared", "unknown")
		case "platform":
			p.Enum = enum("linux", "darwin")
		case "scope":
			if t.Name() == "MetricProvenance" {
				p.Enum = enum("host", "cgroup", "process", "owned-processes", "native-aggregate", "sample-window", "unknown")
			} else {
				p.Enum = enum("host", "cgroup", "unknown")
			}
		case "source":
			if t.Name() == "SessionProject" {
				p.Enum = enum("native", "configured")
			}
		case "wait":
			p.Enum = enum("api", "tool", "user", "approval", "unknown")
		case "approval":
			p.Enum = enum("pending", "approved", "denied", "unknown")
		case "state":
			switch t.Name() {
			case "ProgramStatus":
				p.Enum = enum("idle", "working", "done", "error", "blocked")
			case "SessionGit":
				p.Enum = enum("available", "not-repository", "unavailable")
			case "SessionConnection":
				p.Enum = enum("live", "stale")
			case "SessionActivity", "ListActivity":
				p.Enum = enum("idle", "working", "waiting", "stopped", "unknown")
			case "NativeNotice":
				p.Enum = enum("unavailable", "recovered", "error")
			}
		case "kind":
			if t.Name() == "ProgramStatus" {
				p.Enum = enum("permission", "question", "auth")
			}
		case "code":
			if t.Name() == "ObservationError" {
				p.Enum = enum("unavailable", "not-found", "timeout", "disconnected", "invalid-observation")
			}
		case "sessions":
			if t.Name() == "ConnectorAnnounce" {
				p.MaxItems = n(256)
			} else {
				p.MaxItems = n(100)
			}
		case "machines":
			p.MaxItems = n(100)
		case "processes":
			p.MaxItems = n(128)
		case "principals":
			p.MaxItems = n(256)
		case "requests":
			p.MaxItems = n(32)
		case "messages", "replies":
			p.MaxItems = n(32)
		case "receivers":
			p.MaxItems = n(256)
		}
		if ft.Kind() == reflect.Slice && ft != reflect.TypeFor[json.RawMessage]() {
			p.Type = "array"
			p.Types = nil
		}
		if ft == reflect.TypeFor[Metric]() {
			if unit := metricUnit(t.Name(), name); unit != "" {
				p.Properties["unit"].Enum = enum(unit)
				p.OneOf[0].Properties["value"].Minimum = f(0)
				if unit == "bytes" || unit == "count" {
					p.OneOf[0].Properties["value"].Type = "integer"
					p.OneOf[0].Properties["value"].Maximum = f(9007199254740991)
				}
			}
		}
		if ft == reflect.TypeFor[ListMetric]() {
			if name != "unused" {
				p.OneOf[0].Properties["value"].Minimum = f(0)
			}
			if name == "usedBytes" || name == "capacityBytes" || name == "rssBytes" {
				p.OneOf[0].Properties["value"].Type = "integer"
				p.OneOf[0].Properties["value"].Maximum = f(9007199254740991)
			}
		}
		if name == "label" {
			p.MaxLength = n(512)
		}
		if (t.Name() == "SessionTerminal" || t.Name() == "ListTerminal") && (name == "name" || name == "version") {
			p.MaxLength = n(4096)
			p.Pattern = "^[^\\x00-\\x1f\\x7f-\\x9f]*[^\\s\\x00-\\x1f\\x7f-\\x9f][^\\x00-\\x1f\\x7f-\\x9f]*$"
		}
		if t.Name() == "SessionAgent" && name == "name" {
			p.MaxLength = n(256)
		}
		if t.Name() == "SessionGit" && name == "head" {
			p.MinLength = nil
			p.MaxLength = nil
			p.AnyOf = []*jsonschema.Schema{{Type: "null"}, {Type: "string", MinLength: n(40), MaxLength: n(40)}, {Type: "string", MinLength: n(64), MaxLength: n(64)}}
		}
		if t.Name() == "ObservationError" && name == "message" {
			p.Enum = enum("Native session observation unavailable")
		}
		switch t.Name() {
		case "Message", "MessageSendParams", "MessageReplyParams", "MessageSendInput", "MessageReplyInput", "MessageGetInput", "MessageCaller", "ConnectorMessageAck":
			switch name {
			case "id", "messageId", "replyId", "inReplyTo", "requestId", "connectorId", "incarnation":
				p.MaxLength = n(128)
				p.Pattern = "^[^\\s\\x00-\\x1f\\x7f-\\x9f]+$"
			case "text":
				p.MaxLength = n(8192)
				p.Pattern = "^[^\\x00-\\x08\\x0b-\\x1f\\x7f-\\x9f]*[^\\s\\x00-\\x1f\\x7f-\\x9f][^\\x00-\\x08\\x0b-\\x1f\\x7f-\\x9f]*$"
			case "status":
				if t.Name() == "Message" {
					p.Enum = enum("queued", "dispatched", "accepted", "refused", "expired", "unknown")
				} else if t.Name() == "ConnectorMessageAck" {
					p.Enum = enum("accepted", "refused", "unknown")
				}
			}
		}
		if (t.Name() == "Message" && (name == "to" || name == "from")) ||
			((t.Name() == "MessageSendParams" || t.Name() == "MessageSendInput") && name == "to") ||
			(t.Name() == "MessageCaller" && name == "ref") {
			for _, key := range []string{"machineId", "nativeSessionId", "nativeAgentId"} {
				if id := p.Properties[key]; id != nil {
					id.MaxLength = n(128)
					id.Pattern = "^[^\\s\\x00-\\x1f\\x7f-\\x9f]+$"
				}
			}
		}
		if t.Name() == "ConnectorResult" && name == "observation" {
			a, _ := Schema("SessionInspection")
			b, _ := Schema("ObservationError")
			s.Properties[name] = &jsonschema.Schema{OneOf: []*jsonschema.Schema{a, b}}
		}
		if t.Name() == "NativeMessage" && name == "result" {
			a, _ := Schema("SessionInspection")
			b, _ := Schema("SessionListOutput")
			c, _ := Schema("Message")
			d, _ := Schema("MessageRecord")
			s.Properties[name] = &jsonschema.Schema{OneOf: []*jsonschema.Schema{a, b, c, d}}
		}
		if t.Name() == "NativeMessage" && name == "ack" {
			ack, _ := Schema("ConnectorMessageAck")
			ack.Required = []string{"status"}
			s.Properties[name] = ack
		}
	}
	// Encode discriminated unions with complete branch-specific required and forbidden fields.
	switch t.Name() {
	case "Metric", "ListMetric":
		metricConstraints(s, t.Name() == "ListMetric")
	case "ProgramStatus":
		s.OneOf = []*jsonschema.Schema{{Properties: map[string]*jsonschema.Schema{"state": {Enum: enum("blocked")}}, Required: []string{"kind"}}, {Properties: map[string]*jsonschema.Schema{"state": {Enum: enum("idle", "working", "done", "error")}}, Not: &jsonschema.Schema{Required: []string{"kind"}}}}
	case "SessionGit":
		s.OneOf = []*jsonschema.Schema{{Properties: map[string]*jsonschema.Schema{"state": {Enum: enum("available")}}, Required: []string{"worktreeRoot", "commonDir", "branch", "head"}}, {Properties: map[string]*jsonschema.Schema{"state": {Enum: enum("not-repository", "unavailable")}}, Not: &jsonschema.Schema{AnyOf: []*jsonschema.Schema{{Required: []string{"worktreeRoot"}}, {Required: []string{"commonDir"}}, {Required: []string{"branch"}}, {Required: []string{"head"}}}}}}
	case "ConnectorAccepted":
		s.Properties["accepted"].Enum = []any{true}
	case "NativeMessage":
		s.Properties["version"].Enum = []any{1}
	}
}
func metricConstraints(s *jsonschema.Schema, compact bool) {
	status := s.Properties["status"]
	status.Enum = enum("available", "unavailable")
	if !compact {
		s.Properties["unit"].Enum = enum("count", "bytes", "cores", "milliseconds", "percent", "percent-one-core")
	}
	s.OneOf = []*jsonschema.Schema{{Properties: map[string]*jsonschema.Schema{"status": {Enum: enum("available")}, "value": {Type: "number"}}, Not: &jsonschema.Schema{Required: []string{"reason"}}}, {Properties: map[string]*jsonschema.Schema{"status": {Enum: enum("unavailable")}, "value": {Type: "null"}}, Required: []string{"reason"}}}
	if compact {
		for _, k := range []string{"estimated", "shared", "partial", "stale"} {
			s.Properties[k].Enum = []any{true}
		}
	} else {
		s.AllOf = []*jsonschema.Schema{{If: &jsonschema.Schema{Properties: map[string]*jsonschema.Schema{"unit": {Enum: enum("count", "bytes")}}}, Then: &jsonschema.Schema{Properties: map[string]*jsonschema.Schema{"value": {Types: []string{"integer", "null"}, Minimum: f(0), Maximum: f(9007199254740991)}}}}, {If: &jsonschema.Schema{Properties: map[string]*jsonschema.Schema{"unit": {Enum: enum("cores", "milliseconds", "percent-one-core")}}}, Then: &jsonschema.Schema{Properties: map[string]*jsonschema.Schema{"value": {Minimum: f(0)}}}}}
	}
}
func metricUnit(parent, name string) string {
	switch {
	case strings.Contains(name, "Bytes"):
		return "bytes"
	case strings.HasSuffix(name, "Ms"):
		return "milliseconds"
	case strings.HasSuffix(name, "Count") || name == "count":
		return "count"
	case name == "capacityCores":
		return "cores"
	case name == "usagePercent" || name == "cpuPercent" || name == "ownedCpuPercent":
		return "percent-one-core"
	case strings.HasSuffix(name, "Percent"):
		return "percent"
	}
	return ""
}
func Schema(name string) (*jsonschema.Schema, error) {
	t, ok := schemaTypes[name]
	if !ok {
		return nil, fmt.Errorf("unknown schema %q", name)
	}
	s, err := jsonschema.ForType(t, &jsonschema.ForOptions{IgnoreInvalidTypes: true})
	if err != nil {
		return nil, err
	}
	constrain(s, t)
	return s, nil
}
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var x any
	if err = json.Unmarshal(b, &x); err != nil {
		return nil, err
	}
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if err = validateJSON(t.Name(), x); err != nil {
		return nil, err
	}
	return b, nil
}
func Validate(v any) error {
	_, err := Encode(v)
	return err
}
func validateJSON(name string, x any) error {
	s, err := Schema(name)
	if err != nil {
		return err
	}
	r, err := s.Resolve(nil)
	if err != nil {
		return err
	}
	if err = r.Validate(x); err != nil {
		return err
	}
	return special(x)
}
func special(v any) error {
	switch x := v.(type) {
	case []any:
		for _, e := range x {
			if err := special(e); err != nil {
				return err
			}
		}
	case map[string]any:
		if text, ok := x["text"].(string); ok {
			if strings.TrimSpace(text) == "" {
				return fmt.Errorf("message text is blank")
			}
			for _, r := range text {
				if unicode.IsControl(r) && r != '\n' && r != '\t' {
					return fmt.Errorf("message text contains control characters")
				}
			}
			b, err := json.Marshal(text)
			if err != nil || len(b) > 16*1024 {
				return fmt.Errorf("encoded message text exceeds limit")
			}
		}
		if a, ok := x["startedAt"].(float64); ok {
			if b, ok := x["endedAt"].(float64); ok && b < a {
				return fmt.Errorf("sample window ends before start")
			}
		}
		if raw, ok := x["inspectorUrl"].(string); ok {
			u, e := url.Parse(raw)
			if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
				return fmt.Errorf("invalid inspector URL")
			}
			for _, k := range []string{"tokenFile"} {
				if p, ok := x[k].(string); ok && !privatePath(p) {
					return fmt.Errorf("invalid private path")
				}
			}
			if p, ok := x["project"].(map[string]any); ok {
				if root, ok := p["root"].(string); !ok || !privatePath(root) {
					return fmt.Errorf("invalid project root")
				}
			}
		}
		for _, e := range x {
			if err := special(e); err != nil {
				return err
			}
		}
	}
	return nil
}
func privatePath(p string) bool {
	return filepath.IsAbs(p) && filepath.Clean(p) == p && !strings.ContainsAny(p, "\x00\r\n")
}
func Decode[T any](r io.Reader, maxBytes int64) (T, error) {
	var out T
	b, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return out, err
	}
	if int64(len(b)) > maxBytes {
		return out, fmt.Errorf("JSON exceeds %d bytes", maxBytes)
	}
	var raw any
	d := json.NewDecoder(bytes.NewReader(b))
	if err = d.Decode(&raw); err != nil {
		return out, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return out, fmt.Errorf("trailing JSON")
	}
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if err = validateJSON(t.Name(), raw); err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}
