package wire_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestMessagingContract(t *testing.T) {
	ref := wire.SessionRef{MachineID: "machine", Harness: "pi", NativeSessionID: "session"}
	caller := &wire.MessageCaller{ConnectorID: "connector", Incarnation: "incarnation", Ref: ref}
	message := wire.Message{ID: "message", Sender: "principal", To: &ref, Text: "hello\nworld", CreatedAt: 1, ExpiresAt: 2, Status: "queued"}
	values := []any{
		wire.MessageSendParams{To: ref, Text: "hello"},
		wire.MessageReplyParams{ReplyID: "message", Text: "reply"},
		wire.MessageSendInput{To: ref, Text: "hello", Caller: caller},
		wire.MessageReplyInput{ReplyID: "message", Text: "reply", Caller: caller},
		wire.MessageGetInput{MessageID: "message"},
		message,
		wire.Message{ID: "reply", Sender: "principal", Text: "reply", InReplyTo: "message", CreatedAt: 1, ExpiresAt: 2, Status: "accepted"},
		wire.MessageRecord{Message: message, Replies: []wire.Message{}},
		wire.ConnectorMessageAck{ConnectorID: "connector", Incarnation: "incarnation", MessageID: "message", Status: "accepted"},
		wire.ConnectorPollResult{Requests: []wire.InspectionRequest{}, Messages: []wire.Message{message}},
		wire.NativeMessage{Version: 1, ID: "frame", Method: "message", Message: &message},
		wire.NativeMessage{Version: 1, ID: "frame", Result: json.RawMessage(`{"id":"message","sender":"principal","to":{"machineId":"machine","harness":"pi","nativeSessionId":"session"},"text":"hello","createdAt":1,"expiresAt":2,"status":"queued"}`)},
	}
	for _, value := range values {
		if err := wire.Validate(value); err != nil {
			t.Errorf("%T: %v", value, err)
		}
	}
	for _, value := range []any{wire.InspectorPrincipal{}, wire.ConnectorConfig{}, wire.SessionPresence{}, wire.ProjectListSession(wire.SessionPresence{})} {
		b, err := json.Marshal(value)
		if err != nil || strings.Contains(string(b), `"messaging"`) {
			t.Errorf("default must omit opt-in: %s %v", b, err)
		}
	}
	p := wire.SessionPresence{Ref: ref, Messaging: true}
	if !wire.ProjectListSession(p).Messaging {
		t.Fatal("projection lost messaging opt-in")
	}
}

func TestMessagingRejections(t *testing.T) {
	ref := `{"machineId":"machine","harness":"pi","nativeSessionId":"session"}`
	for _, input := range []string{
		`{"to":{},"text":"hello"}`,
		`{"to":` + ref + `,"text":"hello","extra":true}`,
		`{"to":` + ref + `,"text":"  \n\t"}`,
		`{"to":` + ref + `,"text":"bad\u0000text"}`,
		`{"to":` + ref + `,"text":"` + strings.Repeat("a", 8193) + `"}`,
		`{"to":` + ref + `,"text":"` + strings.Repeat("界", 6000) + `"}`,
		`{"to":` + ref + `,"text":"hello","requestId":"` + strings.Repeat("a", 129) + `"}`,
	} {
		if _, err := wire.Decode[wire.MessageSendInput](strings.NewReader(input), 65536); err == nil {
			t.Errorf("accepted invalid send: %.100s", input)
		}
	}
	if _, err := wire.Decode[wire.MessageSendInput](strings.NewReader(`{"to":`+ref+`,"text":"`+strings.Repeat("a", 8192)+`"}`), 65536); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		`{"messageId":""}`,
		`{"messageId":"id","unknown":1}`,
	} {
		if _, err := wire.Decode[wire.MessageGetInput](strings.NewReader(input), 1024); err == nil {
			t.Errorf("accepted invalid get: %s", input)
		}
	}
	for _, raw := range []string{
		`{"id":"id","sender":"sender","to":` + ref + `,"text":"ok","createdAt":-1,"expiresAt":2,"status":"queued"}`,
		`{"id":"id","sender":"sender","to":` + ref + `,"text":"ok","createdAt":1,"expiresAt":2,"status":"read"}`,
	} {
		if _, err := wire.Decode[wire.Message](strings.NewReader(raw), 4096); err == nil {
			t.Errorf("accepted invalid message: %s", raw)
		}
	}
	if _, err := wire.Decode[wire.NativeMessage](strings.NewReader(`{"version":1,"id":"frame","method":"message_ack","ack":{"status":"accepted"}}`), 4096); err != nil {
		t.Fatal(err)
	}
	rawMessage := `{"id":"id","sender":"sender","to":` + ref + `,"text":"ok","createdAt":1,"expiresAt":2,"status":"queued"}`
	record := `{"message":` + rawMessage + `,"replies":[` + strings.Repeat(rawMessage+`,`, 32) + rawMessage + `]}`
	if _, err := wire.Decode[wire.MessageRecord](strings.NewReader(record), 65536); err == nil {
		t.Fatal("accepted too many replies")
	}
	s, err := wire.Schema("MessageReplyParams")
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(map[string]any{"replyId": "id", "text": "ok", "unknown": true}); err == nil {
		t.Fatal("schema accepted unknown field")
	}
}
