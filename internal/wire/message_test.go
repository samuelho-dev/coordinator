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
	recipient := wire.SessionRecipient(ref)
	message := wire.Message{ID: "message", Sender: "principal", To: &recipient, Text: "hello\nworld", CreatedAt: 1, ExpiresAt: 2, Status: "queued"}
	values := []any{
		wire.MessageSendParams{To: recipient, Text: "hello"},
		wire.MessageReplyParams{ReplyID: "message", Text: "reply"},
		wire.MessageSendInput{To: recipient, Text: "hello", Caller: caller},
		wire.MessageSendInput{To: wire.RecipientRef{Kind: "subscription", SubscriptionID: "subscription"}, Text: "hello"},
		wire.MessageReplyInput{ReplyID: "message", Text: "reply", Caller: caller},
		wire.MessageGetInput{MessageID: "message"},
		message,
		wire.Message{ID: "reply", Sender: "principal", Text: "reply", InReplyTo: "message", CreatedAt: 1, ExpiresAt: 2, Status: "accepted"},
		wire.MessageRecord{Message: message, Replies: []wire.Message{}},
		wire.ConnectorMessageAck{ConnectorID: "connector", Incarnation: "incarnation", MessageID: "message", Status: "accepted"},
		wire.ConnectorPollResult{Requests: []wire.InspectionRequest{}, Messages: []wire.Message{message}},
		wire.NativeMessage{Version: 1, ID: "frame", Method: "message", Message: &message},
		wire.NativeMessage{Version: 1, ID: "frame", Result: json.RawMessage(`{"id":"message","sender":"principal","to":{"kind":"session","session":{"machineId":"machine","harness":"pi","nativeSessionId":"session"}},"text":"hello","createdAt":1,"expiresAt":2,"status":"queued"}`)},
		wire.NativeMessage{Version: 1, ID: "frame", Method: "recipients", Recipients: &wire.RecipientListInput{Limit: 10}},
		wire.NativeMessage{Version: 1, ID: "frame", Result: json.RawMessage(`{"recipients":[],"nextCursor":null}`)},
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
	ref := `{"kind":"session","session":{"machineId":"machine","harness":"pi","nativeSessionId":"session"}}`
	for _, input := range []string{
		`{"to":{},"text":"hello"}`,
		`{"to":{"machineId":"machine","harness":"pi","nativeSessionId":"session"},"text":"hello"}`,
		`{"to":{"kind":"session","session":{"machineId":"machine","harness":"pi","nativeSessionId":"session"},"subscriptionId":"id"},"text":"hello"}`,
		`{"to":{"kind":"session","session":{"machineId":"bad id","harness":"pi","nativeSessionId":"session"}},"text":"hello"}`,
		`{"to":{"kind":"subscription","subscriptionId":"bad id"},"text":"hello"}`,
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

func TestRecipientContract(t *testing.T) {
	ref := wire.SessionRef{MachineID: "machine", Harness: "pi", NativeSessionID: "session"}
	session := wire.SessionRecipient(ref)
	if session.Key() == (wire.RecipientRef{Kind: "subscription", SubscriptionID: ref.Key()}).Key() {
		t.Fatal("recipient keys collide across kinds")
	}
	for _, value := range []any{session, wire.RecipientRef{Kind: "subscription", SubscriptionID: "subscription"}, wire.RecipientListInput{Limit: 10}, wire.RecipientListOutput{Recipients: []wire.Recipient{}}} {
		if err := wire.Validate(value); err != nil {
			t.Fatalf("%T: %v", value, err)
		}
	}
	s, err := wire.Schema("RecipientRef")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := s.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"kind":"session"}`,
		`{"kind":"subscription"}`,
		`{"kind":"session","session":{"machineId":"m","harness":"pi","nativeSessionId":"s"},"subscriptionId":"id"}`,
		`{"kind":"subscription","session":{"machineId":"m","harness":"pi","nativeSessionId":"s"},"subscriptionId":"id"}`,
		`{"kind":"other","subscriptionId":"id"}`,
	} {
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatal(err)
		}
		if err := resolved.Validate(value); err == nil {
			t.Fatalf("schema accepted %s", raw)
		}
		if _, err := wire.Decode[wire.RecipientRef](strings.NewReader(raw), 4096); err == nil {
			t.Fatalf("decoder accepted %s", raw)
		}
	}
	for _, raw := range []string{
		`{"stateDirectory":"relative","callbackHosts":["example.com"]}`,
		`{"stateDirectory":"/tmp/../state","callbackHosts":["example.com"]}`,
		`{"stateDirectory":"/","callbackHosts":["example.com"]}`,
		`{"stateDirectory":"/tmp/state","callbackHosts":["*.example.com"]}`,
		`{"stateDirectory":"/tmp/state","callbackHosts":["Example.com"]}`,
		`{"stateDirectory":"/tmp/state","callbackHosts":["127.0.0.1"]}`,
		`{"stateDirectory":"/tmp/state","callbackHosts":["localhost"]}`,
		`{"stateDirectory":"/tmp/state","callbackHosts":["example.com:443"]}`,
	} {
		if _, err := wire.Decode[wire.InspectorHostedConfig](strings.NewReader(raw), 4096); err == nil {
			t.Fatalf("decoder accepted %s", raw)
		}
	}
	if _, err := wire.Decode[wire.InspectorHostedConfig](strings.NewReader(`{"stateDirectory":"/tmp/state","callbackHosts":["example.com"]}`), 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := wire.Decode[wire.ConnectorConfig](strings.NewReader(`{"inspectorUrl":"https://example.com","codexSocket":"relative"}`), 4096); err == nil {
		t.Fatal("accepted relative codex socket")
	}
}

func TestHostedEventContract(t *testing.T) {
	valid := `{"name":"message.received","arguments":{"name":"ChatGPT"},"delivery":{"mode":"webhook","url":"https://example.com/hook","secret":"whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},"cursor":null,"ttlMs":null}`
	if _, err := wire.Decode[wire.HostedEventSubscribeInput](strings.NewReader(valid), 4096); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		strings.Replace(valid, `"message.received"`, `"other"`, 1),
		strings.Replace(valid, `"ChatGPT"`, `"ChatGPT","extra":true`, 1),
		strings.Replace(valid, `"webhook"`, `"other"`, 1),
		strings.Replace(valid, `https://example.com/hook`, `http://example.com/hook`, 1),
		strings.Replace(valid, `https://example.com/hook`, `https://example.com/hook?secret=x`, 1),
		strings.Replace(valid, `whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA`, `whsec_YQ==`, 1),
		strings.Replace(valid, `,"secret":"whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`, ``, 1),
		strings.Replace(valid, `"cursor":null`, `"cursor":"replay"`, 1),
		strings.Replace(valid, `"ttlMs":null`, `"ttlMs":604800001`, 1),
		strings.Replace(valid, `"ttlMs":null`, `"ttlMs":0`, 1),
	} {
		if _, err := wire.Decode[wire.HostedEventSubscribeInput](strings.NewReader(raw), 4096); err == nil {
			t.Errorf("accepted invalid subscription: %s", raw)
		}
	}
	unsubscribe := `{"name":"message.received","arguments":{"name":"ChatGPT"},"delivery":{"mode":"webhook","url":"https://example.com/hook"}}`
	if _, err := wire.Decode[wire.HostedEventUnsubscribeInput](strings.NewReader(unsubscribe), 4096); err != nil {
		t.Fatal(err)
	}
	withSecret := strings.Replace(unsubscribe, `"url":"https://example.com/hook"`, `"url":"https://example.com/hook","secret":"whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`, 1)
	if _, err := wire.Decode[wire.HostedEventUnsubscribeInput](strings.NewReader(withSecret), 4096); err == nil {
		t.Fatal("accepted unsubscribe secret")
	}
}
