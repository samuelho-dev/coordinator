package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func messageFixture(t *testing.T) (*Registry, wire.InspectorPrincipal, wire.InspectorPrincipal, wire.InspectorPrincipal, wire.ConnectorAnnounce, wire.ConnectorAnnounce) {
	t.Helper()
	principal := func(id string, enabled bool) wire.InspectorPrincipal {
		h := sha256.Sum256([]byte(id))
		return wire.InspectorPrincipal{ID: id, Owner: "owner", TokenHash: hex.EncodeToString(h[:]), Messaging: enabled}
	}
	a, b, disabled := principal("sender", true), principal("receiver", true), principal("disabled", false)
	r, e := NewRegistry(wire.InspectorConfig{Principals: []wire.InspectorPrincipal{a, b, disabled, principal("stranger", true)}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(r.Close)
	ref := wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "target"}
	pub := wire.ConnectorAnnounce{ConnectorID: "publisher", Incarnation: "first", MachineID: "machine", Sessions: []wire.SessionPresence{{Ref: ref, ObservedAt: 1, Messaging: true}}}
	recv := wire.ConnectorAnnounce{ConnectorID: "receiver", Incarnation: "first", MachineID: "machine", Sessions: []wire.SessionPresence{}, Receivers: []wire.SessionRef{ref}}
	if _, e = r.Announce(b, pub); e != nil {
		t.Fatal(e)
	}
	return r, a, b, disabled, pub, recv
}
func TestMessageExternalReceiverAndReply(t *testing.T) {
	r, a, b, disabled, pub, recv := messageFixture(t)
	ref := pub.Sessions[0].Ref
	page, e := r.List(a, wire.SessionListInput{Limit: 10})
	if e != nil || page.Sessions[0].Messaging {
		t.Fatalf("publisher claimed receive: %+v %v", page, e)
	}
	if _, e = r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	page, e = r.List(a, wire.SessionListInput{Limit: 10})
	if e != nil || !page.Sessions[0].Messaging {
		t.Fatalf("receiver missing: %+v %v", page, e)
	}
	if _, e = r.SendMessage(disabled, wire.MessageSendInput{To: ref, Text: "x"}); e == nil {
		t.Fatal("disabled sender accepted")
	}
	input := wire.MessageSendInput{To: ref, Text: "hello", RequestID: "request"}
	m, e := r.SendMessage(a, input)
	if e != nil {
		t.Fatal(e)
	}
	again, e := r.SendMessage(a, input)
	if e != nil || again.ID != m.ID {
		t.Fatalf("idempotency: %+v %v", again, e)
	}
	input.Text = "changed"
	if _, e = r.SendMessage(a, input); e == nil {
		t.Fatal("request conflict accepted")
	}
	poll := wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}
	batch, e := r.Poll(b, poll)
	if e != nil || len(batch.Messages) != 1 || batch.Messages[0].ID != m.ID {
		t.Fatalf("poll: %+v %v", batch, e)
	}
	batch, e = r.Poll(b, poll)
	if e != nil || len(batch.Messages) != 0 {
		t.Fatalf("redelivery: %+v %v", batch, e)
	}
	ack := wire.ConnectorMessageAck{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, MessageID: m.ID, Status: "accepted"}
	if _, e = r.AckMessage(b, ack); e != nil {
		t.Fatal(e)
	}
	if _, e = r.AckMessage(b, ack); e != nil {
		t.Fatal(e)
	}
	if _, e = r.GetMessage(r.principals["stranger"], wire.MessageGetInput{MessageID: m.ID}); e == nil {
		t.Fatal("same-owner nonparticipant read")
	}
	caller := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: ref}
	reply, e := r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "answer", Caller: caller})
	if e != nil || reply.To != nil || reply.InReplyTo != m.ID {
		t.Fatalf("reply: %+v %v", reply, e)
	}
	record, e := r.GetMessage(a, wire.MessageGetInput{MessageID: m.ID})
	if e != nil || len(record.Replies) != 1 || record.Replies[0].ID != reply.ID {
		t.Fatalf("get: %+v %v", record, e)
	}
	if _, e = r.GetMessage(disabled, wire.MessageGetInput{MessageID: m.ID}); e == nil {
		t.Fatal("unauthorized read")
	}
	if _, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "x"}); e == nil {
		t.Fatal("unbound reply")
	}
}
func TestMessageReverseAndFencing(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	if _, e := r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	source := wire.SessionRef{MachineID: "other-machine", Harness: "pi", NativeSessionID: "source"}
	src := wire.ConnectorAnnounce{ConnectorID: "source", Incarnation: "first", MachineID: source.MachineID, Sessions: []wire.SessionPresence{{Ref: source, ObservedAt: 1}}, Receivers: []wire.SessionRef{source}}
	if _, e := r.Announce(a, src); e != nil {
		t.Fatal(e)
	}
	from := &wire.MessageCaller{ConnectorID: src.ConnectorID, Incarnation: src.Incarnation, Ref: source}
	m, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: "hello", Caller: from})
	if e != nil {
		t.Fatal(e)
	}
	target := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: pub.Sessions[0].Ref}
	if _, e = r.Poll(b, wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}); e != nil {
		t.Fatal(e)
	}
	reply, e := r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "back", Caller: target})
	if e != nil || reply.To == nil || reply.To.Key() != source.Key() {
		t.Fatalf("reverse: %+v %v", reply, e)
	}
	batch, e := r.Poll(a, wire.ConnectorPoll{ConnectorID: src.ConnectorID, Incarnation: src.Incarnation})
	if e != nil || len(batch.Messages) != 1 || batch.Messages[0].ID != reply.ID {
		t.Fatalf("reverse poll: %+v %v", batch, e)
	}
	if _, e = r.Disconnect(a, wire.ConnectorDisconnect{ConnectorID: src.ConnectorID, Incarnation: src.Incarnation}); e != nil {
		t.Fatal(e)
	}
	record, e := r.GetMessage(a, wire.MessageGetInput{MessageID: reply.ID})
	if e != nil || record.Message.Status != "unknown" {
		t.Fatalf("lost ack: %+v %v", record, e)
	}
	src.Incarnation = "second"
	if _, e = r.Announce(a, src); e != nil {
		t.Fatal(e)
	}
	batch, e = r.Poll(a, wire.ConnectorPoll{ConnectorID: src.ConnectorID, Incarnation: src.Incarnation})
	if e != nil || len(batch.Messages) != 0 {
		t.Fatalf("redirected: %+v %v", batch, e)
	}
	if _, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "late", Caller: target}); e == nil {
		t.Fatal("reverse route redirected")
	}
}
func TestMessageCapacityAndExpiry(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	if _, e := r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	for i := range 32 {
		if _, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: fmt.Sprint(i)}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: "overflow"}); e == nil {
		t.Fatal("target capacity not enforced")
	}
	for _, m := range r.messages {
		m.created = time.Now().Add(-11 * time.Minute)
	}
	if _, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: strings.Repeat("x", 8193)}); e == nil {
		t.Fatal("oversized text accepted")
	}
	if _, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: "after expiry"}); e != nil {
		t.Fatal(e)
	}
}
func TestMessageReceiverClaimsAndRecordLimit(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	forged := recv
	forged.ConnectorID = "forged"
	if _, e := r.Announce(a, forged); e == nil {
		t.Fatal("cross-principal receiver")
	}
	forged = recv
	forged.MachineID = "wrong"
	if _, e := r.Announce(b, forged); e == nil {
		t.Fatal("cross-machine receiver")
	}
	if _, e := r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	forged = recv
	forged.ConnectorID = "competing"
	if _, e := r.Announce(b, forged); e == nil {
		t.Fatal("competing receiver")
	}
	m, e := r.SendMessage(a, wire.MessageSendInput{To: pub.Sessions[0].Ref, Text: "question"})
	if e != nil {
		t.Fatal(e)
	}
	spoof := &wire.MessageCaller{ConnectorID: pub.ConnectorID, Incarnation: pub.Incarnation, Ref: pub.Sessions[0].Ref}
	if _, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "no", Caller: spoof}); e == nil {
		t.Fatal("publisher caller spoof")
	}
	caller := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: pub.Sessions[0].Ref}
	if _, e = r.Poll(b, wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}); e != nil {
		t.Fatal(e)
	}
	rejected := false
	for range 32 {
		_, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: strings.Repeat("x", 8192), Caller: caller})
		if e != nil {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("aggregate response limit not enforced")
	}
	record, e := r.GetMessage(a, wire.MessageGetInput{MessageID: m.ID})
	if e != nil || size(record) > wire.InspectionLimit {
		t.Fatalf("unreadable record: %v", e)
	}
}
func TestMessageGlobalAndReplyBounds(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	if _, e := r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	ref := pub.Sessions[0].Ref
	root, e := r.SendMessage(a, wire.MessageSendInput{To: ref, Text: "root"})
	if e != nil {
		t.Fatal(e)
	}
	caller := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: ref}
	if _, e = r.Poll(b, wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}); e != nil {
		t.Fatal(e)
	}
	for range 32 {
		if _, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: root.ID, Text: "x", Caller: caller}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: root.ID, Text: "overflow", Caller: caller}); e == nil {
		t.Fatal("reply bound not enforced")
	}
	poll := wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}
	for range 991 {
		batch, err := r.Poll(b, poll)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range batch.Messages {
			if _, err = r.AckMessage(b, wire.ConnectorMessageAck{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, MessageID: m.ID, Status: "accepted"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = r.SendMessage(a, wire.MessageSendInput{To: ref, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, e = r.SendMessage(a, wire.MessageSendInput{To: ref, Text: "overflow"}); e == nil {
		t.Fatal("global bound not enforced")
	}
}
func TestMessageReplyRequiresDispatch(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	if _, e := r.Announce(b, recv); e != nil {
		t.Fatal(e)
	}
	ref := pub.Sessions[0].Ref
	caller := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: ref}
	poll := wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}
	send := func() wire.Message {
		m, e := r.SendMessage(a, wire.MessageSendInput{To: ref, Text: "hello"})
		if e != nil {
			t.Fatal(e)
		}
		return m
	}
	rejected := func(m wire.Message) {
		_, e := r.ReplyMessage(b, wire.MessageReplyInput{ReplyID: m.ID, Text: "reply", Caller: caller})
		if h, ok := e.(*HTTPError); !ok || h.Status != 409 {
			t.Fatalf("reply status: %v", e)
		}
	}
	queued := send()
	rejected(queued)
	if _, e := r.Poll(b, poll); e != nil {
		t.Fatal(e)
	}
	if _, e := r.AckMessage(b, wire.ConnectorMessageAck{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, MessageID: queued.ID, Status: "refused"}); e != nil {
		t.Fatal(e)
	}
	rejected(queued)
	expired := send()
	r.messages[expired.ID].created = time.Now().Add(-11 * time.Minute)
	rejected(expired)
}
