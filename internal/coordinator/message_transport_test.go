package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestMessageLoopback(t *testing.T) {
	hash := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
	p := wire.InspectorPrincipal{ID: "a", Owner: "owner", TokenHash: hash("abcdefghijklmnop"), Messaging: true}
	denied := wire.InspectorPrincipal{ID: "b", Owner: "owner", TokenHash: hash("other-token-12345")}
	reg, err := NewRegistry(wire.InspectorConfig{Principals: []wire.InspectorPrincipal{p, denied}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	ref := wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "session"}
	ann := wire.ConnectorAnnounce{ConnectorID: "connector", Incarnation: "first", MachineID: "machine", Sessions: []wire.SessionPresence{{Ref: ref, ObservedAt: 1}}, Receivers: []wire.SessionRef{ref}}
	if _, err = reg.Announce(p, ann); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Handler(reg))
	defer server.Close()
	client, err := NewClient(wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "abcdefghijklmnop"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sent, err := client.SendMessage(ctx, wire.MessageSendInput{To: ref, Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := client.GetMessage(ctx, wire.MessageGetInput{MessageID: sent.ID})
	if err != nil || record.Message.ID != sent.ID {
		t.Fatalf("get: %+v %v", record, err)
	}
	if _, err = client.ReplyMessage(ctx, wire.MessageReplyInput{ReplyID: sent.ID, Text: "bad"}); err == nil {
		t.Fatal("unbound reply accepted")
	}
	if _, err = reg.Poll(p, wire.ConnectorPoll{ConnectorID: ann.ConnectorID, Incarnation: ann.Incarnation}); err != nil {
		t.Fatal(err)
	}
	reply, err := client.ReplyMessage(ctx, wire.MessageReplyInput{ReplyID: sent.ID, Text: "reply", Caller: &wire.MessageCaller{ConnectorID: ann.ConnectorID, Incarnation: ann.Incarnation, Ref: ref}})
	if err != nil || reply.InReplyTo != sent.ID {
		t.Fatalf("reply: %+v %v", reply, err)
	}
	record, err = client.GetMessage(ctx, wire.MessageGetInput{MessageID: sent.ID})
	if err != nil || len(record.Replies) != 1 || record.Replies[0].ID != reply.ID {
		t.Fatalf("replies: %+v %v", record, err)
	}
	other, err := NewClient(wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "other-token-12345"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.GetMessage(ctx, wire.MessageGetInput{MessageID: sent.ID}); err == nil {
		t.Fatal("permission bypass")
	}
}

func TestScopedMessageSchemaRejectsCaller(t *testing.T) {
	var calls atomic.Int32
	server := makeMCP(mcpReader{bound: true, send: func(context.Context, *mcp.CallToolRequest, wire.MessageSendInput) (wire.Message, error) {
		calls.Add(1)
		return wire.Message{}, nil
	}}, nil)
	a, b := mcp.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := server.Connect(ctx, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	cs, err := client.Connect(ctx, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	result, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "message.send", Arguments: map[string]any{"to": map[string]any{"machineId": "machine", "harness": "omp", "nativeSessionId": "session"}, "text": "hello", "caller": map[string]any{}}})
	if err == nil && (result == nil || !result.IsError) {
		t.Fatal("caller accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("handler invoked")
	}
}

func TestSourceDeliveryAndOptIn(t *testing.T) {
	for _, tc := range []struct {
		name          string
		enabled, fail bool
	}{{"enabled", true, false}, {"disabled", false, false}, {"uncertain", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			enabled := tc.enabled
			ref := wire.SessionRef{MachineID: "machine", Harness: "pi", NativeSessionID: "session"}
			announcements := make(chan wire.ConnectorAnnounce, 2)
			acks := make(chan wire.ConnectorMessageAck, 2)
			var polls, delivered, receivers atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/connector/announce":
					var a wire.ConnectorAnnounce
					_ = json.NewDecoder(r.Body).Decode(&a)
					announcements <- a
				case "/connector/poll":
					result := wire.ConnectorPollResult{Requests: []wire.InspectionRequest{}}
					if polls.Add(1) == 1 {
						result.Messages = []wire.Message{{ID: "message", Sender: "peer", To: &ref, Text: "hello", CreatedAt: 1, ExpiresAt: 2, Status: "dispatched"}}
					}
					_ = json.NewEncoder(w).Encode(result)
					return
				case "/connector/message-ack":
					var a wire.ConnectorMessageAck
					_ = json.NewDecoder(r.Body).Decode(&a)
					acks <- a
				}
				_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
			}))
			defer server.Close()
			cfg := Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "abcdefghijklmnop", Messaging: enabled}, MachineID: "machine"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			var bound wire.ConnectorPoll
			go func() {
				done <- RunSource(ctx, cfg, Source{Bind: func(p wire.ConnectorPoll) { bound = p }, Snapshot: func(context.Context) ([]wire.SessionPresence, error) {
					return []wire.SessionPresence{{Ref: ref, Messaging: true}}, nil
				}, Inspect: func(context.Context, wire.SessionRef) (wire.SessionInspection, error) {
					return wire.SessionInspection{}, errors.New("unused")
				}, Receivers: func(context.Context) ([]wire.SessionRef, error) { receivers.Add(1); return []wire.SessionRef{ref}, nil }, Deliver: func(context.Context, wire.Message) (string, error) {
					delivered.Add(1)
					if tc.fail {
						return "accepted", errors.New("uncertain")
					}
					return "accepted", nil
				}}, nil)
			}()
			select {
			case a := <-announcements:
				if a.Sessions[0].Messaging || (len(a.Receivers) > 0) != enabled {
					t.Fatalf("announce %+v", a)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no announce")
			}
			select {
			case a := <-acks:
				if a.ConnectorID != bound.ConnectorID || a.Incarnation != bound.Incarnation || a.MessageID != "message" || (a.Status == "accepted") != (enabled && !tc.fail) {
					t.Fatalf("ack %+v", a)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no ack")
			}
			if (delivered.Load() > 0) != enabled || (receivers.Load() > 0) != enabled {
				t.Fatal("opt-in ignored")
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("source did not cancel")
			}
		})
	}
}

func TestMessageErrorTextWhitelist(t *testing.T) {
	for _, tc := range []struct {
		status      int
		input, want string
	}{
		{409, "Target does not support messaging", "Target does not support messaging"},
		{403, "Messaging not permitted", "Messaging permission required"},
		{400, "Invalid message reply", "Bound reply caller required"},
		{404, "Message not found", "Message unavailable"},
		{500, "private peer text", "Message request unavailable"},
		{403, "Target does not support messaging", "Message request unavailable"},
	} {
		if got := messageErrorText(tc.status, tc.input); got != tc.want {
			t.Fatalf("got %q want %q", got, tc.want)
		}
	}
}
