package coordinator

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestRecipientsRequireLiveReceiverAndExactUnion(t *testing.T) {
	r, a, b, _, pub, recv := messageFixture(t)
	out, err := r.Recipients(a, wire.RecipientListInput{Limit: 1})
	if err != nil || len(out.Recipients) != 0 {
		t.Fatalf("publisher without receiver: %+v %v", out, err)
	}
	if _, err = r.Announce(b, recv); err != nil {
		t.Fatal(err)
	}
	out, err = r.Recipients(a, wire.RecipientListInput{Limit: 1})
	if err != nil || len(out.Recipients) != 1 || out.Recipients[0].Ref.Key() != wire.SessionRecipient(pub.Sessions[0].Ref).Key() {
		t.Fatalf("live receiver: %+v %v", out, err)
	}
	ref := pub.Sessions[0].Ref
	_, err = r.SendMessage(a, wire.MessageSendInput{To: wire.RecipientRef{Kind: "session", Session: &ref, SubscriptionID: "other"}, Text: "test"})
	if err == nil {
		t.Fatal("mixed union accepted")
	}
	if _, err = r.Disconnect(b, wire.ConnectorDisconnect{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation}); err != nil {
		t.Fatal(err)
	}
	out, err = r.Recipients(a, wire.RecipientListInput{Limit: 1})
	if err != nil || len(out.Recipients) != 0 {
		t.Fatalf("disconnected receiver: %+v %v", out, err)
	}
}

func TestRestoredRouteRequiresOriginalIncarnation(t *testing.T) {
	r, _, b, _, pub, recv := messageFixture(t)
	if _, err := r.Announce(b, recv); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := journalRouteOf(r.route(b.Owner, pub.Sessions[0].Ref))
	saved.ReceiverIncarnation = "different"
	if r.routeLive(r.restoreRoute(b.Owner, routeFromJournal(saved))) {
		t.Fatal("different incarnation restored")
	}
	saved.ReceiverIncarnation = recv.Incarnation
	if !r.routeLive(r.restoreRoute(b.Owner, routeFromJournal(saved))) {
		t.Fatal("exact live binding not restored")
	}
}

func hostedMessageFixture(t *testing.T) (*Registry, wire.InspectorPrincipal, wire.InspectorPrincipal, wire.ConnectorAnnounce, wire.ConnectorAnnounce, hostedSubscription) {
	t.Helper()
	r, _, native, _, pub, recv := messageFixture(t)
	if _, err := r.Announce(native, recv); err != nil {
		t.Fatal(err)
	}
	hosted := wire.InspectorPrincipal{ID: "chat", Owner: native.Owner, Messaging: true}
	r.principals[hosted.ID] = hosted
	r.oauth = &oauthAccess{bindings: map[string]wire.InspectorPrincipal{hosted.ID: hosted}}
	store, err := openHostedStore(filepath.Join(privateTempDir(t), "hosted"))
	if err != nil {
		t.Fatal(err)
	}
	r.hosted = &hostedAccess{registry: r, subscriptions: store}
	t.Cleanup(func() { r.hosted = nil; r.oauth = nil })
	sub := hostedTestSubscription()
	sub.Owner, sub.PrincipalID, sub.Generation = hosted.Owner, hosted.ID, "generation"
	if err := store.put(sub); err != nil {
		t.Fatal(err)
	}
	return r, native, hosted, pub, recv, sub
}

func TestHostedReplyRestoresOriginalNativeRoute(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "replacement"}[replacement], func(t *testing.T) {
			r, native, hosted, pub, recv, sub := hostedMessageFixture(t)
			caller := &wire.MessageCaller{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation, Ref: pub.Sessions[0].Ref}
			ref := wire.RecipientRef{Kind: "subscription", SubscriptionID: sub.ID}
			parent, err := r.SendMessage(native, wire.MessageSendInput{To: ref, Text: "question", Caller: caller})
			if err != nil {
				t.Fatal(err)
			}
			r.messages[parent.ID].message.Status = "accepted"
			reply, err := r.ReplyHostedMessage(hosted, wire.MessageReplyInput{ReplyID: parent.ID, Text: "answer", RequestID: "reply"})
			if err != nil {
				t.Fatal(err)
			}
			r.connectors = map[string]*connector{}
			r.sessions = map[string]*session{}
			r.receivers = map[string]*connector{}
			r.messages = map[string]*messageEntry{}
			r.messageRequests = map[string]messageRequest{}
			if err := r.loadHostedMessages(); err != nil {
				t.Fatal(err)
			}
			r.prune()
			if r.messages[reply.ID].message.Status != "queued" {
				t.Fatal("unresolved route retired")
			}
			if replacement {
				pub.Incarnation = "replacement"
			}
			if _, err := r.Announce(native, pub); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Announce(native, recv); err != nil {
				t.Fatal(err)
			}
			batch, err := r.Poll(native, wire.ConnectorPoll{ConnectorID: recv.ConnectorID, Incarnation: recv.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			if replacement {
				if len(batch.Messages) != 0 || r.messages[reply.ID].message.Status != "unknown" {
					t.Fatal("replacement received saved reply")
				}
				r.messages = map[string]*messageEntry{}
				r.messageRequests = map[string]messageRequest{}
				if err := r.loadHostedMessages(); err != nil {
					t.Fatal(err)
				}
				if r.messages[reply.ID].route.receiverID != "" {
					t.Fatal("retired binding persisted")
				}
			} else if len(batch.Messages) != 1 || batch.Messages[0].ID != reply.ID {
				t.Fatalf("original reply missing: %+v", batch.Messages)
			}
		})
	}
}

func TestJournalPostRenameFailureRetainsIdempotency(t *testing.T) {
	r, native, _, _, _, sub := hostedMessageFixture(t)
	in := wire.MessageSendInput{To: wire.RecipientRef{Kind: "subscription", SubscriptionID: sub.ID}, Text: "once", RequestID: "once"}
	r.journalAfterRename = func() error { return errors.New("injected directory sync failure") }
	if _, err := r.SendMessage(native, in); err == nil || err.Error() != "hosted message journal commit outcome uncertain" {
		t.Fatalf("expected uncertain outcome: %v", err)
	}
	if len(r.messages) != 1 || len(r.messageRequests) != 1 || r.hostedErr == nil {
		t.Fatal("committed admission rolled back")
	}
	var id string
	for id = range r.messages {
	}
	r.journalAfterRename = nil
	r.hostedErr = nil
	r.messages = map[string]*messageEntry{}
	r.messageRequests = map[string]messageRequest{}
	if err := r.loadHostedMessages(); err != nil {
		t.Fatal(err)
	}
	got, err := r.SendMessage(native, in)
	if err != nil || got.ID != id || len(r.messages) != 1 {
		t.Fatalf("retry duplicated admission: %+v %v", got, err)
	}
}
