package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func (r *Registry) Recipients(p wire.InspectorPrincipal, in wire.RecipientListInput) (wire.RecipientListOutput, error) {
	out := wire.RecipientListOutput{Recipients: []wire.Recipient{}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return out, failure(401, "Unauthorized")
	}
	if wire.Validate(in) != nil {
		return out, failure(400, "Invalid recipient request")
	}
	r.prune()
	after := ""
	if in.Cursor != "" {
		c, ok := r.recipientCursors[in.Cursor]
		if !ok || c.owner != p.Owner {
			return out, failure(400, "Invalid recipient cursor")
		}
		after = c.after
	}
	items := map[string]wire.Recipient{}
	for _, s := range r.sessions {
		if s.connector.owner != p.Owner || r.route(p.Owner, s.presence.Ref).receiver == nil {
			continue
		}
		ref := wire.SessionRecipient(s.presence.Ref)
		name := s.presence.Label
		if name == "" {
			name = s.presence.Ref.NativeSessionID
		}
		items[ref.Key()] = wire.Recipient{Ref: ref, Name: name, Application: s.presence.Ref.Harness, Available: true}
	}
	if r.hosted != nil {
		subs, err := r.hosted.subscriptions.list()
		if err != nil {
			return out, err
		}
		for _, sub := range subs {
			if sub.Owner != p.Owner {
				continue
			}
			recipient, ok := r.principals[sub.PrincipalID]
			if !ok {
				continue
			}
			if _, err := r.hosted.subscription(recipient, sub.ID); err != nil {
				continue
			}
			ref := wire.RecipientRef{Kind: "subscription", SubscriptionID: sub.ID}
			expires := float64(sub.ExpiresAt.UnixMilli())
			items[ref.Key()] = wire.Recipient{Ref: ref, Name: sub.Name, Application: "chatgpt", Available: true, ExpiresAt: &expires}
		}
	}
	ids := make([]string, 0, len(items))
	for id := range items {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	last := ""
	for _, id := range ids {
		if len(out.Recipients) >= in.Limit {
			break
		}
		out.Recipients = append(out.Recipients, items[id])
		if size(out)+128 > wire.InspectionLimit {
			out.Recipients = out.Recipients[:len(out.Recipients)-1]
			if len(out.Recipients) == 0 {
				return out, failure(413, "Recipient exceeds response limit")
			}
			break
		}
		last = id
	}
	if len(out.Recipients) < len(ids) {
		id, err := randomID()
		if err != nil {
			return out, err
		}
		if len(r.recipientCursors) >= 1024 {
			var oldest string
			var expiry time.Time
			for k, c := range r.recipientCursors {
				if oldest == "" || c.expires.Before(expiry) {
					oldest, expiry = k, c.expires
				}
			}
			delete(r.recipientCursors, oldest)
		}
		r.recipientCursors[id] = cursor{owner: p.Owner, after: last, expires: time.Now().Add(expiryDuration)}
		out.NextCursor = &id
	}
	return out, nil
}

type journalRoute struct {
	Key, SubscriptionID, SubscriptionGeneration, Principal             string
	ReceiverID, ReceiverIncarnation, PublisherID, PublisherIncarnation string
	ReceiverPrincipal, PublisherPrincipal                              string
}
type journalMessage struct {
	Message                  wire.Message
	Owner, Sender, Recipient string
	Route, Reverse           journalRoute
	Created                  time.Time
	Replies                  []string
}
type hostedJournal struct {
	Messages []journalMessage
	Requests map[string]messageRequestRecord
}
type messageRequestRecord struct{ Payload, ID string }

func journalRouteOf(route messageRoute) journalRoute {
	j := journalRoute{Key: route.key, SubscriptionID: route.subscriptionID, SubscriptionGeneration: route.subscriptionGeneration, Principal: route.principal}
	if route.receiver != nil {
		j.ReceiverID, j.ReceiverIncarnation, j.ReceiverPrincipal = route.receiver.id, route.receiver.incarnation, route.receiver.principal
	}
	if route.publisher != nil {
		j.PublisherID, j.PublisherIncarnation, j.PublisherPrincipal = route.publisher.id, route.publisher.incarnation, route.publisher.principal
	}
	if route.receiver == nil {
		j.ReceiverID, j.ReceiverIncarnation, j.ReceiverPrincipal = route.receiverID, route.receiverIncarnation, route.receiverPrincipal
	}
	if route.publisher == nil {
		j.PublisherID, j.PublisherIncarnation, j.PublisherPrincipal = route.publisherID, route.publisherIncarnation, route.publisherPrincipal
	}
	return j
}

// ponytail: one inspector process owns this bounded journal; add a process lock if sharing the directory.
func routeFromJournal(j journalRoute) messageRoute {
	return messageRoute{key: j.Key, subscriptionID: j.SubscriptionID, subscriptionGeneration: j.SubscriptionGeneration, principal: j.Principal, receiverID: j.ReceiverID, receiverIncarnation: j.ReceiverIncarnation, receiverPrincipal: j.ReceiverPrincipal, publisherID: j.PublisherID, publisherIncarnation: j.PublisherIncarnation, publisherPrincipal: j.PublisherPrincipal}
}

func (r *Registry) saveHostedMessages() (bool, error) {
	j := hostedJournal{Messages: []journalMessage{}, Requests: map[string]messageRequestRecord{}}
	for _, m := range r.messages {
		if m.route.subscriptionID == "" && m.reverse.subscriptionID == "" {
			continue
		}
		j.Messages = append(j.Messages, journalMessage{m.message, m.owner, m.sender, m.recipient, journalRouteOf(m.route), journalRouteOf(m.reverse), m.created, m.replies})
	}
	for k, req := range r.messageRequests {
		for _, m := range j.Messages {
			if m.Message.ID == req.id {
				j.Requests[k] = messageRequestRecord{req.payload, req.id}
				break
			}
		}
	}
	b, err := json.Marshal(j)
	if err != nil {
		return false, err
	}
	if len(b) > maxPrivateBytes {
		return false, errors.New("hosted message journal exceeds bound")
	}
	dir := r.hosted.subscriptions.directory
	if err = checkDir(dir, true); err != nil {
		return false, err
	}
	path := filepath.Join(dir, "messages.json")
	if _, err = readPrivate(path, true); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	f, err := os.CreateTemp(dir, ".messages-")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	if r.journalAfterRename != nil {
		if r.journalAfterRename() != nil {
			return true, errors.New("hosted message journal commit outcome uncertain")
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return true, errors.New("hosted message journal commit outcome uncertain")
	}
	err = d.Sync()
	if e := d.Close(); err == nil {
		err = e
	}
	if err != nil {
		return true, errors.New("hosted message journal commit outcome uncertain")
	}
	return true, nil
}
func (r *Registry) loadHostedMessages() error {
	b, err := readPrivate(filepath.Join(r.hosted.subscriptions.directory, "messages.json"), true)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var j hostedJournal
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(&j) != nil || dec.Decode(new(any)) != io.EOF || len(j.Messages) > 1024 || len(j.Requests) > 1024 {
		return errors.New("invalid hosted message journal")
	}
	for _, m := range j.Messages {
		if wire.Validate(m.Message) != nil || m.Owner == "" || m.Sender == "" || m.Created.IsZero() || len(m.Replies) > 32 || r.messages[m.Message.ID] != nil {
			return errors.New("invalid hosted message journal")
		}
		if time.Since(m.Created) >= messageRetention {
			continue
		}
		if m.Message.Status == "dispatched" && m.Route.SubscriptionID != "" {
			m.Message.Status = "queued"
		}
		r.messages[m.Message.ID] = &messageEntry{message: m.Message, owner: m.Owner, sender: m.Sender, recipient: m.Recipient, route: routeFromJournal(m.Route), reverse: routeFromJournal(m.Reverse), created: m.Created, replies: m.Replies}
	}
	for k, req := range j.Requests {
		if r.messages[req.ID] != nil {
			r.messageRequests[k] = messageRequest{req.Payload, req.ID}
		}
	}
	return nil
}
func (r *Registry) runHostedMessages(ctx context.Context) {
	defer close(r.hostedDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r.mu.Lock()
		r.prune()
		if r.hostedErr != nil {
			r.mu.Unlock()
			continue
		}
		var target *messageEntry
		for _, m := range r.messages {
			if m.route.subscriptionID != "" && m.message.Status == "queued" && r.routeLive(m.route) {
				target = m
				break
			}
		}
		if target == nil {
			r.mu.Unlock()
			continue
		}
		target.message.Status = "dispatched"
		if committed, err := r.saveHostedMessages(); err != nil {
			if !committed {
				target.message.Status = "queued"
			}
			r.hostedErr = err
			r.mu.Unlock()
			continue
		}
		id, sub, generation, message := target.message.ID, target.route.subscriptionID, target.route.subscriptionGeneration, target.message
		r.mu.Unlock()
		err := r.hosted.deliver(ctx, sub, generation, message)
		r.mu.Lock()
		if r.messages[id] == target && target.message.Status == "dispatched" && target.route.subscriptionID == sub {
			if err == nil && r.routeLive(target.route) {
				target.message.Status = "accepted"
			} else {
				target.message.Status = "unknown"
			}
			if _, err := r.saveHostedMessages(); err != nil {
				r.hostedErr = err
			}
		}
		r.mu.Unlock()
	}
}
