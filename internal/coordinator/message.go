package coordinator

import (
	"encoding/json"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

const messageTTL = 10 * time.Minute
const messageRetention = 30 * time.Minute

type messageRoute struct {
	key                                                                string
	subscriptionID, subscriptionGeneration, principal                  string
	receiverID, receiverIncarnation, publisherID, publisherIncarnation string
	receiverPrincipal, publisherPrincipal                              string
	receiver, publisher                                                *connector
}
type messageEntry struct {
	message                  wire.Message
	owner, sender, recipient string
	route, reverse           messageRoute
	created                  time.Time
	replies                  []string
}
type messageRequest struct{ payload, id string }

func (r *Registry) route(owner string, ref wire.SessionRef) messageRoute {
	k := key(owner, ref.Key())
	s, c := r.sessions[k], r.receivers[k]
	if s == nil || c == nil || !live(s.connector) || !live(c) || s.connector.principal != c.principal || s.connector.machine != c.machine {
		return messageRoute{}
	}
	return messageRoute{key: k, receiver: c, publisher: s.connector}
}
func (r *Registry) routeRecipient(p wire.InspectorPrincipal, ref wire.RecipientRef) messageRoute {
	switch {
	case ref.Kind == "session" && ref.Session != nil && ref.SubscriptionID == "":
		return r.route(p.Owner, *ref.Session)
	case ref.Kind == "subscription" && ref.Session == nil && ref.SubscriptionID != "" && r.hosted != nil:
		sub, err := r.hosted.subscription(p, ref.SubscriptionID)
		if err == nil {
			return messageRoute{key: "subscription:" + sub.ID, subscriptionID: sub.ID, subscriptionGeneration: sub.Generation, principal: sub.PrincipalID}
		}
	}
	return messageRoute{}
}
func (r *Registry) restoreRoute(owner string, route messageRoute) messageRoute {
	if route.receiver != nil || route.subscriptionID != "" || route.receiverID == "" || route.publisherID == "" {
		return route
	}
	receiver := r.connectors[key(owner, route.receiverID)]
	publisher := r.connectors[key(owner, route.publisherID)]
	if receiver == nil || publisher == nil || receiver.incarnation != route.receiverIncarnation || publisher.incarnation != route.publisherIncarnation || receiver.principal != route.receiverPrincipal || publisher.principal != route.publisherPrincipal {
		return route
	}
	route.receiver, route.publisher = receiver, publisher
	if !r.routeLive(route) {
		return messageRoute{key: route.key, receiverID: route.receiverID, receiverIncarnation: route.receiverIncarnation, receiverPrincipal: route.receiverPrincipal, publisherID: route.publisherID, publisherIncarnation: route.publisherIncarnation, publisherPrincipal: route.publisherPrincipal}
	}
	return route
}
func (r *Registry) routeLive(route messageRoute) bool {
	if route.subscriptionID != "" {
		p, ok := r.principals[route.principal]
		if !ok || r.hosted == nil {
			return false
		}
		sub, err := r.hosted.subscription(p, route.subscriptionID)
		return err == nil && route.subscriptionGeneration != "" && sub.Generation == route.subscriptionGeneration
	}
	if route.receiver == nil || route.publisher == nil || !live(route.receiver) || !live(route.publisher) {
		return false
	}
	s := r.sessions[route.key]
	return s != nil && s.connector == route.publisher && r.receivers[route.key] == route.receiver && route.receiver.principal == route.publisher.principal
}
func (r *Registry) normalized(s *session, shared map[string]bool, p wire.SessionPresence) wire.SessionPresence {
	p = normalized(s, shared, p)
	p.Messaging = r.route(s.connector.owner, p.Ref).receiver != nil
	return p
}
func (r *Registry) caller(p wire.InspectorPrincipal, caller *wire.MessageCaller) (messageRoute, error) {
	if caller == nil {
		return messageRoute{}, nil
	}
	route := r.route(p.Owner, caller.Ref)
	if route.receiver == nil || route.receiver.principal != p.ID || route.receiver.id != caller.ConnectorID || route.receiver.incarnation != caller.Incarnation {
		return messageRoute{}, failure(403, "Invalid message caller binding")
	}
	return route, nil
}
func (r *Registry) unresolvedRoute(owner string, route messageRoute) bool {
	if route.subscriptionID != "" || route.receiver != nil || route.receiverID == "" || route.publisherID == "" {
		return false
	}
	for _, binding := range []struct{ id, incarnation, principal string }{
		{route.receiverID, route.receiverIncarnation, route.receiverPrincipal},
		{route.publisherID, route.publisherIncarnation, route.publisherPrincipal},
	} {
		c := r.connectors[key(owner, binding.id)]
		if c != nil && (c.incarnation != binding.incarnation || c.principal != binding.principal || c.disconnected) {
			return false
		}
	}
	if s := r.sessions[route.key]; s != nil && (s.connector.id != route.publisherID || s.connector.incarnation != route.publisherIncarnation || s.connector.principal != route.publisherPrincipal) {
		return false
	}
	if c := r.receivers[route.key]; c != nil && (c.id != route.receiverID || c.incarnation != route.receiverIncarnation || c.principal != route.receiverPrincipal) {
		return false
	}
	return true
}
func (r *Registry) invalidateMessages() {
	dirty := false
	for _, m := range r.messages {
		hosted := m.route.subscriptionID != "" || m.reverse.subscriptionID != ""
		m.route = r.restoreRoute(m.owner, m.route)
		m.reverse = r.restoreRoute(m.owner, m.reverse)
		if (m.message.Status == "queued" || m.message.Status == "dispatched") && !r.routeLive(m.route) && !r.unresolvedRoute(m.owner, m.route) {
			m.message.Status = "unknown"
			if hosted && m.route.subscriptionID == "" {
				m.route = messageRoute{}
			}
			dirty = dirty || hosted
		}
		if (m.reverse.receiver != nil && !r.routeLive(m.reverse)) || (m.reverse.receiverID != "" && m.reverse.receiver == nil && !r.unresolvedRoute(m.owner, m.reverse)) {
			m.reverse = messageRoute{}
			dirty = dirty || hosted
		}
	}
	if dirty && r.hosted != nil && r.hostedErr == nil {
		_, r.hostedErr = r.saveHostedMessages()
	}
}
func (r *Registry) pruneMessages(now time.Time) {
	for id, c := range r.receivers {
		s := r.sessions[id]
		if s == nil || !live(c) || !live(s.connector) || c.principal != s.connector.principal {
			delete(r.receivers, id)
		}
	}
	r.invalidateMessages()
	dirty := false
	for id, m := range r.messages {
		if now.Sub(m.created) >= messageRetention {
			dirty = dirty || m.route.subscriptionID != "" || m.reverse.subscriptionID != ""
			delete(r.messages, id)
			continue
		}
		if now.Sub(m.created) >= messageTTL {
			if m.message.Status == "queued" {
				dirty = dirty || m.route.subscriptionID != "" || m.reverse.subscriptionID != ""
				m.message.Status = "expired"
			}
			if m.message.Status == "dispatched" {
				dirty = dirty || m.route.subscriptionID != "" || m.reverse.subscriptionID != ""
				m.message.Status = "unknown"
			}
		}
	}
	for k, req := range r.messageRequests {
		if r.messages[req.id] == nil {
			delete(r.messageRequests, k)
		}
	}
	if dirty && r.hosted != nil && r.hostedErr == nil {
		_, r.hostedErr = r.saveHostedMessages()
	}
}
func (r *Registry) pollMessages(c *connector, out *wire.ConnectorPollResult) {
	for _, m := range r.messages {
		if m.message.Status != "queued" || m.route.receiver != c || !r.routeLive(m.route) {
			continue
		}
		m.message.Status = "dispatched"
		out.Messages = append(out.Messages, m.message)
		if size(out) > wire.InspectionLimit {
			out.Messages = out.Messages[:len(out.Messages)-1]
			m.message.Status = "queued"
			break
		}
		if len(out.Messages) >= 32 {
			break
		}
	}
}
func (r *Registry) messageAuth(p wire.InspectorPrincipal) error {
	if !r.auth(p) {
		return failure(401, "Unauthorized")
	}
	if r.hostedErr != nil {
		return r.hostedErr
	}
	if !p.Messaging {
		return failure(403, "Messaging not permitted")
	}
	return nil
}
func (r *Registry) previous(p wire.InspectorPrincipal, caller *wire.MessageCaller, op, id string, payload any) (wire.Message, bool, error) {
	if id == "" {
		return wire.Message{}, false, nil
	}
	req, ok := r.messageRequests[requestKey(p, caller, op, id)]
	if !ok {
		return wire.Message{}, false, nil
	}
	b, _ := json.Marshal(payload)
	if req.payload != string(b) {
		return wire.Message{}, true, failure(409, "Message request ID conflict")
	}
	return r.messages[req.id].message, true, nil
}
func (r *Registry) record(m *messageEntry) wire.MessageRecord {
	out := wire.MessageRecord{Message: m.message, Replies: []wire.Message{}}
	for _, id := range m.replies {
		if child := r.messages[id]; child != nil {
			out.Replies = append(out.Replies, child.message)
		}
	}
	return out
}
func requestKey(p wire.InspectorPrincipal, caller *wire.MessageCaller, op, id string) string {
	b, _ := json.Marshal([]any{p.ID, caller, op, id})
	return string(b)
}
func (r *Registry) admit(p wire.InspectorPrincipal, caller *wire.MessageCaller, op, requestID string, payload any, text string, to *wire.RecipientRef, route, reverse messageRoute, parent *messageEntry) (wire.Message, error) {
	var zero wire.Message
	b, _ := json.Marshal(payload)
	k := requestKey(p, caller, op, requestID)
	if requestID != "" {
		if req, ok := r.messageRequests[k]; ok {
			if req.payload != string(b) {
				return zero, failure(409, "Message request ID conflict")
			}
			return r.messages[req.id].message, nil
		}
	}
	if len(r.messages) >= 1024 {
		return zero, failure(429, "Message capacity reached")
	}
	if route.receiver != nil || route.subscriptionID != "" {
		count := 0
		for _, m := range r.messages {
			if m.route.key == route.key && (m.message.Status == "queued" || m.message.Status == "dispatched") {
				count++
			}
		}
		if count >= 32 {
			return zero, failure(429, "Target message capacity reached")
		}
	}
	if parent != nil && len(parent.replies) >= 32 {
		return zero, failure(429, "Message reply capacity reached")
	}
	id, err := randomID()
	if err != nil {
		return zero, err
	}
	now := time.Now()
	m := wire.Message{ID: id, Sender: "unbound", To: to, Text: text, CreatedAt: float64(now.UnixMilli()), ExpiresAt: float64(now.Add(messageTTL).UnixMilli()), Status: "queued"}
	if caller != nil {
		ref := wire.SessionRecipient(caller.Ref)
		m.From = &ref
		m.Sender = caller.Ref.NativeSessionID
	}
	if caller == nil && p.TokenHash == "" {
		m.Sender = p.ID
	}
	if parent != nil {
		m.InReplyTo = parent.message.ID
	}
	if to == nil {
		m.Status = "accepted"
	}
	if wire.Validate(m) != nil || size(m)+5 > 16*1024 {
		return zero, failure(413, "Invalid or oversized message")
	}
	if parent != nil {
		record := r.record(parent)
		record.Replies = append(record.Replies, m)
		if size(record)+5*(len(record.Replies)+1) > wire.InspectionLimit {
			return zero, failure(413, "Message record exceeds response limit")
		}
	}
	recipient := ""
	if route.receiver != nil {
		recipient = route.receiver.principal
	} else if route.subscriptionID != "" {
		recipient = route.principal
	} else if parent != nil {
		recipient = parent.sender
	}
	r.messages[id] = &messageEntry{message: m, owner: p.Owner, sender: p.ID, recipient: recipient, route: route, reverse: reverse, created: now}
	if parent != nil {
		parent.replies = append(parent.replies, id)
	}
	if requestID != "" {
		r.messageRequests[k] = messageRequest{payload: string(b), id: id}
	}
	if r.hosted != nil && (route.subscriptionID != "" || reverse.subscriptionID != "" || parent != nil && (parent.route.subscriptionID != "" || parent.reverse.subscriptionID != "")) {
		if committed, err := r.saveHostedMessages(); err != nil {
			r.hostedErr = err
			if !committed {
				delete(r.messages, id)
				delete(r.messageRequests, k)
				if parent != nil {
					parent.replies = parent.replies[:len(parent.replies)-1]
				}
			}
			return zero, err
		}
	}
	return m, nil
}
func (r *Registry) SendMessage(p wire.InspectorPrincipal, in wire.MessageSendInput) (wire.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.messageAuth(p); e != nil {
		return wire.Message{}, e
	}
	r.prune()
	if r.hostedErr != nil {
		return wire.Message{}, r.hostedErr
	}
	if wire.Validate(in) != nil {
		return wire.Message{}, failure(400, "Invalid message")
	}
	reverse, e := r.caller(p, in.Caller)
	if e != nil {
		return wire.Message{}, e
	}
	if m, found, err := r.previous(p, in.Caller, "send", in.RequestID, in); found {
		return m, err
	}
	route := r.routeRecipient(p, in.To)
	if route.receiver == nil && route.subscriptionID == "" {
		return wire.Message{}, failure(409, "Target does not support messaging")
	}
	return r.admit(p, in.Caller, "send", in.RequestID, in, in.Text, &in.To, route, reverse, nil)
}
func (r *Registry) ReplyMessage(p wire.InspectorPrincipal, in wire.MessageReplyInput) (wire.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.messageAuth(p); e != nil {
		return wire.Message{}, e
	}
	r.prune()
	if r.hostedErr != nil {
		return wire.Message{}, r.hostedErr
	}
	if wire.Validate(in) != nil || in.Caller == nil {
		return wire.Message{}, failure(400, "Invalid message reply")
	}
	caller, e := r.caller(p, in.Caller)
	if e != nil {
		return wire.Message{}, e
	}
	parent := r.messages[in.ReplyID]
	if parent == nil || parent.owner != p.Owner {
		return wire.Message{}, failure(404, "Message not found")
	}
	parent.route = r.restoreRoute(parent.owner, parent.route)
	if parent.recipient != p.ID || caller.key != parent.route.key || caller.receiver != parent.route.receiver || caller.publisher != parent.route.publisher || !r.routeLive(parent.route) {
		return wire.Message{}, failure(403, "Message reply caller mismatch")
	}
	if m, found, err := r.previous(p, in.Caller, "reply", in.RequestID, in); found {
		return m, err
	}
	if parent.message.Status == "queued" || parent.message.Status == "expired" || parent.message.Status == "refused" {
		return wire.Message{}, failure(409, "Message was not delivered")
	}
	var to *wire.RecipientRef
	if parent.message.From != nil {
		if !r.routeLive(parent.reverse) {
			return wire.Message{}, failure(409, "Message reply destination unavailable")
		}
		ref := *parent.message.From
		to = &ref
	} else if parent.reverse.subscriptionID != "" {
		if !r.routeLive(parent.reverse) {
			return wire.Message{}, failure(409, "Message reply destination unavailable")
		}
		ref := wire.RecipientRef{Kind: "subscription", SubscriptionID: parent.reverse.subscriptionID}
		to = &ref
	}
	return r.admit(p, in.Caller, "reply", in.RequestID, in, in.Text, to, parent.reverse, caller, parent)
}
func (r *Registry) ReplyHostedMessage(p wire.InspectorPrincipal, in wire.MessageReplyInput) (wire.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.messageAuth(p); err != nil {
		return wire.Message{}, err
	}
	r.prune()
	if r.hostedErr != nil {
		return wire.Message{}, r.hostedErr
	}
	if wire.Validate(in) != nil || in.Caller != nil {
		return wire.Message{}, failure(400, "Invalid message reply")
	}
	parent := r.messages[in.ReplyID]
	if parent == nil || parent.owner != p.Owner {
		return wire.Message{}, failure(404, "Message not found")
	}
	if parent.recipient != p.ID || parent.route.subscriptionID == "" || !r.routeLive(parent.route) {
		return wire.Message{}, failure(403, "Message reply caller mismatch")
	}
	if m, found, err := r.previous(p, nil, "hosted-reply", in.RequestID, in); found {
		return m, err
	}
	if parent.message.Status != "accepted" {
		return wire.Message{}, failure(409, "Message was not delivered")
	}
	parent.reverse = r.restoreRoute(parent.owner, parent.reverse)
	if parent.message.From == nil || !r.routeLive(parent.reverse) {
		return wire.Message{}, failure(409, "Message reply destination unavailable")
	}
	to := *parent.message.From
	return r.admit(p, nil, "hosted-reply", in.RequestID, in, in.Text, &to, parent.reverse, parent.route, parent)
}
func (r *Registry) GetMessage(p wire.InspectorPrincipal, in wire.MessageGetInput) (wire.MessageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return wire.MessageRecord{}, failure(401, "Unauthorized")
	}
	if p.TokenHash != "" && !p.Messaging {
		return wire.MessageRecord{}, failure(403, "Messaging not permitted")
	}
	r.prune()
	if r.hostedErr != nil {
		return wire.MessageRecord{}, r.hostedErr
	}
	if wire.Validate(in) != nil {
		return wire.MessageRecord{}, failure(400, "Invalid message ID")
	}
	m := r.messages[in.MessageID]
	if m == nil || m.owner != p.Owner || (m.sender != p.ID && m.recipient != p.ID) {
		return wire.MessageRecord{}, failure(404, "Message not found")
	}
	return r.record(m), nil
}
func (r *Registry) AckMessage(p wire.InspectorPrincipal, in wire.ConnectorMessageAck) (wire.ConnectorAccepted, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ok := wire.ConnectorAccepted{Accepted: true}
	if e := r.messageAuth(p); e != nil {
		return ok, e
	}
	r.prune()
	if r.hostedErr != nil {
		return ok, r.hostedErr
	}
	if wire.Validate(in) != nil {
		return ok, failure(400, "Invalid message acknowledgement")
	}
	m := r.messages[in.MessageID]
	c := r.find(p, wire.ConnectorPoll{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation})
	if m == nil || c == nil || m.route.receiver != c || !r.routeLive(m.route) {
		return ok, failure(409, "Message acknowledgement fenced")
	}
	if m.message.Status == in.Status {
		return ok, nil
	}
	if m.message.Status != "dispatched" {
		return ok, failure(409, "Message is not dispatched")
	}
	m.message.Status = in.Status
	if r.hosted != nil && (m.route.subscriptionID != "" || m.reverse.subscriptionID != "") {
		if _, err := r.saveHostedMessages(); err != nil {
			r.hostedErr = err
			return ok, err
		}
	}
	return ok, nil
}
