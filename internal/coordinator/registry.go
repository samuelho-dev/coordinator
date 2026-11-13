package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string             { return e.Message }
func failure(status int, message string) error { return &HTTPError{status, message} }

const liveDuration = 30 * time.Second
const expiryDuration = 120 * time.Second
const requestDuration = 10 * time.Second

type connector struct {
	principal, owner, machine, id, incarnation string
	received                                   float64
	seen                                       time.Time
	disconnected                               bool
	retired                                    map[string]bool
}
type session struct {
	presence  wire.SessionPresence
	connector *connector
	claims    []wire.ProcessIdentity
}
type response struct {
	value wire.SessionInspection
	err   error
}
type pending struct {
	id, key     string
	ref         wire.SessionRef
	connector   *connector
	incarnation string
	deadline    time.Time
	delivered   bool
	reply       chan response
}
type cursor struct {
	owner, machine, repository, after string
	expires                           time.Time
}
type Registry struct {
	mu              sync.Mutex
	principals      map[string]wire.InspectorPrincipal
	hashes          map[string][]byte
	connectors      map[string]*connector
	sessions        map[string]*session
	pending         map[string]*pending
	cursors         map[string]cursor
	receivers       map[string]*connector
	messages        map[string]*messageEntry
	messageRequests map[string]messageRequest
	closed          bool
}

func NewRegistry(cfg wire.InspectorConfig) (*Registry, error) {
	r := &Registry{principals: map[string]wire.InspectorPrincipal{}, hashes: map[string][]byte{}, connectors: map[string]*connector{}, sessions: map[string]*session{}, pending: map[string]*pending{}, cursors: map[string]cursor{}}
	r.receivers = map[string]*connector{}
	r.messages = map[string]*messageEntry{}
	r.messageRequests = map[string]messageRequest{}
	seen := map[string]bool{}
	for _, p := range cfg.Principals {
		b, e := hex.DecodeString(p.TokenHash)
		if e != nil || len(b) != 32 || hex.EncodeToString(b) != p.TokenHash || p.ID == "" || p.Owner == "" || seen[p.TokenHash] {
			return nil, failure(500, "Ambiguous inspector credentials")
		}
		if _, ok := r.principals[p.ID]; ok {
			return nil, failure(500, "Ambiguous inspector credentials")
		}
		seen[p.TokenHash] = true
		r.principals[p.ID] = p
		r.hashes[p.ID] = b
	}
	return r, nil
}
func (r *Registry) Authorize(token string) (wire.InspectorPrincipal, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sum := sha256.Sum256([]byte(token))
	var found wire.InspectorPrincipal
	match := 0
	for id, h := range r.hashes {
		if subtle.ConstantTimeCompare(sum[:], h) == 1 {
			found = r.principals[id]
			match = 1
		}
	}
	if match == 0 || r.closed {
		return wire.InspectorPrincipal{}, failure(401, "Unauthorized")
	}
	return found, nil
}
func (r *Registry) auth(p wire.InspectorPrincipal) bool {
	x, ok := r.principals[p.ID]
	return !r.closed && ok && x == p
}
func key(a, b string) string { data, _ := json.Marshal([]string{a, b}); return string(data) }
func processKey(machine string, p wire.ProcessIdentity) string {
	b, _ := json.Marshal([]any{machine, p.PID, p.StartIdentity})
	return string(b)
}
func randomID() (string, error) {
	id, err := newUUID()
	if err != nil {
		return "", failure(500, "Inspector randomness unavailable")
	}
	return id, nil
}
func wall() float64          { return float64(time.Now().UnixMilli()) }
func live(c *connector) bool { return !c.disconnected && time.Since(c.seen) < liveDuration }
func conn(c *connector) *wire.SessionConnection {
	state := "stale"
	if live(c) {
		state = "live"
	}
	until := c.received + float64(liveDuration.Milliseconds())
	if c.disconnected {
		until = c.received
	}
	return &wire.SessionConnection{State: state, ReceivedAt: c.received, LiveUntil: until}
}
func (r *Registry) fail(p *pending, e error) { delete(r.pending, p.id); p.reply <- response{err: e} }
func (r *Registry) remove(id string) {
	delete(r.sessions, id)
	delete(r.receivers, id)
	r.invalidateMessages()
	for _, p := range r.pending {
		if p.key == id {
			r.fail(p, failure(409, "Native session disconnected"))
		}
	}
}
func (r *Registry) prune() {
	now := time.Now()
	for _, p := range r.pending {
		if !now.Before(p.deadline) {
			r.fail(p, failure(504, "Native session observation timed out"))
		}
	}
	for id, c := range r.connectors {
		if now.Sub(c.seen) >= expiryDuration {
			for k, s := range r.sessions {
				if s.connector == c {
					r.remove(k)
				}
			}
			delete(r.connectors, id)
		}
	}
	for id, c := range r.cursors {
		if !now.Before(c.expires) {
			delete(r.cursors, id)
		}
	}
	r.pruneMessages(now)
}
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, p := range r.pending {
		r.fail(p, failure(503, "Inspector disconnected"))
	}
	clear(r.sessions)
	clear(r.connectors)
	clear(r.cursors)
	clear(r.receivers)
	clear(r.messages)
	clear(r.messageRequests)
}
func size(v any) int { b, _ := json.Marshal(v); return len(b) }
func (r *Registry) Announce(p wire.InspectorPrincipal, in wire.ConnectorAnnounce) (wire.ConnectorAccepted, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ok := wire.ConnectorAccepted{Accepted: true}
	if !r.auth(p) {
		return ok, failure(401, "Unauthorized")
	}
	r.prune()
	if len(in.Receivers) > 256 || (len(in.Receivers) > 0 && !p.Messaging) {
		return ok, failure(403, "Messaging receiver not permitted")
	}
	if len(in.Sessions) > 256 {
		return ok, failure(413, "Native session capacity reached")
	}
	ids := map[string]bool{}
	for _, s := range in.Sessions {
		if s.Ref.MachineID != in.MachineID || (s.Parent != nil && s.Parent.MachineID != in.MachineID) {
			return ok, failure(403, "Machine binding mismatch")
		}
		id := key(p.Owner, s.Ref.Key())
		if ids[id] {
			return ok, failure(409, "Duplicate native session")
		}
		ids[id] = true
		s.Connection = &wire.SessionConnection{State: "live", ReceivedAt: wall(), LiveUntil: wall() + 30000}
		page := wire.SessionListOutput{Machines: []wire.ListMachine{wire.ProjectListMachine(s)}, Sessions: []wire.ListSession{wire.ProjectListSession(s)}}
		dummy := "00000000-0000-0000-0000-000000000000"
		page.NextCursor = &dummy
		if size(page)+512 > wire.InspectionLimit {
			return ok, failure(413, "Native session presence exceeds response limit")
		}
	}
	ck := key(p.Owner, in.ConnectorID)
	old := r.connectors[ck]
	if old != nil && (old.principal != p.ID || old.machine != in.MachineID) {
		return ok, failure(403, "Connector binding mismatch")
	}
	if old != nil && old.incarnation != in.Incarnation && (live(old) || old.retired[in.Incarnation] || len(old.retired) >= 16) {
		return ok, failure(409, "Connector incarnation conflict")
	}
	if old != nil && old.disconnected && old.incarnation == in.Incarnation {
		return ok, failure(409, "Connector incarnation retired")
	}
	if old == nil && len(r.connectors) >= 256 {
		return ok, failure(429, "Connector capacity reached")
	}
	n := len(r.sessions)
	for id, s := range r.sessions {
		if s.connector == old && !ids[id] {
			n--
		}
	}
	for id := range ids {
		s := r.sessions[id]
		if s == nil {
			n++
		} else if s.connector != old && live(s.connector) {
			return ok, failure(409, "Native session already has an active publisher")
		}
	}
	if n > 4096 {
		return ok, failure(429, "Native session capacity reached")
	}
	receiverIDs := map[string]bool{}
	for _, ref := range in.Receivers {
		id := key(p.Owner, ref.Key())
		if ref.MachineID != in.MachineID || receiverIDs[id] {
			return ok, failure(403, "Invalid receiver binding")
		}
		receiverIDs[id] = true
		s := r.sessions[id]
		if !ids[id] && s != nil && s.connector.principal != p.ID {
			return ok, failure(403, "Receiver principal mismatch")
		}
		if !ids[id] && (s == nil || !live(s.connector)) {
			return ok, failure(409, "Receiver requires matching live presence")
		}
		if ids[id] && s != nil && s.connector != old && live(s.connector) {
			return ok, failure(409, "Receiver presence publisher conflict")
		}
		if receiver := r.receivers[id]; receiver != nil && receiver != old && live(receiver) {
			return ok, failure(409, "Native session already has an active receiver")
		}
	}
	c := old
	if c == nil || c.incarnation != in.Incarnation {
		retired := map[string]bool{}
		if old != nil {
			retired = old.retired
			retired[old.incarnation] = true
			for id, s := range r.sessions {
				if s.connector == old {
					r.remove(id)
				}
			}
		}
		c = &connector{principal: p.ID, owner: p.Owner, machine: in.MachineID, id: in.ConnectorID, incarnation: in.Incarnation, retired: retired}
		r.connectors[ck] = c
	}
	c.received = wall()
	c.seen = time.Now()
	for id, s := range r.sessions {
		if s.connector == c && !ids[id] {
			r.remove(id)
		}
	}
	for _, presence := range in.Sessions {
		id := key(p.Owner, presence.Ref.Key())
		if s := r.sessions[id]; s != nil && s.connector != c {
			r.remove(id)
		}
		presence.Connection = conn(c)
		r.sessions[id] = &session{presence: presence, connector: c, claims: append([]wire.ProcessIdentity(nil), presence.Processes...)}
	}
	for id, receiver := range r.receivers {
		if receiver == c && !receiverIDs[id] {
			delete(r.receivers, id)
		}
	}
	for id := range receiverIDs {
		r.receivers[id] = c
	}
	r.invalidateMessages()
	return ok, nil
}
func (r *Registry) shared(owner string) map[string]bool {
	seen := map[string]bool{}
	shared := map[string]bool{}
	for _, s := range r.sessions {
		if s.connector.owner != owner || !live(s.connector) {
			continue
		}
		local := map[string]bool{}
		for _, p := range s.claims {
			k := processKey(s.presence.Ref.MachineID, p)
			if local[k] {
				continue
			}
			local[k] = true
			if seen[k] {
				shared[k] = true
			}
			seen[k] = true
		}
	}
	return shared
}
func normalized(s *session, shared map[string]bool, p wire.SessionPresence) wire.SessionPresence {
	p.Connection = conn(s.connector)
	p.Processes = append([]wire.ProcessIdentity(nil), p.Processes...)
	isShared := false
	for i := range p.Processes {
		q := &p.Processes[i]
		if shared[processKey(p.Ref.MachineID, *q)] {
			q.Ownership = "shared"
		}
		if q.Ownership == "shared" {
			isShared = true
		}
	}
	if p.Resources != nil {
		v := *p.Resources
		if v.Attribution == "shared" || isShared {
			v.Attribution = "shared"
			v.CPUPercent.Provenance.Shared = true
			v.CPUCapacityPercent.Provenance.Shared = true
			v.RSSBytes.Provenance.Shared = true
			v.RSSCapacityPercent.Provenance.Shared = true
		}
		if !live(s.connector) || wall()-v.ObservedAt >= 30000 {
			v = wire.StaleSessionResources(v)
		}
		p.Resources = &v
	}
	if p.Machine != nil && p.Machine.Resources != nil {
		m := *p.Machine
		v := *m.Resources
		if !live(s.connector) || wall()-v.ObservedAt >= 30000 {
			v = wire.StaleMachineResources(v)
		}
		m.Resources = &v
		p.Machine = &m
	}
	return p
}
func (r *Registry) List(p wire.InspectorPrincipal, in wire.SessionListInput) (wire.SessionListOutput, error) {
	out := wire.SessionListOutput{Machines: []wire.ListMachine{}, Sessions: []wire.ListSession{}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return out, failure(401, "Unauthorized")
	}
	if in.Limit < 1 || in.Limit > 100 {
		return out, failure(400, "Invalid inspection request or response")
	}
	r.prune()
	after := ""
	if in.Cursor != "" {
		c, ok := r.cursors[in.Cursor]
		if !ok || c.owner != p.Owner || c.machine != in.MachineID || c.repository != in.Repository {
			return out, failure(400, "Invalid session cursor")
		}
		after = c.after
	}
	ids := []string{}
	for id, s := range r.sessions {
		if s.connector.owner != p.Owner || id <= after || (in.MachineID != "" && s.presence.Ref.MachineID != in.MachineID) {
			continue
		}
		if in.Repository != "" && (s.presence.Workspace == nil || s.presence.Workspace.Git == nil || s.presence.Workspace.Git.State != "available" || s.presence.Workspace.Git.CommonDir != in.Repository) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	shared := r.shared(p.Owner)
	machines := map[string]wire.ListMachine{}
	order := []string{}
	next, e := randomID()
	if e != nil {
		return out, e
	}
	last := ""
	for _, id := range ids {
		if len(out.Sessions) >= in.Limit {
			break
		}
		presence := r.normalized(r.sessions[id], shared, r.sessions[id].presence)
		m := wire.ProjectListMachine(presence)
		mk := wire.ListMachineKey(m)
		old, exists := machines[mk]
		wire.MergeListMachines(machines, []wire.ListMachine{m})
		if !exists {
			order = append(order, mk)
		}
		out.Sessions = append(out.Sessions, wire.ProjectListSession(presence))
		out.Machines = out.Machines[:0]
		for _, k := range order {
			out.Machines = append(out.Machines, machines[k])
		}
		out.NextCursor = &next
		if size(out) > wire.InspectionLimit {
			out.Sessions = out.Sessions[:len(out.Sessions)-1]
			if exists {
				machines[mk] = old
			} else {
				delete(machines, mk)
				order = order[:len(order)-1]
			}
			out.Machines = out.Machines[:0]
			for _, k := range order {
				out.Machines = append(out.Machines, machines[k])
			}
			if len(out.Sessions) == 0 {
				return out, failure(413, "Native session presence exceeds response limit")
			}
			break
		}
		last = id
	}
	out.NextCursor = nil
	if len(out.Sessions) < len(ids) && last != "" {
		if len(r.cursors) >= 1024 {
			var oldest string
			var t time.Time
			for id, c := range r.cursors {
				if oldest == "" || c.expires.Before(t) {
					oldest = id
					t = c.expires
				}
			}
			delete(r.cursors, oldest)
		}
		r.cursors[next] = cursor{p.Owner, in.MachineID, in.Repository, last, time.Now().Add(expiryDuration)}
		out.NextCursor = &next
	}
	return out, nil
}
func (r *Registry) Inspect(ctx context.Context, p wire.InspectorPrincipal, ref wire.SessionRef) (wire.SessionInspection, error) {
	var zero wire.SessionInspection
	id, e := randomID()
	if e != nil {
		return zero, e
	}
	r.mu.Lock()
	if !r.auth(p) {
		r.mu.Unlock()
		return zero, failure(401, "Unauthorized")
	}
	r.prune()
	k := key(p.Owner, ref.Key())
	s := r.sessions[k]
	if s == nil {
		r.mu.Unlock()
		return zero, failure(404, "Native session not found")
	}
	if !live(s.connector) {
		r.mu.Unlock()
		return zero, failure(409, "Native session is stale")
	}
	count := 0
	for _, q := range r.pending {
		if q.connector == s.connector {
			count++
		}
	}
	if len(r.pending) >= 1024 || count >= 32 {
		r.mu.Unlock()
		return zero, failure(429, "Inspection capacity reached")
	}
	q := &pending{id: id, key: k, ref: ref, connector: s.connector, incarnation: s.connector.incarnation, deadline: time.Now().Add(requestDuration), reply: make(chan response, 1)}
	r.pending[id] = q
	r.mu.Unlock()
	timer := time.NewTimer(requestDuration)
	defer timer.Stop()
	defer func() { r.mu.Lock(); delete(r.pending, id); r.mu.Unlock() }()
	select {
	case v := <-q.reply:
		return v.value, v.err
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, failure(504, "Native session observation timed out")
	}
}
func (r *Registry) find(p wire.InspectorPrincipal, in wire.ConnectorPoll) *connector {
	c := r.connectors[key(p.Owner, in.ConnectorID)]
	if c == nil || c.principal != p.ID || c.incarnation != in.Incarnation || c.disconnected {
		return nil
	}
	return c
}
func (r *Registry) Poll(p wire.InspectorPrincipal, in wire.ConnectorPoll) (wire.ConnectorPollResult, error) {
	out := wire.ConnectorPollResult{Requests: []wire.InspectionRequest{}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return out, failure(401, "Unauthorized")
	}
	r.prune()
	c := r.find(p, in)
	if c == nil || !live(c) {
		return out, failure(409, "Connector must announce presence")
	}
	r.pollMessages(c, &out)
	for _, q := range r.pending {
		if q.connector != c || q.delivered || q.incarnation != c.incarnation {
			continue
		}
		out.Requests = append(out.Requests, wire.InspectionRequest{RequestID: q.id, Ref: q.ref})
		if size(out) > wire.InspectionLimit {
			out.Requests = out.Requests[:len(out.Requests)-1]
			break
		}
		q.delivered = true
		if len(out.Requests) >= 32 {
			break
		}
	}
	return out, nil
}
func (r *Registry) Complete(p wire.InspectorPrincipal, in wire.ConnectorResult) (wire.ConnectorAccepted, error) {
	ok := wire.ConnectorAccepted{Accepted: true}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return ok, failure(401, "Unauthorized")
	}
	r.prune()
	c := r.find(p, wire.ConnectorPoll{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation})
	q := r.pending[in.RequestID]
	if c == nil || q == nil || q.connector != c || q.incarnation != in.Incarnation || !q.delivered || !live(c) {
		return ok, failure(409, "Inspection request is no longer active")
	}
	var marker struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(in.Observation, &marker)
	if marker.Code != "" {
		v, e := wire.Decode[wire.ObservationError](bytes.NewReader(in.Observation), wire.InspectionLimit)
		if e != nil {
			return ok, failure(400, "Invalid inspection request or response")
		}
		status := 503
		if v.Code == "not-found" {
			status = 404
		}
		r.fail(q, failure(status, "Native session observation unavailable"))
		return ok, nil
	}
	v, e := wire.Decode[wire.SessionInspection](bytes.NewReader(in.Observation), wire.InspectionLimit)
	if e != nil {
		return ok, failure(400, "Invalid inspection request or response")
	}
	if v.Presence.Ref.Key() != q.ref.Key() {
		return ok, failure(409, "Inspection session mismatch")
	}
	s := r.sessions[q.key]
	if s == nil || s.connector != c {
		return ok, failure(409, "Native session publisher changed")
	}
	claims := map[string]wire.ProcessIdentity{}
	for _, x := range s.presence.Processes {
		claims[processKey("", x)] = x
	}
	for _, x := range v.Resources.Processes {
		claims[processKey("", x.Process)] = x.Process
	}
	if len(claims) > 256 {
		return ok, failure(413, "Native process identity capacity reached")
	}
	s.claims = nil
	for _, x := range claims {
		s.claims = append(s.claims, x)
	}
	shared := r.shared(p.Owner)
	ambiguous := false
	for _, x := range s.claims {
		if x.Ownership != "owned" || shared[processKey(s.presence.Ref.MachineID, x)] {
			ambiguous = true
		}
	}
	seen := map[string]bool{}
	rows := make([]wire.ProcessObservation, 0, len(v.Resources.Processes))
	for _, x := range v.Resources.Processes {
		k := processKey("", x.Process)
		if seen[k] {
			ambiguous = true
			continue
		}
		seen[k] = true
		isShared := x.Process.Ownership == "shared" || shared[processKey(s.presence.Ref.MachineID, x.Process)]
		if x.Process.Ownership != "owned" || isShared {
			ambiguous = true
		}
		if isShared {
			x.Process.Ownership = "shared"
			x.CPUPercent.Provenance.Shared = true
			x.CPUTimeMs.Provenance.Shared = true
			x.RSSBytes.Provenance.Shared = true
		}
		rows = append(rows, x)
	}
	v.Resources.Processes = rows
	if ambiguous {
		for _, m := range []*wire.Metric{&v.Resources.OwnedProcessCount, &v.Resources.OwnedCPUPercent, &v.Resources.OwnedRSSBytes} {
			prov := m.Provenance
			prov.Shared = true
			prov.Partial = true
			*m = wire.UnavailableMetric(m.Unit, "Process ownership is shared or unattributable", prov)
		}
	}
	v.Presence = r.normalized(s, shared, v.Presence)
	v.QueriedAt = wall()
	if size(v) > wire.InspectionLimit {
		r.fail(q, failure(413, "Native observation exceeds response limit"))
		return ok, failure(413, "Native observation exceeds response limit")
	}
	delete(r.pending, q.id)
	q.reply <- response{value: v}
	return ok, nil
}
func (r *Registry) Disconnect(p wire.InspectorPrincipal, in wire.ConnectorDisconnect) (wire.ConnectorAccepted, error) {
	ok := wire.ConnectorAccepted{Accepted: true}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.auth(p) {
		return ok, failure(401, "Unauthorized")
	}
	r.prune()
	c := r.find(p, in)
	if c == nil {
		return ok, failure(409, "Connector incarnation mismatch")
	}
	c.disconnected = true
	c.received = wall()
	r.invalidateMessages()
	for _, q := range r.pending {
		if q.connector == c {
			r.fail(q, failure(409, "Native session disconnected"))
		}
	}
	return ok, nil
}
