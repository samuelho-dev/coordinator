package coordinator

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func connectorID() (string, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type Source struct {
	Snapshot  func(context.Context) ([]wire.SessionPresence, error)
	Inspect   func(context.Context, wire.SessionRef) (wire.SessionInspection, error)
	Deliver   func(context.Context, wire.Message) (string, error)
	Bind      func(wire.ConnectorPoll)
	Receivers func(context.Context) ([]wire.SessionRef, error)
}

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func RunSource(ctx context.Context, cfg Config, source Source, notice func(wire.NativeNotice)) error {
	if source.Snapshot == nil || source.Inspect == nil {
		return errors.New("source callbacks required")
	}
	id, err := connectorID()
	if err != nil {
		return err
	}
	incarnation, err := connectorID()
	if err != nil {
		return err
	}
	identity := wire.ConnectorPoll{ConnectorID: id, Incarnation: incarnation}
	if source.Bind != nil {
		source.Bind(identity)
	}
	token, err := Token(cfg.ConnectorConfig)
	if err != nil {
		return err
	}
	client, err := NewClient(cfg.ConnectorConfig)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		var accepted wire.ConnectorAccepted
		_ = client.post(c, "/connector/disconnect", identity, &accepted)
	}()
	var failed, healthy time.Time
	unavailable, reported := false, false
	backoff := 2 * time.Second
	report := func(n wire.NativeNotice) {
		if notice != nil {
			notice(n)
		}
	}
	failure := func(operation string, e error) {
		healthy = time.Time{}
		if failed.IsZero() {
			failed = time.Now()
		}
		if !unavailable && time.Since(failed) >= 5*time.Second {
			unavailable = true
			n := wire.NativeNotice{State: "unavailable", Operation: operation, Code: "error"}
			var h *HTTPError
			if errors.As(e, &h) {
				n.Code = "HttpError"
				n.Status = &h.Status
			}
			report(n)
		}
	}
	success := func() {
		if healthy.IsZero() {
			healthy = time.Now()
		}
		if time.Since(healthy) >= 10*time.Second {
			if unavailable || !reported {
				report(wire.NativeNotice{State: "recovered", Operation: "connection"})
				reported = true
			}
			unavailable = false
			failed = time.Time{}
			backoff = 2 * time.Second
		}
	}
	for ctx.Err() == nil {
		cycle, cancel := context.WithCancel(ctx)
		errs := make(chan error, 3)
		announce := func() error {
			sessions, e := source.Snapshot(cycle)
			if e != nil {
				return e
			}
			if len(sessions) > 256 {
				return errors.New("presence exceeds bound")
			}
			if sessions == nil {
				sessions = []wire.SessionPresence{}
			}
			for i := range sessions {
				sessions[i].Messaging = false
			}
			var receivers []wire.SessionRef
			if cfg.Messaging && source.Deliver != nil && source.Receivers != nil {
				receivers, e = source.Receivers(cycle)
				if e != nil {
					return e
				}
				if len(receivers) > 256 {
					return errors.New("receivers exceed bound")
				}
			}
			var out wire.ConnectorAccepted
			return client.post(cycle, "/connector/announce", wire.ConnectorAnnounce{ConnectorID: id, Incarnation: incarnation, MachineID: cfg.MachineID, Sessions: sessions, Receivers: receivers}, &out)
		}
		if err = announce(); err != nil {
			cancel()
		} else {
			requests := make(chan wire.InspectionRequest, 256)
			go func() {
				for pause(cycle, 10*time.Second) {
					if e := announce(); e != nil {
						errs <- e
						return
					}
				}
				errs <- cycle.Err()
			}()
			go func() {
				for {
					var request wire.InspectionRequest
					select {
					case <-cycle.Done():
						errs <- cycle.Err()
						return
					case request = <-requests:
					}
					detail, c := context.WithTimeout(cycle, 8*time.Second)
					observation, e := source.Inspect(detail, request.Ref)
					c()
					if cycle.Err() != nil {
						errs <- cycle.Err()
						return
					}
					var payload []byte
					if e != nil {
						code := "unavailable"
						if errors.Is(e, context.DeadlineExceeded) {
							code = "timeout"
						}
						var h *HTTPError
						if errors.As(e, &h) && h.Status == 404 {
							code = "not-found"
						}
						payload, _ = json.Marshal(wire.ObservationError{Code: code, Message: "Native session observation unavailable"})
					} else {
						payload, e = json.Marshal(observation)
						if e != nil || len(payload) > wire.InspectionLimit {
							payload, _ = json.Marshal(wire.ObservationError{Code: "invalid-observation", Message: "Native session observation unavailable"})
						}
					}
					var out wire.ConnectorAccepted
					if e = client.post(cycle, "/connector/result", wire.ConnectorResult{ConnectorID: id, Incarnation: incarnation, RequestID: request.RequestID, Observation: payload}, &out); e != nil {
						errs <- e
						return
					}
				}
			}()
			go func() {
				for cycle.Err() == nil {
					var pending wire.ConnectorPollResult
					if e := client.post(cycle, "/connector/poll", identity, &pending); e != nil {
						errs <- e
						return
					}
					for _, message := range pending.Messages {
						status := "unknown"
						if cfg.Messaging && source.Deliver != nil {
							delivery, done := context.WithTimeout(cycle, 8*time.Second)
							result, e := source.Deliver(delivery, message)
							if e == nil && delivery.Err() == nil && (result == "accepted" || result == "refused" || result == "unknown") {
								status = result
							}
							done()
						}
						_, e := client.AckMessage(cycle, wire.ConnectorMessageAck{ConnectorID: id, Incarnation: incarnation, MessageID: message.ID, Status: status})
						if e != nil {
							errs <- e
							return
						}
					}
					for _, request := range pending.Requests {
						select {
						case requests <- request:
						case <-cycle.Done():
							errs <- cycle.Err()
							return
						}
					}
					success()
					if !pause(cycle, time.Second) {
						break
					}
				}
				errs <- cycle.Err()
			}()
			err = <-errs
			cancel()
			<-errs
			<-errs
		}
		if ctx.Err() != nil {
			return nil
		}
		failure("connection", err)
		var h *HTTPError
		if errors.As(err, &h) {
			if h.Status == 401 || h.Status == 403 {
				next, e := Token(cfg.ConnectorConfig)
				if e != nil || next == token {
					return err
				}
				token = next
			} else if h.Status != 409 && h.Status != 429 && h.Status < 500 {
				return err
			}
		}
		jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()))
		if jitter > 30*time.Second {
			jitter = 30 * time.Second
		}
		if !pause(ctx, jitter) {
			return nil
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	return nil
}
func RunConnector(ctx context.Context, cfg Config) error {
	sampler := observe.NewSampler()
	var mu sync.RWMutex
	latest := map[string]wire.SessionInspection{}
	source := Source{}
	source.Snapshot = func(ctx context.Context) ([]wire.SessionPresence, error) {
		resources := sampler.ReadMachine(ctx, 0)
		cached, err := CacheRead(ctx, cfg, &resources)
		if err != nil {
			return nil, err
		}
		next := make(map[string]wire.SessionInspection, len(cached))
		order := make([]string, 0, len(cached))
		for _, item := range cached {
			key := item.Presence.Ref.Key()
			next[key] = item
			order = append(order, key)
		}
		roster, _ := claudeRoster(ctx, cfg, sampler, next)
		for _, p := range roster {
			key := p.Ref.Key()
			if old, ok := next[key]; ok {
				merged := old.Presence
				merged.Processes = p.Processes
				merged.Activity = p.Activity
				merged.ProgramStatus = p.ProgramStatus
				if p.Agent != nil {
					merged.Agent = p.Agent
				}
				if p.Workspace != nil {
					merged.Workspace = p.Workspace
				}
				if p.Machine != nil {
					merged.Machine = p.Machine
				}
				old.Presence = merged
				next[key] = old
			} else {
				next[key] = wire.CreateInspection(p, float64(time.Now().UnixMilli()))
				order = append(order, key)
			}
		}
		presences := make([]wire.SessionPresence, 0, len(order))
		for _, key := range order {
			presences = append(presences, next[key].Presence)
		}
		observed, err := sampler.Enrich(ctx, presences)
		if err != nil {
			return nil, err
		}
		for _, p := range observed {
			item := next[p.Ref.Key()]
			item.Presence = p
			next[p.Ref.Key()] = item
		}
		mu.Lock()
		latest = next
		mu.Unlock()
		return observed, nil
	}
	source.Inspect = func(ctx context.Context, ref wire.SessionRef) (wire.SessionInspection, error) {
		mu.RLock()
		item, ok := latest[ref.Key()]
		mu.RUnlock()
		if !ok {
			return wire.SessionInspection{}, fmt.Errorf("session unavailable")
		}
		item.QueriedAt = float64(time.Now().UnixMilli())
		if item.Presence.Machine == nil || item.Presence.Machine.Resources == nil || item.Presence.Machine.Resources.ScopeID == nil || item.Presence.Machine.Resources.CPU.CapacityCores.Provenance.Stale {
			return item, nil
		}
		return sampler.Inspect(ctx, item)
	}
	return RunSource(ctx, cfg, source, nil)
}
