package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func RunNative(ctx context.Context, cfg Config, harness string, pid int, in io.Reader, out io.Writer) error {
	if (harness != "pi" && harness != "omp") || pid <= 0 || pid != os.Getppid() {
		return errors.New("invalid native host")
	}
	host, err := observe.Identify(ctx, pid)
	if err != nil {
		return err
	}
	observer, err := newNativeObserver(cfg.MachineID, pid, harness)
	if err != nil {
		return err
	}
	client, err := NewClient(cfg.ConnectorConfig)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sampler := observe.NewSampler()
	var mu sync.Mutex
	pending := map[string]chan wire.NativeMessage{}
	var observerMu sync.Mutex
	writes := make(chan wire.NativeMessage, 32)
	writeErr := make(chan error, 1)
	send := func(m wire.NativeMessage) error {
		m.Version = 1
		select {
		case writes <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("native output queue full")
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-writes:
				b, e := json.Marshal(m)
				if e == nil && len(b) > 65536 {
					e = errors.New("native output exceeds bound")
				}
				if e == nil {
					b = append(b, '\n')
					var n int
					n, e = out.Write(b)
					if e == nil && n != len(b) {
						e = io.ErrShortWrite
					}
				}
				if e != nil {
					select {
					case writeErr <- e:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	checkHost := func(ctx context.Context) error {
		if os.Getppid() != pid {
			return errors.New("native host exited")
		}
		now, e := observe.Identify(ctx, pid)
		if e != nil || now.StartIdentity != host.StartIdentity {
			return errors.New("native host identity changed")
		}
		return nil
	}
	var initialized bool
	update := func(s *wire.NativeSnapshot, active bool) error {
		if s == nil || s.Harness != harness {
			return errors.New("invalid snapshot")
		}
		if e := wire.Validate(*s); e != nil {
			return e
		}
		if e := checkHost(ctx); e != nil {
			return e
		}
		observerMu.Lock()
		defer observerMu.Unlock()
		if initialized && !active {
			p, e := observer.presence()
			if e != nil {
				return e
			}
			if p.Ref.NativeSessionID != s.NativeSessionID || p.Ref.NativeAgentID != s.NativeAgentID {
				return errors.New("native session changed")
			}
		}
		observer.update(*s, active)
		initialized = true
		if harness == "pi" {
			p, e := observer.presence()
			if e == nil && p.ProgramStatus != nil {
				_ = send(wire.NativeMessage{Method: "status", Status: p.ProgramStatus})
			}
		}
		return nil
	}
	var workspaceMu sync.Mutex
	var previousWorkspace *wire.SessionWorkspace
	enrichPresence := func(ctx context.Context, p wire.SessionPresence) (wire.SessionPresence, error) {
		if e := checkHost(ctx); e != nil {
			return p, e
		}
		resources := sampler.ReadMachine(ctx, pid)
		if e := checkHost(ctx); e != nil {
			return p, e
		}
		name, _ := os.Hostname()
		p.Machine = &wire.SessionMachine{Name: name, OS: &wire.MachineOS{Platform: runtime.GOOS, Architecture: runtime.GOARCH}, Resources: &resources}
		if p.Workspace != nil {
			workspaceMu.Lock()
			defer workspaceMu.Unlock()
			var project *wire.SessionProject
			if cfg.Project != nil {
				project = &wire.SessionProject{Root: cfg.Project.Root, ID: cfg.Project.ID, Name: cfg.Project.Name, Source: "configured"}
			}
			w := observe.Workspace(ctx, p.Workspace.CWD, previousWorkspace, project)
			previousWorkspace = &w
			p.Workspace = &w
		}
		return p, nil
	}
	source := Source{}
	var binding wire.ConnectorPoll
	source.Bind = func(p wire.ConnectorPoll) { mu.Lock(); binding = p; mu.Unlock() }
	source.Receivers = func(ctx context.Context) ([]wire.SessionRef, error) {
		if err := checkHost(ctx); err != nil {
			return nil, err
		}
		observerMu.Lock()
		defer observerMu.Unlock()
		p, err := observer.presence()
		if err != nil {
			return nil, err
		}
		return []wire.SessionRef{p.Ref}, nil
	}
	source.Deliver = func(ctx context.Context, message wire.Message) (string, error) {
		if err := checkHost(ctx); err != nil {
			return "unknown", err
		}
		observerMu.Lock()
		p, err := observer.presence()
		observerMu.Unlock()
		if err != nil || message.To == nil || p.Ref.Key() != message.To.Key() {
			return "refused", nil
		}
		id, err := connectorID()
		if err != nil {
			return "unknown", err
		}
		ch := make(chan wire.NativeMessage, 1)
		mu.Lock()
		if len(pending) >= 32 {
			mu.Unlock()
			return "unknown", errors.New("too many native requests")
		}
		pending[id] = ch
		mu.Unlock()
		defer func() { mu.Lock(); delete(pending, id); mu.Unlock() }()
		if err := send(wire.NativeMessage{ID: id, Method: "message", Message: &message}); err != nil {
			return "unknown", err
		}
		select {
		case <-ctx.Done():
			return "unknown", ctx.Err()
		case reply := <-ch:
			if reply.Ack == nil {
				return "unknown", errors.New("invalid native acknowledgement")
			}
			switch reply.Ack.Status {
			case "accepted", "refused", "unknown":
				return reply.Ack.Status, nil
			}
			return "unknown", errors.New("invalid native acknowledgement")
		}
	}
	source.Snapshot = func(ctx context.Context) ([]wire.SessionPresence, error) {
		observerMu.Lock()
		p, e := observer.presence()
		observerMu.Unlock()
		if e != nil {
			return nil, e
		}
		p, e = enrichPresence(ctx, p)
		if e != nil {
			return nil, e
		}
		return sampler.Enrich(ctx, []wire.SessionPresence{p})
	}
	source.Inspect = func(ctx context.Context, ref wire.SessionRef) (wire.SessionInspection, error) {
		observerMu.Lock()
		p, e := observer.presence()
		observerMu.Unlock()
		if e != nil {
			return wire.SessionInspection{}, e
		}
		if p.Ref.Key() != ref.Key() {
			return wire.SessionInspection{}, errors.New("session not found")
		}
		if e = checkHost(ctx); e != nil {
			return wire.SessionInspection{}, e
		}
		id, e := connectorID()
		if e != nil {
			return wire.SessionInspection{}, e
		}
		ch := make(chan wire.NativeMessage, 1)
		mu.Lock()
		if len(pending) >= 32 {
			mu.Unlock()
			return wire.SessionInspection{}, errors.New("too many native requests")
		}
		pending[id] = ch
		mu.Unlock()
		defer func() { mu.Lock(); delete(pending, id); mu.Unlock() }()
		if e = send(wire.NativeMessage{ID: id, Method: "snapshot", Ref: &ref}); e != nil {
			return wire.SessionInspection{}, e
		}
		select {
		case <-ctx.Done():
			return wire.SessionInspection{}, ctx.Err()
		case reply := <-ch:
			if reply.Error != nil {
				return wire.SessionInspection{}, errors.New("native getter failed")
			}
			if e = update(reply.Snapshot, false); e != nil {
				return wire.SessionInspection{}, e
			}
		}
		observerMu.Lock()
		item, e := observer.inspect()
		observerMu.Unlock()
		if e != nil {
			return item, e
		}
		if item.Presence.Ref.Key() != ref.Key() {
			return wire.SessionInspection{}, errors.New("native session changed")
		}
		item.Presence, e = enrichPresence(ctx, item.Presence)
		if e != nil {
			return item, e
		}
		return sampler.Inspect(ctx, item)
	}
	type frame struct {
		msg wire.NativeMessage
		err error
	}
	frames := make(chan frame, 1)
	var started bool
	runErr := make(chan error, 1)
	calls := make(chan struct{}, 32)
	defer func() {
		cancel()
		if c, ok := in.(io.Closer); ok {
			_ = c.Close()
		}
		if c, ok := out.(io.Closer); ok {
			_ = c.Close()
		}
		if started {
			<-runErr
		}
	}()
	go func() {
		s := bufio.NewScanner(in)
		s.Buffer(make([]byte, 4096), 65537)
		for s.Scan() {
			line := append([]byte(nil), s.Bytes()...)
			var m wire.NativeMessage
			d := json.NewDecoder(bytes.NewReader(line))
			d.DisallowUnknownFields()
			e := d.Decode(&m)
			if e == nil {
				var extra any
				if d.Decode(&extra) != io.EOF {
					e = errors.New("invalid frame")
				}
			}
			select {
			case frames <- frame{m, e}:
			case <-ctx.Done():
				return
			}
			if e != nil {
				return
			}
		}
		e := s.Err()
		if e == nil {
			e = io.EOF
		}
		select {
		case frames <- frame{err: e}:
		case <-ctx.Done():
		}
	}()
	respond := func(id string, value any, e error) {
		m := wire.NativeMessage{ID: id}
		if e != nil {
			m.Error = &wire.NativeError{Code: "unavailable", Message: "Native operation unavailable"}
		} else {
			m.Result, _ = json.Marshal(value)
		}
		if send(m) != nil {
			cancel()
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-writeErr:
			return e
		case e := <-runErr:
			started = false
			if e != nil {
				return e
			}
			return nil
		case f := <-frames:
			if f.err != nil {
				if errors.Is(f.err, io.EOF) {
					return nil
				}
				return f.err
			}
			m := f.msg
			if m.Version != 1 {
				return errors.New("native protocol version mismatch")
			}
			switch m.Method {
			case "shutdown":
				return nil
			case "update":
				if m.ID != "" {
					return errors.New("invalid update")
				}
				if e := update(m.Snapshot, true); e != nil {
					return e
				}
				if !started {
					started = true
					go func() {
						runErr <- RunSource(ctx, cfg, source, func(n wire.NativeNotice) {
							if send(wire.NativeMessage{Method: "notice", Notice: &n}) != nil {
								cancel()
							}
						})
					}()
				}
			case "snapshot", "message_ack":
				if m.ID == "" {
					return errors.New("invalid snapshot response")
				}
				mu.Lock()
				ch := pending[m.ID]
				mu.Unlock()
				if ch != nil {
					select {
					case ch <- m:
					default:
					}
				}
			case "list", "inspect", "message.send", "message.reply", "message.get":
				if m.ID == "" {
					return errors.New("missing request id")
				}
				select {
				case calls <- struct{}{}:
				default:
					return errors.New("native request capacity exceeded")
				}
				var caller *wire.MessageCaller
				if m.Method == "message.send" || m.Method == "message.reply" || m.Method == "message.get" {
					if !cfg.Messaging || m.Snapshot == nil || m.Snapshot.Harness != harness {
						<-calls
						respond(m.ID, nil, errors.New("messaging unavailable"))
						continue
					}
					observerMu.Lock()
					p, e := observer.presence()
					if e == nil && (p.Ref.NativeSessionID != m.Snapshot.NativeSessionID || p.Ref.NativeAgentID != m.Snapshot.NativeAgentID) {
						e = errors.New("native session changed")
					}
					mu.Lock()
					b := binding
					mu.Unlock()
					if e == nil && b.ConnectorID != "" && b.Incarnation != "" {
						caller = &wire.MessageCaller{ConnectorID: b.ConnectorID, Incarnation: b.Incarnation, Ref: p.Ref}
					}
					observerMu.Unlock()
					if caller == nil || checkHost(ctx) != nil {
						<-calls
						respond(m.ID, nil, errors.New("native caller unavailable"))
						continue
					}
				}
				go func(m wire.NativeMessage) {
					defer func() { <-calls }()
					c, stop := context.WithTimeout(ctx, 10*time.Second)
					defer stop()
					if m.Method == "list" {
						if m.Input == nil {
							respond(m.ID, nil, errors.New("missing input"))
							return
						}
						v, e := client.ListAll(c, *m.Input)
						respond(m.ID, v, e)
					} else if m.Method == "message.send" {
						if m.Send == nil {
							respond(m.ID, nil, errors.New("missing input"))
							return
						}
						id := m.Send.RequestID
						if id == "" {
							id, _ = connectorID()
						}
						v, e := client.SendMessage(c, wire.MessageSendInput{To: m.Send.To, Text: m.Send.Text, RequestID: id, Caller: caller})
						respond(m.ID, v, e)
					} else if m.Method == "message.reply" {
						if m.Reply == nil {
							respond(m.ID, nil, errors.New("missing input"))
							return
						}
						id := m.Reply.RequestID
						if id == "" {
							id, _ = connectorID()
						}
						v, e := client.ReplyMessage(c, wire.MessageReplyInput{ReplyID: m.Reply.ReplyID, Text: m.Reply.Text, RequestID: id, Caller: caller})
						respond(m.ID, v, e)
					} else if m.Method == "message.get" {
						if m.Get == nil {
							respond(m.ID, nil, errors.New("missing input"))
							return
						}
						v, e := client.GetMessage(c, *m.Get)
						respond(m.ID, v, e)
					} else {
						if m.Ref == nil {
							respond(m.ID, nil, errors.New("missing ref"))
							return
						}
						v, e := client.Inspect(c, *m.Ref)
						respond(m.ID, v, e)
					}
				}(m)
			default:
				return fmt.Errorf("unknown native method")
			}
		}
	}
}
