package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func RunReceiver(ctx context.Context, cfg Config, harness string, sessionID string, pid int, in io.Reader, out io.Writer) error {
	ref := wire.SessionRef{MachineID: cfg.MachineID, Harness: harness, NativeSessionID: sessionID}
	if !cfg.Messaging || harness != "hermes" || pid <= 0 || pid != os.Getppid() || wire.Validate(ref) != nil {
		return errors.New("invalid receiver identity")
	}
	host, err := observe.Identify(ctx, pid)
	if err != nil {
		return err
	}
	client, err := NewClient(cfg.ConnectorConfig)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	context.AfterFunc(ctx, func() {
		if c, ok := out.(io.Closer); ok {
			_ = c.Close()
		}
		if c, ok := in.(io.Closer); ok {
			_ = c.Close()
		}
	})
	checkHost := func(ctx context.Context) error {
		if os.Getppid() != pid {
			return errors.New("receiver host exited")
		}
		current, e := observe.Identify(ctx, pid)
		if e != nil || current.StartIdentity != host.StartIdentity {
			return errors.New("receiver host changed")
		}
		return nil
	}
	var writeMu, mu sync.Mutex
	send := func(m wire.NativeMessage) error {
		m.Version = 1
		b, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if len(b) >= 65536 {
			return errors.New("receiver frame too large")
		}
		b = append(b, '\n')
		writeMu.Lock()
		defer writeMu.Unlock()
		n, e := out.Write(b)
		if e == nil && n != len(b) {
			e = io.ErrShortWrite
		}
		if e != nil {
			cancel()
		}
		return e
	}
	if err := send(wire.NativeMessage{Method: "ready", Ref: &ref}); err != nil {
		return err
	}
	var binding wire.ConnectorPoll
	pending := map[string]chan wire.NativeMessage{}
	source := Source{
		Snapshot: func(context.Context) ([]wire.SessionPresence, error) { return []wire.SessionPresence{}, nil },
		Inspect: func(context.Context, wire.SessionRef) (wire.SessionInspection, error) {
			return wire.SessionInspection{}, errors.New("receiver cannot inspect")
		},
		Bind: func(p wire.ConnectorPoll) { mu.Lock(); binding = p; mu.Unlock() },
		Receivers: func(ctx context.Context) ([]wire.SessionRef, error) {
			if e := checkHost(ctx); e != nil {
				return nil, e
			}
			return []wire.SessionRef{ref}, nil
		},
		Deliver: func(ctx context.Context, message wire.Message) (string, error) {
			if e := checkHost(ctx); e != nil {
				return "unknown", e
			}
			if message.To == nil || message.To.Key() != ref.Key() {
				return "refused", nil
			}
			id, e := connectorID()
			if e != nil {
				return "unknown", e
			}
			ch := make(chan wire.NativeMessage, 1)
			mu.Lock()
			if len(pending) >= 32 {
				mu.Unlock()
				return "unknown", errors.New("receiver capacity exceeded")
			}
			pending[id] = ch
			mu.Unlock()
			defer func() { mu.Lock(); delete(pending, id); mu.Unlock() }()
			if e := send(wire.NativeMessage{ID: id, Method: "message", Message: &message}); e != nil {
				return "unknown", e
			}
			select {
			case <-ctx.Done():
				return "unknown", ctx.Err()
			case reply := <-ch:
				if reply.Ack != nil {
					switch reply.Ack.Status {
					case "accepted", "refused", "unknown":
						return reply.Ack.Status, nil
					}
				}
				return "unknown", errors.New("invalid receiver acknowledgement")
			}
		},
	}
	runErr := make(chan error, 1)
	go func() { runErr <- RunSource(ctx, cfg, source, nil); cancel() }()
	defer func() {
		cancel()
		if c, ok := in.(io.Closer); ok {
			_ = c.Close()
		}
		if c, ok := out.(io.Closer); ok {
			_ = c.Close()
		}
		<-runErr
	}()
	frames := make(chan wire.NativeMessage, 1)
	readErr := make(chan error, 1)
	go func() {
		s := bufio.NewScanner(in)
		s.Buffer(make([]byte, 4096), 65537)
		for s.Scan() {
			var m wire.NativeMessage
			d := json.NewDecoder(bytes.NewReader(s.Bytes()))
			d.DisallowUnknownFields()
			if e := d.Decode(&m); e != nil {
				readErr <- e
				return
			}
			var extra any
			if d.Decode(&extra) != io.EOF {
				readErr <- errors.New("invalid receiver frame")
				return
			}
			select {
			case frames <- m:
			case <-ctx.Done():
				return
			}
		}
		if e := s.Err(); e != nil {
			readErr <- e
		} else {
			readErr <- io.EOF
		}
	}()
	respond := func(id string, value any, e error) {
		m := wire.NativeMessage{ID: id}
		if e != nil {
			m.Error = &wire.NativeError{Code: "unavailable", Message: "Native operation unavailable"}
		} else {
			m.Result, _ = json.Marshal(value)
		}
		_ = send(m)
	}
	calls := make(chan struct{}, 32)
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-readErr:
			if errors.Is(e, io.EOF) {
				return nil
			}
			return e
		case m := <-frames:
			if m.Version != 1 {
				return errors.New("receiver protocol version mismatch")
			}
			switch m.Method {
			case "shutdown":
				return nil
			case "message_ack":
				mu.Lock()
				ch := pending[m.ID]
				mu.Unlock()
				if ch != nil {
					select {
					case ch <- m:
					default:
					}
				}
			case "message.send", "message.reply", "message.get":
				if m.ID == "" || checkHost(ctx) != nil {
					return errors.New("invalid receiver request")
				}
				mu.Lock()
				b := binding
				mu.Unlock()
				if b.ConnectorID == "" || b.Incarnation == "" {
					respond(m.ID, nil, errors.New("receiver not bound"))
					continue
				}
				select {
				case calls <- struct{}{}:
				default:
					respond(m.ID, nil, errors.New("receiver capacity exceeded"))
					continue
				}
				go func(m wire.NativeMessage, b wire.ConnectorPoll) {
					defer func() { <-calls }()
					c, stop := context.WithTimeout(ctx, 10*time.Second)
					defer stop()
					caller := &wire.MessageCaller{ConnectorID: b.ConnectorID, Incarnation: b.Incarnation, Ref: ref}
					switch m.Method {
					case "message.send":
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
					case "message.reply":
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
					case "message.get":
						if m.Get == nil {
							respond(m.ID, nil, errors.New("missing input"))
							return
						}
						v, e := client.GetMessage(c, *m.Get)
						respond(m.ID, v, e)
					}
				}(m, b)
			default:
				return errors.New("unknown receiver method")
			}
		}
	}
}
