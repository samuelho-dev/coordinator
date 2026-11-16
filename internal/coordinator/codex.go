package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/observe"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

// codexProxy multiplexes requests over the configured app-server connection.
type codexProxy struct {
	mu      sync.Mutex
	conn    *websocket.Conn
	next    uint64
	pending map[uint64]chan codexResponse
	done    chan struct{}
}
type codexResponse struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func newCodexProxy(ctx context.Context, conn *websocket.Conn) *codexProxy {
	p := &codexProxy{conn: conn, pending: make(map[uint64]chan codexResponse), done: make(chan struct{})}
	conn.SetReadLimit(8 * 1024 * 1024)
	go func() {
		defer close(p.done)
		for {
			kind, b, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if kind != websocket.MessageText {
				continue
			}
			var r codexResponse
			if json.Unmarshal(b, &r) != nil || r.ID == 0 {
				continue
			}
			p.mu.Lock()
			ch := p.pending[r.ID]
			p.mu.Unlock()
			if ch != nil {
				select {
				case ch <- r:
				default:
				}
			}
		}
	}()
	return p
}
func (p *codexProxy) write(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.conn.Write(ctx, websocket.MessageText, b)
}
func (p *codexProxy) call(ctx context.Context, method string, params, result any) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	p.mu.Lock()
	if len(p.pending) >= 32 {
		p.mu.Unlock()
		return errors.New("codex proxy capacity exceeded")
	}
	p.next++
	id := p.next
	ch := make(chan codexResponse, 1)
	p.pending[id] = ch
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	if err := p.write(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return errors.New("codex proxy disconnected")
	case r := <-ch:
		if r.Error != nil {
			return errors.New("codex app-server rejected request")
		}
		if len(r.Result) == 0 {
			return errors.New("invalid codex response")
		}
		return json.Unmarshal(r.Result, result)
	}
}
func checkCodexSocket(path string) error {
	if !canonical(path) {
		return errors.New("codex socket must be an explicit canonical path")
	}
	if err := checkDir(filepath.Dir(path), false); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	uid, ok := owner(info)
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != path || !ok || uid != uint32(os.Getuid()) || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0077 != 0 {
		return errors.New("unsafe codex socket")
	}
	return nil
}
func dialCodex(ctx context.Context, path string) (*websocket.Conn, error) {
	if err := checkCodexSocket(path); err != nil {
		return nil, err
	}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := checkCodexSocket(path); err != nil {
			return nil, err
		}
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	defer transport.CloseIdleConnections()
	dialCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
	if err != nil {
		return nil, fmt.Errorf("codex socket: %w", err)
	}
	return conn, nil
}

type codexThread struct {
	ID            string  `json:"id"`
	CWD           string  `json:"cwd"`
	Model         string  `json:"model"`
	ModelProvider string  `json:"modelProvider"`
	Name          string  `json:"name"`
	CreatedAt     float64 `json:"createdAt"`
	UpdatedAt     float64 `json:"updatedAt"`
	Status        struct {
		Type string `json:"type"`
	} `json:"status"`
}
type codexSource struct {
	proxy    *codexProxy
	cfg      Config
	mu       sync.RWMutex
	binding  wire.ConnectorPoll
	sessions map[string]string
}

func (s *codexSource) ref(id string) wire.SessionRef {
	return wire.SessionRef{MachineID: s.cfg.MachineID, Harness: "codex", NativeSessionID: id}
}
func (s *codexSource) loaded(ctx context.Context) ([]string, error) {
	var all []string
	cursor := ""
	seen := map[string]bool{}
	cursors := map[string]bool{}
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var out struct {
			Data       []string `json:"data"`
			NextCursor *string  `json:"nextCursor"`
		}
		if err := s.proxy.call(ctx, "thread/loaded/list", params, &out); err != nil {
			return nil, err
		}
		for _, id := range out.Data {
			if len(id) == 0 || len(id) > 256 || seen[id] {
				return nil, errors.New("invalid loaded codex thread")
			}
			seen[id] = true
			all = append(all, id)
			if len(all) > 256 {
				return nil, errors.New("too many loaded codex threads")
			}
		}
		if out.NextCursor == nil || *out.NextCursor == "" {
			return all, nil
		}
		if len(*out.NextCursor) > 4096 || cursors[*out.NextCursor] || len(cursors) >= 256 {
			return nil, errors.New("invalid codex cursor")
		}
		cursor = *out.NextCursor
		cursors[cursor] = true
	}
}
func (s *codexSource) hasThread(ctx context.Context, id string) error {
	ids, err := s.loaded(ctx)
	if err != nil {
		return err
	}
	for _, v := range ids {
		if v == id {
			return nil
		}
	}
	return errors.New("codex thread is not loaded")
}
func (s *codexSource) presence(ctx context.Context, id string) (wire.SessionPresence, error) {
	var out struct {
		Thread codexThread `json:"thread"`
	}
	if err := s.proxy.call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false}, &out); err != nil {
		return wire.SessionPresence{}, err
	}
	t := out.Thread
	if t.ID != id {
		return wire.SessionPresence{}, errors.New("codex thread identity mismatch")
	}
	p := wire.SessionPresence{Ref: s.ref(id), ObservedAt: float64(time.Now().UnixMilli()), Label: t.Name}
	if t.CWD != "" {
		w := observe.Workspace(ctx, t.CWD, nil, nil)
		p.Workspace = &w
	}
	if t.Model != "" || t.ModelProvider != "" {
		p.Model = &wire.SessionModel{ID: t.Model, Provider: t.ModelProvider}
	}
	switch t.Status.Type {
	case "idle":
		p.ProgramStatus = &wire.ProgramStatus{State: "idle"}
	case "active":
		p.ProgramStatus = &wire.ProgramStatus{State: "working"}
	case "systemError":
		p.ProgramStatus = &wire.ProgramStatus{State: "error"}
	}
	if t.CreatedAt > 0 {
		v := t.CreatedAt * 1000
		p.StartedAt = &v
	}
	if t.UpdatedAt > 0 {
		v := t.UpdatedAt * 1000
		p.NativeUpdatedAt = &v
		p.LastActivityAt = &v
	}
	return p, nil
}
func (s *codexSource) snapshot(ctx context.Context) ([]wire.SessionPresence, error) {
	ids, err := s.loaded(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]wire.SessionPresence, 0, len(ids))
	for _, id := range ids {
		p, e := s.presence(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, nil
}
func (s *codexSource) caller(ctx context.Context, req *mcp.CallToolRequest) (*wire.MessageCaller, error) {
	if !s.cfg.Messaging || req == nil || req.Params == nil {
		return nil, errors.New("codex messaging unavailable")
	}
	meta := req.Params.Meta
	id, ok := meta["threadId"].(string)
	if !ok || len(id) == 0 || len(id) > 256 || strings.ContainsAny(id, "\x00\r\n") {
		return nil, errors.New("missing or invalid codex thread metadata")
	}
	var session string
	if value, exists := meta["sessionId"]; exists {
		session, ok = value.(string)
		if !ok || len(session) == 0 || len(session) > 256 || strings.ContainsAny(session, "\x00\r\n") {
			return nil, errors.New("invalid codex session metadata")
		}
	}
	if err := s.hasThread(ctx, id); err != nil {
		return nil, err
	}
	s.mu.Lock()
	b := s.binding
	if session != "" {
		if s.sessions == nil {
			s.sessions = make(map[string]string)
		}
		if existing, exists := s.sessions[session]; exists && existing != id {
			s.mu.Unlock()
			return nil, errors.New("codex session changed thread")
		}
		if len(s.sessions) >= 256 && s.sessions[session] == "" {
			s.mu.Unlock()
			return nil, errors.New("codex session capacity exceeded")
		}
		s.sessions[session] = id
	}
	s.mu.Unlock()
	if b.ConnectorID == "" || b.Incarnation == "" {
		return nil, errors.New("codex connector unavailable")
	}
	return &wire.MessageCaller{ConnectorID: b.ConnectorID, Incarnation: b.Incarnation, Ref: s.ref(id)}, nil
}
func (s *codexSource) deliver(ctx context.Context, m wire.Message) (string, error) {
	if m.To == nil || m.To.Kind != "session" || m.To.Session == nil || m.To.SubscriptionID != "" {
		return "refused", nil
	}
	ref := *m.To.Session
	if ref.Harness != "codex" || ref.MachineID != s.cfg.MachineID || ref.NativeAgentID != "" {
		return "refused", nil
	}
	if err := s.hasThread(ctx, ref.NativeSessionID); err != nil {
		return "refused", err
	}
	var out json.RawMessage
	err := s.proxy.call(ctx, "turn/start", map[string]any{"threadId": ref.NativeSessionID, "input": []any{}, "toolOutput": map[string]any{"name": "coordinator_peer_message", "namespace": nil, "output": MessageText(m, "message.reply")}}, &out)
	if err != nil {
		return "unknown", err
	}
	return "accepted", nil
}
func codexMessageFor(message wire.Message, ref wire.SessionRef) bool {
	for _, recipient := range []*wire.RecipientRef{message.From, message.To} {
		if recipient != nil && recipient.Kind == "session" && recipient.Session != nil && recipient.SubscriptionID == "" && recipient.Session.Key() == ref.Key() {
			return true
		}
	}
	return false
}
func ServeCodexMCP(ctx context.Context, cfg Config) error {
	if !cfg.Messaging {
		return errors.New("codex messaging must be enabled")
	}
	client, err := NewClient(cfg.ConnectorConfig)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := dialCodex(ctx, cfg.CodexSocket)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.CloseNow() })
	defer stopClose()
	p := newCodexProxy(ctx, conn)
	go func() {
		select {
		case <-p.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	var init json.RawMessage
	if err = p.call(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "coordinator", "version": Version}, "capabilities": map[string]any{"experimentalApi": true}}, &init); err != nil {
		return err
	}
	if err = p.write(ctx, map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
		return err
	}
	s := &codexSource{proxy: p, cfg: cfg}
	source := Source{Snapshot: s.snapshot, Inspect: func(ctx context.Context, ref wire.SessionRef) (wire.SessionInspection, error) {
		if ref.MachineID != cfg.MachineID || ref.Harness != "codex" || ref.NativeAgentID != "" {
			return wire.SessionInspection{}, errors.New("invalid codex ref")
		}
		if err := s.hasThread(ctx, ref.NativeSessionID); err != nil {
			return wire.SessionInspection{}, err
		}
		presence, err := s.presence(ctx, ref.NativeSessionID)
		if err != nil {
			return wire.SessionInspection{}, err
		}
		return wire.CreateInspection(presence, float64(time.Now().UnixMilli())), nil
	}, Bind: func(b wire.ConnectorPoll) { s.mu.Lock(); s.binding = b; s.mu.Unlock() }, Receivers: func(ctx context.Context) ([]wire.SessionRef, error) {
		ids, err := s.loaded(ctx)
		if err != nil {
			return nil, err
		}
		refs := make([]wire.SessionRef, 0, len(ids))
		for _, id := range ids {
			refs = append(refs, s.ref(id))
		}
		return refs, nil
	}, Deliver: s.deliver}
	sourceErr := make(chan error, 1)
	go func() {
		e := RunSource(ctx, cfg, source, func(n wire.NativeNotice) {
			switch n.State {
			case "unavailable":
				fmt.Fprintln(os.Stderr, "coordinator codex source unavailable")
			case "recovered":
				fmt.Fprintln(os.Stderr, "coordinator codex source recovered")
			}
		})
		sourceErr <- e
		cancel()
	}()
	reader := mcpReader{bound: true,
		list: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.SessionListInput) (wire.SessionListOutput, error) {
			return client.List(ctx, in)
		},
		inspect: func(ctx context.Context, _ *mcp.CallToolRequest, ref wire.SessionRef) (wire.SessionInspection, error) {
			return client.Inspect(ctx, ref)
		},
		recipients: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.RecipientListInput) (wire.RecipientListOutput, error) {
			return client.Recipients(ctx, in)
		},
		send: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageSendInput) (wire.Message, error) {
			caller, e := s.caller(ctx, req)
			if e != nil {
				return wire.Message{}, e
			}
			in.Caller = caller
			return client.SendMessage(ctx, in)
		},
		reply: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageReplyInput) (wire.Message, error) {
			caller, e := s.caller(ctx, req)
			if e != nil {
				return wire.Message{}, e
			}
			in.Caller = caller
			return client.ReplyMessage(ctx, in)
		},
		get: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageGetInput) (wire.MessageRecord, error) {
			caller, e := s.caller(ctx, req)
			if e != nil {
				return wire.MessageRecord{}, e
			}
			record, e := client.GetMessage(ctx, in)
			if e != nil {
				return wire.MessageRecord{}, e
			}
			if !codexMessageFor(record.Message, caller.Ref) {
				return wire.MessageRecord{}, failure(403, "Invalid message caller binding")
			}
			replies := make([]wire.Message, 0, len(record.Replies))
			for _, reply := range record.Replies {
				if codexMessageFor(reply, caller.Ref) {
					replies = append(replies, reply)
				}
			}
			record.Replies = replies
			return record, nil
		},
	}
	err = makeMCP(reader, nil).Run(ctx, &mcp.StdioTransport{MaxLineLength: httpLimit})
	cancel()
	if e := <-sourceErr; e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
		return fmt.Errorf("codex source: %w", e)
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("codex MCP: %w", err)
	}
	return err
}
