package coordinator

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func codexSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cx-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "s")
}

func TestCodexProxyIdentityAndDelivery(t *testing.T) {
	path := codexSocketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	requests := make(chan map[string]any, 16)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, b, e := conn.Read(r.Context())
			if e != nil {
				return
			}
			var req map[string]any
			if json.Unmarshal(b, &req) != nil {
				return
			}
			requests <- req
			if req["method"] == "hang" {
				continue
			}
			var result any
			switch req["method"] {
			case "thread/loaded/list":
				result = map[string]any{"data": []string{"thread-a", "thread-b"}, "nextCursor": nil}
			case "thread/read":
				result = map[string]any{"thread": map[string]any{"id": "thread-a", "cwd": "/tmp", "modelProvider": "test", "status": map[string]any{"type": "active"}}}
			case "turn/start":
				result = map[string]any{"turn": map[string]any{"id": "turn"}}
			default:
				return
			}
			response, _ := json.Marshal(map[string]any{"id": req["id"], "result": result})
			if conn.Write(r.Context(), websocket.MessageText, response) != nil {
				return
			}
		}
	})}
	go server.Serve(listener)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := dialCodex(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	p := newCodexProxy(ctx, conn)
	s := &codexSource{proxy: p, cfg: Config{MachineID: "machine", ConnectorConfig: wire.ConnectorConfig{Messaging: true}}, binding: wire.ConnectorPoll{ConnectorID: "connector", Incarnation: "incarnation"}}
	req := func(meta mcp.Meta) *mcp.CallToolRequest {
		return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Meta: meta}}
	}
	caller, err := s.caller(ctx, req(mcp.Meta{"threadId": "thread-a", "sessionId": "runtime-1"}))
	if err != nil || caller.Ref.NativeSessionID != "thread-a" || caller.ConnectorID != "connector" {
		t.Fatalf("caller: %+v %v", caller, err)
	}
	if _, err = s.caller(ctx, req(mcp.Meta{"threadId": "thread-b", "sessionId": "runtime-1"})); err == nil {
		t.Fatal("runtime identity reused across threads")
	}
	for _, meta := range []mcp.Meta{{}, {"threadId": 12}, {"threadId": "missing"}, {"threadId": "thread-a", "sessionId": true}, {"threadId": strings.Repeat("x", 257)}} {
		if _, err = s.caller(ctx, req(meta)); err == nil {
			t.Fatalf("accepted metadata %#v", meta)
		}
	}
	presence, err := s.presence(ctx, "thread-a")
	if err != nil || presence.ProgramStatus == nil || presence.ProgramStatus.State != "working" || presence.Model == nil || presence.Model.Provider != "test" || len(presence.Processes) != 0 {
		t.Fatalf("presence %+v: %v", presence, err)
	}
	recipient := wire.SessionRecipient(s.ref("thread-a"))
	message := wire.Message{ID: "message-id", Sender: "peer", Text: "untrusted", To: &recipient}
	if !codexMessageFor(message, s.ref("thread-a")) || codexMessageFor(message, s.ref("thread-b")) {
		t.Fatal("message visibility crossed thread boundary")
	}
	status, err := s.deliver(ctx, message)
	if err != nil || status != "accepted" {
		t.Fatalf("delivery %s: %v", status, err)
	}
	var turn map[string]any
	for len(requests) > 0 {
		r := <-requests
		if r["method"] == "turn/start" {
			turn = r
		}
	}
	if turn == nil {
		t.Fatal("missing turn/start")
	}
	params := turn["params"].(map[string]any)
	output := params["toolOutput"].(map[string]any)
	if params["threadId"] != "thread-a" || len(params["input"].([]any)) != 0 || output["name"] != "coordinator_peer_message" || output["namespace"] != nil || !strings.Contains(output["output"].(string), "untrusted") || !strings.Contains(output["output"].(string), "message.reply") {
		t.Fatalf("unexpected turn: %#v", params)
	}
	other := wire.SessionRecipient(wire.SessionRef{MachineID: "other", Harness: "codex", NativeSessionID: "thread-a"})
	message.To = &other
	if status, _ = s.deliver(ctx, message); status != "refused" {
		t.Fatal("accepted wrong machine")
	}
	requestCtx, stop := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { var out any; result <- p.call(requestCtx, "hang", map[string]any{}, &out) }()
	for {
		r := <-requests
		if r["method"] == "hang" {
			break
		}
	}
	stop()
	if err := <-result; err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCodexSocketRequiresPrivateOwnedSocket(t *testing.T) {
	path := codexSocketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err = checkCodexSocket(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if err = checkCodexSocket(path); err == nil {
		t.Fatal("accepted public socket")
	}
	if err = checkCodexSocket("relative.sock"); err == nil {
		t.Fatal("accepted relative socket")
	}
}
