package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func Handler(reg *Registry) http.Handler {
	server := makeMCP(mcpReader{list: func(ctx context.Context, req *mcp.CallToolRequest, in wire.SessionListInput) (wire.SessionListOutput, error) {
		p, e := principalForMCP(reg, req)
		if e != nil {
			return wire.SessionListOutput{}, e
		}
		return reg.List(p, in)
	}, inspect: func(ctx context.Context, req *mcp.CallToolRequest, ref wire.SessionRef) (wire.SessionInspection, error) {
		p, e := principalForMCP(reg, req)
		if e != nil {
			return wire.SessionInspection{}, e
		}
		return reg.Inspect(ctx, p, ref)
	}, send: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageSendInput) (wire.Message, error) {
		p, e := principalForMCP(reg, req)
		if e != nil {
			return wire.Message{}, e
		}
		return reg.SendMessage(p, in)
	}, reply: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageReplyInput) (wire.Message, error) {
		p, e := principalForMCP(reg, req)
		if e != nil {
			return wire.Message{}, e
		}
		return reg.ReplyMessage(p, in)
	}, get: func(ctx context.Context, req *mcp.CallToolRequest, in wire.MessageGetInput) (wire.MessageRecord, error) {
		p, e := principalForMCP(reg, req)
		if e != nil {
			return wire.MessageRecord{}, e
		}
		return reg.GetMessage(p, in)
	}}, nil)
	transport := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, MaxRequestBodyBytes: httpLimit})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		sha := os.Getenv("COORDINATOR_IMAGE_SHA")
		if sha == "" {
			sha = SourceSHA
		}
		if sha != "" {
			w.Header().Set("X-Coordinator-Source-Sha", sha)
		}
		writeError := func(err error) {
			status := 400
			var h *HTTPError
			if errors.As(err, &h) {
				status = h.Status
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Inspection request unavailable"})
		}
		if req.Method == http.MethodGet && req.URL.Path == "/livez" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
		if _, exists := req.Header["Origin"]; exists {
			writeError(failure(403, "Origin prohibited"))
			return
		}
		auth := req.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || len(auth) > 512 {
			writeError(failure(401, "Credential required"))
			return
		}
		principal, e := reg.Authorize(strings.TrimPrefix(auth, "Bearer "))
		if e != nil {
			writeError(e)
			return
		}
		if req.Method == http.MethodGet && (req.URL.Path == "/healthz" || req.URL.Path == "/readyz") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
		if req.ContentLength > httpLimit {
			writeError(failure(413, "Request exceeds bound"))
			return
		}
		if req.URL.Path == "/mcp" {
			transport.ServeHTTP(w, req)
			return
		}
		if req.Method != http.MethodPost {
			writeError(failure(404, "Not found"))
			return
		}
		body, e := io.ReadAll(io.LimitReader(req.Body, httpLimit+1))
		if e != nil {
			writeError(failure(400, "Invalid inspection request or response"))
			return
		}
		if len(body) > httpLimit {
			writeError(failure(413, "Request exceeds bound"))
			return
		}
		var out any
		switch req.URL.Path {
		case "/connector/announce":
			var x wire.ConnectorAnnounce
			x, e = wire.Decode[wire.ConnectorAnnounce](strings.NewReader(string(body)), httpLimit)
			if e == nil {
				out, e = reg.Announce(principal, x)
			}
		case "/connector/poll":
			var x wire.ConnectorPoll
			x, e = wire.Decode[wire.ConnectorPoll](strings.NewReader(string(body)), httpLimit)
			if e == nil {
				out, e = reg.Poll(principal, x)
			}
		case "/connector/result":
			var x wire.ConnectorResult
			x, e = wire.Decode[wire.ConnectorResult](strings.NewReader(string(body)), httpLimit)
			if e == nil {
				out, e = reg.Complete(principal, x)
			}
		case "/connector/message-ack":
			var x wire.ConnectorMessageAck
			x, e = wire.Decode[wire.ConnectorMessageAck](strings.NewReader(string(body)), httpLimit)
			if e == nil {
				out, e = reg.AckMessage(principal, x)
			}
		case "/connector/disconnect":
			var x wire.ConnectorDisconnect
			x, e = wire.Decode[wire.ConnectorDisconnect](strings.NewReader(string(body)), httpLimit)
			if e == nil {
				out, e = reg.Disconnect(principal, x)
			}
		default:
			e = failure(404, "Not found")
		}
		if e != nil {
			writeError(e)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
}
