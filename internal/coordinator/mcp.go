package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

type mcpReader struct {
	list    func(context.Context, *mcp.CallToolRequest, wire.SessionListInput) (wire.SessionListOutput, error)
	inspect func(context.Context, *mcp.CallToolRequest, wire.SessionRef) (wire.SessionInspection, error)
	send    func(context.Context, *mcp.CallToolRequest, wire.MessageSendInput) (wire.Message, error)
	reply   func(context.Context, *mcp.CallToolRequest, wire.MessageReplyInput) (wire.Message, error)
	get     func(context.Context, *mcp.CallToolRequest, wire.MessageGetInput) (wire.MessageRecord, error)
	bound   bool
}

func toolResult(v any, err error) *mcp.CallToolResult {
	var content any
	if err != nil {
		status := 400
		if e, ok := err.(*HTTPError); ok {
			status = e.Status
		}
		content = map[string]any{"error": map[string]any{"_tag": "HttpError", "status": status, "message": "Inspection request unavailable"}}
	} else {
		content = map[string]any{"result": v}
		if size(content) > httpLimit-1024 {
			err = failure(413, "Inspection result exceeds bound")
			content = map[string]any{"error": map[string]any{"_tag": "HttpError", "status": 413, "message": "Inspection request unavailable"}}
		}
	}
	b, _ := json.MarshalIndent(content, "", "  ")
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}, StructuredContent: content, IsError: err != nil}
}

// messageErrorText exposes only fixed, actionable messages, never arbitrary server errors.
func messageErrorText(status int, message string) string {
	switch {
	case status == 409 && message == "Target does not support messaging":
		return "Target does not support messaging"
	case status == 409 && (message == "Message destination unavailable" || message == "Message destination unavailable or does not support messaging"):
		return "Message destination unavailable or does not support messaging"
	case status == 403 && (message == "Messaging not permitted" || message == "Messaging permission required"):
		return "Messaging permission required"
	case status == 400 && (message == "Invalid message reply" || message == "Bound reply caller required"):
		return "Bound reply caller required"
	case status == 404 && (message == "Message not found" || message == "Message unavailable"):
		return "Message unavailable"
	default:
		return "Message request unavailable"
	}
}
func messageToolResult(v any, err error) *mcp.CallToolResult {
	if err == nil {
		return toolResult(v, nil)
	}
	status := 400
	message := ""
	if e, ok := err.(*HTTPError); ok {
		status, message = e.Status, e.Message
	}
	content := map[string]any{"error": map[string]any{"_tag": "HttpError", "status": status, "message": messageErrorText(status, message)}}
	b, _ := json.MarshalIndent(content, "", "  ")
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}, StructuredContent: content, IsError: true}
}
func makeMCP(reader mcpReader, options *mcp.ServerOptions) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "coordinator-inspector", Version: "1.0.0"}, options)
	no := false
	annotations := &mcp.ToolAnnotations{ReadOnlyHint: true, DestructiveHint: &no, IdempotentHint: true, OpenWorldHint: &no}
	listIn, _ := wire.Schema("SessionListInput")
	inspectIn, _ := wire.Schema("SessionInspectInput")
	listOut, _ := wire.Schema("SessionListOutput")
	inspectOut, _ := wire.Schema("SessionInspection")
	output := func(inner *jsonschema.Schema) *jsonschema.Schema {
		return &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"result": inner}, Required: []string{"result"}}
	}
	s.AddTool(&mcp.Tool{Name: "session.list", Description: "List visible native sessions", InputSchema: listIn, OutputSchema: output(listOut), Annotations: annotations}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in, e := wire.Decode[wire.SessionListInput](bytes.NewReader(req.Params.Arguments), httpLimit)
		if e != nil {
			return toolResult(nil, failure(400, "Invalid inspection request or response")), nil
		}
		out, e := reader.list(ctx, req, in)
		return toolResult(out, e), nil
	})
	s.AddTool(&mcp.Tool{Name: "session.inspect", Description: "Inspect a native session", InputSchema: inspectIn, OutputSchema: output(inspectOut), Annotations: annotations}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in, e := wire.Decode[wire.SessionInspectInput](bytes.NewReader(req.Params.Arguments), httpLimit)
		if e != nil {
			return toolResult(nil, failure(400, "Invalid inspection request or response")), nil
		}
		out, e := reader.inspect(ctx, req, in.Ref)
		return toolResult(out, e), nil
	})
	sendSchema, replySchema := "MessageSendInput", "MessageReplyInput"
	if reader.bound {
		sendSchema, replySchema = "MessageSendParams", "MessageReplyParams"
	}
	sendIn, _ := wire.Schema(sendSchema)
	replyIn, _ := wire.Schema(replySchema)
	getIn, _ := wire.Schema("MessageGetInput")
	messageOut, _ := wire.Schema("Message")
	recordOut, _ := wire.Schema("MessageRecord")
	s.AddTool(&mcp.Tool{Name: "message.send", Description: "Send a peer message. This has side effects.", InputSchema: sendIn, OutputSchema: output(messageOut)}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var in wire.MessageSendInput
		var e error
		if reader.bound {
			var p wire.MessageSendParams
			p, e = wire.Decode[wire.MessageSendParams](bytes.NewReader(req.Params.Arguments), httpLimit)
			in = wire.MessageSendInput{To: p.To, Text: p.Text, RequestID: p.RequestID}
		} else {
			in, e = wire.Decode[wire.MessageSendInput](bytes.NewReader(req.Params.Arguments), httpLimit)
		}
		if e != nil {
			return messageToolResult(nil, failure(400, "Invalid message request")), nil
		}
		out, e := reader.send(ctx, req, in)
		return messageToolResult(out, e), nil
	})
	s.AddTool(&mcp.Tool{Name: "message.reply", Description: "Reply to a peer message using its original id as replyId. This has side effects.", InputSchema: replyIn, OutputSchema: output(messageOut)}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var in wire.MessageReplyInput
		var e error
		if reader.bound {
			var p wire.MessageReplyParams
			p, e = wire.Decode[wire.MessageReplyParams](bytes.NewReader(req.Params.Arguments), httpLimit)
			in = wire.MessageReplyInput{ReplyID: p.ReplyID, Text: p.Text, RequestID: p.RequestID}
		} else {
			in, e = wire.Decode[wire.MessageReplyInput](bytes.NewReader(req.Params.Arguments), httpLimit)
		}
		if e != nil {
			return messageToolResult(nil, failure(400, "Invalid message request")), nil
		}
		out, e := reader.reply(ctx, req, in)
		return messageToolResult(out, e), nil
	})
	s.AddTool(&mcp.Tool{Name: "message.get", Description: "Get a message and its replies", InputSchema: getIn, OutputSchema: output(recordOut), Annotations: annotations}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in, e := wire.Decode[wire.MessageGetInput](bytes.NewReader(req.Params.Arguments), httpLimit)
		if e != nil {
			return messageToolResult(nil, failure(400, "Invalid message request")), nil
		}
		out, e := reader.get(ctx, req, in)
		return messageToolResult(out, e), nil
	})
	return s
}
func principalForMCP(reg *Registry, req *mcp.CallToolRequest) (wire.InspectorPrincipal, error) {
	if req.Extra == nil {
		return wire.InspectorPrincipal{}, failure(401, "Unauthorized")
	}
	header := req.Extra.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") || len(header) > 512 {
		return wire.InspectorPrincipal{}, failure(401, "Unauthorized")
	}
	return reg.Authorize(strings.TrimPrefix(header, "Bearer "))
}
func ServeMCP(ctx context.Context, client *Client) error {
	s := makeMCP(mcpReader{list: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.SessionListInput) (wire.SessionListOutput, error) {
		return client.List(ctx, in)
	}, inspect: func(ctx context.Context, _ *mcp.CallToolRequest, ref wire.SessionRef) (wire.SessionInspection, error) {
		return client.Inspect(ctx, ref)
	}, bound: true, send: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.MessageSendInput) (wire.Message, error) {
		return client.SendMessage(ctx, in)
	}, reply: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.MessageReplyInput) (wire.Message, error) {
		return client.ReplyMessage(ctx, in)
	}, get: func(ctx context.Context, _ *mcp.CallToolRequest, in wire.MessageGetInput) (wire.MessageRecord, error) {
		return client.GetMessage(ctx, in)
	}}, nil)
	return s.Run(ctx, &mcp.StdioTransport{MaxLineLength: httpLimit})
}
