package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

const httpLimit = 64 * 1024
const responseLimit = 3 * httpLimit

type Client struct {
	cfg  wire.ConnectorConfig
	base *url.URL
	http *http.Client
}

func NewClient(cfg wire.ConnectorConfig) (*Client, error) {
	u, e := url.Parse(cfg.InspectorURL)
	if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, failure(400, "Invalid inspection request or response")
	}
	return &Client{cfg: cfg, base: u, http: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) token() (string, error) {
	t, e := Token(c.cfg)
	if e != nil || t == "" || len(t) > 505 || strings.ContainsAny(t, "\r\n") {
		return "", failure(400, "Invalid inspection request or response")
	}
	return t, nil
}
func (c *Client) target(path string) string {
	return c.base.ResolveReference(&url.URL{Path: path}).String()
}
func (c *Client) post(ctx context.Context, route string, input any, out any) error {
	b, e := wire.Encode(input)
	if e != nil {
		return failure(400, "Invalid inspection request or response")
	}
	if len(b) > httpLimit {
		return failure(413, "Request exceeds bound")
	}
	token, e := c.token()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.target(route), bytes.NewReader(b))
	if e != nil {
		return failure(400, "Invalid inspection request or response")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := c.http.Do(req)
	if e != nil {
		return failure(502, "Inspection transport unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		status := resp.StatusCode
		if status < 400 || status > 599 {
			status = 502
		}
		return failure(status, "Inspection request rejected")
	}
	b, e = io.ReadAll(io.LimitReader(resp.Body, responseLimit+1))
	if e != nil {
		return failure(502, "Inspection transport unavailable")
	}
	if len(b) > responseLimit {
		return failure(413, "Response exceeds bound")
	}
	switch target := out.(type) {
	case *wire.ConnectorAccepted:
		v, e := wire.Decode[wire.ConnectorAccepted](bytes.NewReader(b), responseLimit)
		if e != nil {
			return failure(400, "Invalid inspection request or response")
		}
		*target = v
	case *wire.ConnectorPollResult:
		v, e := wire.Decode[wire.ConnectorPollResult](bytes.NewReader(b), responseLimit)
		if e != nil {
			return failure(400, "Invalid inspection request or response")
		}
		*target = v
	default:
		return failure(500, "Invalid inspection response type")
	}
	return nil
}

type boundedTransport struct {
	base   http.RoundTripper
	token  string
	status atomic.Int32
}

func (t *boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	resp, e := t.base.RoundTrip(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode >= 400 && resp.StatusCode <= 599 {
		t.status.CompareAndSwap(0, int32(resp.StatusCode))
	}
	resp.Body = &limitedBody{ReadCloser: resp.Body, left: responseLimit}
	return resp, nil
}
func (t *boundedTransport) failure() error {
	if status := t.status.Load(); status != 0 {
		return failure(int(status), "MCP request rejected")
	}
	return failure(502, "Inspection transport unavailable")
}

type limitedBody struct {
	io.ReadCloser
	left int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.left < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1]
	}
	n, e := b.ReadCloser.Read(p)
	b.left -= int64(n)
	if b.left < 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, e
}
func (c *Client) call(ctx context.Context, name string, input any, out any) error {
	if e := wire.Validate(input); e != nil {
		return failure(400, "Invalid inspection request or response")
	}
	token, e := c.token()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	transport := &boundedTransport{base: http.DefaultTransport, token: token}
	hc := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	cl := mcp.NewClient(&mcp.Implementation{Name: "coordinator-inspector", Version: "1.0.0"}, nil)
	session, e := cl.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.target("/mcp"), HTTPClient: hc, MaxRetries: -1, DisableStandaloneSSE: true, MaxEventSize: responseLimit}, nil)
	if e != nil {
		return transport.failure()
	}
	defer session.Close()
	res, e := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: input})
	if e != nil {
		return transport.failure()
	}
	b, e := json.Marshal(res.StructuredContent)
	if e != nil || len(b) > responseLimit {
		return failure(400, "Invalid inspection request or response")
	}
	if res.IsError {
		var v struct {
			Error struct {
				Tag     string `json:"_tag"`
				Status  int    `json:"status"`
				Message string `json:"message"`
			} `json:"error"`
		}
		d := json.NewDecoder(bytes.NewReader(b))
		d.DisallowUnknownFields()
		if d.Decode(&v) != nil || v.Error.Tag != "HttpError" || v.Error.Status < 400 || v.Error.Status > 599 {
			return failure(400, "Invalid inspection request or response")
		}
		if strings.HasPrefix(name, "message.") {
			return failure(v.Error.Status, messageErrorText(v.Error.Status, v.Error.Message))
		}
		return failure(v.Error.Status, "Inspection request unavailable")
	}
	var wrapper struct {
		Result json.RawMessage `json:"result"`
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&wrapper) != nil || len(wrapper.Result) == 0 {
		return failure(400, "Invalid inspection request or response")
	}
	switch v := out.(type) {
	case *wire.SessionListOutput:
		x, e := wire.Decode[wire.SessionListOutput](bytes.NewReader(wrapper.Result), responseLimit)
		if e != nil {
			return failure(400, "Invalid inspection request or response")
		}
		*v = x
	case *wire.SessionInspection:
		x, e := wire.Decode[wire.SessionInspection](bytes.NewReader(wrapper.Result), responseLimit)
		if e != nil {
			return failure(400, "Invalid inspection request or response")
		}
		*v = x
	case *wire.Message:
		x, e := wire.Decode[wire.Message](bytes.NewReader(wrapper.Result), responseLimit)
		if e != nil {
			return failure(400, "Invalid message response")
		}
		*v = x
	case *wire.MessageRecord:
		x, e := wire.Decode[wire.MessageRecord](bytes.NewReader(wrapper.Result), responseLimit)
		if e != nil {
			return failure(400, "Invalid message response")
		}
		*v = x
	default:
		return failure(500, "Invalid inspection response type")
	}
	return nil
}
func (c *Client) List(ctx context.Context, in wire.SessionListInput) (wire.SessionListOutput, error) {
	var out wire.SessionListOutput
	e := c.call(ctx, "session.list", in, &out)
	return out, e
}
func (c *Client) Inspect(ctx context.Context, ref wire.SessionRef) (wire.SessionInspection, error) {
	var out wire.SessionInspection
	e := c.call(ctx, "session.inspect", wire.SessionInspectInput{Ref: ref}, &out)
	return out, e
}
func (c *Client) SendMessage(ctx context.Context, in wire.MessageSendInput) (wire.Message, error) {
	var out wire.Message
	e := c.call(ctx, "message.send", in, &out)
	return out, e
}
func (c *Client) ReplyMessage(ctx context.Context, in wire.MessageReplyInput) (wire.Message, error) {
	var out wire.Message
	e := c.call(ctx, "message.reply", in, &out)
	return out, e
}
func (c *Client) GetMessage(ctx context.Context, in wire.MessageGetInput) (wire.MessageRecord, error) {
	var out wire.MessageRecord
	e := c.call(ctx, "message.get", in, &out)
	return out, e
}
func (c *Client) AckMessage(ctx context.Context, in wire.ConnectorMessageAck) (wire.ConnectorAccepted, error) {
	var out wire.ConnectorAccepted
	e := c.post(ctx, "/connector/message-ack", in, &out)
	return out, e
}
func (c *Client) ListAll(ctx context.Context, in wire.SessionListInput) (wire.SessionListOutput, error) {
	out := wire.SessionListOutput{Machines: []wire.ListMachine{}, Sessions: []wire.ListSession{}}
	in.Cursor = ""
	machines := map[string]wire.ListMachine{}
	seen := map[string]bool{}
	for {
		page, e := c.List(ctx, in)
		if e != nil {
			return out, e
		}
		wire.MergeListMachines(machines, page.Machines)
		out.Sessions = append(out.Sessions, page.Sessions...)
		if len(out.Sessions) > 4096 {
			return out, failure(413, "Response exceeds bound")
		}
		if page.NextCursor == nil {
			break
		}
		if seen[*page.NextCursor] || len(seen) >= 4096 {
			return out, failure(400, "Invalid session cursor")
		}
		seen[*page.NextCursor] = true
		in.Cursor = *page.NextCursor
	}
	keys := make([]string, 0, len(machines))
	for k := range machines {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out.Machines = append(out.Machines, machines[k])
	}
	return out, nil
}
