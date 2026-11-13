package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/coordinator"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

const responseLimit = 192 * 1024

var shaPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

type transport struct{ token string }

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header = req.Header.Clone()
	r.Header.Set("Authorization", "Bearer "+t.token)
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	resp.Body = &boundedBody{ReadCloser: resp.Body, remaining: responseLimit}
	return resp, nil
}

type boundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return 0, errors.New("response exceeds limit")
	}
	return n, err
}
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
func request(client *http.Client, base *url.URL, path, token string) (*http.Response, error) {
	method := http.MethodGet
	var body io.Reader
	if path == "/mcp" {
		method = http.MethodPost
		body = strings.NewReader("{}")
	}
	u := base.ResolveReference(&url.URL{Path: path})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.Do(req)
}
func call(ctx context.Context, session *mcp.ClientSession, name string, args any, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || r == nil || r.IsError {
		return errors.New("tool request failed")
	}
	b, err := json.Marshal(r.StructuredContent)
	if err != nil || len(b) > responseLimit {
		return errors.New("invalid tool response")
	}
	var wrapper struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(b, &wrapper) != nil || len(wrapper.Result) == 0 || bytes.Equal(wrapper.Result, []byte("null")) {
		return errors.New("invalid tool response")
	}
	return json.Unmarshal(wrapper.Result, out)
}
func main1() error {
	flags := map[string]string{}
	for i := 1; i < len(os.Args); i += 2 {
		if i+1 >= len(os.Args) {
			return errors.New("invalid arguments")
		}
		k, v := os.Args[i], os.Args[i+1]
		if (k != "--config" && k != "--source-sha" && k != "--safe-target") || v == "" || strings.HasPrefix(v, "--") {
			return errors.New("invalid arguments")
		}
		if _, exists := flags[k]; exists {
			return errors.New("duplicate argument")
		}
		flags[k] = v
	}
	sha := flags["--source-sha"]
	if sha == "" {
		sha = os.Getenv("COORDINATOR_IMAGE_SHA")
	}
	if !shaPattern.MatchString(sha) {
		return errors.New("invalid source SHA")
	}
	cfg, err := coordinator.LoadConfig(flags["--config"])
	if err != nil {
		return err
	}
	token, err := coordinator.Token(cfg)
	if err != nil || len(token) < 16 || len(token) > 4096 || !tokenPattern.MatchString(token) {
		return errors.New("invalid credential")
	}
	base, err := url.Parse(cfg.InspectorURL)
	if err != nil || base.Scheme != "https" || base.Hostname() == "" || base.User != nil || (base.Path != "" && base.Path != "/") || base.RawQuery != "" || base.ForceQuery || base.Fragment != "" {
		return errors.New("invalid HTTPS authority")
	}
	var target wire.SessionRef
	safe := flags["--safe-target"] != ""
	if safe {
		b, e := coordinator.ReadPrivate(flags["--safe-target"])
		if e != nil {
			return e
		}
		target, e = wire.Decode[wire.SessionRef](bytes.NewReader(b), 65536)
		if e != nil {
			return e
		}
	}
	plain := &http.Client{Timeout: 10 * time.Second, CheckRedirect: noRedirect}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return err
	}
	for _, credential := range []string{"", "invalid-" + hex.EncodeToString(random[:])} {
		for _, path := range []string{"/readyz", "/mcp"} {
			resp, e := request(plain, base, path, credential)
			if e != nil {
				return e
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
				return errors.New("unauthenticated request accepted")
			}
		}
	}
	ready, err := request(plain, base, "/readyz", token)
	if err != nil {
		return err
	}
	ready.Body.Close()
	if ready.StatusCode != http.StatusOK || ready.Header.Get("X-Coordinator-Source-Sha") != sha {
		return errors.New("readiness mismatch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	hc := &http.Client{Transport: transport{token: token}, Timeout: 30 * time.Second, CheckRedirect: noRedirect}
	client := mcp.NewClient(&mcp.Implementation{Name: "coordinator-deployment-smoke", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: base.ResolveReference(&url.URL{Path: "/mcp"}).String(), HTTPClient: hc, MaxRetries: -1, DisableStandaloneSSE: true, MaxEventSize: responseLimit}, nil)
	if err != nil {
		return err
	}
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	if err != nil || tools == nil {
		return errors.New("tool discovery failed")
	}
	foundTools := map[string]bool{}
	for _, tool := range tools.Tools {
		foundTools[tool.Name] = true
	}
	if !foundTools["session.list"] || !foundTools["session.inspect"] {
		return errors.New("inspection protocol unavailable")
	}
	if safe {
		cursor := ""
		seen := map[string]bool{}
		found := false
		for range 10 {
			args := map[string]any{"machineId": target.MachineID, "limit": 100}
			if cursor != "" {
				args["cursor"] = cursor
			}
			var result struct {
				Sessions []struct {
					Ref json.RawMessage `json:"ref"`
				} `json:"sessions"`
				NextCursor json.RawMessage `json:"nextCursor"`
			}
			if err = call(ctx, session, "session.list", args, &result); err != nil || result.Sessions == nil || len(result.NextCursor) == 0 {
				return errors.New("invalid list response")
			}
			for _, item := range result.Sessions {
				ref, e := wire.Decode[wire.SessionRef](bytes.NewReader(item.Ref), responseLimit)
				if e != nil {
					return e
				}
				if ref == target {
					found = true
					break
				}
			}
			if found || bytes.Equal(result.NextCursor, []byte("null")) {
				break
			}
			if json.Unmarshal(result.NextCursor, &cursor) != nil || cursor == "" || seen[cursor] {
				return errors.New("invalid cursor")
			}
			seen[cursor] = true
		}
		if !found {
			return errors.New("target not discoverable")
		}
		var result struct {
			Presence struct {
				Ref json.RawMessage `json:"ref"`
			} `json:"presence"`
		}
		if err = call(ctx, session, "session.inspect", map[string]any{"ref": target}, &result); err != nil {
			return errors.New("inspection failed")
		}
		ref, e := wire.Decode[wire.SessionRef](bytes.NewReader(result.Presence.Ref), responseLimit)
		if e != nil || ref != target {
			return errors.New("inspection target mismatch")
		}
	}
	fmt.Printf("{\"scenario\":\"deployment-acceptance\",\"status\":\"ok\",\"sourceSha\":%q,\"verifiedTlsAndHostname\":true,\"authenticationRejected\":true,\"readinessSourceMatched\":true,\"explicitSafeTargetInspected\":%t,\"readOnly\":true}\n", sha, safe)
	return nil
}
func main() {
	if main1() != nil {
		fmt.Fprintln(os.Stderr, `{"scenario":"deployment-acceptance","status":"failed","error":"Acceptance failed; private diagnostics withheld"}`)
		os.Exit(1)
	}
}
