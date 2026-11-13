package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const sourceSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type ref struct {
	MachineID       string `json:"machineId"`
	Harness         string `json:"harness"`
	NativeSessionID string `json:"nativeSessionId"`
	NativeAgentID   string `json:"nativeAgentId,omitempty"`
}
type listResult struct {
	Sessions []struct {
		Ref ref `json:"ref"`
	} `json:"sessions"`
	Machines   []json.RawMessage `json:"machines"`
	NextCursor *string           `json:"nextCursor"`
}
type inspection struct {
	Presence struct {
		Ref ref `json:"ref"`
	} `json:"presence"`
	Resources struct {
		Processes []struct {
			Process struct {
				PID int `json:"pid"`
			} `json:"process"`
			RSS struct {
				Status string `json:"status"`
			} `json:"rssBytes"`
		} `json:"processes"`
		Owned struct {
			Status string `json:"status"`
		} `json:"ownedProcessCount"`
	} `json:"resources"`
}

func (r *listResult) UnmarshalJSON(data []byte) error {
	type plain listResult
	var value plain
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &value) != nil || json.Unmarshal(data, &fields) != nil || value.Sessions == nil || value.Machines == nil {
		return errors.New("invalid session.list result")
	}
	cursor, ok := fields["nextCursor"]
	if !ok {
		return errors.New("missing nextCursor")
	}
	if string(cursor) != "null" {
		var s string
		if json.Unmarshal(cursor, &s) != nil {
			return errors.New("invalid nextCursor")
		}
	}
	*r = listResult(value)
	return nil
}
func (r *inspection) UnmarshalJSON(data []byte) error {
	type plain inspection
	var value plain
	var shape struct {
		Resources *struct {
			Processes []struct {
				Process *struct {
					PID *int `json:"pid"`
				} `json:"process"`
				RSS *struct {
					Status string `json:"status"`
				} `json:"rssBytes"`
			} `json:"processes"`
			Owned *struct {
				Status string `json:"status"`
			} `json:"ownedProcessCount"`
		} `json:"resources"`
	}
	valid := func(s string) bool { return s == "available" || s == "unavailable" }
	if json.Unmarshal(data, &value) != nil || json.Unmarshal(data, &shape) != nil || shape.Resources == nil || shape.Resources.Processes == nil || shape.Resources.Owned == nil || !valid(shape.Resources.Owned.Status) {
		return errors.New("invalid session.inspect result")
	}
	for _, p := range shape.Resources.Processes {
		if p.Process == nil || p.Process.PID == nil || p.RSS == nil || !valid(p.RSS.Status) {
			return errors.New("invalid session.inspect process")
		}
	}
	*r = inspection(value)
	return nil
}

type authTransport struct{ token string }

func (t authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c := r.Clone(r.Context())
	c.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(c)
}

type process struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	done    chan error
	mu      sync.Mutex
	frames  []map[string]any
	invalid bool
	secret  bool
	stopped bool
}

func (p *process) send(v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = p.stdin.Write(append(b, '\n'))
	return e
}
func (p *process) snapshot() ([]map[string]any, bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]map[string]any(nil), p.frames...), p.invalid, p.secret
}
func (p *process) finish(d time.Duration) error {
	_ = p.stdin.Close()
	select {
	case e := <-p.done:
		p.stopped = true
		return e
	case <-time.After(d):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.stopped = true
		return errors.New("child exit deadline exceeded")
	}
}
func start(command string, args []string, env []string, dir, token string, handler func(map[string]any)) (*process, error) {
	c := exec.Command(command, args...)
	c.Env = env
	c.Dir = dir
	in, e := c.StdinPipe()
	if e != nil {
		return nil, e
	}
	out, e := c.StdoutPipe()
	if e != nil {
		return nil, e
	}
	stderr, e := c.StderrPipe()
	if e != nil {
		return nil, e
	}
	if e = c.Start(); e != nil {
		return nil, e
	}
	p := &process{cmd: c, stdin: in, done: make(chan error, 1)}
	var readers sync.WaitGroup
	readers.Add(2)
	go func() {
		defer readers.Done()
		s := bufio.NewScanner(out)
		s.Buffer(make([]byte, 4096), 4<<20)
		total := 0
		for s.Scan() {
			line := s.Bytes()
			if handler == nil {
				p.mu.Lock()
				p.secret = p.secret || bytes.Contains(line, []byte(token))
				p.mu.Unlock()
				continue
			}
			total += len(line)
			var m map[string]any
			bad := total > 4<<20 || bytes.Contains(line, []byte("\x1b]7501")) || json.Unmarshal(line, &m) != nil || m == nil
			p.mu.Lock()
			bad = bad || len(p.frames) >= 4096
			p.invalid = p.invalid || bad
			p.secret = p.secret || bytes.Contains(line, []byte(token))
			if !bad {
				p.frames = append(p.frames, m)
			}
			p.mu.Unlock()
			if !bad {
				handler(m)
			}
		}
		if s.Err() != nil {
			p.mu.Lock()
			p.invalid = true
			p.mu.Unlock()
		}
	}()
	go func() {
		defer readers.Done()
		b, _ := io.ReadAll(io.LimitReader(stderr, 4<<20))
		if bytes.Contains(b, []byte(token)) {
			p.mu.Lock()
			p.secret = true
			p.mu.Unlock()
		}
	}()
	go func() { readers.Wait(); p.done <- c.Wait() }()
	return p, nil
}
func until(label string, f func() (bool, error)) error {
	end := time.Now().Add(30 * time.Second)
	for time.Now().Before(end) {
		ok, e := f()
		if e != nil {
			return e
		}
		if ok {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New(label + " deadline exceeded")
}
func random() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func envSet(base []string, values map[string]string) []string {
	out := make([]string, 0, len(base)+len(values))
	for _, s := range base {
		k, _, _ := strings.Cut(s, "=")
		if _, ok := values[k]; !ok {
			out = append(out, s)
		}
	}
	for k, v := range values {
		out = append(out, k+"="+v)
	}
	return out
}
func isolatedEnv() []string {
	var out []string
	for _, entry := range os.Environ() {
		k, _, _ := strings.Cut(entry, "=")
		if k == "PATH" || k == "LANG" || k == "TERM" || k == "TMPDIR" || strings.HasPrefix(k, "LC_") {
			out = append(out, entry)
		}
	}
	return out
}
func call(s *mcp.ClientSession, name string, args any, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, e := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if e != nil || r == nil || r.IsError {
		return errors.New(name + " failed")
	}
	b, e := json.Marshal(r.StructuredContent)
	if e != nil {
		return errors.New("invalid tool response")
	}
	var w struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(b, &w) != nil || len(w.Result) == 0 || json.Unmarshal(w.Result, out) != nil {
		return errors.New("invalid tool response")
	}
	return nil
}
func list(s *mcp.ClientSession, limit int, cursor *string) (listResult, error) {
	var r listResult
	args := map[string]any{"limit": limit}
	if cursor != nil {
		args["cursor"] = *cursor
	}
	e := call(s, "session.list", args, &r)
	if e == nil && (r.Sessions == nil || r.Machines == nil) {
		e = errors.New("invalid session.list result")
	}
	for _, v := range r.Sessions {
		if v.Ref.MachineID == "" || v.Ref.NativeSessionID == "" || (v.Ref.Harness != "pi" && v.Ref.Harness != "omp" && v.Ref.Harness != "hermes" && v.Ref.Harness != "codex" && v.Ref.Harness != "claude") {
			e = errors.New("invalid session reference")
		}
	}
	return r, e
}
func request(url, method, token string) (int, http.Header, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var body io.Reader
	if method == "POST" {
		body = strings.NewReader("{}")
	}
	req, e := http.NewRequestWithContext(ctx, method, url, body)
	if e != nil {
		return 0, nil, e
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		return 0, nil, e
	}
	defer res.Body.Close()
	return res.StatusCode, res.Header, nil
}
func executable(root, harness string) (string, []string, error) {
	key := "COORDINATOR_TEST_" + strings.ToUpper(harness)
	if v := os.Getenv(key); v != "" {
		p, e := exec.LookPath(v)
		return p, nil, e
	}
	scope := "@earendil-works"
	runtimeName := "node"
	if harness == "omp" {
		scope = "@oh-my-pi"
		runtimeName = "bun"
	}
	dir := filepath.Join(root, "node_modules", scope, "pi-coding-agent")
	data, e := os.ReadFile(filepath.Join(dir, "package.json"))
	if e == nil {
		var manifest struct {
			Bin json.RawMessage `json:"bin"`
		}
		if json.Unmarshal(data, &manifest) == nil {
			var bins map[string]string
			var entry string
			if json.Unmarshal(manifest.Bin, &bins) == nil {
				entry = bins[harness]
			} else {
				_ = json.Unmarshal(manifest.Bin, &entry)
			}
			if entry != "" {
				r, e := exec.LookPath(runtimeName)
				if e != nil {
					return "", nil, e
				}
				return r, []string{filepath.Join(dir, entry)}, nil
			}
		}
	}
	local := filepath.Join(root, "node_modules", ".bin", harness)
	if s, e := os.Stat(local); e == nil && !s.IsDir() {
		return local, nil, nil
	}
	p, e := exec.LookPath(harness)
	return p, nil, e
}
func run() (err error) {
	checkout, e := os.Getwd()
	if e != nil {
		return e
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	binary := os.Getenv("COORDINATOR_BINARY")
	if binary == "" {
		binary = filepath.Join(checkout, "dist", "bin", runtime.GOOS+"-"+arch, "coordinator")
	}
	binary, e = filepath.Abs(binary)
	if e != nil {
		return e
	}
	root, e := os.MkdirTemp("", "coordinator-go-smoke-")
	if e != nil {
		return errors.New("temporary directory failed")
	}
	defer os.RemoveAll(root)
	home := filepath.Join(root, "home")
	if e = os.Mkdir(home, 0700); e != nil {
		return errors.New("home creation failed")
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return errors.New("port allocation failed")
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	token := random()
	digest := sha256.Sum256([]byte(token))
	config := filepath.Join(root, "server.json")
	conf, _ := json.Marshal(map[string]any{"host": "127.0.0.1", "port": port, "principals": []any{map[string]any{"id": "owner", "owner": "smoke", "tokenHash": hex.EncodeToString(digest[:])}}})
	if e = os.WriteFile(config, conf, 0600); e != nil {
		return errors.New("config creation failed")
	}
	env := envSet(isolatedEnv(), map[string]string{"HOME": home, "XDG_CONFIG_HOME": filepath.Join(home, "config"), "XDG_STATE_HOME": filepath.Join(home, "state"), "XDG_CACHE_HOME": filepath.Join(home, "cache"), "PI_OFFLINE": "1", "COORDINATOR_API_URL": url, "COORDINATOR_API_TOKEN": token, "COORDINATOR_CONFIG": config, "COORDINATOR_IMAGE_SHA": sourceSHA})
	var children []*process
	defer func() {
		for _, p := range children {
			if !p.stopped {
				if p.cmd.Process != nil {
					_ = p.cmd.Process.Kill()
				}
				select {
				case <-p.done:
				case <-time.After(3 * time.Second):
				}
			}
			_, invalid, secret := p.snapshot()
			if err == nil && (invalid || secret) {
				err = errors.New("unsafe diagnostics or malformed stdout")
			}
		}
	}()
	inspector, e := start(binary, []string{"inspector"}, env, root, token, nil)
	if e != nil {
		return errors.New("inspector start failed")
	}
	children = append(children, inspector)
	if e = until("inspector readiness", func() (bool, error) {
		s, h, e := request(url+"/readyz", "GET", token)
		return e == nil && s == 200 && h.Get("x-coordinator-source-sha") == sourceSHA, nil
	}); e != nil {
		return e
	}
	for _, credential := range []string{"", "bad-" + random()} {
		for _, path := range []string{"/readyz", "/mcp"} {
			method := "GET"
			if path == "/mcp" {
				method = "POST"
			}
			s, _, e := request(url+path, method, credential)
			if e != nil || (s != 401 && s != 403) {
				return errors.New("unauthenticated request accepted")
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	client := mcp.NewClient(&mcp.Implementation{Name: "coordinator-go-smoke", Version: "1.0.0"}, nil)
	session, e := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url + "/mcp", HTTPClient: &http.Client{Transport: authTransport{token}, Timeout: 10 * time.Second}, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	cancel()
	if e != nil {
		return errors.New("MCP connection failed")
	}
	defer session.Close()
	empty, e := list(session, 100, nil)
	if e != nil {
		return e
	}
	if len(empty.Sessions) != 0 {
		return errors.New("initial list not empty")
	}
	var reverse atomic.Int32
	var natives []*process
	for _, h := range []string{"pi", "omp"} {
		snapshot := map[string]any{"harness": h, "nativeSessionId": "pipe-" + h + "-" + random(), "cwd": root, "isIdle": true}
		var p *process
		ready := make(chan struct{})
		p, e = start(binary, []string{"native", "--harness", h, "--pid", fmt.Sprint(os.Getpid())}, env, root, token, func(m map[string]any) {
			<-ready
			if m["version"] != float64(1) {
				p.mu.Lock()
				p.invalid = true
				p.mu.Unlock()
				return
			}
			if m["method"] == "snapshot" {
				reverse.Add(1)
				if p.send(map[string]any{"version": 1, "id": m["id"], "method": "snapshot", "snapshot": snapshot}) != nil {
					p.mu.Lock()
					p.invalid = true
					p.mu.Unlock()
				}
			}
		})
		if e != nil {
			return errors.New("native start failed")
		}
		close(ready)
		children = append(children, p)
		natives = append(natives, p)
		if p.send(map[string]any{"version": 1, "method": "update", "snapshot": snapshot}) != nil {
			return errors.New("native update failed")
		}
	}
	var all listResult
	if e = until("native announce", func() (bool, error) { var e error; all, e = list(session, 100, nil); return len(all.Sessions) >= 2, e }); e != nil {
		return e
	}
	var refs []ref
	for _, v := range all.Sessions {
		if strings.HasPrefix(v.Ref.NativeSessionID, "pipe-") {
			refs = append(refs, v.Ref)
		}
	}
	if len(refs) != 2 || refs[0].MachineID != refs[1].MachineID || len(all.Machines) != 1 {
		return errors.New("native machine merge failed")
	}
	first, e := list(session, 1, nil)
	if e != nil {
		return e
	}
	if len(first.Sessions) != 1 || first.NextCursor == nil {
		return errors.New("first page invalid")
	}
	second, e := list(session, 1, first.NextCursor)
	if e != nil {
		return e
	}
	if len(second.Sessions) != 1 || first.Sessions[0].Ref.NativeSessionID == second.Sessions[0].Ref.NativeSessionID || reverse.Load() != 0 {
		return errors.New("paging or list getter failed")
	}
	for _, r := range refs {
		var detail inspection
		if e = call(session, "session.inspect", map[string]any{"ref": r}, &detail); e != nil {
			return e
		}
		found := false
		for _, p := range detail.Resources.Processes {
			if p.Process.PID == os.Getpid() && p.RSS.Status == "available" {
				found = true
			}
		}
		if detail.Presence.Ref != r || !found || (r.Harness == "omp" && detail.Resources.Owned.Status != "unavailable") {
			return errors.New("native inspection mismatch")
		}
	}
	if reverse.Load() != 2 {
		return errors.New("fresh snapshot count mismatch")
	}
	for _, p := range natives {
		if e = p.finish(10 * time.Second); e != nil {
			return errors.New("native EOF failed")
		}
	}
	if os.Getenv("COORDINATOR_SMOKE_NATIVE") != "0" {
		for _, h := range []string{"pi", "omp"} {
			if e = hostSmoke(h, checkout, root, home, binary, token, env, session, &children); e != nil {
				return e
			}
		}
	} else {
		fmt.Println("Native host smoke explicitly disabled: COORDINATOR_SMOKE_NATIVE=0")
	}
	return nil
}
func hostSmoke(h, checkout, root, home, binary, token string, env []string, session *mcp.ClientSession, children *[]*process) error {
	exe, prefix, e := executable(checkout, h)
	if e != nil {
		return fmt.Errorf("%s host executable unavailable", h)
	}
	destination := filepath.Join(root, h+"-host")
	agentDir := filepath.Join(home, "."+h, "agent")
	hostEnv := envSet(env, map[string]string{"PI_CODING_AGENT_DIR": agentDir})
	var extra []string
	if h == "omp" {
		if os.MkdirAll(agentDir, 0700) != nil {
			return errors.New("host configuration failed")
		}
		model := `{"providers":{"inspector-smoke":{"baseUrl":"http://127.0.0.1:9/v1","api":"openai-completions","auth":"none","models":[{"id":"inspector-smoke","contextWindow":8192,"maxTokens":1024}]}}}`
		if os.WriteFile(filepath.Join(agentDir, "models.yml"), []byte(model), 0600) != nil {
			return errors.New("host configuration failed")
		}
		extra = []string{"--provider", "inspector-smoke", "--model", "inspector-smoke", "--no-tools", "--no-rules", "--no-skills", "--no-lsp", "--no-ui"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	command := exec.CommandContext(ctx, binary, "install", "--target", h, "--destination", destination, "--source")
	command.Dir = root
	command.Env = hostEnv
	output, e := command.Output()
	cancel()
	if e != nil || bytes.Contains(output, []byte(token)) {
		return errors.New("native install failed")
	}
	var installed struct {
		Plugin string `json:"plugin"`
	}
	plugin := filepath.Join(destination, "dist", "plugins", h+".js")
	if json.Unmarshal(output, &installed) != nil || installed.Plugin != plugin {
		return errors.New("invalid native install")
	}
	if pkg := os.Getenv("COORDINATOR_PACKAGE_ROOT"); pkg != "" {
		data, e := os.ReadFile(filepath.Join(pkg, "dist", "plugins", h+".js"))
		if e != nil || os.WriteFile(plugin, data, 0600) != nil {
			return errors.New("package plugin unavailable")
		}
	}
	args := append(prefix, []string{"-e", plugin, "--mode", "rpc", "--session-dir", home}...)
	args = append(args, extra...)
	host, e := start(exe, args, hostEnv, root, token, func(map[string]any) {})
	if e != nil {
		return errors.New("native host start failed")
	}
	*children = append(*children, host)
	find := func(id string) map[string]any {
		frames, _, _ := host.snapshot()
		for _, f := range frames {
			if f["id"] == id {
				return f
			}
		}
		return nil
	}
	noTurn := func() error {
		frames, invalid, secret := host.snapshot()
		if invalid || secret {
			return errors.New("unsafe native host output")
		}
		for _, f := range frames {
			if f["type"] == "agent_start" || (f["type"] == "prompt_result" && f["agentInvoked"] == true) {
				return errors.New("native list started model turn")
			}
		}
		return nil
	}
	if host.send(map[string]any{"id": "startup", "type": "get_state"}) != nil {
		return errors.New("native startup request failed")
	}
	if e = until("native startup", func() (bool, error) { f := find("startup"); return f != nil && f["success"] == true, noTurn() }); e != nil {
		return e
	}
	if e = until("native host announce", func() (bool, error) {
		r, e := list(session, 100, nil)
		if e != nil {
			return false, e
		}
		for _, s := range r.Sessions {
			if s.Ref.Harness == h && !strings.HasPrefix(s.Ref.NativeSessionID, "pipe-") {
				return true, noTurn()
			}
		}
		return false, noTurn()
	}); e != nil {
		return e
	}
	if host.send(map[string]any{"id": "list", "type": "prompt", "message": "/coordinator list"}) != nil {
		return errors.New("native list request failed")
	}
	if e = until("native list acknowledgment", func() (bool, error) {
		if e := noTurn(); e != nil {
			return false, e
		}
		f := find("list")
		if f == nil || (f["type"] != "response" && f["type"] != "prompt_result") {
			return false, nil
		}
		if f["success"] == false {
			return false, errors.New("native list rejected")
		}
		return true, nil
	}); e != nil {
		return e
	}
	count := 0
	if e = until("native list message", func() (bool, error) {
		if e := noTurn(); e != nil {
			return false, e
		}
		id := fmt.Sprintf("messages-%d", count)
		count++
		if host.send(map[string]any{"id": id, "type": "get_messages"}) != nil {
			return false, errors.New("native messages request failed")
		}
		var f map[string]any
		if e := until("native messages response", func() (bool, error) {
			if e := noTurn(); e != nil {
				return false, e
			}
			f = find(id)
			return f != nil && f["type"] == "response", nil
		}); e != nil {
			return false, e
		}
		if f["success"] != true {
			return false, errors.New("native messages rejected")
		}
		data, ok := f["data"].(map[string]any)
		if !ok {
			return false, errors.New("invalid native messages")
		}
		messages, ok := data["messages"].([]any)
		if !ok {
			return false, errors.New("invalid native messages")
		}
		for _, v := range messages {
			entry, ok := v.(map[string]any)
			if !ok || entry["customType"] != "coordinator-list" {
				continue
			}
			details, ok := entry["details"].(map[string]any)
			if !ok {
				return false, errors.New("invalid native list details")
			}
			if _, ok = details["sessions"].([]any); !ok {
				return false, errors.New("invalid native list details")
			}
			encoded, _ := json.Marshal(entry)
			if bytes.Contains(encoded, []byte(token)) {
				return false, errors.New("native list exposed token")
			}
			return true, noTurn()
		}
		return false, nil
	}); e != nil {
		return e
	}
	if e = host.finish(30 * time.Second); e != nil {
		return errors.New("native host exit failed")
	}
	return noTurn()
}
func main() {
	if e := run(); e != nil {
		b, _ := json.Marshal(map[string]any{"scenario": "go-smoke", "status": "failed", "message": e.Error()})
		fmt.Fprintln(os.Stderr, string(b))
		os.Exit(1)
	}
	fmt.Printf("{\"scenario\":\"ephemeral-go-inspector\",\"status\":\"ok\",\"nativeHosts\":%t}\n", os.Getenv("COORDINATOR_SMOKE_NATIVE") != "0")
}
