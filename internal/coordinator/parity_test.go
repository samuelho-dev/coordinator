package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestCursorBoundToOwnerAndFilters(t *testing.T) {
	r, a, b, in := fixture(t)
	second := in.Sessions[0]
	second.Ref.NativeSessionID = "second"
	in.Sessions[0].Workspace = &wire.SessionWorkspace{CWD: "/work", Git: &wire.SessionGit{State: "available", ObservedAt: 1, WorktreeRoot: "/work", CommonDir: "/main/.git"}}
	second.Workspace = in.Sessions[0].Workspace
	in.Sessions = append(in.Sessions, second)
	if _, err := r.Announce(a, in); err != nil {
		t.Fatal(err)
	}
	filter := wire.SessionListInput{Limit: 1, MachineID: in.MachineID, Repository: "/main/.git"}
	page, err := r.List(a, filter)
	if err != nil || page.NextCursor == nil || len(page.Sessions) != 1 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	filter.Cursor = *page.NextCursor
	next, err := r.List(a, filter)
	if err != nil || len(next.Sessions) != 1 || next.Sessions[0].Ref.Key() == page.Sessions[0].Ref.Key() {
		t.Fatalf("continuation: %+v %v", next, err)
	}
	for _, tc := range []struct {
		principal wire.InspectorPrincipal
		input     wire.SessionListInput
	}{
		{b, filter}, {a, wire.SessionListInput{Limit: 1, Cursor: filter.Cursor, MachineID: "other", Repository: filter.Repository}},
		{a, wire.SessionListInput{Limit: 1, Cursor: filter.Cursor, MachineID: filter.MachineID}},
		{a, wire.SessionListInput{Limit: 1, Cursor: filter.Cursor, Repository: filter.Repository}},
		{a, wire.SessionListInput{Limit: 1, Cursor: filter.Cursor, MachineID: filter.MachineID, Repository: "/main/.git-other"}},
	} {
		if _, err := r.List(tc.principal, tc.input); err == nil {
			t.Errorf("cursor accepted outside scope: %+v", tc.input)
		}
	}
}

func TestSampleAndLeaseStaleness(t *testing.T) {
	r, a, _, in := fixture(t)
	at := wall() - 31000
	scope := "boot"
	p := wire.MetricProvenance{Source: "native", Scope: "host", ScopeDescription: "host accounting", ObservedAt: &at}
	metric := func(unit string, value float64) wire.Metric { return wire.NewMetric(unit, value, p) }
	resources := wire.MachineResources{Scope: "host", ScopeID: &scope, Source: "native", ObservedAt: at, CPU: wire.MachineCPU{UsagePercent: metric("percent-one-core", 50), CapacityCores: metric("cores", 2), UsedPercent: metric("percent", 25)}, Memory: wire.MachineMemory{UsedBytes: metric("bytes", 20), CapacityBytes: metric("bytes", 100), AvailableBytes: metric("bytes", 80), UsedPercent: metric("percent", 20)}}
	in.Sessions[0].Machine = &wire.SessionMachine{Resources: &resources}
	if _, err := r.Announce(a, in); err != nil {
		t.Fatal(err)
	}
	page, err := r.List(a, wire.SessionListInput{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := page.Machines[0].Resources
	if got == nil || !got.CPU.UsedPercent.Stale || got.ObservedAt != at || got.Memory.Accounting != "host accounting" {
		t.Fatalf("aged sample: %+v", got)
	}
	resources.ObservedAt = wall()
	in.Sessions[0].Machine.Resources = &resources
	if _, err = r.Announce(a, in); err != nil {
		t.Fatal(err)
	}
	page, err = r.List(a, wire.SessionListInput{Limit: 10})
	if err != nil || page.Machines[0].Resources.CPU.UsedPercent.Stale {
		t.Fatalf("fresh sample: %+v %v", page, err)
	}
	if _, err = r.Disconnect(a, wire.ConnectorDisconnect{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation}); err != nil {
		t.Fatal(err)
	}
	page, err = r.List(a, wire.SessionListInput{Limit: 10})
	if err != nil || !page.Machines[0].Resources.CPU.UsedPercent.Stale || page.Machines[0].Resources.ObservedAt != resources.ObservedAt {
		t.Fatalf("disconnected sample: %+v %v", page, err)
	}
}

func TestConcurrentInspectionCorrelation(t *testing.T) {
	r, a, b, in := fixture(t)
	second := in.Sessions[0]
	second.Ref.NativeSessionID = "second"
	in.Sessions = append(in.Sessions, second)
	if _, err := r.Announce(a, in); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Inspect(context.Background(), b, in.Sessions[0].Ref); err == nil {
		t.Fatal("outsider inspected owner session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type response struct {
		got wire.SessionInspection
		err error
	}
	done := make(chan response, 2)
	for _, p := range in.Sessions {
		go func(p wire.SessionPresence) { got, err := r.Inspect(ctx, a, p.Ref); done <- response{got, err} }(p)
	}
	requests := make([]wire.InspectionRequest, 0, 2)
	for len(requests) < 2 && ctx.Err() == nil {
		poll, err := r.Poll(a, wire.ConnectorPoll{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, poll.Requests...)
		if len(requests) < 2 {
			time.Sleep(time.Millisecond)
		}
	}
	if len(requests) != 2 || requests[0].RequestID == requests[1].RequestID {
		t.Fatalf("requests: %+v", requests)
	}
	for i := len(requests) - 1; i >= 0; i-- {
		q := requests[i]
		var p wire.SessionPresence
		for _, candidate := range in.Sessions {
			if candidate.Ref.Key() == q.Ref.Key() {
				p = candidate
			}
		}
		raw, err := json.Marshal(wire.CreateInspection(p, 1))
		if err != nil {
			t.Fatal(err)
		}
		result := wire.ConnectorResult{ConnectorID: in.ConnectorID, Incarnation: in.Incarnation, RequestID: q.RequestID, Observation: raw}
		if _, err = r.Complete(b, result); err == nil {
			t.Fatal("outsider completed inspection")
		}
		if _, err = r.Complete(a, result); err != nil {
			t.Fatal(err)
		}
		if _, err = r.Complete(a, result); err == nil {
			t.Fatal("duplicate completion accepted")
		}
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
			seen[result.got.Presence.Ref.NativeSessionID] = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if len(seen) != 2 || !seen["session"] || !seen["second"] {
		t.Fatalf("incorrect correlation: %v", seen)
	}
}

func TestMCPProtocolAndMalformedArguments(t *testing.T) {
	r, a, _, in := fixture(t)
	if _, err := r.Announce(a, in); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Handler(r))
	defer server.Close()
	for _, version := range []string{"2025-11-25", "2025-06-18", "2025-03-26"} {
		t.Run(version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := mcp.NewClient(&mcp.Implementation{Name: "contract-test", Version: "1"}, nil)
			hc := &http.Client{Transport: &boundedTransport{base: http.DefaultTransport, token: "secret"}}
			session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: hc, DisableStandaloneSSE: true, MaxRetries: -1}, &mcp.ClientSessionOptions{ProtocolVersion: version})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if session.InitializeResult().ProtocolVersion != version {
				t.Fatalf("negotiated %s", session.InitializeResult().ProtocolVersion)
			}
			tools, err := session.ListTools(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			names := map[string]bool{}
			for _, tool := range tools.Tools {
				names[tool.Name] = true
			}
			if len(names) != 5 || !names["session.list"] || !names["session.inspect"] || !names["message.send"] || !names["message.reply"] || !names["message.get"] {
				t.Fatalf("unexpected tools: %v", names)
			}
			for _, tc := range []struct {
				name string
				args map[string]any
			}{
				{"session.list", map[string]any{}}, {"session.list", map[string]any{"limit": 0}}, {"session.list", map[string]any{"limit": 1, "repository": nil}},
				{"session.inspect", map[string]any{"ref": map[string]any{"machineId": "machine", "harness": "omp", "nativeSessionId": "session", "transcript": "private"}}},
			} {
				result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tc.name, Arguments: tc.args})
				if err == nil && (result == nil || !result.IsError) {
					t.Errorf("accepted malformed arguments: %v", tc.args)
				}
				if result != nil {
					b, _ := json.Marshal(result)
					if strings.Contains(string(b), "private") {
						t.Fatal("private argument leaked")
					}
				}
			}
		})
	}
}
