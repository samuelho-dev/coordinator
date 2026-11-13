package coordinator

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestNativeReverseSnapshotAndEOF(t *testing.T) {
	ref := wire.SessionRef{MachineID: "machine", Harness: "omp", NativeSessionID: "session"}
	results := make(chan wire.ConnectorResult, 1)
	announces := make(chan wire.ConnectorAnnounce, 4)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connector/announce":
			var a wire.ConnectorAnnounce
			_ = json.NewDecoder(r.Body).Decode(&a)
			announces <- a
			_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
		case "/connector/poll":
			requests := []wire.InspectionRequest{}
			if polls.Add(1) == 1 {
				requests = append(requests, wire.InspectionRequest{RequestID: "request", Ref: ref})
			}
			_ = json.NewEncoder(w).Encode(wire.ConnectorPollResult{Requests: requests})
		case "/connector/result":
			var result wire.ConnectorResult
			_ = json.NewDecoder(r.Body).Decode(&result)
			results <- result
			_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
		default:
			_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
		}
	}))
	defer server.Close()
	cfg := Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "abcdefghijklmnop"}, MachineID: "machine"}
	hostReader, hostWriter := io.Pipe()
	childReader, childWriter := io.Pipe()
	defer childReader.Close()
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() { done <- RunNative(ctx, cfg, "omp", os.Getppid(), hostReader, childWriter) }()
	send := json.NewEncoder(hostWriter)
	snapshot := wire.NativeSnapshot{Harness: "omp", NativeSessionID: "session", CWD: "/", IsIdle: true}
	if e := send.Encode(wire.NativeMessage{Version: 1, Method: "update", Snapshot: &snapshot}); e != nil {
		t.Fatal(e)
	}
	messages := make(chan wire.NativeMessage, 1)
	go func() {
		scanner := bufio.NewScanner(childReader)
		for scanner.Scan() {
			var m wire.NativeMessage
			if json.Unmarshal(scanner.Bytes(), &m) == nil && m.Method == "snapshot" {
				messages <- m
			}
		}
	}()
	select {
	case request := <-messages:
		if request.Ref == nil || *request.Ref != ref || request.ID == "" {
			t.Fatal("invalid reverse request")
		}
		snapshot.Model = &wire.SessionModel{Provider: "native", ID: "fresh-model"}
		if e := send.Encode(wire.NativeMessage{Version: 1, ID: request.ID, Method: "snapshot", Snapshot: &snapshot}); e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("no reverse request")
	}
	select {
	case result := <-results:
		var item wire.SessionInspection
		if e := json.Unmarshal(result.Observation, &item); e != nil || item.Presence.Model == nil || item.Presence.Model.ID != "fresh-model" {
			t.Fatalf("getter refresh missing: %s", result.Observation)
		}
	case <-ctx.Done():
		t.Fatal("no inspection result")
	}
	<-announces
	snapshot.NativeSessionID = "next"
	if e := send.Encode(wire.NativeMessage{Version: 1, Method: "update", Snapshot: &snapshot}); e != nil {
		t.Fatal(e)
	}
	select {
	case a := <-announces:
		if len(a.Sessions) != 1 || a.Sessions[0].Ref.NativeSessionID != "next" {
			t.Fatal("session switch not announced")
		}
	case <-ctx.Done():
		t.Fatal("session switch timed out")
	}
	_ = hostWriter.Close()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EOF did not stop host")
	}
}

func TestNativeListPreservesPagination(t *testing.T) {
	const token = "pagination-token-12345"
	hash := sha256.Sum256([]byte(token))
	principal := wire.InspectorPrincipal{ID: "owner", Owner: "owner", TokenHash: hex.EncodeToString(hash[:])}
	reg, err := NewRegistry(wire.InspectorConfig{Principals: []wire.InspectorPrincipal{principal}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	sessions := []wire.SessionPresence{}
	for _, id := range []string{"one", "two"} {
		sessions = append(sessions, wire.SessionPresence{Ref: wire.SessionRef{MachineID: "machine", Harness: "pi", NativeSessionID: id}, ObservedAt: 1})
	}
	if _, err := reg.Announce(principal, wire.ConnectorAnnounce{ConnectorID: "connector", Incarnation: "first", MachineID: "machine", Sessions: sessions}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Handler(reg))
	defer server.Close()
	in, input := io.Pipe()
	output, out := io.Pipe()
	defer input.Close()
	defer output.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunNative(ctx, Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: server.URL, APIToken: token}, MachineID: "machine"}, "pi", os.Getppid(), in, out)
	}()
	frames := make(chan wire.NativeMessage, 4)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			var frame wire.NativeMessage
			if json.Unmarshal(scanner.Bytes(), &frame) == nil && frame.ID != "" {
				frames <- frame
			}
		}
	}()
	encoder := json.NewEncoder(input)
	page := func(id, cursor string) wire.SessionListOutput {
		t.Helper()
		if err := encoder.Encode(wire.NativeMessage{Version: 1, ID: id, Method: "list", Input: &wire.SessionListInput{Limit: 1, Cursor: cursor}}); err != nil {
			t.Fatal(err)
		}
		select {
		case frame := <-frames:
			var result wire.SessionListOutput
			if frame.ID != id || frame.Error != nil || json.Unmarshal(frame.Result, &result) != nil || len(result.Sessions) != 1 {
				t.Fatalf("invalid page: %+v", frame)
			}
			return result
		case <-ctx.Done():
			t.Fatal("native list did not respond")
			return wire.SessionListOutput{}
		}
	}
	first := page("first", "")
	if first.NextCursor == nil {
		t.Fatal("first page lost continuation cursor")
	}
	second := page("second", *first.NextCursor)
	if second.NextCursor != nil || second.Sessions[0].Ref == first.Sessions[0].Ref {
		t.Fatal("continuation did not advance to the second session")
	}
	input.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
