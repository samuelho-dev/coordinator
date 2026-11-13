package coordinator

import (
	"bufio"
	"context"
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
