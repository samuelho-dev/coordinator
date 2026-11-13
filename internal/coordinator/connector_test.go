package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuelho-dev/coordinator/internal/wire"
)

func TestRunSourceRequestsFreshInspection(t *testing.T) {
	ref := wire.SessionRef{MachineID: "machine", Harness: "pi", NativeSessionID: "session"}
	result := make(chan wire.ConnectorResult, 1)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connector/poll":
			requests := []wire.InspectionRequest{}
			if polls.Add(1) == 1 {
				requests = append(requests, wire.InspectionRequest{RequestID: "request", Ref: ref})
			}
			_ = json.NewEncoder(w).Encode(wire.ConnectorPollResult{Requests: requests})
		case "/connector/result":
			var v wire.ConnectorResult
			_ = json.NewDecoder(r.Body).Decode(&v)
			result <- v
			_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
		default:
			_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
		}
	}))
	defer server.Close()
	cfg := Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "abcdefghijklmnop"}, MachineID: "machine"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- RunSource(ctx, cfg, Source{Snapshot: func(context.Context) ([]wire.SessionPresence, error) { return []wire.SessionPresence{{Ref: ref}}, nil }, Inspect: func(_ context.Context, r wire.SessionRef) (wire.SessionInspection, error) {
			calls.Add(1)
			return wire.CreateInspection(wire.SessionPresence{Ref: r}, float64(time.Now().UnixMilli())), nil
		}}, nil)
	}()
	select {
	case got := <-result:
		if got.RequestID != "request" || calls.Load() != 1 {
			t.Fatal("inspection callback not used")
		}
		var item wire.SessionInspection
		if e := json.Unmarshal(got.Observation, &item); e != nil || item.Presence.Ref != ref {
			t.Fatalf("wrong observation: %s", got.Observation)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("inspection not completed")
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connector did not stop")
	}
}
