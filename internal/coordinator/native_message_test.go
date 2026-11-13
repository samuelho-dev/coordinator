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

func TestReceiverPrivatePipe(t *testing.T) {
	ref := wire.SessionRef{MachineID: "machine", Harness: "hermes", NativeSessionID: "session"}
	announcements := make(chan wire.ConnectorAnnounce, 4)
	acks := make(chan wire.ConnectorMessageAck, 4)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connector/announce":
			var a wire.ConnectorAnnounce
			_ = json.NewDecoder(r.Body).Decode(&a)
			announcements <- a
		case "/connector/poll":
			result := wire.ConnectorPollResult{Requests: []wire.InspectionRequest{}}
			if polls.Add(1) == 1 {
				result.Messages = []wire.Message{{ID: "original", Sender: "peer", To: &ref, Text: "hello", CreatedAt: 1, ExpiresAt: 2, Status: "dispatched"}}
			}
			_ = json.NewEncoder(w).Encode(result)
			return
		case "/connector/message-ack":
			var a wire.ConnectorMessageAck
			_ = json.NewDecoder(r.Body).Decode(&a)
			acks <- a
		}
		_ = json.NewEncoder(w).Encode(wire.ConnectorAccepted{Accepted: true})
	}))
	defer server.Close()
	cfg := Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: server.URL, APIToken: "abcdefghijklmnop", Messaging: true}, MachineID: "machine"}
	hostRead, childWrite := io.Pipe()
	childRead, hostWrite := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- RunReceiver(context.Background(), cfg, "hermes", "session", os.Getppid(), childRead, childWrite)
	}()
	scan := bufio.NewScanner(hostRead)
	if !scan.Scan() {
		t.Fatal("missing ready")
	}
	var ready wire.NativeMessage
	if e := json.Unmarshal(scan.Bytes(), &ready); e != nil || ready.Method != "ready" || ready.Ref == nil || ready.Ref.Key() != ref.Key() {
		t.Fatalf("ready: %+v %v", ready, e)
	}
	select {
	case a := <-announcements:
		if len(a.Sessions) != 0 || len(a.Receivers) != 1 || a.Receivers[0].Key() != ref.Key() {
			t.Fatalf("announce: %+v", a)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing announce")
	}
	frames := make(chan wire.NativeMessage, 1)
	go func() {
		if scan.Scan() {
			var m wire.NativeMessage
			_ = json.Unmarshal(scan.Bytes(), &m)
			frames <- m
		}
	}()
	select {
	case m := <-frames:
		if m.Method != "message" || m.Message == nil || m.Message.ID != "original" {
			t.Fatalf("delivery: %+v", m)
		}
		_ = json.NewEncoder(hostWrite).Encode(wire.NativeMessage{Version: 1, ID: m.ID, Method: "message_ack", Ack: &wire.ConnectorMessageAck{Status: "accepted"}})
	case <-time.After(5 * time.Second):
		t.Fatal("missing delivery")
	}
	select {
	case a := <-acks:
		if a.MessageID != "original" || a.Status != "accepted" {
			t.Fatalf("ack: %+v", a)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing ack")
	}
	_ = hostWrite.Close()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EOF did not stop receiver")
	}
	_ = hostRead.Close()
}

func TestReceiverRequiresOptIn(t *testing.T) {
	if err := RunReceiver(context.Background(), Config{MachineID: "machine"}, "hermes", "session", os.Getppid(), nil, nil); err == nil {
		t.Fatal("disabled receiver accepted")
	}
}

type notifyingWriter struct {
	*io.PipeWriter
	entered chan struct{}
}

func (w notifyingWriter) Write(p []byte) (int, error) { close(w.entered); return w.PipeWriter.Write(p) }

func TestReceiverCancellationUnblocksOutput(t *testing.T) {
	cfg := Config{ConnectorConfig: wire.ConnectorConfig{InspectorURL: "http://127.0.0.1:1", APIToken: "abcdefghijklmnop", Messaging: true}, MachineID: "machine"}
	read, write := io.Pipe()
	defer read.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		done <- RunReceiver(ctx, cfg, "hermes", "session", os.Getppid(), nil, notifyingWriter{write, entered})
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("receiver exited before write: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not write")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not unblock output")
	}
}
