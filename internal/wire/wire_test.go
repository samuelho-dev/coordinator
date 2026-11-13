package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBoundaryAndProjection(t *testing.T) {
	input := `{"ref":{"machineId":"m","harness":"pi","nativeSessionId":"s"},"observedAt":1,"startedAt":null,"processes":[]}`
	p, err := Decode[SessionPresence](strings.NewReader(input), int64(len(input)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(ProjectListSession(p))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"startedAt":null`) || !strings.Contains(string(b), `"processes":[]`) {
		t.Fatalf("projection lost optional fields: %s", b)
	}
	p.Processes = nil
	for _, v := range []any{p, ProjectListSession(p)} {
		b, err := json.Marshal(v)
		if err != nil || strings.Contains(string(b), `"processes"`) {
			t.Fatalf("nil processes should be omitted: %s %v", b, err)
		}
	}
	p.Processes = []ProcessIdentity{}
	b, err = json.Marshal(p)
	if err != nil || !strings.Contains(string(b), `"processes":[]`) {
		t.Fatalf("empty processes should be encoded: %s %v", b, err)
	}
	for _, bad := range []string{`{"limit":1,"repository":null}`, `{"limit":101}`, `{"limit":1,"secret":true}`} {
		if _, err := Decode[SessionListInput](strings.NewReader(bad), 1024); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	if _, err := Decode[SessionListInput](strings.NewReader(`{"limit":1,"repository":"/repo"}`), 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode[SessionListInput](strings.NewReader(`{"limit":1}`), 2); err == nil {
		t.Fatal("accepted oversized input")
	}
}
func TestKeyMatchesNativeJSON(t *testing.T) {
	r := SessionRef{MachineID: "<&\u2028\\u2028", Harness: "pi", NativeSessionID: "s"}
	want := "[\"<&\u2028\\\\u2028\",\"pi\",\"s\",null]"
	if got := r.Key(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
