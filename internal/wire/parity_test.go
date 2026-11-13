package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPresenceConsumerContract(t *testing.T) {
	base := `{"ref":{"machineId":"mac","harness":"claude","nativeSessionId":"session"},"observedAt":200}`
	for _, extra := range []string{
		``, `,"startedAt":null,"lastActivityAt":null,"processes":[]`,
		`,"terminal":{"name":"future-terminal"}`,
		`,"terminal":{"name":"Terminal","version":"1.0"}`,
		`,"workspace":{"cwd":"/work","git":{"state":"available","observedAt":190,"worktreeRoot":"/work","commonDir":"/main/.git","branch":null,"head":"` + strings.Repeat("a", 64) + `"}}`,
		`,"workspace":{"cwd":"/work","git":{"state":"not-repository","observedAt":190}}`,
		`,"programStatus":{"state":"blocked","kind":"permission"}`,
	} {
		raw := strings.TrimSuffix(base, "}") + extra + `}`
		p, err := Decode[SessionPresence](strings.NewReader(raw), 65536)
		if err != nil {
			t.Errorf("valid %s: %v", raw, err)
			continue
		}
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var want, got any
		if err := json.Unmarshal([]byte(raw), &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		wb, _ := json.Marshal(want)
		gb, _ := json.Marshal(got)
		if string(wb) != string(gb) {
			t.Errorf("round trip: got %s want %s", gb, wb)
		}
	}
	for _, extra := range []string{
		`,"terminal":null`, `,"terminal":{"version":"1"}`, `,"terminal":{"name":"   "}`,
		`,"terminal":{"name":"name\nbad"}`, `,"terminal":{"name":"ok","version":"bad\u0085version"}`,
		`,"terminal":{"name":"` + strings.Repeat("x", 4097) + `"}`, `,"terminal":{"name":"ok","hookToken":"secret"}`,
		`,"agent":{"name":"a","instructions":"private"}`, `,"model":{"id":"m","messages":[]}`,
		`,"machine":{"os":{"platform":"linux","prompt":"private"}}`, `,"workspace":{"cwd":"/work","transcript":"private"}`,
		`,"workspace":{"cwd":"/work","project":{"root":"/work","source":"inferred"}}`,
		`,"workspace":{"cwd":"/work","git":{"state":"available","observedAt":1,"worktreeRoot":"/work","commonDir":"/git","head":null}}`,
		`,"workspace":{"cwd":"/work","git":{"state":"not-repository","observedAt":1,"worktreeRoot":"/work"}}`,
		`,"workspace":{"cwd":"/work","git":{"state":"available","observedAt":1,"worktreeRoot":"/work","commonDir":"/git","branch":null,"head":"` + strings.Repeat("A", 40) + `"}}`,
		`,"programStatus":{"state":"blocked"}`, `,"programStatus":{"state":"working","kind":"permission"}`,
		`,"activity":{"state":"working","toolInput":"private"}`, `,"machineName":"old"`, `,"processes":null`,
		`,"activity":{"state":"working","goals":[]}`, `,"activity":{"state":"working","todos":[]}`,
	} {
		raw := strings.TrimSuffix(base, "}") + extra + `}`
		if _, err := Decode[SessionPresence](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted invalid %s", raw)
		}
	}
	for _, raw := range []string{`{"observedAt":1}`, `{"ref":{"machineId":"m","harness":"other","nativeSessionId":"s"},"observedAt":1}`, `{"ref":{"machineId":"m","harness":"pi","nativeSessionId":"s"}}`} {
		if _, err := Decode[SessionPresence](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted invalid %s", raw)
		}
	}
}

func TestMetricConsumerContract(t *testing.T) {
	provenance := `{"source":"native","scope":"host","scopeDescription":"host accounting","observedAt":100,"nativeUpdatedAt":null,"estimated":false,"derived":false,"shared":false,"partial":false,"stale":false}`
	metric := func(unit, value string) string {
		return `{"status":"available","value":` + value + `,"unit":"` + unit + `","provenance":` + provenance + `}`
	}
	presence := func(cpu, capacity, used string) string {
		return `{"ref":{"machineId":"m","harness":"pi","nativeSessionId":"s"},"observedAt":100,"machine":{"resources":{"scope":"host","scopeId":"boot","source":"native","observedAt":100,"cpu":{"usagePercent":` + metric("percent-one-core", cpu) + `,"capacityCores":` + metric("cores", capacity) + `,"usedPercent":` + metric("percent", "150") + `},"memory":{"usedBytes":` + metric("bytes", used) + `,"capacityBytes":` + metric("bytes", "100") + `,"availableBytes":` + metric("bytes", "0") + `,"usedPercent":` + metric("percent", "110") + `}}}}`
	}
	good := presence("75", "0.5", "9007199254740991")
	if _, err := Decode[SessionPresence](strings.NewReader(good), 65536); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{presence("-1", "0.5", "1"), presence("1", "-1", "1"), presence("1", "1", "0.5"), presence("1", "1", "9007199254740992"), strings.Replace(good, `"unit":"cores"`, `"unit":"count"`, 1), strings.Replace(good, `"scopeId":"boot"`, `"scopeId":""`, 1), strings.Replace(good, `"observedAt":100`, `"observedAt":-1`, 1), strings.Replace(good, `"scope":"host","scopeId"`, `"privatePath":"secret","scope":"host","scopeId"`, 1)} {
		if _, err := Decode[SessionPresence](strings.NewReader(bad), 65536); err == nil {
			t.Errorf("accepted invalid telemetry %s", bad)
		}
	}
	for _, bad := range []string{`{"status":"available","value":null,"unit":"bytes","provenance":` + provenance + `}`, `{"status":"unavailable","value":null,"unit":"bytes","provenance":` + provenance + `}`, `{"status":"available","value":0,"reason":"bad","unit":"bytes","provenance":` + provenance + `}`} {
		if _, err := Decode[Metric](strings.NewReader(bad), 65536); err == nil {
			t.Errorf("accepted invalid metric %s", bad)
		}
	}
	p := SessionPresence{Ref: SessionRef{MachineID: "m", Harness: "pi", NativeSessionID: "s"}, ObservedAt: 100}
	inspection := CreateInspection(p, 100)
	b, err := Encode(inspection)
	if err != nil {
		t.Fatal(err)
	}
	marshaled, err := json.Marshal(inspection)
	if err != nil || string(b) != string(marshaled) {
		t.Fatalf("Encode differs from Marshal: %s %s %v", b, marshaled, err)
	}
	for _, field := range []string{"userMessageCount", "assistantMessageCount", "toolCallCount", "toolResultCount", "totalMessageCount", "pendingMessageCount", "routedModelCounts"} {
		raw := strings.Replace(string(b), `"activity":{`, `"activity":{"`+field+`":`+metric("count", "1")+`,`, 1)
		if _, err := Decode[SessionInspection](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted removed activity field %s", field)
		}
	}
	for _, field := range []string{"context", "tokens", "cost"} {
		raw := strings.Replace(string(b), `"resources":`, `"`+field+`":{},"resources":`, 1)
		if _, err := Decode[SessionInspection](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted removed inspection field %s", field)
		}
	}
	native := `{"harness":"pi","nativeSessionId":"s","cwd":"/work","isIdle":true`
	for _, field := range []string{"context", "usage", "aggregate"} {
		raw := native + `,"` + field + `":{}}`
		if _, err := Decode[NativeSnapshot](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted removed native field %s", field)
		}
	}
	if _, err := Decode[NativeSnapshot](strings.NewReader(native+`}`), 65536); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []string{"tokens", "usd", "premium-requests", "provider-defined"} {
		if _, err := Decode[Metric](strings.NewReader(metric(unit, "1")), 65536); err == nil {
			t.Errorf("accepted removed metric unit %s", unit)
		}
	}
	for _, scope := range []string{"current-context", "latest-native-update"} {
		raw := strings.Replace(metric("count", "1"), `"scope":"host"`, `"scope":"`+scope+`"`, 1)
		if _, err := Decode[Metric](strings.NewReader(raw), 65536); err == nil {
			t.Errorf("accepted removed metric scope %s", scope)
		}
	}
	if _, err := Encode(SessionListInput{Limit: 101}); err == nil {
		t.Fatal("encoded invalid request")
	}
}

func TestNewestWholeMachineMerge(t *testing.T) {
	scope := "boot"
	sample := func(at, cpu, memory float64, stale bool) ListMachine {
		return ListMachine{MachineID: "m", ScopeID: &scope, Resources: &ListMachineResources{ObservedAt: at, CPU: ListMachineCPU{UsedPercent: ListMetric{Value: &cpu, Stale: stale}}, Memory: ListMachineMemory{Accounting: "sample", UsedBytes: ListMetric{Value: &memory, Stale: stale}}}}
	}
	machines := map[string]ListMachine{}
	MergeListMachines(machines, []ListMachine{sample(2, 10, 11, true), sample(1, 90, 91, false), sample(2, 20, 21, false)})
	got := machines[ListMachineKey(sample(2, 0, 0, false))]
	if got.Resources == nil || *got.Resources.CPU.UsedPercent.Value != 20 || *got.Resources.Memory.UsedBytes.Value != 21 || got.Resources.Memory.UsedBytes.Stale {
		t.Fatalf("mixed or stale snapshot: %+v", got)
	}
	MergeListMachines(machines, []ListMachine{sample(3, 30, 31, true)})
	got = machines[ListMachineKey(got)]
	if *got.Resources.CPU.UsedPercent.Value != 30 || *got.Resources.Memory.UsedBytes.Value != 31 {
		t.Fatalf("newest snapshot lost: %+v", got)
	}
}
