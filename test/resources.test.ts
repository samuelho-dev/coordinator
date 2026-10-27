import { NodeServices } from "@effect/platform-node";
import { Effect, Exit, FileSystem, PlatformError } from "effect";
import { assert, it } from "@effect/vitest";
import { identifyProcess, observeResources, observeSessionResources, projectSessionResources, parseDarwinSnapshot, type ResourceRoot, type ResourceProcess, type ResourceObservation, type SessionResourceTarget } from "../src/resources.ts";
import { metric, unavailableMetric, type MachineResources, type MetricProvenance, type SessionPresence } from "../src/inspection.ts";
import { Host } from "../src/platform.ts";

const linuxHost = {
  homeDirectory: "/home/test",
  hostname: "test-host",
  release: "test-release",
  platform: "linux",
  architecture: "x64",
  pid: 1,
  ppid: 0,
  uid: 1000,
  executable: "/usr/bin/node",
};
const bootId = "11111111-1111-4111-8111-111111111111";
// Native getconf and monotonic sampling windows require live clocks; /proc contents are fixtures.
const sample = (roots: ReadonlyArray<ResourceRoot>, reuseRoot = false, options: {
  readonly unreadable?: PlatformError.PlatformError;
  readonly enumerated?: ReadonlyArray<string>;
} = {}) => {
  let snapshot = 0;
  const fs = FileSystem.makeNoop({
    readDirectory: () => Effect.sync(() => { snapshot += 1; return [...(options.enumerated ?? ["10", "11", "12"])]; }),
    readFileString: (path) => Effect.gen(function*() {
      if (path === "/proc/sys/kernel/random/boot_id") return `${bootId}\n`;
      const pid = Number(path.split("/").at(2));
      if (pid === 11 && options.unreadable) return yield* options.unreadable;
      const ppid = pid === 10 ? 0 : pid === 11 ? 10 : 11;
      const birth = pid === 10 && reuseRoot && snapshot >= 2 ? 200 : pid;
      const ticks = snapshot >= 2 ? 20 : 10;
      const fields = Array.from({ length: 22 }, (_, index) => {
        if (index === 0) return "S";
        if (index === 1) return `${ppid}`;
        if (index === 11) return `${ticks}`;
        if (index === 12) return "2";
        if (index === 19) return `${birth}`;
        if (index === 21) return "3";
        return "0";
      });
      return `${pid} (worker with ) parentheses) ${fields.join(" ")}\n`;
    }),
  });
  return observeResources(roots).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provide(NodeServices.layer),
    Effect.provideService(Host, linuxHost),
  );
};

it.live("deduplicates overlapping roots and keeps shared descendants out of owned totals", () => Effect.gen(function*() {
  const owned = yield* sample([
    { pid: 10, ownership: "exclusive" },
    { pid: 10, ownership: "exclusive" },
    { pid: 11, ownership: "exclusive" },
  ]);
  assert.strictEqual(owned.processes.length, 3);
  assert.strictEqual(owned.ownedProcessCount, 3);
  assert.strictEqual(owned.partial, false);
  const root = owned.processes.find((value) => value.pid === 10);
  if (root === undefined) assert.fail("root process missing");
  assert.strictEqual(root.startTime, `linux:${bootId}:10`);
  assert.strictEqual(root.ppid, 0);
  if (root.rssBytes === null) assert.fail("root RSS unavailable");
  assert.ok(root.rssBytes > 0);
  assert.strictEqual(owned.ownedRssBytes, root.rssBytes * 3);
  if (root.cpuPercent === null || root.cpuTimeMs === null || root.windowMs === null) assert.fail("root CPU sample unavailable");
  // Fixture CPU delta is 10 native ticks; final cumulative CPU is 22 ticks.
  assert.ok(Math.abs(root.cpuPercent - (root.cpuTimeMs * 10 / 22) / root.windowMs * 100) < 0.000001);
  const shared = yield* sample([
    { pid: 10, ownership: "shared" },
    { pid: 11, ownership: "exclusive" },
    { pid: 12, ownership: "unknown" },
  ]);
  assert.strictEqual(shared.processes.length, 3);
  assert.strictEqual(shared.sharedProcessCount, 3);
  assert.strictEqual(shared.ownedProcessCount, 0);
  assert.strictEqual(shared.ownedCpuPercent, null);
  assert.strictEqual(shared.ownedRssBytes, null);
  assert.strictEqual(shared.partial, false);
}));

it.live("does not attach children when a root birth changes or the expected birth is stale", () => Effect.gen(function*() {
  for (const reuse of [false, true]) {
    const observation = yield* sample([{ pid: 10, startTime: reuse ? `linux:${bootId}:10` : "linux:stale", ownership: "exclusive" }], reuse);
    assert.strictEqual(observation.partial, true);
    assert.strictEqual(observation.ownedProcessCount, 0);
    assert.strictEqual(observation.ownedCpuPercent, null);
    assert.strictEqual(observation.ownedRssBytes, null);
    assert.strictEqual(observation.processes.length, 1);
    assert.strictEqual(observation.processes.at(0)?.unavailableReason, reuse ? "pid-reused" : "start-identity-mismatch");
    assert.strictEqual(observation.processes.at(0)?.rssBytes, null);
  }
}));

it.live("captures presence identity without sampling counters or enumerating descendants", () => Effect.gen(function*() {
  const fs = FileSystem.makeNoop({
    readFileString: (path) => Effect.sync(() => {
      if (path === "/proc/sys/kernel/random/boot_id") return `${bootId}\n`;
      assert.strictEqual(path, "/proc/10/stat");
      const fields = Array.from({ length: 22 }, (_, index) => index === 1 ? "1" : index === 19 ? "12345" : "not-a-counter");
      return `10 (identity only) ${fields.join(" ")}`;
    }),
  });
  const identity = yield* identifyProcess(10).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provide(NodeServices.layer),
    Effect.provideService(Host, linuxHost),
  );
  assert.deepStrictEqual(identity, { status: "available", pid: 10, ppid: 1, startTime: `linux:${bootId}:12345` });
}));

it.live("rejects cached process bindings from a different boot despite identical PID and ticks", () => Effect.gen(function*() {
  const oldBootId = "22222222-2222-4222-8222-222222222222";
  const previous = yield* identifyProcess(10).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({
      readFileString: (path) => Effect.succeed(path === "/proc/sys/kernel/random/boot_id"
        ? `${oldBootId}\n`
        : `10 (identity only) ${Array.from({ length: 22 }, (_, index) => index === 19 ? "10" : "0").join(" ")}`),
    })),
    Effect.provide(NodeServices.layer),
    Effect.provideService(Host, linuxHost),
  );
  if (previous.status !== "available") assert.fail("previous boot identity unavailable");
  const observation = yield* sample([{ pid: 10, startTime: previous.startTime, ownership: "exclusive" }]);
  assert.notStrictEqual(observation.processes.at(0)?.startTime, previous.startTime);
  assert.strictEqual(observation.processes.at(0)?.unavailableReason, "start-identity-mismatch");
  assert.strictEqual(observation.ownedProcessCount, 0);
  assert.strictEqual(observation.ownedRssBytes, null);
}));

it.live("fails honestly when native boot identity is unreadable or malformed", () => Effect.gen(function*() {
  const error = PlatformError.systemError({ _tag: "PermissionDenied", module: "FileSystem", method: "readFile" });
  for (const malformed of [false, true]) {
    const fs = FileSystem.makeNoop({
      readFileString: (path) => {
        if (path === "/proc/sys/kernel/random/boot_id") return malformed ? Effect.succeed("") : Effect.fail(error);
        const fields = Array.from({ length: 22 }, (_, index) => index === 1 ? "1" : index === 19 ? "10" : "0");
        return Effect.succeed(`10 (identity only) ${fields.join(" ")}`);
      },
    });
    for (const operation of [
      identifyProcess(10).pipe(Effect.asVoid),
      observeResources([{ pid: 10, ownership: "exclusive" }]).pipe(Effect.asVoid),
    ]) {
      yield* operation.pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provide(NodeServices.layer),
        Effect.provideService(Host, linuxHost),
        Effect.flip,
      );
    }
  }
}));

it.live("normalizes expected native proc errno failures and preserves missing attribution", () => Effect.gen(function*() {
  for (const code of ["EPERM", "ESRCH"]) {
    const error = PlatformError.systemError({
      _tag: "Unknown", module: "FileSystem", method: "readFile",
      cause: Object.assign(new Error(), { code }),
    });
    const identity = yield* identifyProcess(11).pipe(
      Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.fail(error) })),
      Effect.provide(NodeServices.layer),
      Effect.provideService(Host, linuxHost),
    );
    assert.deepStrictEqual(identity, { status: "unavailable", reason: "process-exited-or-permission-denied" });
    const observation = yield* sample([{ pid: 10, ownership: "exclusive" }], false, { unreadable: error });
    assert.strictEqual(observation.partial, true);
    assert.strictEqual(observation.unavailableReason, "process-exited-or-permission-denied");
    assert.strictEqual(observation.ownedProcessCount, 1);
    assert.deepStrictEqual(observation.processes.map((value) => value.pid), [10]);
    assert.ok(observation.ownedRssBytes !== null && observation.ownedRssBytes > 0);
  }
}));

it.live("retains unexpected native proc errors", () => Effect.gen(function*() {
  const error = PlatformError.systemError({
    _tag: "Unknown", module: "FileSystem", method: "readFile",
    cause: Object.assign(new Error(), { code: "EIO" }),
  });
  const failure = yield* identifyProcess(11).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.fail(error) })),
    Effect.provide(NodeServices.layer),
    Effect.provideService(Host, linuxHost),
    Effect.flip,
  );
  assert.strictEqual(failure.cause, error);
}));

it.live("reports truncation at the unique PID union boundary while retaining requested roots", () => Effect.gen(function*() {
  for (const count of [4095, 4096]) {
    const observation = yield* sample([{ pid: 10, ownership: "exclusive" }], false, {
      enumerated: Array.from({ length: count }, (_, index) => `${index + 100}`),
    });
    assert.strictEqual(observation.truncated, count === 4096);
    assert.strictEqual(observation.partial, count === 4096);
    assert.strictEqual(observation.ownedProcessCount, 1);
    assert.strictEqual(observation.processes.at(0)?.pid, 10);
  }
}));

it.effect("marks ambiguous Darwin task zeroes unavailable without rejecting idle resident tasks", () => Effect.gen(function*() {
  const snapshot = yield* parseDarwinSnapshot({
    text: "10 1 Mon Oct 19 12:00:00 2026 0:00.00 0\n11 10 Mon Oct 19 12:00:01 2026 0:00.00 64\n12 10 Mon Oct 19 12:00:02 2026 0:01.25 128\n",
    truncated: false,
  });
  assert.strictEqual(snapshot.unavailable.get(10), "task-counters-unreadable-or-zero");
  assert.strictEqual(snapshot.counters.get(10)?.startTime, "darwin:Mon Oct 19 12:00:00 2026");
  assert.strictEqual(snapshot.unavailable.has(11), false);
  assert.strictEqual(snapshot.counters.get(11)?.cpuTimeMs, 0);
  assert.strictEqual(snapshot.counters.get(11)?.rssBytes, 64 * 1024);
  assert.strictEqual(snapshot.counters.get(12)?.cpuTimeMs, 1250);
  assert.strictEqual(snapshot.counters.get(12)?.rssBytes, 128 * 1024);
  assert.strictEqual(snapshot.truncated, false);
}));

const provenance: MetricProvenance = {
  source: "fixture", scope: "cgroup", scopeDescription: "Fixture container",
  observedAt: 1000, nativeUpdatedAt: null, estimated: false, derived: false, shared: true, partial: false, stale: false,
};
const capacity: MachineResources = {
  scope: "cgroup", scopeId: "machine:fixture", source: "fixture", observedAt: 1000,
  cpu: { capacityCores: metric("cores", 2, provenance), usagePercent: unavailableMetric("percent-one-core"), usedPercent: unavailableMetric("percent") },
  memory: { capacityBytes: metric("bytes", 1000, provenance), usedBytes: unavailableMetric("bytes"), availableBytes: unavailableMetric("bytes"), usedPercent: unavailableMetric("percent") },
};
const session = (id: string, pid: number, ownership: "owned" | "shared" | "unknown" = "owned"): SessionPresence => ({
  ref: { machineId: "fixture", harness: "omp", nativeSessionId: id }, observedAt: 1000,
  machine: { resources: capacity }, processes: [{ pid, startIdentity: `birth:${pid}`, ownership }],
});
const processRow = (pid: number, ppid: number, cpuPercent: number | null, rssBytes: number | null): ResourceProcess => ({
  pid, ppid, startTime: `birth:${pid}`, ownership: "exclusive", cpuPercent, rssBytes, cpuTimeMs: 500,
  sampleStartedAt: 1000, sampledAt: 1200, windowMs: 200, unavailableReason: null,
});
const observation = (processes: readonly ResourceProcess[], partial = false): ResourceObservation => ({
  processes, sampleStartedAt: 1000, sampledAt: 1200, windowMs: 200, ownedProcessCount: processes.length,
  ownedCpuPercent: null, ownedRssBytes: null, sharedProcessCount: 0, unknownProcessCount: 0,
  partial, truncated: false, unavailableReason: partial ? "incomplete" : null,
});
const target = (pid: number, before = capacity, after = before): SessionResourceTarget => ({
  pid, startIdentity: `birth:${pid}`, before, after,
});

it.effect("normalizes actual session work and partitions separately identified child trees without duplicate charging", () => Effect.sync(() => {
  const sessions = [session("parent", 10), session("child", 11)];
  const rows = [processRow(10, 0, 150, 200), processRow(11, 10, 50, 100), processRow(12, 11, 25, 50)];
  const result = projectSessionResources(sessions, observation(rows), [target(10), target(11)], 1200);
  assert.strictEqual(result[0]?.resources?.cpuPercent.value, 150);
  assert.strictEqual(result[0]?.resources?.cpuCapacityPercent.value, 75);
  assert.strictEqual(result[0]?.resources?.rssCapacityPercent.value, 20);
  assert.strictEqual(result[1]?.resources?.cpuPercent.value, 75);
  assert.strictEqual(result[1]?.resources?.cpuCapacityPercent.value, 37.5);
  assert.strictEqual(result[1]?.resources?.rssCapacityPercent.value, 15);
  assert.deepStrictEqual(result[1]?.resources?.cpuPercent.provenance.sampleWindow, { startedAt: 1000, endedAt: 1200 });
  const unclamped = projectSessionResources([session("parent", 10)], observation([processRow(10, 0, 500, 2000)]), [target(10)], 1200);
  assert.strictEqual(unclamped[0]?.resources?.cpuCapacityPercent.value, 250);
  assert.strictEqual(unclamped[0]?.resources?.rssCapacityPercent.value, 200);
}));

it.effect("reports shared measurements without splitting and restores exclusive attribution after a claimant retires", () => Effect.sync(() => {
  const sample = observation([processRow(10, 0, 120, 300)]);
  const shared = projectSessionResources([session("a", 10), session("b", 10)], sample, [target(10)], 1200);
  for (const presence of shared) {
    assert.strictEqual(presence.resources?.attribution, "shared");
    assert.strictEqual(presence.resources?.cpuCapacityPercent.value, 60);
    assert.strictEqual(presence.resources?.rssCapacityPercent.value, 30);
    assert.strictEqual(presence.resources?.cpuPercent.provenance.shared, true);
    assert.strictEqual(presence.resources?.rssCapacityPercent.provenance.shared, true);
  }
  const refreshed = { ...shared[0], ...session("a", 10) };
  const remaining = projectSessionResources([refreshed], sample, [target(10)], 1200)[0]?.resources;
  assert.strictEqual(remaining?.attribution, "owned");
  assert.strictEqual(remaining?.cpuCapacityPercent.provenance.shared, false);
  const explicit = projectSessionResources([session("explicit", 10, "shared")], sample, [target(10)], 1200)[0]?.resources;
  assert.strictEqual(explicit?.attribution, "shared");
  assert.strictEqual(explicit?.cpuCapacityPercent.provenance.scope, "process");
  assert.strictEqual(projectSessionResources([session("unknown", 10, "unknown")], sample, [target(10)], 1200)[0]?.resources?.attribution, "unknown");
}));

it.effect("rejects missing stale nonpositive and changed denominators independently without hiding process use", () => Effect.sync(() => {
  const sample = observation([processRow(10, 0, 100, 200)]);
  const badCpu: MachineResources[] = [
    { ...capacity, cpu: { ...capacity.cpu, capacityCores: unavailableMetric("cores") } },
    { ...capacity, cpu: { ...capacity.cpu, capacityCores: metric("cores", 0, provenance) } },
    { ...capacity, cpu: { ...capacity.cpu, capacityCores: metric("cores", 2, { ...provenance, stale: true }) } },
    { ...capacity, cpu: { ...capacity.cpu, capacityCores: metric("cores", 4, provenance) } },
  ];
  for (const after of badCpu) {
    const resources = projectSessionResources([session("a", 10)], sample, [target(10, capacity, after)], 1200)[0]?.resources;
    assert.strictEqual(resources?.cpuPercent.value, 100);
    assert.strictEqual(resources?.cpuCapacityPercent.status, "unavailable");
    assert.strictEqual(resources?.rssCapacityPercent.value, 20);
  }
  const memoryMissing = { ...capacity, memory: { ...capacity.memory, capacityBytes: unavailableMetric("bytes") } };
  const resources = projectSessionResources([session("a", 10)], sample, [target(10, capacity, memoryMissing)], 1200)[0]?.resources;
  assert.strictEqual(resources?.cpuCapacityPercent.value, 50);
  assert.strictEqual(resources?.rssCapacityPercent.status, "unavailable");
  const staleSource = { ...session("a", 10), machine: { resources: { ...capacity, cpu: { ...capacity.cpu, capacityCores: metric("cores", 2, { ...provenance, stale: true }) } } } };
  assert.strictEqual(projectSessionResources([staleSource], sample, [target(10)], 1200)[0]?.resources?.cpuCapacityPercent.status, "unavailable");
}));

it.effect("never substitutes unrelated scope or changed birth and preserves inaccessible source summaries stale", () => Effect.sync(() => {
  const presence = session("a", 10);
  const sample = observation([processRow(10, 0, 100, 200)]);
  const captured = projectSessionResources([presence], sample, [target(10)], 1200)[0];
  if (!captured) assert.fail("Source summary missing");
  for (const scopeId of [null, "machine:other"]) {
    const other = { ...capacity, scopeId };
    const inaccessible = projectSessionResources([captured], sample, [target(10, other)], 5000)[0]?.resources;
    assert.strictEqual(inaccessible?.observedAt, 1200);
    assert.strictEqual(inaccessible?.cpuCapacityPercent.value, 50);
    assert.strictEqual(inaccessible?.cpuCapacityPercent.provenance.stale, true);
    const changed = projectSessionResources([presence], sample, [target(10, capacity, other)], 1200)[0]?.resources;
    assert.strictEqual(changed?.cpuCapacityPercent.status, "unavailable");
    assert.strictEqual(changed?.rssCapacityPercent.status, "unavailable");
  }
  for (const startTime of [null, "birth:reused"]) {
    const changed = projectSessionResources([presence], observation([{ ...processRow(10, 0, 100, 200), startTime }]), [target(10)], 1200)[0]?.resources;
    assert.strictEqual(changed?.cpuPercent.status, "unavailable");
    assert.strictEqual(changed?.rssBytes.status, "unavailable");
  }
  assert.strictEqual(projectSessionResources([presence], sample, [], 1200)[0]?.resources?.cpuPercent.status, "unavailable");
}));

it.effect("retains partial CPU or RSS honestly and marks unavailable sessions explicitly", () => Effect.sync(() => {
  for (const [cpu, rss] of [[null, 200], [100, null]] as const) {
    const resources = projectSessionResources([session("a", 10)], observation([processRow(10, 0, cpu, rss)], true), [target(10)], 1200)[0]?.resources;
    assert.strictEqual(resources?.cpuPercent.value, cpu);
    assert.strictEqual(resources?.rssBytes.value, rss);
    assert.strictEqual(resources?.cpuCapacityPercent.value, cpu === null ? null : 50);
    assert.strictEqual(resources?.rssCapacityPercent.value, rss === null ? null : 20);
    assert.strictEqual(resources?.cpuPercent.provenance.partial, true);
  }
  const unbound = { ...session("a", 10), processes: [] };
  const resources = projectSessionResources([unbound], null, [], 1200)[0]?.resources;
  assert.strictEqual(resources?.attribution, "unknown");
  assert.strictEqual(resources?.cpuPercent.status, "unavailable");
  assert.strictEqual(resources?.rssBytes.status, "unavailable");
}));

it.effect("publishes unavailable unsupported observations without requiring a process scan", () => Effect.gen(function*() {
  const result = yield* observeSessionResources([session("a", 10)]).pipe(
    Effect.provide(NodeServices.layer), Effect.provideService(Host, { ...linuxHost, platform: "unsupported" }),
  );
  assert.strictEqual(result.sample, null);
  assert.strictEqual(result.sessions[0]?.resources?.cpuCapacityPercent.status, "unavailable");
  assert.strictEqual(result.sessions[0]?.resources?.rssCapacityPercent.status, "unavailable");
}));

it.live("reuses a single raw batch for bound sessions without machine denominators and preserves operational failures", () => Effect.gen(function*() {
  let scans = 0;
  const fs = FileSystem.makeNoop({
    readDirectory: () => Effect.sync(() => { scans += 1; return ["10", "11", "12"]; }),
    readFileString: (path) => Effect.sync(() => {
      if (path === "/proc/sys/kernel/random/boot_id") return bootId;
      if (!path.endsWith("/stat")) return "";
      const pid = Number(path.split("/").at(2));
      const fields = Array.from({ length: 22 }, (_, index) =>
        index === 1 ? `${pid === 10 ? 0 : pid - 1}` : index === 19 ? `${pid}` : index === 11 ? `${scans * pid}` : index === 21 ? "3" : "0");
      return `${pid} (fixture) ${fields.join(" ")}`;
    }),
  });
  const presences: SessionPresence[] = [10, 11].map((pid) => ({
    ref: { machineId: "fixture", harness: "omp", nativeSessionId: `${pid}` }, observedAt: 1000,
    processes: [{ pid, startIdentity: `linux:${bootId}:${pid}`, ownership: "owned" }],
  }));
  const result = yield* observeSessionResources(presences).pipe(
    Effect.provideService(FileSystem.FileSystem, fs), Effect.provide(NodeServices.layer), Effect.provideService(Host, linuxHost),
  );
  assert.strictEqual(scans, 2);
  assert.strictEqual(result.sample?.processes.length, 3);
  assert.strictEqual(result.sample?.ownedProcessCount, 3);
  assert.strictEqual(result.sessions[0]?.resources?.cpuCapacityPercent.status, "unavailable");
  assert.strictEqual(result.sessions[1]?.resources?.rssCapacityPercent.status, "unavailable");
  const denied = PlatformError.systemError({ _tag: "PermissionDenied", module: "FileSystem", method: "readFile" });
  const unavailable = yield* observeSessionResources(presences).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.fail(denied) })),
    Effect.provide(NodeServices.layer), Effect.provideService(Host, linuxHost),
  );
  assert.strictEqual(unavailable.sample, null);
  assert.strictEqual(unavailable.sessions[0]?.resources?.cpuPercent.status, "unavailable");
}));

it.effect("does not swallow resource sampler defects", () => Effect.gen(function*() {
  const defect = new Error("fixture implementation defect");
  const exit = yield* observeSessionResources([session("a", 10)]).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.die(defect) })),
    Effect.provide(NodeServices.layer), Effect.provideService(Host, linuxHost), Effect.exit,
  );
  assert.deepStrictEqual(exit, Exit.die(defect));
}));
