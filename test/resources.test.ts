import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, PlatformError } from "effect";
import { assert, it } from "@effect/vitest";
import { identifyProcess, observeResources, parseDarwinSnapshot, type ResourceRoot } from "../src/resources.ts";
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
