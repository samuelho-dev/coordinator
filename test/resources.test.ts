import assert from "node:assert/strict";
import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, PlatformError } from "effect";
import { it } from "vitest";
import { identifyProcess, observeResources, parseDarwinSnapshot, type ResourceRoot } from "../src/resources.ts";
const bootId = "11111111-1111-4111-8111-111111111111";
// Native getconf supplies this host's units; /proc counters and boot identity are deterministic fixtures.
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
  return Effect.runPromise(observeResources(roots).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provide(NodeServices.layer),
  ));
};

it.skipIf(process.platform !== "linux")("deduplicates overlapping roots and keeps shared descendants out of owned totals", async () => {
  const owned = await sample([
    { pid: 10, ownership: "exclusive" },
    { pid: 10, ownership: "exclusive" },
    { pid: 11, ownership: "exclusive" },
  ]);
  assert.equal(owned.processes.length, 3);
  assert.equal(owned.ownedProcessCount, 3);
  assert.equal(owned.partial, false);
  const root = owned.processes.find((value) => value.pid === 10);
  assert.ok(root);
  assert.equal(root.startTime, `linux:${bootId}:10`);
  assert.equal(root.ppid, 0);
  assert.ok(root.rssBytes !== null && root.rssBytes > 0);
  assert.equal(owned.ownedRssBytes, root.rssBytes * 3);
  assert.ok(root.cpuPercent !== null && root.cpuTimeMs !== null && root.windowMs !== null);
  // Fixture CPU delta is 10 native ticks; final cumulative CPU is 22 ticks.
  assert.ok(Math.abs(root.cpuPercent - (root.cpuTimeMs * 10 / 22) / root.windowMs * 100) < 0.000001);
  const shared = await sample([
    { pid: 10, ownership: "shared" },
    { pid: 11, ownership: "exclusive" },
    { pid: 12, ownership: "unknown" },
  ]);
  assert.equal(shared.processes.length, 3);
  assert.equal(shared.sharedProcessCount, 3);
  assert.equal(shared.ownedProcessCount, 0);
  assert.equal(shared.ownedCpuPercent, null);
  assert.equal(shared.ownedRssBytes, null);
  assert.equal(shared.partial, false);
});

it.skipIf(process.platform !== "linux")("does not attach children when a root birth changes or the expected birth is stale", async () => {
  for (const reuse of [false, true]) {
    const observation = await sample([{ pid: 10, startTime: reuse ? `linux:${bootId}:10` : "linux:stale", ownership: "exclusive" }], reuse);
    assert.equal(observation.partial, true);
    assert.equal(observation.ownedProcessCount, 0);
    assert.equal(observation.ownedCpuPercent, null);
    assert.equal(observation.ownedRssBytes, null);
    assert.equal(observation.processes.length, 1);
    assert.equal(observation.processes.at(0)?.unavailableReason, reuse ? "pid-reused" : "start-identity-mismatch");
    assert.equal(observation.processes.at(0)?.rssBytes, null);
  }
});

it.skipIf(process.platform !== "linux")("captures presence identity without sampling counters or enumerating descendants", async () => {
  const fs = FileSystem.makeNoop({
    readFileString: (path) => Effect.sync(() => {
      if (path === "/proc/sys/kernel/random/boot_id") return `${bootId}\n`;
      assert.equal(path, "/proc/10/stat");
      const fields = Array.from({ length: 22 }, (_, index) => index === 1 ? "1" : index === 19 ? "12345" : "not-a-counter");
      return `10 (identity only) ${fields.join(" ")}`;
    }),
  });
  const identity = await Effect.runPromise(identifyProcess(10).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provide(NodeServices.layer),
  ));
  assert.deepEqual(identity, { status: "available", pid: 10, ppid: 1, startTime: `linux:${bootId}:12345` });
});

it.skipIf(process.platform !== "linux")("rejects cached process bindings from a different boot despite identical PID and ticks", async () => {
  const oldBootId = "22222222-2222-4222-8222-222222222222";
  const previous = await Effect.runPromise(identifyProcess(10).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({
      readFileString: (path) => Effect.succeed(path === "/proc/sys/kernel/random/boot_id"
        ? `${oldBootId}\n`
        : `10 (identity only) ${Array.from({ length: 22 }, (_, index) => index === 19 ? "10" : "0").join(" ")}`),
    })),
    Effect.provide(NodeServices.layer),
  ));
  assert.equal(previous.status, "available");
  if (previous.status !== "available") assert.fail("previous boot identity unavailable");
  const observation = await sample([{ pid: 10, startTime: previous.startTime, ownership: "exclusive" }]);
  assert.notEqual(observation.processes.at(0)?.startTime, previous.startTime);
  assert.equal(observation.processes.at(0)?.unavailableReason, "start-identity-mismatch");
  assert.equal(observation.ownedProcessCount, 0);
  assert.equal(observation.ownedRssBytes, null);
});

it.skipIf(process.platform !== "linux")("fails honestly when native boot identity is unreadable or malformed", async () => {
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
      const failure = await Effect.runPromise(operation.pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provide(NodeServices.layer),
        Effect.flip,
      ));
      assert.equal(failure.operation, malformed ? "parse Linux boot identity" : "read Linux boot identity");
    }
  }
});

it.skipIf(process.platform !== "linux")("normalizes expected native proc errno failures and preserves missing attribution", async () => {
  for (const code of ["EPERM", "ESRCH"]) {
    const error = PlatformError.systemError({
      _tag: "Unknown", module: "FileSystem", method: "readFile",
      cause: Object.assign(new Error(), { code }),
    });
    const identity = await Effect.runPromise(identifyProcess(11).pipe(
      Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.fail(error) })),
      Effect.provide(NodeServices.layer),
    ));
    assert.deepEqual(identity, { status: "unavailable", reason: "process-exited-or-permission-denied" });
    const observation = await sample([{ pid: 10, ownership: "exclusive" }], false, { unreadable: error });
    assert.equal(observation.partial, true);
    assert.equal(observation.unavailableReason, "process-exited-or-permission-denied");
    assert.equal(observation.ownedProcessCount, 1);
    assert.deepEqual(observation.processes.map((value) => value.pid), [10]);
    assert.ok(observation.ownedRssBytes !== null && observation.ownedRssBytes > 0);
  }
});

it.skipIf(process.platform !== "linux")("retains unexpected native proc errors", async () => {
  const error = PlatformError.systemError({
    _tag: "Unknown", module: "FileSystem", method: "readFile",
    cause: Object.assign(new Error(), { code: "EIO" }),
  });
  const failure = await Effect.runPromise(identifyProcess(11).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({ readFileString: () => Effect.fail(error) })),
    Effect.provide(NodeServices.layer),
    Effect.flip,
  ));
  assert.equal(failure.operation, "read process counters");
  assert.equal(failure.cause, error);
});

it.skipIf(process.platform !== "linux")("reports truncation at the unique PID union boundary while retaining requested roots", async () => {
  for (const count of [4095, 4096]) {
    const observation = await sample([{ pid: 10, ownership: "exclusive" }], false, {
      enumerated: Array.from({ length: count }, (_, index) => `${index + 100}`),
    });
    assert.equal(observation.truncated, count === 4096);
    assert.equal(observation.partial, count === 4096);
    assert.equal(observation.ownedProcessCount, 1);
    assert.equal(observation.processes.at(0)?.pid, 10);
  }
});

it("marks ambiguous Darwin task zeroes unavailable without rejecting idle resident tasks", async () => {
  const snapshot = await Effect.runPromise(parseDarwinSnapshot({
    text: "10 1 Mon Oct 19 12:00:00 2026 0:00.00 0\n11 10 Mon Oct 19 12:00:01 2026 0:00.00 64\n12 10 Mon Oct 19 12:00:02 2026 0:01.25 128\n",
    truncated: false,
  }));
  assert.equal(snapshot.unavailable.get(10), "task-counters-unreadable-or-zero");
  assert.equal(snapshot.counters.get(10)?.startTime, "darwin:Mon Oct 19 12:00:00 2026");
  assert.equal(snapshot.unavailable.has(11), false);
  assert.equal(snapshot.counters.get(11)?.cpuTimeMs, 0);
  assert.equal(snapshot.counters.get(11)?.rssBytes, 64 * 1024);
  assert.equal(snapshot.counters.get(12)?.cpuTimeMs, 1250);
  assert.equal(snapshot.counters.get(12)?.rssBytes, 128 * 1024);
  assert.equal(snapshot.truncated, false);
});
