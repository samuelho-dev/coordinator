import { NodeServices } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import { Clock, Effect, FileSystem, Option, PlatformError, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { MachineResources } from "../src/inspection.ts";
import { makeMachineSampler } from "../src/machine.ts";
import { Host } from "../src/platform.ts";

const host = { homeDirectory: "/home/test", hostname: "fixture", release: "fixture", platform: "linux", architecture: "x64", pid: 42, ppid: 1, uid: 1000, executable: "/usr/bin/node" };
const boot = "11111111-1111-4111-8111-111111111111";
const missing = (file: string, denied = false) => PlatformError.systemError({ _tag: denied ? "PermissionDenied" : "NotFound", module: "FileSystem", method: "readFileString", pathOrDescriptor: file });
const fixture = Effect.gen(function*() {
  const nativeFs = yield* FileSystem.FileSystem;
  const info = yield* nativeFs.stat("/tmp");
  const clock = yield* Clock.Clock;
  let time = 1_000_000_000n;
  let wall = 1000;
  const files = new Map<string, string>([
    ["/proc/self/cgroup", "0::/"],
    ["/proc/self/mountinfo", "1 0 0:28 / /cg ro - cgroup2 cgroup rw"],
    ["/proc/sys/kernel/random/boot_id", boot],
    ["/sys/devices/system/cpu/online", "0-7"],
    ["/proc/meminfo", "MemTotal: 8192 kB\nMemAvailable: 4096 kB\nMemFree: 1 kB"],
    ["/cg/cpu.stat", "usage_usec 1000000"], ["/cg/cpu.max", "200000 100000"], ["/cg/cpuset.cpus.effective", "0-3"],
    ["/cg/memory.current", "4096"], ["/cg/memory.max", "16384"],
  ]);
  const denied = new Set<string>();
  let reads = 0;
  let inode = 100;
  const fs = FileSystem.makeNoop({
    readFileString: (file) => Effect.suspend(() => {
      reads += 1;
      const value = files.get(file);
      return value !== undefined && !denied.has(file) ? Effect.succeed(value) : Effect.fail(missing(file, denied.has(file)));
    }),
    stat: () => Effect.sync(() => ({ ...info, ino: Option.some(inode) })),
    readLink: () => Effect.succeed("cgroup:[123]"),
  });
  const localClock = { ...clock, monotonicTimeNanos: Effect.sync(() => time), currentTimeMillis: Effect.sync(() => wall) };
  const makeTarget = (pid?: number) => makeMachineSampler(pid).pipe(Effect.provideService(FileSystem.FileSystem, fs), Effect.provideService(Host, host), Effect.provideService(Clock.Clock, localClock));
  const make = makeTarget();
  return { files, denied, make, makeTarget, fs, localClock, advance: (nanos = 1_000_000_000n) => { time += nanos; wall += Number(nanos / 1_000_000n); }, replace: () => { inode += 1; }, reads: () => reads };
}).pipe(Effect.provide(NodeServices.layer));

it.effect("samples cgroup namespace root directly, shares concurrent reads and preserves quota bursts", () => Effect.gen(function*() {
  const f = yield* fixture;
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  const first = yield* sampler.read;
  assert.strictEqual(first.scope, "cgroup");
  assert.ok(first.scopeId);
  assert.strictEqual(first.cpu.usagePercent.status, "unavailable");
  assert.strictEqual(first.cpu.capacityCores.value, 2);
  assert.strictEqual(first.memory.usedBytes.value, 4096);
  assert.strictEqual(first.memory.capacityBytes.value, 16384);
  assert.strictEqual(first.memory.availableBytes.status, "unavailable");
  f.advance(); f.files.set("/cg/cpu.stat", "usage_usec 4000000");
  const [a, b] = yield* Effect.all([sampler.read, sampler.read], { concurrency: "unbounded" });
  assert.strictEqual(a, b);
  assert.strictEqual(a.cpu.usagePercent.value, 300);
  assert.strictEqual(a.cpu.usedPercent.value, 150);
  yield* Schema.decodeEffect(MachineResources)(a);
  const reads = f.reads();
  const shared = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  assert.strictEqual(shared, sampler);
  yield* shared.read;
  assert.strictEqual(f.reads(), reads);
}));

it.effect("rebaselines resets, boot/inode replacement, CPU hotplug, and nonpositive time", () => Effect.gen(function*() {
  const f = yield* fixture;
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  yield* sampler.read;
  for (const change of [
    () => f.files.set("/cg/cpu.stat", "usage_usec 1"),
    () => f.files.set("/proc/sys/kernel/random/boot_id", "different-boot"),
    () => f.replace(),
    () => f.files.set("/sys/devices/system/cpu/online", "0-6"),
  ]) {
    f.advance(); change();
    assert.strictEqual((yield* sampler.read).cpu.usagePercent.status, "unavailable");
  }
  f.advance(-10_000_000_000n);
  assert.strictEqual((yield* sampler.read).cpu.usagePercent.status, "unavailable");
}));

it.effect("maps mount roots and visible ancestors, unlimited limits, escaped paths and namespace-relative membership", () => Effect.gen(function*() {
  const f = yield* fixture;
  f.files.set("/proc/self/cgroup", "0::/parent/child");
  f.files.set("/proc/self/mountinfo", "1 0 0:28 /parent /cg\\040space ro - cgroup2 cgroup rw");
  for (const [file, value] of [["cpu.stat", "usage_usec 100"], ["cpu.max", "max 100000"], ["cpuset.cpus.effective", "0-3"], ["memory.current", "100"], ["memory.max", "max"]]) {
    f.files.set(`/cg space/child/${file}`, value ?? "");
  }
  f.files.set("/cg space/cpu.max", "50000 100000"); f.files.set("/cg space/memory.max", "1000");
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  const first = yield* sampler.read;
  assert.strictEqual(first.cpu.capacityCores.value, 0.5);
  assert.strictEqual(first.memory.capacityBytes.value, 1000);
  f.advance();
  f.files.set("/proc/self/cgroup", "0::/child");
  f.files.set("/cg space/child/cgroup.procs", "42\n43");
  assert.strictEqual((yield* sampler.read).memory.usedBytes.value, 100);
  f.advance(); f.files.set("/cg space/child/cgroup.procs", "99");
  const absent = yield* sampler.read;
  assert.strictEqual(absent.scope, "cgroup");
  assert.strictEqual(absent.scopeId, null);
  assert.strictEqual(absent.memory.usedBytes.status, "unavailable");
}));

it.effect("keeps missing/denied isolated counters unavailable, not host data", () => Effect.gen(function*() {
  for (const deny of [false, true]) {
    const f = yield* fixture;
    if (deny) f.denied.add("/cg/memory.current"); else f.files.delete("/cg/memory.current");
    f.files.set("/cg/cpu.stat", "usage_usec broken");
    const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
    const result = yield* sampler.read;
    assert.strictEqual(result.scope, "cgroup");
    assert.strictEqual(result.memory.usedBytes.status, "unavailable");
    assert.strictEqual(result.cpu.usagePercent.status, "unavailable");
    yield* Schema.decodeEffect(MachineResources)(result);
  }
}));

it.effect("collects split v1/hybrid controllers with nanosecond deltas and hierarchical limits", () => Effect.gen(function*() {
  const f = yield* fixture;
  f.files.set("/proc/self/cgroup", "0::/\n2:cpu,cpuacct:/group\n3:memory:/group\n4:cpuset:/group");
  f.files.set("/proc/self/mountinfo", "1 0 0:28 / /cg ro - cgroup2 cgroup rw\n2 0 0:29 /group /cpu rw - cgroup cgroup rw,cpu,cpuacct\n3 0 0:30 /group /mem rw - cgroup cgroup rw,memory\n4 0 0:31 /group /set rw - cgroup cgroup rw,cpuset");
  for (const [file, value] of [["/cpu/cpuacct.usage", "1000000000"], ["/cpu/cpu.cfs_quota_us", "-1"], ["/cpu/cpu.cfs_period_us", "100000"], ["/set/cpuset.effective_cpus", "1-2"], ["/mem/memory.usage_in_bytes", "500"], ["/mem/memory.limit_in_bytes", "9223372036854771712"]]) f.files.set(file ?? "", value ?? "");
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  const first = yield* sampler.read;
  assert.strictEqual(first.source, "linux-cgroup-hybrid");
  assert.strictEqual(first.cpu.capacityCores.value, 2);
  assert.strictEqual(first.memory.capacityBytes.value, 8192 * 1024);
  f.advance(); f.files.set("/cpu/cpuacct.usage", "1500000000");
  assert.strictEqual((yield* sampler.read).cpu.usagePercent.value, 50);
}));

it.effect("uses Linux execution ticks without double-counting guests or replacing MemAvailable by MemFree", () => Effect.gen(function*() {
  const f = yield* fixture;
  f.files.set("/proc/self/cgroup", ""); f.files.set("/proc/self/mountinfo", "1 0 0:1 / / rw - ext4 disk rw");
  f.files.set("/proc/stat", "cpu 10 0 10 50 10 10 0 10 100 0\ncpu0 0\ncpu1 0");
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  assert.strictEqual((yield* sampler.read).memory.usedBytes.value, 4096 * 1024);
  f.advance(); f.files.set("/proc/stat", "cpu 20 0 20 100 20 20 0 20 200 0\ncpu0 0\ncpu1 0");
  const result = yield* sampler.read;
  assert.strictEqual(result.cpu.usagePercent.value, 60);
  assert.strictEqual(result.cpu.usedPercent.value, 30);
  f.advance(); f.files.set("/proc/meminfo", "MemTotal: 8192 kB\nMemFree: 100 kB");
  assert.strictEqual((yield* sampler.read).memory.usedBytes.status, "unavailable");
}));

it.effect("distinguishes denied root limits from absent root files and rejects malformed cpusets", () => Effect.gen(function*() {
  const f = yield* fixture;
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  f.files.delete("/cg/cpu.max"); f.files.delete("/cg/memory.max");
  const unlimited = yield* sampler.read;
  assert.strictEqual(unlimited.cpu.capacityCores.value, 4);
  assert.strictEqual(unlimited.memory.capacityBytes.value, 8192 * 1024);
  f.advance(); f.denied.add("/cg/cpu.max"); f.denied.add("/cg/memory.max");
  const denied = yield* sampler.read;
  assert.strictEqual(denied.cpu.capacityCores.status, "unavailable");
  assert.strictEqual(denied.memory.capacityBytes.status, "unavailable");
  assert.strictEqual(denied.memory.usedBytes.value, 4096);
  f.advance(); f.denied.clear(); f.files.set("/cg/cpuset.cpus.effective", "3-1");
  assert.strictEqual((yield* sampler.read).cpu.capacityCores.status, "unavailable");
}));

it.effect("applies v1 ancestor memory limits only with hierarchical accounting enabled", () => Effect.gen(function*() {
  const f = yield* fixture;
  f.files.set("/proc/self/cgroup", "2:cpu,cpuacct:/child\n3:memory:/child\n4:cpuset:/child");
  f.files.set("/proc/self/mountinfo", "2 0 0:29 / /cpu rw - cgroup cgroup rw,cpu,cpuacct\n3 0 0:30 / /mem rw - cgroup cgroup rw,memory\n4 0 0:31 / /set rw - cgroup cgroup rw,cpuset");
  for (const directory of ["/cpu", "/cpu/child"]) {
    f.files.set(`${directory}/cpu.cfs_quota_us`, "-1"); f.files.set(`${directory}/cpu.cfs_period_us`, "100000");
  }
  f.files.set("/cpu/child/cpuacct.usage", "1000000000");
  f.files.set("/set/child/cpuset.cpus", ""); f.files.set("/set/cpuset.cpus", "0-1");
  f.files.set("/mem/child/memory.usage_in_bytes", "100");
  f.files.set("/mem/child/memory.limit_in_bytes", "1000");
  f.files.set("/mem/memory.limit_in_bytes", "500"); f.files.set("/mem/memory.use_hierarchy", "1");
  const sampler = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  const hierarchical = yield* sampler.read;
  assert.strictEqual(hierarchical.memory.capacityBytes.value, 500);
  assert.strictEqual(hierarchical.cpu.capacityCores.value, 2);
  f.advance(); f.files.set("/mem/memory.use_hierarchy", "0");
  assert.strictEqual((yield* sampler.read).memory.capacityBytes.value, 1000);
}));

it.effect("samples target PID mount roots, isolates baselines, and refuses missing or reused target identities", () => Effect.gen(function*() {
  const f = yield* fixture;
  const targetStat = (birth: number) => `99 (target with ) paren) ${Array.from({ length: 22 }, (_, index) => index === 19 ? String(birth) : "0").join(" ")}`;
  f.files.set("/proc/99/stat", targetStat(100));
  f.files.set("/proc/99/cgroup", "0::/");
  f.files.set("/proc/99/mountinfo", "1 0 0:28 / /cg ro - cgroup2 cgroup rw");
  for (const [file, value] of [...f.files]) if (file.startsWith("/cg/")) f.files.set(`/proc/99/root${file}`, value);
  f.files.set("/proc/99/root/cg/memory.current", "777");
  const self = yield* f.make.pipe(Effect.provide(NodeServices.layer));
  const target = yield* f.makeTarget(99).pipe(Effect.provide(NodeServices.layer));
  assert.strictEqual((yield* self.read).memory.usedBytes.value, 4096);
  assert.strictEqual((yield* target.read).memory.usedBytes.value, 777);
  f.advance(); f.files.set("/proc/99/root/cg/cpu.stat", "usage_usec 1500000");
  assert.strictEqual((yield* target.read).cpu.usagePercent.value, 50);
  // A reused PID in the same cgroup must not inherit a previous process's interval.
  f.files.set("/proc/99/stat", targetStat(200));
  assert.strictEqual((yield* target.read).cpu.usagePercent.status, "unavailable");
  f.files.delete("/proc/99/stat");
  const vanished = yield* target.read;
  assert.strictEqual(vanished.scope, "unknown");
  assert.strictEqual(vanished.scopeId, null);
  assert.strictEqual(vanished.memory.usedBytes.status, "unavailable");
}));

it.effect("uses real Darwin VM page sizes and compressor occupancy without speculative double-counting", () => Effect.gen(function*() {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const clock = yield* Clock.Clock;
  let time = 1_000_000_000n, cpu = 100;
  let vm = "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 50.\nPages active: 100.\nPages inactive: 200.\nPages speculative: 30.\nPages wired down: 10.\nPages stored in compressor: 900.\nPages occupied by compressor: 20.";
  const nativeSpawner: typeof spawner = { ...spawner, spawn: (command) => Effect.gen(function*() {
    if (!ChildProcess.isStandardCommand(command)) return yield* Effect.die("Shell invocation forbidden");
    assert.ok(["/usr/sbin/sysctl", "/usr/bin/vm_stat"].includes(command.command));
    const output = command.command === "/usr/bin/vm_stat" ? vm : command.args.includes("hw.memsize") ? "34359738368" : "{ sec = 123, usec = 4 }";
    const child = yield* spawner.spawn(ChildProcess.make("/usr/bin/true", [], { stdin: "ignore" }));
    return { ...child, stdout: Stream.make(new TextEncoder().encode(output)) };
  }) };
  const sampler = yield* makeMachineSampler().pipe(
    Effect.provideService(Host, { ...host, platform: "darwin", cpuTimes: Effect.sync(() => [{ user: cpu, nice: 0, sys: 0, idle: 100, irq: 0 }]) }),
    Effect.provideService(Clock.Clock, { ...clock, monotonicTimeNanos: Effect.sync(() => time) }),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, nativeSpawner),
  );
  const first = yield* sampler.read;
  assert.strictEqual(first.memory.usedBytes.value, 330 * 16384);
  assert.strictEqual(first.memory.capacityBytes.value, 34359738368);
  assert.strictEqual(first.memory.availableBytes.status, "unavailable");
  time += 1_000_000_000n; cpu += 500;
  assert.strictEqual((yield* sampler.read).cpu.usagePercent.value, 50);
  time += 1_000_000_000n; vm = vm.replace("Pages occupied by compressor: 20.", "Pages occupied by compressor: malformed.");
  const partial = yield* sampler.read;
  assert.strictEqual(partial.memory.usedBytes.status, "unavailable");
  assert.strictEqual(partial.memory.capacityBytes.value, 34359738368);
  yield* Schema.decodeEffect(MachineResources)(partial);
}).pipe(Effect.provide(NodeServices.layer)));
