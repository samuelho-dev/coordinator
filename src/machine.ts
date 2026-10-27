import { Clock, Crypto, Effect, FileSystem, Option, Path, Semaphore, Stream } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { metric, unavailableMetric, type MachineResources, type MetricProvenance, type MetricUnit } from "./inspection.ts";
import { Host } from "./platform.ts";

type Boundary = { readonly directory: string; readonly mount: string; readonly identity: string | null };
type Native = {
  scope: MachineResources["scope"]; source: string; identity: string | null;
  usage: number | null; total: number | null; capacity: number | null;
  topology: string; used: number | null; memory: number | null; available: number | null;
  cpuDefinition: string; memoryDefinition: string; partial: boolean;
  cpuAt: bigint | null;
};
type Baseline = { identity: string; topology: string; usage: number; total: number | null; at: bigint; wall: number };
const number = (text: string | null): number | null => {
  if (text === null || !/^\d+(?:\.\d+)?$/.test(text.trim())) return null;
  const value = Number(text.trim());
  return Number.isFinite(value) && value >= 0 ? value : null;
};
const integer = (text: string | null): number | null => {
  const value = number(text);
  return value !== null && Number.isSafeInteger(value) ? value : null;
};
const fields = (text: string | null) => new Map((text ?? "").split("\n").map((line) => {
  const [key = "", value = ""] = line.trim().split(/[:\s]+/);
  return [key, integer(value)] as const;
}));
const minimum = (values: ReadonlyArray<number | null>): number | null => {
  const present = values.filter((value): value is number => value !== null);
  return present.length ? Math.min(...present) : null;
};
const cpuSet = (text: string | null): number | null => {
  if (!text?.trim()) return null;
  let count = 0, previous = -1;
  for (const part of text.trim().split(",")) {
    const match = /^(\d+)(?:-(\d+))?$/.exec(part);
    if (!match) return null;
    const first = integer(match[1] ?? null), last = integer(match[2] ?? match[1] ?? null);
    if (first === null || last === null || first <= previous || last < first) return null;
    count += last - first + 1;
    previous = last;
  }
  return count > 0 && Number.isSafeInteger(count) ? count : null;
};
const blank = (source: string): Native => ({
  scope: "unknown", source, identity: null, usage: null, total: null, capacity: null, topology: "unknown",
  used: null, memory: null, available: null, cpuDefinition: "Native CPU counters unavailable",
  memoryDefinition: "Native memory counters unavailable", partial: true, cpuAt: null,
});
const unescapeMount = (text: string) => text.replace(/\\(040|011|012|134)/g, (_, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));
// Sharing follows injected native context, never a global hostname or installation identity.
export interface MachineSampler { readonly read: Effect.Effect<MachineResources> }
const samplers = new WeakMap<FileSystem.FileSystem, WeakMap<object, WeakMap<object, MachineSampler>>>();

export const makeMachineSampler = (pid?: number) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const host = yield* Host;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const clock = yield* Clock.Clock;
  let hosts = samplers.get(fs);
  if (!hosts) { hosts = new WeakMap(); samplers.set(fs, hosts); }
  let clocks = hosts.get(host);
  if (!clocks) { clocks = new WeakMap(); hosts.set(host, clocks); }
  const existing = pid === undefined ? clocks.get(clock) : undefined;
  if (existing) return existing;
  const lock = yield* Semaphore.make(1);
  let baseline: Baseline | null = null;
  let latest: { at: bigint; value: MachineResources } | null = null;
  const targetPid = pid ?? host.pid;
  const validTarget = Number.isSafeInteger(targetPid) && targetPid > 0;
  const proc = pid === undefined ? "/proc/self" : `/proc/${targetPid}`;
  let latestBirth: string | null = null;
  const readFile = (file: string) => fs.readFileString(file).pipe(
    Effect.map((text) => text.length <= 1024 * 1024 ? text.trim() : null), Effect.catch(() => Effect.succeed(null)),
  );
  const readLimit = (file: string, root: boolean) => fs.readFileString(file).pipe(
    Effect.map((text) => text.length <= 1024 ? text.trim() : null),
    Effect.catch((error) => Effect.succeed(root && error.reason._tag === "NotFound" ? "max" : null)),
  );
  const command = (executable: string, args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
    const child = yield* spawner.spawn(ChildProcess.make(executable, args, {
      env: { LC_ALL: "C", LANG: "C", PATH: "/usr/bin:/bin:/usr/sbin:/sbin" }, extendEnv: false,
      stdin: "ignore", forceKillAfter: "100 millis",
    }));
    yield* Effect.forkScoped(Stream.runDrain(child.stderr));
    let bytes = 0;
    const text = yield* child.stdout.pipe(Stream.map((chunk) => { bytes += chunk.length; return chunk; }),
      Stream.takeUntil(() => bytes > 65536), Stream.decodeText(), Stream.runFold(() => "", (a, b) => a + b));
    if (bytes > 65536 || (yield* child.exitCode) !== 0) return null;
    return text.trim();
  })).pipe(Effect.timeout("2 seconds"), Effect.catch(() => Effect.succeed(null)));
  const hash = (identity: string) => crypto.digest("SHA-256", new TextEncoder().encode(identity)).pipe(
    Effect.map((digest) => `machine:${Hex.encode(digest)}`), Effect.catch(() => Effect.succeed(null)),
  );
  const ancestors = (boundary: Boundary) => {
    const result: string[] = [];
    let current = boundary.directory;
    while (result.length < 128) {
      result.push(current);
      if (current === boundary.mount) return result;
      const parent = path.dirname(current);
      if (parent === current) break;
      current = parent;
    }
    return null;
  };
  const linux = Effect.gen(function*() {
    const membership = yield* readFile(`${proc}/cgroup`);
    const mounts = yield* readFile(`${proc}/mountinfo`);
    const boot = yield* readFile("/proc/sys/kernel/random/boot_id");
    if (membership === null || mounts === null) return blank("linux-scope-unavailable");
    const groups = membership.split("\n").filter(Boolean).map((line) => /^(\d+):([^:]*):(\/.*)$/.exec(line));
    if (groups.some((group) => group === null)) return blank("linux-scope-unavailable");
    const mountRows = mounts.split("\n").flatMap((line) => {
      const [before, after] = line.split(" - ");
      const left = before?.split(" "), right = after?.split(" ");
      if (!left || !right || (right[0] !== "cgroup" && right[0] !== "cgroup2")) return [];
      const root = unescapeMount(left[3] ?? ""), mounted = unescapeMount(left[4] ?? "");
      const mount = pid === undefined ? mounted : path.join(proc, "root", mounted);
      return root.startsWith("/") && mounted.startsWith("/") ? [{ root, mount, device: left[2], type: right[0], controllers: (right[2] ?? "").split(",") }] : [];
    });
    const resolve = (controller: string, v2: boolean) => Effect.gen(function*() {
      const group = groups.find((row) => row && (v2 ? row[1] === "0" && row[2] === "" : row[2]?.split(",").includes(controller)));
      const member = group?.[3];
      if (!member || member.split("/").some((part) => part === ".." || part === ".")) return null;
      for (const row of mountRows.filter((row) => v2 ? row.type === "cgroup2" : row.type === "cgroup" && row.controllers.includes(controller)).sort((a, b) => b.root.length - a.root.length)) {
        const rooted = row.root === "/" || member === row.root || member.startsWith(`${row.root}/`);
        const relative = rooted ? member.slice(row.root === "/" ? 1 : row.root.length) : member;
        const directory = path.resolve(row.mount, `.${relative.startsWith("/") ? relative : `/${relative}`}`);
        if (directory !== row.mount && !directory.startsWith(`${row.mount}/`)) continue;
        // Nonmatching mount roots may be expressed outside our cgroup namespace. Prove membership before using them.
        if (!rooted) {
          const procs = yield* readFile(path.join(directory, "cgroup.procs"));
          if (!procs?.split(/\s+/).includes(String(targetPid))) continue;
        }
        const info = yield* fs.stat(directory).pipe(Effect.option);
        const inode = Option.isSome(info) ? Option.getOrNull(info.value.ino) : null;
        return { directory, mount: row.mount, identity: inode === null ? null : `${row.device}:${row.root}:${member}:${inode}` };
      }
      return null;
    });
    const v2 = groups.some((row) => row?.[1] === "0" && row[2] === "");
    if (groups.length > 0) {
      const hasV1 = (controller: string) => groups.some((row) => row?.[2]?.split(",").includes(controller));
      const cpuV2 = v2 && !hasV1("cpuacct"), memoryV2 = v2 && !hasV1("memory"), setV2 = v2 && !hasV1("cpuset");
      const cpu = yield* resolve(cpuV2 ? "" : "cpuacct", cpuV2);
      const quota = cpuV2 ? cpu : yield* resolve("cpu", false);
      const memory = memoryV2 ? yield* resolve("", true) : yield* resolve("memory", false);
      const cpuset = yield* resolve(setV2 ? "" : "cpuset", setV2);
      const result = blank(v2 && groups.some((row) => row?.[2]) ? "linux-cgroup-hybrid" : v2 ? "linux-cgroup-v2" : "linux-cgroup-v1");
      result.scope = "cgroup";
      result.cpuDefinition = "Cgroup cumulative execution / monotonic elapsed; visible ancestor quota/cpuset bounds, not guaranteed allocation; hidden ancestors unknown";
      result.memoryDefinition = memoryV2 ? "memory.current accounted charges including descendants/cache/kernel; visible ancestor memory.max hard bound; headroom is not available memory" : "memory.usage_in_bytes fuzzy accounted charges; visible hierarchical hard limits; headroom is not available memory";
      const namespace = yield* fs.readLink(`${proc}/ns/cgroup`).pipe(Effect.catch(() => Effect.succeed(null)));
      result.identity = boot && namespace && cpu?.identity && memory?.identity ? `${boot}:${namespace}:${cpu.identity}:${memory.identity}:${quota?.identity}:${cpuset?.identity}` : null;
      result.usage = cpu ? cpuV2 ? fields(yield* readFile(path.join(cpu.directory, "cpu.stat"))).get("usage_usec") ?? null : number(yield* readFile(path.join(cpu.directory, "cpuacct.usage"))) : null;
      if (result.usage !== null) result.usage *= cpuV2 ? 1000 : 1;
      result.cpuAt = yield* clock.monotonicTimeNanos;
      const capacities: Array<number | null> = [];
      let limitUnknown = quota === null;
      if (quota) {
        const chain = ancestors(quota);
        if (chain === null) limitUnknown = true;
        for (const directory of chain ?? []) {
          if (cpuV2) {
            // Only ENOENT at a hierarchy root means no root quota; denied is unknown.
            const text = yield* readLimit(path.join(directory, "cpu.max"), directory === quota.mount);
            if (text === "max") continue;
            if (text === null) { limitUnknown = true; continue; }
            const [q, p] = text.split(/\s+/), period = number(p ?? null), value = number(q ?? null);
            if (period === null || period <= 0 || (q !== "max" && value === null)) limitUnknown = true;
            else if (value !== null) capacities.push(value / period);
          } else {
            const q = yield* readFile(path.join(directory, "cpu.cfs_quota_us"));
            const period = number(yield* readFile(path.join(directory, "cpu.cfs_period_us")));
            const value = number(q);
            if (period === null || period <= 0 || (q !== "-1" && value === null)) limitUnknown = true;
            else if (value !== null) capacities.push(value / period);
          }
        }
      }
      const set = cpuset ? yield* fs.readFileString(path.join(cpuset.directory, setV2 ? "cpuset.cpus.effective" : "cpuset.effective_cpus")).pipe(
        Effect.map((text) => text.length <= 65536 ? text.trim() : null),
        Effect.catch((error) => Effect.succeed(!setV2 && error.reason._tag === "NotFound" ? "" : null)),
      ) : null;
      let setCount = cpuSet(set);
      let effectiveSet = set;
      if (!setV2 && set === "" && cpuset) {
        // Older v1 kernels inherit an empty cpuset.cpus from visible ancestors.
        for (const directory of ancestors(cpuset) ?? []) {
          const inherited = yield* readFile(path.join(directory, "cpuset.cpus"));
          if (inherited === null) break;
          if (inherited === "") continue;
          setCount = cpuSet(inherited);
          effectiveSet = inherited;
          break;
        }
      }
      const online = yield* readFile("/sys/devices/system/cpu/online");
      capacities.push(setCount, cpuSet(online));
      result.capacity = limitUnknown || (cpuset !== null && setCount === null) ? null : minimum(capacities);
      result.topology = `${online}:${effectiveSet}:${result.capacity}`;
      if (memory) {
        result.used = integer(yield* readFile(path.join(memory.directory, memoryV2 ? "memory.current" : "memory.usage_in_bytes")));
        const limits: Array<number | null> = [];
        let unknown = false;
        const chain = ancestors(memory);
        if (!chain) unknown = true;
        for (const directory of chain ?? []) {
          if (!memoryV2 && directory !== memory.directory) {
            const hierarchy = yield* readFile(path.join(directory, "memory.use_hierarchy"));
            if (hierarchy === "0") continue;
            if (hierarchy !== "1") { unknown = true; continue; }
          }
          const text = memoryV2
            ? yield* readLimit(path.join(directory, "memory.max"), directory === memory.mount)
            : yield* readFile(path.join(directory, "memory.limit_in_bytes"));
          if (memoryV2 && text === "max") continue;
          const value = number(text);
          // v1 PAGE_COUNTER_MAX is a page-aligned LONG_MAX sentinel, not real RAM.
          if (!memoryV2 && value !== null && value >= 0x7fffffff00000000) continue;
          if (value === null || value <= 0) unknown = true;
          else limits.push(value);
        }
        if (!memoryV2 && chain && chain.length > 1) {
          const hierarchical = fields(yield* readFile(path.join(memory.directory, "memory.stat"))).get("hierarchical_memory_limit") ?? null;
          if (hierarchical !== null && hierarchical < 0x7fffffff00000000) limits.push(hierarchical);
        }
        const physical = fields(yield* readFile("/proc/meminfo")).get("MemTotal");
        if (physical !== null && physical !== undefined) limits.push(physical * 1024);
        result.memory = unknown ? null : minimum(limits);
      }
      if ((yield* readFile(`${proc}/cgroup`)) !== membership || (yield* readFile("/proc/sys/kernel/random/boot_id")) !== boot) return blank("linux-scope-changed");
      result.partial = result.identity === null || result.usage === null || result.capacity === null || result.used === null || result.memory === null;
      return result;
    }
    // No membership is the only unambiguous no-cgroup path. An unresolved membership never widens to host scope.
    if (mountRows.length > 0) return blank("linux-scope-unavailable");
    const result = blank("linux-proc");
    result.scope = "host"; result.identity = boot ? `linux:${boot}` : null;
    const stat = yield* readFile("/proc/stat");
    const values = stat?.split("\n").find((line) => line.startsWith("cpu "))?.trim().split(/\s+/).slice(1, 9).map((value) => integer(value));
    const count = stat?.split("\n").filter((line) => /^cpu\d+\s/.test(line)).length ?? 0;
    if (values?.length === 8 && values.every((value) => value !== null)) {
      result.usage = values.reduce((sum, value, index) => sum + ([0, 1, 2, 5, 6].includes(index) ? value : 0), 0);
      result.total = values.reduce((sum, value) => sum + value, 0);
    }
    result.cpuAt = yield* clock.monotonicTimeNanos;
    result.capacity = count > 0 ? count : null;
    result.topology = `${stat?.split("\n").filter((line) => /^cpu\d+\s/.test(line)).map((line) => line.split(/\s+/)[0]).join(",")}:${yield* readFile("/sys/devices/system/cpu/online")}`;
    const mem = fields(yield* readFile("/proc/meminfo"));
    result.memory = mem.get("MemTotal") === undefined || mem.get("MemTotal") === null ? null : (mem.get("MemTotal") ?? 0) * 1024;
    result.available = mem.get("MemAvailable") === undefined || mem.get("MemAvailable") === null ? null : (mem.get("MemAvailable") ?? 0) * 1024;
    if (result.memory !== null && result.available !== null && result.available <= result.memory) result.used = result.memory - result.available;
    else result.available = null;
    result.cpuDefinition = "Execution user+nice+system+irq+softirq / all non-guest CPU ticks; excludes iowait/steal execution; guest not counted twice";
    result.memoryDefinition = "MemTotal minus MemAvailable; available is kernel MemAvailable, never MemFree";
    result.partial = result.usage === null || result.used === null || result.identity === null;
    return result;
  });
  const darwin = Effect.gen(function*() {
    const result = blank("darwin"); result.scope = "host";
    const boot = yield* command("/usr/sbin/sysctl", ["-n", "kern.boottime"]);
    result.identity = boot ? `darwin:${host.hostname}:${boot}` : null;
    const cpus = host.cpuTimes ? yield* host.cpuTimes.pipe(Effect.catch(() => Effect.succeed(null))) : null;
    result.cpuAt = yield* clock.monotonicTimeNanos;
    if (cpus?.length && cpus.every((cpu) => Object.values(cpu).every((value) => Number.isFinite(value) && value >= 0))) {
      result.usage = cpus.reduce((sum, cpu) => sum + cpu.user + cpu.nice + cpu.sys + cpu.irq, 0) * 1e6;
      result.capacity = cpus.length;
    }
    result.topology = String(cpus?.length ?? 0);
    result.memory = integer(yield* command("/usr/sbin/sysctl", ["-n", "hw.memsize"]));
    const vm = yield* command("/usr/bin/vm_stat", []);
    const page = integer(/page size of (\d+) bytes/.exec(vm ?? "")?.[1] ?? null);
    const counter = (name: string) => integer(new RegExp(`^${name}:\\s+(\\d+)\\.?$`, "m").exec(vm ?? "")?.[1] ?? null);
    const active = counter("Pages active"), inactive = counter("Pages inactive"), wired = counter("Pages wired down"), compressed = counter("Pages occupied by compressor");
    if (page !== null && page > 0 && active !== null && inactive !== null && wired !== null && compressed !== null) {
      const used = (active + inactive + wired + compressed) * page;
      if (Number.isSafeInteger(used)) result.used = used;
    }
    result.cpuDefinition = "Native aggregate user+nice+system+irq execution / monotonic elapsed time";
    result.memoryDefinition = "vm_stat (active + inactive + wired + compressor-occupied) pages * actual page size; excludes free/speculative; includes inactive cache; not Activity Monitor memory pressure; available not defined";
    result.partial = result.identity === null || result.usage === null || result.used === null || result.memory === null;
    return result;
  });
  const targetBirth = pid === undefined ? Effect.succeed("self") : host.platform === "linux"
    ? readFile(`${proc}/stat`).pipe(Effect.map((text) => {
      if (!text) return null;
      const end = text.lastIndexOf(")");
      const birth = text.slice(end + 1).trim().split(/\s+/)[19];
      return end > 0 && birth && /^\d+$/.test(birth) ? birth : null;
    }))
    : host.platform === "darwin" ? command("/bin/ps", ["-p", String(targetPid), "-o", "lstart="]) : Effect.succeed(null);
  const read = lock.withPermit(Effect.gen(function*() {
    const birth = validTarget ? yield* targetBirth : null;
    const now = yield* clock.monotonicTimeNanos;
    // ponytail: share one native observation for simultaneous publishers, not a new timer or daemon.
    if (birth !== null && birth === latestBirth && latest && now >= latest.at && now - latest.at < 100_000_000n) return latest.value;
    let native = birth === null ? blank("target-identity-unavailable") : yield* (host.platform === "linux" ? linux : host.platform === "darwin" ? darwin : Effect.succeed(blank("unsupported-platform")));
    if (pid !== undefined && birth !== (yield* targetBirth)) native = blank("target-identity-changed");
    native.topology += `:target:${birth}`;
    latestBirth = birth;
    const at = native.cpuAt ?? (yield* clock.monotonicTimeNanos);
    const observedAt = yield* clock.currentTimeMillis;
    const scopeId = native.identity === null ? null : yield* hash(native.identity);
    let usage: number | null = null;
    const previous = baseline;
    if (scopeId !== null && native.usage !== null && previous && previous.identity === scopeId && previous.topology === native.topology && at > previous.at && native.usage >= previous.usage) {
      const elapsed = Number(at - previous.at);
      if (native.total !== null && previous.total !== null && native.total > previous.total && native.capacity !== null && native.usage - previous.usage <= native.total - previous.total) {
        usage = (native.usage - previous.usage) / (native.total - previous.total) * native.capacity * 100;
      } else if (native.total === null && previous.total === null) usage = (native.usage - previous.usage) / elapsed * 100;
    }
    baseline = scopeId !== null && native.usage !== null ? { identity: scopeId, topology: native.topology, usage: native.usage, total: native.total, at, wall: observedAt } : null;
    const provenance = (description: string, derived = false, window = false): MetricProvenance => ({
      source: native.source, scope: native.scope, scopeDescription: description, observedAt, nativeUpdatedAt: observedAt,
      estimated: false, derived, shared: true, partial: native.partial, stale: false,
      ...(window && previous && observedAt >= previous.wall ? { sampleWindow: { startedAt: previous.wall, endedAt: observedAt } } : {}),
    });
    const value = <U extends MetricUnit>(unit: U, amount: number | null, description: string, derived = false, window = false) => amount !== null && Number.isFinite(amount) && amount >= 0 && (unit !== "bytes" || Number.isSafeInteger(amount))
      ? metric(unit, amount, provenance(description, derived, window))
      : unavailableMetric(unit, window ? "CPU interval warming up, reset, topology changed, or native source unavailable" : "Native measurement unavailable or no defined value for this scope", provenance(description, derived, window));
    const result: MachineResources = {
      scope: native.scope, scopeId, source: native.source, observedAt,
      cpu: {
        usagePercent: value("percent-one-core", usage, native.cpuDefinition, true, true),
        capacityCores: value("cores", native.capacity, "Visible logical CPU/cpuset/quota bound; shared ancestor budgets and hidden namespace ancestors are not guaranteed capacity"),
        usedPercent: value("percent", usage !== null && native.capacity !== null && native.capacity > 0 ? usage / native.capacity : null, native.cpuDefinition, true, true),
      },
      memory: {
        usedBytes: value("bytes", native.used, native.memoryDefinition, host.platform !== "linux" || native.scope === "host"),
        capacityBytes: value("bytes", native.memory, native.memoryDefinition),
        availableBytes: value("bytes", native.available, native.scope === "host" && host.platform === "linux" ? "Kernel MemAvailable" : "No native allocatable/available-memory definition; limit minus usage is not available memory"),
        usedPercent: value("percent", native.used !== null && native.memory !== null && native.memory > 0 ? native.used / native.memory * 100 : null, native.memoryDefinition, true),
      },
    };
    latest = { at: yield* clock.monotonicTimeNanos, value: result };
    return result;
  }));
  const sampler = { read };
  if (pid === undefined) clocks.set(clock, sampler);
  return sampler;
});
