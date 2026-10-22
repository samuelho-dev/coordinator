import { Clock, Data, Effect, FileSystem, Option, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Host } from "./platform.ts";

export interface ResourceRoot {
  readonly pid: number;
  readonly startTime?: string | undefined;
  readonly ownership: "exclusive" | "shared" | "unknown";
}
export interface ResourceProcess {
  readonly pid: number;
  readonly startTime: string | null;
  readonly ppid: number | null;
  readonly ownership: ResourceRoot["ownership"];
  readonly cpuPercent: number | null;
  readonly cpuTimeMs: number | null;
  readonly rssBytes: number | null;
  readonly sampleStartedAt: number | null;
  readonly sampledAt: number | null;
  readonly windowMs: number | null;
  readonly unavailableReason: string | null;
}
export interface ResourceObservation {
  readonly processes: ReadonlyArray<ResourceProcess>;
  readonly sampleStartedAt: number;
  readonly sampledAt: number;
  readonly windowMs: number;
  readonly ownedProcessCount: number;
  readonly ownedCpuPercent: number | null;
  /** Sum of resident sets, not unique physical RAM (shared pages can occur in multiple sets). */
  readonly ownedRssBytes: number | null;
  readonly sharedProcessCount: number;
  readonly unknownProcessCount: number;
  readonly partial: boolean;
  readonly truncated: boolean;
  readonly unavailableReason: string | null;
}
export class ResourceError extends Data.TaggedError("ResourceError")<{
  readonly operation: string;
  readonly cause?: unknown;
}> {}

const maxProcesses = 4096;
const maxOutputBytes = 1024 * 1024;
const integer = Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))));
const decodeInteger = (text: string) => Schema.decodeEffect(integer)(text).pipe(
  Effect.mapError((cause) => new ResourceError({ operation: "parse native process integer", cause })),
);
interface Counter {
  readonly pid: number;
  readonly ppid: number;
  readonly startTime: string;
  readonly cpuTimeMs: number;
  readonly rssBytes: number;
  readonly observedAt: number;
  readonly monotonicAt: bigint;
}
interface Snapshot {
  readonly counters: Map<number, Counter>;
  readonly unavailable: Map<number, string>;
  readonly truncated: boolean;
}
const command = (executable: string, args: ReadonlyArray<string>, allowNoMatch = false) => Effect.scoped(Effect.gen(function*() {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(ChildProcess.make(executable, args, {
    env: { LC_ALL: "C", LANG: "C", TZ: "UTC", PATH: "/usr/bin:/bin" },
    extendEnv: false, stdin: "ignore", forceKillAfter: "100 millis",
  }));
  yield* Effect.forkScoped(Stream.runDrain(child.stderr));
  let bytes = 0;
  let truncated = false;
  const text = yield* child.stdout.pipe(
    Stream.map((chunk) => {
      const remaining = maxOutputBytes - bytes;
      bytes += chunk.length;
      truncated = bytes > maxOutputBytes;
      return chunk.subarray(0, remaining);
    }),
    Stream.takeUntil(() => truncated),
    Stream.decodeText(),
    Stream.runFold(() => "", (text, chunk) => text + chunk),
  );
  if (!truncated) {
    const exitCode = yield* child.exitCode;
    if (exitCode !== 0 && !(allowNoMatch && exitCode === 1 && text.trim() === "")) return yield* new ResourceError({ operation: `execute ${executable}` });
  }
  return { text, truncated };
})).pipe(
  Effect.timeout("5 seconds"),
  Effect.mapError((cause) => new ResourceError({ operation: `execute ${executable}`, cause })),
);

const expectedProcErrno = Schema.decodeUnknownOption(Schema.Struct({ code: Schema.Literals(["EPERM", "ESRCH"]) }));

const linuxFields = (pid: number) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs.readFileString(`/proc/${pid}/stat`).pipe(
    Effect.catch((error) => error.reason._tag === "NotFound" || error.reason._tag === "PermissionDenied"
      || (error.reason._tag === "Unknown" && Option.isSome(expectedProcErrno(error.reason.cause)))
      ? Effect.succeed(null)
      : Effect.fail(new ResourceError({ operation: "read process counters", cause: error }))),
  );
  if (text === null) return null;
  // comm may contain whitespace and closing parentheses; the final ')' delimits it.
  const end = text.lastIndexOf(")");
  const fields = text.slice(end + 1).trim().split(/\s+/);
  if (end < 0 || fields.length < 22) return yield* new ResourceError({ operation: "parse /proc stat" });
  return fields;
});

const linuxBootId = () => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const bootId = (yield* fs.readFileString("/proc/sys/kernel/random/boot_id").pipe(
    Effect.mapError((cause) => new ResourceError({ operation: "read Linux boot identity", cause })),
  )).trim();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(bootId)) {
    return yield* new ResourceError({ operation: "parse Linux boot identity" });
  }
  return bootId;
});

const linuxCounter = (pid: number, hz: number, pageSize: number, bootId: string) => Effect.gen(function*() {
  const fields = yield* linuxFields(pid);
  if (fields === null) return null;
  const ppid = yield* decodeInteger(fields.at(1) ?? "");
  const user = yield* decodeInteger(fields.at(11) ?? "");
  const system = yield* decodeInteger(fields.at(12) ?? "");
  const birth = yield* decodeInteger(fields.at(19) ?? "");
  const rss = yield* decodeInteger(fields.at(21) ?? "");
  return { pid, ppid, startTime: `linux:${bootId}:${birth}`, cpuTimeMs: (user + system) * 1000 / hz, rssBytes: rss * pageSize, observedAt: yield* Clock.currentTimeMillis, monotonicAt: yield* Clock.monotonicTimeNanos };
});

export const parseDarwinSnapshot = (output: { readonly text: string; readonly truncated: boolean }) => Effect.gen(function*() {
  const counters = new Map<number, Counter>();
  const unavailable = new Map<number, string>();
  const lines = output.text.split("\n");
  // A bounded pipe can end mid-row. Never interpret that fragment as a process.
  if (output.truncated) lines.pop();
  let truncated = output.truncated;
  for (const line of lines) {
    if (!line.trim()) continue;
    if (counters.size === maxProcesses) { truncated = true; break; }
    const row = /^\s*(\d+)\s+(\d+)\s+([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(?:(\d+)-)?(?:(\d+):)?(\d+):(\d{2})(?:\.(\d{1,2}))?\s+(\d+)\s*$/.exec(line);
    if (!row) return yield* new ResourceError({ operation: "parse Darwin ps counters" });
    const pid = yield* decodeInteger(row.at(1) ?? "");
    const ppid = yield* decodeInteger(row.at(2) ?? "");
    const days = yield* decodeInteger(row.at(4) ?? "0");
    const hours = yield* decodeInteger(row.at(5) ?? "0");
    const minutes = yield* decodeInteger(row.at(6) ?? "");
    const seconds = yield* decodeInteger(row.at(7) ?? "");
    const fraction = yield* decodeInteger((row.at(8) ?? "0").padEnd(2, "0"));
    const rss = yield* decodeInteger(row.at(9) ?? "");
    const cpuTimeMs = (((days * 24 + hours) * 60 + minutes) * 60 + seconds) * 1000 + fraction * 10;
    // ps has no task-read validity flag: simultaneous zeroes may be uninitialized counters.
    if (cpuTimeMs === 0 && rss === 0) unavailable.set(pid, "task-counters-unreadable-or-zero");
    counters.set(pid, { pid, ppid, startTime: `darwin:${row.at(3)?.replace(/\s+/g, " ")}`, cpuTimeMs, rssBytes: rss * 1024, observedAt: yield* Clock.currentTimeMillis, monotonicAt: yield* Clock.monotonicTimeNanos });
  }
  return { counters, unavailable, truncated };
});
const darwinSnapshot = command("/bin/ps", ["-axo", "pid=,ppid=,lstart=,time=,rss="]).pipe(Effect.flatMap(parseDarwinSnapshot));
export type ProcessIdentification =
  | { readonly status: "available"; readonly pid: number; readonly ppid: number; readonly startTime: string }
  | { readonly status: "unavailable"; readonly reason: string };

/** Capture native identity for presence without measuring CPU or RSS. */
export const identifyProcess = (pid: number): Effect.Effect<ProcessIdentification, ResourceError, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner | Host> => Effect.gen(function*() {
  if (!Number.isSafeInteger(pid) || pid <= 0) return yield* new ResourceError({ operation: "invalid process identity PID" });
  const host = yield* Host;
  if (host.platform === "linux") {
    const fields = yield* linuxFields(pid);
    if (fields === null) return { status: "unavailable", reason: "process-exited-or-permission-denied" };
    const ppid = yield* decodeInteger(fields.at(1) ?? "");
    const birth = yield* decodeInteger(fields.at(19) ?? "");
    const bootId = yield* linuxBootId();
    return { status: "available", pid, ppid, startTime: `linux:${bootId}:${birth}` };
  }
  if (host.platform !== "darwin") return { status: "unavailable", reason: "unsupported-platform" };
  const output = yield* command("/bin/ps", ["-p", `${pid}`, "-o", "pid=,ppid=,lstart="], true);
  if (output.truncated) return { status: "unavailable", reason: "identity-output-truncated" };
  if (!output.text.trim()) return { status: "unavailable", reason: "process-exited" };
  const row = /^\s*(\d+)\s+(\d+)\s+([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s*$/.exec(output.text);
  // ponytail: native ps lstart resolves only seconds; same-second PID reuse cannot be distinguished.
  if (!row) return yield* new ResourceError({ operation: "parse Darwin process identity" });
  const actualPid = yield* decodeInteger(row.at(1) ?? "");
  const ppid = yield* decodeInteger(row.at(2) ?? "");
  if (actualPid !== pid) return yield* new ResourceError({ operation: "unexpected native process identity" });
  return { status: "available", pid, ppid, startTime: `darwin:${row.at(3)?.replace(/\s+/g, " ")}` };
});


/** On-demand interval CPU (100% = one core); Linux ticks / Darwin ps hundredths, not lifetime %CPU.
 * startTime is native identity: Linux boot_id + start ticks or Darwin C-locale lstart (one-second resolution).
 * No process command lines, file contents, or host resource totals are returned.
 */
export const observeResources = (roots: ReadonlyArray<ResourceRoot>): Effect.Effect<ResourceObservation, ResourceError, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner | Host> => Effect.gen(function*() {
  if (roots.length > maxProcesses || roots.some((root) => !Number.isSafeInteger(root.pid) || root.pid <= 0)) {
    return yield* new ResourceError({ operation: "invalid resource roots" });
  }
  const host = yield* Host;
  const started = yield* Clock.currentTimeMillis;
  const empty = (reason: string | null, sampledAt: number): ResourceObservation => ({
    processes: [], sampleStartedAt: started, sampledAt, windowMs: 0,
    ownedProcessCount: 0, ownedCpuPercent: null, ownedRssBytes: null,
    sharedProcessCount: 0, unknownProcessCount: 0, partial: reason !== null, truncated: false, unavailableReason: reason,
  });
  if (host.platform !== "linux" && host.platform !== "darwin") return empty("unsupported-platform", yield* Clock.currentTimeMillis);
  if (roots.length === 0) return empty(null, yield* Clock.currentTimeMillis);
  const fs = yield* FileSystem.FileSystem;
  let hz = 1;
  let pageSize = 1;
  let bootId = "";
  if (host.platform === "linux") {
    bootId = yield* linuxBootId();
    const ticks = yield* command("/usr/bin/getconf", ["CLK_TCK"]);
    const pages = yield* command("/usr/bin/getconf", ["PAGESIZE"]);
    hz = yield* decodeInteger(ticks.text.trim());
    pageSize = yield* decodeInteger(pages.text.trim());
    if (ticks.truncated || pages.truncated || hz === 0 || pageSize === 0) return yield* new ResourceError({ operation: "read Linux native units" });
  }
  const snapshot = (): Effect.Effect<Snapshot, ResourceError, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner> => host.platform === "darwin" ? darwinSnapshot : Effect.gen(function*() {
    const names = yield* fs.readDirectory("/proc").pipe(Effect.mapError((cause) => new ResourceError({ operation: "enumerate processes", cause })));
    const pids = names.filter((name) => /^\d+$/.test(name)).map(Number);
    // Requested roots always survive the cap, even on a busy host.
    const unique = [...new Set([...roots.map((root) => root.pid), ...pids])];
    const selected = unique.slice(0, maxProcesses);
    const counters = new Map<number, Counter>();
    const unavailable = new Map<number, string>();
    for (const pid of selected) {
      const counter = yield* linuxCounter(pid, hz, pageSize, bootId);
      if (counter) counters.set(pid, counter);
      else unavailable.set(pid, "process-exited-or-permission-denied");
    }
    return { counters, unavailable, truncated: unique.length > selected.length };
  });
  // Dedicated root reads bracket the entire attribution operation, not just the sleep.
  const rootSnapshot = (): Effect.Effect<Map<number, Counter>, ResourceError, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner> => host.platform === "darwin"
    ? darwinSnapshot.pipe(Effect.map((value) => value.counters))
    : Effect.gen(function*() {
      const counters = new Map<number, Counter>();
      for (const root of roots) {
        const counter = yield* linuxCounter(root.pid, hz, pageSize, bootId);
        if (counter) counters.set(root.pid, counter);
      }
      return counters;
    });
  const before = yield* rootSnapshot();
  const sampleStartedAt = yield* Clock.currentTimeMillis;
  const monotonicStart = yield* Clock.monotonicTimeNanos;
  const first = yield* snapshot();
  yield* Effect.sleep("200 millis");
  const second = yield* snapshot();
  const windowMs = Number((yield* Clock.monotonicTimeNanos) - monotonicStart) / 1_000_000;
  const sampledAt = yield* Clock.currentTimeMillis;
  const after = yield* rootSnapshot();
  const validRoots = new Map<number, ResourceRoot["ownership"]>();
  const invalidRoots = new Map<number, string>();
  for (const root of roots) {
    const initial = before.get(root.pid);
    const final = after.get(root.pid);
    if (!initial || !final) invalidRoots.set(root.pid, "root-exited-or-unavailable");
    else if (initial.startTime !== final.startTime) invalidRoots.set(root.pid, "pid-reused");
    else if (root.startTime !== undefined && root.startTime !== initial.startTime) invalidRoots.set(root.pid, "start-identity-mismatch");
    else {
      const existing = validRoots.get(root.pid);
      validRoots.set(root.pid, existing === "shared" || root.ownership === "shared" ? "shared" : existing === "unknown" || root.ownership === "unknown" ? "unknown" : "exclusive");
    }
  }
  for (const pid of invalidRoots.keys()) validRoots.delete(pid);
  const ownership = (counter: Counter, source: Snapshot): ResourceRoot["ownership"] | null => {
    let current: Counter | undefined = counter;
    let result: ResourceRoot["ownership"] | null = null;
    const visited = new Set<number>();
    let unstable = false;
    while (current && !visited.has(current.pid)) {
      visited.add(current.pid);
      const other = source === first ? second.counters.get(current.pid) : first.counters.get(current.pid);
      const stable = other?.startTime === current.startTime && other.ppid === current.ppid;
      const root = validRoots.get(current.pid);
      if (root) {
        if (before.get(current.pid)?.startTime !== current.startTime) return null;
        result = result === "shared" || root === "shared" ? "shared" : result === "unknown" || root === "unknown" ? "unknown" : "exclusive";
      }
      if ((!stable && current.pid !== counter.pid) || invalidRoots.has(current.pid)) unstable = true;
      current = source.counters.get(current.ppid);
    }
    if (result !== null && unstable) { partial = true; return "unknown"; }
    return result;
  };
  const processes = new Map<number, ResourceProcess>();
  let partial = first.truncated || second.truncated || invalidRoots.size > 0 || first.unavailable.size > 0 || second.unavailable.size > 0;
  for (const counter of [...first.counters.values(), ...second.counters.values()]) {
    if (processes.has(counter.pid)) continue;
    const start = first.counters.get(counter.pid);
    const end = second.counters.get(counter.pid);
    const owner = ownership(counter, start === counter ? first : second);
    if (owner === null) continue;
    let reason = first.unavailable.get(counter.pid) ?? second.unavailable.get(counter.pid) ?? null;
    if (reason === null) {
      if (!start) reason = "not-present-at-sample-start";
      else if (!end) reason = "process-exited";
      else if (start.startTime !== end.startTime) reason = "pid-reused";
      else if (start.ppid !== end.ppid) reason = "parent-changed";
      else if (end.cpuTimeMs < start.cpuTimeMs) reason = "cpu-counter-regressed";
    }
    if (reason !== null) partial = true;
    processes.set(counter.pid, {
      pid: counter.pid, ppid: counter.ppid, startTime: counter.startTime, ownership: reason === null ? owner : "unknown",
      cpuPercent: reason === null && start && end ? (end.cpuTimeMs - start.cpuTimeMs) / (Number(end.monotonicAt - start.monotonicAt) / 1_000_000) * 100 : null,
      cpuTimeMs: reason === null && end ? end.cpuTimeMs : null,
      rssBytes: reason === null && end ? end.rssBytes : null,
      sampleStartedAt: start?.observedAt ?? null,
      sampledAt: end?.observedAt ?? null,
      windowMs: start && end ? Number(end.monotonicAt - start.monotonicAt) / 1_000_000 : null,
      unavailableReason: reason,
    });
  }
  for (const root of roots) {
    if (processes.has(root.pid)) continue;
    partial = true;
    const counter = before.get(root.pid);
    processes.set(root.pid, { pid: root.pid, startTime: counter?.startTime ?? null, ppid: counter?.ppid ?? null, ownership: "unknown", cpuPercent: null, cpuTimeMs: null, rssBytes: null, sampleStartedAt: null, sampledAt: null, windowMs: null, unavailableReason: invalidRoots.get(root.pid) ?? "root-not-present-in-snapshot" });
  }
  const values = [...processes.values()];
  const owned = values.filter((value) => value.ownership === "exclusive");
  const cpu = owned.flatMap((value) => value.cpuPercent === null ? [] : [value.cpuPercent]);
  const rss = owned.flatMap((value) => value.rssBytes === null ? [] : [value.rssBytes]);
  return {
    processes: values, sampleStartedAt, sampledAt, windowMs,
    ownedProcessCount: owned.length,
    ownedCpuPercent: cpu.length ? cpu.reduce((sum, value) => sum + value, 0) : null,
    ownedRssBytes: rss.length ? rss.reduce((sum, value) => sum + value, 0) : null,
    sharedProcessCount: values.filter((value) => value.ownership === "shared").length,
    unknownProcessCount: values.filter((value) => value.ownership === "unknown").length,
    partial, truncated: first.truncated || second.truncated,
    unavailableReason: partial ? first.unavailable.values().next().value ?? second.unavailable.values().next().value ?? "incomplete-process-observation" : null,
  };
});
