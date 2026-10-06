import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Option, Queue, Scope, Semaphore, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { Reactivity } from "effect/reactivity";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { Address, Work, RunnerConfig, RunnerRow, Json, JsonValue, RunnerEvent, RunnerResult, RunnerInspection, NativeFrame, NativeResponse, Acceptance, HttpError, CommandError, Commands, type CommandCapabilities, type CommandInput, type CommandNames, type RunnerReleased, type RunnerRelease, type RunnerRequest, type NativeChild, type RunnerAdmission, type Task } from "./contracts.ts";
import { Platform } from "./platform.ts";
import { Mcp, serverLayer, decode, decodeEffect, decodeJson, encodeJson, canonicalJson, LIMIT, MCP_RESULT_LIMIT, mcpResultSizer } from "./http.ts";
import { migrateRunner } from "./database.ts";

const FRAME_LIMIT = 4 * 1024 * 1024;
const RETAIN_EVENTS = 128;
const Terminal: Record<string, true> = { SUCCEEDED: true, FAILED: true };
function record(value: unknown): value is Json { return typeof value === "object" && value !== null && !Array.isArray(value); }
export const CHILD_UID_BASE = 1000000;
/** setpriv clears groups and every capability before executing an untrusted child. */
export function isolationArgs(uid: number): string[] {
  // Effect has no operating-system platform identifier API; enrollment also proves Linux isolation through setpriv.
  if (process.platform !== "linux" || !Number.isSafeInteger(uid) || uid < CHILD_UID_BASE || uid >= 0xffffffff) throw new Error("Isolated Linux child UID required; macOS is plugin-only");
  return [`--reuid=${uid}`, `--regid=${uid}`, "--clear-groups", "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs"];
}
export const childEnvironment = Effect.fn("Runner.childEnvironment")(function*(home?: string) {
  const env: Record<string, string | undefined> = { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", ...(home ? { HOME: home } : {}) };
  for (const key of ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "OMP_AUTH_BROKER_URL", "OMP_AUTH_BROKER_TOKEN"]) {
    const value = yield* Config.option(Config.String(key));
    if (Option.isSome(value) && value.value) env[key] = value.value;
  }
  return env;
});
export const ownChildDirectory = Effect.fn("Runner.ownChildDirectory")(function*(path: string, uid: number): Effect.fn.Return<void, unknown, Platform> {
  const { fs: fileSystem, path: paths } = yield* Platform;
  const join = paths.join;
  for (const name of yield* (fileSystem.readDirectory(path))) {
    const child = join(path, name);
    // FileSystem.stat/chown follow links; leave link ownership alone rather than touch their targets.
    if (Option.isSome(yield* (Effect.option(fileSystem.readLink(child))))) continue;
    if ((yield* (fileSystem.stat(child))).type === "Directory") yield* ownChildDirectory(child, uid);
    else yield* (fileSystem.chown(child, uid, uid));
  }
  yield* (fileSystem.chown(path, uid, uid));
});
export const requireIsolation = Effect.fn("Runner.requireIsolation")(function*(config: RunnerConfig) {
  const platform = yield* Platform;
  const { fs: fileSystem, path: paths } = platform;
  const { dirname, isAbsolute, join, relative, resolve, sep } = paths;
  const inside = (root: string, path: string) => { const r = relative(root, path); return r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
  const privatePaths = [config.stateDirectory, dirname(config.tokenFile), config.modelDirectory, ...Object.values(config.repositories)];
  if (!isAbsolute(config.childDirectory) || resolve(config.childDirectory) !== config.childDirectory ||
    privatePaths.some((path) => inside(config.childDirectory, path) || inside(path, config.childDirectory))) return yield* Effect.fail(new CommandError({ message: "Child root must be separate from private runner data" }));
  if (!isAbsolute(config.modelDirectory) || (yield* fileSystem.realPath(config.modelDirectory)) !== config.modelDirectory || (yield* (fileSystem.stat(config.modelDirectory))).type !== "Directory") return yield* Effect.fail(new CommandError({ message: "Canonical model seed required" }));
  yield* (fileSystem.makeDirectory(config.childDirectory, { recursive: true, mode: 0o711 }));
  if ((yield* fileSystem.realPath(config.childDirectory)) !== config.childDirectory) return yield* Effect.fail(new CommandError({ message: "Canonical child root required" }));
  yield* (fileSystem.chmod(config.childDirectory, 0o711));
  for (let parent = dirname(config.childDirectory); parent !== "/"; parent = dirname(parent)) {
    if (!((yield* (fileSystem.stat(parent))).mode & 0o001)) return yield* Effect.fail(new CommandError({ message: "Child root ancestors must permit traversal without exposing private runner directories" }));
  }
  const modelFiles = [".omp/agent/config.yml", ".omp/agent/auth.json", ".omp/agent/models.json",
    ".pi/agent/settings.json", ".pi/agent/auth.json", ".pi/agent/models.json"];
  const checkSeed = Effect.fnUntraced(function*(directory: string): Effect.fn.Return<void, unknown> {
    for (const entry of yield* (fileSystem.readDirectory(directory))) {
      const path = join(directory, entry), name = relative(config.modelDirectory, path);
      if ((yield* fileSystem.realPath(path)) !== path) return yield* Effect.fail(new CommandError({ message: "Model seed must contain only scoped model config/auth, not sessions, history or symlinks" }));
      const info = yield* (fileSystem.stat(path));
      if (info.type === "Directory" && modelFiles.some((file) => file.startsWith(`${name}/`))) yield* checkSeed(path);
      else if (info.type !== "File" || !modelFiles.includes(name)) return yield* Effect.fail(new CommandError({ message: "Model seed must contain only scoped model config/auth, not sessions, history or symlinks" }));
    }
  });
  yield* checkSeed(config.modelDirectory);
  yield* Effect.gen(function*() {
    const status = yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), "/usr/bin/cat", "/proc/self/status"], { env: yield* childEnvironment(), extendEnv: false }), 10000);
    const fields = Object.fromEntries(status.split("\n").map((line) => { const colon = line.indexOf(":"); return [line.slice(0, colon), line.slice(colon + 1).trim()]; }));
    if (fields.Uid !== "1000000\t1000000\t1000000\t1000000" || fields.Gid !== "1000000\t1000000\t1000000\t1000000" || fields.Groups !== "" ||
      ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"].some((key) => fields[key] !== "0000000000000000") || fields.NoNewPrivs !== "1") return yield* Effect.fail(new CommandError({ message: "Kernel isolation proof failed" }));
    for (const launcher of Object.values(config.launchers)) yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), launcher, "--version"], { env: yield* childEnvironment("/tmp"), extendEnv: false }), 10000);
  }).pipe(Effect.mapError(() => new CommandError({ message: "OS child UID/capability isolation unavailable; runner enrollment/launch blocked" })));
});
/** LF wire framing accepts optional CRLF; invalid UTF-8 and incomplete frames are errors. */
export class LfFrames {
  private pending = new Uint8Array(0);
  push(chunk: Uint8Array): unknown[] {
    const bytes = new Uint8Array(this.pending.length + chunk.length);
    bytes.set(this.pending); bytes.set(chunk, this.pending.length);
    const frames: unknown[] = []; let start = 0;
    for (let end = bytes.indexOf(10); end !== -1; end = bytes.indexOf(10, start)) {
      if (end - start > FRAME_LIMIT) throw new Error("RPC frame overflow");
      const line = bytes.subarray(start, bytes[end - 1] === 13 ? end - 1 : end);
      if (line.length === 0) throw new Error("Invalid LF frame");
      frames.push(decodeJson(JsonValue, new TextDecoder("utf-8", { fatal: true }).decode(line)));
      start = end + 1;
    }
    this.pending = bytes.slice(start);
    if (this.pending.length > FRAME_LIMIT) throw new Error("RPC frame overflow");
    return frames;
  }
  end(): void { if (this.pending.length) throw new Error("Incomplete RPC frame"); }
}

const byteLength = (value: string) => new TextEncoder().encode(value).byteLength;
const inspectionSize = mcpResultSizer(Commands.inspect.structuredOutput);
const nativeResponseSize = mcpResultSizer(Commands.command.structuredOutput);

class RunnerConfiguration extends Context.Service<RunnerConfiguration, RunnerConfig>()("coordinator/RunnerConfiguration") {}

const makeRunner = Effect.fn("Runner.make")(function*(input: RunnerConfig) {
  const platform = yield* Platform, rpc = yield* Mcp;
  const config = decode(RunnerConfig, input);
  const { fs: fileSystem, path: paths } = platform;
  const nativeEnvironment = yield* childEnvironment();
  const { dirname, isAbsolute, join, relative, resolve, sep } = paths;
  const inside = (root: string, path: string) => { const r = relative(root, path); return r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
  const git = Effect.fnUntraced(function*(cwd: string, args: string[]) { return (yield* platform.exec(ChildProcess.make("git", args, { cwd, env: nativeEnvironment, extendEnv: false }))).trim(); });
  if (!isAbsolute(config.stateDirectory) || !isAbsolute(config.tokenFile)) return yield* Effect.fail(new CommandError({ message: "Absolute private paths required" }));
  const endpoint = new URL(config.brokerUrl);
  if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) return yield* Effect.fail(new CommandError({ message: "TLS required" }));
  yield* requireIsolation(config).pipe(Effect.provideService(Platform, platform));
  const token = yield* platform.privateFile(config.tokenFile);
  if (token.length < 32 || token.length > 480) return yield* Effect.fail(new CommandError({ message: "Invalid credential length" }));
  if (config.controlTokenHash === (yield* platform.digest(token))) return yield* Effect.fail(new CommandError({ message: "Distinct enrollment and control credentials required" }));
  // Effect FileSystem has no process umask API; preserve the private default for SQLite WAL/SHM creation.
  process.umask(0o077);
  yield* fileSystem.makeDirectory(config.stateDirectory, { recursive: true, mode: 0o700 });
  if (((yield* fileSystem.stat(config.stateDirectory)).mode & 0o077) !== 0) return yield* Effect.fail(new CommandError({ message: "Private state directory required" }));
  if ((yield* fileSystem.realPath(config.stateDirectory)) !== config.stateDirectory) return yield* Effect.fail(new CommandError({ message: "Canonical state directory required" }));
  for (const [repo, path] of Object.entries(config.repositories)) {
    if (!isAbsolute(path) || (yield* platform.inspectedRepo(yield* fileSystem.realPath(path))) !== repo) return yield* Effect.fail(new CommandError({ message: "Invalid allowlisted repository" }));
  }
  for (const [name, path] of Object.entries(config.launchers)) {
    const info = yield* fileSystem.stat(path);
    if (!["omp", "pi"].includes(name) || !isAbsolute(path) || info.type !== "File" || !(info.mode & 0o111)) return yield* Effect.fail(new CommandError({ message: "Invalid launcher" }));
  }
  const ownerScope = yield* Scope.Scope;
  const fork = Effect.fnUntraced(function*(effect: Effect.Effect<unknown, unknown>) {
    yield* effect.pipe(Effect.catchCause(() => Effect.void), Effect.forkScoped, Effect.provideService(Scope.Scope, ownerScope));
  });
  const children = new Map<string, NativeChild>(), admissions = new Map<string, RunnerAdmission>();
  const reportLock = yield* Semaphore.make(1);
  const fatal = yield* Deferred.make<never, CommandError>();
  let closed = false, ownershipLost = false, heartbeatBusy = false, reportsBusy = false;
  const owned = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.suspend<A, E | CommandError, R>(() =>
    ownershipLost ? Deferred.await(fatal) : effect.pipe(Effect.raceFirst(Deferred.await(fatal))));
  const ownership = yield* platform.acquireJournalOwnership(join(config.stateDirectory, "runner.lock"));
  yield* ownership.exitCode.pipe(Effect.catch(() => Effect.succeed(undefined)), Effect.andThen(Effect.gen(function*() {
    closed = true; ownershipLost = true;
    const error = new CommandError({ message: "Kernel journal ownership lost" });
    for (const child of children.values()) child.lost = true;
    yield* Deferred.fail(fatal, error);
    for (const child of children.values()) {
      for (const pending of child.pending.values()) yield* Deferred.fail(pending.response, error);
      child.pending.clear();
      yield* Scope.close(child.scope, Exit.void);
    }
  })), Effect.catchCause(() => Effect.void), Effect.forkScoped({ startImmediately: true }), Effect.provideService(Scope.Scope, ownerScope));
  const sql = yield* owned(SqliteClient.make({ filename: join(config.stateDirectory, "runner.sqlite") }).pipe(Effect.provide(Reactivity.layer)));
  yield* owned(fileSystem.chmod(join(config.stateDirectory, "runner.sqlite"), 0o600));

  const initialize = Effect.fn("Runner.initialize")(function*(): Effect.fn.Return<void, unknown> {
      for (const statement of [
        "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA busy_timeout=5000",
      ]) yield* sql.unsafe(statement);
      yield* migrateRunner(sql);
      yield* sql.unsafe("PRAGMA secure_delete=ON");
      if (((yield* sql.unsafe<Json>("PRAGMA user_version"))[0])?.user_version === 0) {
        // One-time scrub of legacy raw journals, including deleted SQLite/WAL pages.
        (yield* sql.unsafe("UPDATE tasks SET reason='historical diagnostic omitted' WHERE reason IS NOT NULL"));
        for (const event of (yield* sql.unsafe<Json>("SELECT seq,payload FROM events"))) {
          if (typeof event.seq !== "number") return yield* Effect.fail(new CommandError({ message: "Invalid journal event identity" }));
          (yield* sql.unsafe("UPDATE events SET payload=? WHERE seq=?", [encodeJson(RunnerEvent, eventMetadata(decodeJson(JsonValue, String(event.payload)))), event.seq]));
        }
        for (const row of (yield* sql.unsafe<Json>("SELECT taskId,result FROM tasks WHERE result IS NOT NULL"))) {
          if (typeof row.taskId !== "string") return yield* Effect.fail(new CommandError({ message: "Invalid journal task identity" }));
          (yield* sql.unsafe("UPDATE tasks SET result=? WHERE taskId=?", [encodeJson(RunnerResult, resultMetadata(decodeJson(JsonValue, String(row.result)))), row.taskId]));
        }
        for (const statement of ["VACUUM", "PRAGMA wal_checkpoint(TRUNCATE)", "PRAGMA user_version=1"]) yield* sql.unsafe(statement);
      }
      // ponytail: no stdio reattachment exists; uncertain owned children reserve their slot until operator reconciliation.
      (yield* sql.unsafe("UPDATE tasks SET state='HELD',reason='restart: launch or prompt outcome unknown' WHERE occupied=1 AND state!='ACCEPTED'"));
      for (const value of (yield* sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks WHERE occupied=1 AND pid IS NOT NULL", [config.childUidBase]))) {
        const row = decode(RunnerRow, value);
        // Effect ChildProcessSpawner can inspect owned handles only; no arbitrary recorded-PID liveness API exists.
        try { process.kill(row.pid!, 0); }
        catch (error) {
          if (record(error) && error.code === "ESRCH") (yield* sql.unsafe("UPDATE tasks SET occupied=0,reason='restart: child gone; outcome remains unknown' WHERE taskId=?", [row.taskId]));
        }
      }
  });
  const getRow = Effect.fn("Runner.row")(function*(taskId: string): Effect.fn.Return<RunnerRow | undefined, unknown> { const value = ((yield* sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks WHERE taskId=?", [config.childUidBase, taskId]))[0]); return value ? decode(RunnerRow, value) : undefined; });
  const requireRow = Effect.fn("Runner.requireRow")(function*(taskId: string): Effect.fn.Return<RunnerRow, unknown> { const row = (yield* getRow(taskId)); if (!row) return yield* new HttpError({ status: 404, message: "Task not accepted" }); return row; });
  const taskWork = (row: RunnerRow): Work => { return decodeJson(Work, row.work); };
  const acceptance = (row: RunnerRow): Acceptance => {
    return { taskId: row.taskId, accepted: true, state: row.state, worktree: row.worktree,
      ...(row.address ? { address: decodeJson(Address, row.address) } : {}), ...(row.reason ? { reason: row.reason } : {}) };
  };
  const load = Effect.fn("Runner.load")(function*(): Effect.fn.Return<number, unknown> { return Number(((yield* sql.unsafe<Json>("SELECT count(*) AS count FROM tasks WHERE occupied=1"))[0])!.count); });
  const resultMetadata = (value: unknown): Json | null => {
    if (!record(value)) return null;
    const result: Json = {};
    for (const key of ["launched", "hostSettled", "evidenceVerified"]) if (typeof value[key] === "boolean") result[key] = value[key];
    if (typeof value.exitCode === "number" && Number.isSafeInteger(value.exitCode)) result.exitCode = value.exitCode;
    if (typeof value.signal === "string" && /^SIG[A-Z0-9]{1,16}$/.test(value.signal)) result.signal = value.signal;
    return result;
  };
  const eventMetadata = (value: unknown): Json => {
    if (!record(value)) return { type: "host_event", contentOmitted: true };
    const type = typeof value.type === "string" && /^[a-z_]{1,40}$/.test(value.type) ? value.type : "host_event";
    const result: Json = { type, contentOmitted: true };
    if (type === "readback") result.address = decode(Address, value.address);
    if (typeof value.state === "string" && ["ACCEPTED", "LAUNCHING", "READY", "PROMPT_INTENT", "RUNNING", "SUCCEEDED", "FAILED", "HELD"].includes(value.state)) result.state = value.state;
    if (typeof value.success === "boolean") result.success = value.success;
    if (typeof value.status === "string" && ["completed", "failed", "aborted", "error"].includes(value.status)) result.status = value.status;
    if (typeof value.command === "string" && ["get_state", "get_messages", "bash", "prompt", "abort"].includes(value.command)) result.command = value.command;
    for (const key of ["fingerprint", "digest"]) if (typeof value[key] === "string" && /^[a-f0-9]{64}$/.test(value[key])) result[key] = value[key];
    if (typeof value.id === "string" && /^[a-f0-9-]{36}$/.test(value.id)) result.id = value.id;
    return result;
  };
  const event = Effect.fn("Runner.event")(function*(taskId: string, value: unknown): Effect.fn.Return<void, unknown> {
    const payload = encodeJson(RunnerEvent, eventMetadata(value));
    (yield* sql.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", [taskId, payload]));
    (yield* sql.unsafe("DELETE FROM events WHERE taskId=? AND seq NOT IN (SELECT seq FROM events WHERE taskId=? ORDER BY seq DESC LIMIT ?)", [taskId, taskId, RETAIN_EVENTS]));
  }, owned);
  const state = Effect.fn("Runner.state")(function*(taskId: string, state: string, reason: string | null = null, result: unknown = null): Effect.fn.Return<void, unknown> {
    (yield* sql.unsafe("UPDATE tasks SET state=?,reason=?,result=? WHERE taskId=?", [state, reason, encodeJson(RunnerResult, resultMetadata(result)), taskId]));
    (yield* event(taskId, { type: "runner_state", state, reason }));
  }, owned);
  const handle = Effect.fn("Runner.handle")(function*(request: RunnerRequest, controlToken: string): Effect.fn.Return<unknown, unknown> {
    if (!(yield* platform.checkToken(controlToken, config.controlTokenHash))) return yield* new HttpError({ status: 401, message: "Control credential required" });
    if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
    // Capabilities are composed only after checking the trusted control credential.
    const capabilities: CommandCapabilities<"runner"> = {
      acceptTask: admit,
      journalTask: Effect.fnUntraced(function*({ taskId }: CommandInput<"journal">): Effect.fn.Return<Acceptance, unknown> {
        if (admissions.has(taskId)) return yield* new HttpError({ status: 503, message: "Acceptance pending; ownership unknown" });
        const row = yield* getRow(taskId); return row ? acceptance(row) : { taskId, accepted: false, state: "not-accepted" };
      }),
      inspectTask: Effect.fnUntraced(function*({ taskId, cursor = 0 }: CommandInput<"inspect">): Effect.fn.Return<RunnerInspection, unknown> {
        const row = yield* requireRow(taskId);
        const rows = yield* sql.unsafe<Json>("SELECT seq,payload FROM events WHERE taskId=? AND seq>? ORDER BY seq LIMIT 2", [taskId, cursor]);
        const oldest = (yield* sql.unsafe<Json>("SELECT min(seq) AS seq FROM events WHERE taskId=?", [taskId]))[0]?.seq;
        const events: RunnerEvent[] = [];
        let nextCursor = cursor;
        if (typeof oldest === "number" && cursor < oldest - 1) events.push({ type: "gap", reason: "retention or cursor gap", before: oldest });
        for (const event of rows) {
          const value = decodeJson(RunnerEvent, String(event.payload));
          events.push(value); nextCursor = Number(event.seq);
          if (inspectionSize({ result: { ...acceptance(row), events, nextCursor } }) > MCP_RESULT_LIMIT) {
            events.pop(); events.push({ type: "gap", reason: "event exceeds remaining response bound", seq: Number(event.seq) }); break;
          }
        }
        return { ...acceptance(row), events, nextCursor };
      }),
      commandTask: Effect.fnUntraced(function*({ taskId, type }: CommandInput<"command">) {
        yield* requireRow(taskId);
        const response = yield* nativeRequest(taskId, { type });
        if (type === "abort") yield* state(taskId, "HELD", "operator requested abort; await host outcome");
        return boundedResponse(response);
      }),
      prepareTask: Effect.fnUntraced(function*(input: CommandInput<"prepare">) {
        const row = yield* requireRow(input.taskId);
        if (row.state !== "READY" || row.promptId || !row.address) return yield* new HttpError({ status: 409, message: "Corrected readback requires unprompted live owned child" });
        const prior = decodeJson(Address, row.address);
        if (input.address.revision <= prior.revision || input.address.sessionId !== prior.sessionId || input.address.incarnation !== prior.incarnation) return yield* new HttpError({ status: 409, message: "Correction revision or child binding mismatch" });
        const actual = yield* readback(row, yield* nativeRequest(row.taskId, { type: "get_state" }), input.address);
        if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Corrected consequential readback mismatch" });
        if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
        yield* owned(sql.unsafe("UPDATE tasks SET address=? WHERE taskId=? AND state='READY' AND promptId IS NULL", [encodeJson(Address, actual), row.taskId]));
        yield* event(row.taskId, { type: "readback", address: actual }); return actual;
      }),
      releaseTask: release,
    };
    return yield* Commands[request.method].executeDecoded(capabilities, request.params, decodeEffect);
  }, Effect.catch((error) => Effect.fail(error instanceof HttpError ? error : new HttpError({ status: 400, message: "Invalid request or unavailable owned child" }))));
  const brokerTask = Effect.fn("Runner.brokerTask")(function*(work: Work, address?: Address): Effect.fn.Return<Task, unknown> {
    const task = yield* rpc.call(config.brokerUrl, token, "task.get", { taskId: work.taskId });
    if (task.taskId !== work.taskId || task.runnerId !== config.id || task.fingerprint !== (yield* platform.fingerprint(work)) || (yield* platform.fingerprint(task.request)) !== (yield* platform.fingerprint(work))) return yield* new HttpError({ status: 403, message: "Broker task binding mismatch" });
    if (address && (!task.address || !task.readback || task.disposition !== "ready" || task.confirmedRevision !== address.revision ||
      (yield* platform.fingerprint(task.address)) !== (yield* platform.fingerprint(address)) || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)))) return yield* new HttpError({ status: 409, message: "Broker confirmation or current readback mismatch" });
    return task;
  });
  const accept = Effect.fn("Runner.accept")(function*(work: Work): Effect.fn.Return<Acceptance, unknown> {
    if (byteLength(encodeJson(Work, work)) > LIMIT - 1024) return yield* new HttpError({ status: 413, message: "Work too large" });
    const existing = (yield* getRow(work.taskId)); const hash = (yield* platform.fingerprint(work));
    if (existing) { if (existing.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return acceptance(existing); }
    if ((yield* load()) >= config.slots) return { taskId: work.taskId, accepted: false, state: "full", reason: "full" };
    yield* brokerTask(work);
    const concurrent = (yield* getRow(work.taskId));
    if (concurrent) { if (concurrent.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return acceptance(concurrent); }
    const repo = config.repositories[work.repo]; const launcher = config.launchers[work.launcher];
    if (!repo || !launcher) return yield* new HttpError({ status: 403, message: "Repository or launcher not allowlisted" });
    if (!work.ref || work.ref.startsWith("-") || /[\r\n\0]/.test(work.ref)) return yield* new HttpError({ status: 400, message: "Invalid ref" });
    if ((yield* git(repo, ["rev-parse", "--verify", `${work.ref}^{commit}`])) !== work.sha || (yield* git(repo, ["rev-parse", "--verify", `${work.sha}^{commit}`])) !== work.sha) return yield* new HttpError({ status: 409, message: "Exact ref/SHA unavailable" });
    const assigned = yield* brokerTask(work);
    if (work.deadline <= (yield* Clock.currentTimeMillis) || assigned.state !== "ASSIGNING" && assigned.state !== "ACCEPTED") return yield* new HttpError({ status: 409, message: "Assignment expired or no longer dispatchable" });
    const accepted = (yield* getRow(work.taskId));
    if (accepted) { if (accepted.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return acceptance(accepted); }
    const worktree = join(config.childDirectory, (yield* platform.digest(work.taskId)), "repo");
    const incarnation = yield* platform.crypto.randomUUIDv4;
    const reserved = yield* owned(sql.withTransaction(Effect.gen(function*() {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      const existing = yield* getRow(work.taskId);
      if (existing) {
        if (existing.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" });
        return acceptance(existing);
      }
      if ((yield* load()) >= config.slots) return { taskId: work.taskId, accepted: false, state: "full", reason: "full" };
      if (work.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Assignment expired before durable acceptance" });
      yield* sql.unsafe("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (?,?,?,?,'ACCEPTED',1,?)", [work.taskId, hash, encodeJson(Work, work), worktree, incarnation]);
      return acceptance(yield* requireRow(work.taskId));
    })));
    if (!reserved.accepted) return reserved;
    (yield* event(work.taskId, { type: "accepted", fingerprint: hash }));
    (yield* schedule(work.taskId));
    return acceptance((yield* requireRow(work.taskId)));
  });
  const verifyCheckout = Effect.fn("Runner.verifyCheckout")(function*(row: RunnerRow) {
    const work = taskWork(row);
    if ((yield* fileSystem.realPath(row.worktree)) !== row.worktree || !inside(yield* fileSystem.realPath(config.childDirectory), row.worktree)) return yield* Effect.fail(new CommandError({ message: "Worktree escaped child root" }));
    // Effect's cwd access preflight checks real UID; env chdir honors DAC_OVERRIDE before setpriv drops privileges.
    const inspect = Effect.fnUntraced(function*(args: string[]) { return (yield* platform.exec(ChildProcess.make("/usr/bin/env", [`--chdir=${row.worktree}`, "/usr/bin/setpriv", ...isolationArgs(row.childUid), "git", ...args], { env: nativeEnvironment, extendEnv: false }))).trim(); });
    if ((yield* inspect(["remote"])) !== "origin" || platform.canonicalRemote(yield* inspect(["remote", "get-url", "--all", "origin"])) !== work.repo || (yield* inspect(["rev-parse", "HEAD"])) !== work.sha || (yield* inspect(["rev-parse", "--verify", `${work.ref}^{commit}`])) !== work.sha) return yield* Effect.fail(new CommandError({ message: "Checkout identity mismatch" }));
  });
  const readback = Effect.fn("Runner.readback")(function*(row: RunnerRow, response: NativeFrame, template?: Address): Effect.fn.Return<Address, unknown> {
    const work = taskWork(row); const data = response.data;
    if (response.success !== true || !record(data) || typeof data.sessionId !== "string" || typeof data.sessionFile !== "string" || data.isStreaming !== false) return yield* Effect.fail(new CommandError({ message: "Unverified durable host identity" }));
    const sessions = yield* (fileSystem.realPath(join(config.childDirectory, (yield* platform.digest(row.taskId)), "sessions")));
    if (!isAbsolute(data.sessionFile) || !inside(sessions, resolve(data.sessionFile))) return yield* Effect.fail(new CommandError({ message: "Unexpected session path" }));
    const cwdResponse = yield* nativeRequest(row.taskId, { type: "bash", command: "pwd -P" });
    const cwdData = cwdResponse.data;
    if (!record(cwdData) || cwdData.exitCode !== 0 || typeof cwdData.output !== "string" || cwdData.output.trim() !== row.worktree) return yield* Effect.fail(new CommandError({ message: "Host cwd mismatch" }));
    yield* verifyCheckout(row);
    // Native session identity is the actual addressable root worker, not a fabricated requested ID.
    return decode(Address, { messageId: template?.messageId ?? row.taskId, revision: template?.revision ?? 1, workerId: data.sessionId, sessionId: data.sessionId,
      incarnation: row.incarnation, repo: work.repo, ref: work.ref, sha: work.sha,
      scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline });
  });
  const boundedResponse = (response: NativeFrame): NativeResponse => {
    const projected = response.command === "get_state" && record(response.data)
      ? decodeJson(NativeResponse, canonicalJson({ type: "response", id: response.id, command: "get_state", success: response.success,
        data: { sessionId: response.data.sessionId, sessionFile: response.data.sessionFile, isStreaming: response.data.isStreaming, isCompacting: response.data.isCompacting,
          isSettled: response.data.isSettled, messageCount: response.data.messageCount, pendingMessageCount: response.data.pendingMessageCount, queuedMessageCount: response.data.queuedMessageCount } }))
      : decode(NativeResponse, response);
    return nativeResponseSize({ result: projected }) <= MCP_RESULT_LIMIT ? projected : decode(NativeResponse, { type: "gap", reason: "host response exceeds endpoint bound", command: response.command });
  };
  const frame = Effect.fn("Runner.frame")(function*(taskId: string, child: NativeChild | undefined, value: unknown): Effect.fn.Return<void, unknown> {
    if (!record(value) || typeof value.type !== "string") return yield* Effect.fail(new CommandError({ message: "Malformed RPC object" }));
    if (value.type === "rpc_chunk") return yield* Effect.fail(new CommandError({ message: "Unnegotiated chunk protocol" }));
    if (value.type === "response") {
      if (typeof value.id !== "string" || typeof value.command !== "string" || typeof value.success !== "boolean") return yield* Effect.fail(new CommandError({ message: "Malformed response" }));
      const pending = child?.pending.get(value.id);
      if (pending) {
        if (pending.type !== value.command) return yield* Effect.fail(new CommandError({ message: "Response command mismatch" }));
        child!.pending.delete(value.id);
        if (value.success) yield* Deferred.succeed(pending.response, value); else yield* Deferred.fail(pending.response, new CommandError({ message: "Host command rejected" }));
      }
      (yield* event(taskId, { type: "host_response", id: value.id, command: value.command, success: value.success })); return;
    }
    (yield* event(taskId, { type: value.type === "readback" ? "host_event" : value.type, status: value.status }));
    const row = (yield* requireRow(taskId)); if (!row.promptId || !child || child.lost) return;
    if (value.type === "agent_start") child.started = true;
    if (value.type === "message_end" && record(value.message) && ["error", "aborted"].includes(String(value.message.stopReason))) child.failed = true;
    if (taskWork(row).launcher === "omp") {
      if (value.type === "prompt_result" && value.id === row.promptId) {
        if (value.status !== "completed") child.failed = true;
        if (value.sessionSettled === true) yield* fork(finish(taskId, child));
      } else if (value.type === "session_settled" && child.started) yield* fork(finish(taskId, child));
    } else if ((value.type === "agent_end" || value.type === "agent_settled") && child.started) yield* fork(finish(taskId, child));
  });
  const finish = Effect.fn("Runner.finish")(function*(taskId: string, child: NativeChild): Effect.fn.Return<void, unknown> {
    if (child.completing || child.lost) return; child.completing = true;
      const response = yield* nativeRequest(taskId, { type: "get_state" }); const data = response.data;
      if (!record(data) || data.isStreaming !== false || data.isCompacting !== false || data.hasPendingAsyncWork === true || data.isSettled === false || Number(data.pendingMessageCount ?? data.queuedMessageCount ?? 0) !== 0) { child.completing = false; return; }
      const row = (yield* requireRow(taskId));
      if (!row.address || data.sessionId !== decodeJson(Address, row.address).sessionId) return yield* Effect.fail(new CommandError({ message: "Host identity changed" }));
      (yield* state(taskId, child.failed ? "FAILED" : "SUCCEEDED", null, { hostSettled: true, evidenceVerified: false }));
      yield* Queue.end(child.input); yield* fork(heartbeat());

  }, (effect, taskId) => effect.pipe(Effect.catch(() => hold(taskId, "completion identity/outcome uncertain"))));
  const release = Effect.fn("Runner.release")(function*(input: RunnerRelease): Effect.fn.Return<RunnerReleased, unknown> {
    const row = (yield* requireRow(input.taskId)); const work = taskWork(row);
    if (!row.address || (yield* platform.fingerprint(input.address)) !== (yield* platform.fingerprint(decodeJson(Address, row.address))) || input.prompt !== work.prompt) return yield* new HttpError({ status: 409, message: "Release address or approved prompt mismatch" });
    if (row.promptId) {
      if (row.state === "RUNNING" || Terminal[row.state]) return { released: true };
      return yield* new HttpError({ status: 409, message: "Prompt delivery uncertain; never replay" });
    }
    if (row.state !== "READY") return yield* new HttpError({ status: 409, message: "Child not ready" });
    if (row.detached) return yield* new HttpError({ status: 403, message: "Task no longer assigned" });
    yield* brokerTask(work, input.address);
    const response = yield* nativeRequest(row.taskId, { type: "get_state" }); const actual = yield* readback((yield* requireRow(row.taskId)), response, decodeJson(Address, row.address));
    if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Actual child address changed" });
    // Native readback awaited RPCs; recheck the ledger immediately before reserving prompt intent.
    yield* brokerTask(work, input.address);
    if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
    const promptId = (yield* platform.crypto.randomUUIDv4);
    yield* owned(sql.withTransaction(Effect.gen(function*() {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
      const reserved = yield* sql.unsafe("UPDATE tasks SET state='PROMPT_INTENT',promptId=? WHERE taskId=? AND state='READY' AND promptId IS NULL RETURNING taskId", [promptId, row.taskId]);
      if (reserved.length !== 1) return yield* new HttpError({ status: 409, message: "Prompt already reserved" });
      yield* event(row.taskId, { type: "prompt_intent", id: promptId, digest: yield* platform.digest(work.prompt) });
    })));
    yield* nativeRequest(row.taskId, { type: "prompt", message: work.prompt }, 60000, promptId, input.address.deadline).pipe(Effect.catch(() => Effect.gen(function*() {
      yield* hold(row.taskId, "prompt send/admission uncertain; never replay");
      return yield* new HttpError({ status: 409, message: "Prompt send uncertain; held" });
    })));
    if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
    yield* owned(sql.unsafe("UPDATE tasks SET state='RUNNING' WHERE taskId=? AND state='PROMPT_INTENT'", [row.taskId]));
    yield* fork(heartbeat());
    return { released: true };
  });
  const heartbeat = Effect.fn("Runner.heartbeat")(function*() {
    if (closed || heartbeatBusy) return;
    heartbeatBusy = true;
    yield* Effect.gen(function*() {
      const rows = (yield* sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks WHERE detached=0 ORDER BY taskId", [config.childUidBase])).map((value) => decode(RunnerRow, value));
      const journal: Acceptance[] = []; let size = 1024;
      for (const row of rows) { const item = acceptance(row); const bytes = byteLength(encodeJson(Acceptance, item)); if (size + bytes > LIMIT - 1024) break; journal.push(item); size += bytes; }
      yield* rpc.call(config.brokerUrl, token, "runner.heartbeat", { runnerId: config.id, slots: config.slots, load: yield* load(), journal }, 4000);
      if (!reportsBusy) yield* fork(reports(rows));
    }).pipe(Effect.catch(() => Effect.void), Effect.ensuring(Effect.sync(() => { heartbeatBusy = false; })));
    // Broker outages never interrupt native work or replay uncertain prompt sends.
  });
  const reports = Effect.fn("Runner.reports")(function*(rows: readonly RunnerRow[]) {
    reportsBusy = true;
    yield* Effect.forEach(rows, (value) => Effect.gen(function*() {
      const row = yield* getRow(value.taskId);
      if (!row || row.detached) return;
      yield* Effect.gen(function*() {
        if (row.address && row.state === "READY") yield* rpc.call(config.brokerUrl, token, "runner.readback", { runnerId: config.id, taskId: row.taskId, address: decodeJson(Address, row.address) });
        if (["RUNNING", "SUCCEEDED", "FAILED", "HELD"].includes(row.state)) yield* rpc.call(config.brokerUrl, token, "runner.report", { runnerId: config.id, taskId: row.taskId, state: row.state, result: row.result ? decodeJson(JsonValue, row.result) : {} });
      }).pipe(Effect.catch((error) => Effect.gen(function*() {
        if (!(error instanceof HttpError) || error.status !== 403) return;
        // Read the typed error rather than treating a network failure or report conflict as stale ownership.
        const obsolete = yield* rpc.call(config.brokerUrl, token, "task.get", { taskId: row.taskId }).pipe(Effect.map(() => false), Effect.catch((bindingError) => Effect.succeed(bindingError instanceof HttpError && bindingError.status === 403)));
        if (!obsolete || closed) return;
        if (!Terminal[row.state]) yield* hold(row.taskId, "broker assignment withdrawn; no replay");
        yield* owned(sql.unsafe("UPDATE tasks SET detached=1,occupied=CASE WHEN state='HELD' AND pid IS NULL THEN 0 ELSE occupied END WHERE taskId=?", [row.taskId]));
      })));
    })).pipe(Effect.asVoid, Effect.ensuring(Effect.sync(() => { reportsBusy = false; })));
  }, (effect) => reportLock.withPermit(effect));

  const hold = Effect.fn("Runner.hold")(function*(taskId: string, reason: string) {
    yield* state(taskId, "HELD", reason);
    const child = children.get(taskId);
    if (child) {
      child.lost = true;
      for (const pending of child.pending.values()) yield* Deferred.fail(pending.response, new CommandError({ message: "Child RPC unavailable" }));
      child.pending.clear();
    }
  });
  const nativeRequest = Effect.fn("Runner.request")(function*(taskId: string, params: { type: CommandInput<CommandNames<"native">>["type"]; command?: string; message?: string }, timeout = 30000, forcedId?: string, approvedDeadline?: number): Effect.fn.Return<NativeFrame, unknown> {
    const id = forcedId ?? (yield* platform.crypto.randomUUIDv4);
    const send = Effect.fnUntraced(function*(frame: NativeFrame): Effect.fn.Return<NativeFrame, unknown> {
      const child = children.get(taskId);
      if (!child || child.lost) return yield* Effect.fail(new CommandError({ message: "Owned child stdio unavailable" }));
      const response = yield* Deferred.make<Json, unknown>();
      child.pending.set(id, { type: frame.type, response });
      return yield* Effect.gen(function*() {
        if (frame.type === "prompt" && (approvedDeadline === undefined || approvedDeadline <= (yield* Clock.currentTimeMillis))) {
          yield* hold(taskId, "Approved deadline elapsed before prompt enqueue; never replay");
          return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
        }
        yield* Queue.offer(child.input, new TextEncoder().encode(`${encodeJson(NativeFrame, frame)}\n`));
        return yield* decodeEffect(NativeFrame, yield* Deferred.await(response));
      }).pipe(Effect.timeout(timeout), Effect.ensuring(Effect.sync(() => child.pending.delete(id))));
    });
    const capabilities: CommandCapabilities<"native"> = {
      nativeGetState: send,
      nativeGetMessages: send,
      nativeBash: send,
      nativePrompt: send,
      nativeAbort: send,
    };
    return yield* Commands[`native.${params.type}`].executeDecoded(capabilities, { id, ...params }, decodeEffect);
  });
  const admit = Effect.fn("Runner.admit")(function*(work: Work): Effect.fn.Return<Acceptance, unknown> {
    const hash = yield* platform.fingerprint(work);
    const pending = admissions.get(work.taskId);
    if (pending) {
      if (pending.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" });
      return yield* Deferred.await(pending.response);
    }
    const response = yield* Deferred.make<Acceptance, unknown>();
    admissions.set(work.taskId, { fingerprint: hash, response });
    yield* fork(accept(work).pipe(Effect.exit, Effect.flatMap((exit) => Deferred.done(response, exit)), Effect.ensuring(Effect.sync(() => admissions.delete(work.taskId)))));
    return yield* Deferred.await(response);
  });
  const schedule = Effect.fnUntraced(function*(taskId: string) { if (!closed) yield* fork(launch(taskId)); });


  const launch = Effect.fn("Runner.launch")(function*(taskId: string) {
    let launchIntent = false;
    let childScope: Scope.Closeable | undefined;
    yield* Effect.gen(function*() {
      const row = yield* requireRow(taskId);
      if (row.state !== "ACCEPTED" || row.detached || children.has(taskId)) return;
      const work = taskWork(row), repo = config.repositories[work.repo]!;
      if (row.childUid >= config.childUidBase + CHILD_UID_BASE) return yield* Effect.fail(new CommandError({ message: "Reserved child UID range exhausted" }));
      if (work.deadline <= (yield* Clock.currentTimeMillis)) {
        yield* state(taskId, "FAILED", "Approved deadline elapsed before launch", { launched: false, evidenceVerified: false });
        yield* owned(sql.unsafe("UPDATE tasks SET occupied=0 WHERE taskId=?", [taskId]));
        yield* fork(heartbeat()); return;
      }
      const childRoot = join(config.childDirectory, yield* platform.digest(taskId));
      yield* fileSystem.makeDirectory(childRoot, { recursive: true, mode: 0o700 });
      if (Option.isNone(yield* Effect.option(fileSystem.stat(join(row.worktree, ".git"))))) {
        yield* git(repo, ["clone", "--no-hardlinks", "--no-checkout", repo, row.worktree]);
        yield* git(row.worktree, ["remote", "set-url", "origin", yield* git(repo, ["remote", "get-url", "origin"])]);
        yield* git(row.worktree, ["checkout", "--detach", work.sha]);
        yield* git(row.worktree, ["fetch", "--no-tags", repo, "+refs/heads/*:refs/heads/*"]);
      }
      yield* fileSystem.chmod(row.worktree, 0o700);
      const sessions = join(childRoot, "sessions"), home = join(childRoot, "home");
      yield* fileSystem.makeDirectory(sessions, { recursive: true, mode: 0o700 });
      yield* fileSystem.makeDirectory(home, { recursive: true, mode: 0o700 });
      yield* fileSystem.copy(config.modelDirectory, home, { overwrite: true });
      yield* ownChildDirectory(childRoot, row.childUid).pipe(Effect.provideService(Platform, platform));
      yield* verifyCheckout(row);
      yield* state(taskId, "LAUNCHING"); // Persisted intent precedes spawn; an uncertain launch is never replayed.
      launchIntent = true;
      childScope = yield* Scope.fork(ownerScope);
      const input = yield* Queue.make<Uint8Array, Cause.Done>({ capacity: 32 });
      const process = yield* platform.spawner.spawn(ChildProcess.make("/usr/bin/env", [`--chdir=${row.worktree}`, "/usr/bin/setpriv", ...isolationArgs(row.childUid), config.launchers[work.launcher]!, "--mode", "rpc", "--session-dir", sessions, "--no-extensions"], {
        env: { ...nativeEnvironment, HOME: home }, extendEnv: false, detached: true, killSignal: "SIGTERM", forceKillAfter: "5 seconds"
      })).pipe(Effect.provideService(Scope.Scope, childScope));
      if (closed || ownershipLost) {
        yield* Scope.close(childScope, Exit.void);
        return yield* Effect.fail(new CommandError({ message: "Runner journal ownership unavailable" }));
      }
      const child: NativeChild = { process, scope: childScope, input, pending: new Map(), lost: false, completing: false, started: false, failed: false };
      children.set(taskId, child);
      yield* owned(sql.unsafe("UPDATE tasks SET pid=? WHERE taskId=?", [Number(process.pid), taskId]));
      yield* Scope.addFinalizer(child.scope, Effect.gen(function*() {
        const row = yield* getRow(taskId);
        if (row && !Terminal[row.state]) yield* hold(taskId, "child scope closed: outcome unknown; no replay");
      }).pipe(Effect.catchCause(() => Effect.void)));
      const childFork = Effect.fnUntraced(function*(effect: Effect.Effect<unknown, unknown>) {
        yield* effect.pipe(Effect.catch(() => hold(taskId, "invalid, truncated, or oversized host RPC output")), Effect.forkScoped, Effect.provideService(Scope.Scope, child.scope));
      });
      const frames = new LfFrames();
      yield* childFork(Stream.fromQueue(input).pipe(Stream.run(process.stdin)));
      yield* childFork(process.stdout.pipe(Stream.runForEach((chunk) => Effect.gen(function*() {
        const values = yield* Effect.try(() => frames.push(chunk));
        for (const value of values) yield* frame(taskId, child, value);
      })), Effect.andThen(Effect.try(() => frames.end())), Effect.andThen(Effect.suspend(() => child.completing ? Effect.void : hold(taskId, "host stdio disconnected")))));
      // Drain provider diagnostics without exposing or persisting credentials/transcript.
      yield* childFork(Stream.runDrain(process.stderr));
      yield* fork(Effect.gen(function*() {
        const exitCode = yield* process.exitCode;
        for (const pending of child.pending.values()) yield* Deferred.fail(pending.response, new CommandError({ message: "Child exited" }));
        child.pending.clear();
        if (ownershipLost) return;
        const current = yield* requireRow(taskId);
        if (!Terminal[current.state]) yield* state(taskId, "FAILED", "owned child exited before confirmed completion", { exitCode: Number(exitCode) });
        yield* owned(sql.unsafe("UPDATE tasks SET occupied=0 WHERE taskId=?", [taskId]));
        children.delete(taskId);
        yield* Scope.close(child.scope, Exit.void);
        yield* heartbeat();
      }).pipe(Effect.catch(() => hold(taskId, "child exit outcome uncertain"))));
      const response = yield* nativeRequest(taskId, { type: "get_state" }, 60000);
      const address = yield* readback(yield* requireRow(taskId), response);
      if (closed) return yield* Effect.fail(new CommandError({ message: "Runner journal ownership unavailable" }));
      yield* owned(sql.unsafe("UPDATE tasks SET address=?,state='READY',reason=NULL WHERE taskId=? AND state='LAUNCHING'", [encodeJson(Address, address), taskId]));
      yield* event(taskId, { type: "readback", address });
      yield* fork(heartbeat());
    }).pipe(Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.gen(function*() {
      if (launchIntent) yield* hold(taskId, "launch/readback uncertain; no automatic relaunch");
      else {
        if (childScope) yield* Scope.close(childScope, Exit.void);
        yield* state(taskId, "FAILED", "allowlisted checkout preparation failed");
        yield* owned(sql.unsafe("UPDATE tasks SET occupied=0 WHERE taskId=?", [taskId]));
      }
    })));
  });


  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    closed = true;
    for (const [taskId, child] of children) {
      child.lost = true;
      for (const pending of child.pending.values()) yield* Deferred.fail(pending.response, new CommandError({ message: "Runner child RPC unavailable" }));
      child.pending.clear();
      if (!ownershipLost) {
        const row = yield* getRow(taskId);
        if (row && !Terminal[row.state]) yield* hold(taskId, "runner shutdown: child outcome unknown; no replay");
      }
      yield* Scope.close(child.scope, Exit.void);
    }
    // Do not clear uncertain prompt IDs/slots; restart reconciles recorded PIDs without replay.
    children.clear();
  }).pipe(Effect.catchCause(() => Effect.void)));
  yield* owned(initialize());
  const serverContext = yield* owned(Layer.build(serverLayer(config.host, config.port)));
  const server = Context.get(serverContext, HttpServer.HttpServer);
  const authorize = Effect.fn("Runner.authorize")(function*(bearer: string) {
    if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
    if (!(yield* platform.checkToken(bearer, config.controlTokenHash)) && !(yield* platform.checkToken(bearer, yield* platform.digest(token)))) return yield* new HttpError({ status: 401, message: "Unauthorized" });
    return bearer;
  });
  yield* owned(rpc.serve(server, { target: "runner", authorize, handle: (bearer, request) => handle(request, bearer) }));
  yield* owned(Effect.gen(function*() {
    for (const value of yield* sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks WHERE state='ACCEPTED' AND occupied=1 AND detached=0", [config.childUidBase])) yield* schedule(decode(RunnerRow, value).taskId);
    yield* fork(Effect.forever(heartbeat().pipe(Effect.andThen(Effect.sleep("5 seconds")))));
  }));
  if (ownershipLost) return yield* Deferred.await(fatal);
  return { config, sql, server, failure: Deferred.await(fatal), authorize, handle, row: getRow, state, event, frame, reports, heartbeat, launch };
});

export class Runner extends Context.Service<Runner>()("coordinator/Runner", {
  make: Effect.gen(function*() { return yield* makeRunner(yield* RunnerConfiguration); })
}) {
  static layer(config: RunnerConfig) { return Layer.effect(Runner, Runner.make).pipe(Layer.provide(Layer.succeed(RunnerConfiguration, config))); }
}
