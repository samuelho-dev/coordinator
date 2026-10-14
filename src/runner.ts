import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Option, Scope, Schema, Semaphore } from "effect";
import { Hex } from "effect/encoding";
import { fileURLToPath } from "node:url";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { Address, Work, RunnerConfig, RunnerRow, Json, JsonValue, RunnerEvent, RunnerResult, RunnerInspection, RunnerNativeState, TaskId, MessageId, SessionId, IncarnationId, WorkerId, UserId, Acceptance, HttpError, CommandError, Commands, type CommandCapabilities, type CommandInput, type CommandOutput, type RunnerReleased, type RunnerRelease, type RunnerRequest, type RunnerAdmission, type Task } from "./contracts.ts";
import { Platform } from "./platform.ts";
import { Mcp, serverLayer, LIMIT, MCP_RESULT_LIMIT } from "./http.ts";
import { assertRunnerSchema } from "./database.ts";
import { Postgres } from "./postgres.ts";
import { Database } from "./kysely.ts";
import { sql as expression } from "kysely";
import { RunnerEventsId } from "./generated/database/types.ts";
import { ProcessId } from "./contracts.ts";
import { connectNativeClient, type NativeClient } from "@samuelho/coordinator-native-control/client";
import { NativeBinding, NativePluginConfig, NativeSubmissionId, NativeEventSequence, NativeState, type NativeLifecycleEvent, type NativePromptAdmission, type NativePromptResult } from "@samuelho/coordinator-native-control/contracts";

type OwnedNativeChild = { client: NativeClient; scope: Scope.Closeable; lost: boolean };
const bindingText = Schema.encodeUnknownSync(Schema.fromJsonString(NativeBinding), { onExcessProperty: "error" });
const sameBinding = (left: NativeBinding, right: NativeBinding) => left.host === right.host && left.sessionId === right.sessionId && left.incarnation === right.incarnation && left.generation === right.generation;

const RETAIN_EVENTS = 128;
const Terminal: Record<string, true> = { SUCCEEDED: true, FAILED: true };
function record(value: unknown): value is Json { return typeof value === "object" && value !== null && !Array.isArray(value); }
export const CHILD_UID_BASE = UserId.make(1000000);
/** setpriv clears groups and every capability before executing an untrusted child. */
export function isolationArgs(uid: UserId): string[] {
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
export const ownChildDirectory = Effect.fn("Runner.ownChildDirectory")(function*(path: string, uid: UserId): Effect.fn.Return<void, unknown, Platform> {
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
  const privatePaths = [config.stateDirectory, dirname(config.tokenFile), dirname(config.databaseUrlFile), config.modelDirectory, ...Object.values(config.repositories)];
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
    const status = yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), "/usr/bin/cat", "/proc/self/status"], { env: yield* childEnvironment(), extendEnv: false }), 10000).pipe(Effect.withSpan("Runner.requireIsolation.kernel"));
    const fields = Object.fromEntries(status.split("\n").map((line) => { const colon = line.indexOf(":"); return [line.slice(0, colon), line.slice(colon + 1).trim()]; }));
    if (fields.Uid !== "1000000\t1000000\t1000000\t1000000" || fields.Gid !== "1000000\t1000000\t1000000\t1000000" || fields.Groups !== "" ||
      ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"].some((key) => fields[key] !== "0000000000000000") || fields.NoNewPrivs !== "1") return yield* Effect.fail(new CommandError({ message: "Kernel isolation proof failed" }));
    for (const launcher of Object.values(config.launchers)) yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), launcher, "--version"], { env: yield* childEnvironment("/tmp"), extendEnv: false }), 10000).pipe(Effect.withSpan("Runner.requireIsolation.launcherVersion"));
  }).pipe(Effect.withSpan("Runner.requireIsolation.proof"), Effect.mapError(() => new CommandError({ message: "OS child UID/capability isolation unavailable; runner enrollment/launch blocked" })));
});

const byteLength = (value: string) => new TextEncoder().encode(value).byteLength;
const inspectionText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands.inspect.structuredOutput)), { onExcessProperty: "error" });
const nativeResponseText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands.command.structuredOutput)), { onExcessProperty: "error" });

class RunnerConfiguration extends Context.Service<RunnerConfiguration, RunnerConfig>()("coordinator/RunnerConfiguration") {}

const makeRunner = Effect.fn("Runner.make")(function*(input: RunnerConfig) {
  const platform = yield* Platform, rpc = yield* Mcp;
  const config = input;
  const { fs: fileSystem, path: paths } = platform;
  const nativeEnvironment = yield* childEnvironment();
  const { isAbsolute, join, relative, resolve, sep } = paths;
  const inside = (root: string, path: string) => { const r = relative(root, path); return r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
  const git = Effect.fnUntraced(function*(cwd: string, args: string[]) { return (yield* platform.exec(ChildProcess.make("git", args, { cwd, env: nativeEnvironment, extendEnv: false }))).trim(); });
  if (!isAbsolute(config.stateDirectory) || !isAbsolute(config.tokenFile)) return yield* Effect.fail(new CommandError({ message: "Absolute private paths required" }));
  const endpoint = new URL(config.brokerUrl);
  if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) return yield* Effect.fail(new CommandError({ message: "TLS required" }));
  yield* requireIsolation(config).pipe(Effect.provideService(Platform, platform));
  const token = yield* platform.privateFile(config.tokenFile);
  if (token.length < 32 || token.length > 480) return yield* Effect.fail(new CommandError({ message: "Invalid credential length" }));
  if (config.controlTokenHash === (yield* platform.digest(token))) return yield* Effect.fail(new CommandError({ message: "Distinct enrollment and control credentials required" }));
  // Effect FileSystem has no process umask API; preserve the private workspace default.
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
  const children = new Map<TaskId, OwnedNativeChild>(), admissions = new Map<TaskId, RunnerAdmission>();
  const reportLock = yield* Semaphore.make(1);
  const lifecycleLock = yield* Semaphore.make(1);
  const fatal = yield* Deferred.make<never, CommandError>();
  let closed = false, ownershipLost = false, heartbeatBusy = false, reportsBusy = false;
  const owned = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.suspend<A, E | CommandError, R>(() =>
    ownershipLost ? Deferred.await(fatal) : effect.pipe(Effect.raceFirst(Deferred.await(fatal))));
  const ownership = yield* platform.acquireJournalOwnership(join(config.stateDirectory, "runner.lock")).pipe(Effect.withSpan("Runner.acquireJournalOwnership"));
  yield* ownership.exitCode.pipe(Effect.catch(() => Effect.succeed(undefined)), Effect.andThen(Effect.gen(function*() {
    closed = true; ownershipLost = true;
    const error = new CommandError({ message: "Kernel journal ownership lost" });
    for (const child of children.values()) child.lost = true;
    yield* Deferred.fail(fatal, error);
    for (const child of children.values()) {
      yield* Scope.close(child.scope, Exit.void);
    }
  })), Effect.catchCause(() => Effect.void), Effect.forkScoped({ startImmediately: true }), Effect.provideService(Scope.Scope, ownerScope));
  const postgres = yield* Postgres;
  const sql = postgres.sql, db = yield* Database;
  yield* postgres.failure.pipe(Effect.catch((error) => Effect.gen(function*() {
    closed = true; ownershipLost = true;
    yield* Deferred.fail(fatal, new CommandError({ message: `PostgreSQL ownership lost (${error._tag})` }));
  })), Effect.forkScoped({ startImmediately: true }), Effect.provideService(Scope.Scope, ownerScope));
  // uint32 UIDs are exactly representable as float8, including bases above signed int4.
  const taskRows = db.client.selectFrom("runner_tasks").selectAll().select((eb) => eb(eb.fn<number>("float8", [eb.val(config.childUidBase)]), expression<UserId>`+`, eb.ref("sequence")).as("childUid"));
  const initialize = Effect.fn("Runner.initialize")(function*(): Effect.fn.Return<void, unknown> {
    yield* assertRunnerSchema(sql);
    // ponytail: no stdio reattachment exists; uncertain owned children reserve their slot until operator reconciliation.
    yield* db.execute(db.client.updateTable("runner_tasks").set({ state: "HELD", reason: "restart: launch or prompt outcome unknown" }).where("occupied", "=", 1).where("state", "!=", "ACCEPTED").returning("taskId"));
    for (const row of yield* db.execute(taskRows.where("occupied", "=", 1).where("pid", "is not", null))) {
      // Effect ChildProcessSpawner can inspect owned handles only; no arbitrary recorded-PID liveness API exists.
      try { process.kill(row.pid!, 0); }
      catch (error) {
        if (record(error) && error.code === "ESRCH") yield* db.execute(db.client.updateTable("runner_tasks").set({ occupied: 0, reason: "restart: child gone; outcome remains unknown" }).where("taskId", "=", row.taskId).returning("taskId"));
      }
    }
  });
  const getRow = Effect.fn("Runner.row")(function*(taskId: TaskId): Effect.fn.Return<RunnerRow | undefined, unknown> { return (yield* db.execute(taskRows.where("taskId", "=", taskId)))[0]; });
  const requireRow = Effect.fn("Runner.requireRow")(function*(taskId: TaskId): Effect.fn.Return<RunnerRow, unknown> { const row = (yield* getRow(taskId)); if (!row) return yield* new HttpError({ status: 404, message: "Task not accepted" }); return row; });
  const taskWork = (row: RunnerRow): Work => { return Schema.decodeUnknownSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(row.work); };
  const acceptance = (row: RunnerRow): Acceptance => {
    return { taskId: row.taskId, accepted: true, state: row.state, worktree: row.worktree,
      ...(row.address ? { address: Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address) } : {}), ...(row.reason ? { reason: row.reason } : {}) };
  };
  const load = Effect.fn("Runner.load")(function*(): Effect.fn.Return<number, unknown> { return Number((yield* db.execute(db.client.selectFrom("runner_tasks").select((eb) => eb.fn.countAll<bigint>().as("count")).where("occupied", "=", 1)))[0]!.count); });
  const event = Effect.fn("Runner.event")(function*(taskId: TaskId, value: RunnerEvent): Effect.fn.Return<void, unknown> {
    const payload = Schema.encodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })({ ...value, contentOmitted: true });
    yield* db.execute(db.client.insertInto("runner_events").values({ taskId, payload }).returning("seq"));
    yield* db.execute(db.client.deleteFrom("runner_events").where("taskId", "=", taskId).where("seq", "not in", db.client.selectFrom("runner_events").select("seq").where("taskId", "=", taskId).orderBy("seq", "desc").limit(RETAIN_EVENTS)).returning("seq"));
  }, owned);
  const state = Effect.fn("Runner.state")(function*(taskId: TaskId, state: NonNullable<RunnerEvent["state"]>, reason: string | null = null, result: RunnerResult = null): Effect.fn.Return<void, unknown> {
    yield* db.execute(db.client.updateTable("runner_tasks").set({ state, reason, result: result === null ? null : Schema.encodeUnknownSync(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })(result) }).where("taskId", "=", taskId).returning("taskId"));
    (yield* event(taskId, { type: "runner_state", state }));
  }, owned);
  const handle = Effect.fn("Runner.handle")(function*(request: RunnerRequest, controlToken: string): Effect.fn.Return<CommandOutput<RunnerRequest["method"]>, unknown> {
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
        const rows = yield* db.execute(db.client.selectFrom("runner_events").select(["seq", "payload"]).where("taskId", "=", taskId).where("seq", ">", RunnerEventsId.make(cursor)).orderBy("seq").limit(2));
        const oldest = (yield* db.execute(db.client.selectFrom("runner_events").select("seq").where("taskId", "=", taskId).orderBy("seq").limit(1)))[0]?.seq;
        const events: RunnerEvent[] = [];
        let nextCursor = cursor;
        if (typeof oldest === "number" && cursor < oldest - 1) events.push({ type: "gap", reason: "retention or cursor gap", before: oldest });
        for (const event of rows) {
          const value = Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload));
          events.push(value); nextCursor = Number(event.seq);
          if (new TextEncoder().encode(inspectionText({ result: { ...acceptance(row), events, nextCursor } })).byteLength > MCP_RESULT_LIMIT) {
            events.pop(); events.push({ type: "gap", reason: "event exceeds remaining response bound", seq: Number(event.seq) }); break;
          }
        }
        return { ...acceptance(row), events, nextCursor };
      }),
      commandTask: Effect.fnUntraced(function*(input: CommandInput<"command">): Effect.fn.Return<CommandOutput<"command">, unknown> {
        const { taskId, type } = input;
        yield* requireRow(taskId);
        const child = yield* requireChild(taskId);
        const response = type === "get_state"
          ? projectState(yield* child.client.call("native.get_state", { binding: child.client.binding }, { timeoutMs: 30000 }))
          : type === "get_messages"
            ? yield* child.client.call("native.get_messages", { binding: child.client.binding, ...(input.type === "get_messages" && input.cursor !== undefined ? { cursor: input.cursor } : {}), ...(input.type === "get_messages" && input.limit !== undefined ? { limit: input.limit } : {}) }, { timeoutMs: 30000 })
            : yield* child.client.call("native.abort", { binding: child.client.binding }, { timeoutMs: 30000 });
        if (!sameBinding(response.binding, child.client.binding)) {
          child.lost = true;
          yield* hold(taskId, "native control response owner generation changed");
          return yield* new CommandError({ message: "Native control response binding mismatch" });
        }
        if (type === "abort") yield* hold(taskId, "operator requested abort; await host outcome");
        return byteLength(nativeResponseText({ result: response })) <= MCP_RESULT_LIMIT ? response : { type: "gap", reason: "native response exceeds endpoint bound" };
      }),
      prepareTask: Effect.fnUntraced(function*(input: CommandInput<"prepare">) {
        const row = yield* requireRow(input.taskId);
        if (row.state !== "READY" || row.promptId || !row.address) return yield* new HttpError({ status: 409, message: "Corrected readback requires unprompted live owned child" });
        const prior = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address);
        if (input.address.revision <= prior.revision || input.address.sessionId !== prior.sessionId || input.address.incarnation !== prior.incarnation) return yield* new HttpError({ status: 409, message: "Correction revision or child binding mismatch" });
        const child = yield* requireChild(row.taskId);
        const actual = yield* readback(row, yield* child.client.call("native.get_state", { binding: child.client.binding }, { timeoutMs: 30000 }), input.address);
        if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Corrected consequential readback mismatch" });
        if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
        yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ address: Schema.encodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(actual) }).where("taskId", "=", row.taskId).where("state", "=", "READY").where("promptId", "is", null).returning("taskId")));
        yield* event(row.taskId, { type: "readback", address: actual }); return actual;
      }),
      releaseTask: release,
    };
    switch (request.method) {
      case "acceptTask": return yield* Commands.acceptTask.execute(capabilities, request.params);
      case "journal": return yield* Commands.journal.execute(capabilities, request.params);
      case "inspect": return yield* Commands.inspect.execute(capabilities, request.params);
      case "command": return yield* Commands.command.execute(capabilities, request.params);
      case "prepare": return yield* Commands.prepare.execute(capabilities, request.params);
      case "release": return yield* Commands.release.execute(capabilities, request.params);
      default: {
        const exhaustive: never = request;
        throw new Error(`Unsupported runner request: ${exhaustive}`);
      }
    }
  }, Effect.catch((error) => Effect.fail(error instanceof HttpError ? error : new HttpError({ status: ownershipLost ? 503 : 400, message: ownershipLost ? "Runner journal ownership unavailable" : "Invalid request or unavailable owned child" }))));
  const brokerTask = Effect.fn("Runner.brokerTask")(function*(work: Work, address?: Address): Effect.fn.Return<Task, unknown> {
    const task = yield* rpc.call(config.brokerUrl, token, "task.get", { taskId: work.taskId });
    if (task.taskId !== work.taskId || task.runnerId !== config.id || task.fingerprint !== (yield* platform.fingerprint(work)) || (yield* platform.fingerprint(task.request)) !== (yield* platform.fingerprint(work))) return yield* new HttpError({ status: 403, message: "Broker task binding mismatch" });
    if (address && (!task.address || !task.readback || task.disposition !== "ready" || task.confirmedRevision !== address.revision ||
      (yield* platform.fingerprint(task.address)) !== (yield* platform.fingerprint(address)) || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)))) return yield* new HttpError({ status: 409, message: "Broker confirmation or current readback mismatch" });
    return task;
  });
  const accept = Effect.fn("Runner.accept")(function*(work: Work): Effect.fn.Return<Acceptance, unknown> {
    if (byteLength(Schema.encodeUnknownSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work)) > LIMIT - 1024) return yield* new HttpError({ status: 413, message: "Work too large" });
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
    const incarnation = IncarnationId.make(yield* platform.crypto.randomUUIDv4());
    const reserved = yield* owned(sql.withTransaction(Effect.gen(function*() {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      const existing = yield* getRow(work.taskId);
      if (existing) {
        if (existing.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" });
        return acceptance(existing);
      }
      if ((yield* load()) >= config.slots) return { taskId: work.taskId, accepted: false, state: "full", reason: "full" };
      if (work.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Assignment expired before durable acceptance" });
      yield* db.execute(db.client.insertInto("runner_tasks").values({ taskId: work.taskId, fingerprint: hash, work: Schema.encodeUnknownSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work), worktree, state: "ACCEPTED", occupied: 1, incarnation }).returning("taskId"));
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
  const requireChild = Effect.fnUntraced(function*(taskId: TaskId) {
    const child = children.get(taskId);
    if (!child || child.lost) return yield* new CommandError({ message: "Owned native MCP connection unavailable" });
    const row = yield* requireRow(taskId);
    if (row.nativeBinding === null || row.nativeBinding !== bindingText(child.client.binding)) return yield* new CommandError({ message: "Native owner generation changed" });
    return child;
  });
  const readback = Effect.fn("Runner.readback")(function*(row: RunnerRow, data: NativeState, template?: Address): Effect.fn.Return<Address, unknown> {
    const work = taskWork(row), child = yield* requireChild(row.taskId);
    if (!sameBinding(data.binding, child.client.binding) || data.binding.host !== work.launcher || typeof data.sessionFile !== "string" || data.cwd !== row.worktree ||
      data.currentRunId !== null || data.isStreaming || data.isCompacting !== false || data.pendingMessageCount !== 0 || data.pendingAsyncWorkCount !== 0) return yield* new CommandError({ message: "Unverified idle durable native identity" });
    const sessions = yield* fileSystem.realPath(join(config.childDirectory, (yield* platform.digest(row.taskId)), "sessions"));
    if (!isAbsolute(data.sessionFile) || !inside(sessions, resolve(data.sessionFile)) || (yield* fileSystem.realPath(data.sessionFile)) !== data.sessionFile) return yield* new CommandError({ message: "Unexpected session path" });
    const shell = yield* child.client.call("native.bash", { binding: child.client.binding, command: "pwd -P && git remote && git remote get-url --all origin && git rev-parse HEAD && git rev-parse --verify " + "'" + work.ref.replaceAll("'", "'\\''") + "^{commit}'" }, { timeoutMs: 30000 });
    const lines = shell.output.trim().split("\n");
    if (!sameBinding(shell.binding, data.binding) || shell.exitCode !== 0 || shell.truncated || shell.killed || lines.length !== 5 || lines[0] !== row.worktree || lines[1] !== "origin" || platform.canonicalRemote(lines[2]!) !== work.repo || lines[3] !== work.sha || lines[4] !== work.sha) return yield* new CommandError({ message: "Native cwd or checkout identity mismatch" });
    yield* verifyCheckout(row);
    return { messageId: template?.messageId ?? MessageId.make(row.taskId, { disableChecks: true }), revision: template?.revision ?? 1, workerId: WorkerId.make(data.binding.sessionId, { disableChecks: true }), sessionId: SessionId.make(data.binding.sessionId, { disableChecks: true }),
      incarnation: row.incarnation, repo: work.repo, ref: work.ref, sha: work.sha,
      scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  });
  const projectState = (data: NativeState): typeof RunnerNativeState.Type => ({
    binding: data.binding, ...(data.sessionFile === undefined ? {} : { sessionFile: data.sessionFile }), cwd: data.cwd,
    currentRunId: data.currentRunId, isStreaming: data.isStreaming, isCompacting: data.isCompacting,
    pendingMessageCount: data.pendingMessageCount, pendingAsyncWorkCount: data.pendingAsyncWorkCount, messageCount: data.messageCount,
  });
  const lifecycle = Effect.fn("Runner.lifecycle")(function*(taskId: TaskId, child: OwnedNativeChild, value: NativeLifecycleEvent): Effect.fn.Return<void, unknown> {
    // The native journal owns lifecycle evidence while database serving is fenced.
    if (ownershipLost) return;
    const row = yield* requireRow(taskId);
    if (child.lost || closed || row.nativeBinding !== bindingText(value.binding) || !sameBinding(value.binding, child.client.binding)) return yield* new CommandError({ message: "Stale native lifecycle owner" });
    if (value.seq <= row.nativeCursor) return;
    if (value.seq !== row.nativeCursor + 1) return yield* new CommandError({ message: "Native lifecycle cursor gap" });
    yield* owned(sql.withTransaction(Effect.gen(function*() {
      yield* db.execute(db.client.updateTable("runner_tasks").set({ nativeCursor: value.seq }).where("taskId", "=", taskId).where("nativeBinding", "=", row.nativeBinding).where("nativeCursor", "=", row.nativeCursor).returning("taskId"));
      yield* event(taskId, { type: "native_lifecycle", binding: value.binding, runId: value.runId, nativeCursor: value.seq, kind: value.kind, ...(value.kind === "run_settled" ? { status: value.outcome } : {}) });
    })));
    if (value.kind === "session_retired") {
      child.lost = true;
      if (!Terminal[row.state]) yield* hold(taskId, "native owner generation retired; no replay");
    }
  }, (effect) => lifecycleLock.withPermit(effect).pipe(Effect.catchCause((cause) => ownershipLost ? Effect.void : Effect.failCause(cause))));
  const release = Effect.fn("Runner.release")(function*(input: RunnerRelease): Effect.fn.Return<RunnerReleased, unknown> {
    const row = (yield* requireRow(input.taskId)); const work = taskWork(row);
    if (!row.address || (yield* platform.fingerprint(input.address)) !== (yield* platform.fingerprint(Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address))) || input.prompt !== work.prompt) return yield* new HttpError({ status: 409, message: "Release address or approved prompt mismatch" });
    if (row.promptId) {
      if (row.state === "RUNNING" || Terminal[row.state]) return { released: true };
      return yield* new HttpError({ status: 409, message: "Prompt delivery uncertain; never replay" });
    }
    if (row.state !== "READY") return yield* new HttpError({ status: 409, message: "Child not ready" });
    if (row.detached) return yield* new HttpError({ status: 403, message: "Task no longer assigned" });
    yield* brokerTask(work, input.address);
    const child = yield* requireChild(row.taskId);
    const response = yield* child.client.call("native.get_state", { binding: child.client.binding }, { timeoutMs: 30000 });
    const actual = yield* readback(yield* requireRow(row.taskId), response, Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address));
    if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Actual child address changed" });
    // Native readback awaited RPCs; recheck the ledger immediately before reserving prompt intent.
    yield* brokerTask(work, input.address);
    if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
    const promptId = NativeSubmissionId.make(yield* platform.crypto.randomUUIDv4());
    yield* owned(sql.withTransaction(Effect.gen(function*() {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
      const reserved = yield* db.execute(db.client.updateTable("runner_tasks").set({ state: "PROMPT_INTENT", promptId }).where("taskId", "=", row.taskId).where("state", "=", "READY").where("promptId", "is", null).returning("taskId"));
      if (reserved.length !== 1) return yield* new HttpError({ status: 409, message: "Prompt already reserved" });
      yield* event(row.taskId, { type: "prompt_intent", id: promptId, digest: yield* platform.digest(work.prompt) });
    })));
    const admitted = yield* Deferred.make<void, CommandError>();
    const proof: { admission?: Extract<NativeLifecycleEvent, { kind: "submission_admitted" }>; terminal?: Extract<NativeLifecycleEvent, { kind: "run_settled" }> } = {};
    const cursor = (yield* requireRow(row.taskId)).nativeCursor;
    const request = Effect.gen(function*() {
      const remaining = input.address.deadline - (yield* Clock.currentTimeMillis);
      if (remaining <= 0 || child.lost || closed) return yield* new CommandError({ message: "Approved deadline or native ownership unavailable before send" });
      const result: NativePromptResult = yield* child.client.prompt({ binding: child.client.binding, submissionId: promptId, message: work.prompt }, {
        timeoutMs: remaining, after: cursor,
        onLifecycle: async (value) => {
          if (value.kind === "submission_admitted" && value.submissionId === promptId) proof.admission = value;
          if (value.kind === "run_settled") proof.terminal = value;
          await Effect.runPromise(lifecycle(row.taskId, child, value));
        },
        onAdmission: async (value: NativePromptAdmission) => {
          if (ownershipLost) return;
          await Effect.runPromise(owned(sql.withTransaction(Effect.gen(function*() {
            const observed = proof.admission;
            if (!observed || child.lost || closed || input.address.deadline <= (yield* Clock.currentTimeMillis) || !sameBinding(value.binding, child.client.binding) || value.submissionId !== promptId || value.runId === null ||
              observed.seq !== value.seq || observed.runId !== value.runId || observed.disposition !== value.disposition || !sameBinding(observed.binding, value.binding)) return yield* new CommandError({ message: "Whole run admission not proven by native lifecycle" });
            const changed = yield* db.execute(db.client.updateTable("runner_tasks").set((eb) => ({ state: "RUNNING", nativeRunId: value.runId, nativeCursor: eb.fn<NativeEventSequence>("greatest", [eb.ref("nativeCursor"), eb.val(value.seq)]) })).where("taskId", "=", row.taskId).where("state", "=", "PROMPT_INTENT").where("promptId", "=", promptId).where("nativeBinding", "=", bindingText(value.binding)).returning("taskId"));
            if (changed.length !== 1) return yield* new CommandError({ message: "Durable native prompt admission fence changed" });
          }))));
          await Effect.runPromise(Deferred.succeed(admitted, undefined));
        },
      });
      if (ownershipLost) {
        yield* fork(Scope.close(child.scope, Exit.void));
        return;
      }
      yield* owned(sql.withTransaction(Effect.gen(function*() {
        const current = yield* requireRow(row.taskId), terminal = proof.terminal, admission = proof.admission;
        if (child.lost || closed || current.state !== "RUNNING" || current.promptId !== promptId || current.nativeBinding !== bindingText(child.client.binding) || current.address !== row.address || current.incarnation !== row.incarnation ||
          result.disposition !== "settled" || !terminal || !admission || !sameBinding(result.final.binding, child.client.binding) || !sameBinding(terminal.binding, result.final.binding) ||
          terminal.runId !== current.nativeRunId || result.final.runId !== current.nativeRunId || terminal.outcome !== result.final.outcome ||
          !sameBinding(result.admission.binding, admission.binding) || result.admission.seq !== admission.seq || result.admission.disposition !== admission.disposition ||
          result.admission.submissionId !== promptId || result.admission.runId !== current.nativeRunId || terminal.seq <= result.admission.seq || terminal.seq > current.nativeCursor) return yield* new CommandError({ message: "Whole run terminal result or durable generation fence unproven" });
        const nextState = terminal.outcome === "completed" ? "SUCCEEDED" : terminal.outcome === "error" ? "FAILED" : "HELD";
        const changed = yield* db.execute(db.client.updateTable("runner_tasks").set({
          state: nextState, reason: terminal.outcome === "aborted" ? "native run aborted; not completed" : null,
          result: Schema.encodeUnknownSync(Schema.fromJsonString(RunnerResult))({ hostSettled: true, evidenceVerified: false }),
        }).where("taskId", "=", row.taskId).where("state", "=", "RUNNING").where("promptId", "=", promptId).where("nativeBinding", "=", bindingText(child.client.binding)).where("nativeRunId", "=", terminal.runId).where("address", "=", row.address).where("incarnation", "=", row.incarnation).returning("taskId"));
        if (changed.length !== 1) return yield* new CommandError({ message: "Whole run terminal journal fence changed" });
        yield* event(row.taskId, { type: "runner_state", state: nextState });
      })));
      yield* fork(heartbeat());
      yield* fork(Scope.close(child.scope, Exit.void));
    }).pipe(Effect.catchCause(() => Effect.gen(function*() {
      yield* hold(row.taskId, "prompt admission or whole run outcome uncertain; never replay");
      yield* Deferred.fail(admitted, new CommandError({ message: "Prompt admission uncertain; held" }));
    })));
    yield* request.pipe(Effect.forkScoped, Effect.provideService(Scope.Scope, child.scope));
    yield* Deferred.await(admitted).pipe(Effect.timeout(Math.min(30000, input.address.deadline - (yield* Clock.currentTimeMillis))), Effect.catch(() => Effect.gen(function*() {
      yield* hold(row.taskId, "prompt admission deadline exceeded or uncertain; never replay");
      return yield* new HttpError({ status: 409, message: "Prompt admission uncertain; held" });
    })));
    yield* fork(heartbeat());
    return { released: true };
  });
  const heartbeat = Effect.fn("Runner.heartbeat")(function*() {
    if (closed || heartbeatBusy) return;
    heartbeatBusy = true;
    yield* Effect.gen(function*() {
      const rows = yield* db.execute(taskRows.where("detached", "=", 0).orderBy("taskId"));
      const journal: Acceptance[] = []; let size = 1024;
      for (const row of rows) { const item = acceptance(row); const bytes = byteLength(Schema.encodeUnknownSync(Schema.fromJsonString(Acceptance), { onExcessProperty: "error" })(item)); if (size + bytes > LIMIT - 1024) break; journal.push(item); size += bytes; }
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
        if (row.address && row.state === "READY") yield* rpc.call(config.brokerUrl, token, "runner.readback", { runnerId: config.id, taskId: row.taskId, address: Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address) });
        if (row.state === "RUNNING" || row.state === "SUCCEEDED" || row.state === "FAILED" || row.state === "HELD") yield* rpc.call(config.brokerUrl, token, "runner.report", { runnerId: config.id, taskId: row.taskId, state: row.state, result: row.result ? Schema.decodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(row.result) : {} });
      }).pipe(Effect.catch((error) => Effect.gen(function*() {
        if (!(error instanceof HttpError) || error.status !== 403) return;
        // Read the typed error rather than treating a network failure or report conflict as stale ownership.
        const obsolete = yield* rpc.call(config.brokerUrl, token, "task.get", { taskId: row.taskId }).pipe(Effect.map(() => false), Effect.catch((bindingError) => Effect.succeed(bindingError instanceof HttpError && bindingError.status === 403)));
        if (!obsolete || closed) return;
        if (!Terminal[row.state]) yield* hold(row.taskId, "broker assignment withdrawn; no replay");
        yield* owned(db.execute(db.client.updateTable("runner_tasks").set((eb) => ({ detached: 1, occupied: eb.case().when(eb.and([eb("state", "=", "HELD"), eb("pid", "is", null)])).then(0).else(eb.ref("occupied")).end() })).where("taskId", "=", row.taskId).returning("taskId")));
      })));
    })).pipe(Effect.asVoid, Effect.ensuring(Effect.sync(() => { reportsBusy = false; })));
  }, (effect) => reportLock.withPermit(effect));

  const hold = Effect.fn("Runner.hold")(function*(taskId: TaskId, reason: string): Effect.fn.Return<void, unknown> {
    // A close/abort race must not erase an already proven whole-run terminal result.
    const changed = yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ state: "HELD", reason, result: null }).where("taskId", "=", taskId).where("state", "not in", ["SUCCEEDED", "FAILED"]).returning("taskId")));
    if (changed.length) yield* event(taskId, { type: "runner_state", state: "HELD" });
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
  const schedule = Effect.fnUntraced(function*(taskId: TaskId) { if (!closed) yield* fork(launch(taskId)); });


  const launch = Effect.fn("Runner.launch")(function*(taskId: TaskId) {
    let launchIntent = false;
    let childScope: Scope.Closeable | undefined;
    yield* Effect.gen(function*() {
      const row = yield* requireRow(taskId);
      if (row.state !== "ACCEPTED" || row.detached || children.has(taskId)) return;
      const work = taskWork(row), repo = config.repositories[work.repo]!;
      if (row.childUid >= config.childUidBase + CHILD_UID_BASE) return yield* Effect.fail(new CommandError({ message: "Reserved child UID range exhausted" }));
      if (work.deadline <= (yield* Clock.currentTimeMillis)) {
        yield* state(taskId, "FAILED", "Approved deadline elapsed before launch", { launched: false, evidenceVerified: false });
        yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).returning("taskId")));
        yield* fork(heartbeat()); return;
      }
      const childRoot = join(config.childDirectory, yield* platform.digest(taskId));
      yield* fileSystem.makeDirectory(childRoot, { recursive: true, mode: 0o700 }).pipe(Effect.withSpan("Runner.launch.childRoot"));
      if (Option.isNone(yield* Effect.option(fileSystem.stat(join(row.worktree, ".git"))))) {
        yield* git(repo, ["clone", "--no-hardlinks", "--no-checkout", repo, row.worktree]).pipe(Effect.withSpan("Runner.launch.clone"));
        yield* git(row.worktree, ["remote", "set-url", "origin", yield* git(repo, ["remote", "get-url", "origin"])]).pipe(Effect.withSpan("Runner.launch.remote"));
        yield* git(row.worktree, ["checkout", "--detach", work.sha]).pipe(Effect.withSpan("Runner.launch.checkout"));
        yield* git(row.worktree, ["fetch", "--no-tags", repo, "+refs/heads/*:refs/heads/*"]).pipe(Effect.withSpan("Runner.launch.fetch"));
      }
      yield* fileSystem.chmod(row.worktree, 0o700).pipe(Effect.withSpan("Runner.launch.worktreeMode"));
      const sessions = join(childRoot, "sessions"), home = join(childRoot, "home");
      yield* fileSystem.makeDirectory(sessions, { recursive: true, mode: 0o700 }).pipe(Effect.withSpan("Runner.launch.sessions"));
      yield* fileSystem.makeDirectory(home, { recursive: true, mode: 0o700 }).pipe(Effect.withSpan("Runner.launch.home"));
      yield* fileSystem.copy(config.modelDirectory, home, { overwrite: true }).pipe(Effect.withSpan("Runner.launch.modelSeed"));
      yield* ownChildDirectory(childRoot, row.childUid).pipe(Effect.provideService(Platform, platform));
      yield* verifyCheckout(row);
      yield* state(taskId, "LAUNCHING").pipe(Effect.withSpan("Runner.launch.intent")); // Persisted intent precedes spawn; an uncertain launch is never replayed.
      launchIntent = true;
      childScope = yield* Scope.fork(ownerScope).pipe(Effect.withSpan("Runner.launch.scope"));
      const nativeConfig = join(childRoot, "native-plugin.json"), endpointFile = join(childRoot, "native-endpoint.json"), nativeTokenFile = join(childRoot, "native.token");
      const nativeToken = Hex.encode(yield* platform.crypto.randomBytes(32));
      yield* fileSystem.writeFileString(nativeTokenFile, nativeToken, { flag: "wx", mode: 0o600 });
      yield* fileSystem.writeFileString(nativeConfig, Schema.encodeSync(Schema.fromJsonString(NativePluginConfig))({ host: work.launcher, endpointFile, tokenFile: nativeTokenFile }), { flag: "wx", mode: 0o600 });
      const env: Record<string, string> = { HOME: home, COORDINATOR_NATIVE_CONFIG: nativeConfig };
      for (const [key, value] of Object.entries(nativeEnvironment)) if (value !== undefined) env[key] = value;
      yield* fileSystem.chown(nativeTokenFile, row.childUid, row.childUid);
      yield* fileSystem.chown(nativeConfig, row.childUid, row.childUid);
      const client = yield* connectNativeClient({
        host: work.launcher, command: "/usr/bin/env",
        args: [`--chdir=${row.worktree}`, "/usr/bin/setpriv", ...isolationArgs(row.childUid), config.launchers[work.launcher]!, "--mode", "rpc", "--session-dir", sessions, "--no-extensions", "-e", fileURLToPath(new URL("../dist/plugins/native-plugin.js", import.meta.url))],
        env, cwd: config.stateDirectory, connectTimeoutMs: 60000, plugin: { endpointFile, token: nativeToken },
      }).pipe(Effect.provideService(Scope.Scope, childScope), Effect.withSpan("Runner.launch.connect"));
      const childPid = yield* Schema.decodeEffect(ProcessId)(client.pid);
      const child: OwnedNativeChild = { client, scope: childScope, lost: false };
      children.set(taskId, child);
      yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ pid: childPid, nativeBinding: bindingText(client.binding), nativeCursor: NativeEventSequence.make(0) }).where("taskId", "=", taskId).where("state", "=", "LAUNCHING").returning("taskId"))).pipe(Effect.withSpan("Runner.launch.persistBinding"));
      if (closed || ownershipLost) {
        yield* Scope.close(childScope, Exit.void);
        return yield* new CommandError({ message: "Runner journal ownership unavailable" });
      }
      yield* Scope.addFinalizer(child.scope, Effect.gen(function*() {
        const row = yield* getRow(taskId);
        if (row && !Terminal[row.state]) yield* hold(taskId, "child scope closed: outcome unknown; no replay");
      }).pipe(Effect.catchCause(() => Effect.void)));
      // Only an observed physical SDK process close releases the ownership slot.
      yield* fork(Effect.gen(function*() {
        yield* client.observedProcessClosed.pipe(Effect.withSpan("Runner.launch.observeProcessClosed"));
        if (ownershipLost) {
          children.delete(taskId);
          return;
        }
        const current = yield* requireRow(taskId);
        if (!Terminal[current.state]) yield* hold(taskId, "owned native child closed before confirmed whole run completion");
        yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).where("pid", "=", childPid).returning("taskId")));
        children.delete(taskId);
        yield* Scope.close(child.scope, Exit.void).pipe(Effect.withSpan("Runner.launch.closePhysicalScope"));
        yield* heartbeat();
      }).pipe(Effect.catch(() => hold(taskId, "physical child close outcome uncertain"))));
      const watch = yield* client.watchLifecycle({ binding: client.binding, after: NativeEventSequence.make(0) }, (value) => Effect.runPromise(lifecycle(taskId, child, value))).pipe(Effect.provideService(Scope.Scope, child.scope), Effect.withSpan("Runner.launch.watchLifecycle"));
      // Registered after acquisition: consume typed observer loss durably before the SDK fallback finalizer.
      yield* Scope.addFinalizer(child.scope, watch.close.pipe(
        Effect.catch((error) => hold(taskId, `native MCP lifecycle observer close failed (${error.code}); physical child ownership retained`)),
        Effect.orDie,
      ));
      yield* watch.connectionClosed.pipe(Effect.catch((failure) => Effect.gen(function*() {
        child.lost = true;
        // Own the failed observer's typed close before its scoped fallback can turn it into a defect.
        // This releases the subscription only; the process and occupied slot remain owned.
        yield* watch.close.pipe(Effect.matchEffect({
          onFailure: (error) => hold(taskId, `native MCP lifecycle observer close failed (${error.code}); physical child ownership retained`),
          onSuccess: () => hold(taskId, `native MCP lifecycle connection lost (${failure.code}); physical child ownership retained`),
        }));
      })), Effect.forkScoped, Effect.provideService(Scope.Scope, child.scope));
      const response = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 60000 }).pipe(Effect.withSpan("Runner.launch.getState"));
      if (!client.capabilities.includes("native.prompt")) {
        yield* hold(taskId, `${work.launcher} public extension hooks cannot prove prompt admission and final outcome; no prompt sent`);
        yield* fork(heartbeat());
        return;
      }
      const address = yield* readback(yield* requireRow(taskId), response).pipe(Effect.withSpan("Runner.launch.readback"));
      if (closed) return yield* Effect.fail(new CommandError({ message: "Runner journal ownership unavailable" }));
      yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ address: Schema.encodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(address), state: "READY", reason: null }).where("taskId", "=", taskId).where("state", "=", "LAUNCHING").returning("taskId"))).pipe(Effect.withSpan("Runner.launch.ready"));
      yield* event(taskId, { type: "readback", address });
      yield* fork(heartbeat());
    }).pipe(Effect.withSpan("Runner.launch.acquire"), Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.gen(function*() {
      // Fixed ownership metadata only; retain the failed acquisition span before holding uncertain intent.
      yield* Effect.annotateCurrentSpan({ launchIntent, ownershipLost, closed, hasChildScope: childScope !== undefined, hasOwnedChild: children.has(taskId) });
      if (launchIntent) {
        yield* hold(taskId, "launch/readback uncertain; no automatic relaunch");
        if (childScope && !children.has(taskId)) {
          // Failed discovery has no registered observer; scoped SDK cleanup still owns physical closure.
          const closure = yield* Scope.close(childScope, Exit.void).pipe(Effect.exit);
          if (closure._tag === "Success" && !ownershipLost) {
            yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).where("state", "=", "HELD").where("pid", "is", null).returning("taskId")));
            yield* fork(heartbeat());
          } else if (closure._tag === "Failure") yield* hold(taskId, "native MCP acquisition cleanup failed; physical child ownership retained");
        }
      }
      else {
        if (childScope) yield* Scope.close(childScope, Exit.void);
        yield* state(taskId, "FAILED", "allowlisted checkout preparation failed");
        yield* owned(db.execute(db.client.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).returning("taskId")));
      }
    })));
  });


  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    closed = true;
    for (const [taskId, child] of children) {
      child.lost = true;
      if (!ownershipLost) {
        const row = yield* getRow(taskId);
        if (row && !Terminal[row.state]) yield* hold(taskId, "runner shutdown: child outcome unknown; no replay");
      }
      yield* Scope.close(child.scope, Exit.void).pipe(Effect.withSpan("Runner.shutdown.closeChildScope"));
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
  const live = Effect.void;
  // ponytail: one outstanding probe; a stalled database must not accumulate queries.
  let pendingReadiness: Deferred.Deferred<void, HttpError> | undefined;
  const ready = Effect.suspend(() => closed ? Effect.fail(new HttpError({ status: 503, message: "Runner ownership unavailable" })) : Effect.void).pipe(Effect.andThen(Effect.gen(function*() {
    let probe = pendingReadiness;
    if (!probe) {
      probe = yield* Deferred.make<void, HttpError>();
      pendingReadiness = probe;
      const completion = probe;
      yield* owned(assertRunnerSchema(sql)).pipe(
        Effect.asVoid,
        Effect.catchCause(() => Effect.fail(new HttpError({ status: 503, message: "Runner database not ready" }))),
        Effect.exit,
        Effect.flatMap((exit) => Deferred.done(completion, exit)),
        Effect.ensuring(Effect.sync(() => { pendingReadiness = undefined; })),
        Effect.forkScoped({ startImmediately: true }),
        Effect.provideService(Scope.Scope, ownerScope),
      );
    }
    yield* Deferred.await(probe).pipe(Effect.interruptible, Effect.timeout("1 second"), Effect.mapError(() => new HttpError({ status: 503, message: "Runner database not ready" })));
  })));
  yield* owned(rpc.serve(server, { target: "runner", authorize, handle: (bearer, request) => handle(request, bearer) }, { live, ready }));
  yield* owned(Effect.gen(function*() {
    for (const row of yield* db.execute(taskRows.where("state", "=", "ACCEPTED").where("occupied", "=", 1).where("detached", "=", 0))) yield* schedule(row.taskId);
    yield* fork(Effect.forever(heartbeat().pipe(Effect.andThen(Effect.sleep("5 seconds")))));
  }));
  if (ownershipLost) return yield* Deferred.await(fatal);
  return { config, sql, db, server, failure: Deferred.await(fatal), authorize, handle, row: getRow, state, event, reports, heartbeat, launch };
});

export class Runner extends Context.Service<Runner>()("coordinator/Runner", {
  make: Effect.gen(function*() { return yield* makeRunner(yield* RunnerConfiguration); })
}) {
  static layer(config: RunnerConfig) { return Layer.effect(Runner, Runner.make).pipe(Layer.provide(Layer.merge(Layer.succeed(RunnerConfiguration, config), Postgres.layer(config.databaseUrlFile)))); }
}
