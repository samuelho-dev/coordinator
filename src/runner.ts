import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Option, Scope, Schema, Semaphore, Schedule, type PlatformError } from "effect";
import { Hex } from "effect/encoding";
import { fileURLToPath } from "node:url";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import type { Kysely } from "kysely";
import { Address, Work, RunnerConfig, type RunnerRow, RunnerEvent, RunnerResult, RunnerAgentState, WorkerId, UserId, Acceptance, HttpError, CommandError, Commands, RunnerCommand, RunnerControlEnum, RunnerRejectionStateEnum, RunnerReportStates, DispositionEnum, type CommandInput, type CommandOutput, type RunnerRelease, type RunnerReleased, RunnerRequest, type RunnerAdmission } from "./contracts.ts";
import { Platform, redactedCause } from "./platform.ts";
import { Mcp, serverLayer, LIMIT, MCP_RESULT_LIMIT } from "./mcp.ts";
import { assertRunnerSchema } from "./database.ts";
import { awaitDatabaseLoss, DatabaseService, type DatabaseError } from "./database-service.ts";
import { RunnerEventId, PeerId, SessionId, RunnerTask, type TaskId, type DB } from "./generated/database/types.ts";
import { RunnerStateEnum, TaskStateEnum } from "./generated/database/enums.ts";
import { connectAgentClient, type AgentClient } from "./agent/client.ts";
import { AgentTools, AgentRunOutcomeEnum, AgentBinding, AgentConfig, AgentSubmissionId, AgentEventSequence, AgentState, type AgentLifecycleEvent, type AgentPromptAdmission, type AgentPromptResult } from "./agent/contracts.ts";

type OwnedAgentChild = { client: AgentClient; scope: Scope.Closeable; lost: boolean };
const bindingText = Schema.encodeSync(Schema.fromJsonString(AgentBinding), { onExcessProperty: "error" });
const sameBinding = (left: AgentBinding, right: AgentBinding) => left.host === right.host && left.sessionId === right.sessionId && left.incarnation === right.incarnation && left.generation === right.generation;

const RETAIN_EVENTS = 128;
const missingProcess = Schema.Struct({ code: Schema.Literal("ESRCH") });
type RunnerAdmissionError = DatabaseError | Schema.SchemaError | HttpError | CommandError | PlatformError.PlatformError | Cause.TimeoutError;
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
export const ownChildDirectory = Effect.fn("Runner.ownChildDirectory")(function*(path: string, uid: UserId): Effect.fn.Return<void, PlatformError.PlatformError, Platform> {
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
  const checkSeed = Effect.fnUntraced(function*(directory: string): Effect.fn.Return<void, PlatformError.PlatformError | CommandError> {
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
const inspectionText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.inspectTask.structuredOutput)), { onExcessProperty: "error" });
const agentResponseText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.commandTask.structuredOutput)), { onExcessProperty: "error" });

class RunnerConfiguration extends Context.Service<RunnerConfiguration, RunnerConfig>()("coordinator/RunnerConfiguration") {}

const makeRunner = Effect.fn("Runner.make")(function*(input: RunnerConfig) {
  const platform = yield* Platform, rpc = yield* Mcp;
  const config = input;
  const { fs: fileSystem, path: paths } = platform;
  const agentEnvironment = yield* childEnvironment();
  const { isAbsolute, join, relative, resolve, sep } = paths;
  const inside = (root: string, path: string) => { const r = relative(root, path); return r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
  const git = Effect.fnUntraced(function*(cwd: string, args: string[]) { return (yield* platform.exec(ChildProcess.make("git", args, { cwd, env: agentEnvironment, extendEnv: false }))).trim(); });
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
  const fork = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(
    Effect.tapCause((cause) => Effect.logError(`runner background failure: ${redactedCause(cause)}`)),
    Effect.forkScoped, Effect.provideService(Scope.Scope, ownerScope), Effect.asVoid,
  );
  const children = new Map<TaskId, OwnedAgentChild>(), admissions = new Map<TaskId, RunnerAdmission<RunnerAdmissionError>>();
  const reportLock = yield* Semaphore.make(1);
  const lifecycleLock = yield* Semaphore.make(1);
  const fatal = yield* Deferred.make<never, CommandError>();
  let closed = false, ownershipLost = false, heartbeatBusy = false, reportsBusy = false;
  const owned = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.suspend<A, E | CommandError, R>(() =>
    ownershipLost ? Deferred.await(fatal) : effect.pipe(Effect.raceFirst(Deferred.await(fatal))));
  const ownership = yield* platform.acquireJournalOwnership(join(config.stateDirectory, "runner.lock")).pipe(Effect.withSpan("Runner.acquireJournalOwnership"));
  yield* ownership.exitCode.pipe(Effect.exit, Effect.andThen(Effect.gen(function*() {
    closed = true; ownershipLost = true;
    const error = new CommandError({ message: "Kernel journal ownership lost" });
    for (const child of children.values()) child.lost = true;
    yield* Deferred.fail(fatal, error);
    for (const child of children.values()) {
      yield* Scope.close(child.scope, Exit.void);
    }
  })), Effect.forkScoped({ startImmediately: true }), Effect.provideService(Scope.Scope, ownerScope));
  const database = yield* DatabaseService;
  yield* awaitDatabaseLoss(database).pipe(Effect.catch((error) => Effect.gen(function*() {
    closed = true; ownershipLost = true;
    yield* Deferred.fail(fatal, new CommandError({ message: `PostgreSQL ownership lost (${error._tag})` }));
  })), Effect.forkScoped({ startImmediately: true }), Effect.provideService(Scope.Scope, ownerScope));
  // uint32 UIDs are exactly representable as float8, including bases above signed int4.
  const taskRows = (db: Kysely<DB>) => db.selectFrom("runner_tasks").selectAll().select((eb) => eb.fn<UserId>("float8", [eb(eb.cast(eb.val(config.childUidBase), "float8"), "+", eb.ref("sequence"))]).as("childUid"));
  const initialize = Effect.fn("Runner.initialize")(function*() {
    yield* assertRunnerSchema(database);
    // ponytail: no stdio reattachment exists; uncertain owned children reserve their slot until operator reconciliation.
    yield* database.query((db) => db.updateTable("runner_tasks").set({ state: RunnerStateEnum.HELD, reason: "restart: launch or prompt outcome unknown" }).where("occupied", "=", 1).where("state", "!=", RunnerStateEnum.ACCEPTED).returning("taskId"));
    for (const row of yield* database.query((db) => taskRows(db).where("occupied", "=", 1).where("pid", "is not", null))) {
      // Effect ChildProcessSpawner can inspect owned handles only; no arbitrary recorded-PID liveness API exists.
      try { process.kill(row.pid!, 0); }
      catch (error) {
        if (Option.isSome(Schema.decodeUnknownOption(missingProcess)(error))) yield* database.query((db) => db.updateTable("runner_tasks").set({ occupied: 0, reason: "restart: child gone; outcome remains unknown" }).where("taskId", "=", row.taskId).returning("taskId"));
      }
    }
  });
  const getRow = Effect.fn("Runner.row")(function*(taskId: TaskId) { return (yield* database.query((db) => taskRows(db).where("taskId", "=", taskId)))[0]; });
  const requireRow = Effect.fn("Runner.requireRow")(function*(taskId: TaskId) { const row = (yield* getRow(taskId)); if (!row) return yield* new HttpError({ status: 404, message: "Task not accepted" }); return row; });
  const taskWork = (row: RunnerRow) => Schema.decodeEffect(Schema.fromJsonString(Work), { onExcessProperty: "error" })(row.work);
  const taskAddress = (row: RunnerRow) => row.address === null ? Effect.succeed(undefined) : Schema.decodeEffect(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address);
  const acceptance = Effect.fnUntraced(function*(row: RunnerRow) {
    const address = yield* taskAddress(row);
    const accepted: Extract<Acceptance, { readonly accepted: true }> = { taskId: row.taskId, accepted: true, state: row.state, worktree: row.worktree,
      ...(address ? { address } : {}), ...(row.reason ? { reason: row.reason } : {}) };
    return accepted;
  });
  const load = Effect.fn("Runner.load")(function*() { return Number((yield* database.query((db) => db.selectFrom("runner_tasks").select((eb) => eb.fn.countAll<bigint>().as("count")).where("occupied", "=", 1)))[0]!.count); });
  const event = Effect.fn("Runner.event")(function*(taskId: TaskId, value: RunnerEvent) {
    const payload = Schema.encodeSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })({ ...value, contentOmitted: true });
    yield* database.query((db) => db.insertInto("runner_events").values({ taskId, payload }).returning("seq"));
    yield* database.query((db) => db.deleteFrom("runner_events").where("taskId", "=", taskId).where("seq", "not in", db.selectFrom("runner_events").select("seq").where("taskId", "=", taskId).orderBy("seq", "desc").limit(RETAIN_EVENTS)).returning("seq"));
  }, owned);
  const state = Effect.fn("Runner.state")(function*(taskId: TaskId, state: RunnerStateEnum, reason: string | null = null, result: RunnerResult = null) {
    yield* database.query((db) => db.updateTable("runner_tasks").set({ state, reason, result: result === null ? null : Schema.encodeSync(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })(result) }).where("taskId", "=", taskId).returning("taskId"));
    (yield* event(taskId, { type: "runner_state", state }));
  }, owned);
  const handle = Effect.fn("Runner.handle")(function*(request: RunnerRequest, controlToken: string) {
    if (!(yield* platform.checkToken(controlToken, config.controlTokenHash))) return yield* new HttpError({ status: 401, message: "Control credential required" });
    if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
    // Capabilities are composed only after checking the trusted control credential.
    const capabilities = {
      acceptTask: admit,
      journalTask: Effect.fnUntraced(function*({ taskId }: CommandInput<typeof Commands.journalTask>) {
        if (admissions.has(taskId)) return yield* new HttpError({ status: 503, message: "Acceptance pending; ownership unknown" });
        const row = yield* getRow(taskId);
        const result: Acceptance = row ? yield* acceptance(row) : { taskId, accepted: false, state: RunnerRejectionStateEnum.notAccepted };
        return result;
      }),
      inspectTask: Effect.fnUntraced(function*({ taskId, cursor = 0 }: CommandInput<typeof Commands.inspectTask>) {
        const row = yield* requireRow(taskId), accepted = yield* acceptance(row);
        const rows = yield* database.query((db) => db.selectFrom("runner_events").select(["seq", "payload"]).where("taskId", "=", taskId).where("seq", ">", RunnerEventId.make(cursor)).orderBy("seq").limit(2));
        const oldest = (yield* database.query((db) => db.selectFrom("runner_events").select("seq").where("taskId", "=", taskId).orderBy("seq").limit(1)))[0]?.seq;
        const events: RunnerEvent[] = [];
        let nextCursor = cursor;
        if (typeof oldest === "number" && cursor < oldest - 1) events.push({ type: "gap", reason: "retention or cursor gap", before: oldest });
        for (const event of rows) {
          const value = yield* Schema.decodeEffect(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(event.payload);
          events.push(value); nextCursor = Number(event.seq);
          if (new TextEncoder().encode(inspectionText({ result: { ...accepted, events, nextCursor } })).byteLength > MCP_RESULT_LIMIT) {
            events.pop(); events.push({ type: "gap", reason: "event exceeds remaining response bound", seq: Number(event.seq) }); break;
          }
        }
        return { ...accepted, events, nextCursor };
      }),
      commandTask: Effect.fnUntraced(function*(input: CommandInput<typeof Commands.commandTask>) {
        const { taskId, type } = input;
        yield* requireRow(taskId);
        const child = yield* requireChild(taskId);
        const response = yield* RunnerCommand.match(input, {
          [RunnerControlEnum.getState]: () => child.client.call(AgentTools.getState, { binding: child.client.binding }, { timeoutMs: 30000 }).pipe(Effect.map(projectState)),
          [RunnerControlEnum.getMessages]: ({ cursor, limit }) => child.client.call(AgentTools.getMessages, { binding: child.client.binding, ...(cursor === undefined ? {} : { cursor }), ...(limit === undefined ? {} : { limit }) }, { timeoutMs: 30000 }),
          [RunnerControlEnum.abort]: () => child.client.call(AgentTools.abort, { binding: child.client.binding }, { timeoutMs: 30000 }),
        });
        if (!sameBinding(response.binding, child.client.binding)) {
          child.lost = true;
          yield* hold(taskId, "agent control response owner generation changed");
          return yield* new CommandError({ message: "Agent control response binding mismatch" });
        }
        if (type === RunnerControlEnum.abort) yield* hold(taskId, "operator requested abort; await host outcome");
        const bounded: CommandOutput<typeof Commands.commandTask> = byteLength(agentResponseText({ result: response })) <= MCP_RESULT_LIMIT ? response : { type: "gap", reason: "agent response exceeds endpoint bound" };
        return bounded;
      }),
      prepareTask: Effect.fnUntraced(function*(input: CommandInput<typeof Commands.prepareTask>) {
        const row = yield* requireRow(input.taskId);
        if (row.state !== RunnerStateEnum.READY || row.promptId || !row.address) return yield* new HttpError({ status: 409, message: "Corrected readback requires unprompted live owned child" });
        const prior = yield* taskAddress(row);
        if (!prior || input.address.revision <= prior.revision || input.address.sessionId !== prior.sessionId || input.address.incarnation !== prior.incarnation) return yield* new HttpError({ status: 409, message: "Correction revision or child binding mismatch" });
        const child = yield* requireChild(row.taskId);
        const actual = yield* readback(row, yield* child.client.call(AgentTools.getState, { binding: child.client.binding }, { timeoutMs: 30000 }), input.address);
        if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Corrected consequential readback mismatch" });
        if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
        yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ address: Schema.encodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(actual) }).where("taskId", "=", row.taskId).where("state", "=", RunnerStateEnum.READY).where("promptId", "is", null).returning("taskId")));
        yield* event(row.taskId, { type: "readback", address: actual }); return actual;
      }),
      releaseTask: release,
    };
    return yield* RunnerRequest.match(request, {
      [Commands.acceptTask.name]: ({ params }: typeof Commands.acceptTask.request.Type) => Commands.acceptTask.executeDecoded(capabilities, params),
      [Commands.journalTask.name]: ({ params }: typeof Commands.journalTask.request.Type) => Commands.journalTask.executeDecoded(capabilities, params),
      [Commands.inspectTask.name]: ({ params }: typeof Commands.inspectTask.request.Type) => Commands.inspectTask.executeDecoded(capabilities, params),
      [Commands.commandTask.name]: ({ params }: typeof Commands.commandTask.request.Type) => Commands.commandTask.executeDecoded(capabilities, params),
      [Commands.prepareTask.name]: ({ params }: typeof Commands.prepareTask.request.Type) => Commands.prepareTask.executeDecoded(capabilities, params),
      [Commands.releaseTask.name]: ({ params }: typeof Commands.releaseTask.request.Type) => Commands.releaseTask.executeDecoded(capabilities, params),
    });
  }, Effect.catch((error) => Effect.fail(error._tag === "HttpError" ? error : new HttpError({ status: ownershipLost ? 503 : 400, message: ownershipLost ? "Runner journal ownership unavailable" : "Invalid request or unavailable owned child" }))));
  const brokerTask = Effect.fn("Runner.brokerTask")(function*(work: Work, address?: Address) {
    const task = yield* rpc.call(config.brokerUrl, token, Commands.getTask, { taskId: work.taskId });
    if (task.taskId !== work.taskId || task.runnerId !== config.id || task.fingerprint !== (yield* platform.fingerprint(work)) || (yield* platform.fingerprint(task.request)) !== (yield* platform.fingerprint(work))) return yield* new HttpError({ status: 403, message: "Broker task binding mismatch" });
    if (address && (!task.address || !task.readback || task.disposition !== DispositionEnum.ready || task.confirmedRevision !== address.revision ||
      (yield* platform.fingerprint(task.address)) !== (yield* platform.fingerprint(address)) || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)))) return yield* new HttpError({ status: 409, message: "Broker confirmation or current readback mismatch" });
    return task;
  });
  const accept = Effect.fn("Runner.accept")(function*(work: Work): Effect.fn.Return<Acceptance, RunnerAdmissionError> {
    if (byteLength(Schema.encodeSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work)) > LIMIT - 1024) return yield* new HttpError({ status: 413, message: "Work too large" });
    const existing = (yield* getRow(work.taskId)); const hash = (yield* platform.fingerprint(work));
    if (existing) { if (existing.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return yield* acceptance(existing); }
    if ((yield* load()) >= config.slots) return { taskId: work.taskId, accepted: false, state: RunnerRejectionStateEnum.full, reason: "full" };
    yield* brokerTask(work);
    const concurrent = (yield* getRow(work.taskId));
    if (concurrent) { if (concurrent.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return yield* acceptance(concurrent); }
    const repo = config.repositories[work.repo]; const launcher = config.launchers[work.launcher];
    if (!repo || !launcher) return yield* new HttpError({ status: 403, message: "Repository or launcher not allowlisted" });
    if (!work.ref || work.ref.startsWith("-") || /[\r\n\0]/.test(work.ref)) return yield* new HttpError({ status: 400, message: "Invalid ref" });
    if ((yield* git(repo, ["rev-parse", "--verify", `${work.ref}^{commit}`])) !== work.sha || (yield* git(repo, ["rev-parse", "--verify", `${work.sha}^{commit}`])) !== work.sha) return yield* new HttpError({ status: 409, message: "Exact ref/SHA unavailable" });
    const assigned = yield* brokerTask(work);
    if (work.deadline <= (yield* Clock.currentTimeMillis) || assigned.state !== TaskStateEnum.ASSIGNING && assigned.state !== TaskStateEnum.ACCEPTED) return yield* new HttpError({ status: 409, message: "Assignment expired or no longer dispatchable" });
    const accepted = (yield* getRow(work.taskId));
    if (accepted) { if (accepted.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" }); return yield* acceptance(accepted); }
    const worktree = join(config.childDirectory, (yield* platform.digest(work.taskId)), "repo");
    const incarnation = RunnerTask.fields.incarnation.make(yield* platform.crypto.randomUUIDv4());
    const reserved = yield* owned(database.transaction(Effect.gen(function*(): Effect.fn.Return<Acceptance, DatabaseError | Schema.SchemaError | HttpError> {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      const existing = yield* getRow(work.taskId);
      if (existing) {
        if (existing.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" });
        return yield* acceptance(existing);
      }
      if ((yield* load()) >= config.slots) return { taskId: work.taskId, accepted: false, state: RunnerRejectionStateEnum.full, reason: "full" };
      if (work.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Assignment expired before durable acceptance" });
      yield* database.query((db) => db.insertInto("runner_tasks").values({ taskId: work.taskId, fingerprint: hash, work: Schema.encodeSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work), worktree, state: RunnerStateEnum.ACCEPTED, occupied: 1, incarnation }).returning("taskId"));
      return yield* acceptance(yield* requireRow(work.taskId));
    })));
    if (!reserved.accepted) return reserved;
    (yield* event(work.taskId, { type: "accepted", fingerprint: hash }));
    (yield* schedule(work.taskId));
    return yield* acceptance((yield* requireRow(work.taskId)));
  });
  const verifyCheckout = Effect.fn("Runner.verifyCheckout")(function*(row: RunnerRow) {
    const work = yield* taskWork(row);
    if ((yield* fileSystem.realPath(row.worktree)) !== row.worktree || !inside(yield* fileSystem.realPath(config.childDirectory), row.worktree)) return yield* Effect.fail(new CommandError({ message: "Worktree escaped child root" }));
    // Effect's cwd access preflight checks real UID; env chdir honors DAC_OVERRIDE before setpriv drops privileges.
    const inspect = Effect.fnUntraced(function*(args: string[]) { return (yield* platform.exec(ChildProcess.make("/usr/bin/env", [`--chdir=${row.worktree}`, "/usr/bin/setpriv", ...isolationArgs(row.childUid), "git", ...args], { env: agentEnvironment, extendEnv: false }))).trim(); });
    if ((yield* inspect(["remote"])) !== "origin" || platform.canonicalRemote(yield* inspect(["remote", "get-url", "--all", "origin"])) !== work.repo || (yield* inspect(["rev-parse", "HEAD"])) !== work.sha || (yield* inspect(["rev-parse", "--verify", `${work.ref}^{commit}`])) !== work.sha) return yield* Effect.fail(new CommandError({ message: "Checkout identity mismatch" }));
  });
  const requireChild = Effect.fnUntraced(function*(taskId: TaskId) {
    const child = children.get(taskId);
    if (!child || child.lost) return yield* new CommandError({ message: "Owned agent MCP connection unavailable" });
    const row = yield* requireRow(taskId);
    if (row.agentBinding === null || row.agentBinding !== bindingText(child.client.binding)) return yield* new CommandError({ message: "Agent owner generation changed" });
    return child;
  });
  const readback = Effect.fn("Runner.readback")(function*(row: RunnerRow, data: AgentState, template?: Address) {
    const work = yield* taskWork(row), child = yield* requireChild(row.taskId);
    if (!sameBinding(data.binding, child.client.binding) || data.binding.host !== work.launcher || typeof data.sessionFile !== "string" || data.cwd !== row.worktree ||
      data.currentRunId !== null || data.isStreaming || data.isCompacting !== false || data.pendingMessageCount !== 0 || data.pendingAsyncWorkCount !== 0) return yield* new CommandError({ message: "Unverified idle durable agent identity" });
    const sessions = yield* fileSystem.realPath(join(config.childDirectory, (yield* platform.digest(row.taskId)), "sessions"));
    if (!isAbsolute(data.sessionFile) || !inside(sessions, resolve(data.sessionFile)) || (yield* fileSystem.realPath(data.sessionFile)) !== data.sessionFile) return yield* new CommandError({ message: "Unexpected session path" });
    const shell = yield* child.client.call(AgentTools.bash, { binding: child.client.binding, command: "pwd -P && git remote && git remote get-url --all origin && git rev-parse HEAD && git rev-parse --verify " + "'" + work.ref.replaceAll("'", "'\\''") + "^{commit}'" }, { timeoutMs: 30000 });
    const lines = shell.output.trim().split("\n");
    if (!sameBinding(shell.binding, data.binding) || shell.exitCode !== 0 || shell.truncated || shell.killed || lines.length !== 5 || lines[0] !== row.worktree || lines[1] !== "origin" || platform.canonicalRemote(lines[2]!) !== work.repo || lines[3] !== work.sha || lines[4] !== work.sha) return yield* new CommandError({ message: "Agent cwd or checkout identity mismatch" });
    yield* verifyCheckout(row);
    return { messageId: template?.messageId ?? PeerId.make(row.taskId), revision: template?.revision ?? 1, workerId: WorkerId.make(data.binding.sessionId), sessionId: SessionId.make(data.binding.sessionId),
      incarnation: row.incarnation, repo: work.repo, ref: work.ref, sha: work.sha,
      scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  });
  const projectState = (data: AgentState): typeof RunnerAgentState.Type => ({
    binding: data.binding, ...(data.sessionFile === undefined ? {} : { sessionFile: data.sessionFile }), cwd: data.cwd,
    currentRunId: data.currentRunId, isStreaming: data.isStreaming, isCompacting: data.isCompacting,
    pendingMessageCount: data.pendingMessageCount, pendingAsyncWorkCount: data.pendingAsyncWorkCount, messageCount: data.messageCount,
  });
  const lifecycle = Effect.fn("Runner.lifecycle")(function*(taskId: TaskId, child: OwnedAgentChild, value: AgentLifecycleEvent) {
    // The agent journal owns lifecycle evidence while database serving is fenced.
    if (ownershipLost) return;
    const row = yield* requireRow(taskId);
    if (child.lost || closed || row.agentBinding !== bindingText(value.binding) || !sameBinding(value.binding, child.client.binding)) return yield* new CommandError({ message: "Stale agent lifecycle owner" });
    if (value.seq <= row.agentCursor) return;
    if (value.seq !== row.agentCursor + 1) return yield* new CommandError({ message: "Agent lifecycle cursor gap" });
    yield* owned(database.transaction(Effect.gen(function*() {
      yield* database.query((db) => db.updateTable("runner_tasks").set({ agentCursor: value.seq }).where("taskId", "=", taskId).where("agentBinding", "=", row.agentBinding).where("agentCursor", "=", row.agentCursor).returning("taskId"));
      yield* event(taskId, { type: "agent_lifecycle", binding: value.binding, runId: value.runId, agentCursor: value.seq, kind: value.kind, ...(value.kind === "run_settled" ? { status: value.outcome } : {}) });
    })));
    if (value.kind === "session_retired") {
      child.lost = true;
      if (!(row.state === RunnerStateEnum.SUCCEEDED || row.state === RunnerStateEnum.FAILED)) yield* hold(taskId, "agent owner generation retired; no replay");
    }
  }, (effect) => lifecycleLock.withPermit(effect).pipe(Effect.catchCause((cause) => ownershipLost ? Effect.void : Effect.failCause(cause))));
  const release = Effect.fn("Runner.release")(function*(input: RunnerRelease) {
    const released: RunnerReleased = { released: true };
    const row = (yield* requireRow(input.taskId)); const work = yield* taskWork(row), address = yield* taskAddress(row);
    if (!address || (yield* platform.fingerprint(input.address)) !== (yield* platform.fingerprint(address)) || input.prompt !== work.prompt) return yield* new HttpError({ status: 409, message: "Release address or approved prompt mismatch" });
    if (row.promptId) {
      if (row.state === RunnerStateEnum.RUNNING || (row.state === RunnerStateEnum.SUCCEEDED || row.state === RunnerStateEnum.FAILED)) return released;
      return yield* new HttpError({ status: 409, message: "Prompt delivery uncertain; never replay" });
    }
    if (row.state !== RunnerStateEnum.READY) return yield* new HttpError({ status: 409, message: "Child not ready" });
    if (row.detached) return yield* new HttpError({ status: 403, message: "Task no longer assigned" });
    yield* brokerTask(work, input.address);
    const child = yield* requireChild(row.taskId);
    const response = yield* child.client.call(AgentTools.getState, { binding: child.client.binding }, { timeoutMs: 30000 });
    const actual = yield* readback(yield* requireRow(row.taskId), response, address);
    if ((yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(input.address))) return yield* new HttpError({ status: 409, message: "Actual child address changed" });
    // Agent readback awaited RPCs; recheck the ledger immediately before reserving prompt intent.
    yield* brokerTask(work, input.address);
    if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
    const promptId = AgentSubmissionId.make(yield* platform.crypto.randomUUIDv4());
    yield* owned(database.transaction(Effect.gen(function*() {
      if (closed) return yield* new HttpError({ status: 503, message: "Runner journal ownership unavailable" });
      if (input.address.deadline <= (yield* Clock.currentTimeMillis)) return yield* new HttpError({ status: 409, message: "Approved prompt deadline elapsed" });
      const reserved = yield* database.query((db) => db.updateTable("runner_tasks").set({ state: RunnerStateEnum.PROMPT_INTENT, promptId }).where("taskId", "=", row.taskId).where("state", "=", RunnerStateEnum.READY).where("promptId", "is", null).returning("taskId"));
      if (reserved.length !== 1) return yield* new HttpError({ status: 409, message: "Prompt already reserved" });
      yield* event(row.taskId, { type: "prompt_intent", id: promptId, digest: yield* platform.digest(work.prompt) });
    })));
    const admitted = yield* Deferred.make<void, CommandError>();
    const proof: { admission?: Extract<AgentLifecycleEvent, { kind: "submission_admitted" }>; terminal?: Extract<AgentLifecycleEvent, { kind: "run_settled" }> } = {};
    const cursor = (yield* requireRow(row.taskId)).agentCursor;
    const request = Effect.gen(function*() {
      const remaining = input.address.deadline - (yield* Clock.currentTimeMillis);
      if (remaining <= 0 || child.lost || closed) return yield* new CommandError({ message: "Approved deadline or agent ownership unavailable before send" });
      const result: AgentPromptResult = yield* child.client.prompt({ binding: child.client.binding, submissionId: promptId, message: work.prompt }, {
        timeoutMs: remaining, after: cursor,
        onLifecycle: async (value) => {
          if (value.kind === "submission_admitted" && value.submissionId === promptId) proof.admission = value;
          if (value.kind === "run_settled") proof.terminal = value;
          await Effect.runPromise(lifecycle(row.taskId, child, value));
        },
        onAdmission: async (value: AgentPromptAdmission) => {
          if (ownershipLost) return;
          await Effect.runPromise(owned(database.transaction(Effect.gen(function*() {
            const observed = proof.admission;
            if (!observed || child.lost || closed || input.address.deadline <= (yield* Clock.currentTimeMillis) || !sameBinding(value.binding, child.client.binding) || value.submissionId !== promptId || value.runId === null ||
              observed.seq !== value.seq || observed.runId !== value.runId || observed.disposition !== value.disposition || !sameBinding(observed.binding, value.binding)) return yield* new CommandError({ message: "Whole run admission not proven by agent lifecycle" });
            const changed = yield* database.query((db) => db.updateTable("runner_tasks").set((eb) => ({ state: RunnerStateEnum.RUNNING, agentRunId: value.runId, agentCursor: eb.fn<AgentEventSequence>("greatest", [eb.ref("agentCursor"), eb.val(value.seq)]) })).where("taskId", "=", row.taskId).where("state", "=", RunnerStateEnum.PROMPT_INTENT).where("promptId", "=", promptId).where("agentBinding", "=", bindingText(value.binding)).returning("taskId"));
            if (changed.length !== 1) return yield* new CommandError({ message: "Durable agent prompt admission fence changed" });
          }))));
          await Effect.runPromise(Deferred.succeed(admitted, undefined));
        },
      });
      if (ownershipLost) {
        yield* fork(Scope.close(child.scope, Exit.void));
        return;
      }
      yield* owned(database.transaction(Effect.gen(function*() {
        const current = yield* requireRow(row.taskId), terminal = proof.terminal, admission = proof.admission;
        if (child.lost || closed || current.state !== RunnerStateEnum.RUNNING || current.promptId !== promptId || current.agentBinding !== bindingText(child.client.binding) || current.address !== row.address || current.incarnation !== row.incarnation ||
          result.disposition !== "settled" || !terminal || !admission || !sameBinding(result.final.binding, child.client.binding) || !sameBinding(terminal.binding, result.final.binding) ||
          terminal.runId !== current.agentRunId || result.final.runId !== current.agentRunId || terminal.outcome !== result.final.outcome ||
          !sameBinding(result.admission.binding, admission.binding) || result.admission.seq !== admission.seq || result.admission.disposition !== admission.disposition ||
          result.admission.submissionId !== promptId || result.admission.runId !== current.agentRunId || terminal.seq <= result.admission.seq || terminal.seq > current.agentCursor) return yield* new CommandError({ message: "Whole run terminal result or durable generation fence unproven" });
        const nextState = terminal.outcome === AgentRunOutcomeEnum.completed ? RunnerStateEnum.SUCCEEDED : terminal.outcome === AgentRunOutcomeEnum.error ? RunnerStateEnum.FAILED : RunnerStateEnum.HELD;
        const changed = yield* database.query((db) => db.updateTable("runner_tasks").set({
          state: nextState, reason: terminal.outcome === AgentRunOutcomeEnum.aborted ? "agent run aborted; not completed" : null,
          result: Schema.encodeSync(Schema.fromJsonString(RunnerResult))({ hostSettled: true, evidenceVerified: false }),
        }).where("taskId", "=", row.taskId).where("state", "=", RunnerStateEnum.RUNNING).where("promptId", "=", promptId).where("agentBinding", "=", bindingText(child.client.binding)).where("agentRunId", "=", terminal.runId).where("address", "=", row.address).where("incarnation", "=", row.incarnation).returning("taskId"));
        if (changed.length !== 1) return yield* new CommandError({ message: "Whole run terminal journal fence changed" });
        yield* event(row.taskId, { type: "runner_state", state: nextState });
      })));
      yield* fork(heartbeat());
      yield* fork(Scope.close(child.scope, Exit.void));
    }).pipe(Effect.catchCause((cause) => Effect.gen(function*() {
      yield* hold(row.taskId, "prompt admission or whole run outcome uncertain; never replay");
      yield* Deferred.fail(admitted, new CommandError({ message: "Prompt admission uncertain; held" }));
      return yield* Effect.failCause(cause);
    })));
    yield* request.pipe(Effect.forkScoped, Effect.provideService(Scope.Scope, child.scope));
    yield* Deferred.await(admitted).pipe(Effect.timeout(Math.min(30000, input.address.deadline - (yield* Clock.currentTimeMillis))), Effect.catch(() => Effect.gen(function*() {
      yield* hold(row.taskId, "prompt admission deadline exceeded or uncertain; never replay");
      return yield* new HttpError({ status: 409, message: "Prompt admission uncertain; held" });
    })));
    yield* fork(heartbeat());
    return released;
  });
  const heartbeat = Effect.fn("Runner.heartbeat")(function*(): Effect.fn.Return<void, DatabaseError | Schema.SchemaError | HttpError | CommandError | Cause.TimeoutError> {
    if (closed || heartbeatBusy) return;
    heartbeatBusy = true;
    yield* Effect.gen(function*() {
      const rows = yield* database.query((db) => taskRows(db).where("detached", "=", 0).orderBy("taskId"));
      const journal: Acceptance[] = []; let size = 1024;
      for (const row of rows) { const item = yield* acceptance(row); const bytes = byteLength(Schema.encodeSync(Schema.fromJsonString(Acceptance), { onExcessProperty: "error" })(item)); if (size + bytes > LIMIT - 1024) break; journal.push(item); size += bytes; }
      yield* rpc.call(config.brokerUrl, token, Commands.heartbeatRunner, { runnerId: config.id, slots: config.slots, load: yield* load(), journal }, 4000);
      if (!reportsBusy) yield* fork(reports(rows).pipe(Effect.flatMap((outcomes) => Effect.forEach(outcomes, Effect.fromResult, { discard: true }))));
    }).pipe(Effect.ensuring(Effect.sync(() => { heartbeatBusy = false; })));
    // Broker outages never interrupt agent work or replay uncertain prompt sends.
  });
  const reports = Effect.fn("Runner.reports")(function*(rows: readonly RunnerRow[]) {
    reportsBusy = true;
    return yield* Effect.forEach(rows, (value) => Effect.gen(function*() {
      const row = yield* getRow(value.taskId);
      if (!row || row.detached) return;
      yield* Effect.gen(function*() {
        const address = yield* taskAddress(row);
        if (address && row.state === RunnerStateEnum.READY) yield* rpc.call(config.brokerUrl, token, Commands.readbackRunner, { runnerId: config.id, taskId: row.taskId, address });
        const reportState = RunnerReportStates[row.state];
        if (reportState !== undefined) yield* rpc.call(config.brokerUrl, token, Commands.reportRunner, { runnerId: config.id, taskId: row.taskId, state: reportState, result: row.result ? (yield* Schema.decodeEffect(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })(row.result)) ?? {} : {} });
      }).pipe(Effect.catch((error) => Effect.gen(function*() {
        if (error._tag !== "HttpError" || error.status !== 403) return yield* Effect.fail(error);
        // Read the typed error rather than treating a network failure or report conflict as stale ownership.
        const obsolete = yield* rpc.call(config.brokerUrl, token, Commands.getTask, { taskId: row.taskId }).pipe(Effect.map(() => false), Effect.catchTag("HttpError", (bindingError) => bindingError.status === 403 ? Effect.succeed(true) : Effect.fail(bindingError)));
        if (!obsolete || closed) return;
        if (!(row.state === RunnerStateEnum.SUCCEEDED || row.state === RunnerStateEnum.FAILED)) yield* hold(row.taskId, "broker assignment withdrawn; no replay");
        yield* owned(database.query((db) => db.updateTable("runner_tasks").set((eb) => ({ detached: 1, occupied: eb.case().when(eb.and([eb("state", "=", RunnerStateEnum.HELD), eb("pid", "is", null)])).then(0).else(eb.ref("occupied")).end() })).where("taskId", "=", row.taskId).returning("taskId")));
      })));
    }).pipe(Effect.result)).pipe(Effect.ensuring(Effect.sync(() => { reportsBusy = false; })));
  }, (effect) => reportLock.withPermit(effect));

  const hold = Effect.fn("Runner.hold")(function*(taskId: TaskId, reason: string) {
    // After losing journal ownership, only the agent journal may record new evidence.
    if (ownershipLost) return;
    // A close/abort race must not erase an already proven whole-run terminal result.
    const changed = yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ state: RunnerStateEnum.HELD, reason, result: null }).where("taskId", "=", taskId).where("state", "not in", [RunnerStateEnum.SUCCEEDED, RunnerStateEnum.FAILED]).returning("taskId")));
    if (changed.length) yield* event(taskId, { type: "runner_state", state: RunnerStateEnum.HELD });
  });
  const admit = Effect.fn("Runner.admit")(function*(work: Work) {
    const hash = yield* platform.fingerprint(work);
    const pending = admissions.get(work.taskId);
    if (pending) {
      if (pending.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Task fingerprint conflict" });
      return yield* Deferred.await(pending.response);
    }
    const response = yield* Deferred.make<Acceptance, RunnerAdmissionError>();
    admissions.set(work.taskId, { fingerprint: hash, response });
    yield* fork(accept(work).pipe(Effect.exit, Effect.flatMap((exit) => Deferred.done(response, exit)), Effect.ensuring(Effect.sync(() => admissions.delete(work.taskId)))));
    return yield* Deferred.await(response);
  });
  const schedule = Effect.fnUntraced(function*(taskId: TaskId): Effect.fn.Return<void> { if (!closed) yield* fork(launch(taskId)); });


  const launch = Effect.fn("Runner.launch")(function*(taskId: TaskId) {
    let launchIntent = false;
    let childScope: Scope.Closeable | undefined;
    yield* Effect.gen(function*() {
      const row = yield* requireRow(taskId);
      if (row.state !== RunnerStateEnum.ACCEPTED || row.detached || children.has(taskId)) return;
      const work = yield* taskWork(row), repo = config.repositories[work.repo]!;
      if (row.childUid >= config.childUidBase + CHILD_UID_BASE) return yield* Effect.fail(new CommandError({ message: "Reserved child UID range exhausted" }));
      if (work.deadline <= (yield* Clock.currentTimeMillis)) {
        yield* state(taskId, RunnerStateEnum.FAILED, "Approved deadline elapsed before launch", { launched: false, evidenceVerified: false });
        yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).returning("taskId")));
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
      yield* state(taskId, RunnerStateEnum.LAUNCHING).pipe(Effect.withSpan("Runner.launch.intent")); // Persisted intent precedes spawn; an uncertain launch is never replayed.
      launchIntent = true;
      childScope = yield* Scope.fork(ownerScope).pipe(Effect.withSpan("Runner.launch.scope"));
      const agentConfig = join(childRoot, "agent-config.json"), endpointFile = join(childRoot, "agent-endpoint.json"), agentTokenFile = join(childRoot, "agent.token");
      const agentToken = Hex.encode(yield* platform.crypto.randomBytes(32));
      yield* fileSystem.writeFileString(agentTokenFile, agentToken, { flag: "wx", mode: 0o600 });
      yield* fileSystem.writeFileString(agentConfig, Schema.encodeSync(Schema.fromJsonString(AgentConfig))({ host: work.launcher, endpointFile, tokenFile: agentTokenFile }), { flag: "wx", mode: 0o600 });
      const env: Record<string, string> = { HOME: home, COORDINATOR_AGENT_CONFIG: agentConfig };
      for (const [key, value] of Object.entries(agentEnvironment)) if (value !== undefined) env[key] = value;
      yield* fileSystem.chown(agentTokenFile, row.childUid, row.childUid);
      yield* fileSystem.chown(agentConfig, row.childUid, row.childUid);
      const client = yield* connectAgentClient({
        host: work.launcher, command: "/usr/bin/env",
        args: [`--chdir=${row.worktree}`, "/usr/bin/setpriv", ...isolationArgs(row.childUid), config.launchers[work.launcher]!, "--mode", "rpc", "--session-dir", sessions, "--no-extensions", "-e", fileURLToPath(new URL("../dist/plugins/runner-plugin.js", import.meta.url))],
        env, cwd: config.stateDirectory, connectTimeoutMs: 60000, plugin: { endpointFile, token: agentToken },
      }).pipe(Effect.provideService(Scope.Scope, childScope), Effect.withSpan("Runner.launch.connect"));
      const childPid = RunnerTask.fields.pid.members[0].make(client.pid, { disableChecks: true });
      const child: OwnedAgentChild = { client, scope: childScope, lost: false };
      children.set(taskId, child);
      yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ pid: childPid, agentBinding: bindingText(client.binding), agentCursor: AgentEventSequence.make(0) }).where("taskId", "=", taskId).where("state", "=", RunnerStateEnum.LAUNCHING).returning("taskId"))).pipe(Effect.withSpan("Runner.launch.persistBinding"));
      if (closed || ownershipLost) {
        yield* Scope.close(childScope, Exit.void);
        return yield* new CommandError({ message: "Runner journal ownership unavailable" });
      }
      yield* Scope.addFinalizer(child.scope, Effect.gen(function*() {
        if (ownershipLost) return;
        const row = yield* getRow(taskId);
        if (row && !(row.state === RunnerStateEnum.SUCCEEDED || row.state === RunnerStateEnum.FAILED)) yield* hold(taskId, "child scope closed: outcome unknown; no replay");
      }).pipe(Effect.orDie));
      // Only an observed physical SDK process close releases the ownership slot.
      yield* fork(Effect.gen(function*() {
        yield* client.observedProcessClosed.pipe(Effect.withSpan("Runner.launch.observeProcessClosed"));
        if (ownershipLost) {
          children.delete(taskId);
          return;
        }
        const current = yield* requireRow(taskId);
        if (!(current.state === RunnerStateEnum.SUCCEEDED || current.state === RunnerStateEnum.FAILED)) yield* hold(taskId, "owned agent child closed before confirmed whole run completion");
        yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).where("pid", "=", childPid).returning("taskId")));
        children.delete(taskId);
        yield* Scope.close(child.scope, Exit.void).pipe(Effect.withSpan("Runner.launch.closePhysicalScope"));
        yield* heartbeat();
      }).pipe(Effect.catch((error) => hold(taskId, "physical child close outcome uncertain").pipe(Effect.andThen(Effect.fail(error))))));
      const watch = yield* client.watchLifecycle({ binding: client.binding, after: AgentEventSequence.make(0) }, (value) => Effect.runPromise(lifecycle(taskId, child, value))).pipe(Effect.provideService(Scope.Scope, child.scope), Effect.withSpan("Runner.launch.watchLifecycle"));
      // Registered after acquisition: consume typed observer loss durably before the SDK fallback finalizer.
      yield* Scope.addFinalizer(child.scope, watch.close.pipe(
        Effect.catch((error) => hold(taskId, `agent MCP lifecycle observer close failed (${error.code}); physical child ownership retained`)),
        Effect.orDie,
      ));
      yield* watch.connectionClosed.pipe(Effect.catch((failure) => Effect.gen(function*() {
        child.lost = true;
        // Own the failed observer's typed close before its scoped fallback can turn it into a defect.
        // This releases the subscription only; the process and occupied slot remain owned.
        yield* watch.close.pipe(Effect.matchEffect({
          onFailure: (error) => hold(taskId, `agent MCP lifecycle observer close failed (${error.code}); physical child ownership retained`),
          onSuccess: () => hold(taskId, `agent MCP lifecycle connection lost (${failure.code}); physical child ownership retained`),
        }));
      })), Effect.forkScoped, Effect.provideService(Scope.Scope, child.scope));
      const response = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 60000 }).pipe(Effect.withSpan("Runner.launch.getState"));
      if (!client.capabilities.includes(AgentTools.prompt.name)) {
        yield* hold(taskId, `${work.launcher} public extension hooks cannot prove prompt admission and final outcome; no prompt sent`);
        yield* fork(heartbeat());
        return;
      }
      const address = yield* readback(yield* requireRow(taskId), response).pipe(Effect.withSpan("Runner.launch.readback"));
      if (closed) return yield* Effect.fail(new CommandError({ message: "Runner journal ownership unavailable" }));
      yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ address: Schema.encodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(address), state: RunnerStateEnum.READY, reason: null }).where("taskId", "=", taskId).where("state", "=", RunnerStateEnum.LAUNCHING).returning("taskId"))).pipe(Effect.withSpan("Runner.launch.ready"));
      yield* event(taskId, { type: "readback", address });
      yield* fork(heartbeat());
    }).pipe(Effect.withSpan("Runner.launch.acquire"), Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.gen(function*() {
      // Fixed ownership metadata only; retain the failed acquisition span before holding uncertain intent.
      yield* Effect.annotateCurrentSpan({ launchIntent, ownershipLost, closed, hasChildScope: childScope !== undefined, hasOwnedChild: children.has(taskId) });
      yield* Effect.logError(`runner launch failure: ${redactedCause(cause)}`);
      if (launchIntent) {
        yield* hold(taskId, "launch/readback uncertain; no automatic relaunch");
        if (childScope && !children.has(taskId)) {
          // Failed discovery has no registered observer; scoped SDK cleanup still owns physical closure.
          const closure = yield* Scope.close(childScope, Exit.void).pipe(Effect.exit);
          if (closure._tag === "Success" && !ownershipLost) {
            yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).where("state", "=", RunnerStateEnum.HELD).where("pid", "is", null).returning("taskId")));
            yield* fork(heartbeat());
          } else if (closure._tag === "Failure") yield* hold(taskId, "agent MCP acquisition cleanup failed; physical child ownership retained");
        }
      }
      else {
        if (childScope) yield* Scope.close(childScope, Exit.void);
        yield* state(taskId, RunnerStateEnum.FAILED, "allowlisted checkout preparation failed");
        yield* owned(database.query((db) => db.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", taskId).returning("taskId")));
      }
    })));
  });


  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    closed = true;
    for (const [taskId, child] of children) {
      child.lost = true;
      if (!ownershipLost) {
        const row = yield* getRow(taskId);
        if (row && !(row.state === RunnerStateEnum.SUCCEEDED || row.state === RunnerStateEnum.FAILED)) yield* hold(taskId, "runner shutdown: child outcome unknown; no replay");
      }
      yield* Scope.close(child.scope, Exit.void).pipe(Effect.withSpan("Runner.shutdown.closeChildScope"));
    }
    // Do not clear uncertain prompt IDs/slots; restart reconciles recorded PIDs without replay.
    children.clear();
  }).pipe(Effect.orDie));
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
      yield* owned(assertRunnerSchema(database)).pipe(
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
    for (const row of yield* database.query((db) => taskRows(db).where("state", "=", RunnerStateEnum.ACCEPTED).where("occupied", "=", 1).where("detached", "=", 0))) yield* schedule(row.taskId);
    yield* fork(Effect.forever(heartbeat().pipe(
      Effect.tapError((error) => Effect.logWarning(`runner heartbeat failed: ${redactedCause(Cause.fail(error))}`)),
      Effect.retry(Schedule.spaced("5 seconds")),
      Effect.andThen(Effect.sleep("5 seconds")),
    )));
  }));
  if (ownershipLost) return yield* Deferred.await(fatal);
  return { config, database, server, failure: Deferred.await(fatal), authorize, handle, row: getRow, state, event, reports, heartbeat, launch };
});

export class Runner extends Context.Service<Runner>()("coordinator/Runner", {
  make: Effect.gen(function*() { return yield* makeRunner(yield* RunnerConfiguration); })
}) {
  static layer(config: RunnerConfig) { return Layer.effect(Runner, Runner.make).pipe(Layer.provide(Layer.merge(Layer.succeed(RunnerConfiguration, config), DatabaseService.layer()))); }
}
