import { Cause, Clock, Config, ConfigProvider, Context as EffectContext, Crypto, Deferred, Duration, Effect, Exit, Fiber, Layer, ManagedRuntime, Option, PlatformError, Schedule, Schema, Scope, Semaphore, Stdio as EffectStdio } from "effect";
import { Hex } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { Commands, CommandError, ConnectHosts, HttpError, PluginConfig, PluginDeclined, Json, SessionId, IncarnationId, WorkerId, Sha, JournalHandledKey, JournalReleaseKey } from "./contracts.ts";
import type { CommandCapabilities, CommandInput, CommandOutput, Context, Host, McpToolName, Repo } from "./contracts.ts";
import { NativeControlError, NativeEventSequence, NativePluginConfig, NativeSubmissionId, nativeEventsUri } from "@samuelho/coordinator-native-control/contracts";
import type { NativeLifecycleEvent } from "@samuelho/coordinator-native-control/contracts";
import { makeNativeControl } from "./native-plugin.ts";
import type { NativeSession } from "./native-session.ts";
import { Mcp } from "./http.ts";
import { Platform, redactedCause } from "./platform.ts";
import { Journal } from "./journal.ts";
import { makeProgramStatus, type ProgramStatus } from "./plugin-status.ts";

const display = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown, {
  replacer: (_, item: unknown) => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
    return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
  },
}), { onExcessProperty: "error" });
const pluginCommands = Object.entries(Commands).filter((entry): entry is [string, Extract<(typeof Commands)[keyof typeof Commands], { target: "plugin" }>] => entry[1].target === "plugin");
const makePlugin = Effect.fn("Plugin.make")(function*() {
  const platform = yield* Platform;
  const configuredPath = yield* Config.option(Config.String("COORDINATOR_CONFIG"));
  let path: string;
  if (Option.isSome(configuredPath)) path = configuredPath.value;
  else {
    const configuredDirectory = yield* Config.option(Config.String("XDG_CONFIG_HOME"));
    const directory = Option.isSome(configuredDirectory) ? configuredDirectory.value : platform.path.join(yield* Config.String("HOME"), ".config");
    path = platform.path.join(directory, "coordinator", "plugin.json");
  }
  if (!platform.path.isAbsolute(path)) return yield* Effect.fail(new CommandError({ message: "Coordinator configuration path must be absolute" }));
  if (Option.isNone(configuredPath) && !(yield* platform.fs.exists(path))) return undefined;
  const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path));
  if (!platform.path.isAbsolute(config.stateDirectory) || !platform.path.isAbsolute(config.tokenFile) || !["omp", "pi"].includes(config.host) || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(config.machineId)) return yield* Effect.fail(new CommandError({ message: "Invalid coordinator configuration" }));
  const token = yield* platform.privateFile(config.tokenFile);
  const connectHosts = config.connectHosts === undefined
    ? yield* Config.String("COORDINATOR_CONNECT_HOSTS_JSON").pipe(Config.withDefault("{}"))
    : yield* Schema.encodeEffect(Schema.fromJsonString(ConnectHosts))(config.connectHosts);
  const mcp = EffectContext.get(yield* Layer.build(Mcp.layer.pipe(Layer.provide(ConfigProvider.layer(
    ConfigProvider.fromUnknown({ COORDINATOR_CONNECT_HOSTS_JSON: connectHosts }),
  )))), Mcp);
  const stdio = yield* EffectStdio.Stdio;
  let base: "unknown" | "idle" | "working" | "done" | "error" = "unknown";
  let session: string | undefined;
  const inputs = new Map<string, "approval" | "question">();
  const report = Effect.fn("Plugin.reportStatus")(function*(ctx: Context, clear = false) {
    const status = active?.status;
    if (!status || active?.ctx !== ctx) return;
    let permission = false;
    for (const input of inputs.values()) if (input === "approval") { permission = true; break; }
    const kind = permission ? "permission" : inputs.size > 0 ? "question" : undefined;
    yield* (clear ? status.clear() : status.report(base === "unknown" ? undefined : base, kind)).pipe(
      Effect.tapError(() => Effect.sync(() => ctx.ui.notify("Coordinator terminal status unavailable; displayed status may be stale.", "warning"))),
    );
  });
  const clearStatus = Effect.fn("Plugin.clearStatus")(function*(status: ProgramStatus) {
    inputs.clear();
    yield* status.clear();
  });
  const native = Effect.fn("Plugin.ownedStatus")(function*(event: NativeLifecycleEvent, ctx: Context) {
    if (!active || nativeEventsUri(event.binding) !== nativeEventsUri(active.owner.binding)) return;
    if (event.kind === "session_started") { session = event.binding.sessionId; base = "idle"; inputs.clear(); }
    else if (session !== event.binding.sessionId) { base = "unknown"; inputs.clear(); }
    else if (event.kind === "session_retired") {
      base = "unknown"; inputs.clear();
      yield* controlFailure(active, Cause.fail(new NativeControlError({ code: "binding_mismatch", message: "Owned native binding retired" })), false);
    }
    else if (event.kind === "run_started") base = "working";
    else if (event.kind === "input_requested") inputs.set(event.inputId, event.inputKind === "approval" ? "approval" : "question");
    else if (event.kind === "input_resolved") {
      if (!inputs.delete(event.inputId)) base = "unknown";
    } else if (event.kind === "run_settled") {
      base = inputs.size > 0 ? "unknown" : event.outcome === "completed" ? "done" : event.outcome === "error" ? "error" : "idle";
    }
    yield* report(ctx);
  });
  const semaphore = yield* Semaphore.make(1);
  const pluginScope = yield* Scope.Scope;
  const recoverySchedule = Schedule.exponential("1 second").pipe(
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.millis(Math.min(30000, Duration.toMillis(duration))))),
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.millis(Math.max(1000, Math.min(30000, Duration.toMillis(duration)))))),
  );
  let active: { ctx: Context; parentSessionId: string; sessionId: SessionId; incarnation: IncarnationId; workerId: WorkerId;
    owner: NativeSession; scope: Scope.Closeable; journal: Journal["Service"]; repo: Repo; heartbeat: number; dispatching: boolean;
    status: ProgramStatus; outage: boolean; paused: boolean; restart: boolean; nextAttempt: number; epoch: number;
    step: (now: number, input: unknown) => Effect.Effect<[Duration.Duration, Duration.Duration], Cause.Done<Duration.Duration>> } | undefined;
  let sessionScope: Scope.Closeable | undefined;
  const serial = <A, E, R>(effect: Effect.Effect<A, E, R>) => semaphore.withPermits(1)(effect);
  const controlFailure = (current: NonNullable<typeof active>, cause: Cause.Cause<unknown>, broker: boolean, presence = false) => Effect.gen(function*() {
    if (active !== current || Cause.hasInterrupts(cause) || current.paused) return;
    const found = Cause.findErrorOption(cause);
    const error = Option.isSome(found) ? found.value : undefined;
    const conflict = presence && error instanceof HttpError && (error.status === 409 || error.status === 410);
    const transient = broker && (conflict || error instanceof Cause.TimeoutError ||
      error instanceof CommandError && error.reason === "transport" ||
      error instanceof HttpError && (error.status === 408 || error.status === 429 || error.status >= 500 && error.status !== 501) &&
        !(error.protocolCode !== undefined && error.protocolCode >= -32603 && error.protocolCode <= -32600));
    const epoch = ++current.epoch;
    current.paused = !transient;
    current.restart ||= conflict;
    if (!current.outage) {
      current.outage = true;
      yield* notify(current.ctx, transient ? "Coordinator connection unavailable; reconnecting automatically. Native work continues." :
        "Coordinator control paused; check authorization, configuration or local journal. Native work continues.", "warning");
    }
    if (active !== current || current.epoch !== epoch) return;
    yield* current.status.network(transient ? "retrying" : "paused").pipe(Effect.catchCause(() => Effect.void));
    if (!transient) return;
    const now = yield* Clock.currentTimeMillis;
    const delay = yield* current.step(now, undefined).pipe(Effect.orDie);
    let lowerBound = 0;
    if (error instanceof HttpError && error.retryAfter !== undefined) {
      const value = error.retryAfter.trim();
      const seconds = /^\d+$/.test(value) ? Number(value) : undefined;
      const date = seconds === undefined ? Date.parse(value) : NaN;
      lowerBound = seconds !== undefined && Number.isFinite(seconds) ? seconds * 1000 : Number.isFinite(date) ? Math.max(0, date - now) : 0;
    }
    if (active === current && current.epoch === epoch) current.nextAttempt = Math.max(current.nextAttempt, now + Math.max(Duration.toMillis(delay[1]), lowerBound));
  });
  const held = <A, E, R>(_operation: string, effect: Effect.Effect<A, E, R>, owner = active) => effect.pipe(
    Effect.tapCause((cause) => owner ? controlFailure(owner, cause, false) : Effect.void),
  );
  const call = <const M extends McpToolName>(method: M, params: CommandInput<M>, owner = active) => Effect.suspend(() => {
    const current = owner;
    if (current && (current !== active || current.paused)) return Effect.fail(new CommandError({ message: "Coordinator control paused" }));
    return mcp.call(config.brokerUrl, token, method, params).pipe(
      Effect.tapCause((cause) => current ? controlFailure(current, cause, true, method === "session.heartbeat" || method === "session.poll") : Effect.void),
    );
  });
  const notify = (ctx: Context, text: string, level: "info" | "warning" | "error" = "info") => Effect.sync(() => ctx.ui.notify(text, level));
  const confirm = (ctx: Context, title: string, text: string) => Effect.gen(function*() {
    inputs.set("coordinator-approval", "approval");
    return yield* Effect.gen(function*() {
      yield* report(ctx);
      return yield* Effect.tryPromise({ try: () => ctx.ui.confirm(title, text), catch: () => new CommandError({ message: "Coordinator approval UI unavailable" }) });
    }).pipe(Effect.ensuring(Effect.gen(function*() {
      inputs.delete("coordinator-approval");
      yield* report(ctx);
    }).pipe(Effect.orDie)));
  });
  const local = Effect.fn("Plugin.local")(function*(ctx: Context) {
    return { repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote),
      ref: (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: ctx.cwd }))).trim(),
      sha: Sha.make((yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: ctx.cwd }))).trim()) };
  });
  const tick = Effect.fn("Plugin.tick")(function*() {
    const current = active;
    if (!current || current.paused || current.restart) return;
    if ((yield* Clock.currentTimeMillis) < current.nextAttempt) return;
    const { ctx, sessionId, incarnation, workerId, journal } = current;
    if (ctx.sessionManager.getSessionId() !== current.parentSessionId) return;
    const epoch = current.epoch;
    let heartbeated = false;
    current.repo = yield* held("repository inspection", platform.inspectedRepo(ctx.cwd, config.selectedRemote));
    if (current.outage || (yield* Clock.currentTimeMillis) - current.heartbeat >= 10000) {
      yield* call("session.heartbeat", { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId });
      current.heartbeat = (yield* Clock.currentTimeMillis);
      heartbeated = true;
    }
    const poll = yield* call("session.poll", { sessionId, incarnation });
    if (active !== current || current.paused || current.restart || current.epoch !== epoch) return;
    if (current.outage && heartbeated) {
      current.outage = false;
      current.nextAttempt = 0;
      current.step = yield* Schedule.toStep(recoverySchedule);
      yield* current.status.network(undefined).pipe(Effect.catchCause(() => Effect.void));
      if (active !== current || current.paused || current.restart || current.epoch !== epoch) return;
      yield* notify(ctx, "Coordinator connection recovered.");
    }
    if (poll.archiveEnabled === false && journal.state.consent) yield* held("journal write (erase)", journal.erase());
    for (const message of poll.messages) {
      if (message.incarnation !== incarnation || message.expiresAt <= (yield* Clock.currentTimeMillis)) continue;
      const newlyHandled = yield* held("journal write (receive)", journal.receive(message.messageId, message.payload));
      if (newlyHandled) yield* notify(ctx, `Coordinator message ${message.messageId}; /coordinator inbox shows the durably received content.`);
      yield* call("peer.ack", { sessionId, incarnation, messageId: message.messageId });
    }
    for (const offer of poll.offers) {
      const handledKey = JournalHandledKey.make(`offer:${offer.taskId}`);
      if (yield* held("journal read", journal.handled(handledKey))) continue;
      yield* notify(ctx, `Coordinator offer ${offer.taskId}: ${offer.request.repo} ${offer.request.ref} ${offer.request.sha}. /coordinator status {"taskId":"${offer.taskId}"}; then accept or decline explicitly.`);
      yield* held("journal write (offer)", journal.handle(handledKey));
    }
    for (const task of poll.releases) {
      const address = task.address;
      if (!address || task.confirmedRevision !== address.revision || task.disposition !== "ready" || !task.readback || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)) || address.sessionId !== sessionId || address.incarnation !== incarnation) continue;
      const approvedAddress = task.instruction?.address;
      if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== task.request.repo || (approvedAddress ? (yield* platform.fingerprint(address)) !== (yield* platform.fingerprint(approvedAddress)) : address.ref !== task.request.ref || address.sha !== task.request.sha) || address.workerId !== workerId) continue;
      if (current.dispatching || current.owner.pendingSubmissions > 0 || current.owner.session.isStreaming || current.owner.session.isCompacting ||
        (current.owner.backend.host === "pi" ? current.owner.backend.session.pendingMessageCount > 0 :
          current.owner.backend.session.queuedMessageCount > 0 || current.owner.backend.session.hasPendingAsyncWork())) continue;
      const actual = yield* held("repository inspection", local(ctx));
      if (actual.repo !== address.repo || actual.ref !== address.ref || actual.sha !== address.sha) {
        yield* call("task.readback", { taskId: task.taskId, address, disposition: "hold", reason: "Local repository/ref/SHA changed", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
        continue;
      }
      const key = JournalReleaseKey.make(`${task.taskId}:${address.revision}`);
      if (journal.state.releases[key]) {
        if (journal.state.releases[key] === "intent") {
          yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "HELD", result: { reason: "Release delivery uncertain; explicit new addressed revision required" } });
        }
        continue;
      }
      if (!(yield* held("journal write (release intent)", journal.releaseIntent(key)))) continue;
      if (active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch) return;
      const submissionId = NativeSubmissionId.make(yield* platform.crypto.randomUUIDv4());
      if (active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch) return;
      current.dispatching = true;
      let admissionFenced = false;
      // Durable intent precedes SDK admission. No response/connection failure can replay this revision.
      yield* Effect.suspend(() => active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch
        ? Effect.void
        : current.owner.prompt({ binding: current.owner.binding, submissionId, message: task.instruction?.prompt ?? task.request.prompt },
        (admission) => Effect.runPromise(Effect.gen(function*() {
          yield* held("journal write (release sent)", journal.releaseSent(key), current);
          if (admission.disposition !== "handled") yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "RUNNING",
            result: { revision: address.revision, admission, evidenceVerified: false } }, current);
        })), () => {
          const allowed = active === current && !current.outage && !current.paused && !current.restart && current.epoch === epoch;
          admissionFenced ||= !allowed;
          return allowed;
        }).pipe(
        Effect.flatMap((result) => call("task.report", { taskId: task.taskId, sessionId, incarnation,
          state: result.disposition === "handled" || result.final.outcome === "aborted" ? "HELD" : result.final.outcome === "error" ? "FAILED" : "SUCCEEDED",
          result: { revision: address.revision, admission: result.admission, ...(result.disposition === "settled" ? { final: result.final } : {}),
            evidenceVerified: false, reason: "SDK execution outcome; deliverable evidence remains separate" } }, current)))).pipe(
        Effect.catchCause((cause) => Effect.gen(function*() {
          if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause);
          if (admissionFenced) return;
          const failure = Cause.findErrorOption(cause);
          if (Option.isSome(failure) && failure.value instanceof NativeControlError && (failure.value.code === "binding_mismatch" || failure.value.code === "invalid_input")) {
            yield* controlFailure(current, cause, false);
            return;
          }
          if (current.outage || active !== current) return;
          const reason = `SDK delivery or outcome: ${redactedCause(cause)}; explicit new addressed revision required`;
          yield* notify(ctx, `Coordinator dispatch held at ${reason}.`, "warning");
          return yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "HELD",
            result: { revision: address.revision, reason } }, current);
        })),
        Effect.ensuring(Effect.sync(() => { current.dispatching = false; })),
        Effect.tapCause((cause) => current.outage || Cause.hasInterrupts(cause) || active !== current ? Effect.void :
          notify(ctx, `Coordinator SDK outcome report unavailable: ${redactedCause(cause)}; reconcile this revision explicitly.`, "warning")),
        Effect.forkIn(current.scope),
      );
    }
    yield* held("journal scan", journal.scan(current.owner.manager.getSessionFile()));
    yield* journal.upload((entry) => call("archive.append", { sessionId, ...entry }), 1);
  });
  const stop = Effect.fn("Plugin.stop")(function*() {
    const previous = active, scope = sessionScope;
    active = undefined;
    sessionScope = undefined;
    yield* (previous ? previous.status.network(undefined).pipe(Effect.andThen(clearStatus(previous.status))) : Effect.void).pipe(Effect.ensuring(Effect.gen(function*() {
      if (scope) yield* Scope.close(scope, Exit.void);
      if (previous) yield* call("session.stop", { sessionId: previous.sessionId, incarnation: previous.incarnation }).pipe(
        Effect.catchCause(() => notify(previous.ctx, "Coordinator stop unavailable; prior enrollment expires by heartbeat lease.", "warning")));
    }).pipe(Effect.orDie)));
  });
  yield* Effect.addFinalizer(() => serial(stop()).pipe(Effect.orDie));
  let restartOwner: (ctx: Context, owner: NonNullable<typeof active>) => Effect.Effect<void, unknown>;
  const start = Effect.fn("Plugin.start")((ctx: Context, recovering = false, expected = active) => serial(Effect.gen(function*() {
    const initial = yield* Deferred.make<void>();
    if (recovering && active !== expected) {
      yield* Deferred.succeed(initial, undefined);
      return initial;
    }
    if (recovering && (!expected || expected.paused || expected.dispatching || !expected.owner.retireIfIdle())) {
      yield* Deferred.succeed(initial, undefined);
      return initial;
    }
    const outage = recovering && active?.outage === true;
    const recoveryStep = recovering ? active?.step : undefined;
    yield* stop();
    const parentSessionId = ctx.sessionManager.getSessionId();
    const scope = yield* Scope.make();
    sessionScope = scope;
    yield* platform.fs.makeDirectory(config.stateDirectory, { recursive: true, mode: 0o700 });
    const configuredNative = yield* Config.option(Config.String("COORDINATOR_NATIVE_CONFIG"));
    const nativeConfig = yield* Effect.gen(function*() {
      if (Option.isSome(configuredNative)) {
        if (!platform.path.isAbsolute(configuredNative.value)) return yield* new CommandError({ message: "Absolute native configuration path required" });
        const value = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativePluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configuredNative.value));
        if (value.host !== config.host) return yield* new CommandError({ message: "Native owner host mismatch" });
        return value;
      }
      const directory = platform.path.join(config.stateDirectory, `native-${yield* platform.digest(parentSessionId)}`);
      yield* platform.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
      const tokenFile = platform.path.join(directory, "control.token");
      if (!(yield* platform.fs.exists(tokenFile))) yield* platform.fs.writeFileString(tokenFile, Hex.encode(yield* platform.crypto.randomBytes(32)), { flag: "wx", mode: 0o600 });
      return { host: config.host, endpointFile: platform.path.join(directory, "endpoint.json"), tokenFile };
    });
    const owner = yield* makeNativeControl(nativeConfig, ctx).pipe(Effect.provideService(Scope.Scope, scope),
      Effect.provideService(Platform, platform), Effect.provideService(Crypto.Crypto, platform.crypto),
      Effect.onExit((exit) => exit._tag === "Failure" ? Scope.close(scope, exit) : Effect.void));
    const sessionId = SessionId.make(owner.binding.sessionId), workerId = WorkerId.make(sessionId);
    const context = yield* Layer.buildWithScope(Journal.layer(config.stateDirectory, sessionId).pipe(Layer.provide(Layer.succeed(Platform, platform))), scope);
    const status = yield* makeProgramStatus(ctx).pipe(Effect.provideService(Scope.Scope, scope), Effect.provideService(EffectStdio.Stdio, stdio));
    active = { ctx, parentSessionId, sessionId, workerId, incarnation: IncarnationId.make(owner.binding.incarnation), owner, scope,
      repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote), journal: EffectContext.get(context, Journal), heartbeat: 0, dispatching: false,
      status, outage, paused: false, restart: false, nextAttempt: 0, epoch: 0, step: recoveryStep ?? (yield* Schedule.toStep(recoverySchedule)) };
    let updates = Promise.resolve();
    const unsubscribe = owner.journal.subscribe((event) => {
      updates = updates.then(() => Effect.runPromise(native(event, ctx).pipe(Effect.catchCause((cause) => Effect.sync(() => {
        base = "unknown";
        ctx.ui.notify(`Coordinator owned SDK status unavailable: ${redactedCause(cause)}; displayed status may be stale.`, "warning");
      })))));
    });
    yield* Effect.addFinalizer(() => Effect.promise(async () => { unsubscribe(); await updates; })).pipe(Effect.provideService(Scope.Scope, scope));
    for (const event of owner.journal.read(owner.binding, NativeEventSequence.make(0)).entries) yield* native(event, ctx);
    const current = active;
    yield* Effect.addFinalizer(() => Deferred.succeed(initial, undefined)).pipe(Effect.provideService(Scope.Scope, scope));
    yield* Effect.gen(function*() {
      while (active === current && ctx.sessionManager.getSessionId() === current.parentSessionId) {
        if (current.restart && !current.paused && !current.dispatching && current.owner.pendingSubmissions === 0 && !current.owner.session.isStreaming &&
          !current.owner.session.isCompacting && (current.owner.backend.host === "pi" ? current.owner.backend.session.pendingMessageCount === 0 :
            current.owner.backend.session.queuedMessageCount === 0 && !current.owner.backend.session.hasPendingAsyncWork())) {
          yield* Effect.sleep(Math.max(0, current.nextAttempt - (yield* Clock.currentTimeMillis)));
          yield* restartOwner(ctx, current).pipe(Effect.forkIn(pluginScope));
        }
        yield* tick().pipe(
          Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.failCause(cause) :
            current.outage ? Effect.void : controlFailure(current, cause, false)),
          Effect.ensuring(Deferred.succeed(initial, undefined)),
        );
        yield* Effect.sleep(Math.max(1000, current.nextAttempt - (yield* Clock.currentTimeMillis)));
      }
    }).pipe(Effect.forkIn(scope));
    return initial;
  })).pipe(Effect.flatMap(Deferred.await)));
  restartOwner = (ctx, owner) => start(ctx, true, owner);
  const command = Effect.fn("Plugin.command")((args: string, ctx: Context) => Effect.suspend(() => {
    const scope = active?.scope;
    const operation = Effect.gen(function*() {
    const split = args.trim().indexOf(" ");
    const action = split < 0 ? args.trim() : args.trim().slice(0, split);
    if (action === "" || action === "help") {
      return yield* notify(ctx, pluginCommands.map(([id, entry]) => `${id.slice("plugin.".length)}: ${entry.description}`).join("\n"));
    }
    const entry = pluginCommands.find(([id]) => id === `plugin.${action}`)?.[1];
    if (!entry) return yield* notify(ctx, "Unknown Coordinator action; use /coordinator help. Send free-form messages in normal chat.", "error");
    const data: unknown = split < 0 ? {} : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Json), { onExcessProperty: "error" })(args.trim().slice(split + 1));
    const current = active;
    if (!current || current.parentSessionId !== ctx.sessionManager.getSessionId()) return yield* new CommandError({ message: "Session not enrolled" });
    const { sessionId, incarnation, workerId, journal } = current;
    const approve = Effect.fn("Plugin.approve")(function*(title: string, text: string) {
      if (!ctx.hasUI || !(yield* confirm(ctx, title, text))) return yield* Effect.fail(new PluginDeclined({ message: "Human approval declined or unavailable" }));
    });
    const readback = Effect.fn("Plugin.readback")(function*(data: CommandInput<"plugin.readback">, disposition: "ready" | "hold" | "clarify" | "unable") {
      const { address } = data;
      if (address.sessionId !== sessionId || address.incarnation !== incarnation || address.workerId !== workerId) return yield* Effect.fail(new CommandError({ message: "Readback must address this native session incarnation and worker" }));
      if (disposition === "ready") {
        const actual = yield* local(ctx);
        if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== actual.repo || address.sha !== actual.sha || address.ref !== actual.ref) return yield* Effect.fail(new CommandError({ message: "Readback does not match current local repository/ref/SHA/worker/deadline" }));
      }
      return yield* call("task.readback", { ...data, disposition });
    });
    const capabilities: CommandCapabilities<"plugin", CommandError | PluginDeclined | HttpError | Cause.TimeoutError | PlatformError.PlatformError> = {
      pluginParticipantConsent: (data) => Effect.gen(function*() {
        yield* approve("Your participant archive consent", `${data.enabled ? "Grant" : "Revoke"} YOUR consent for owner ${data.owner}, session ${data.sessionId}? Content omitted; graph metadata only.`);
        return yield* call("archive.participantConsent", data);
      }),
      pluginConsent: ({ participants }) => Effect.gen(function*() {
        yield* approve("Your session archive consent", `Grant YOUR consent to archive this session with participants ${participants.join(", ")}? Each other participant must separately enroll consent with their own credential; broker checks it. Only graph metadata is archived across branches; content is omitted.`);
        yield* call("archive.consent", { sessionId, participants, enabled: true });
        yield* held("journal write (consent)", journal.consent(participants), current);
        return { enabled: true as const };
      }),
      pluginOptout: () => Effect.gen(function*() {
        // Revoke server enrollment first; failure leaves local spool intact for retry.
        yield* call("archive.consent", { sessionId, participants: journal.state.participants, enabled: false });
        yield* held("journal write (erase)", journal.erase(), current);
        yield* call("archive.delete", { sessionId });
        return { enabled: false as const };
      }),
      pluginAccept: (data) => call("peer.reply", { ...data, sessionId, incarnation, decision: "accepted" }),
      pluginDecline: (data) => call("peer.reply", { ...data, sessionId, incarnation, decision: "declined" }),
      pluginSubmit: (work) => Effect.gen(function*() {
        yield* approve("Approve coordinator task", `${work.taskId}: ${work.repo} ${work.ref} ${work.sha}; deadline ${work.deadline}; runner ${work.launcher}. Submit this exact approved work?`);
        return yield* call("task.submit", work);
      }),
      pluginReadback: (data) => readback(data, "ready"),
      pluginHold: (data) => readback(data, "hold"),
      pluginClarify: (data) => readback(data, "clarify"),
      pluginUnable: (data) => readback(data, "unable"),
      pluginAddress: (data) => call("task.address", data),
      pluginConfirm: (data) => Effect.gen(function*() {
        yield* approve("Confirm coordinator release", `Release this exact ready addressed revision?\n${display(data)}`);
        return yield* call("task.confirm", data);
      }),
      pluginHandoff: (data) => Effect.gen(function*() {
        yield* approve("Approve coordinator handoff", `Transfer ownership to this exact native recipient?\n${display(data)}`);
        return yield* call("task.handoff", data);
      }),
      pluginInstruction: (instruction) => Effect.gen(function*() {
        yield* approve("Approve later addressed instruction", `Approve this exact new addressed revision? Existing native work is not retroactively gated.\n${display(instruction)}`);
        return yield* call("task.instruction", instruction);
      }),
      pluginReport: (data) => call("task.report", { ...data, sessionId, incarnation }),
      pluginEvidenceTarget: (data) => call("evidence.target", data),
      pluginEvidenceCi: (data) => call("evidence.ci", data),
      pluginEvidenceLocal: (data) => call("evidence.local", data),
      pluginEvidenceReview: (data) => call("evidence.review", data),
      pluginEvidenceDeployment: (data) => call("evidence.deployment", data),
      pluginStatus: (data) => call("task.get", data),
      pluginSessions: () => Effect.gen(function*() {
        const result: CommandOutput<"plugin.sessions"> = [];
        let cursor: SessionId | undefined;
        do {
          const page = yield* call("session.list", cursor === undefined ? {} : { cursor });
          result.push(...page.sessions);
          cursor = page.nextCursor;
        } while (cursor !== undefined);
        return result;
      }),
      pluginAttach: (data) => call("task.attach", data),
      pluginControl: (data) => Effect.gen(function*() {
        if (data.type !== "get_state") yield* approve("Approve sensitive native control", `${data.type === "abort" ? "Abort this native child; the task remains held for reconciliation" : "Read potentially sensitive native child messages"}?\n${display(data)}`);
        return yield* call("task.control", data);
      }),
      pluginArchive: (data) => call("archive.list", { ...data, sessionId }),
      pluginInbox: (data) => Effect.gen(function*() {
        if (data.clear === true) {
          yield* held("journal write (clear inbox)", journal.clearInbox(), current);
          return { cleared: true as const };
        }
        return journal.state.inbox;
      }),
      pluginIdentity: () => Effect.gen(function*() {
        return { sessionId, incarnation, machineId: config.machineId, ...(yield* local(ctx)), workerId, workerIdentitySource: "native-session" as const };
      }),
    };
    const result = yield* entry.executeDecoded(capabilities, data);
    // Status/readback is user-visible, not model context. Never echo credentials or arbitrary server errors.
    yield* notify(ctx, display(result));
  }).pipe(
    Effect.catchTag("PluginDeclined", () => Effect.void),
    Effect.tapCause((cause) => Cause.hasInterrupts(cause) ? Effect.void :
      notify(ctx, "Coordinator command failed; no automatic retry. Check exact JSON fields, authorization and connection.", "error")),
    );
    return scope ? operation.pipe(Effect.forkIn(scope), Effect.flatMap(Fiber.join)) : operation;
  }));
  return { config, start, stop: () => serial(stop()), command, native };
});

export class Plugin extends EffectContext.Service<Plugin>()("coordinator/Plugin", { make: makePlugin }) {
  static readonly layer = Layer.effect(this, this.make());
}

// Public extension callbacks are the only Promise/runtime boundary.
export default async function coordinator(host: Host): Promise<void> {
  const layer = Plugin.layer.pipe(Layer.provide(Platform.layer), Layer.provide(NodeServices.layer));
  let runtime = ManagedRuntime.make(layer);
  try {
    const initial = await runtime.runPromise(Plugin);
    if (!initial) { await runtime.dispose(); return; }
    let disposed = false;
    const start = async (_event: unknown, ctx: Context): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (disposed) { runtime = ManagedRuntime.make(layer); disposed = false; }
      await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.start(ctx) : Effect.void));
    };
    host.on("session_start", start);
    if (initial.config.host === "omp") host.on("session_switch", start);
    host.on("session_shutdown", async () => { await runtime.dispose(); disposed = true; });
    host.registerCommand("coordinator", { description: "Coordinator: explicit addressed dispatch, status, consent and runner-owned control (JSON arguments)", handler: (args, ctx) => runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.command(args, ctx) : Effect.void).pipe(
      Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator command failed; no automatic retry" }))),
    )) });
  } catch { await runtime.dispose(); throw new CommandError({ message: "Coordinator plugin initialization failed; check private configuration and connection" }); }
}
