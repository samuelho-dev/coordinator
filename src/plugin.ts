import { Cause, Clock, Config, ConfigProvider, Context as EffectContext, Crypto, Effect, Exit, Layer, ManagedRuntime, Option, PlatformError, Schema, Scope, Semaphore, Stdio as EffectStdio, Stream } from "effect";
import { Base64, Hex } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { Commands, CommandError, ConnectHosts, HttpError, PluginConfig, PluginDeclined, Json, SessionId, IncarnationId, WorkerId, Sha, JournalHandledKey, JournalReleaseKey } from "./contracts.ts";
import type { CommandCapabilities, CommandInput, CommandOutput, Context, Host, McpToolName, Repo } from "./contracts.ts";
import { NativeEventSequence, NativePluginConfig, NativeSubmissionId, nativeEventsUri } from "@samuelho/coordinator-native-control/contracts";
import type { NativeLifecycleEvent } from "@samuelho/coordinator-native-control/contracts";
import { makeNativeControl } from "./native-plugin.ts";
import type { NativeSession } from "./native-session.ts";
import { Mcp } from "./http.ts";
import { Platform, redactedCause } from "./platform.ts";
import { Journal } from "./journal.ts";

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
  const terminal = yield* stdio.stdoutIsTerminal;
  const tui = (ctx: Context) => terminal && ctx.hasUI && ctx.mode === "tui" && ctx.agent?.kind !== "sub";
  let base: "unknown" | "idle" | "working" | "done" | "error" = "unknown";
  let session: string | undefined;
  const inputs = new Map<string, "approval" | "question">();
  // Producer for the Program Status Protocol, not a host/transport workaround.
  // Spec: https://gist.github.com/mitchellh/7acae3abd8355c1c00287d67e96c913a
  // ponytail: one bounded root record; no IDs, progress, titles or heartbeat.
  const permissionMessage = Base64.encode("Waiting for permission");
  const questionMessage = Base64.encode("Waiting for an answer");
  const report = Effect.fn("Plugin.reportStatus")(function*(ctx: Context, clear = false) {
    if (!tui(ctx)) return;
    let permission = false;
    for (const input of inputs.values()) if (input === "approval") { permission = true; break; }
    const kind = permission ? "permission" : inputs.size > 0 ? "question" : undefined;
    // Unknown is not a protocol state: clear stale terminal records on lost authority.
    const fields = clear || base === "unknown" ? "state=clear" : kind ? `state=blocked:kind=${kind}:msg=${permission ? permissionMessage : questionMessage}` : `state=${base}`;
    yield* Stream.make(`\x1b]7501;app=${config.host}:${fields}\x1b\\`).pipe(
      Stream.run(stdio.stdout({ endOnDone: false })),
      Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator terminal status unavailable" }))),
      Effect.tapError(() => Effect.sync(() => ctx.ui.notify("Coordinator terminal status unavailable; displayed status may be stale.", "warning"))),
    );
  });
  const clearStatus = Effect.fn("Plugin.clearStatus")(function*(ctx: Context) {
    inputs.clear();
    yield* report(ctx, true);
  });
  const native = Effect.fn("Plugin.ownedStatus")(function*(event: NativeLifecycleEvent, ctx: Context) {
    if (!active || nativeEventsUri(event.binding) !== nativeEventsUri(active.owner.binding)) return;
    if (event.kind === "session_started") { session = event.binding.sessionId; base = "idle"; inputs.clear(); }
    else if (session !== event.binding.sessionId) { base = "unknown"; inputs.clear(); }
    else if (event.kind === "session_retired") { base = "unknown"; inputs.clear(); }
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
  let active: { ctx: Context; parentSessionId: string; sessionId: SessionId; incarnation: IncarnationId; workerId: WorkerId;
    owner: NativeSession; scope: Scope.Closeable; journal: Journal["Service"]; repo: Repo; heartbeat: number; dispatching: boolean } | undefined;
  let sessionScope: Scope.Closeable | undefined;
  const serial = <A, E, R>(effect: Effect.Effect<A, E, R>) => semaphore.withPermits(1)(effect);
  const held = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) => effect.pipe(
    Effect.tapCause((cause) => active ? notify(active.ctx, `Coordinator dispatch held at ${operation}: ${redactedCause(cause)}.`, "warning") : Effect.void),
  );
  const call = <const M extends McpToolName>(method: M, params: CommandInput<M>) => held(`broker ${method}`, mcp.call(config.brokerUrl, token, method, params));
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
    if (!current) return;
    const { ctx, sessionId, incarnation, workerId, journal } = current;
    if (ctx.sessionManager.getSessionId() !== current.parentSessionId) return;
    current.repo = yield* held("repository inspection", platform.inspectedRepo(ctx.cwd, config.selectedRemote));
    if ((yield* Clock.currentTimeMillis) - current.heartbeat >= 10000) {
      yield* call("session.heartbeat", { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId });
      current.heartbeat = (yield* Clock.currentTimeMillis);
    }
    const poll = yield* call("session.poll", { sessionId, incarnation });
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
      const submissionId = NativeSubmissionId.make(yield* platform.crypto.randomUUIDv4());
      current.dispatching = true;
      // Durable intent precedes SDK admission. No response/connection failure can replay this revision.
      yield* current.owner.prompt({ binding: current.owner.binding, submissionId, message: task.instruction?.prompt ?? task.request.prompt },
        (admission) => Effect.runPromise(Effect.gen(function*() {
          yield* held("journal write (release sent)", journal.releaseSent(key));
          if (admission.disposition !== "handled") yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "RUNNING",
            result: { revision: address.revision, admission, evidenceVerified: false } });
        }))).pipe(
        Effect.flatMap((result) => call("task.report", { taskId: task.taskId, sessionId, incarnation,
          state: result.disposition === "handled" || result.final.outcome === "aborted" ? "HELD" : result.final.outcome === "error" ? "FAILED" : "SUCCEEDED",
          result: { revision: address.revision, admission: result.admission, ...(result.disposition === "settled" ? { final: result.final } : {}),
            evidenceVerified: false, reason: "SDK execution outcome; deliverable evidence remains separate" } })),
        Effect.catchCause((cause) => Effect.gen(function*() {
          const reason = `SDK delivery or outcome: ${redactedCause(cause)}; explicit new addressed revision required`;
          yield* notify(ctx, `Coordinator dispatch held at ${reason}.`, "warning");
          return yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "HELD",
            result: { revision: address.revision, reason } });
        })),
        Effect.ensuring(Effect.sync(() => { current.dispatching = false; })),
        Effect.tapCause((cause) => notify(ctx, `Coordinator SDK outcome report unavailable: ${redactedCause(cause)}; reconcile this revision explicitly.`, "warning")),
        Effect.forkIn(current.scope),
      );
    }
    yield* held("journal scan", journal.scan(current.owner.manager.getSessionFile()));
    yield* held("journal upload", journal.upload((entry) => call("archive.append", { sessionId, ...entry }), 1));
  });
  const stop = Effect.fn("Plugin.stop")(function*() {
    const previous = active, scope = sessionScope;
    active = undefined;
    sessionScope = undefined;
    yield* (previous ? clearStatus(previous.ctx) : Effect.void).pipe(Effect.ensuring(Effect.gen(function*() {
      if (scope) yield* Scope.close(scope, Exit.void);
      if (previous) yield* call("session.stop", { sessionId: previous.sessionId, incarnation: previous.incarnation }).pipe(
        Effect.catchCause(() => notify(previous.ctx, "Coordinator stop unavailable; prior enrollment expires by heartbeat lease.", "warning")));
    }).pipe(Effect.orDie)));
  });
  yield* Effect.addFinalizer(() => serial(stop()).pipe(Effect.orDie));
  const start = Effect.fn("Plugin.start")((ctx: Context) => serial(Effect.gen(function*() {
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
    active = { ctx, parentSessionId, sessionId, workerId, incarnation: IncarnationId.make(owner.binding.incarnation), owner, scope,
      repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote), journal: EffectContext.get(context, Journal), heartbeat: 0, dispatching: false };
    let updates = Promise.resolve();
    const unsubscribe = owner.journal.subscribe((event) => {
      updates = updates.then(() => Effect.runPromise(native(event, ctx).pipe(Effect.catchCause((cause) => Effect.sync(() => {
        base = "unknown";
        ctx.ui.notify(`Coordinator owned SDK status unavailable: ${redactedCause(cause)}; displayed status may be stale.`, "warning");
      })))));
    });
    yield* Effect.addFinalizer(() => Effect.promise(async () => { unsubscribe(); await updates; })).pipe(Effect.provideService(Scope.Scope, scope));
    for (const event of owner.journal.read(owner.binding, NativeEventSequence.make(0)).entries) yield* native(event, ctx);
    yield* Effect.gen(function*() {
      yield* Effect.sleep("1 second");
      yield* serial(tick()).pipe(Effect.catchCause((cause) => Effect.logWarning(`Coordinator periodic dispatch recovery: ${redactedCause(cause)}`)));
    }).pipe(Effect.forever, Effect.forkIn(scope));
    yield* tick();
  })));
  const command = Effect.fn("Plugin.command")((args: string, ctx: Context) => serial(Effect.gen(function*() {
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
        yield* journal.consent(participants);
        return { enabled: true as const };
      }),
      pluginOptout: () => Effect.gen(function*() {
        // Revoke server enrollment first; failure leaves local spool intact for retry.
        yield* call("archive.consent", { sessionId, participants: journal.state.participants, enabled: false });
        yield* journal.erase();
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
          yield* journal.clearInbox();
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
    Effect.tapCause(() => notify(ctx, "Coordinator command failed; no automatic retry. Check exact JSON fields, authorization and connection.", "error")),
  )));
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
