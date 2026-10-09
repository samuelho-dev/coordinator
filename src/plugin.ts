import { Cause, Clock, Config, Context as EffectContext, Effect, Exit, Layer, ManagedRuntime, Option, PlatformError, Schema, Scope, Semaphore, Stdio as EffectStdio, Stream } from "effect";
import { Base64 } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { Commands, CommandError, HttpError, PluginConfig, PluginDeclined, Json, SessionId, IncarnationId, WorkerId, Sha, JournalHandledKey, JournalReleaseKey, type CommandCapabilities, type CommandInput, type Context, type Host, type PluginActive, type McpToolName, type NativeEventName } from "./contracts.ts";
import { Mcp } from "./http.ts";
import { Platform } from "./platform.ts";
import { Journal } from "./journal.ts";

const display = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown, {
  replacer: (_, item: unknown) => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
    return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
  },
}), { onExcessProperty: "error" });
const pluginCommands = Object.entries(Commands).filter((entry): entry is [string, Extract<(typeof Commands)[keyof typeof Commands], { target: "plugin" }>] => entry[1].target === "plugin");
const makePlugin = Effect.fn("Plugin.make")(function*(host: Host) {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const configuredPath = yield* Config.option(Config.String("COORDINATOR_CONFIG"));
  if (Option.isNone(configuredPath)) return undefined;
  const path = configuredPath.value;
  if (!platform.path.isAbsolute(path)) return yield* Effect.fail(new CommandError({ message: "COORDINATOR_CONFIG must be absolute" }));
  const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path));
  if (!platform.path.isAbsolute(config.stateDirectory) || !platform.path.isAbsolute(config.tokenFile) || !["omp", "pi"].includes(config.host) || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(config.machineId)) return yield* Effect.fail(new CommandError({ message: "Invalid coordinator configuration" }));
  const token = yield* platform.privateFile(config.tokenFile);
  const stdio = yield* EffectStdio.Stdio;
  const terminal = yield* stdio.stdoutIsTerminal;
  const tui = (ctx: Context) => terminal && ctx.hasUI && ctx.mode === "tui" && ctx.agent?.kind !== "sub";
  let statusContext: Context | undefined;
  let base: "unknown" | "idle" | "working" = "unknown";
  let session: string | undefined;
  let healthy = false;
  let running = false;
  const inputs = new Map<string, "approval" | "question">();
  const promptKinds: string[] = [];
  const prompt = Schema.Struct({ kind: Schema.Literals(["select", "confirm", "input", "editor", "custom"]) });
  const approval = Schema.Struct({ sessionId: Schema.String, toolCallId: Schema.String });
  // Producer for the Program Status Protocol, not a host/transport workaround.
  // Spec: https://gist.github.com/mitchellh/7acae3abd8355c1c00287d67e96c913a
  // ponytail: one bounded root record; no IDs, progress, titles or heartbeat.
  const permissionMessage = Base64.encode("Waiting for permission");
  const questionMessage = Base64.encode("Waiting for an answer");
  const report = Effect.fn("Plugin.reportStatus")(function*(ctx: Context, clear = false) {
    statusContext = ctx;
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
  const native = Effect.fn("Plugin.publicStatus")(function*(name: NativeEventName, event: unknown, ctx: Context) {
    const unknown = () => { base = "unknown"; healthy = false; running = false; inputs.clear(); promptKinds.length = 0; };
    if (name === "session_start" || name === "session_switch") {
      session = ctx.sessionManager.getSessionId();
      inputs.clear(); promptKinds.length = 0;
      running = false;
      healthy = true; base = ctx.isIdle() && !ctx.hasPendingMessages() ? "idle" : "unknown";
    } else if (name === "session_shutdown") unknown();
    else if (!healthy || session !== ctx.sessionManager.getSessionId()) unknown();
    else if (name === "agent_start") {
      running = true;
      base = "working";
    } else if (name === "agent_settled" && config.host === "pi") {
      // Stock Pi publishes settlement without an outcome. Pre-settle outcomes can
      // be invalidated by late cancellation; clear rather than invent done/error.
      if (!running || !ctx.isIdle() || ctx.hasPendingMessages() || inputs.size > 0) unknown();
      else { running = false; base = "unknown"; }
    } else if (name === "ui_prompt_start" || name === "ui_prompt_end") {
      const decoded = yield* Schema.decodeUnknownEffect(prompt)(event).pipe(Effect.option);
      if (Option.isNone(decoded)) unknown();
      else if (name === "ui_prompt_start") {
        promptKinds.push(decoded.value.kind);
        inputs.set(`prompt:${promptKinds.length}`, "question");
      } else if (promptKinds.at(-1) !== decoded.value.kind) unknown();
      else { inputs.delete(`prompt:${promptKinds.length}`); promptKinds.pop(); }
    } else if (name === "tool_approval_requested" || name === "tool_approval_resolved") {
      const decoded = yield* Schema.decodeUnknownEffect(approval)(event).pipe(Effect.option);
      if (Option.isNone(decoded) || decoded.value.sessionId !== session) unknown();
      else {
        const key = `approval:${decoded.value.toolCallId}`;
        if (name === "tool_approval_requested") {
          if (inputs.has(key)) unknown(); else inputs.set(key, "approval");
        } else if (!inputs.delete(key)) unknown();
      }
    } else if (name === "agent_end" || name === "session_stop") {
      // OMP's stock end/stop hooks precede possible continuation/async settlement.
      // ponytail: clear uncertain authority; no polling or invented completion hook.
      if (config.host === "omp") base = "unknown";
    } else {
      if (running) base = "working";
    }
    yield* report(ctx);
  });
  const semaphore = yield* Semaphore.make(1);
  let active: PluginActive | undefined;
  let sessionScope: Scope.Closeable | undefined;
  const serial = <A, E, R>(effect: Effect.Effect<A, E, R>) => semaphore.withPermits(1)(effect);
  const call = <const M extends McpToolName>(method: M, params: CommandInput<M>) => mcp.call(config.brokerUrl, token, method, params);
  const notify = (ctx: Context, text: string, level: "info" | "warning" | "error" = "info") => Effect.sync(() => ctx.ui.notify(text, level));
  const confirm = (ctx: Context, title: string, text: string) => Effect.tryPromise({
    try: () => ctx.ui.confirm(title, text),
    catch: () => new CommandError({ message: "Coordinator approval UI unavailable" }),
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
    if (ctx.sessionManager.getSessionId() !== sessionId) return;
    current.repo = (yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote));
    if ((yield* Clock.currentTimeMillis) - current.heartbeat >= 10000) {
      yield* call("session.heartbeat", { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId });
      current.heartbeat = (yield* Clock.currentTimeMillis);
    }
    const poll = yield* call("session.poll", { sessionId, incarnation });
    if (poll.archiveEnabled === false && journal.state.consent) yield* journal.erase();
    for (const message of poll.messages) {
      if (message.incarnation !== incarnation || message.expiresAt <= (yield* Clock.currentTimeMillis)) continue;
      const newlyHandled = yield* journal.receive(message.messageId, message.payload);
      if (newlyHandled) yield* notify(ctx, `Coordinator message ${message.messageId}; /coordinator inbox shows the durably received content.`);
      yield* call("peer.ack", { sessionId, incarnation, messageId: message.messageId });
    }
    for (const offer of poll.offers) {
      const handledKey = JournalHandledKey.make(`offer:${offer.taskId}`);
      if (yield* journal.handled(handledKey)) continue;
      yield* notify(ctx, `Coordinator offer ${offer.taskId}: ${offer.request.repo} ${offer.request.ref} ${offer.request.sha}. /coordinator status {"taskId":"${offer.taskId}"}; then accept or decline explicitly.`);
      yield* journal.handle(handledKey);
    }
    for (const task of poll.releases) {
      const address = task.address;
      if (!address || task.confirmedRevision !== address.revision || task.disposition !== "ready" || !task.readback || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)) || address.sessionId !== sessionId || address.incarnation !== incarnation) continue;
      const approvedAddress = task.instruction?.address;
      if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== task.request.repo || (approvedAddress ? (yield* platform.fingerprint(address)) !== (yield* platform.fingerprint(approvedAddress)) : address.ref !== task.request.ref || address.sha !== task.request.sha) || address.workerId !== workerId) continue;
      if (!ctx.isIdle() || ctx.hasPendingMessages()) continue;
      const actual = yield* local(ctx);
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
      if (!(yield* journal.releaseIntent(key))) continue;
      // Durable intent precedes the host call. A crash/throw is held, never retried.
      yield* Effect.sync(() => host.sendUserMessage(task.instruction?.prompt ?? task.request.prompt, { deliverAs: "followUp" }));
      yield* journal.releaseSent(key);
      yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "HELD", result: { revision: address.revision, deliveryAttempted: true, evidenceVerified: false, reason: "Public extension send does not acknowledge admission; explicit outcome required, never replay" } });
    }
    yield* journal.scan(ctx.sessionManager.getSessionFile());
    yield* journal.upload((entry) => call("archive.append", { sessionId, ...entry }), 1);
  });
  const stop = Effect.fn("Plugin.stop")(function*() {
    const previous = active;
    active = undefined;
    if (sessionScope) { yield* Scope.close(sessionScope, Exit.void); sessionScope = undefined; }
    if (previous) yield* call("session.stop", { sessionId: previous.sessionId, incarnation: previous.incarnation }).pipe(
      Effect.catchCause(() => notify(previous.ctx, "Coordinator stop unavailable; prior enrollment expires by heartbeat lease.", "warning"))
    );
  });
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    yield* (statusContext ? clearStatus(statusContext) : Effect.void).pipe(Effect.ensuring(serial(stop())), Effect.orDie);
  }));
  const start = Effect.fn("Plugin.start")((ctx: Context) => serial(Effect.gen(function*() {
    yield* stop();
    const sessionId = yield* Schema.decodeUnknownEffect(SessionId, { onExcessProperty: "error" })(ctx.sessionManager.getSessionId());
    const workerId = WorkerId.make(sessionId);
    sessionScope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(Journal.layer(config.stateDirectory, sessionId).pipe(Layer.provide(Layer.succeed(Platform, platform))), sessionScope);
    active = { ctx, sessionId, workerId, incarnation: IncarnationId.make(yield* platform.crypto.randomUUIDv4), repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote), journal: EffectContext.get(context, Journal), heartbeat: 0 };
    yield* Effect.gen(function*() {
      yield* Effect.sleep("1 second");
      yield* serial(tick()).pipe(Effect.catchCause(() => notify(ctx, "Coordinator connection or journal unavailable; dispatch held.", "warning")));
    }).pipe(Effect.forever, Effect.forkIn(sessionScope));
    yield* tick();
  })));
  const command = Effect.fn("Plugin.command")((args: string, ctx: Context) => serial(Effect.gen(function*() {
    const split = args.trim().indexOf(" ");
    const action = split < 0 ? args.trim() : args.trim().slice(0, split);
    if (action === "" || action === "help") {
      return yield* notify(ctx, pluginCommands.map(([id, entry]) => `${id.slice("plugin.".length)}: ${entry.description}`).join("\n"));
    }
    const entry = pluginCommands.find(([id]) => id === `plugin.${action}`)?.[1];
    if (!entry) return yield* new CommandError({ message: "Unknown coordinator action; use /coordinator help" });
    const data: unknown = split < 0 ? {} : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Json), { onExcessProperty: "error" })(args.trim().slice(split + 1));
    const current = active;
    if (!current || current.sessionId !== ctx.sessionManager.getSessionId()) return yield* new CommandError({ message: "Session not enrolled" });
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
  static layer(host: Host) { return Layer.effect(this, this.make(host)); }
}

// Public extension callbacks are the only Promise/runtime boundary.
export default async function coordinator(host: Host): Promise<void> {
  const layer = Plugin.layer(host).pipe(Layer.provide(Layer.merge(Platform.layer, Mcp.layer)), Layer.provide(NodeServices.layer));
  let runtime = ManagedRuntime.make(layer);
  try {
    const initial = await runtime.runPromise(Plugin);
    if (!initial) { await runtime.dispose(); return; }
    let disposed = false;
    const start = async (_event: unknown, ctx: Context): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (disposed) { runtime = ManagedRuntime.make(layer); disposed = false; }
      await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.native("session_start", _event, ctx) : Effect.void));
      await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.start(ctx) : Effect.void));
    };
    host.on("session_start", start);
    if (initial.config.host === "omp") host.on("session_switch", start);
    const observe = (name: NativeEventName) => host.on(name, async (event, ctx): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (!disposed) await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.native(name, event, ctx) : Effect.void));
    });
    observe("agent_start");
    observe("agent_end");
    if (initial.config.host === "pi") {
      observe("agent_settled");
      observe("ui_prompt_start");
      observe("ui_prompt_end");
    } else {
      const hooks: NativeEventName[] = ["session_stop", "auto_retry_start", "auto_retry_end", "auto_compaction_start", "auto_compaction_end", "tool_approval_requested", "tool_approval_resolved"];
      for (const name of hooks) observe(name);
    }
    host.on("session_shutdown", async () => { await runtime.dispose(); disposed = true; });
    host.registerCommand("coordinator", { description: "Coordinator: explicit addressed dispatch, status, consent and runner-owned control (JSON arguments)", handler: (args, ctx) => runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.command(args, ctx) : Effect.void).pipe(
      Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator command failed; no automatic retry" }))),
    )) });
  } catch { await runtime.dispose(); throw new CommandError({ message: "Coordinator plugin initialization failed; check private configuration and connection" }); }
}
