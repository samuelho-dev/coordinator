import { Clock, Config, Context as EffectContext, Effect, Exit, Layer, ManagedRuntime, Option, Scope, Semaphore, Stdio as EffectStdio, Stream } from "effect";
import { Base64 } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { Commands, CommandError, PluginConfig, PluginDeclined, Json, type CommandCapabilities, type CommandInput, type Context, type Host, type PluginActive, type McpToolName, type NativeEventName } from "./contracts.ts";
import { decodeJson, decodeEffect, canonicalJson, Mcp } from "./http.ts";
import { Platform } from "./platform.ts";
import { Journal } from "./journal.ts";

const pluginCommands = Object.entries(Commands).filter((entry): entry is [string, Extract<(typeof Commands)[keyof typeof Commands], { target: "plugin" }>] => entry[1].target === "plugin");
const makePlugin = Effect.fn("Plugin.make")(function*(host: Host) {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const configuredPath = yield* Config.option(Config.String("COORDINATOR_CONFIG"));
  if (Option.isNone(configuredPath)) return undefined;
  const path = configuredPath.value;
  if (!platform.path.isAbsolute(path)) return yield* Effect.fail(new CommandError({ message: "COORDINATOR_CONFIG must be absolute" }));
  const config = decodeJson(PluginConfig, yield* platform.privateFile(path));
  if (!platform.path.isAbsolute(config.stateDirectory) || !platform.path.isAbsolute(config.tokenFile) || !["omp", "pi"].includes(config.host) || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(config.machineId)) return yield* Effect.fail(new CommandError({ message: "Invalid coordinator configuration" }));
  const token = yield* platform.privateFile(config.tokenFile);
  const stdio = yield* EffectStdio.Stdio;
  const terminal = yield* stdio.stdoutIsTerminal;
  const tui = (ctx: Context) => terminal && ctx.hasUI && ctx.mode === "tui";
  let statusContext: Context | undefined;
  let base: "idle" | "working" | "done" | "error" = "idle";
  let outcome: "completed" | "aborted" | "error" = "completed";
  let confirmations = 0;
  let questions = 0;
  const approvals = new Set<string>();
  // ponytail: one bounded root record; no IDs, progress, titles or heartbeat.
  const permissionMessage = Base64.encode("Waiting for permission");
  const questionMessage = Base64.encode("Waiting for an answer");
  const report = Effect.fn("Plugin.reportStatus")(function*(ctx: Context, clear = false) {
    statusContext = ctx;
    if (!tui(ctx)) return;
    const permission = confirmations > 0 || approvals.size > 0;
    const kind = permission ? "permission" : questions > 0 ? "question" : undefined;
    const fields = clear ? "state=clear" : kind ? `state=blocked:kind=${kind}:msg=${permission ? permissionMessage : questionMessage}` : `state=${base}`;
    yield* Stream.make(`\x1b]7501;app=${config.host}:${fields}\x1b\\`).pipe(Stream.run(stdio.stdout({ endOnDone: false })), Effect.catchCause(() => Effect.void));
  });
  const resetStatus = Effect.fn("Plugin.resetStatus")(function*(ctx: Context, exiting: boolean) {
    const blocked = confirmations > 0 || approvals.size > 0 || questions > 0;
    confirmations = 0;
    questions = 0;
    approvals.clear();
    if (!exiting || blocked || (base !== "done" && base !== "error")) yield* report(ctx, true);
    if (!exiting) { base = "idle"; outcome = "completed"; }
  });
  const native = Effect.fn("Plugin.nativeStatus")(function*(name: NativeEventName, event: unknown, ctx: Context) {
    const data = typeof event === "object" && event !== null ? event : undefined;
    if (name === "agent_start") { base = "working"; outcome = "completed"; }
    if (name === "input") {
      if (!ctx.isIdle()) return;
      base = "idle";
      outcome = "completed";
    }
    if (name === "turn_end" || name === "message_end") {
      const message = data && "message" in data ? data.message : undefined;
      if (typeof message === "object" && message !== null && "role" in message && message.role === "assistant" && "stopReason" in message) {
        outcome = message.stopReason === "aborted" ? "aborted" : message.stopReason === "error" ? "error" : "completed";
      }
    }
    if (name === "agent_before_settle" || name === "turn_end") {
      if (data && "outcome" in data && (data.outcome === "completed" || data.outcome === "aborted" || data.outcome === "error")) outcome = data.outcome;
    }
    if (name === "agent_end") {
      if (data && "willContinue" in data && data.willContinue === true) return;
      // Pi >=0.80.4 settles after automatic retry/compaction/continuation.
      if (config.host === "pi") return;
      if (ctx.hasPendingMessages()) return;
      base = outcome === "aborted" ? "idle" : outcome === "error" ? "error" : "done";
    }
    if (name === "agent_settled") base = outcome === "aborted" ? "idle" : outcome === "error" ? "error" : "done";
    if (name === "tool_approval_requested" && tui(ctx) && data && "toolCallId" in data && typeof data.toolCallId === "string") approvals.add(data.toolCallId);
    if (name === "tool_approval_resolved" && data && "toolCallId" in data && typeof data.toolCallId === "string") approvals.delete(data.toolCallId);
    if (name === "ui_prompt_start" && tui(ctx) && data && "kind" in data && typeof data.kind === "string" && ["select", "confirm", "input", "editor", "custom"].includes(data.kind)) questions++;
    if (name === "ui_prompt_end") questions = Math.max(0, questions - 1);
    yield* report(ctx);
  });
  const semaphore = yield* Semaphore.make(1);
  let active: PluginActive | undefined;
  let sessionScope: Scope.Closeable | undefined;
  const serial = <A, E>(effect: Effect.Effect<A, E>) => semaphore.withPermits(1)(effect);
  const call = <const M extends McpToolName>(method: M, params: unknown) => mcp.call(config.brokerUrl, token, method, params);
  const notify = (ctx: Context, text: string, level: "info" | "warning" | "error" = "info") => Effect.sync(() => ctx.ui.notify(text, level));
  const confirm = (ctx: Context, title: string, text: string) => Effect.acquireUseRelease(
    Effect.sync(() => { confirmations++; }),
    () => report(ctx).pipe(Effect.andThen(Effect.tryPromise(() => ctx.ui.confirm(title, text)))),
    () => Effect.gen(function*() {
      confirmations = Math.max(0, confirmations - 1);
      yield* report(ctx);
    }),
  );
  const local = Effect.fn("Plugin.local")(function*(ctx: Context) {
    return { repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote),
      ref: (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: ctx.cwd }))).trim(),
      sha: (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: ctx.cwd }))).trim() };
  });
  const tick = Effect.fn("Plugin.tick")(function*() {
    const current = active;
    if (!current) return;
    const { ctx, sessionId, incarnation, journal } = current;
    if (ctx.sessionManager.getSessionId() !== sessionId) return;
    current.repo = (yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote));
    if ((yield* Clock.currentTimeMillis) - current.heartbeat >= 10000) {
      yield* call("session.heartbeat", { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId: sessionId });
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
      if (yield* journal.handled(`offer:${offer.taskId}`)) continue;
      yield* notify(ctx, `Coordinator offer ${offer.taskId}: ${offer.request.repo} ${offer.request.ref} ${offer.request.sha}. /coordinator status {"taskId":"${offer.taskId}"}; then accept or decline explicitly.`);
      yield* journal.handle(`offer:${offer.taskId}`);
    }
    for (const task of poll.releases) {
      const address = task.address;
      if (!address || task.confirmedRevision !== address.revision || task.disposition !== "ready" || !task.readback || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)) || address.sessionId !== sessionId || address.incarnation !== incarnation) continue;
      const approvedAddress = task.instruction?.address;
      if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== task.request.repo || (approvedAddress ? (yield* platform.fingerprint(address)) !== (yield* platform.fingerprint(approvedAddress)) : address.ref !== task.request.ref || address.sha !== task.request.sha) || address.workerId !== sessionId) continue;
      if (!ctx.isIdle() || ctx.hasPendingMessages()) continue;
      const actual = yield* local(ctx);
      if (actual.repo !== address.repo || actual.ref !== address.ref || actual.sha !== address.sha) {
        yield* call("task.readback", { taskId: task.taskId, address, disposition: "hold", reason: "Local repository/ref/SHA changed", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
        continue;
      }
      const key = `${task.taskId}:${address.revision}`;
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
      yield* call("task.report", { taskId: task.taskId, sessionId, incarnation, state: "RUNNING", result: { revision: address.revision, promptAdmitted: true, evidenceVerified: false } });
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
    if (statusContext) yield* resetStatus(statusContext, true);
    yield* serial(stop());
  }));
  const start = Effect.fn("Plugin.start")((ctx: Context) => serial(Effect.gen(function*() {
    yield* stop();
    yield* resetStatus(ctx, false);
    yield* report(ctx);
    const sessionId = ctx.sessionManager.getSessionId();
    sessionScope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(Journal.layer(config.stateDirectory, sessionId).pipe(Layer.provide(Layer.succeed(Platform, platform))), sessionScope);
    active = { ctx, sessionId, incarnation: yield* platform.crypto.randomUUIDv4, repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote), journal: EffectContext.get(context, Journal), heartbeat: 0 };
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
    if (!entry) throw new Error("Unknown coordinator action; use /coordinator help");
    const data: unknown = split < 0 ? {} : decodeJson(Json, args.trim().slice(split + 1));
    const current = active;
    if (!current || current.sessionId !== ctx.sessionManager.getSessionId()) throw new Error("Session not enrolled");
    const { sessionId, incarnation, journal } = current;
    const approve = Effect.fn("Plugin.approve")(function*(title: string, text: string) {
      if (!ctx.hasUI || !(yield* confirm(ctx, title, text))) return yield* Effect.fail(new PluginDeclined({ message: "Human approval declined or unavailable" }));
    });
    const readback = Effect.fn("Plugin.readback")(function*(data: CommandInput<"plugin.readback">, disposition: "ready" | "hold" | "clarify" | "unable") {
      const { address } = data;
      if (address.sessionId !== sessionId || address.incarnation !== incarnation || address.workerId !== sessionId) return yield* Effect.fail(new CommandError({ message: "Readback must address this native session incarnation and worker" }));
      if (disposition === "ready") {
        const actual = yield* local(ctx);
        if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== actual.repo || address.sha !== actual.sha || address.ref !== actual.ref) return yield* Effect.fail(new CommandError({ message: "Readback does not match current local repository/ref/SHA/worker/deadline" }));
      }
      return yield* call("task.readback", { ...data, disposition });
    });
    const capabilities: CommandCapabilities<"plugin"> = {
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
        yield* approve("Confirm coordinator release", `Release this exact ready addressed revision?\n${canonicalJson(data)}`);
        return yield* call("task.confirm", data);
      }),
      pluginHandoff: (data) => Effect.gen(function*() {
        yield* approve("Approve coordinator handoff", `Transfer ownership to this exact native recipient?\n${canonicalJson(data)}`);
        return yield* call("task.handoff", data);
      }),
      pluginInstruction: (instruction) => Effect.gen(function*() {
        yield* approve("Approve later addressed instruction", `Approve this exact new addressed revision? Existing native work is not retroactively gated.\n${canonicalJson(instruction)}`);
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
        if (data.type !== "get_state") yield* approve("Approve sensitive native control", `${data.type === "abort" ? "Abort this native child; the task remains held for reconciliation" : "Read potentially sensitive native child messages"}?\n${canonicalJson(data)}`);
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
        return { sessionId, incarnation, machineId: config.machineId, ...(yield* local(ctx)), workerId: sessionId, workerIdentitySource: "native-session" as const };
      }),
    };
    const result = yield* entry.executeDecoded(capabilities, data, decodeEffect);
    // Status/readback is user-visible, not model context. Never echo credentials or arbitrary server errors.
    yield* notify(ctx, canonicalJson(result));
  }).pipe(
    Effect.catch((error) => error instanceof PluginDeclined ? Effect.void : Effect.fail(error)),
    Effect.catchCause(() => notify(ctx, "Coordinator command failed; no automatic retry. Check exact JSON fields, authorization and connection.", "error")),
  )));
  return { config, start, stop: () => serial(stop()), command, native };
});

export class Plugin extends EffectContext.Service<Plugin>()("coordinator/Plugin", { make: makePlugin }) {
  static layer(host: Host) { return Layer.effect(this, this.make(host)); }
}

// This initializer and native SDK callbacks are the only Promise/runtime boundary.
export default async function coordinator(host: Host): Promise<void> {
  const layer = Plugin.layer(host).pipe(Layer.provide(Layer.merge(Platform.layer, Mcp.layer)), Layer.provide(NodeServices.layer));
  let runtime = ManagedRuntime.make(layer);
  try {
    const initial = await runtime.runPromise(Plugin);
    if (!initial) { await runtime.dispose(); return; }
    let disposed = false;
    const start = async (_event: unknown, ctx: Context): Promise<void> => {
      if (disposed) { runtime = ManagedRuntime.make(layer); disposed = false; }
      await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.start(ctx) : Effect.void));
    };
    host.on("session_start", start);
    if (initial.config.host === "omp") host.on("session_switch", start);
    const observe = (name: NativeEventName) => host.on(name, async (event, ctx): Promise<void> => {
      if (!disposed) await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.native(name, event, ctx) : Effect.void));
    });
    for (const name of ["input", "agent_start", "agent_end", "turn_end", "message_end"] satisfies NativeEventName[]) observe(name);
    if (initial.config.host === "pi") {
      for (const name of ["agent_before_settle", "agent_settled", "ui_prompt_start", "ui_prompt_end"] satisfies NativeEventName[]) observe(name);
    } else {
      for (const name of ["tool_approval_requested", "tool_approval_resolved"] satisfies NativeEventName[]) observe(name);
    }
    host.on("session_shutdown", async () => { await runtime.dispose(); disposed = true; });
    host.registerCommand("coordinator", { description: "Coordinator: explicit addressed dispatch, status, consent and runner-owned control (JSON arguments)", handler: (args, ctx) => runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.command(args, ctx) : Effect.void)) });
  } catch (error) { await runtime.dispose(); throw error; }
}
