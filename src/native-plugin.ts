import { ByteSize, Config, Effect, Exit, Layer, ManagedRuntime, Option, Schema, Scope } from "effect";
import { HttpIncomingMessage, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpServer, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { Buffer } from "node:buffer";
import type { ExtensionAPI as PiExtensionAPI, ExtensionContext as PiContext } from "@earendil-works/pi-coding-agent";
import type { ExtensionContext as OmpContext } from "@oh-my-pi/pi-coding-agent";
import { NativeBinding, NativeControlError, NativeGeneration, NativeIncarnationId, NativeInputId, NativePluginConfig, NativePluginEndpoint, NativePrivateResultBytes, NativeRunId, NativeSessionId, boundedJsonSchema, nativeEventsUri } from "@samuelho/coordinator-native-control/contracts";
import { NativeLifecycleJournal } from "@samuelho/coordinator-native-control/lifecycle";
import { nativeControlHttpLayer, nativeTool } from "@samuelho/coordinator-native-control/server";
import { Platform } from "./platform.ts";
import type { Host, NativeEventName } from "./contracts.ts";

type ContextFields = "cwd" | "mode" | "hasUI" | "sessionManager" | "model" | "isIdle" | "hasPendingMessages" | "abort" | "getSystemPrompt";
type PublicContext = (Pick<PiContext, ContextFields> | Pick<OmpContext, ContextFields>) & { agent?: { kind: "main" | "sub" } | undefined };
type PublicHost = {
  on(name: NativeEventName, handler: (event: unknown, ctx: PublicContext) => Promise<void>): unknown;
  sendUserMessage: Host["sendUserMessage"];
  exec: PiExtensionAPI["exec"];
};
type LifecycleObserver = (name: NativeEventName, event: unknown, ctx: PublicContext) => Effect.Effect<void, unknown>;
const approval = Schema.Struct({ sessionId: Schema.String, toolCallId: Schema.String });
const prompt = Schema.Struct({ kind: Schema.Literals(["select", "confirm", "input", "editor", "custom"]) });
const phases: Record<string, { phase: "retry" | "compaction"; transition: "started" | "ended" }> = {
  auto_retry_start: { phase: "retry", transition: "started" }, auto_retry_end: { phase: "retry", transition: "ended" },
  auto_compaction_start: { phase: "compaction", transition: "started" }, auto_compaction_end: { phase: "compaction", transition: "ended" },
};

const transcriptJson = Effect.fn("Plugin.transcriptJson")(function*(value: unknown, kind: "entry" | "message") {
  // Match the official JSON wire: omit optional undefined fields and honor public toJSON values.
  const serialized = yield* Effect.try({
    try: () => JSON.stringify(value),
    catch: () => new NativeControlError({ code: "native_failure", message: `Public transcript ${kind} is not JSON serializable` }),
  });
  const json = yield* Schema.decodeUnknownEffect(Schema.String)(serialized).pipe(Effect.mapError(() =>
    new NativeControlError({ code: "native_failure", message: `Public transcript ${kind} is not JSON serializable` })));
  if (Buffer.byteLength(json, "utf8") > NativePrivateResultBytes) return yield* new NativeControlError({ code: "native_failure", message: `Public transcript ${kind} exceeds the native contract` });
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(boundedJsonSchema))(json).pipe(Effect.mapError(() =>
    new NativeControlError({ code: "native_failure", message: `Public transcript ${kind} exceeds the native contract` })));
});

const makeControl = Effect.fn("Plugin.nativeControl")(function*(config: NativePluginConfig, initial: PublicContext, host: PublicHost) {
  const platform = yield* Platform;
  if (!platform.path.isAbsolute(config.endpointFile) || !platform.path.isAbsolute(config.tokenFile) ||
    (yield* platform.fs.realPath(config.tokenFile)) !== config.tokenFile ||
    ((yield* platform.fs.stat(platform.path.dirname(config.endpointFile))).mode & 0o077) !== 0) {
    return yield* new NativeControlError({ code: "invalid_input", message: "Canonical private plugin control paths required" });
  }
  if (yield* platform.fs.exists(config.endpointFile)) return yield* new NativeControlError({ code: "binding_mismatch", message: "Plugin endpoint is already owned; refusing to replace it" });
  const token = yield* platform.privateFile(config.tokenFile);
  if (token.length < 32 || token.length > 480) return yield* new NativeControlError({ code: "invalid_input", message: "Invalid private plugin credential" });
  const tokenHash = yield* platform.digest(token);
  const binding: NativeBinding = {
    host: config.host, sessionId: yield* Schema.decodeUnknownEffect(NativeSessionId)(initial.sessionManager.getSessionId()).pipe(Effect.mapError(() => new NativeControlError({ code: "invalid_input", message: "Invalid public session identity" }))),
    incarnation: NativeIncarnationId.make(yield* platform.crypto.randomUUIDv4), generation: NativeGeneration.make(1),
  };
  let ctx = initial;
  let runId: NativeRunId | null = null;
  const journal = new NativeLifecycleJournal();
  const questions: { id: NativeInputId; kind: string }[] = [];
  const approvals = new Map<string, NativeInputId>();
  journal.append({ type: "native_lifecycle", kind: "session_started", binding, runId: null, at: Date.now() });
  const bound = (requested?: NativeBinding) => Effect.gen(function*() {
    if (ctx.sessionManager.getSessionId() !== binding.sessionId || (requested !== undefined && nativeEventsUri(requested) !== nativeEventsUri(binding))) {
      return yield* new NativeControlError({ code: "binding_mismatch", message: "Public extension owner generation changed" });
    }
  });
  const tools = [
    nativeTool("native.get_state", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const model = ctx.model;
      const sessionFile = ctx.sessionManager.getSessionFile();
      return {
        binding, ...(sessionFile === undefined ? {} : { sessionFile }), cwd: ctx.cwd,
        model: model === undefined ? null : { id: model.id, name: model.name, api: model.api, provider: model.provider, reasoning: model.reasoning,
          input: model.input, cost: model.cost, contextWindow: model.contextWindow ?? null, maxTokens: model.maxTokens ?? null },
        currentRunId: runId, isStreaming: !ctx.isIdle(), isCompacting: null,
        pendingMessageCount: ctx.hasPendingMessages() ? null : 0, pendingAsyncWorkCount: null,
        messageCount: ctx.sessionManager.getBranch().reduce((count, entry) => entry.type === "message" ? count + 1 : count, 0),
        systemPrompt: ctx.getSystemPrompt(),
      };
    })),
    nativeTool("native.get_messages", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = ctx.sessionManager.getBranch().filter((entry) => entry.type === "message");
      const previous = input.cursor === undefined ? -1 : entries.findIndex((entry) => entry.id === input.cursor);
      if (input.cursor !== undefined && previous < 0) return yield* new NativeControlError({ code: "resource_gap", message: "Transcript cursor is no longer on this branch" });
      const page = entries.slice(previous + 1, previous + 1 + (input.limit ?? 128));
      const more = previous + 1 + page.length < entries.length;
      return { binding, messages: yield* Effect.forEach(page, (entry) => transcriptJson(entry.message, "message")),
        nextCursor: more ? page.at(-1)?.id ?? null : null, more };
    })),
    nativeTool("native.abort", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      ctx.abort();
      return { binding, runId, disposition: "requested" };
    })),
    nativeTool("native.steer", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      if (input.images !== undefined) return yield* new NativeControlError({ code: "unsupported_operation", message: "Image steering is not exposed by this plugin" });
      host.sendUserMessage(input.message, { deliverAs: "steer" });
      return null;
    })),
    nativeTool("native.follow_up", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      if (input.images !== undefined) return yield* new NativeControlError({ code: "unsupported_operation", message: "Image follow-up is not exposed by this plugin" });
      host.sendUserMessage(input.message, { deliverAs: "followUp" });
      return null;
    })),
    nativeTool("native.get_entries", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = ctx.sessionManager.getEntries();
      const previous = input.since === undefined ? -1 : entries.findIndex((entry) => entry.id === input.since);
      if (input.since !== undefined && previous < 0) return yield* new NativeControlError({ code: "resource_gap", message: "Append-history cursor is unavailable" });
      return { entries: yield* Effect.forEach(entries.slice(previous + 1), (entry) => transcriptJson(entry, "entry")), leafId: ctx.sessionManager.getLeafId() };
    })),
    nativeTool("native.bash", (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      if (input.excludeFromContext === false) return yield* new NativeControlError({ code: "unsupported_operation", message: "Public extension exec cannot append native bash transcript entries" });
      const result = yield* Effect.tryPromise({
        try: (signal) => host.exec("/bin/sh", ["-c", input.command], { cwd: ctx.cwd, signal, timeout: 30000 }),
        catch: () => new NativeControlError({ code: "native_failure", message: "Public extension command execution failed" }),
      });
      return { binding, exitCode: result.code === -1 ? null : result.code, output: result.stdout + result.stderr, truncated: false, killed: result.killed };
    })),
  ];
  // ponytail: publish only real public capabilities. Neither stock host exposes
  // the admission plus final-outcome proof required by native.prompt.
  const app = yield* HttpRouter.toHttpEffect(nativeControlHttpLayer({
    host: config.host, binding, journal, tools, name: "coordinator-public-plugin", version: "0.1.0",
  }));
  const server = yield* NodeHttpServer.make(() => createServer({ requestTimeout: 15000 }), { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" });
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const authorization = request.headers.authorization;
    if (!authorization?.startsWith("Bearer ") || authorization.length > 512 || !(yield* platform.checkToken(authorization.slice(7), tokenHash))) {
      return HttpServerResponse.empty({ status: 401, headers: { "cache-control": "no-store" } });
    }
    return yield* app.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(64 * 1024)));
  }));
  const address = server.address;
  if (address._tag !== "InetAddressV4") return yield* new NativeControlError({ code: "native_failure", message: "Plugin control requires an IPv4 loopback address" });
  const pendingEndpoint = `${config.endpointFile}.${binding.incarnation}.pending`;
  yield* Effect.acquireRelease(
    platform.fs.writeFileString(pendingEndpoint, Schema.encodeSync(Schema.fromJsonString(NativePluginEndpoint))({ url: `http://127.0.0.1:${address.port}/mcp`, binding }), { flag: "wx", mode: 0o600 }),
    () => platform.fs.remove(pendingEndpoint, { force: true }).pipe(Effect.orDie),
  );
  yield* platform.fs.link(pendingEndpoint, config.endpointFile);
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    journal.append({ type: "native_lifecycle", kind: "session_retired", binding, runId, at: Date.now(), reason: "public extension session closed" });
    yield* platform.fs.remove(config.endpointFile);
  }).pipe(Effect.orDie));
  yield* platform.fs.remove(pendingEndpoint);
  return Effect.fn("Plugin.nativeLifecycle")(function*(name: NativeEventName, event: unknown, current: PublicContext) {
    if (current.sessionManager.getSessionId() !== binding.sessionId) return;
    ctx = current;
    if (name === "agent_start" && runId === null) {
      runId = NativeRunId.make(yield* platform.crypto.randomUUIDv4);
      journal.append({ type: "native_lifecycle", kind: "run_started", binding, runId, at: Date.now() });
    } else if (name === "agent_settled" && config.host === "pi") {
      // The public hook proves quiescence, not completed/aborted/error. No final event.
      runId = null;
    } else if (name === "ui_prompt_start" || name === "ui_prompt_end") {
      const input = yield* Schema.decodeUnknownEffect(prompt)(event).pipe(Effect.mapError(() => new NativeControlError({ code: "native_failure", message: "Invalid public prompt lifecycle event" })));
      if (name === "ui_prompt_start") {
        const id = NativeInputId.make(yield* platform.crypto.randomUUIDv4);
        questions.push({ id, kind: input.kind });
        journal.append({ type: "native_lifecycle", kind: "input_requested", binding, runId, at: Date.now(), inputId: id, inputKind: "question" });
      } else {
        const previous = questions.pop();
        if (previous === undefined || previous.kind !== input.kind) return yield* new NativeControlError({ code: "native_failure", message: "Public prompt lifecycle is unbalanced" });
        journal.append({ type: "native_lifecycle", kind: "input_resolved", binding, runId, at: Date.now(), inputId: previous.id, inputKind: "question" });
      }
    } else if (name === "tool_approval_requested" || name === "tool_approval_resolved") {
      const input = yield* Schema.decodeUnknownEffect(approval)(event).pipe(Effect.mapError(() => new NativeControlError({ code: "native_failure", message: "Invalid public approval lifecycle event" })));
      if (input.sessionId !== binding.sessionId) return;
      if (name === "tool_approval_requested") {
        if (approvals.has(input.toolCallId)) return yield* new NativeControlError({ code: "native_failure", message: "Duplicate public approval request" });
        const id = NativeInputId.make(yield* platform.crypto.randomUUIDv4);
        approvals.set(input.toolCallId, id);
        journal.append({ type: "native_lifecycle", kind: "input_requested", binding, runId, at: Date.now(), inputId: id, inputKind: "approval" });
      } else {
        const id = approvals.get(input.toolCallId);
        if (id === undefined) return yield* new NativeControlError({ code: "native_failure", message: "Public approval resolution has no owner" });
        approvals.delete(input.toolCallId);
        journal.append({ type: "native_lifecycle", kind: "input_resolved", binding, runId, at: Date.now(), inputId: id, inputKind: "approval" });
      }
    } else {
      const phase = phases[name];
      if (phase !== undefined) journal.append({ type: "native_lifecycle", kind: "run_phase", binding, runId, at: Date.now(), ...phase });
    }
  });
});

export default async function nativeControl(host: PublicHost): Promise<void> {
  let runtime = ManagedRuntime.make(Layer.merge(Platform.layer, NodeServices.layer));
  let stopped = false;
  try {
    const path = await runtime.runPromise(Config.option(Config.String("COORDINATOR_NATIVE_CONFIG")));
    if (Option.isNone(path)) { await runtime.dispose(); return; }
    const config = await runtime.runPromise(Effect.gen(function*() {
      const platform = yield* Platform;
      if (!platform.path.isAbsolute(path.value)) return yield* new NativeControlError({ code: "invalid_input", message: "Absolute plugin configuration path required" });
      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativePluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path.value)).pipe(Effect.mapError(() => new NativeControlError({ code: "invalid_input", message: "Invalid private native plugin configuration" })));
    }));
    let scope: Scope.Closeable | undefined;
    let observe: LifecycleObserver | undefined;
    const start = async (_event: unknown, ctx: PublicContext): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (stopped) { runtime = ManagedRuntime.make(Layer.merge(Platform.layer, NodeServices.layer)); stopped = false; }
      observe = undefined;
      if (scope !== undefined) await runtime.runPromise(Scope.close(scope, Exit.void));
      const currentScope = await runtime.runPromise(Scope.make());
      scope = currentScope;
      observe = await runtime.runPromise(makeControl(config, ctx, host).pipe(
        Effect.provideService(Scope.Scope, currentScope),
        Effect.onExit((exit) => exit._tag === "Failure" ? Scope.close(currentScope, exit) : Effect.void),
      ));
    };
    host.on("session_start", start);
    if (config.host === "omp") host.on("session_switch", start);
    const hooks: NativeEventName[] = config.host === "pi"
      ? ["agent_start", "agent_end", "agent_settled", "ui_prompt_start", "ui_prompt_end"]
      : ["agent_start", "agent_end", "session_stop", "auto_retry_start", "auto_retry_end", "auto_compaction_start", "auto_compaction_end", "tool_approval_requested", "tool_approval_resolved"];
    for (const name of hooks) host.on(name, async (event, ctx): Promise<void> => {
      if (ctx.agent?.kind !== "sub" && observe !== undefined) await runtime.runPromise(observe(name, event, ctx));
    });
    host.on("session_shutdown", async () => {
      observe = undefined;
      if (scope !== undefined) { await runtime.runPromise(Scope.close(scope, Exit.void)); scope = undefined; }
      stopped = true;
      await runtime.dispose();
    });
  } catch { await runtime.dispose(); throw new NativeControlError({ code: "native_failure", message: "Public native plugin initialization failed; check private configuration" }); }
}
