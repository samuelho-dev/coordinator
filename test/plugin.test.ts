import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Clock, ConfigProvider, Context as EffectContext, Deferred, Effect, Layer, PlatformError, Schema, Sink, Stdio } from "effect";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { Mcp, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { CommandError, HttpError, Task, Principal, PluginConfig, SessionId, JournalReleaseKey, type Context, type BrokerRequest, type IncarnationId } from "../src/contracts.ts";
import { Plugin } from "../src/plugin.ts";

test("native plugin persists intent before send and holds uncertain delivery after scoped restart", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-plugin-" });
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["-c", "user.name=Coordinator Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "test"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const sha = (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: directory }))).trim();
  const stateDirectory = platform.path.join(directory, "state");
  yield* platform.fs.makeDirectory(stateDirectory, { mode: 0o700 });
  const tokenFile = platform.path.join(directory, "token");
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const config = platform.path.join(directory, "config.json");
  let approval = true;
  const terminalOutput: string[] = [];
  const notifications: { text: string; level: "info" | "warning" | "error" }[] = [];
  const status = (frame = terminalOutput.at(-1)) => Object.fromEntries((frame?.slice(7, -2) ?? "").split(":").map((pair) => {
    const equals = pair.indexOf("=");
    return [pair.slice(0, equals), pair.slice(equals + 1)];
  }));
  const ctx: Context = { cwd: directory, mode: "tui", hasUI: true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "native-session", getSessionFile: () => undefined },
    ui: { notify: (text, level = "info") => { notifications.push({ text, level }); }, confirm: async () => approval } };
  let sends = 0;
  let incarnation: IncarnationId | undefined;
  const reports: Extract<BrokerRequest, { method: "task.report" }>["params"][] = [];
  let requests = 0;
  const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
  const server = EffectContext.get(serverContext, HttpServer.HttpServer);
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] })),
    handle: (_principal, request) => Effect.gen(function*() {
      requests++;
      const now = yield* Clock.currentTimeMillis;
      if (request.method === "session.heartbeat") { incarnation = request.params.incarnation; return { liveUntil: now + 30000 }; }
      if (request.method === "session.stop") return { stopped: true };
      const address = { messageId: "message", revision: 1, sessionId: "native-session", incarnation, workerId: "native-session", repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: now + 60000 };
      const work = { taskId: "task", prompt: "Substantive task", repo: address.repo, ref: address.ref, sha, launcher: "pi", scope: address.scope, restrictions: address.restrictions, expectedEvidence: [], deadline: address.deadline, workerId: address.workerId, githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
      const task = Schema.decodeUnknownSync(Task, { onExcessProperty: "error" })({ taskId: "task", owner: "owner", fingerprint: yield* platform.fingerprint(work), state: "ACCEPTED", createdAt: now, updatedAt: now, address, readback: address, confirmedRevision: 1, disposition: "ready", request: work });
      if (request.method === "session.poll") return { messages: [], offers: [], releases: [task] };
      if (request.method === "task.report") {
        const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, SessionId.make("native-session")).pipe(Layer.provide(Layer.succeed(Platform, platform)))), Journal);
        assert.equal(persisted.state.releases[JournalReleaseKey.make("task:1")], "intent");
        reports.push(request.params);
        return { ...task, state: request.params.state, result: request.params.result };
      }
      return yield* Effect.fail(new CommandError({ message: `Unexpected route ${request.method}` }));
    }).pipe(Effect.scoped),
  });
  if (!("port" in server.address)) return yield* Effect.fail(new CommandError({ message: "Expected TCP server" }));
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const pluginContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => { sends++; throw new CommandError({ message: "native delivery uncertain" }); } }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({
      stdoutIsTerminal: Effect.succeed(true),
      stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => {
        if (typeof chunk === "string") terminalOutput.push(chunk);
      })),
    })),
  ));
  const plugin = EffectContext.get(pluginContext, Plugin);
  if (!plugin) return yield* Effect.fail(new CommandError({ message: "Expected configured plugin" }));
  assert.equal((yield* Effect.exit(plugin.start(ctx)))._tag, "Failure");
  const emit = (name: Parameters<typeof plugin.native>[0], event: unknown = {}, context = ctx) => plugin.native(name, event, context);
  yield* emit("session_start");
  assert.equal(status().state, "idle", "Broker admission is not execution");
  yield* emit("ui_prompt_start", { kind: "confirm", title: "secret" });
  assert.equal(status().kind, "question", "Stock Pi confirm is a question, not proof of tool permission");
  yield* emit("ui_prompt_end", { kind: "confirm" });
  assert.equal(status().state, "idle");
  yield* emit("agent_start");
  yield* emit("agent_end", { messages: [{ role: "assistant", stopReason: "stop", content: "secret" }] });
  assert.equal(status().state, "working", "Per-loop completion is not settlement");
  yield* emit("agent_before_settle", { outcome: "error" });
  assert.equal(status().state, "working", "Pre-settlement evidence is not a terminal fence");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Stock Pi settlement does not publish a final outcome");
  const beforeNativeControl = terminalOutput.length;
  yield* emit("session_shutdown", {}, { ...ctx, mode: "rpc" });
  assert.equal(terminalOutput.length, beforeNativeControl, "MCP must never receive terminal OSC");
  assert.equal(sends, 1);
  const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, SessionId.make("native-session"))), Journal);
  assert.equal(persisted.state.releases[JournalReleaseKey.make("task:1")], "intent");
  yield* plugin.stop();
  assert.ok((yield* Effect.flip(plugin.command("identity", ctx))) instanceof CommandError, "A retired session fails instead of succeeding with a diagnostic");
  yield* plugin.start(ctx);
  const priorRequests = requests;
  assert.ok((yield* Effect.flip(plugin.command('report {"taskId":"task","state":"HELD","result":{},"sessionId":"forged"}', ctx))) instanceof HttpError);
  assert.equal(requests, priorRequests, "Catalog rejects model-supplied session identity before remote execution");
  assert.ok((yield* Effect.flip(plugin.command("unknown", ctx))) instanceof CommandError);
  yield* Effect.flip(plugin.command('report {"secret":"private-command-content"', ctx));
  assert.equal(requests, priorRequests, "Unknown or malformed commands must fail before remote execution");
  assert.equal(notifications.at(-1)?.level, "error");
  assert.ok(!notifications.some(({ text }) => text.includes("private-command-content")), "Diagnostics must not expose malformed command content");
  approval = false;
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("ui_prompt_start", { kind: "confirm" });
  assert.equal(status().kind, "question");
  yield* plugin.command('confirm {"taskId":"task","revision":1}', ctx);
  assert.equal(status().state, "blocked", "Broker approval handling cannot resolve public UI input");
  yield* emit("ui_prompt_end", { kind: "confirm" });
  yield* emit("agent_before_settle", { outcome: "aborted" });
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Pre-settle cancellation is not a final public outcome");
  yield* plugin.command('control {"taskId":"task","type":"get_messages"}', ctx);
  yield* plugin.command('control {"taskId":"task","type":"abort"}', ctx);
  assert.equal(requests, priorRequests, "Declined native approval must not reach the broker");
  assert.equal(sends, 1);
  assert.equal(reports[0]?.state, "HELD");
  yield* plugin.stop();
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "omp" }), { mode: 0o600 });
  const ompContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => { assert.fail("Status must not dispatch work"); } }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({
      stdoutIsTerminal: Effect.succeed(true),
      stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => {
        if (typeof chunk === "string") terminalOutput.push(chunk);
      })),
    })),
  ));
  const omp = EffectContext.get(ompContext, Plugin);
  if (!omp) return yield* new CommandError({ message: "Expected configured OMP plugin" });
  const ompEmit = (name: Parameters<typeof omp.native>[0], event: unknown = {}, context = ctx) => omp.native(name, event, context);
  yield* ompEmit("session_start");
  yield* ompEmit("agent_start");
  yield* ompEmit("auto_retry_start", { attempt: 1, errorMessage: "secret" });
  yield* ompEmit("auto_retry_end", { success: true });
  assert.equal(status().state, "working", "Retry success does not settle owned work");
  yield* ompEmit("tool_approval_requested", { sessionId: "native-session", toolCallId: "private-permission", toolName: "secret" });
  assert.equal(status().kind, "permission");
  yield* ompEmit("tool_approval_resolved", { sessionId: "native-session", toolCallId: "private-permission", approved: true });
  assert.equal(status().state, "working", "Approval resolution is not settlement");
  yield* ompEmit("agent_end", { willContinue: false }, { ...ctx, isIdle: () => true, hasPendingMessages: () => false });
  assert.equal(status().state, "clear", "Even idle/empty snapshots and willContinue=false do not prove physical settlement");
  yield* ompEmit("session_stop", { messages: [], turn_id: 1 });
  assert.equal(status().state, "clear", "Pre-settlement stop may still request continuation");
  yield* ompEmit("session_switch");
  yield* ompEmit("agent_start");
  yield* ompEmit("tool_approval_requested", { sessionId: "native-session", toolCallId: "waiting" });
  yield* ompEmit("tool_approval_resolved", { sessionId: "native-session", toolCallId: "wrong" });
  assert.equal(status().state, "clear", "Unmatched approval resolution loses authority");
  yield* ompEmit("agent_start");
  assert.equal(status().state, "clear", "A later event cannot repair a lost lifecycle");
  yield* ompEmit("session_switch");
  yield* ompEmit("tool_approval_requested", { sessionId: "previous-session", toolCallId: "waiting" });
  assert.equal(status().state, "clear", "Previous-session approval cannot block the new session");

  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("ui_prompt_start", { kind: "input", title: "secret" });
  yield* emit("ui_prompt_start", { kind: "confirm" });
  yield* emit("ui_prompt_end", { kind: "confirm" });
  assert.equal(status().state, "blocked", "Nested prompt resolution retains the outer wait");
  yield* emit("ui_prompt_end", { kind: "editor" });
  assert.equal(status().state, "clear", "Mismatched prompt resolution cannot restore working");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("agent_before_settle", { outcome: "error" });
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "A pre-settle outcome cannot manufacture terminal authority");
  yield* emit("agent_start");
  assert.equal(status().state, "working", "A new genuine run remains observable after outcome-less settlement");
  for (const context of [{ ...ctx, isIdle: () => false }, { ...ctx, hasPendingMessages: () => true }]) {
    yield* emit("session_start");
    yield* emit("agent_start");
    yield* emit("agent_settled", {}, context);
    assert.equal(status().state, "clear", "Active or queued work contradicts final settlement");
  }
  const cancellation = new AbortController();
  yield* emit("session_start");
  yield* emit("agent_start", {}, { ...ctx, signal: cancellation.signal });
  yield* emit("agent_before_settle", { outcome: "completed" });
  cancellation.abort();
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Late cancellation leaves no authoritative final outcome");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("agent_before_settle", { outcome: "completed" });
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Stock missing-field settlement must not invent completion");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("agent_before_settle", { outcome: "error" });
  yield* emit("agent_start");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Continuation cannot settle using the previous boundary outcome");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("ui_prompt_start", { kind: "custom" });
  yield* emit("agent_before_settle", { outcome: "error" });
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "A settlement callback cannot erase an owned unresolved prompt");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("agent_before_settle", { outcome: "error" });
  yield* emit("agent_settled", {}, { ...ctx, sessionManager: { ...ctx.sessionManager, getSessionId: () => "previous-session" } });
  assert.equal(status().state, "clear", "Cross-session settlement cannot restore authority");
  yield* emit("session_start");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Unstarted or retired runs have no completion authority");
  yield* emit("agent_start");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Late settlement cannot repair invalidated authority");
  yield* emit("session_start");
  yield* emit("agent_start");
  yield* emit("session_shutdown");
  yield* emit("agent_settled");
  assert.equal(status().state, "clear", "Retirement invalidates late completion");
  const silentConditions: { terminal: boolean; context: Context }[] = [
    { terminal: false, context: ctx }, { terminal: true, context: { ...ctx, hasUI: false } },
    { terminal: true, context: { ...ctx, mode: "rpc" } }, { terminal: true, context: { ...ctx, mode: "json" } },
    { terminal: true, context: { ...ctx, mode: "print" } }, { terminal: true, context: { ...ctx, agent: { kind: "sub" } } },
  ];
  for (const conditions of silentConditions) {
    const silentContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => assert.fail("Status cannot deliver") }).pipe(
      Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
      Layer.provide(Stdio.layerTest({
        stdoutIsTerminal: Effect.succeed(conditions.terminal),
        stdout: () => Sink.forEach(() => Effect.sync(() => assert.fail("Non-TTY or UI-less native status must not write stdout"))),
      })),
    ));
    const silent = EffectContext.get(silentContext, Plugin);
    if (!silent) return yield* new CommandError({ message: "Expected silent plugin" });
    yield* silent.native("session_start", {}, conditions.context);
    yield* silent.native("agent_start", {}, conditions.context);
    yield* silent.native("session_stop", { messages: ["secret"] }, conditions.context);
  }
  const failedOutputContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => assert.fail("Status cannot deliver") }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({
      stdoutIsTerminal: Effect.succeed(true),
      stdout: () => Sink.forEach(() => Effect.fail(new PlatformError.PlatformError(new PlatformError.BadArgument({ module: "Stdio", method: "stdout", description: "private-terminal-failure" })))),
    })),
  ));
  const failedOutput = EffectContext.get(failedOutputContext, Plugin);
  if (!failedOutput) return yield* new CommandError({ message: "Expected failing terminal plugin" });
  assert.ok((yield* Effect.flip(failedOutput.native("session_start", {}, ctx))) instanceof CommandError, "Terminal failure cannot silently report success");
  assert.equal(notifications.at(-1)?.level, "warning");
  assert.ok(!notifications.some(({ text }) => text.includes("private-terminal-failure")), "Terminal diagnostics must omit raw failures");
  yield* failedOutput.native("session_shutdown", {}, { ...ctx, mode: "rpc" });
  // Consumer state/privacy checks only; this capture is not a terminal renderer or host acceptance.
  for (const frame of terminalOutput) {
    assert.ok(["idle", "working", "blocked", "done", "error", "clear"].includes(status(frame).state ?? ""), "Only standard states reach the terminal");
    assert.ok(!/private|secret|native-session|test-token/.test(frame), "OSC contains only constant control-free metadata");
    assert.ok(!/[\x00-\x1f\x7f]/.test(frame.slice(7, -2)));
  }
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));

test("a refused startup heartbeat keeps scoped enrollment alive until the prior lease fence clears", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-enrollment-" });
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const stateDirectory = platform.path.join(directory, "state");
  const tokenFile = platform.path.join(directory, "token");
  const config = platform.path.join(directory, "config.json");
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
  const server = EffectContext.get(serverContext, HttpServer.HttpServer);
  const polled = yield* Deferred.make<void>();
  let heartbeatAttempts = 0;
  let polls = 0;
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] })),
    handle: (_principal, request) => Effect.gen(function*() {
      if (request.method === "session.heartbeat") {
        heartbeatAttempts++;
        if (heartbeatAttempts === 1) return yield* new HttpError({ status: 409, message: "Prior session incarnation still live" });
        return { liveUntil: (yield* Clock.currentTimeMillis) + 30000 };
      }
      if (request.method === "session.poll") {
        polls++;
        yield* Deferred.succeed(polled, undefined);
        return { messages: [], offers: [], releases: [] };
      }
      if (request.method === "session.stop") return { stopped: true };
      return yield* Effect.fail(new CommandError({ message: `Unexpected route ${request.method}` }));
    }),
  });
  if (!("port" in server.address)) return yield* Effect.fail(new CommandError({ message: "Expected TCP server" }));
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const ctx: Context = { cwd: directory, mode: "rpc", hasUI: true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "native-session", getSessionFile: () => undefined },
    ui: { notify: () => {}, confirm: async () => true } };
  const pluginContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => { assert.fail("Enrollment must not admit a prompt"); } }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({})),
  ));
  const plugin = EffectContext.get(pluginContext, Plugin);
  if (!plugin) return yield* Effect.fail(new CommandError({ message: "Expected configured plugin" }));
  assert.equal((yield* Effect.exit(plugin.start(ctx)))._tag, "Failure");
  assert.equal(heartbeatAttempts, 1);
  yield* Deferred.await(polled).pipe(Effect.timeout("5 seconds"));
  assert.equal(heartbeatAttempts, 2);
  yield* plugin.stop();
  const stoppedPolls = polls;
  yield* Effect.sleep("1200 millis");
  assert.equal(polls, stoppedPolls, "Closing the session scope must cancel periodic polling");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));
