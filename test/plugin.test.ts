import assert from "node:assert/strict";
import test from "node:test";
import { Clock, ConfigProvider, Context as EffectContext, Deferred, Effect, Layer, Schema, Sink, Stdio } from "effect";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { NativeBinding, NativeEventSequence, NativeRunId } from "@samuelho/coordinator-native-control/contracts";
import { Mcp, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { CommandError, HttpError, Task, Principal, PluginConfig, JournalReleaseKey } from "../src/contracts.ts";
import type { Context, BrokerRequest } from "../src/contracts.ts";
import { Plugin } from "../src/plugin.ts";

test("ordinary host startup discovers private plugin config and fences owned SDK delivery", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-plugin-" });
  yield* platform.fs.chmod(directory, 0o700);
  const agentDir = platform.path.join(directory, "agent");
  yield* platform.fs.makeDirectory(agentDir, { mode: 0o700 });
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
  }));
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["-c", "user.name=Coordinator Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "test"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const sha = (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: directory }))).trim();
  const stateDirectory = platform.path.join(directory, "state"), tokenFile = platform.path.join(directory, "token");
  const configDirectory = platform.path.join(directory, "config", "coordinator"), config = platform.path.join(configDirectory, "plugin.json");
  yield* platform.fs.makeDirectory(configDirectory, { recursive: true, mode: 0o700 });
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const terminalOutput: string[] = [], notifications: string[] = [];
  const ctx: Context = { cwd: directory, mode: "tui", hasUI: true, model: { provider: "coordinator-unavailable", id: "unavailable" },
    sessionManager: { getSessionId: () => "surrounding-host-session" },
    ui: { notify: (text) => { notifications.push(text); }, confirm: async () => false } };
  let identity: { sessionId: string; incarnation: string; workerId?: string | undefined } | undefined;
  let task: Task | undefined;
  let attempts = 0;
  const reported = yield* Deferred.make<void>();
  const server = EffectContext.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal)({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] })),
    handle: (_principal, request) => Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      if (request.method === "session.heartbeat") {
        identity = request.params;
        if (task === undefined) {
          const address = { messageId: "message", revision: 1, sessionId: identity.sessionId, incarnation: identity.incarnation, workerId: identity.workerId,
            repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: now + 60000 };
          const work = { taskId: "task", prompt: "SDK_UNAVAILABLE_PROVIDER", repo: address.repo, ref: address.ref, sha,
            launcher: "pi", scope: address.scope, restrictions: address.restrictions, expectedEvidence: [], deadline: address.deadline, workerId: address.workerId,
            githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
          task = Schema.decodeUnknownSync(Task)({ taskId: "task", owner: "owner", fingerprint: yield* platform.fingerprint(work), state: "ACCEPTED", createdAt: now, updatedAt: now,
            address, readback: address, confirmedRevision: 1, disposition: "ready", request: work });
        }
        return { liveUntil: now + 30000 };
      }
      if (request.method === "session.poll") return { messages: [], offers: [], releases: task === undefined ? [] : [task] };
      if (request.method === "session.stop") return { stopped: true };
      if (request.method === "task.report" && task !== undefined) {
        const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, request.params.sessionId)), Journal);
        assert.equal(persisted.state.releases[JournalReleaseKey.make("task:1")], "sent", "SDK admission must follow durable intent before reporting");
        if (request.params.state === "RUNNING") attempts++;
        else {
          assert.equal(request.params.state, "FAILED");
          yield* Deferred.succeed(reported, undefined);
        }
        return { ...task, state: request.params.state, result: request.params.result };
      }
      return yield* new CommandError({ message: "Unexpected broker route" });
    }).pipe(Effect.scoped, Effect.provideService(Platform, platform)),
  });
  if (server.address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected TCP server" });
  yield* platform.fs.writeFileString(platform.path.join(agentDir, "models.json"), JSON.stringify({ providers: {
    "coordinator-unavailable": { baseUrl: `http://127.0.0.1:${server.address.port}/unavailable/v1`, api: "openai-completions", apiKey: "isolated-fixture-only", models: [{ id: "unavailable" }] },
  } }), { mode: 0o600 });
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const service = EffectContext.get(yield* Layer.build(Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ XDG_CONFIG_HOME: platform.path.join(directory, "config") }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(true), stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => { if (typeof chunk === "string") terminalOutput.push(chunk); })) })),
  )), Plugin);
  assert.ok(service);
  yield* service.start(ctx);
  yield* Deferred.await(reported).pipe(Effect.timeout("15 seconds"));
  assert.ok(identity);
  assert.notEqual(identity.sessionId, ctx.sessionManager.getSessionId());
  assert.equal(identity.workerId, identity.sessionId);
  const binding = Schema.decodeUnknownSync(NativeBinding)({ host: "pi", sessionId: identity.sessionId, incarnation: identity.incarnation, generation: 1 });
  const runId = NativeRunId.make("consumer-run"), seq = NativeEventSequence.make(1);
  yield* service.native({ type: "native_lifecycle", kind: "run_started", binding, runId, at: 1, seq }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=working/);
  yield* service.native({ type: "native_lifecycle", kind: "run_phase", binding, runId, at: 2, seq, phase: "continuation", transition: "ended" }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=working/, "A continuation boundary is not settlement");
  yield* service.native({ type: "native_lifecycle", kind: "run_settled", binding, runId, at: 3, seq, outcome: "completed" }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=done/);
  const beforeMalformed = notifications.length;
  assert.equal((yield* Effect.exit(service.command('report {"secret":"private-command-content"', ctx)))._tag, "Failure");
  assert.ok(notifications.length > beforeMalformed);
  assert.ok(!notifications.some((text) => text.includes("private-command-content")));
  yield* service.command('confirm {"taskId":"task","revision":1}', ctx);
  yield* service.stop();
  assert.equal((yield* Effect.exit(service.command("identity", ctx)))._tag, "Failure");
  yield* service.start(ctx);
  yield* Effect.sleep("1200 millis");
  assert.equal(attempts, 1, "A new actual SDK identity cannot replay the prior addressed revision");
  for (const frame of terminalOutput) assert.ok(!/consumer-run|surrounding-host-session|private|test-token/.test(frame), "Terminal status contains only constant metadata");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));

test("a refused startup heartbeat retains scoped enrollment until its lease fence clears", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform, mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-enrollment-" });
  yield* platform.fs.chmod(directory, 0o700);
  const agentDir = platform.path.join(directory, "agent");
  yield* platform.fs.makeDirectory(agentDir, { mode: 0o700 });
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  yield* Effect.addFinalizer(() => Effect.sync(() => { if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir; }));
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const stateDirectory = platform.path.join(directory, "state"), tokenFile = platform.path.join(directory, "token"), config = platform.path.join(directory, "config.json");
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const server = EffectContext.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  const polled = yield* Deferred.make<void>();
  let heartbeatAttempts = 0, polls = 0;
  yield* mcp.serve(server, { target: "broker", authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal)({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] })),
    handle: (_principal, request: BrokerRequest) => Effect.gen(function*() {
      if (request.method === "session.heartbeat") { heartbeatAttempts++; if (heartbeatAttempts === 1) return yield* new HttpError({ status: 409, message: "Prior incarnation still live" }); return { liveUntil: (yield* Clock.currentTimeMillis) + 30000 }; }
      if (request.method === "session.poll") { polls++; yield* Deferred.succeed(polled, undefined); return { messages: [], offers: [], releases: [] }; }
      if (request.method === "session.stop") return { stopped: true };
      return yield* new CommandError({ message: "Unexpected broker route" });
    }),
  });
  if (server.address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected TCP server" });
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const ctx: Context = { cwd: directory, mode: "rpc", hasUI: false,
    sessionManager: { getSessionId: () => "parent-session" }, ui: { notify: () => {}, confirm: async () => false } };
  const plugin = EffectContext.get(yield* Layer.build(Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(false) })),
  )), Plugin);
  assert.ok(plugin);
  assert.equal((yield* Effect.exit(plugin.start(ctx)))._tag, "Failure");
  assert.equal(heartbeatAttempts, 1);
  yield* Deferred.await(polled).pipe(Effect.timeout("15 seconds"));
  assert.equal(heartbeatAttempts, 2);
  yield* plugin.stop();
  const stoppedPolls = polls;
  yield* Effect.sleep("1200 millis");
  assert.equal(polls, stoppedPolls, "Retirement cancels periodic polling");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));

test("unconfigured hosts stay disabled but present insecure or malformed plugin config fails closed", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const home = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-config-" });
  const directory = platform.path.join(home, ".config", "coordinator"), config = platform.path.join(directory, "plugin.json");
  const layer = Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ HOME: home }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(false) })),
  );
  assert.equal(EffectContext.get(yield* Layer.build(layer), Plugin), undefined);
  yield* platform.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  yield* platform.fs.writeFileString(config, "{}", { mode: 0o644 });
  assert.equal((yield* Effect.exit(Layer.build(layer)))._tag, "Failure", "Present public-readable enrollment must not silently disable the plugin");
  yield* platform.fs.chmod(config, 0o600);
  assert.equal((yield* Effect.exit(Layer.build(layer)))._tag, "Failure", "Present malformed enrollment must not silently disable the plugin");
})).pipe(Effect.provide(Platform.layer))));
