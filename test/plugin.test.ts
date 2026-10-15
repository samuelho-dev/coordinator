import { JournalReleaseStateEnum } from "../src/contracts.ts";
import { Commands, type CommandOutput, RoleEnum, DispositionEnum } from "../src/contracts.ts";
import { AgentRunOutcomeEnum, AgentRunPhaseEnum, AgentPhaseTransitionEnum } from "../src/agent/contracts.ts";
import { TaskStateEnum } from "../src/generated/database/enums.ts";
import { AgentHostEnum } from "../src/agent/contracts.ts";
import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Cause, Clock, ConfigProvider, Context as EffectContext, Deferred, Duration, Effect, Layer, Schema, Sink, Stdio } from "effect";
import { ChildProcess } from "effect/process";
import { TestClock } from "effect/testing";
import { HttpServer } from "effect/http";
import { AgentBinding, AgentEventSequence, AgentRunId } from "../src/agent/contracts.ts";
import { Mcp, serverLayer } from "../src/mcp.ts";
import { Platform, redactedCause } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { CommandError, Delivery, HttpError, MachineId, Task, Principal, PluginConfig, JournalReleaseKey } from "../src/contracts.ts";
import { SessionId } from "../src/generated/database/types.ts";
import type { Context, BrokerRequest } from "../src/contracts.ts";
import { Plugin } from "../src/plugin.ts";

it.live("ordinary host startup discovers private plugin config and fences owned SDK delivery", () => Effect.scoped(Effect.gen(function*() {
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
  let terminalInput: ((data: string) => { consume?: boolean; data?: string } | undefined) | undefined;
  const ctx: Context = { cwd: directory, mode: "tui", hasUI: true, model: { provider: "coordinator-unavailable", id: "unavailable" },
    sessionManager: { getSessionId: () => "surrounding-host-session" },
    ui: { notify: (text) => { notifications.push(text); }, confirm: async () => false,
      onTerminalInput: (handler) => { terminalInput = handler; return () => { terminalInput = undefined; }; } } };
  let identity: { sessionId: string; incarnation: string; workerId?: string | undefined } | undefined;
  let task: Task | undefined;
  let attempts = 0;
  let brokerRequests = 0;
  let delivered = false, conflictPolls = 0;
  let polled = yield* Deferred.make<void>();
  const admitted = yield* Deferred.make<void>();
  const server = EffectContext.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal)({ id: RoleEnum.session, owner: "owner", tokenHash: "a".repeat(64), roles: [RoleEnum.session], repos: ["github.com/example/repo"] })),
    handle: (_principal, request) => Effect.gen(function*() {
      brokerRequests++;
      const now = yield* Clock.currentTimeMillis;
      if (request.method === Commands.heartbeatSession.name) {
        identity = request.params;
        if (task === undefined) {
          const address = { messageId: "message", revision: 1, sessionId: identity.sessionId, incarnation: identity.incarnation, workerId: identity.workerId,
            repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: now + 60000 };
          const work = { taskId: "task", prompt: "SDK_UNAVAILABLE_PROVIDER", repo: address.repo, ref: address.ref, sha,
            launcher: AgentHostEnum.pi, scope: address.scope, restrictions: address.restrictions, expectedEvidence: [], deadline: address.deadline, workerId: address.workerId,
            githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
          task = Schema.decodeUnknownSync(Task)({ taskId: "task", owner: "owner", fingerprint: yield* platform.fingerprint(work), state: TaskStateEnum.ACCEPTED, createdAt: now, updatedAt: now,
            address, readback: address, confirmedRevision: 1, disposition: DispositionEnum.ready, request: work });
        }
        const output: CommandOutput<typeof Commands.heartbeatSession> = { liveUntil: now + 30000 }; return output;
      }
      if (request.method === Commands.pollSession.name) {
        yield* Deferred.succeed(polled, undefined);
        const messages = delivered ? [Schema.decodeUnknownSync(Delivery)({ messageId: "delivery-conflict", incarnation: request.params.incarnation,
          payload: ++conflictPolls, expiresAt: now + 60000 })] : [];
        const output: CommandOutput<typeof Commands.pollSession> = { messages, offers: [], releases: task === undefined ? [] : [task] }; return output;
      }
      if (request.method === Commands.acknowledgePeer.name) { const output: CommandOutput<typeof Commands.acknowledgePeer> = { handled: true }; return output; }
      if (request.method === Commands.stopSession.name) { const output: CommandOutput<typeof Commands.stopSession> = { stopped: true }; return output; }
      if (request.method === Commands.reportTask.name && task !== undefined) {
        const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, request.params.sessionId)), Journal);
        assert.equal(persisted.state.releases[JournalReleaseKey.make("task:1")], JournalReleaseStateEnum.sent, "SDK admission must follow durable intent before reporting");
        if (request.params.state === TaskStateEnum.RUNNING) {
          attempts++;
          yield* Deferred.succeed(admitted, undefined);
          return yield* new HttpError({ status: 503, message: "PRIVATE_LOST_ADMISSION_REPORT" });
        } else {
          assert.ok(request.params.state === TaskStateEnum.FAILED || request.params.state === TaskStateEnum.HELD);
          delivered = true;
        }
        const output: CommandOutput<typeof Commands.reportTask> = { ...task, state: request.params.state, result: request.params.result }; return output;
      }
      return yield* new CommandError({ message: "Unexpected broker route" });
    }).pipe(Effect.scoped, Effect.provideService(Platform, platform)),
  });
  if (server.address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected TCP server" });
  yield* platform.fs.writeFileString(platform.path.join(agentDir, "models.json"), JSON.stringify({ providers: {
    "coordinator-unavailable": { baseUrl: `http://127.0.0.1:${server.address.port}/unavailable/v1`, api: "openai-completions", apiKey: "isolated-fixture-only", models: [{ id: "unavailable" }] },
  } }), { mode: 0o600 });
  yield* platform.fs.writeFileString(config, Schema.encodeSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: MachineId.make("workstation/東京"), host: AgentHostEnum.pi }), { mode: 0o600 });
  const service = EffectContext.get(yield* Layer.build(Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ XDG_CONFIG_HOME: platform.path.join(directory, "config") }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(true), stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => {
      if (typeof chunk !== "string") return;
      terminalOutput.push(chunk);
      if (chunk.includes("7501;?")) queueMicrotask(() => terminalInput?.("\x1b]7501;?\x1b\\"));
    })) })),
  )), Plugin);
  assert.ok(service);
  yield* service.command("hi", ctx);
  assert.match(notifications.at(-1) ?? "", /\/coordinator help/, "Unknown actions must give a usable help command");
  assert.equal(brokerRequests, 0, "Unknown actions must not reach the broker or enroll a session");
  yield* service.start(ctx);
  yield* Deferred.await(polled).pipe(Effect.timeout("15 seconds"));
  yield* Deferred.await(admitted).pipe(Effect.timeout("15 seconds"));
  polled = yield* Deferred.make<void>();
  assert.ok(identity);
  assert.notEqual(identity.sessionId, ctx.sessionManager.getSessionId());
  assert.equal(identity.workerId, identity.sessionId);
  const binding = Schema.decodeUnknownSync(AgentBinding)({ host: AgentHostEnum.pi, sessionId: identity.sessionId, incarnation: identity.incarnation, generation: 1 });
  const runId = AgentRunId.make("consumer-run"), seq = AgentEventSequence.make(1);
  yield* service.agent({ type: "agent_lifecycle", kind: "run_started", binding, runId, at: 1, seq }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=working/);
  yield* service.agent({ type: "agent_lifecycle", kind: "run_phase", binding, runId, at: 2, seq, phase: AgentRunPhaseEnum.continuation, transition: AgentPhaseTransitionEnum.ended }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=working/, "A continuation boundary is not settlement");
  yield* service.agent({ type: "agent_lifecycle", kind: "run_settled", binding, runId, at: 3, seq, outcome: AgentRunOutcomeEnum.completed }, ctx);
  assert.match(terminalOutput.at(-1) ?? "", /state=done/);
  yield* Deferred.await(polled).pipe(Effect.timeout("15 seconds"));
  assert.equal(attempts, 1, "A lost admission report cannot replay an already admitted native prompt during recovery");
  const retained = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, SessionId.make(identity.sessionId))), Journal);
  assert.equal(retained.state.releases[JournalReleaseKey.make("task:1")], JournalReleaseStateEnum.sent, "Journal pause cannot clear admitted release uncertainty");
  for (const frame of terminalOutput.filter(frame => !frame.includes("7501;?"))) {
    assert.match(frame, /id=coordinator/);
    assert.match(frame, /app=coordinator/);
  }
  const beforeMalformed = notifications.length;
  assert.equal((yield* Effect.exit(service.command('report {"secret":"private-command-content"', ctx)))._tag, "Failure");
  assert.ok(notifications.length > beforeMalformed);
  assert.ok(!notifications.some((text) => text.includes("private-command-content")));
  yield* service.command('confirm {"taskId":"task","revision":1}', ctx);
  yield* service.stop();
  assert.ok(terminalOutput.some(frame => /id=coordinator/.test(frame) && /state=clear/.test(frame)), "Retirement clears only the owned PSP record");
  assert.equal((yield* Effect.exit(service.command("identity", ctx)))._tag, "Failure");
  polled = yield* Deferred.make<void>();
  yield* service.start(ctx);
  yield* Deferred.await(polled).pipe(Effect.timeout("15 seconds"));
  assert.equal(attempts, 1, "A new actual SDK identity cannot replay the prior addressed revision");
  for (const frame of terminalOutput) assert.ok(!/consumer-run|surrounding-host-session|private|test-token/.test(frame), "Terminal status contains only constant metadata");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer))));

it.live("a refused startup heartbeat retains scoped enrollment until its lease fence clears", () => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform, mcp = yield* Mcp;
  const clock = yield* TestClock.make({ warningDelay: "1 minute" });
  yield* clock.setTime(Date.now());
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
  const polled = yield* Deferred.make<void>(), retrying = yield* Deferred.make<void>();
  const received = yield* Deferred.make<void>(), journalHeld = yield* Deferred.make<void>();
  const retrySleeping = yield* Deferred.make<void>(), pollSleeping = yield* Deferred.make<void>();
  const traffic: { method: string; sessionId: string; incarnation: string }[] = [];
  let heartbeatAttempts = 0, polls = 0, liveUntil = 0, polledAt: number | undefined;
  const eligible = () => polledAt !== undefined && clock.currentTimeMillisUnsafe() < liveUntil && clock.currentTimeMillisUnsafe() - polledAt < 10000;
  const ownerClock: Clock.Clock = { ...clock, sleep: duration => Effect.gen(function*() {
    // Synchronize socket completion with the loop's next virtual sleep, not wall time.
    if (Duration.toMillis(duration) <= 1250) {
      if (heartbeatAttempts === 1 && polls === 0) yield* Deferred.succeed(retrySleeping, undefined);
      if (polls === 1) yield* Deferred.succeed(pollSleeping, undefined);
    }
    yield* clock.sleep(duration);
  }) };
  yield* mcp.serve(server, { target: "broker", authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal)({ id: RoleEnum.session, owner: "owner", tokenHash: "a".repeat(64), roles: [RoleEnum.session], repos: ["github.com/example/repo"] })),
    handle: (_principal, request: BrokerRequest) => Effect.gen(function*() {
      if (request.method === Commands.heartbeatSession.name) {
        traffic.push({ method: request.method, sessionId: request.params.sessionId, incarnation: request.params.incarnation });
        heartbeatAttempts++;
        if (heartbeatAttempts === 1) return yield* new HttpError({ status: 409, message: "PRIVATE_ENDPOINT_TOKEN_PATH" });
        liveUntil = clock.currentTimeMillisUnsafe() + 30000;
        const output: CommandOutput<typeof Commands.heartbeatSession> = { liveUntil }; return output;
      }
      if (request.method === Commands.pollSession.name) {
        traffic.push({ method: request.method, sessionId: request.params.sessionId, incarnation: request.params.incarnation });
        polledAt = clock.currentTimeMillisUnsafe();
        polls++;
        yield* Deferred.succeed(polled, undefined);
        return { messages: [Schema.decodeUnknownSync(Delivery)({ messageId: "private-fixture-message", incarnation: request.params.incarnation,
          payload: polls === 1 ? "PRIVATE_INITIAL_PAYLOAD" : "PRIVATE_CONFLICT_PAYLOAD", expiresAt: clock.currentTimeMillisUnsafe() + 60000 })], offers: [], releases: [] };
      }
      if (request.method === Commands.acknowledgePeer.name) { const output: CommandOutput<typeof Commands.acknowledgePeer> = { handled: true }; return output; }
      if (request.method === Commands.stopSession.name) {
        traffic.push({ method: request.method, sessionId: request.params.sessionId, incarnation: request.params.incarnation });
        const output: CommandOutput<typeof Commands.stopSession> = { stopped: true }; return output;
      }
      return yield* new CommandError({ message: "Unexpected broker route" });
    }),
  });
  if (server.address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected TCP server" });
  yield* platform.fs.writeFileString(config, Schema.encodeSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: MachineId.make("machine"), host: AgentHostEnum.pi }), { mode: 0o600 });
  const notifications: string[] = [], statuses = new Map<string, string>();
  const ctx: Context = { cwd: directory, mode: "rpc", hasUI: true,
    sessionManager: { getSessionId: () => "parent-session" }, ui: { notify: (text) => {
      notifications.push(text);
      if (text.includes("control paused")) Effect.runSync(Deferred.succeed(journalHeld, undefined));
      if (text.includes("durably received")) Effect.runSync(Deferred.succeed(received, undefined));
    }, setStatus: (key, value) => {
      if (value === undefined) statuses.delete(key); else statuses.set(key, value);
      if (key === "coordinator.network" && value === "Coordinator reconnecting") Effect.runSync(Deferred.succeed(retrying, undefined));
    }, confirm: async () => false } };
  const plugin = EffectContext.get(yield* Layer.build(Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(Layer.succeed(Clock.Clock, ownerClock)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(false) })),
  )), Plugin);
  assert.ok(plugin);
  assert.equal((yield* Effect.exit(plugin.start(ctx).pipe(Effect.provideService(Clock.Clock, ownerClock), Effect.timeout("5 seconds"))))._tag, "Success", "Startup returns without waiting for broker recovery");
  yield* Deferred.await(retrying).pipe(Effect.timeout("15 seconds"));
  yield* Deferred.await(retrySleeping).pipe(Effect.timeout("15 seconds"));
  assert.ok(!notifications.some(text => text.includes("PRIVATE_ENDPOINT_TOKEN_PATH") || text.includes(directory) || text.includes("test-token")), "Startup diagnostics do not expose broker secrets or private paths");
  assert.equal(heartbeatAttempts, 1);
  assert.equal(polls, 0);
  assert.equal(eligible(), false, "A refused heartbeat cannot make the retained owner delivery eligible");
  assert.equal((yield* Effect.exit(plugin.command("identity", ctx)))._tag, "Failure", "Broker control remains unavailable while enrollment is fenced; the retained scope must still recover below");
  const refused = traffic[0];
  assert.ok(refused);
  yield* clock.adjust(999);
  assert.deepEqual(traffic, [refused], "No polling or owner retirement occurs before the retry fence");
  assert.equal(eligible(), false);
  yield* clock.adjust(251);
  yield* Deferred.await(polled).pipe(Effect.timeout("15 seconds"));
  assert.equal(heartbeatAttempts, 2);
  const recovered = traffic.find(call => call.method === Commands.pollSession.name);
  assert.ok(recovered);
  assert.notEqual(recovered.incarnation, refused.incarnation, "Recovery must not reuse the refused native incarnation");
  assert.deepEqual(traffic.map(call => call.method), ["session.heartbeat", "session.stop", "session.heartbeat", "session.poll"], "The old enrollment retires before a fresh heartbeat permits delivery");
  assert.equal(traffic[1]?.incarnation, refused.incarnation);
  assert.equal(traffic[2]?.incarnation, recovered.incarnation);
  assert.equal(eligible(), true, "An actual fresh heartbeat and poll restore delivery eligibility");
  yield* Deferred.await(received).pipe(Effect.timeout("15 seconds"));
  yield* Deferred.await(pollSleeping).pipe(Effect.timeout("15 seconds"));
  yield* clock.adjust(1000);
  yield* Deferred.await(journalHeld).pipe(Effect.timeout("15 seconds"));
  assert.equal(polls, 2, "The repeated message reaches the real journal conflict fence");
  yield* clock.adjust(2200);
  assert.equal(polls, 2, "A latched inbox conflict pauses unchanged polling until reconciliation");
  assert.equal(statuses.get("coordinator.paused"), "Coordinator network paused", "A local journal failure remains visibly paused");
  assert.ok(!notifications.some((text) => /PRIVATE_|test-token/.test(text)), "Private causal messages and inbox payloads stay redacted");
  yield* plugin.stop();
  const stoppedPolls = polls;
  yield* clock.adjust(1200);
  assert.equal(polls, stoppedPolls, "Retirement cancels periodic polling");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer))));

it("safe causal summaries never decode unknown defects as typed success", () => {
  assert.match(redactedCause(Cause.die({ _tag: "HttpError", status: 200, message: "PRIVATE" })), /unknown defect/);
  assert.match(redactedCause(Cause.fail({ _tag: "UnknownPrivateTag", token: "PRIVATE" })), /unknown failure/);
  assert.ok(!redactedCause(Cause.fail(new HttpError({ status: 503, message: "PRIVATE" }))).includes("PRIVATE"));
});

it.live("unconfigured hosts stay disabled but present insecure or malformed plugin config fails closed", () => Effect.scoped(Effect.gen(function*() {
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
})).pipe(Effect.provide(Platform.layer)));
