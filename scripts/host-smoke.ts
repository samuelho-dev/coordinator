import { AgentRunPhaseEnum } from "../src/agent/contracts.ts";
import { RoleEnum, DispositionEnum, OfferDecisionEnum } from "../src/contracts.ts";
import { AgentRunOutcomeEnum, AgentAdmissionDispositionEnum } from "../src/agent/contracts.ts";
import { TaskStateEnum } from "../src/generated/database/enums.ts";
import assert from "node:assert/strict";
import { createServer, request as httpRequest } from "node:http";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { Clock, Config, Context, Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { connectAgentClient } from "../src/agent/client.ts";
import { AgentTools, AgentHostEnum, AgentEventSequence, AgentGeneration, AgentHost, AgentConfig, AgentSubmissionId } from "../src/agent/contracts.ts";
import type { AgentBinding, AgentLifecycleEvent } from "../src/agent/contracts.ts";
import { BrokerConfig, CommandError, Commands, GitHubRepositoryId, JournalHandledKey, JournalState, LiveSession, MachineId, PluginConfig } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { Broker } from "../src/broker.ts";
import { Mcp } from "../src/mcp.ts";
import { dropPostgres, postgresConfig, provisionPostgres } from "../test/postgres-fixture.ts";
import { TaskId, SessionId, PeerId, Evidence } from "../src/generated/database/types.ts";

// Installed CLIs, unmodified public SDKs and real provider parsing against a local SSE fixture.
// No external provider, global profile, SDK UI consent or terminal rendering is inferred.
const runHostSmoke = () => Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs, path: { join, resolve }, crypto } = platform;
  const root = resolve(import.meta.dirname, "..");
  const directory = yield* fs.makeTempDirectory({ prefix: "coordinator-public-plugins-" });
  yield* fs.chmod(directory, 0o700);
  const transcript = join(directory, "transcript.json");
  const path = yield* Config.String("PATH");
  const pi = yield* Config.String("COORDINATOR_TEST_PI");
  const omp = yield* Config.String("COORDINATOR_TEST_OMP");
  const piVersion = yield* Config.String("COORDINATOR_TEST_PI_VERSION");
  const ompVersion = yield* Config.String("COORDINATOR_TEST_OMP_VERSION");
  assert.equal(piVersion, "1.0.4");
  assert.equal(ompVersion, "omp/18.6.1");
  const exercisedControls = [AgentTools.getState, AgentTools.getMessages, AgentTools.getEntries, AgentTools.bash, AgentTools.abort, AgentTools.prompt, AgentTools.abortAndPrompt, AgentTools.steer, AgentTools.followUp];
  const guardedNegativeControls = [AgentTools.steer, AgentTools.followUp];
  let fixtureCalls = 0;
  const paused = new Map<string, () => void>();
  const pauseRequests = new Map<string, number>();
  const retryAttempts = new Map<string, number>(), compacted = new Set<string>(), asyncIssued = new Set<string>();
  let asyncDirectory = "";
  const requestBody = Schema.Struct({ messages: Schema.Array(Schema.Struct({ role: Schema.String, content: Schema.optional(Schema.Json) })),
    tools: Schema.optional(Schema.Array(Schema.Struct({ function: Schema.Struct({ name: Schema.String }) }))) });
  const provider = createServer((request, response) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    request.on("data", (chunk: Buffer) => { bytes += chunk.length; if (bytes > 1024 * 1024) request.destroy(); else chunks.push(chunk); });
    request.on("end", () => {
      try {
        const wire = Schema.decodeSync(Schema.fromJsonString(requestBody))(Buffer.concat(chunks).toString("utf8"));
        fixtureCalls++;
        const user = JSON.stringify(wire.messages.findLast((message) => message.role === "user")?.content ?? "");
        const primary = wire.tools?.some((tool) => tool.function.name === "bash") === true;
        const retry = user.includes("SDK_RETRY_pi") ? AgentHostEnum.pi : user.includes("SDK_RETRY_omp") ? AgentHostEnum.omp : undefined;
        if (primary && retry !== undefined) {
          const attempt = (retryAttempts.get(retry) ?? 0) + 1;
          retryAttempts.set(retry, attempt);
          // OMP does six HTTP attempts plus one provider replay before session-level retry.
          if (attempt <= (retry === AgentHostEnum.omp ? 12 : 1)) {
            response.writeHead(503, { "content-type": "application/json" }); response.end(JSON.stringify({ error: { message: "Scripted temporary provider overload", type: "server_error" } })); return;
          }
        }
        const compact = user.includes("SDK_COMPACT_pi") ? AgentHostEnum.pi : user.includes("SDK_COMPACT_omp") ? AgentHostEnum.omp : undefined;
        if (primary && compact !== undefined && !compacted.has(compact)) { compacted.add(compact); response.writeHead(400, { "content-type": "application/json" }); response.end(JSON.stringify({ error: { message: "maximum context length exceeded", code: "context_length_exceeded", type: "invalid_request_error" } })); return; }
        if (primary && user.includes("SDK_ERROR")) { response.writeHead(400, { "content-type": "application/json" }); response.end(JSON.stringify({ error: { message: "Scripted non-retryable provider rejection", type: "invalid_request_error" } })); return; }
        response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
        const chunk = (delta: Schema.JsonObject, finish: string | null = null) => response.write(`data: ${JSON.stringify({ id: "local-fixture", object: "chat.completion.chunk", created: 1, model: "scripted", choices: [{ index: 0, delta, finish_reason: finish }], ...(finish === null ? {} : { usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } }) })}\n\n`);
        const finish = () => { chunk({}, "stop"); response.end("data: [DONE]\n\n"); };
        if (primary && user.includes("SDK_TOOL") && !wire.messages.some((message) => message.role === "tool" && JSON.stringify(message.content).includes("SDK_TOOL_OK"))) {
          chunk({ role: "assistant", tool_calls: [{ index: 0, id: "fixture-bash", type: "function", function: { name: "bash", arguments: JSON.stringify({ command: "printf SDK_TOOL_OK" }) } }] });
          chunk({}, "tool_calls"); response.end("data: [DONE]\n\n"); return;
        }
        const job = primary && user.includes("SDK_ASYNC_CANCEL") && !asyncIssued.has("cancel") ? "cancel" :
          primary && user.includes("SDK_ASYNC_ONE") && !asyncIssued.has("two") ? "two" :
          primary && user.includes("SDK_ASYNC") && !user.includes("SDK_ASYNC_CANCEL") && !asyncIssued.has("one") ? "one" : undefined;
        if (job !== undefined) {
          asyncIssued.add(job);
          const output = job === "cancel" ? "SDK_ASYNC_CANCELLED" : job === "one" ? "SDK_ASYNC_ONE" : "SDK_ASYNC_TWO";
          const command = `sleep ${job === "cancel" ? "30" : "1"}; printf ${output} > '${asyncDirectory}/${job}'; printf ${output}`;
          chunk({ role: "assistant", tool_calls: [{ index: 0, id: `fixture-async-${job}`, type: "function", function: { name: "bash", arguments: JSON.stringify({ command, async: true }) } }] });
          chunk({}, "tool_calls"); response.end("data: [DONE]\n\n"); return;
        }
        chunk({ role: "assistant", content: "SDK_FIXTURE_OK" });
        if (primary && user.includes("SDK_ABORT")) return;
        const pause = primary && user.includes("SDK_PAUSE_pi") ? AgentHostEnum.pi : primary && user.includes("SDK_PAUSE_omp") ? AgentHostEnum.omp : undefined;
        if (pause !== undefined) { pauseRequests.set(pause, (pauseRequests.get(pause) ?? 0) + 1); paused.set(pause, finish); response.once("close", () => paused.delete(pause)); }
        else finish();
      } catch (error) { console.error("Local scripted provider request failed", error); response.destroy(); }
    });
  });
  yield* Effect.acquireRelease(
    Effect.promise(() => new Promise<void>((resolve, reject) => { provider.once("error", reject); provider.listen(0, "127.0.0.1", resolve); })),
    () => Effect.promise(() => new Promise<void>((resolve, reject) => { provider.closeAllConnections(); provider.close((error) => error ? reject(error) : resolve()); })),
  );
  const providerAddress = provider.address();
  if (providerAddress === null || typeof providerAddress === "string") return yield* new CommandError({ message: "Expected local provider address" });
  const models = { providers: { "coordinator-scripted": { api: "openai-completions", baseUrl: `http://127.0.0.1:${providerAddress.port}/v1`, apiKey: "local-script",
    models: [{ id: "scripted", name: "Scripted local provider", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 512, supportsTools: true,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } };
  const repo = yield* platform.inspectedRepo(root);
  const coordinatorToken = Hex.encode(yield* crypto.randomBytes(32));
  const sessionToken = Hex.encode(yield* crypto.randomBytes(32));
  const brokerDatabaseUrl = yield* provisionPostgres();
  const brokerConfig = yield* Schema.decodeUnknownEffect(BrokerConfig)({
    host: "127.0.0.1", port: 0,
    backupDirectory: join(directory, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: Object.fromEntries([[repo, 1]]),
    principals: [
      { id: RoleEnum.coordinator, owner: "stock-smoke", roles: [RoleEnum.coordinator], repos: [repo], tokenHash: yield* platform.digest(coordinatorToken) },
      { id: RoleEnum.session, owner: "stock-smoke", roles: [RoleEnum.session], repos: [repo], tokenHash: yield* platform.digest(sessionToken) },
    ],
  });
  const brokerScope = yield* Scope.fork(yield* Effect.scope);
  const brokerContext = yield* Layer.build(Broker.layer(brokerConfig).pipe(
    Layer.provideMerge(Mcp.layer), Layer.provide(Layer.succeed(Platform, platform)),
  )).pipe(Effect.provideService(Scope.Scope, brokerScope), Effect.provide(postgresConfig(brokerDatabaseUrl)));
  const broker = Context.get(brokerContext, Broker);
  const db = broker.database;
  const mcp = Context.get(brokerContext, Mcp);
  const address = broker.server.address;
  if (address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected loopback IPv4 broker" });
  const brokerUrl = `http://127.0.0.1:${address.port}`;
  assert.equal((yield* mcp.call(brokerUrl, coordinatorToken, Commands.health, {})).status, "ok");
  const until = Effect.fnUntraced(function*<A, E, R>(read: () => Effect.Effect<A, E, R>, ready: (value: A) => boolean) {
    const deadline = (yield* Clock.currentTimeMillis) + 30000;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const value = yield* read();
      if (ready(value)) return value;
      yield* Effect.sleep(100);
    }
    return yield* new CommandError({ message: "Stock plugin enrollment or durable delivery deadline expired" });
  });
  const observations: { host: AgentHost; binding: AgentBinding; pid: number; capabilities: readonly string[]; physicalProcessClosed: boolean }[] = [];
  const controlChecks: { host: AgentHost; runId: string; transientPollFaults: number; recoveredHeartbeatPollPairs: number; permanentPresenceFaults: number; quietControlRequests: number; completedOutcomes: number; providerHeldRequests: number; fencedTaskState: string; retiredOwnerRequests: number }[] = [];
  for (const host of Object.values(AgentHostEnum)) {
    const binary = yield* fs.realPath(host === AgentHostEnum.pi ? pi : omp);
    const home = join(directory, host), agent = join(home, "agent"), sessions = join(home, "sessions");
    for (const folder of [home, agent, sessions]) yield* fs.makeDirectory(folder, { mode: 0o700 });
    yield* fs.writeFileString(join(agent, host === AgentHostEnum.pi ? "models.json" : "models.yml"), JSON.stringify(models), { flag: "wx", mode: 0o600 });
    asyncDirectory = home;
    yield* fs.writeFileString(join(agent, host === AgentHostEnum.pi ? "settings.json" : "config.yml"), JSON.stringify({
      compaction: { keepRecentTokens: 1, reserveTokens: 512 }, ...(host === AgentHostEnum.omp ? { async: { enabled: true } } : {}),
    }), { flag: "wx", mode: 0o600 });
    const env = { HOME: home, PATH: path, LANG: "C.UTF-8", TERM: "dumb", PI_CODING_AGENT_DIR: agent,
      XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state"), XDG_CACHE_HOME: join(home, "cache") };
    const version = (yield* platform.exec(ChildProcess.make(binary, ["--version"], { cwd: root, env, extendEnv: false }))).trim();
    assert.equal(version, host === AgentHostEnum.pi ? piVersion : ompVersion, `${host}: installed CLI version mismatch`);
    const token = Hex.encode(yield* crypto.randomBytes(32));
    const tokenFile = join(home, "agent.token"), endpointFile = join(home, "endpoint.json"), configFile = join(home, "agent-config.json");
    yield* fs.writeFileString(tokenFile, token, { flag: "wx", mode: 0o600 });
    yield* fs.writeFileString(configFile, yield* Schema.encodeEffect(Schema.fromJsonString(AgentConfig))({ host, endpointFile, tokenFile }), { flag: "wx", mode: 0o600 });
    const pluginDirectory = join(env.XDG_CONFIG_HOME, "coordinator");
    yield* fs.makeDirectory(pluginDirectory, { recursive: true, mode: 0o700 });
    const brokerTokenFile = join(home, "broker.token"), pluginConfigFile = join(pluginDirectory, "plugin.json"), stateDirectory = join(home, "coordinator-state");
    yield* fs.writeFileString(brokerTokenFile, sessionToken, { flag: "wx", mode: 0o600 });
    // ponytail: one private transparent proxy per host; Node's no-agent request adds no HTTP pool.
    let fault: "none" | "poll" | "presence" = "none";
    const traffic: { method: string; sessionId?: string; incarnation?: string; status: number; completed: boolean }[] = [];
    let proxyRequests = 0;
    const rpc = Schema.Struct({ method: Schema.String, params: Schema.optional(Schema.Struct({
      name: Schema.optional(Schema.String), arguments: Schema.optional(Schema.Struct({
        sessionId: Schema.optional(Schema.String), incarnation: Schema.optional(Schema.String),
      })),
    })) });
    const proxy = createServer((request, response) => {
      proxyRequests++;
      const chunks: Buffer[] = [];
      let bytes = 0;
      request.on("data", (chunk: Buffer) => { bytes += chunk.length; if (bytes > 64 * 1024) request.destroy(); else chunks.push(chunk); });
      request.on("end", () => {
        try {
          const body = Buffer.concat(chunks);
          const wire = body.length === 0 ? undefined : Schema.decodeSync(Schema.fromJsonString(rpc))(body.toString("utf8"));
          const method = wire?.method === "tools/call" ? wire.params?.name ?? wire.method : wire?.method ?? request.method ?? "";
          const entry = { method, ...wire?.params?.arguments, status: 0, completed: false };
          traffic.push(entry);
          if ((fault === "poll" && method === Commands.pollSession.name) || (fault === "presence" && (method === Commands.heartbeatSession.name || method === Commands.pollSession.name))) {
            entry.status = fault === "poll" ? 503 : 403;
            if (fault === "poll") fault = "none";
            response.writeHead(entry.status, { "content-type": "application/json" });
            response.end(JSON.stringify({ error: "Private control fault fixture" }));
            entry.completed = true;
            return;
          }
          const upstream = httpRequest(new URL(request.url ?? "/mcp", brokerUrl), { method: request.method, headers: { ...request.headers, host: `127.0.0.1:${address.port}` }, agent: false }, (incoming) => {
            entry.status = incoming.statusCode ?? 502;
            response.writeHead(entry.status, incoming.headers);
            incoming.once("end", () => { entry.completed = true; });
            incoming.once("error", () => response.destroy());
            incoming.pipe(response);
          });
          upstream.once("error", () => response.destroy());
          response.once("close", () => upstream.destroy());
          upstream.end(body);
        } catch (error) { console.error("Private control proxy failed", error); response.destroy(); }
      });
    });
    yield* Effect.acquireRelease(
      Effect.promise(() => new Promise<void>((resolve, reject) => { proxy.once("error", reject); proxy.listen(0, "127.0.0.1", resolve); })),
      () => Effect.promise(() => new Promise<void>((resolve, reject) => { proxy.closeAllConnections(); proxy.close((error) => error ? reject(error) : resolve()); })),
    );
    const proxyAddress = proxy.address();
    if (proxyAddress === null || typeof proxyAddress === "string") return yield* new CommandError({ message: "Expected private control proxy address" });
    yield* fs.writeFileString(pluginConfigFile, yield* Schema.encodeEffect(Schema.fromJsonString(PluginConfig))({
      brokerUrl: `http://127.0.0.1:${proxyAddress.port}`, tokenFile: brokerTokenFile, machineId: MachineId.make(`stock-${host}`), stateDirectory, host,
    }), { flag: "wx", mode: 0o600 });
    const scope = yield* Scope.fork(yield* Effect.scope);
    const client = yield* connectAgentClient({
      host, command: binary, args: ["--mode", "rpc", "--session-dir", sessions, "--no-extensions", "--model", "coordinator-scripted/scripted",
        ...(host === AgentHostEnum.omp ? ["--no-ui"] : []), "-e", join(root, "dist/plugins/plugin.js")],
      cwd: root, env: { ...env, COORDINATOR_AGENT_CONFIG: configFile }, connectTimeoutMs: 60000, plugin: { endpointFile, token },
    }).pipe(Effect.provideService(Scope.Scope, scope));
    yield* Effect.gen(function*() {
      const state = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(state.binding.host, host);
      assert.equal(state.cwd, root);
      for (const control of [...exercisedControls, ...guardedNegativeControls]) {
        assert.ok(client.capabilities.includes(control.name), `${host}: supported control ${control.name} must be advertised`);
      }
      assert.equal(state.isCompacting, false);
      assert.equal(state.pendingAsyncWorkCount, 0);
      const messages = yield* client.call(AgentTools.getMessages, { binding: client.binding }, { timeoutMs: 10000 });
      assert.deepEqual(messages.messages, []);
      const entries = yield* client.call(AgentTools.getEntries, { binding: client.binding }, { timeoutMs: 10000 });
      assert.ok(Array.isArray(entries.entries));
      const enrolled = yield* until(() => Effect.sync(() => [...broker.sessions.values()].filter((session) =>
        session.host === host && session.machineId === `stock-${host}` && session.verified && session.polledAt > 0)),
        (sessions) => sessions.length === 1);
      const session = enrolled[0]!;
      assert.equal(session.sessionId, client.binding.sessionId);
      assert.equal(session.repo, repo);
      const durableRows = yield* db.query((client) => client.selectFrom("sessions").select("data").where("id", "=", SessionId.make(session.sessionId)));
      assert.equal(durableRows.length, 1);
      const durableSession = yield* Schema.decodeEffect(Schema.fromJsonString(LiveSession))(durableRows[0]!.data);
      assert.equal(durableSession.incarnation, session.incarnation);
      const messageId = PeerId.make(yield* crypto.randomUUIDv4());
      const payload = { note: `Real ${host} public-plugin durable inbox delivery; no prompt requested` };
      const sent = yield* mcp.call(brokerUrl, coordinatorToken, Commands.sendPeer, {
        sessionId: session.sessionId, incarnation: session.incarnation, messageId, payload, ttlMs: 60000,
      });
      assert.equal(sent.committed, true);
      assert.equal(sent.delivered, false);
      const journalFile = join(stateDirectory, `${yield* platform.digest(session.sessionId)}.json`);
      yield* until(() => Effect.gen(function*() {
        if (!(yield* fs.exists(journalFile))) return false;
        const journal = yield* Schema.decodeEffect(Schema.fromJsonString(JournalState))(yield* fs.readFileString(journalFile));
        if (!journal.inbox[messageId]) return false;
        assert.deepEqual(journal.inbox[messageId].payload, payload);
        assert.equal(journal.inbox[messageId].digest, yield* platform.fingerprint(payload));
        assert.equal(journal.handled[JournalHandledKey.make(`peer:${messageId}`)], true);
        assert.equal(journal.consent, false, "Headless smoke cannot forge participant consent");
        const queued = yield* db.query((client) => client.selectFrom("peers").select("id").where("id", "=", PeerId.make(messageId)));
        return queued.length === 0;
      }), Boolean);
      const events: AgentLifecycleEvent[] = [];
      const watch = yield* client.watchLifecycle({ binding: client.binding, after: AgentEventSequence.make(0) }, (event) => { events.push(event); }).pipe(Effect.provideService(Scope.Scope, scope));
      yield* watch.drain;
      assert.equal(events[0]?.kind, "session_started");
      assert.equal(events.some((event) => event.kind === "run_settled"), false);
      const wrong = { ...client.binding, generation: AgentGeneration.make(client.binding.generation + 1) };
      yield* client.call(AgentTools.getMessages, { binding: wrong }, { timeoutMs: 10000 }).pipe(Effect.match({
        onSuccess: () => assert.fail("A foreign generation must not read private transcript data"),
        onFailure: (error) => assert.equal(error.code, "binding_mismatch"),
      }));
      for (const control of guardedNegativeControls) {
        yield* client.call(control, { binding: wrong, message: "Foreign generation must not dispatch this input" }, { timeoutMs: 10000 }).pipe(Effect.match({
          onSuccess: () => assert.fail(`${control.name}: foreign generation must not deliver input`),
          onFailure: (error) => assert.equal(error.code, "binding_mismatch"),
        }));
      }
      const shell = yield* client.call(AgentTools.bash, { binding: client.binding, command: "printf PLUGIN_EXEC_OK" }, { timeoutMs: 10000 });
      assert.equal(shell.exitCode, 0);
      assert.equal(shell.output, "PLUGIN_EXEC_OK");
      const abort = yield* client.call(AgentTools.abort, { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(abort.disposition, "requested", "Public abort acknowledges the request, not an outcome");
      const submit = (message: string) => client.prompt({ binding: client.binding, submissionId: AgentSubmissionId.make(randomUUID()), message }, {
        after: AgentEventSequence.make(0), timeoutMs: 60000, onAdmission: (admission) => { assert.equal(admission.binding.sessionId, session.sessionId); },
      });
      const complete = yield* submit("SDK_TOOL");
      assert.equal(complete.disposition, "settled");
      if (complete.disposition !== "settled") return yield* new CommandError({ message: "Expected SDK run" });
      assert.equal(complete.final.outcome, AgentRunOutcomeEnum.completed);
      const privateMessages = yield* client.call(AgentTools.getMessages, { binding: client.binding }, { timeoutMs: 10000 });
      assert.match(JSON.stringify(privateMessages.messages), /SDK_TOOL_OK/, "The actual agent tool continuation must be persisted in this owned transcript");
      const pausedRun = yield* Effect.forkIn(submit(`SDK_PAUSE_${host}`), scope);
      yield* until(() => Effect.sync(() => paused.has(host)), Boolean);
      let queuedAdmission = false;
      const queuedRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: AgentSubmissionId.make(randomUUID()), message: "SDK_QUEUED_INPUT", streamingBehavior: "followUp" }, {
        after: AgentEventSequence.make(0), timeoutMs: 60000, onAdmission: (admission) => { assert.equal(admission.disposition, "queued"); queuedAdmission = true; },
      }), scope);
      yield* until(() => Effect.sync(() => queuedAdmission), Boolean);
      yield* client.call(AgentTools.followUp, { binding: client.binding, message: "SDK_CONTINUATION" }, { timeoutMs: 10000 });
      yield* client.call(AgentTools.steer, { binding: client.binding, message: "SDK_STEERING" }, { timeoutMs: 10000 });
      const beforeIndependent = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
      let independentAdmitted = false;
      const independentRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: AgentSubmissionId.make(randomUUID()), message: "SDK_INDEPENDENT" }, {
        after: AgentEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { independentAdmitted = true; },
      }), scope);
      yield* until(() => client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 }),
        (value) => value.pendingMessageCount !== null && beforeIndependent.pendingMessageCount !== null && value.pendingMessageCount === beforeIndependent.pendingMessageCount + 1);
      assert.equal(independentAdmitted, false, "An independent FIFO ticket has no SDK admission while another run owns the stream");
      const finish = paused.get(host);
      assert.ok(finish);
      finish();
      const continued = yield* Fiber.join(pausedRun);
      assert.equal(continued.disposition, "settled");
      if (continued.disposition !== "settled") return yield* new CommandError({ message: "Expected continued SDK run" });
      assert.equal(continued.final.outcome, AgentRunOutcomeEnum.completed);
      const queued = yield* Fiber.join(queuedRun);
      assert.equal(queued.disposition, "settled");
      if (queued.disposition !== "settled") return yield* new CommandError({ message: "Expected SDK queued continuation" });
      assert.equal(queued.final.runId, continued.final.runId, "An actually queued input shares the active logical run, never a fabricated second outcome");
      const independent = yield* Fiber.join(independentRun);
      if (independent.disposition !== "settled") return yield* new CommandError({ message: "Expected independent SDK run" });
      assert.equal(independent.admission.disposition, AgentAdmissionDispositionEnum.started);
      assert.equal(independent.final.outcome, AgentRunOutcomeEnum.completed);
      assert.notEqual(independent.final.runId, continued.final.runId);
      const continuationMessages = yield* client.call(AgentTools.getMessages, { binding: client.binding }, { timeoutMs: 10000 });
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_QUEUED_INPUT/);
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_CONTINUATION/);
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_STEERING/);
      const retryResult = yield* submit(`SDK_RETRY_${host}`);
      if (retryResult.disposition !== "settled") return yield* new CommandError({ message: "Expected retried SDK run" });
      assert.equal(retryResult.final.outcome, AgentRunOutcomeEnum.completed);
      const compactResult = yield* submit(`SDK_COMPACT_${host}`);
      if (compactResult.disposition !== "settled") return yield* new CommandError({ message: "Expected compacted SDK run" });
      assert.equal(compactResult.final.outcome, AgentRunOutcomeEnum.completed);
      yield* watch.drain;
      assert.ok(events.some((event) => event.kind === "run_phase" && event.runId === retryResult.final.runId && event.phase === AgentRunPhaseEnum.retry), `${host}: SDK retry must remain inside ${retryResult.final.runId}; observed ${JSON.stringify(events.filter((event) => event.kind === "run_phase"))}`);
      assert.ok(events.some((event) => event.kind === "run_phase" && event.runId === compactResult.final.runId && event.phase === AgentRunPhaseEnum.compaction), `${host}: SDK compaction must remain inside ${compactResult.final.runId}; observed ${JSON.stringify(events.filter((event) => event.kind === "run_phase"))}`);
      const failed = yield* submit("SDK_ERROR");
      assert.equal(failed.disposition, "settled");
      if (failed.disposition !== "settled") return yield* new CommandError({ message: "Expected error SDK run" });
      assert.equal(failed.final.outcome, "error");
      let admittedAbort = false;
      const abortRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: AgentSubmissionId.make(yield* crypto.randomUUIDv4()), message: "SDK_ABORT" }, {
        after: AgentEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { admittedAbort = true; },
      }), scope);
      yield* until(() => Effect.sync(() => admittedAbort), Boolean);
      yield* client.call(AgentTools.abort, { binding: client.binding }, { timeoutMs: 10000 });
      const cancelled = yield* Fiber.join(abortRun);
      assert.equal(cancelled.disposition, "settled");
      if (cancelled.disposition !== "settled") return yield* new CommandError({ message: "Expected cancelled SDK run" });
      assert.equal(cancelled.final.outcome, "aborted");
      let replacing = false;
      const previousRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: AgentSubmissionId.make(randomUUID()), message: "SDK_ABORT" }, {
        after: AgentEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { replacing = true; },
      }), scope);
      yield* until(() => Effect.sync(() => replacing), Boolean);
      const replacement = yield* client.call(AgentTools.abortAndPrompt, { binding: client.binding, submissionId: AgentSubmissionId.make(randomUUID()), message: "SDK_REPLACEMENT" }, { timeoutMs: 60000 });
      const replaced = yield* Fiber.join(previousRun);
      if (replacement.disposition !== "settled" || replaced.disposition !== "settled") return yield* new CommandError({ message: "Expected replacement outcomes" });
      assert.equal(replaced.final.outcome, "aborted");
      assert.equal(replacement.final.outcome, "completed");
      assert.notEqual(replaced.final.runId, replacement.final.runId);
      if (host === AgentHostEnum.omp) {
        const background = yield* Effect.forkIn(submit("SDK_ASYNC"), scope);
        yield* until(() => client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 }), (value) => value.pendingAsyncWorkCount === null);
        const settled = yield* Fiber.join(background);
        if (settled.disposition !== "settled") return yield* new CommandError({ message: "Expected async SDK run" });
        assert.equal(settled.final.outcome, "completed");
        assert.equal(yield* fs.readFileString(join(home, "one")), "SDK_ASYNC_ONE");
        assert.equal(yield* fs.readFileString(join(home, "two")), "SDK_ASYNC_TWO", "A wake that starts another generation must finish before settlement");
        const cancellation = yield* Effect.forkIn(submit("SDK_ASYNC_CANCEL"), scope);
        yield* until(() => client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 }), (value) => value.pendingAsyncWorkCount === null);
        yield* client.call(AgentTools.abort, { binding: client.binding }, { timeoutMs: 10000 });
        const abortedAsync = yield* Fiber.join(cancellation);
        if (abortedAsync.disposition !== "settled") return yield* new CommandError({ message: "Expected async cancellation" });
        assert.equal(abortedAsync.final.outcome, "aborted");
        assert.equal(yield* fs.exists(join(home, "cancel")), false);
      }
      yield* watch.drain;
      for (const result of [complete, continued, independent, retryResult, compactResult, failed, cancelled, replaced, replacement]) assert.equal(events.filter((event) => event.kind === "run_settled" && event.runId === result.final.runId).length, 1);
      const ref = (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: root, env: { PATH: path }, extendEnv: false }))).trim();
      const sha = Evidence.fields.sha.make((yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root, env: { PATH: path }, extendEnv: false }))).trim());
      const taskId = TaskId.make(`owned-sdk-${host}`), deadline = (yield* Clock.currentTimeMillis) + 60000;
      const workerId = session.workerId;
      assert.ok(workerId);
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.submitTask, { taskId, prompt: "SDK_BROKER", repo, ref, sha, launcher: host, scope: "owned SDK execution",
        restrictions: "local scripted provider only", expectedEvidence: [], deadline, workerId, githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["supplementary"], bypassPeer: false, approved: true });
      yield* mcp.call(brokerUrl, sessionToken, Commands.replyPeer, { taskId, sessionId: session.sessionId, incarnation: session.incarnation, decision: OfferDecisionEnum.accepted });
      const addressed = { messageId: PeerId.make(`release-${host}`), revision: 1, sessionId: session.sessionId, incarnation: session.incarnation, workerId,
        repo, ref, sha, scope: "owned SDK execution", restrictions: "local scripted provider only", expectedEvidence: [], deadline };
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.addressTask, { taskId, address: addressed });
      yield* mcp.call(brokerUrl, sessionToken, Commands.readbackTask, { taskId, address: addressed, disposition: DispositionEnum.ready });
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.confirmTask, { taskId, revision: 1 });
      const delivered = yield* until(() => mcp.call(brokerUrl, coordinatorToken, Commands.getTask, { taskId }), (task) => task.state === TaskStateEnum.SUCCEEDED || task.state === TaskStateEnum.FAILED || task.state === TaskStateEnum.HELD);
      assert.equal(delivered.state, TaskStateEnum.SUCCEEDED);
      const proof = yield* Schema.decodeUnknownEffect(Schema.Struct({ admission: Schema.Struct({ binding: Schema.Struct({ sessionId: Schema.String }), submissionId: Schema.String }), final: Schema.Struct({ outcome: Schema.String }) }))(delivered.result);
      assert.equal(proof.admission.binding.sessionId, client.binding.sessionId);
      assert.equal(proof.final.outcome, "completed");
      const postRun = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(postRun.currentRunId, null);
      assert.equal(postRun.pendingMessageCount, 0);
      // All original agent and broker-addressed gates finish before permanent pause.
      const pauseCallsBefore = pauseRequests.get(host) ?? 0;
      const running = yield* Effect.forkIn(submit(`SDK_PAUSE_${host}`), scope);
      yield* until(() => Effect.sync(() => paused.has(host)), Boolean);
      const heldState = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(heldState.isStreaming, true);
      const runId = heldState.currentRunId;
      assert.ok(runId);
      const assertHeld = Effect.fnUntraced(function*() {
        const state = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
        assert.deepEqual(state.binding, client.binding);
        assert.equal(state.currentRunId, runId);
        assert.equal(state.isStreaming, true);
        assert.equal(state.pendingMessageCount, 0);
        yield* watch.drain;
        assert.equal(events.some((event) => event.kind === "run_settled" && event.runId === runId), false);
        assert.equal(paused.has(host), true, "Control pause must not close the real provider stream");
        assert.equal(pauseRequests.get(host), pauseCallsBefore + 1, "Control recovery must not resubmit the model prompt");
      });
      const presenceBefore = broker.sessions.get(session.sessionId);
      assert.ok(presenceBefore);
      const start = traffic.length, previousPoll = presenceBefore.polledAt, previousLease = presenceBefore.liveUntil;
      fault = "poll";
      yield* until(() => Effect.sync(() => traffic.slice(start).some((entry) => entry.method === Commands.pollSession.name && entry.status === 503 && entry.completed)), Boolean);
      yield* assertHeld();
      const failureIndex = traffic.findIndex((entry, index) => index >= start && entry.method === Commands.pollSession.name && entry.status === 503);
      yield* until(() => Effect.sync(() => {
        const recovered = traffic.slice(failureIndex + 1).filter((entry) => entry.method === Commands.heartbeatSession.name || entry.method === Commands.pollSession.name);
        const presence = broker.sessions.get(session.sessionId);
        return recovered.length >= 2 && recovered[0]?.method === Commands.heartbeatSession.name && recovered[0].status === 200 && recovered[0].completed &&
          recovered[1]?.method === Commands.pollSession.name && recovered[1].status === 200 && recovered[1].completed &&
          presence !== undefined && presence.incarnation === session.incarnation && presence.liveUntil > previousLease && presence.polledAt > previousPoll;
      }), Boolean);
      const pair = traffic.slice(failureIndex + 1).filter((entry) => entry.method === Commands.heartbeatSession.name || entry.method === Commands.pollSession.name).slice(0, 2);
      for (const entry of pair) { assert.equal(entry.sessionId, session.sessionId); assert.equal(entry.incarnation, session.incarnation); }
      yield* assertHeld();
      const fencedTaskId = TaskId.make(`pause-fenced-${host}`), fencedDeadline = (yield* Clock.currentTimeMillis) + 60000;
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.submitTask, { taskId: fencedTaskId, prompt: "SDK_FENCED_MUST_NOT_RUN", repo, ref, sha, launcher: host, scope: "owned SDK execution",
        restrictions: "local scripted provider only", expectedEvidence: [], deadline: fencedDeadline, workerId, githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["supplementary"], bypassPeer: false, approved: true });
      yield* mcp.call(brokerUrl, sessionToken, Commands.replyPeer, { taskId: fencedTaskId, sessionId: session.sessionId, incarnation: session.incarnation, decision: OfferDecisionEnum.accepted });
      const fencedAddress = { ...addressed, messageId: PeerId.make(`pause-fenced-release-${host}`), deadline: fencedDeadline };
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.addressTask, { taskId: fencedTaskId, address: fencedAddress });
      yield* mcp.call(brokerUrl, sessionToken, Commands.readbackTask, { taskId: fencedTaskId, address: fencedAddress, disposition: DispositionEnum.ready });
      yield* mcp.call(brokerUrl, coordinatorToken, Commands.confirmTask, { taskId: fencedTaskId, revision: 1 });
      fault = "presence";
      yield* until(() => Effect.sync(() => traffic.some((entry) => (entry.method === Commands.pollSession.name || entry.method === Commands.heartbeatSession.name) && entry.status === 403 && entry.completed)), Boolean);
      yield* assertHeld();
      yield* Effect.sleep(500);
      const quietStart = proxyRequests, callsWhileHeld = fixtureCalls;
      yield* Effect.sleep(11000);
      assert.equal(proxyRequests, quietStart, "Static credentials must stop unchanged control traffic, including initialization");
      assert.equal(fixtureCalls, callsWhileHeld, "No background admission or business replay while the provider is held");
      yield* assertHeld();
      const release = paused.get(host);
      assert.ok(release);
      release();
      const survived = yield* Fiber.join(running);
      if (survived.disposition !== "settled") return yield* new CommandError({ message: "Expected SDK outcome after permanent control pause" });
      assert.equal(survived.final.runId, runId);
      assert.equal(survived.final.outcome, "completed");
      yield* Effect.sleep(3000);
      yield* watch.drain;
      const completedOutcomes = events.filter((event) => event.kind === "run_settled" && event.runId === runId).length;
      assert.equal(completedOutcomes, 1);
      assert.equal(proxyRequests, quietStart, "Finishing accepted work must not restart paused presence");
      assert.equal(fixtureCalls, callsWhileHeld, "An addressed release must remain fenced even after the accepted run completes");
      const finalState = yield* client.call(AgentTools.getState, { binding: client.binding }, { timeoutMs: 10000 });
      assert.deepEqual(finalState.binding, client.binding);
      assert.equal(finalState.currentRunId, null);
      assert.equal(finalState.pendingMessageCount, 0);
      const fencedTask = yield* mcp.call(brokerUrl, coordinatorToken, Commands.getTask, { taskId: fencedTaskId });
      assert.equal(fencedTask.state, TaskStateEnum.ACCEPTED);
      assert.equal(fencedTask.confirmedRevision, 1);
      assert.equal((yield* mcp.call(brokerUrl, coordinatorToken, Commands.getTask, { taskId })).state, TaskStateEnum.SUCCEEDED, "The prior addressed business result must never be replayed or regressed");
      assert.equal(traffic.filter((entry) => entry.status === 503).length, 1);
      assert.equal(traffic.filter((entry) => entry.status === 403).length, 1, "Static authorization failure must not retry presence");
      controlChecks.push({ host, runId, transientPollFaults: traffic.filter((entry) => entry.status === 503).length,
        recoveredHeartbeatPollPairs: pair.length / 2, permanentPresenceFaults: traffic.filter((entry) => entry.status === 403).length,
        quietControlRequests: proxyRequests - quietStart, completedOutcomes, providerHeldRequests: (pauseRequests.get(host) ?? 0) - pauseCallsBefore,
        fencedTaskState: fencedTask.state, retiredOwnerRequests: 0 });
      yield* watch.close;
    }).pipe(Effect.ensuring(Scope.close(scope, Exit.void)));
    yield* client.observedProcessClosed.pipe(Effect.timeout("10 seconds"));
    const retiredRequests = proxyRequests;
    yield* Effect.sleep(2200);
    const retiredOwnerRequests = proxyRequests - retiredRequests;
    assert.equal(retiredOwnerRequests, 0, `${host}: a physically closed owner must never resume old-binding traffic`);
    controlChecks[controlChecks.length - 1]!.retiredOwnerRequests = retiredOwnerRequests;
    observations.push({ host, binding: client.binding, pid: client.pid, capabilities: client.capabilities, physicalProcessClosed: true });
  }
  yield* Scope.close(brokerScope, Exit.void);
  yield* dropPostgres(brokerDatabaseUrl);
  yield* fs.writeFileString(transcript, JSON.stringify({ accepted: true, observations, controlChecks,
    controlsAcceptance: "accepted", exercisedControls: exercisedControls.map(descriptor => descriptor.name),
    guardedNegativeControls: { controls: guardedNegativeControls.map(descriptor => descriptor.name), result: "binding_mismatch before delivery" },
    sendDeliveryAcceptance: "accepted: actual SDK tool, steering, queued follow-up and independent FIFO admission",
    brokerPluginAcceptance: "accepted: authenticated enrollment, durable inbox and confirmed addressed SDK execution",
    wholeRunAcceptance: "accepted: correlated completed/error/aborted outcomes after retry, compaction, continuation, multi-generation OMP async wake and cancellation",
    terminalAcceptance: "not-exercised",
    consentedCaptureAcceptance: "not-exercised: real participant consent requires SDK UI; broker regression remains in the separate unfiltered agent suite",
    fixtureModelCalls: fixtureCalls, externalModelCalls: 0 }), { flag: "wx", mode: 0o600 });
  console.log(JSON.stringify({ accepted: true, transcript, privateHome: directory, controlChecks, controlsAcceptance: "accepted", exercisedControls: exercisedControls.map(descriptor => descriptor.name), guardedNegativeControls: guardedNegativeControls.map(descriptor => descriptor.name),
    brokerPluginAcceptance: "accepted", wholeRunAcceptance: "accepted", terminalAcceptance: "not-exercised", consentedCaptureAcceptance: "not-exercised", fixtureModelCalls: fixtureCalls, externalModelCalls: 0 }));
});
NodeRuntime.runMain(Commands.runHostSmoke.executeDecoded({ runHostSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Platform.layer),
));
