import assert from "node:assert/strict";
import { createServer } from "node:http";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { Clock, Config, Context, Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { connectNativeClient } from "@samuelho/coordinator-native-control/client";
import { NativeEventSequence, NativeGeneration, NativeHost, NativePluginConfig, NativeSubmissionId } from "@samuelho/coordinator-native-control/contracts";
import type { NativeBinding, NativeLifecycleEvent, NativeToolName } from "@samuelho/coordinator-native-control/contracts";
import { BrokerConfig, CommandError, Commands, GitHubRepositoryId, JournalHandledKey, JournalState, LiveSession, MachineId, MessageId, PluginConfig, Sha, TaskId } from "../src/contracts.ts";
import type { Json } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { Broker } from "../src/broker.ts";
import { Mcp } from "../src/http.ts";

// Installed CLIs, unmodified public SDKs and real provider parsing against a local SSE fixture.
// No external provider, global profile, native UI consent or terminal rendering is inferred.
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
  const exercisedControls: readonly NativeToolName[] = ["native.get_state", "native.get_messages", "native.get_entries", "native.bash", "native.abort", "native.prompt", "native.abort_and_prompt", "native.steer", "native.follow_up"];
  const guardedNegativeControls: ("native.steer" | "native.follow_up")[] = ["native.steer", "native.follow_up"];
  let fixtureCalls = 0;
  const paused = new Map<string, () => void>();
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
        const wire = Schema.decodeUnknownSync(Schema.fromJsonString(requestBody))(Buffer.concat(chunks).toString("utf8"));
        fixtureCalls++;
        const user = JSON.stringify(wire.messages.findLast((message) => message.role === "user")?.content ?? "");
        const primary = wire.tools?.some((tool) => tool.function.name === "bash") === true;
        const retry = user.includes("SDK_RETRY_pi") ? "pi" : user.includes("SDK_RETRY_omp") ? "omp" : undefined;
        if (primary && retry !== undefined) {
          const attempt = (retryAttempts.get(retry) ?? 0) + 1;
          retryAttempts.set(retry, attempt);
          // OMP does six HTTP attempts plus one provider replay before session-level retry.
          if (attempt <= (retry === "omp" ? 12 : 1)) {
            response.writeHead(503, { "content-type": "application/json" }); response.end(JSON.stringify({ error: { message: "Scripted temporary provider overload", type: "server_error" } })); return;
          }
        }
        const compact = user.includes("SDK_COMPACT_pi") ? "pi" : user.includes("SDK_COMPACT_omp") ? "omp" : undefined;
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
        const pause = primary && user.includes("SDK_PAUSE_pi") ? "pi" : primary && user.includes("SDK_PAUSE_omp") ? "omp" : undefined;
        if (pause !== undefined) { paused.set(pause, finish); response.once("close", () => paused.delete(pause)); }
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
  const brokerConfig = yield* Schema.decodeUnknownEffect(BrokerConfig)({
    database: join(directory, "broker", "ledger.sqlite"), host: "127.0.0.1", port: 0,
    backupDirectory: join(directory, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: Object.fromEntries([[repo, 1]]),
    principals: [
      { id: "coordinator", owner: "stock-smoke", roles: ["coordinator"], repos: [repo], tokenHash: yield* platform.digest(coordinatorToken) },
      { id: "session", owner: "stock-smoke", roles: ["session"], repos: [repo], tokenHash: yield* platform.digest(sessionToken) },
    ],
  });
  const brokerContext = yield* Layer.build(Broker.layer(brokerConfig).pipe(
    Layer.provideMerge(Mcp.layer), Layer.provide(Layer.succeed(Platform, platform)),
  ));
  const broker = Context.get(brokerContext, Broker);
  const mcp = Context.get(brokerContext, Mcp);
  const address = broker.server.address;
  if (address._tag !== "InetAddressV4") return yield* new CommandError({ message: "Expected loopback IPv4 broker" });
  const brokerUrl = `http://127.0.0.1:${address.port}`;
  assert.equal((yield* mcp.call(brokerUrl, coordinatorToken, "health", {})).status, "ok");
  const until = Effect.fnUntraced(function*<A, E, R>(read: () => Effect.Effect<A, E, R>, ready: (value: A) => boolean) {
    const deadline = (yield* Clock.currentTimeMillis) + 30000;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const value = yield* read();
      if (ready(value)) return value;
      yield* Effect.sleep(100);
    }
    return yield* new CommandError({ message: "Stock plugin enrollment or durable delivery deadline expired" });
  });
  const observations: { host: NativeHost; binding: NativeBinding; pid: number; capabilities: readonly string[]; physicalProcessClosed: boolean }[] = [];
  for (const host of NativeHost.literals) {
    const binary = yield* fs.realPath(host === "pi" ? pi : omp);
    const home = join(directory, host), agent = join(home, "agent"), sessions = join(home, "sessions");
    for (const folder of [home, agent, sessions]) yield* fs.makeDirectory(folder, { mode: 0o700 });
    yield* fs.writeFileString(join(agent, host === "pi" ? "models.json" : "models.yml"), JSON.stringify(models), { flag: "wx", mode: 0o600 });
    asyncDirectory = home;
    yield* fs.writeFileString(join(agent, host === "pi" ? "settings.json" : "config.yml"), JSON.stringify({
      compaction: { keepRecentTokens: 1, reserveTokens: 512 }, ...(host === "omp" ? { async: { enabled: true } } : {}),
    }), { flag: "wx", mode: 0o600 });
    const env = { HOME: home, PATH: path, LANG: "C.UTF-8", TERM: "dumb", PI_CODING_AGENT_DIR: agent,
      XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state"), XDG_CACHE_HOME: join(home, "cache") };
    const version = (yield* platform.exec(ChildProcess.make(binary, ["--version"], { cwd: root, env, extendEnv: false }))).trim();
    assert.equal(version, host === "pi" ? piVersion : ompVersion, `${host}: installed CLI version mismatch`);
    const token = Hex.encode(yield* crypto.randomBytes(32));
    const tokenFile = join(home, "native.token"), endpointFile = join(home, "endpoint.json"), configFile = join(home, "native-plugin.json");
    yield* fs.writeFileString(tokenFile, token, { flag: "wx", mode: 0o600 });
    yield* fs.writeFileString(configFile, yield* Schema.encodeEffect(Schema.fromJsonString(NativePluginConfig))({ host, endpointFile, tokenFile }), { flag: "wx", mode: 0o600 });
    const pluginDirectory = join(env.XDG_CONFIG_HOME, "coordinator");
    yield* fs.makeDirectory(pluginDirectory, { recursive: true, mode: 0o700 });
    const brokerTokenFile = join(home, "broker.token"), pluginConfigFile = join(pluginDirectory, "plugin.json"), stateDirectory = join(home, "coordinator-state");
    yield* fs.writeFileString(brokerTokenFile, sessionToken, { flag: "wx", mode: 0o600 });
    yield* fs.writeFileString(pluginConfigFile, yield* Schema.encodeEffect(Schema.fromJsonString(PluginConfig))({
      brokerUrl, tokenFile: brokerTokenFile, machineId: MachineId.make(`stock-${host}`), stateDirectory, host,
    }), { flag: "wx", mode: 0o600 });
    const scope = yield* Scope.fork(yield* Effect.scope);
    const client = yield* connectNativeClient({
      host, command: binary, args: ["--mode", "rpc", "--session-dir", sessions, "--no-extensions", "--model", "coordinator-scripted/scripted",
        ...(host === "omp" ? ["--no-ui"] : []), "-e", join(root, "dist/plugins/plugin.js")],
      cwd: root, env: { ...env, COORDINATOR_NATIVE_CONFIG: configFile }, connectTimeoutMs: 60000, plugin: { endpointFile, token },
    }).pipe(Effect.provideService(Scope.Scope, scope));
    yield* Effect.gen(function*() {
      const state = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(state.binding.host, host);
      assert.equal(state.cwd, root);
      for (const control of [...exercisedControls, ...guardedNegativeControls]) {
        assert.ok(client.capabilities.includes(control), `${host}: supported control ${control} must be advertised`);
      }
      assert.equal(state.isCompacting, false);
      assert.equal(state.pendingAsyncWorkCount, 0);
      const messages = yield* client.call("native.get_messages", { binding: client.binding }, { timeoutMs: 10000 });
      assert.deepEqual(messages.messages, []);
      const entries = yield* client.call("native.get_entries", { binding: client.binding }, { timeoutMs: 10000 });
      assert.ok(Array.isArray(entries.entries));
      const enrolled = yield* until(() => Effect.sync(() => [...broker.sessions.values()].filter((session) =>
        session.host === host && session.machineId === `stock-${host}` && session.verified && session.polledAt > 0)),
        (sessions) => sessions.length === 1);
      const session = enrolled[0]!;
      assert.equal(session.sessionId, client.binding.sessionId);
      assert.equal(session.repo, repo);
      const durableRows = yield* broker.sql.unsafe<Json>("SELECT data FROM sessions WHERE id=?", [session.sessionId]);
      assert.equal(durableRows.length, 1);
      const durableSession = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LiveSession))(durableRows[0]!.data);
      assert.equal(durableSession.incarnation, session.incarnation);
      const messageId = MessageId.make(yield* crypto.randomUUIDv4);
      const payload = { note: `Real ${host} public-plugin durable inbox delivery; no prompt requested` };
      const sent = yield* mcp.call(brokerUrl, coordinatorToken, "peer.send", {
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
        const queued = yield* broker.sql.unsafe<Json>("SELECT id FROM peers WHERE id=?", [messageId]);
        return queued.length === 0;
      }), Boolean);
      const events: NativeLifecycleEvent[] = [];
      const watch = yield* client.watchLifecycle({ binding: client.binding, after: NativeEventSequence.make(0) }, (event) => { events.push(event); }).pipe(Effect.provideService(Scope.Scope, scope));
      yield* watch.drain;
      assert.equal(events[0]?.kind, "session_started");
      assert.equal(events.some((event) => event.kind === "run_settled"), false);
      const wrong = { ...client.binding, generation: NativeGeneration.make(client.binding.generation + 1) };
      yield* client.call("native.get_messages", { binding: wrong }, { timeoutMs: 10000 }).pipe(Effect.match({
        onSuccess: () => assert.fail("A foreign generation must not read private transcript data"),
        onFailure: (error) => assert.equal(error.code, "binding_mismatch"),
      }));
      for (const control of guardedNegativeControls) {
        yield* client.call(control, { binding: wrong, message: "Foreign generation must not dispatch this input" }, { timeoutMs: 10000 }).pipe(Effect.match({
          onSuccess: () => assert.fail(`${control}: foreign generation must not deliver input`),
          onFailure: (error) => assert.equal(error.code, "binding_mismatch"),
        }));
      }
      const shell = yield* client.call("native.bash", { binding: client.binding, command: "printf PLUGIN_EXEC_OK" }, { timeoutMs: 10000 });
      assert.equal(shell.exitCode, 0);
      assert.equal(shell.output, "PLUGIN_EXEC_OK");
      const abort = yield* client.call("native.abort", { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(abort.disposition, "requested", "Public abort acknowledges the request, not an outcome");
      const submit = (message: string) => client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(randomUUID()), message }, {
        after: NativeEventSequence.make(0), timeoutMs: 60000, onAdmission: (admission) => { assert.equal(admission.binding.sessionId, session.sessionId); },
      });
      const complete = yield* submit("SDK_TOOL");
      assert.equal(complete.disposition, "settled");
      if (complete.disposition !== "settled") return yield* new CommandError({ message: "Expected SDK run" });
      assert.equal(complete.final.outcome, "completed");
      const privateMessages = yield* client.call("native.get_messages", { binding: client.binding }, { timeoutMs: 10000 });
      assert.match(JSON.stringify(privateMessages.messages), /SDK_TOOL_OK/, "The actual native tool continuation must be persisted in this owned transcript");
      const pausedRun = yield* Effect.forkIn(submit(`SDK_PAUSE_${host}`), scope);
      yield* until(() => Effect.sync(() => paused.has(host)), Boolean);
      let queuedAdmission = false;
      const queuedRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(randomUUID()), message: "SDK_QUEUED_INPUT", streamingBehavior: "followUp" }, {
        after: NativeEventSequence.make(0), timeoutMs: 60000, onAdmission: (admission) => { assert.equal(admission.disposition, "queued"); queuedAdmission = true; },
      }), scope);
      yield* until(() => Effect.sync(() => queuedAdmission), Boolean);
      yield* client.call("native.follow_up", { binding: client.binding, message: "SDK_CONTINUATION" }, { timeoutMs: 10000 });
      yield* client.call("native.steer", { binding: client.binding, message: "SDK_STEERING" }, { timeoutMs: 10000 });
      const beforeIndependent = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 });
      let independentAdmitted = false;
      const independentRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(randomUUID()), message: "SDK_INDEPENDENT" }, {
        after: NativeEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { independentAdmitted = true; },
      }), scope);
      yield* until(() => client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 }),
        (value) => value.pendingMessageCount !== null && beforeIndependent.pendingMessageCount !== null && value.pendingMessageCount === beforeIndependent.pendingMessageCount + 1);
      assert.equal(independentAdmitted, false, "An independent FIFO ticket has no SDK admission while another run owns the stream");
      const finish = paused.get(host);
      assert.ok(finish);
      finish();
      const continued = yield* Fiber.join(pausedRun);
      assert.equal(continued.disposition, "settled");
      if (continued.disposition !== "settled") return yield* new CommandError({ message: "Expected continued SDK run" });
      assert.equal(continued.final.outcome, "completed");
      const queued = yield* Fiber.join(queuedRun);
      assert.equal(queued.disposition, "settled");
      if (queued.disposition !== "settled") return yield* new CommandError({ message: "Expected SDK queued continuation" });
      assert.equal(queued.final.runId, continued.final.runId, "An actually queued input shares the active logical run, never a fabricated second outcome");
      const independent = yield* Fiber.join(independentRun);
      if (independent.disposition !== "settled") return yield* new CommandError({ message: "Expected independent SDK run" });
      assert.equal(independent.admission.disposition, "started");
      assert.equal(independent.final.outcome, "completed");
      assert.notEqual(independent.final.runId, continued.final.runId);
      const continuationMessages = yield* client.call("native.get_messages", { binding: client.binding }, { timeoutMs: 10000 });
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_QUEUED_INPUT/);
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_CONTINUATION/);
      assert.match(JSON.stringify(continuationMessages.messages), /SDK_STEERING/);
      const retryResult = yield* submit(`SDK_RETRY_${host}`);
      if (retryResult.disposition !== "settled") return yield* new CommandError({ message: "Expected retried SDK run" });
      assert.equal(retryResult.final.outcome, "completed");
      const compactResult = yield* submit(`SDK_COMPACT_${host}`);
      if (compactResult.disposition !== "settled") return yield* new CommandError({ message: "Expected compacted SDK run" });
      assert.equal(compactResult.final.outcome, "completed");
      yield* watch.drain;
      assert.ok(events.some((event) => event.kind === "run_phase" && event.runId === retryResult.final.runId && event.phase === "retry"), `${host}: SDK retry must remain inside ${retryResult.final.runId}; observed ${JSON.stringify(events.filter((event) => event.kind === "run_phase"))}`);
      assert.ok(events.some((event) => event.kind === "run_phase" && event.runId === compactResult.final.runId && event.phase === "compaction"), `${host}: SDK compaction must remain inside ${compactResult.final.runId}; observed ${JSON.stringify(events.filter((event) => event.kind === "run_phase"))}`);
      const failed = yield* submit("SDK_ERROR");
      assert.equal(failed.disposition, "settled");
      if (failed.disposition !== "settled") return yield* new CommandError({ message: "Expected error SDK run" });
      assert.equal(failed.final.outcome, "error");
      let admittedAbort = false;
      const abortRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(yield* crypto.randomUUIDv4), message: "SDK_ABORT" }, {
        after: NativeEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { admittedAbort = true; },
      }), scope);
      yield* until(() => Effect.sync(() => admittedAbort), Boolean);
      yield* client.call("native.abort", { binding: client.binding }, { timeoutMs: 10000 });
      const cancelled = yield* Fiber.join(abortRun);
      assert.equal(cancelled.disposition, "settled");
      if (cancelled.disposition !== "settled") return yield* new CommandError({ message: "Expected cancelled SDK run" });
      assert.equal(cancelled.final.outcome, "aborted");
      let replacing = false;
      const previousRun = yield* Effect.forkIn(client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(randomUUID()), message: "SDK_ABORT" }, {
        after: NativeEventSequence.make(0), timeoutMs: 60000, onAdmission: () => { replacing = true; },
      }), scope);
      yield* until(() => Effect.sync(() => replacing), Boolean);
      const replacement = yield* client.call("native.abort_and_prompt", { binding: client.binding, submissionId: NativeSubmissionId.make(randomUUID()), message: "SDK_REPLACEMENT" }, { timeoutMs: 60000 });
      const replaced = yield* Fiber.join(previousRun);
      if (replacement.disposition !== "settled" || replaced.disposition !== "settled") return yield* new CommandError({ message: "Expected replacement outcomes" });
      assert.equal(replaced.final.outcome, "aborted");
      assert.equal(replacement.final.outcome, "completed");
      assert.notEqual(replaced.final.runId, replacement.final.runId);
      if (host === "omp") {
        const background = yield* Effect.forkIn(submit("SDK_ASYNC"), scope);
        yield* until(() => client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 }), (value) => value.pendingAsyncWorkCount === null);
        const settled = yield* Fiber.join(background);
        if (settled.disposition !== "settled") return yield* new CommandError({ message: "Expected async SDK run" });
        assert.equal(settled.final.outcome, "completed");
        assert.equal(yield* fs.readFileString(join(home, "one")), "SDK_ASYNC_ONE");
        assert.equal(yield* fs.readFileString(join(home, "two")), "SDK_ASYNC_TWO", "A wake that starts another generation must finish before settlement");
        const cancellation = yield* Effect.forkIn(submit("SDK_ASYNC_CANCEL"), scope);
        yield* until(() => client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 }), (value) => value.pendingAsyncWorkCount === null);
        yield* client.call("native.abort", { binding: client.binding }, { timeoutMs: 10000 });
        const abortedAsync = yield* Fiber.join(cancellation);
        if (abortedAsync.disposition !== "settled") return yield* new CommandError({ message: "Expected async cancellation" });
        assert.equal(abortedAsync.final.outcome, "aborted");
        assert.equal(yield* fs.exists(join(home, "cancel")), false);
      }
      yield* watch.drain;
      for (const result of [complete, continued, independent, retryResult, compactResult, failed, cancelled, replaced, replacement]) assert.equal(events.filter((event) => event.kind === "run_settled" && event.runId === result.final.runId).length, 1);
      const ref = (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: root }))).trim();
      const sha = Sha.make((yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }))).trim());
      const taskId = TaskId.make(`owned-sdk-${host}`), deadline = (yield* Clock.currentTimeMillis) + 60000;
      const workerId = session.workerId;
      assert.ok(workerId);
      yield* mcp.call(brokerUrl, coordinatorToken, "task.submit", { taskId, prompt: "SDK_BROKER", repo, ref, sha, launcher: host, scope: "owned SDK execution",
        restrictions: "local scripted provider only", expectedEvidence: [], deadline, workerId, githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["supplementary"], bypassPeer: false, approved: true });
      yield* mcp.call(brokerUrl, sessionToken, "peer.reply", { taskId, sessionId: session.sessionId, incarnation: session.incarnation, decision: "accepted" });
      const addressed = { messageId: MessageId.make(`release-${host}`), revision: 1, sessionId: session.sessionId, incarnation: session.incarnation, workerId,
        repo, ref, sha, scope: "owned SDK execution", restrictions: "local scripted provider only", expectedEvidence: [], deadline };
      yield* mcp.call(brokerUrl, coordinatorToken, "task.address", { taskId, address: addressed });
      yield* mcp.call(brokerUrl, sessionToken, "task.readback", { taskId, address: addressed, disposition: "ready" });
      yield* mcp.call(brokerUrl, coordinatorToken, "task.confirm", { taskId, revision: 1 });
      const delivered = yield* until(() => mcp.call(brokerUrl, coordinatorToken, "task.get", { taskId }), (task) => task.state === "SUCCEEDED" || task.state === "FAILED" || task.state === "HELD");
      assert.equal(delivered.state, "SUCCEEDED");
      const proof = yield* Schema.decodeUnknownEffect(Schema.Struct({ admission: Schema.Struct({ binding: Schema.Struct({ sessionId: Schema.String }), submissionId: Schema.String }), final: Schema.Struct({ outcome: Schema.String }) }))(delivered.result);
      assert.equal(proof.admission.binding.sessionId, client.binding.sessionId);
      assert.equal(proof.final.outcome, "completed");
      const postRun = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(postRun.currentRunId, null);
      assert.equal(postRun.pendingMessageCount, 0);
      yield* watch.close;
    }).pipe(Effect.ensuring(Scope.close(scope, Exit.void)));
    yield* client.observedProcessClosed.pipe(Effect.timeout("10 seconds"));
    observations.push({ host, binding: client.binding, pid: client.pid, capabilities: client.capabilities, physicalProcessClosed: true });
  }
  yield* fs.writeFileString(transcript, JSON.stringify({ accepted: true, observations,
    controlsAcceptance: "accepted", exercisedControls,
    guardedNegativeControls: { controls: guardedNegativeControls, result: "binding_mismatch before delivery" },
    sendDeliveryAcceptance: "accepted: actual SDK tool, steering, queued follow-up and independent FIFO admission",
    brokerPluginAcceptance: "accepted: authenticated enrollment, durable inbox and confirmed addressed SDK execution",
    wholeRunAcceptance: "accepted: correlated completed/error/aborted outcomes after retry, compaction, continuation, multi-generation OMP async wake and cancellation",
    terminalAcceptance: "not-exercised",
    consentedCaptureAcceptance: "not-exercised: real participant consent requires native UI; broker regression remains in the separate unfiltered native suite",
    fixtureModelCalls: fixtureCalls, externalModelCalls: 0 }), { flag: "wx", mode: 0o600 });
  console.log(JSON.stringify({ accepted: true, transcript, privateHome: directory, controlsAcceptance: "accepted", exercisedControls, guardedNegativeControls,
    brokerPluginAcceptance: "accepted", wholeRunAcceptance: "accepted", terminalAcceptance: "not-exercised", consentedCaptureAcceptance: "not-exercised", fixtureModelCalls: fixtureCalls, externalModelCalls: 0 }));
});
NodeRuntime.runMain(Commands["program.smoke:hosts"].execute({ runHostSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Platform.layer),
));
