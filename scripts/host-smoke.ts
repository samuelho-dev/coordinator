import assert from "node:assert/strict";
import { Clock, Config, Context, Effect, Exit, Layer, Schema, Scope } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { connectNativeClient } from "@samuelho/coordinator-native-control/client";
import { NativeEventSequence, NativeGeneration, NativeHost, NativePluginConfig, NativeSubmissionId, type NativeBinding, type NativeLifecycleEvent } from "@samuelho/coordinator-native-control/contracts";
import { BrokerConfig, CommandError, Commands, JournalHandledKey, JournalState, LiveSession, MachineId, MessageId, PluginConfig, type Json } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { Broker } from "../src/broker.ts";
import { Mcp } from "../src/http.ts";

// Installed stock CLIs and public extensions only. No fork, host build, SDK rewrite,
// global installation, provider call or terminal-rendering acceptance is inferred.
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
  const exercisedControls: readonly ["native.get_state", "native.get_messages", "native.get_entries", "native.bash", "native.abort"] = ["native.get_state", "native.get_messages", "native.get_entries", "native.bash", "native.abort"];
  const guardedNegativeControls: ("native.steer" | "native.follow_up")[] = ["native.steer", "native.follow_up"];
  const repo = yield* platform.inspectedRepo(root);
  const coordinatorToken = Hex.encode(yield* crypto.randomBytes(32));
  const sessionToken = Hex.encode(yield* crypto.randomBytes(32));
  const brokerConfig = yield* Schema.decodeUnknownEffect(BrokerConfig)({
    database: join(directory, "broker", "ledger.sqlite"), host: "127.0.0.1", port: 0,
    backupDirectory: join(directory, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: {},
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
    const env = { HOME: home, PATH: path, LANG: "C.UTF-8", TERM: "dumb", PI_CODING_AGENT_DIR: agent,
      XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state"), XDG_CACHE_HOME: join(home, "cache") };
    const version = (yield* platform.exec(ChildProcess.make(binary, ["--version"], { cwd: root, env, extendEnv: false }))).trim();
    assert.equal(version, host === "pi" ? piVersion : ompVersion, `${host}: installed CLI version mismatch`);
    const token = Hex.encode(yield* crypto.randomBytes(32));
    const tokenFile = join(home, "native.token"), endpointFile = join(home, "endpoint.json"), configFile = join(home, "native-plugin.json");
    yield* fs.writeFileString(tokenFile, token, { flag: "wx", mode: 0o600 });
    yield* fs.writeFileString(configFile, yield* Schema.encodeEffect(Schema.fromJsonString(NativePluginConfig))({ host, endpointFile, tokenFile }), { flag: "wx", mode: 0o600 });
    const brokerTokenFile = join(home, "broker.token"), pluginConfigFile = join(home, "coordinator-plugin.json"), stateDirectory = join(home, "coordinator-state");
    yield* fs.writeFileString(brokerTokenFile, sessionToken, { flag: "wx", mode: 0o600 });
    yield* fs.writeFileString(pluginConfigFile, yield* Schema.encodeEffect(Schema.fromJsonString(PluginConfig))({
      brokerUrl, tokenFile: brokerTokenFile, machineId: MachineId.make(`stock-${host}`), stateDirectory, host,
    }), { flag: "wx", mode: 0o600 });
    const scope = yield* Scope.fork(yield* Effect.scope);
    const client = yield* connectNativeClient({
      host, command: binary, args: ["--mode", "rpc", "--session-dir", sessions, "--no-extensions",
        // ponytail: OMP needs a catalog model to start RPC; no auth or provider call is needed.
        ...(host === "omp" ? ["--model", "openai/gpt-4o", "--no-ui"] : []),
        "-e", join(root, "dist/plugins/plugin.js"), "-e", join(root, "dist/plugins/native-plugin.js")],
      cwd: root, env: { ...env, COORDINATOR_CONFIG: pluginConfigFile, COORDINATOR_NATIVE_CONFIG: configFile }, connectTimeoutMs: 60000, plugin: { endpointFile, token },
    }).pipe(Effect.provideService(Scope.Scope, scope));
    yield* Effect.gen(function*() {
      const state = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 10000 });
      assert.equal(state.binding.host, host);
      assert.equal(state.cwd, root);
      for (const control of [...exercisedControls, ...guardedNegativeControls]) {
        assert.ok(client.capabilities.includes(control), `${host}: supported control ${control} must be advertised`);
      }
      assert.equal(state.isCompacting, null, "Unknown public busy fields must not become false");
      assert.equal(state.pendingAsyncWorkCount, null, "Unknown async ownership must not authorize readiness");
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
      yield* client.call("native.prompt", { binding: client.binding, submissionId: NativeSubmissionId.make(yield* crypto.randomUUIDv4), message: "This unsupported request must never reach a provider" }, { timeoutMs: 10000 }).pipe(Effect.match({
        onSuccess: () => assert.fail("Stock public hooks cannot manufacture whole-run prompt proof"),
        onFailure: (error) => assert.equal(error.code, "unsupported_operation"),
      }));
      yield* watch.drain;
      assert.equal(events.some((event) => event.kind === "submission_admitted" || event.kind === "run_settled"), false);
      yield* watch.close;
    }).pipe(Effect.ensuring(Scope.close(scope, Exit.void)));
    yield* client.observedProcessClosed.pipe(Effect.timeout("10 seconds"));
    observations.push({ host, binding: client.binding, pid: client.pid, capabilities: client.capabilities, physicalProcessClosed: true });
  }
  yield* fs.writeFileString(transcript, JSON.stringify({ accepted: true, observations,
    controlsAcceptance: "accepted", exercisedControls,
    guardedNegativeControls: { controls: guardedNegativeControls, result: "binding_mismatch before delivery" },
    sendDeliveryAcceptance: "not-exercised: idle public sendUserMessage starts a turn; no provider requested",
    brokerPluginAcceptance: "accepted: authenticated heartbeat/poll and durable inbox/ack for both stock hosts",
    wholeRunAcceptance: "blocked: stock public hooks lack correlated prompt admission and final outcome",
    terminalAcceptance: "not-exercised",
    consentedCaptureAcceptance: "not-exercised: real participant consent requires native UI; broker regression remains in the separate unfiltered native suite",
    modelCallsRequested: 0 }), { flag: "wx", mode: 0o600 });
  console.log(JSON.stringify({ accepted: true, transcript, privateHome: directory, controlsAcceptance: "accepted", exercisedControls, guardedNegativeControls,
    brokerPluginAcceptance: "accepted", wholeRunAcceptance: "blocked", terminalAcceptance: "not-exercised", consentedCaptureAcceptance: "not-exercised", modelCallsRequested: 0 }));
});
NodeRuntime.runMain(Commands["program.smoke:hosts"].execute({ runHostSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Platform.layer),
));
