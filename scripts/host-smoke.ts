// Node assert is the test oracle; Effect has no equivalent assertion API.
import assert from "node:assert/strict";
import { Cause, Clock, Config, Deferred, Effect, Exit, Fiber, Layer, Queue, Scope, Stream } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { HttpServer } from "effect/http";
import { NodeRuntime } from "@effect/platform-node";
import { NativeFrame, NativeRequestId, TaskId, MessageId, SessionId, MachineId, WorkerId, PrincipalId, OwnerId, ArchiveEntryId, GitHubRepositoryId, Repo, Sha, NativeState, NativeIdentity, Inbox, JournalState, InstallPlan, JsonValue, BrokerConfig, PluginConfig, Commands, CommandError, HttpError, type FramePending, type Observation, type ArchiveRow, type Address, type Work } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { canonicalJson, decode, decodeJson, encodeJson, Mcp, serverLayer } from "../src/http.ts";
import { LfFrames } from "../src/runner.ts";

// Only explicitly synthetic sessions and an empty, local Git repository are read.
// No model prompt, real host profile, user history, GitHub credential or service manager is used.
const runHostSmoke = () => Effect.gen(function*() {
  const platform = yield* Platform, mcpService = yield* Mcp;
  const { fs: fileSystem, path, crypto } = platform;
  const { join, resolve } = path;
  const root = resolve(import.meta.dirname, "..");
  const directory = yield* fileSystem.makeTempDirectory({ prefix: "coordinator-native-hosts-" });
  yield* fileSystem.chmod(directory, 0o700);
  const transcriptPath = join(directory, "transcript.json");
  const observations: Observation[] = [];
  let success = true;
  const record = Effect.fnUntraced(function*(operation: string, observed: unknown) {
    observations.push({ operation, observed: decodeJson(JsonValue, canonicalJson(observed)) });
    // Effect has no operating-system platform identifier API.
    yield* privateJson(transcriptPath, { platform: process.platform, macRuntimeProved: false, observations });
  });
  const privateJson = (file: string, value: JsonValue) => fileSystem.writeFileString(file, encodeJson(JsonValue, value), { mode: 0o600 });
  const privateDirectory = Effect.fnUntraced(function*(file: string) {
    yield* fileSystem.makeDirectory(file, { recursive: true, mode: 0o700 }); return file;
  });
  const ownerScope = yield* Effect.scope;
  const children = new Map<ChildProcessSpawner.ChildProcessHandle, Scope.Closeable>();
  const binaryPath = yield* Config.String("PATH").pipe(Config.withDefault("/usr/bin:/bin"));
  const ompBinary = yield* Config.String("COORDINATOR_TEST_OMP").pipe(Config.withDefault("/home/admin/.bun/bin/omp"));
  // Effect has no current runtime executable-path API.
  const runtime = process.execPath;
  // Whitelist, not inherited provider credentials/profile environment.
  const isolatedEnvironment = (home: string, agent: string) => ({
    PATH: binaryPath, HOME: home, USER: "coordinator-synthetic", LANG: "C.UTF-8", TERM: "dumb",
    XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state"),
    XDG_CACHE_HOME: join(home, "cache"), PI_CODING_AGENT_DIR: agent, PI_CODING_AGENT_SESSION_DIR: join(home, "sessions"),
    // Synthetic invalid key permits native model discovery; slash-command proof must start no model turn.
    OPENAI_API_KEY: "coordinator-synthetic-invalid-key",
  });
  const terminate = Effect.fnUntraced(function*(child: ChildProcessSpawner.ChildProcessHandle) {
    // The installed Effect adapter targets the detached process group, including descendants after leader exit.
    yield* child.kill({ killSignal: "SIGTERM", forceKillAfter: "3 seconds" }).pipe(Effect.timeout(7000), Effect.catch(error =>
      child.isRunning.pipe(Effect.flatMap(running => running ? Effect.fail(error) : Effect.void))
    ));
    assert.equal(yield* child.isRunning, false, "Owned host failed to terminate");
  });
  const start = Effect.fnUntraced(function*(binary: string, args: string[], cwd: string, env: Record<string, string>) {
    const scope = yield* Scope.fork(ownerScope);
    const input = yield* Queue.make<Uint8Array, Cause.Done>({ capacity: 32 });
    const child = yield* platform.spawner.spawn(ChildProcess.make(binary, args, {
      cwd, env, extendEnv: false, detached: true, killSignal: "SIGTERM", forceKillAfter: "1 second",
    })).pipe(Effect.provideService(Scope.Scope, scope));
    children.set(child, scope);
    yield* Scope.addFinalizer(scope, terminate(child).pipe(Effect.catchCause(() => Effect.gen(function*() {
      success = false; yield* record("cleanup-failed", { ownedProcessTerminationFailed: true });
    }).pipe(Effect.orDie))));
    yield* Stream.fromQueue(input).pipe(Stream.run(child.stdin), Effect.catchCause(() => Effect.void), Effect.forkIn(scope));
    return [child, input, scope] as const;
  });
  const stop = Effect.fnUntraced(function*(child: ChildProcessSpawner.ChildProcessHandle) {
    const scope = children.get(child);
    if (scope) { yield* Scope.close(scope, Exit.void); children.delete(child); }
  });
  const until = <T>(read: () => Effect.Effect<T, unknown>, check: (value: T) => boolean, label: string) => Effect.gen(function*() {
    for (;;) { const value = yield* read(); if (check(value)) return value; yield* Effect.sleep(100); }
  }).pipe(Effect.timeoutOrElse({ duration: 30000, orElse: () => Effect.fail(new CommandError({ message: `${label}: deadline expired` })) }));
  class NativeRpc {
    readonly frames: NativeFrame[] = [];
    readonly pending = new Map<NativeRequestId, FramePending>();
    failure: CommandError | undefined;
    private diagnostics = "";
    constructor(readonly child: ChildProcessSpawner.ChildProcessHandle, readonly input: Queue.Queue<Uint8Array, Cause.Done>, readonly host: string) {}
    initialize(scope: Scope.Scope) {
      const frames = new LfFrames();
      return Effect.gen({ self: this }, function*() {
        yield* this.child.stdout.pipe(Stream.runForEach(chunk => Effect.gen({ self: this }, function*() {
          const values = yield* Effect.try(() => frames.push(chunk));
          for (const value of values) {
            const frame = yield* Effect.try(() => decode(NativeFrame, value));
            this.frames.push(frame);
            if (frame.type === "response" && frame.id) {
              const pending = this.pending.get(frame.id);
              if (pending) {
                this.pending.delete(frame.id);
                if (frame.success === true) yield* Deferred.succeed(pending, frame);
                else yield* Deferred.fail(pending, new CommandError({ message: `${this.host}: ${frame.command} native RPC rejected (unsupported API or extension failure)` }));
              }
            }
          }
        })), Effect.andThen(Effect.try(() => frames.end())), Effect.catchCause(() => this.fail(new CommandError({ message: `${this.host}: invalid native RPC stdout (unsupported API)` }))), Effect.forkIn(scope));
        yield* this.child.stderr.pipe(Stream.decodeText(), Stream.runForEach(text => Effect.sync(() => {
          if (this.diagnostics.length < 4096) this.diagnostics += text.slice(0, 4096 - this.diagnostics.length);
        })), Effect.catchCause(() => Effect.void), Effect.forkIn(scope));
        yield* this.child.exitCode.pipe(Effect.flatMap(code => this.fail(new CommandError({ message: `${this.host}: native process exited (${code}); ${this.diagnostics}` }))), Effect.catchCause(() => this.fail(new CommandError({ message: `${this.host}: native process unavailable` }))), Effect.forkIn(scope));
      });
    }
    fail(error: CommandError) {
      return Effect.gen({ self: this }, function*() {
        this.failure = error;
        for (const pending of this.pending.values()) yield* Deferred.fail(pending, error);
        this.pending.clear();
      });
    }
    send(value: JsonValue) {
      return Effect.suspend(() => this.failure ? Effect.fail(this.failure) : Queue.offer(this.input, new TextEncoder().encode(`${encodeJson(JsonValue, value)}\n`)));
    }
    request(type: string, fields: Record<string, JsonValue> = {}) {
      return Effect.gen({ self: this }, function*() {
        const id = NativeRequestId.make(yield* crypto.randomUUIDv4), result = yield* Deferred.make<NativeFrame, CommandError>();
        this.pending.set(id, result);
        return yield* Effect.gen({ self: this }, function*() {
          yield* this.send({ type, id, ...fields });
          const frame = yield* Deferred.await(result);
          assert.equal(frame.command, type); return frame;
        }).pipe(Effect.timeoutOrElse({ duration: 60000, orElse: () => Effect.fail(new CommandError({ message: `${this.host}: ${type} response deadline expired; frames=${this.frames.map(frame => frame.type).join(",")}; ${this.diagnostics}` })) }), Effect.ensuring(Effect.sync(() => this.pending.delete(id))));
      });
    }
    frame(from: number, check: (frame: NativeFrame) => boolean) {
      return until(() => Effect.try(() => {
        if (this.failure) throw this.failure;
        return this.frames.slice(from).find(check);
      }), value => value !== undefined, `${this.host}: missing expected extension_ui_request`).pipe(Effect.map(frame => { assert.ok(frame); return frame; }));
    }
    command(action: string, check: (value: unknown) => boolean) {
      return Effect.gen({ self: this }, function*() {
        const from = this.frames.length;
        yield* this.request("prompt", { message: `/coordinator ${action}` });
        const frame = yield* this.frame(from, item => {
          if (item.type !== "extension_ui_request" || item.method !== "notify" || typeof item.message !== "string") return false;
          if (item.notifyType === "error") throw new CommandError({ message: `${this.host}: coordinator command failed` });
          let value: JsonValue;
          try { value = decodeJson(JsonValue, item.message); } catch { return false; }
          return check(value);
        });
        assert.ok(typeof frame.message === "string");
        return decodeJson(JsonValue, frame.message);
      });
    }
    rejectedCommand(action: string) {
      return Effect.gen({ self: this }, function*() {
        const from = this.frames.length;
        yield* this.request("prompt", { message: `/coordinator ${action}` });
        yield* this.frame(from, item => item.type === "extension_ui_request" && item.method === "notify" && item.notifyType === "error");
      });
    }
  }
  const run = Effect.fnUntraced(function*(binary: string, args: string[], cwd: string, env: Record<string, string>) {
    const [child, input, scope] = yield* start(binary, args, cwd, env);
    return yield* Effect.gen(function*() {
      yield* Queue.end(input);
      yield* Stream.runDrain(child.stderr).pipe(Effect.forkIn(scope));
      let size = 0;
      const output = yield* child.stdout.pipe(Stream.mapEffect(bytes => {
        size += bytes.length;
        return size > 1024 * 1024 ? Effect.fail(new CommandError({ message: "Native command output limit exceeded" })) : Effect.succeed(bytes);
      }), Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk));
      assert.equal(yield* child.exitCode, 0, `Native command failed: ${args[0]} (output suppressed)`);
      return output;
    }).pipe(Effect.timeout(30000), Effect.ensuring(stop(child)));
  });
  const repo = Repo.make("github.com/samuelho-dev/coordinator"), owner = OwnerId.make("synthetic-host-smoke-owner");
  const coordinatorToken = Hex.encode(yield* crypto.randomBytes(32)), sessionToken = Hex.encode(yield* crypto.randomBytes(32));
  const brokerPort = yield* Effect.scoped(Effect.gen(function*() {
    const server = yield* HttpServer.HttpServer;
    assert.equal(server.address._tag, "InetAddressV4");
    if (server.address._tag !== "InetAddressV4") return yield* Effect.fail(new CommandError({ message: "No loopback port" }));
    return server.address.port;
  }).pipe(Effect.provide(serverLayer("127.0.0.1", 0))));
  const brokerUrl = `http://127.0.0.1:${brokerPort}`;
  const brokerConfig: BrokerConfig = { database: join(directory, "broker", "ledger.sqlite"), host: "127.0.0.1", port: brokerPort,
    backupDirectory: join(directory, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000,
    repositoryIds: { [repo]: GitHubRepositoryId.make(1) },
    principals: [{ id: PrincipalId.make("synthetic-control"), owner, roles: ["coordinator"], repos: [repo], tokenHash: yield* platform.digest(coordinatorToken) },
      { id: PrincipalId.make("synthetic-native"), owner, roles: ["session"], repos: [repo], tokenHash: yield* platform.digest(sessionToken) }] };
  const brokerPath = join(directory, "broker.json"); yield* fileSystem.writeFileString(brokerPath, encodeJson(BrokerConfig, brokerConfig), { mode: 0o600 });
  const home = yield* privateDirectory(join(directory, "broker-home")), brokerEnv = isolatedEnvironment(home, join(home, "agent"));
  const syntheticRepo = yield* privateDirectory(join(directory, "synthetic-repository"));
  const gitEnv = { ...brokerEnv, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" };
  const git = (args: string[]) => run("git", args, syntheticRepo, gitEnv).pipe(Effect.map(text => text.trim()));
  const rpc = mcpService.call;
  const rejected = (effect: Effect.Effect<unknown, unknown>, status: number) => effect.pipe(Effect.match({
    onFailure: error => assert.ok(error instanceof HttpError && error.status === status),
    onSuccess: () => assert.fail("Expected command rejection"),
  }));
  yield* Effect.gen(function*() {
  yield* git(["init", "--initial-branch=main"]);
  yield* git(["-c", "user.name=Synthetic Smoke", "-c", "user.email=synthetic@example.invalid", "commit", "--allow-empty", "-m", "Synthetic native host smoke only"]);
  yield* git(["remote", "add", "origin", "https://github.com/samuelho-dev/coordinator.git"]);
  const sha = decode(Sha, yield* git(["rev-parse", "HEAD"]));
  const [broker, , brokerScope] = yield* start(runtime, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "src/main.ts"), "broker"], root, { ...brokerEnv, COORDINATOR_CONFIG: brokerPath });
  yield* Stream.runDrain(broker.stdout).pipe(Effect.forkIn(brokerScope));
  yield* Stream.runDrain(broker.stderr).pipe(Effect.forkIn(brokerScope));
  yield* until(() => Effect.gen(function*() {
    if (!(yield* broker.isRunning)) return yield* Effect.fail(new CommandError({ message: "Private broker exited" }));
    return yield* rpc(brokerUrl, coordinatorToken, "health", {}, 1000).pipe(Effect.as(true), Effect.catch(() => Effect.succeed(false)));
  }), value => value, "Private broker readiness");
  for (const host of ["omp", "pi"] as const) {
    const binary = host === "omp" ? ompBinary : join(root, "node_modules/.bin/pi");
    const hostHome = yield* privateDirectory(join(directory, host)), agent = yield* privateDirectory(join(hostHome, "agent"));
    const env = isolatedEnvironment(hostHome, agent);
    const version = (yield* run(binary, ["--version"], syntheticRepo, env)).trim();
    assert.match(version, host === "omp" ? /\b18\.6\.1\b/ : /\b1\.0\.4\b/, `${host}: unexpected installed native version`);
    yield* record(`${host}.version`, { binary, version });
    const tokenFile = join(hostHome, "session.token"); yield* (fileSystem.writeFileString(tokenFile, sessionToken, { mode: 0o600 }));
    const stateDirectory = yield* privateDirectory(join(hostHome, "journal")), configPath = join(hostHome, "plugin.json");
    const config: PluginConfig = { host, brokerUrl, tokenFile, stateDirectory, machineId: MachineId.make(`synthetic-${host}`), selectedRemote: "origin" }; yield* (fileSystem.writeFileString(configPath, encodeJson(PluginConfig, config), { mode: 0o600 }));
    const plan = decodeJson(InstallPlan, yield* run(runtime, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "scripts/install.ts"), "--host", host, "--runtime", runtime, "--host-binary", binary, "--plugin-config", configPath, "--plugin-only", "--dry-run"], root, env));
    assert.equal(plan.dryRun, true); assert.equal(plan.service, undefined); assert.ok(plan.launcher?.startsWith(`${hostHome}/`));
    yield* record(`${host}.installer-dry-run`, plan);
    const syntheticId = SessionId.make(yield* crypto.randomUUIDv4), entryId = ArchiveEntryId.make("synthetic-diagnostic-only");
    const sessionFile = join(hostHome, "synthetic-owned-session.jsonl");
    const timestamp = new Date(yield* Clock.currentTimeMillis).toISOString();
    yield* (fileSystem.writeFileString(sessionFile, [
      { type: "session", version: 3, id: syntheticId, timestamp, cwd: syntheticRepo },
      { type: "custom", id: entryId, parentId: null, timestamp, customType: "coordinator-smoke-synthetic-only", data: { diagnostic: "SYNTHETIC_CONTENT_MUST_NOT_UPLOAD" } },
    ].map(item => encodeJson(JsonValue, decode(JsonValue, item))).join("\n") + "\n", { mode: 0o600 }));
    const sessions = yield* privateDirectory(join(hostHome, "sessions"));
    const launch = Effect.fnUntraced(function*(explicit: boolean) {
      const [child, input, scope] = yield* start(binary, ["--mode", "rpc", "--session", sessionFile, "--session-dir", sessions, ...(explicit ? ["-e", join(root, "src/plugin.ts")] : [])], syntheticRepo, { ...env, COORDINATOR_CONFIG: configPath });
      const native = new NativeRpc(child, input, host); yield* native.initialize(scope); return native;
    });
    let native = yield* launch(true);
    const state = decode(NativeState, (yield* native.request("get_state")).data);
    assert.equal(state.sessionId, syntheticId); assert.equal(resolve(state.sessionFile!), sessionFile); assert.equal(state.isStreaming, false);
    let identity = decode(NativeIdentity, yield* native.command("identity", value => decode(NativeIdentity, value).sessionId === syntheticId));
    assert.equal(identity.machineId, `synthetic-${host}`); assert.equal(identity.repo, repo); assert.equal(identity.ref, "refs/heads/main"); assert.equal(identity.sha, sha); assert.match(identity.incarnation, /^[a-f0-9-]{36}$/);
    assert.equal(identity.workerId, state.sessionId); assert.equal(identity.workerIdentitySource, "native-session");
    yield* record(`${host}.explicit-extension-session-start`, { state, identity });
    const messageId = MessageId.make(yield* crypto.randomUUIDv4), payload = { note: "synthetic durable native inbox" };
    const payloadDigest = yield* platform.fingerprint(payload), journalHash = yield* platform.digest(syntheticId);
    yield* rpc(brokerUrl, coordinatorToken, "peer.send", { messageId, sessionId: identity.sessionId, incarnation: identity.incarnation, payload, ttlMs: 60000 });
    const inbox = yield* until(() => native.command("inbox", value => typeof value === "object" && value !== null).pipe(Effect.map(value => decode(Inbox, value))), value => value[messageId]?.digest === payloadDigest, `${host}: durable inbox receipt`);
    assert.deepEqual(inbox[messageId]!.payload, payload);
    const persisted = decodeJson(JournalState, yield* (fileSystem.readFileString(join(stateDirectory, `${journalHash}.json`))));
    assert.deepEqual(persisted.inbox[messageId], inbox[messageId]);
    yield* until(() => rpc(brokerUrl, sessionToken, "session.poll", { sessionId: identity.sessionId, incarnation: identity.incarnation }), value => value.messages.length === 0, `${host}: broker peer acknowledgement`);
    yield* record(`${host}.native-durable-inbox-and-ack`, { messageId, entry: inbox[messageId] });
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0, "Native commands must not start model work");
    yield* stop(native.child);
    // Execute only native registration, never systemctl/launchctl or user's real HOME.
    yield* run(binary, plan.operations[0]!.args, root, env);
    yield* record(`${host}.native-registration`, { isolatedAgent: agent, package: root, serviceStarted: false });
    native = yield* launch(false); // Registration must discover the real plugin, without -e.
    assert.equal(decode(NativeState, (yield* native.request("get_state")).data).sessionId, syntheticId);
    const previous = identity;
    identity = decode(NativeIdentity, yield* native.command("identity", value => decode(NativeIdentity, value).sessionId === syntheticId));
    assert.notEqual(identity.incarnation, previous.incarnation);
    assert.deepEqual(decode(Inbox, yield* native.command("inbox", value => decode(Inbox, value)[messageId]?.digest === payloadDigest))[messageId], inbox[messageId]);
    yield* record(`${host}.installed-extension-restart`, { identity, persistedInboxMessageId: messageId });
    assert.equal(identity.workerId, syntheticId); assert.equal(identity.workerIdentitySource, "native-session");
    // Native SIGTERM need not emit session_shutdown: preserve the broker's prior-incarnation lease fence.
    // This send is accepted only after the installed extension itself has heartbeated AND polled.
    const enrollmentProbe = { messageId: MessageId.make(yield* crypto.randomUUIDv4), sessionId: identity.sessionId, incarnation: identity.incarnation, payload: { note: "fresh installed native enrollment" }, ttlMs: 60000 };
    const enrollmentDigest = yield* platform.fingerprint(enrollmentProbe.payload);
    yield* until(() => rpc(brokerUrl, coordinatorToken, "peer.send", enrollmentProbe).pipe(Effect.as(true), Effect.catch(error =>
      error instanceof HttpError && error.status === 409 ? Effect.succeed(false) : Effect.fail(error)
    )), value => value, `${host}: actual restarted native heartbeat/poll after prior lease`);
    yield* until(() => native.command("inbox", value => typeof value === "object" && value !== null).pipe(Effect.map(value => decode(Inbox, value))), inbox => inbox[enrollmentProbe.messageId]?.digest === enrollmentDigest, `${host}: fresh installed inbox receipt`);
    yield* record(`${host}.installed-native-fresh-enrollment`, { sessionId: identity.sessionId, incarnation: identity.incarnation, workerId: identity.workerId, priorIncarnationNoLongerAuthoritative: true });
    // Actual session_start performs heartbeat then poll; task.submit must find that fresh native enrollment.
    // No runner is started: a correctly targeted peer must own the offer before any runner dispatch.
    const work = Effect.fnUntraced(function*(workerId = identity.workerId) { return {
      taskId: TaskId.make(yield* crypto.randomUUIDv4), repo, ref: identity.ref, sha, launcher: host,
      prompt: "Synthetic bounded work; must not enter model context without coordinator confirmation.",
      scope: "Synthetic native ownership/readback only", restrictions: "No model turn or external operation",
      expectedEvidence: ["synthetic addressed ready readback"], deadline: (yield* Clock.currentTimeMillis) + 60000,
      workerId, githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["synthetic-native-readback"], bypassPeer: false, approved: true,
    } satisfies Work; });
    const poll = () => rpc(brokerUrl, sessionToken, "session.poll", { sessionId: identity.sessionId, incarnation: identity.incarnation });
    const offered = Effect.fnUntraced(function*(request: Work) {
      const from = native.frames.length;
      const task = yield* rpc(brokerUrl, coordinatorToken, "task.submit", request);
      assert.equal(task.offerDecision, "pending"); assert.equal(task.state, "QUEUED");
      assert.equal(task.peerSession, identity.sessionId); assert.equal(task.peerIncarnation, identity.incarnation); assert.equal(task.runnerId, undefined);
      yield* native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "notify" && typeof frame.message === "string" && frame.message.startsWith(`Coordinator offer ${task.taskId}:`));
      return task;
    });
    const wrong = yield* rpc(brokerUrl, coordinatorToken, "task.submit", (yield* work(WorkerId.make(`wrong-${yield* crypto.randomUUIDv4}`))));
    assert.equal(wrong.offerDecision, "expired"); assert.equal(wrong.peerSession, undefined);
    yield* record(`${host}.wrong-target-not-offered`, { taskId: wrong.taskId, targetedWorker: wrong.request.workerId, enrolledWorker: identity.workerId, offerDecision: wrong.offerDecision });
    const declinedOffer = yield* offered(yield* work());
    assert.ok(!(yield* poll()).offers.some(task => task.taskId === wrong.taskId));
    const declined = decode(Commands["peer.reply"].output, yield* native.command(`decline ${encodeJson(JsonValue, { taskId: declinedOffer.taskId })}`, value => decode(Commands["peer.reply"].output, value).offerDecision === "declined"));
    assert.equal(declined.runnerId, undefined); assert.equal(declined.state, "QUEUED");
    assert.ok(!(yield* poll()).offers.some(task => task.taskId === declined.taskId));
    yield* record(`${host}.native-fresh-offer-decline`, { taskId: declined.taskId, workerId: identity.workerId, peerSession: declined.peerSession, offerDecision: declined.offerDecision, runnerDispatched: false });
    const acceptedOffer = yield* offered(yield* work());
    const accepted = decode(Commands["peer.reply"].output, yield* native.command(`accept ${encodeJson(JsonValue, { taskId: acceptedOffer.taskId })}`, value => decode(Commands["peer.reply"].output, value).offerDecision === "accepted"));
    assert.equal(accepted.state, "ACCEPTED"); assert.equal(accepted.runnerId, undefined);
    assert.deepEqual((yield* poll()).releases, []);
    const address: Address = {
      messageId: MessageId.make(yield* crypto.randomUUIDv4), revision: 1, workerId: identity.workerId, sessionId: identity.sessionId,
      incarnation: identity.incarnation, repo, ref: identity.ref, sha, scope: accepted.request.scope,
      restrictions: accepted.request.restrictions, expectedEvidence: accepted.request.expectedEvidence, deadline: accepted.request.deadline,
    };
    yield* rpc(brokerUrl, coordinatorToken, "task.address", { taskId: accepted.taskId, address });
    for (const change of [{ workerId: WorkerId.make(`wrong-${(yield* crypto.randomUUIDv4)}`) }, { repo: Repo.make("github.com/synthetic/wrong") }, { ref: "refs/heads/wrong" }, { sha: Sha.make("0".repeat(40)) }, { deadline: (yield* Clock.currentTimeMillis) - 1 }]) {
      yield* native.rejectedCommand(`readback ${encodeJson(JsonValue, { taskId: accepted.taskId, address: { ...address, ...change } })}`);
    }
    const ready = decode(Commands["task.readback"].output, yield* native.command(`readback ${encodeJson(JsonValue, { taskId: accepted.taskId, address })}`, value => decode(Commands["task.readback"].output, value).disposition === "ready"));
    assert.deepEqual(ready.readback, address); assert.equal(ready.state, "ACCEPTED");
    assert.equal(ready.confirmedRevision, undefined); assert.equal(ready.runnerId, undefined);
    yield* Effect.sleep(1200); // Let the actual extension tick observe readiness without confirmation.
    assert.deepEqual((yield* poll()).releases, []);
    const nativeAfterReady = decode(NativeState, (yield* native.request("get_state")).data);
    assert.equal(nativeAfterReady.isStreaming, false); assert.equal(nativeAfterReady.messageCount, state.messageCount);
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0);
    yield* record(`${host}.native-accepted-addressed-ready`, { taskId: ready.taskId, identity, address, readback: ready.readback, disposition: ready.disposition, coordinatorConfirmed: false, substantivePromptAdmitted: false, runnerDispatched: false, rejectedMismatches: ["worker", "repo", "ref", "sha", "deadline"] });
    const archive = Effect.fnUntraced(function*() {
      const rows: ArchiveRow[] = []; let cursor = 0;
      for (;;) { const page = yield* rpc(brokerUrl, coordinatorToken, "archive.list", { sessionId: syntheticId, cursor }); rows.push(...page); if (page.length < 2) return rows; cursor = page.at(-1)!.sequence; }
    });
    assert.deepEqual(yield* archive(), []);
    // Grant solely OUR synthetic participant's metadata consent via actual host confirmation.
    const from = native.frames.length;
    const participant = PrincipalId.make("synthetic-native");
    const consent = yield* native.request("prompt", { message: `/coordinator consent ${encodeJson(JsonValue, { participants: [participant] })}` }).pipe(Effect.forkScoped); // Scoped fiber cannot leak a pending confirmation.
    const confirmation = yield* native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "confirm" && frame.title === "Your session archive consent");
    assert.ok(typeof confirmation.message === "string" && confirmation.message.includes(participant)); assert.ok(typeof confirmation.message === "string" && confirmation.message.includes("content is omitted"));
    assert.ok(confirmation.id);
    yield* native.send({ type: "extension_ui_response", id: confirmation.id, confirmed: true }); yield* Fiber.join(consent);
    yield* native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "notify" && frame.message === '{"enabled":true}');
    const archived = yield* until(archive, rows => rows.some(row => row.entryId === entryId), `${host}: synthetic metadata upload`);
    const diagnostic = archived.find(row => row.entryId === entryId)!;
    assert.deepEqual(diagnostic.payload, { id: entryId, parentId: null, type: "custom", contentOmitted: true });
    assert.equal(diagnostic.digest, (yield* platform.fingerprint(diagnostic.payload))); assert.ok(diagnostic.gaps.includes("content-omitted"));
    assert.ok(!encodeJson(JsonValue, archived).includes("SYNTHETIC_CONTENT_MUST_NOT_UPLOAD"));
    yield* record(`${host}.native-confirmed-synthetic-consent`, { confirmation: { method: confirmation.method, title: confirmation.title, message: confirmation.message }, diagnostic });
    yield* native.command("optout", value => value !== null && typeof value === "object" && "enabled" in value && value.enabled === false);
    assert.deepEqual(yield* archive(), []);
    yield* rejected(rpc(brokerUrl, sessionToken, "archive.append", { sessionId: syntheticId, entryId, digest: diagnostic.digest, payload: diagnostic.payload, gaps: diagnostic.gaps }), 403);
    const erased = decodeJson(JournalState, yield* (fileSystem.readFileString(join(stateDirectory, `${journalHash}.json`))));
    assert.equal(erased.consent, false); assert.deepEqual(erased.spool, []); assert.deepEqual(erased.seen, {});
    yield* record(`${host}.native-optout`, { serverArchiveRows: 0, appendAfterRevoke: 403, localSpoolErased: true });
    const changed = yield* native.request("new_session");
    assert.ok(changed.data !== null && typeof changed.data === "object" && "cancelled" in changed.data && changed.data.cancelled === false);
    const replacement = decode(NativeState, (yield* native.request("get_state")).data); assert.notEqual(replacement.sessionId, syntheticId);
    const next = decode(NativeIdentity, yield* native.command("identity", value => decode(NativeIdentity, value).sessionId === replacement.sessionId));
    assert.notEqual(next.incarnation, identity.incarnation);
    yield* rejected(rpc(brokerUrl, coordinatorToken, "peer.send", { messageId: MessageId.make(yield* crypto.randomUUIDv4), sessionId: syntheticId, incarnation: identity.incarnation, payload, ttlMs: 60000 }), 409);
    assert.equal(replacement.isStreaming, false);
    yield* record(`${host}.native-session-replacement-lifecycle`, { oldSessionId: syntheticId, replacement: next, oldAddressRejected: 409 });
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0, "Installed native commands must not start model work");
    yield* stop(native.child);
  }
  yield* record("completed", { hosts: ["omp18.6.1", "pi1.0.4"], modelCallsRequested: 0, macRuntimeProved: false });
  }).pipe(Effect.catchCause(cause => Effect.gen(function*() {
    success = false;
    const failure = Cause.squash(cause);
    yield* record("failed", { reason: failure instanceof Error ? failure.message.replaceAll(coordinatorToken, "[redacted]").replaceAll(sessionToken, "[redacted]") : "Unknown native smoke failure" });
  })), Effect.ensuring(Effect.gen(function*() {
    for (const child of [...children.keys()]) yield* stop(child);
    console.log(encodeJson(JsonValue, { success, transcript: transcriptPath, privateSyntheticDirectory: directory, macRuntimeProved: false }));
  })));
  if (!success) return yield* Effect.fail(new CommandError({ message: "Installed-host smoke failed; see private synthetic transcript" }));
});
NodeRuntime.runMain(Commands["program.smoke:hosts"].execute({ runHostSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer)),
), { disableErrorReporting: true });
