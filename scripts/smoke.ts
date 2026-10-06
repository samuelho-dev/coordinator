// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Queue, Stdio, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { BrokerConfig, RunnerConfig, ModelProfile, NativeFrame, Json, JsonValue, HttpError, Commands, CommandError, type CommandInput, type FramePending, type Work, type Address, type SessionKey } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { inspectGithubRepository } from "../src/evidence.ts";
import { Mcp, serverLayer, decode, decodeJson, decodeEffect, encodeJson } from "../src/http.ts";
import { CHILD_UID_BASE, LfFrames, requireIsolation, isolationArgs, childEnvironment, ownChildDirectory } from "../src/runner.ts";

// Effect has no executable-path, runtime-version or own-PID service; process is used only for these boundary facts.
const runSmoke = (flags: CommandInput<"program.smoke">) => Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs: fileSystem, path: { join, resolve }, crypto, spawner, digest, fingerprint, exec } = platform;
  const { call: rpc, serve } = yield* Mcp;
  const client = yield* HttpClient.HttpClient;
  const stdio = yield* Stdio.Stdio;
  const print = (line: string) => Stream.make(`${line}\n`).pipe(Stream.run(stdio.stdout()));
  const setting = (key: string, fallback: string) => Config.String(key).pipe(Config.withDefault(fallback));
  const uuid = crypto.randomUUIDv4;
  const secret = Effect.fnUntraced(function*() { return Hex.encode(yield* crypto.randomBytes(32)); });
  const rejects = Effect.fnUntraced(function*<A, E, R>(operation: Effect.Effect<A, E, R>, expected: number) {
    const result = yield* Effect.exit(operation);
    assert.ok(Exit.isFailure(result), "Expected operation to reject");
    if (Exit.isFailure(result)) {
      const error = Cause.squash(result.cause);
      assert.ok(error instanceof HttpError);
      assert.equal(error.status, expected);
    }
  });
  const until = Effect.fnUntraced(function*<A, E, R>(read: () => Effect.Effect<A, E, R>, check: (value: A) => boolean, milliseconds = 90000) {
    const deadline = (yield* Clock.currentTimeMillis) + milliseconds;
    let error: unknown;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const result = yield* Effect.exit(Effect.suspend(read));
      if (Exit.isSuccess(result)) { if (check(result.value)) return result.value; }
      else error = Cause.squash(result.cause);
      yield* Effect.sleep(200);
    }
    return yield* Effect.fail(new CommandError({ message: `Scenario deadline expired${error instanceof Error ? `: ${error.message.slice(0, 200)}` : ""}` }));
  });
  const exit = Effect.fnUntraced(function*(child: ChildProcessSpawner.ChildProcessHandle, signal: "SIGTERM" | "SIGKILL") {
    if (yield* child.isRunning) yield* child.kill({ killSignal: signal });
    yield* until(() => child.isRunning, (running) => !running, 10000);
  });
  const sqlRows = Effect.fnUntraced(function*(filename: string, statement: string, params: readonly (string | number)[] = []) {
    return yield* Effect.gen(function*() {
      const sql = yield* SqliteClient.SqliteClient;
      return yield* sql.unsafe<Json>(statement, params);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, readonly: true })), Effect.scoped);
  });
  if (flags["--runner-isolation-only"]) {
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-isolation-" });
    yield* fileSystem.chmod(directory, 0o711);
    const credentials = join(directory, "credentials"), children = join(directory, "children"), model = join(directory, "model");
    const ownRoot = join(children, "own"), sibling = join(children, "sibling");
    const own = join(ownRoot, "repo"), sessions = join(ownRoot, "sessions"), home = join(ownRoot, "home");
    for (const path of [credentials, children, model, ownRoot, own, sessions, home, sibling]) yield* fileSystem.makeDirectory(path, { mode: 0o700 });
    const token = join(credentials, "runner.token"), controlPath = join(credentials, "control.token"), ledger = join(directory, "runner.sqlite");
    for (const path of [token, controlPath, ledger, join(sibling, "private")]) yield* fileSystem.writeFileString(path, "ISOLATION_SECRET", { mode: 0o600 });
    yield* fileSystem.makeDirectory(join(home, ".omp/agent"), { recursive: true, mode: 0o700 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/config.yml"), encodeJson(ModelProfile, { modelRoles: { default: yield* setting("COORDINATOR_MODEL", "openai-codex/gpt-6.1-sol:xhigh") } }), { mode: 0o600 });
    const launcher = yield* setting("COORDINATOR_TEST_OMP", "/usr/local/bin/omp");
    const config: RunnerConfig = { id: "isolation-probe", brokerUrl: "http://127.0.0.1", tokenFile: token, controlTokenHash: yield* digest("control"),
      stateDirectory: join(directory, "state"), host: "127.0.0.1", port: 0, slots: 1, repositories: {}, launchers: { omp: launcher },
      modelDirectory: model, childDirectory: children, childUidBase: CHILD_UID_BASE };
    yield* requireIsolation(config);
    yield* ownChildDirectory(ownRoot, CHILD_UID_BASE + 1); yield* ownChildDirectory(sibling, CHILD_UID_BASE + 2);
    const input = yield* Queue.make<Uint8Array, Cause.Done>();
    yield* Effect.addFinalizer(() => Queue.end(input));
    // env chdir uses effective supervisor capabilities; Effect's cwd preflight uses real-UID access.
    const child = yield* spawner.spawn(ChildProcess.make("/usr/bin/env", [`--chdir=${own}`, "/usr/bin/setpriv", ...isolationArgs(CHILD_UID_BASE + 1), launcher, "--mode", "rpc", "--session-dir", sessions, "--no-extensions"],
      // Actual omp requires a discoverable provider even for idle RPC. This deliberately unusable fixture key enables model discovery, not inference.
      // The isolation proof sends only get_state, get_messages and bash; it never submits a model prompt or needs real provider credentials.
      { env: { ...(yield* childEnvironment(home)), OPENAI_API_KEY: "coordinator-isolation-no-model-call" }, extendEnv: false, stdin: Stream.fromQueue(input), forceKillAfter: "1 second" }));
    const nativeDeadline = (yield* Clock.currentTimeMillis) + 60000;
    const frames = new LfFrames(), pending = new Map<string, FramePending>();
    const failPending = (error: unknown) => Effect.forEach(pending.values(), (request) => Deferred.fail(request, error instanceof CommandError ? error : new CommandError({ message: error instanceof Error ? error.message : String(error) })), { discard: true });
    yield* Effect.forkScoped(Stream.runDrain(child.stderr));
    yield* Effect.forkScoped(child.stdout.pipe(Stream.runForEach((chunk) => Effect.gen(function*() {
      for (const frame of frames.push(chunk)) {
        if (typeof frame === "object" && frame !== null && "id" in frame && typeof frame.id === "string") {
          const request = pending.get(frame.id);
          if (request) { yield* Deferred.succeed(request, decode(NativeFrame, frame)); pending.delete(frame.id); }
        }
      }
    })), Effect.catchCause((cause) => failPending(Cause.squash(cause)))));
    yield* Effect.forkScoped(child.exitCode.pipe(Effect.matchEffect({
      onFailure: failPending, onSuccess: () => failPending(new CommandError({ message: "Native isolation probe exited" })),
    })));
    const request = Effect.fnUntraced(function*(type: string, message?: string) {
      const id = yield* uuid, result = yield* Deferred.make<NativeFrame, CommandError>();
      pending.set(id, result);
      return yield* Effect.gen(function*() {
        yield* Queue.offer(input, new TextEncoder().encode(`${encodeJson(JsonValue, { id, type, ...(message ? type === "bash" ? { command: message } : { message } : {}) })}\n`));
        return yield* Deferred.await(result).pipe(Effect.timeout(Math.max(1, nativeDeadline - (yield* Clock.currentTimeMillis))));
      }).pipe(Effect.ensuring(Effect.sync(() => { pending.delete(id); })));
    });
    assert.equal((yield* request("get_state")).success, true);
    assert.equal((yield* request("get_messages")).success, true);
    const quote = (path: string) => `'${path.replace(/'/g, "'\\''")}'`;
    // Effect cannot inspect another process's environ; /proc access itself is the isolation proof.
    const denied = [token, controlPath, ledger, join(sibling, "private"), `/proc/${process.pid}/environ`];
    const command = `set -eu; ${denied.map((path) => `if cat ${quote(path)} >/dev/null 2>&1; then exit 41; fi`).join("; ")}; ` +
      `test -z "\${GITHUB_PAT-}\${GITHUB_TOKEN-}\${GH_TOKEN-}\${OP_SERVICE_ACCOUNT_TOKEN-}\${COORDINATOR_CONFIG-}"; ` +
      `test "$(id -u)" = "${CHILD_UID_BASE + 1}"; test "$PWD" = ${quote(own)}; ` +
      `touch approved-write; test -r "$HOME/.omp/agent/config.yml"; touch ${quote(join(sessions, "approved-session-write"))}; ` +
      `checked=0; while IFS=: read -r key value; do set -- $value; case "$key" in Groups) test "$#" = 0; checked=$((checked+1));; CapInh|CapPrm|CapEff|CapBnd|CapAmb) test "$1" = 0000000000000000; checked=$((checked+1));; NoNewPrivs) test "$1" = 1; checked=$((checked+1));; esac; done </proc/self/status; test "$checked" = 7; printf ISOLATION_OK`;
    const response = yield* request("bash", command);
    assert.equal(response.success, true);
    assert.ok(typeof response.data === "object" && response.data !== null && "exitCode" in response.data && response.data.exitCode === 0);
    yield* child.kill({ killSignal: "SIGKILL" });
    yield* until(() => child.isRunning, (running) => !running, 3000);
    yield* print(encodeJson(JsonValue, { scenario: "real-native-os-isolation", childUid: CHILD_UID_BASE + 1, forbiddenPathsDenied: denied.length,
      ownWorkspaceAndSessionWritable: true, modelProfileReadable: true, parentEnvironDenied: true, siblingUidDistinct: true,
      capabilitiesZero: true, supplementalGroupsEmpty: true, noNewPrivileges: true, nativeStateAndMessagesUsable: true, forbiddenEnvironmentAbsent: true, supervisorTerminationProved: true }));
    return;
  }
const root = resolve(import.meta.dirname, "..");
const state = (yield* setting("COORDINATOR_SMOKE_DIRECTORY", "")) || (yield* fileSystem.makeTempDirectory({ prefix: "coordinator-sockets-" }));
yield* (fileSystem.makeDirectory(state, { recursive: true, mode: 0o711 })); yield* (fileSystem.chmod(state, 0o711));
const repo = "github.com/samuelho-dev/coordinator";
const sha = (yield* exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }))).trim();
const identity = yield* inspectGithubRepository(repo, yield* Config.String("GITHUB_READ_TOKEN"));
const coordinator = (yield* secret()), recipient = (yield* secret()), runnerToken = (yield* secret()), runnerControl = (yield* secret()), reviewer = (yield* secret()), writer = (yield* secret()), outsider = (yield* secret());
const tokenDirectory = join(state, "tokens"); yield* (fileSystem.makeDirectory(tokenDirectory, { mode: 0o700 }));
yield* (fileSystem.writeFileString(join(tokenDirectory, "homelab-runner"), runnerControl, { mode: 0o600 }));
const runnerEnrollmentPath = join(tokenDirectory, "runner-enrollment.token"); yield* (fileSystem.writeFileString(runnerEnrollmentPath, runnerToken, { mode: 0o600 }));
const alternateToken = (yield* secret()), alternateControl = (yield* secret());
const alternateEnrollmentPath = join(tokenDirectory, "alternate-enrollment.token"); yield* (fileSystem.writeFileString(alternateEnrollmentPath, alternateToken, { mode: 0o600 }));
yield* (fileSystem.writeFileString(join(tokenDirectory, "alternate-runner"), alternateControl, { mode: 0o600 }));
const port = () => Effect.scoped(Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer;
  const address = server.address;
  assert.equal(address._tag, "InetAddressV4");
  if (address._tag !== "InetAddressV4") return yield* Effect.fail(new CommandError({ message: "No TCP port" }));
  return address.port;
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0))));
const brokerPort = yield* port(), runnerPort = yield* port(), proxyPort = yield* port(), alternatePort = yield* port();
const brokerUrl = `http://127.0.0.1:${brokerPort}`, runnerUrl = `http://127.0.0.1:${runnerPort}`, proxyUrl = `http://127.0.0.1:${proxyPort}`, alternateUrl = `http://127.0.0.1:${alternatePort}`;
const brokerConfig: BrokerConfig = { database: join(state, "broker", "ledger.sqlite"), host: "127.0.0.1", port: brokerPort,
  backupDirectory: join(state, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000, githubTokenEnv: "GITHUB_READ_TOKEN",
  repositoryIds: { [repo]: identity.id },
  principals: [
    { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: (yield* digest(coordinator)) },
    { id: "recipient", owner: "owner", roles: ["session"], repos: [repo], tokenHash: (yield* digest(recipient)) },
    { id: "homelab-runner", owner: "owner", roles: ["runner"], repos: [repo], tokenHash: (yield* digest(runnerToken)), controlTokenHash: (yield* digest(runnerControl)), runnerUrl: proxyUrl },
    { id: "alternate-runner", owner: "owner", roles: ["runner"], repos: [repo], tokenHash: (yield* digest(alternateToken)), controlTokenHash: (yield* digest(alternateControl)), runnerUrl: alternateUrl },
    { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: (yield* digest(reviewer)) },
    { id: "writer", owner: "owner", roles: ["archive-writer"], repos: [repo], tokenHash: (yield* digest(writer)) },
    { id: "outsider", owner: "outsider", roles: ["coordinator", "session"], repos: [repo], tokenHash: (yield* digest(outsider)) },
  ] };
const launcher = yield* setting("COORDINATOR_TEST_OMP", "/home/admin/.bun/bin/omp");
const runnerConfig: RunnerConfig = { id: "homelab-runner", brokerUrl, tokenFile: runnerEnrollmentPath, host: "127.0.0.1", port: runnerPort,
  stateDirectory: join(state, "runner"), slots: 1, repositories: { [repo]: root }, launchers: { omp: launcher }, controlTokenHash: (yield* digest(runnerControl)),
  modelDirectory: yield* setting("COORDINATOR_MODEL_DIRECTORY", "/work/home"), childDirectory: join(state, "children"), childUidBase: 1000000 };
const brokerPath = join(state, "broker.json"), runnerPath = join(state, "runner.json");
yield* (fileSystem.writeFileString(brokerPath, encodeJson(BrokerConfig, brokerConfig), { mode: 0o600 })); yield* (fileSystem.writeFileString(runnerPath, encodeJson(RunnerConfig, runnerConfig), { mode: 0o600 }));
const alternateConfig: RunnerConfig = { ...runnerConfig, id: "alternate-runner", port: alternatePort, tokenFile: alternateEnrollmentPath, stateDirectory: join(state, "alternate-runner"), childDirectory: join(state, "alternate-children"), childUidBase: 2000000, controlTokenHash: (yield* digest(alternateControl)) };
const alternatePath = join(state, "alternate.json"); yield* (fileSystem.writeFileString(alternatePath, encodeJson(RunnerConfig, alternateConfig), { mode: 0o600 }));
const transcript: JsonValue[] = [];
const evidence = Effect.fnUntraced(function*(scenario: string, observed: unknown) { const entry = decode(JsonValue, { scenario, observed, time: new Date(yield* Clock.currentTimeMillis).toISOString() }); transcript.push(entry); yield* print(encodeJson(JsonValue, entry)); });
const processFor = Effect.fnUntraced(function*(mode: "broker" | "runner", config: string) {
  const env = { ...(yield* childEnvironment(yield* setting("HOME", "/tmp"))), COORDINATOR_CONFIG: config, COORDINATOR_RUNNER_TOKEN_DIRECTORY: tokenDirectory };
  const readToken = yield* setting("GITHUB_READ_TOKEN", "");
  const hosts = yield* setting("COORDINATOR_CONNECT_HOSTS_JSON", "{}");
  const runtimeEnv = { ...env, COORDINATOR_CONNECT_HOSTS_JSON: hosts, ...(mode === "broker" && readToken ? { GITHUB_READ_TOKEN: readToken } : {}) };
  const child = yield* spawner.spawn(ChildProcess.make(process.execPath, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "src/main.ts"), mode], {
    cwd: root, env: runtimeEnv, extendEnv: false, stdin: "ignore", forceKillAfter: "1 second",
  }));
  yield* Effect.forkScoped(Stream.runDrain(child.stdout)); yield* Effect.forkScoped(Stream.runDrain(child.stderr));
  return child;
});
const health = (url: string, token: string) => rpc(url, token, "health", {}).pipe(Effect.map((response) => response.status === "ok"));
const ready = (url: string) => until(() => health(url, url === runnerUrl ? runnerToken : coordinator), Boolean, 30000);
let obstructJournal = false, acceptanceCommitted = false;
const proxyContext = yield* Layer.build(serverLayer("127.0.0.1", proxyPort));
const proxy = Context.get(proxyContext, HttpServer.HttpServer);
yield* serve(proxy, { target: "runner",
  authorize: (token) => token === runnerControl ? Effect.succeed(token) : Effect.fail(new HttpError({ status: 401, message: "Unauthorized" })),
  handle: (_principal, { method, params }) => Effect.gen(function*() {
    if (obstructJournal && method === "journal") return yield* Effect.fail(new HttpError({ status: 503, message: "Intentional journal outage" }));
    const response = yield* rpc(runnerUrl, runnerControl, method, params);
    if (obstructJournal && method === "acceptTask" && typeof response === "object" && response !== null && "accepted" in response && response.accepted === true) { acceptanceCommitted = true; yield* Effect.sleep(20000); }
    return response;
  }),
});
let broker = yield* processFor("broker", brokerPath); let runner: ChildProcessSpawner.ChildProcessHandle | undefined, alternate: ChildProcessSpawner.ChildProcessHandle | undefined;
let workerPid: number | undefined, uncertainPid: number | undefined;
yield* Effect.addFinalizer(() => Effect.gen(function*() {
  for (const child of [alternate, runner, broker]) if (child) yield* exit(child, "SIGTERM").pipe(Effect.ignore);
  // Effect cannot signal arbitrary PIDs recovered from a different supervisor's journal.
  for (const pid of [workerPid, uncertainPid]) if (pid) yield* Effect.try(() => process.kill(pid, "SIGTERM")).pipe(Effect.ignore);
}));
  yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, (yield* secret()), "task.get", { taskId: "missing" }), 401);
  const session = { sessionId: "native-owner-session", incarnation: (yield* uuid), machineId: "homelab", host: "omp", repo, workerId: "existing-omp-smoke-worker" };
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session); yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session));
  const work: Work = { taskId: `smoke-${(yield* uuid)}`, repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "Respond exactly COORDINATOR_SMOKE_OK. Do not call tools, change files, merge or deploy.", scope: "Socket lifecycle smoke only", restrictions: "No file modifications, merging or deployment", expectedEvidence: ["one owned real omp child", "exact-SHA result"], deadline: (yield* Clock.currentTimeMillis) + 600000,
    workerId: "existing-omp-smoke-worker", githubRepoId: identity.id, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
  yield* rejects(rpc(brokerUrl, writer, "task.submit", work), 403);
  yield* evidence("authentication-and-operation-authorization", { invalidCredential: 401, archiveWriterCannotDispatch: 403 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.submit", { ...work, taskId: "wrong-enrolled-repository", githubRepoId: identity.id + 1 }), 409);
  yield* evidence("inspected-enrollment-without-CI-credential", { repositoryId: identity.id, mismatchedIdRejected: true, broadGithubCredentialNotInherited: true });
  const peerTask = yield* rpc(brokerUrl, coordinator, "task.submit", work);
  assert.equal(peerTask.offerDecision, "pending"); assert.equal(peerTask.runnerId, undefined);
  yield* evidence("peer-first-owner-offer", { taskId: work.taskId, state: peerTask.state, decision: peerTask.offerDecision, runnerAssigned: false });
  const message = { messageId: (yield* uuid), ...sessionKey(session), payload: { note: "addressed owner notification" }, ttlMs: 60000 };
  yield* rpc(brokerUrl, coordinator, "peer.send", message);
  const before = yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.equal(before.messages.length, 1);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: (yield* uuid) }), 409);
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session);
  const replay = yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.deepEqual(replay.messages, before.messages);
  yield* rpc(brokerUrl, recipient, "peer.ack", { ...sessionKey(session), messageId: message.messageId });
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  yield* evidence("broker-crash-same-incarnation-replay", { replayed: 1, newSendBeforeHeartbeatRejected: true, handledAckDeletes: true });
  yield* rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: "expires", ttlMs: 100 }); yield* Effect.sleep(150);
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  const address: Address = { messageId: work.taskId, revision: 1, workerId: work.workerId, ...sessionKey(session), repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  yield* rpc(brokerUrl, recipient, "peer.reply", { taskId: work.taskId, ...sessionKey(session), decision: "accepted" });
  yield* rpc(brokerUrl, coordinator, "task.address", { taskId: work.taskId, address });
  yield* rejects(rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: { ...address, ref: "wrong-ref" }, disposition: "ready" }), 409);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "hold", reason: "yield* correction", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "ready" }); yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 });
  const second = { ...session, sessionId: "handoff-session", incarnation: (yield* uuid) }; yield* rpc(brokerUrl, recipient, "session.heartbeat", second); yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second));
  yield* rejects(rpc(brokerUrl, coordinator, "task.handoff", { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, "task.report", { taskId: work.taskId, ...sessionKey(session), state: "SUCCEEDED", result: { syntheticPeerGate: true } });
  yield* rpc(brokerUrl, coordinator, "task.handoff", { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), 409);
  const handoffAddress = { ...address, ...sessionKey(second), revision: 2 };
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: handoffAddress, disposition: "ready" }); yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 });
  yield* evidence("expiry-correction-hold-and-handoff-gate", { expiredPeerNotDelivered: true, wrongRefRejected: true, heldConfirmationRejected: true, handoffRequiresNewReadback: true });
  const instructionAddress = { ...handoffAddress, messageId: "later-owned-message", revision: 3, scope: "A separately approved later instruction" };
  yield* rpc(brokerUrl, coordinator, "task.instruction", { taskId: work.taskId, address: instructionAddress, prompt: "Later approved instruction; do not repeat the launch prompt", approved: true });
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second))).releases.length, 0);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: instructionAddress, disposition: "ready" });
  yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 3 });
  const later = (yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second))).releases[0]!;
  assert.equal(later.instruction?.prompt, "Later approved instruction; do not repeat the launch prompt");
  yield* rpc(brokerUrl, recipient, "task.report", { taskId: work.taskId, ...sessionKey(second), state: "SUCCEEDED", result: { syntheticPeerGate: true } });
  yield* evidence("later-native-instruction-gate", { sameTaskId: later.taskId, previousRevisionCannotRelease: true, newReadbackRequired: true, approvedPromptDistinct: true });
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner"], enabled: true });
  const payload = { id: "entry-one", parentId: null, type: "message", contentOmitted: true };
  const archive = { sessionId: session.sessionId, entryId: "entry-one", digest: (yield* fingerprint(payload)), payload, gaps: ["content-omitted", "unlinked"] };
  yield* rpc(brokerUrl, writer, "archive.append", archive); yield* rpc(brokerUrl, writer, "archive.append", archive);
  const unsafePayload = { ...payload, text: "unsafe-raw-content-must-not-persist" };
  // Send malformed arguments directly to MCP: the typed client deliberately rejects them before transport.
  const unsafeResponse = yield* client.execute(HttpClientRequest.post(new URL("/mcp", brokerUrl)).pipe(
    HttpClientRequest.setHeaders({ authorization: `Bearer ${writer}`, accept: "application/json, text/event-stream",
      "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": Commands["archive.append"].name }),
    HttpClientRequest.bodyText(encodeJson(JsonValue, { jsonrpc: "2.0", id: "unsafe-archive", method: "tools/call",
      params: { name: Commands["archive.append"].name, arguments: { ...archive, entryId: "unsafe", payload: unsafePayload, digest: yield* fingerprint(unsafePayload) },
        _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} } } }), "application/json"),
  ));
  assert.equal(unsafeResponse.status, 200);
  const unsafeText = yield* unsafeResponse.text;
  const unsafeMessage = decodeJson(Json, unsafeResponse.headers["content-type"]?.includes("text/event-stream")
    ? unsafeText.split(/\r?\n/).filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n")
    : unsafeText);
  assert.ok(typeof unsafeMessage === "object" && unsafeMessage !== null && "result" in unsafeMessage);
  const unsafeResult = decode(Json, unsafeMessage.result);
  assert.ok(typeof unsafeResult === "object" && unsafeResult !== null && "isError" in unsafeResult);
  assert.equal(unsafeResult.isError, true);
  const archived = yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId });
  assert.equal(archived.length, 1);
  assert.equal(encodeJson(JsonValue, archived).includes(unsafePayload.text), false);
  assert.equal((yield* sqlRows(brokerConfig.database, "SELECT count(*) AS count FROM archive WHERE payload LIKE ?", [`%${unsafePayload.text}%`]))[0]?.count, 0);
  yield* rejects(rpc(brokerUrl, writer, "archive.delete", { sessionId: session.sessionId }), 403);
  assert.deepEqual(yield* rpc(brokerUrl, outsider, "archive.list", { sessionId: session.sessionId }), []);
  const snapshotNames = new Set(yield* (fileSystem.readDirectory(brokerConfig.backupDirectory)));
  const snapshotName = yield* until(() => fileSystem.readDirectory(brokerConfig.backupDirectory).pipe(Effect.map((names) => names.find((name) => name.endsWith(".sqlite") && !snapshotNames.has(name)))), (name) => Boolean(name), 75000);
  const snapshot = join(brokerConfig.backupDirectory, snapshotName!);
  for (const table of ["archive", "consents", "participant_consents"]) assert.equal((yield* sqlRows(snapshot, `SELECT count(*) AS count FROM ${table}`))[0]?.count, 0);
  assert.ok((yield* sqlRows(snapshot, "SELECT id FROM tasks WHERE id=?", [work.taskId]))[0]); 
  assert.equal((yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId })).length, 1);
  yield* rejects(rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: false }), 403);
  yield* rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: true });
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner", "outsider"], enabled: true });
  yield* rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: false });
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session);
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).archiveEnabled, false);
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner"], enabled: true });
  yield* rpc(brokerUrl, writer, "archive.append", archive);
  yield* rpc(brokerUrl, coordinator, "archive.delete", { sessionId: session.sessionId });
  yield* rejects(rpc(brokerUrl, writer, "archive.append", archive), 403);
  yield* exit(broker, "SIGTERM"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  yield* evidence("archive-privacy-idempotency-revocation-and-restart", { uniqueEntries: 1, appendOnlyCannotDelete: true, otherOwnerSeesNoRows: true, optoutRejectsAppendAfterRestart: true, snapshotsExcludeArchiveAndConsent: true });
  runner = yield* processFor("runner", runnerPath); yield* ready(runnerUrl); yield* Effect.sleep(300);
  const worker = { ...work, taskId: `worker-${(yield* uuid)}`, bypassPeer: true };
  const submitted = yield* rpc(brokerUrl, coordinator, "task.submit", worker); const duplicate = yield* rpc(brokerUrl, coordinator, "task.submit", worker); assert.equal(submitted.taskId, duplicate.taskId);
  yield* rejects(rpc(brokerUrl, coordinator, "task.submit", { ...worker, prompt: "different" }), 409);
  const accepted = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => Boolean(task.readback), 120000);
  assert.equal(accepted.address?.sha, sha); assert.equal(accepted.runnerId, "homelab-runner");
  const runnerDb = join(runnerConfig.stateDirectory, "runner.sqlite");
  const row = (yield* sqlRows(runnerDb, "SELECT pid FROM tasks WHERE taskId=?", [worker.taskId]))[0]; workerPid = Number(row?.pid); 
  assert.ok(workerPid > 0);
  yield* rejects(rpc(runnerUrl, runnerToken, "acceptTask", worker), 403);
  assert.equal((yield* rpc(runnerUrl, runnerControl, "acceptTask", worker)).taskId, worker.taskId);
  const corrected = { ...accepted.address!, revision: 2 };
  const correctedTask = yield* rpc(brokerUrl, coordinator, "task.address", { taskId: worker.taskId, address: corrected });
  assert.deepEqual(correctedTask.readback, corrected);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 1 }), 409);
  const full = yield* rpc(runnerUrl, runnerControl, "acceptTask", { ...worker, taskId: "full-capacity" }); assert.equal(full.accepted, false); assert.equal(full.reason, "full");
  yield* rejects(rpc(brokerUrl, outsider, "task.get", { taskId: worker.taskId }), 403);
  yield* rejects(rpc(brokerUrl, writer, "task.attach", { taskId: worker.taskId }), 403);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  const attached = yield* rpc(brokerUrl, coordinator, "task.attach", { taskId: worker.taskId }); assert.equal(attached.taskId, worker.taskId); // Effect has no arbitrary-PID liveness API; this PID is owned by the runner process.
  yield* Effect.try(() => process.kill(workerPid!, 0));
  const history = yield* rpc(brokerUrl, coordinator, "task.control", { taskId: worker.taskId, type: "get_messages" });
  assert.ok("data" in history && history.data !== undefined);
  const historyData = decode(Json, history.data); assert.ok(Array.isArray(historyData.messages));
  assert.equal(encodeJson(JsonValue, historyData.messages).includes(worker.prompt), false);
  yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 2 });
  const finished = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => ["SUCCEEDED", "FAILED", "HELD"].includes(task.state), 180000);
  assert.equal(finished.state, "SUCCEEDED", `Real host final state ${finished.state}: ${encodeJson(JsonValue, finished.result ?? null)}`);
  const finalDb = join(runnerConfig.stateDirectory, "runner.sqlite");
  const one = (yield* sqlRows(finalDb, "SELECT count(*) AS count FROM tasks WHERE taskId=?", [worker.taskId]))[0]; const launches = (yield* sqlRows(finalDb, "SELECT count(*) AS count FROM events WHERE taskId=? AND json_extract(payload,'$.state')='LAUNCHING'", [worker.taskId]))[0]; assert.equal(one?.count, 1); assert.equal(launches?.count, 1);
  assert.equal(encodeJson(JsonValue, decode(JsonValue, (yield* sqlRows(finalDb, "SELECT payload FROM events WHERE taskId=?", [worker.taskId])))).includes("COORDINATOR_SMOKE_OK"), false); 
  yield* evidence("real-owned-omp-child-duplicate-full-and-coordinator-exit", { sha, workerPid, nativeSession: accepted.address!.sessionId, duplicateConflict: true, fullSlotsRejectedWithoutAcceptance: true, brokerCrashAttachPreserved: true, finalState: finished.state, acceptanceRows: 1, launchEvents: 1, evidenceVerified: false });
  yield* rpc(brokerUrl, coordinator, "evidence.local", { taskId: worker.taskId, sha, commands: ["real socket smoke"], passed: true });
  yield* rpc(brokerUrl, coordinator, "evidence.deployment", { taskId: worker.taskId, sha: "a".repeat(40), environment: "independent-demo-environment", url: "https://example.invalid", observedState: "not deployed", provenance: "smoke distinct negative observation" });
  yield* rejects(rpc(brokerUrl, coordinator, "evidence.review", { taskId: worker.taskId, sha, decision: "approved", note: "unauthorized" }), 403);
  yield* rpc(brokerUrl, reviewer, "evidence.review", { taskId: worker.taskId, sha, decision: "changes-requested", note: "Smoke credential provenance; not user approval" });
  yield* rpc(brokerUrl, coordinator, "evidence.target", { taskId: worker.taskId, sha: "b".repeat(40), ref: "main", requiredLanes: ["CI/verify"] });
  const observed = yield* rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }); assert.equal(observed.state, "SUCCEEDED"); assert.ok(observed.evidence); assert.ok(observed.evidence.every((item) => item.stale));
  yield* evidence("independent-evidence-and-new-SHA-staleness", { runnerStateUnchanged: observed.state, oldLocalCiDeploymentReviewStale: true, reviewerAuthorizationSeparate: true });
  yield* until(() => sqlRows(join(runnerConfig.stateDirectory, "runner.sqlite"), "SELECT sum(occupied) AS occupied FROM tasks").pipe(Effect.map((rows) => rows[0]?.occupied)), (occupied) => occupied === 0, 30000);
  yield* Effect.sleep(5500);
  obstructJournal = true;
  const uncertainWork = { ...worker, taskId: `uncertain-${(yield* uuid)}` };
  yield* rpc(brokerUrl, coordinator, "task.submit", uncertainWork);
  yield* until(() => Effect.sync(() => acceptanceCommitted), Boolean, 30000);
  alternate = yield* processFor("runner", alternatePath);
  yield* until(() => health(alternateUrl, alternateToken), Boolean, 30000);
  const ambiguous = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: uncertainWork.taskId }), (task) => task.state === "HELD", 45000);
  assert.equal(ambiguous.runnerId, "homelab-runner");
  yield* Effect.sleep(1500);
  const alternateDb = join(alternateConfig.stateDirectory, "runner.sqlite");
  assert.equal((yield* sqlRows(alternateDb, "SELECT count(*) AS count FROM tasks WHERE taskId=?", [uncertainWork.taskId]))[0]?.count, 0); 
  obstructJournal = false;
  const reconciled = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: uncertainWork.taskId }), (task) => task.state === "ACCEPTED" && Boolean(task.readback), 90000);
  assert.equal(reconciled.runnerId, "homelab-runner");
  const journal = join(runnerConfig.stateDirectory, "runner.sqlite");
  uncertainPid = Number((yield* sqlRows(journal, "SELECT pid FROM tasks WHERE taskId=?", [uncertainWork.taskId]))[0]?.pid); assert.ok(uncertainPid > 0);
  assert.equal((yield* sqlRows(journal, "SELECT count(*) AS count FROM events WHERE taskId=? AND json_extract(payload,'$.state')='LAUNCHING'", [uncertainWork.taskId]))[0]?.count, 1);
  assert.equal((yield* sqlRows(journal, "SELECT count(*) AS count FROM events WHERE taskId=? AND json_extract(payload,'$.type')='prompt_intent'", [uncertainWork.taskId]))[0]?.count, 0); 
  yield* exit(runner!, "SIGKILL"); runner = yield* processFor("runner", runnerPath); yield* ready(runnerUrl);
  const recovered = yield* rpc(runnerUrl, runnerControl, "journal", { taskId: uncertainWork.taskId }); assert.equal(recovered.state, "HELD");
  const restarted = join(runnerConfig.stateDirectory, "runner.sqlite");
  assert.equal((yield* sqlRows(restarted, "SELECT count(*) AS count FROM events WHERE taskId=? AND json_extract(payload,'$.state')='LAUNCHING'", [uncertainWork.taskId]))[0]?.count, 1); 
  yield* evidence("ambiguous-socket-acceptance-and-runner-crash", { acceptanceCommittedBeforeReplyTimeout: true, journalOutageHeldSameRunner: true, freeAlternateHadNoAcceptance: true, reconciledOriginalAcceptance: true, launchEvents: 1, promptIntents: 0, restartState: recovered.state, automaticRelaunch: false });
  yield* (fileSystem.writeFileString(join(state, "transcript.json"), encodeJson(JsonValue, { brokerRuntime: process.version, host: (yield* exec(ChildProcess.make(launcher, ["--version"]))).trim(), privateRepository: identity.private, repositoryId: identity.id, sha, scenarios: transcript }), { mode: 0o600 }));
  yield* print(`Evidence: ${join(state, "transcript.json")}`);
 });
const runIsolationSmoke = () => runSmoke({ "--runner-isolation-only": true });
const program = Effect.gen(function*() {
  const args = [...(yield* (yield* Stdio.Stdio).args)];
  if (args.length === 1 && args[0] === "--runner-isolation-only") {
    return yield* Commands["program.smoke:isolation"].execute({ runIsolationSmoke }, {});
  }
  return yield* Commands["program.smoke"].executeDecoded(
    { runSmoke }, Object.fromEntries(args.map(flag => [flag, true])), decodeEffect,
  );
});
NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer))));
function sessionKey(session: SessionKey): SessionKey { return { sessionId: session.sessionId, incarnation: session.incarnation }; }
