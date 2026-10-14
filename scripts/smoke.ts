// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Cause, Clock, Config, Context, Effect, Exit, Layer, Schema, Scope, Stdio, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { makeDatabase, type DatabaseClient } from "../src/kysely.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { dropPostgres, postgresAdmin, provisionPostgres } from "../test/postgres-fixture.ts";
import { restoreBrokerDatabase } from "../src/backup.ts";
import { BrokerConfig, RunnerConfig, RunnerNativeState, RunnerEvent, ModelProfile, TaskId, MessageId, SessionId, IncarnationId, MachineId, WorkerId, RunnerId, PrincipalId, OwnerId, ArchiveEntryId, GitHubRepositoryId, UserId, ProcessId, Repo, Sha, Json, JsonValue, HttpError, Commands, CommandError, type CommandInput, type Work, type Address, type SessionKey } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { inspectGithubRepository } from "../src/evidence.ts";
import { Mcp, serverLayer } from "../src/http.ts";
import { CHILD_UID_BASE, requireIsolation, isolationArgs, childEnvironment, ownChildDirectory } from "../src/runner.ts";
import { connectNativeClient } from "@samuelho/coordinator-native-control/client";
import { NativeMessagesPage, NativePluginConfig } from "@samuelho/coordinator-native-control/contracts";

// Effect has no executable-path, runtime-version or own-PID service; process is used only for these boundary facts.
const runSmoke = (flags: CommandInput<"program.smoke">) => Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs: fileSystem, path: { join, resolve }, crypto, spawner, digest, fingerprint, exec } = platform;
  const { call: rpc, serve } = yield* Mcp;
  const client = yield* HttpClient.HttpClient;
  const stdio = yield* Stdio.Stdio;
  const print = (line: string) => Stream.make(`${line}\n`).pipe(Stream.run(stdio.stdout()));
  const setting = (key: string, fallback: string) => Config.String(key).pipe(Config.withDefault(fallback));
  const uuid = crypto.randomUUIDv4();
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
  const postgresRead = Effect.fnUntraced(function*<A, E, R>(urlFile: string, query: (db: DatabaseClient) => Effect.Effect<A, E, R>) {
    return yield* Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient;
      return yield* query(makeDatabase(sql));
    }).pipe(Effect.provide(postgresAdmin(urlFile)), Effect.scoped);
  });
  const runnerEvents = (urlFile: string, taskId: TaskId) => postgresRead(urlFile, (db) =>
    db.execute(db.client.selectFrom("runner_events").select("payload").where("taskId", "=", taskId).orderBy("seq"))).pipe(
    Effect.flatMap((rows) => Effect.forEach(rows, (row) => Schema.decodeUnknownEffect(Schema.fromJsonString(RunnerEvent))(row.payload))));
  if (flags["--runner-isolation-only"]) {
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-isolation-" });
    yield* fileSystem.chmod(directory, 0o711);
    const credentials = join(directory, "credentials"), children = join(directory, "children"), model = join(directory, "model");
    const ownRoot = join(children, "own"), sibling = join(children, "sibling");
    const own = join(ownRoot, "repo"), sessions = join(ownRoot, "sessions"), home = join(ownRoot, "home");
    for (const path of [credentials, children, model, ownRoot, own, sessions, home, sibling]) yield* fileSystem.makeDirectory(path, { mode: 0o700 });
    const token = join(credentials, "runner.token"), controlPath = join(credentials, "control.token"), ledger = join(directory, "database.url");
    for (const path of [token, controlPath, ledger, join(sibling, "private")]) yield* fileSystem.writeFileString(path, "ISOLATION_SECRET", { mode: 0o600 });
    yield* fileSystem.makeDirectory(join(home, ".omp/agent"), { recursive: true, mode: 0o700 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/config.yml"), Schema.encodeUnknownSync(Schema.fromJsonString(ModelProfile), { onExcessProperty: "error" })({ modelRoles: { default: yield* setting("COORDINATOR_MODEL", "openai-codex/gpt-6.1-sol:xhigh") } }), { mode: 0o600 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/auth.json"), "{}", { mode: 0o600 });
    yield* fileSystem.makeDirectory(join(model, ".omp/agent"), { recursive: true, mode: 0o700 });
    const parentProfile = join(model, ".omp/agent/auth.json");
    yield* fileSystem.writeFileString(parentProfile, "ISOLATION_SECRET", { mode: 0o600 });
    const launcher = yield* setting("COORDINATOR_TEST_OMP", "/usr/local/bin/omp");
    const config: RunnerConfig = { id: RunnerId.make("isolation-probe"), databaseUrlFile: ledger, brokerUrl: "http://127.0.0.1", tokenFile: token, controlTokenHash: yield* digest("control"),
      stateDirectory: join(directory, "state"), host: "127.0.0.1", port: 0, slots: 1, repositories: {}, launchers: { omp: launcher },
      modelDirectory: model, childDirectory: children, childUidBase: UserId.make(CHILD_UID_BASE) };
    yield* requireIsolation(config);
    const ownUid = UserId.make(CHILD_UID_BASE + 1), siblingUid = UserId.make(CHILD_UID_BASE + 2);
    const nativeConfig = join(ownRoot, "native-plugin.json"), endpointFile = join(ownRoot, "native-endpoint.json"), nativeTokenFile = join(ownRoot, "native.token");
    const nativeToken = yield* secret();
    yield* fileSystem.writeFileString(nativeTokenFile, nativeToken, { flag: "wx", mode: 0o600 });
    yield* fileSystem.writeFileString(nativeConfig, Schema.encodeSync(Schema.fromJsonString(NativePluginConfig))({ host: "omp", endpointFile, tokenFile: nativeTokenFile }), { flag: "wx", mode: 0o600 });
    yield* ownChildDirectory(ownRoot, ownUid); yield* ownChildDirectory(sibling, siblingUid);
    const nativeScope = yield* Scope.fork(yield* Effect.scope);
    // Stock RPC stdio remains private; the extension owns its separate MCP endpoint.
    const native = yield* connectNativeClient({
      host: "omp", command: "/usr/bin/env",
      args: [`--chdir=${own}`, "/usr/bin/setpriv", ...isolationArgs(ownUid), launcher, "--mode", "rpc", "--session-dir", sessions, "--no-extensions", "-e", resolve(import.meta.dirname, "../dist/plugins/native-plugin.js")],
      cwd: directory, env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", HOME: home, COORDINATOR_NATIVE_CONFIG: nativeConfig }, connectTimeoutMs: 60000,
      plugin: { endpointFile, token: nativeToken },
    }).pipe(Effect.provideService(Scope.Scope, nativeScope));
    assert.ok(native.pid > 0);
    const nativeState = yield* native.call("native.get_state", { binding: native.binding }, { timeoutMs: 60000 });
    assert.equal(nativeState.cwd, own);
    const nativeMessages = yield* native.call("native.get_messages", { binding: native.binding, limit: 256 }, { timeoutMs: 60000 });
    assert.deepEqual(nativeMessages.messages, []);
    const quote = (path: string) => `'${path.replace(/'/g, "'\\''")}'`;
    // Effect cannot inspect another process's environ; /proc access itself is the isolation proof.
    const denied = [token, controlPath, ledger, parentProfile, join(sibling, "private"), `/proc/${process.pid}/environ`];
    const command = `set -eu; ${denied.map((path) => `if cat ${quote(path)} >/dev/null 2>&1; then exit 41; fi`).join("; ")}; ` +
      `test -z "\${GITHUB_PAT-}\${GITHUB_TOKEN-}\${GH_TOKEN-}\${OP_SERVICE_ACCOUNT_TOKEN-}\${COORDINATOR_CONFIG-}\${OPENAI_API_KEY-}\${ANTHROPIC_API_KEY-}\${GEMINI_API_KEY-}\${OMP_AUTH_BROKER_TOKEN-}"; ` +
      `test "$(id -u)" = "${CHILD_UID_BASE + 1}"; test "$PWD" = ${quote(own)}; test "$(stat -c %a "$HOME")" = 700; ` +
      `touch approved-write; test -r "$HOME/.omp/agent/config.yml"; test -r "$HOME/.omp/agent/auth.json"; test "$(stat -c %a "$HOME/.omp/agent/auth.json")" = 600; touch ${quote(join(sessions, "approved-session-write"))}; ` +
      `checked=0; while IFS=: read -r key value; do set -- $value; case "$key" in Groups) test "$#" = 0; checked=$((checked+1));; CapInh|CapPrm|CapEff|CapBnd|CapAmb) test "$1" = 0000000000000000; checked=$((checked+1));; NoNewPrivs) test "$1" = 1; checked=$((checked+1));; esac; done </proc/self/status; test "$checked" = 7; printf ISOLATION_OK`;
    const response = yield* native.call("native.bash", { binding: native.binding, command }, { timeoutMs: 60000 });
    assert.equal(response.exitCode, 0);
    assert.equal(response.killed, false);
    assert.ok(response.output.includes("ISOLATION_OK"));
    yield* Scope.close(nativeScope, Exit.void);
    yield* native.observedProcessClosed.pipe(Effect.timeout(10000));
    yield* print(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ scenario: "real-native-os-isolation", childUid: CHILD_UID_BASE + 1, forbiddenPathsDenied: denied.length,
      ownWorkspaceAndSessionWritable: true, modelProfileReadable: true, privateAuthProfileReadable: true, privateHomeMode: "0700", parentProfileDenied: true, parentEnvironDenied: true, siblingUidDistinct: true,
      capabilitiesZero: true, supplementalGroupsEmpty: true, noNewPrivileges: true, nativeStateAndMessagesUsable: true, forbiddenEnvironmentAbsent: true, sdkPid: native.pid, physicalScopedCloseProved: true }));
    return;
  }
const root = resolve(import.meta.dirname, "..");
const state = (yield* setting("COORDINATOR_SMOKE_DIRECTORY", "")) || (yield* fileSystem.makeTempDirectory({ prefix: "coordinator-sockets-" }));
yield* (fileSystem.makeDirectory(state, { recursive: true, mode: 0o711 })); yield* (fileSystem.chmod(state, 0o711));
const repo = Repo.make("github.com/samuelho-dev/coordinator");
const sha = Schema.decodeUnknownSync(Sha, { onExcessProperty: "error" })((yield* exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }))).trim());
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
const brokerDirectory = join(state, "broker");
yield* fileSystem.makeDirectory(brokerDirectory, { mode: 0o700 });
const brokerConfig: BrokerConfig = { databaseUrlFile: yield* provisionPostgres(brokerDirectory), host: "127.0.0.1", port: brokerPort,
  backupDirectory: join(state, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000, githubTokenEnv: "GITHUB_READ_TOKEN",
  repositoryIds: { [repo]: identity.id },
  principals: [
    { id: PrincipalId.make("coordinator"), owner: OwnerId.make("owner"), roles: ["coordinator"], repos: [repo], tokenHash: (yield* digest(coordinator)) },
    { id: PrincipalId.make("recipient"), owner: OwnerId.make("owner"), roles: ["session"], repos: [repo], tokenHash: (yield* digest(recipient)) },
    { id: PrincipalId.make("homelab-runner"), owner: OwnerId.make("owner"), roles: ["runner"], repos: [repo], tokenHash: (yield* digest(runnerToken)), controlTokenHash: (yield* digest(runnerControl)), runnerUrl: proxyUrl },
    { id: PrincipalId.make("alternate-runner"), owner: OwnerId.make("owner"), roles: ["runner"], repos: [repo], tokenHash: (yield* digest(alternateToken)), controlTokenHash: (yield* digest(alternateControl)), runnerUrl: alternateUrl },
    { id: PrincipalId.make("reviewer"), owner: OwnerId.make("owner"), roles: ["reviewer"], repos: [repo], tokenHash: (yield* digest(reviewer)) },
    { id: PrincipalId.make("writer"), owner: OwnerId.make("owner"), roles: ["archive-writer"], repos: [repo], tokenHash: (yield* digest(writer)) },
    { id: PrincipalId.make("outsider"), owner: OwnerId.make("outsider"), roles: ["coordinator", "session"], repos: [repo], tokenHash: (yield* digest(outsider)) },
  ] };
const launcher = yield* setting("COORDINATOR_TEST_OMP", "/home/admin/.bun/bin/omp");
const runnerDatabaseDirectory = join(state, "runner-database"), alternateDatabaseDirectory = join(state, "alternate-database");
for (const directory of [runnerDatabaseDirectory, alternateDatabaseDirectory]) yield* fileSystem.makeDirectory(directory, { mode: 0o700 });
const runnerConfig: RunnerConfig = { id: RunnerId.make("homelab-runner"), databaseUrlFile: yield* provisionPostgres(runnerDatabaseDirectory), brokerUrl, tokenFile: runnerEnrollmentPath, host: "127.0.0.1", port: runnerPort,
  stateDirectory: join(state, "runner"), slots: 1, repositories: { [repo]: root }, launchers: { omp: launcher }, controlTokenHash: (yield* digest(runnerControl)),
  modelDirectory: yield* setting("COORDINATOR_MODEL_DIRECTORY", "/work/home"), childDirectory: join(state, "children"), childUidBase: UserId.make(1000000) };
const brokerPath = join(state, "broker.json"), runnerPath = join(state, "runner.json");
yield* (fileSystem.writeFileString(brokerPath, Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(brokerConfig), { mode: 0o600 })); yield* (fileSystem.writeFileString(runnerPath, Schema.encodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(runnerConfig), { mode: 0o600 }));
const alternateConfig: RunnerConfig = { ...runnerConfig, databaseUrlFile: yield* provisionPostgres(alternateDatabaseDirectory), id: RunnerId.make("alternate-runner"), port: alternatePort, tokenFile: alternateEnrollmentPath, stateDirectory: join(state, "alternate-runner"), childDirectory: join(state, "alternate-children"), childUidBase: UserId.make(2000000), controlTokenHash: (yield* digest(alternateControl)) };
const alternatePath = join(state, "alternate.json"); yield* (fileSystem.writeFileString(alternatePath, Schema.encodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(alternateConfig), { mode: 0o600 }));
const transcript: JsonValue[] = [];
const evidence = Effect.fnUntraced(function*(scenario: string, observed: unknown) { const entry = Schema.decodeUnknownSync(JsonValue, { onExcessProperty: "error" })({ scenario, observed, time: new Date(yield* Clock.currentTimeMillis).toISOString() }); transcript.push(entry); yield* print(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(entry)); });
const processFor = Effect.fnUntraced(function*(mode: "broker" | "runner", config: string) {
  const env = { ...(yield* childEnvironment(yield* setting("HOME", "/tmp"))), COORDINATOR_CONFIG: config, COORDINATOR_RUNNER_TOKEN_DIRECTORY: tokenDirectory };
  const readToken = yield* setting("GITHUB_READ_TOKEN", "");
  const hosts = yield* setting("COORDINATOR_CONNECT_HOSTS_JSON", "{}");
  const ca = yield* setting("NODE_EXTRA_CA_CERTS", "");
  const runtimeEnv = { ...env, COORDINATOR_CONNECT_HOSTS_JSON: hosts, ...(mode === "broker" && readToken ? { GITHUB_READ_TOKEN: readToken } : {}),
    ...(mode === "broker" && ca ? { NODE_EXTRA_CA_CERTS: ca } : {}) };
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
let workerPid: ProcessId | undefined, uncertainPid: ProcessId | undefined;
yield* Effect.addFinalizer(() => Effect.gen(function*() {
  for (const child of [alternate, runner, broker]) if (child) yield* exit(child, "SIGTERM").pipe(Effect.ignore);
  // Effect cannot signal arbitrary PIDs recovered from a different supervisor's journal.
  for (const pid of [workerPid, uncertainPid]) if (pid) yield* Effect.try(() => process.kill(pid, "SIGTERM")).pipe(Effect.ignore);
  if (!(yield* broker.isRunning)) yield* dropPostgres(brokerConfig.databaseUrlFile).pipe(Effect.orDie);
  for (const config of [runnerConfig, alternateConfig]) yield* dropPostgres(config.databaseUrlFile).pipe(Effect.orDie);
}).pipe(Effect.orDie));
  yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, (yield* secret()), "task.get", { taskId: TaskId.make("missing") }), 401);
  const session = { sessionId: SessionId.make("native-owner-session"), incarnation: IncarnationId.make(yield* uuid), machineId: MachineId.make("homelab"), host: "omp" as const, repo, workerId: WorkerId.make("existing-omp-smoke-worker") };
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session); yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session));
  const work: Work = { taskId: TaskId.make(`smoke-${(yield* uuid)}`), repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "Respond exactly COORDINATOR_SMOKE_OK. Do not call tools, change files, merge or deploy.", scope: "Socket lifecycle smoke only", restrictions: "No file modifications, merging or deployment", expectedEvidence: ["one owned real omp child", "exact-SHA result"], deadline: (yield* Clock.currentTimeMillis) + 600000,
    workerId: session.workerId, githubRepoId: identity.id, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
  yield* rejects(rpc(brokerUrl, writer, "task.submit", work), 403);
  yield* evidence("authentication-and-operation-authorization", { invalidCredential: 401, archiveWriterCannotDispatch: 403 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.submit", { ...work, taskId: TaskId.make("wrong-enrolled-repository"), githubRepoId: GitHubRepositoryId.make(identity.id + 1) }), 409);
  yield* evidence("inspected-enrollment-without-CI-credential", { repositoryId: identity.id, mismatchedIdRejected: true, broadGithubCredentialNotInherited: true });
  const peerTask = yield* rpc(brokerUrl, coordinator, "task.submit", work);
  assert.equal(peerTask.offerDecision, "pending"); assert.equal(peerTask.runnerId, undefined);
  yield* evidence("peer-first-owner-offer", { taskId: work.taskId, state: peerTask.state, decision: peerTask.offerDecision, runnerAssigned: false });
  const message = { messageId: MessageId.make(yield* uuid), ...sessionKey(session), payload: { note: "addressed owner notification" }, ttlMs: 60000 };
  yield* rpc(brokerUrl, coordinator, "peer.send", message);
  const before = yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.equal(before.messages.length, 1);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: MessageId.make(yield* uuid) }), 409);
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session);
  const replay = yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.deepEqual(replay.messages, before.messages);
  yield* rpc(brokerUrl, recipient, "peer.ack", { ...sessionKey(session), messageId: message.messageId });
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  yield* evidence("broker-crash-same-incarnation-replay", { replayed: 1, newSendBeforeHeartbeatRejected: true, handledAckDeletes: true });
  yield* rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: MessageId.make("expires"), ttlMs: 100 }); yield* Effect.sleep(150);
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  const address: Address = { messageId: MessageId.make(work.taskId), revision: 1, workerId: work.workerId, ...sessionKey(session), repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  yield* rpc(brokerUrl, recipient, "peer.reply", { taskId: work.taskId, ...sessionKey(session), decision: "accepted" });
  yield* rpc(brokerUrl, coordinator, "task.address", { taskId: work.taskId, address });
  yield* rejects(rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: { ...address, ref: "wrong-ref" }, disposition: "ready" }), 409);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "hold", reason: "yield* correction", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "ready" }); yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 });
  const second = { ...session, sessionId: SessionId.make("handoff-session"), incarnation: IncarnationId.make(yield* uuid) }; yield* rpc(brokerUrl, recipient, "session.heartbeat", second); yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second));
  yield* rejects(rpc(brokerUrl, coordinator, "task.handoff", { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, "task.report", { taskId: work.taskId, ...sessionKey(session), state: "SUCCEEDED", result: { syntheticPeerGate: true } });
  yield* rpc(brokerUrl, coordinator, "task.handoff", { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 });
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), 409);
  const handoffAddress = { ...address, ...sessionKey(second), revision: 2 };
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: handoffAddress, disposition: "ready" }); yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 });
  yield* evidence("expiry-correction-hold-and-handoff-gate", { expiredPeerNotDelivered: true, wrongRefRejected: true, heldConfirmationRejected: true, handoffRequiresNewReadback: true });
  const instructionAddress = { ...handoffAddress, messageId: MessageId.make("later-owned-message"), revision: 3, scope: "A separately approved later instruction" };
  yield* rpc(brokerUrl, coordinator, "task.instruction", { taskId: work.taskId, address: instructionAddress, prompt: "Later approved instruction; do not repeat the launch prompt", approved: true });
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second))).releases.length, 0);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: instructionAddress, disposition: "ready" });
  yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 3 });
  const later = (yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(second))).releases[0]!;
  assert.equal(later.instruction?.prompt, "Later approved instruction; do not repeat the launch prompt");
  yield* rpc(brokerUrl, recipient, "task.report", { taskId: work.taskId, ...sessionKey(second), state: "SUCCEEDED", result: { syntheticPeerGate: true } });
  yield* evidence("later-native-instruction-gate", { sameTaskId: later.taskId, previousRevisionCannotRelease: true, newReadbackRequired: true, approvedPromptDistinct: true });
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: [PrincipalId.make("recipient")], enabled: true });
  const payload = { id: ArchiveEntryId.make("entry-one"), parentId: null, type: "message", contentOmitted: true as const };
  const archive = { sessionId: session.sessionId, entryId: ArchiveEntryId.make("entry-one"), digest: (yield* fingerprint(payload)), payload, gaps: ["content-omitted", "unlinked"] };
  yield* rpc(brokerUrl, writer, "archive.append", archive); yield* rpc(brokerUrl, writer, "archive.append", archive);
  const unsafePayload = { ...payload, text: "unsafe-raw-content-must-not-persist" };
  // Send malformed arguments directly to MCP: the typed client deliberately rejects them before transport.
  const archiveId = "archive.append";
  const archiveMethod = Commands[archiveId].method ?? archiveId;
  const unsafeResponse = yield* client.execute(HttpClientRequest.post(new URL("/mcp", brokerUrl)).pipe(
    HttpClientRequest.setHeaders({ authorization: `Bearer ${writer}`, accept: "application/json, text/event-stream",
      "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": archiveMethod }),
    HttpClientRequest.bodyText(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ jsonrpc: "2.0", id: "unsafe-archive", method: "tools/call",
      params: { name: archiveMethod, arguments: { ...archive, entryId: "unsafe", payload: unsafePayload, digest: yield* fingerprint(unsafePayload) },
        _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} } } }), "application/json"),
  ));
  assert.equal(unsafeResponse.status, 200);
  const unsafeText = yield* unsafeResponse.text;
  const unsafeMessage = Schema.decodeUnknownSync(Schema.fromJsonString(Json), { onExcessProperty: "error" })(unsafeResponse.headers["content-type"]?.includes("text/event-stream")
    ? unsafeText.split(/\r?\n/).filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n")
    : unsafeText);
  assert.ok(typeof unsafeMessage === "object" && unsafeMessage !== null && "result" in unsafeMessage);
  const unsafeResult = Schema.decodeUnknownSync(Json, { onExcessProperty: "error" })(unsafeMessage.result);
  assert.ok(typeof unsafeResult === "object" && unsafeResult !== null && "isError" in unsafeResult);
  assert.equal(unsafeResult.isError, true);
  const archived = yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId });
  assert.equal(archived.length, 1);
  assert.equal(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(archived).includes(unsafePayload.text), false);
  assert.deepEqual(yield* postgresRead(brokerConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("archive").select("sequence").where("payload", "like", `%${unsafePayload.text}%`))), []);
  yield* rejects(rpc(brokerUrl, writer, "archive.delete", { sessionId: session.sessionId }), 403);
  assert.deepEqual(yield* rpc(brokerUrl, outsider, "archive.list", { sessionId: session.sessionId }), []);
  const snapshotNames = new Set(yield* (fileSystem.readDirectory(brokerConfig.backupDirectory)));
  const snapshotName = yield* until(() => fileSystem.readDirectory(brokerConfig.backupDirectory).pipe(Effect.map((names) => names.find((name) => name.endsWith(".pgdump") && !snapshotNames.has(name)))), (name) => Boolean(name), 75000);
  const snapshot = join(brokerConfig.backupDirectory, snapshotName!);
  const snapshotDirectory = join(state, "snapshot-database");
  yield* fileSystem.makeDirectory(snapshotDirectory, { mode: 0o700 });
  const snapshotUrlFile = yield* provisionPostgres(snapshotDirectory);
  yield* restoreBrokerDatabase({ ...brokerConfig, databaseUrlFile: snapshotUrlFile }, snapshot, true);
  for (const table of Array.of<"archive" | "consents" | "participant_consents">("archive", "consents", "participant_consents")) assert.deepEqual(yield* postgresRead(snapshotUrlFile, (db) => db.execute(db.client.selectFrom(table).select("owner").limit(1))), []);
  assert.ok((yield* postgresRead(snapshotUrlFile, (db) => db.execute(db.client.selectFrom("tasks").select("id").where("id", "=", work.taskId))))[0]);
  yield* dropPostgres(snapshotUrlFile);
  assert.equal((yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId })).length, 1);
  yield* rejects(rpc(brokerUrl, outsider, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: false }), 403);
  yield* rpc(brokerUrl, outsider, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: true });
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: [PrincipalId.make("recipient"), PrincipalId.make("outsider")], enabled: true });
  yield* rpc(brokerUrl, outsider, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: false });
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  yield* rpc(brokerUrl, recipient, "session.heartbeat", session);
  assert.equal((yield* rpc(brokerUrl, recipient, "session.poll", sessionKey(session))).archiveEnabled, false);
  yield* rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: [PrincipalId.make("recipient")], enabled: true });
  yield* rpc(brokerUrl, writer, "archive.append", archive);
  yield* rpc(brokerUrl, coordinator, "archive.delete", { sessionId: session.sessionId });
  yield* rejects(rpc(brokerUrl, writer, "archive.append", archive), 403);
  yield* exit(broker, "SIGTERM"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  yield* evidence("archive-privacy-idempotency-revocation-and-restart", { uniqueEntries: 1, appendOnlyCannotDelete: true, otherOwnerSeesNoRows: true, optoutRejectsAppendAfterRestart: true, snapshotsExcludeArchiveAndConsent: true });
  runner = yield* processFor("runner", runnerPath); yield* ready(runnerUrl); yield* Effect.sleep(300);
  const worker = { ...work, taskId: TaskId.make(`worker-${(yield* uuid)}`), bypassPeer: true };
  const submitted = yield* rpc(brokerUrl, coordinator, "task.submit", worker); const duplicate = yield* rpc(brokerUrl, coordinator, "task.submit", worker); assert.equal(submitted.taskId, duplicate.taskId);
  yield* rejects(rpc(brokerUrl, coordinator, "task.submit", { ...worker, prompt: "different" }), 409);
  const accepted = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => Boolean(task.readback), 120000);
  assert.equal(accepted.address?.sha, sha); assert.equal(accepted.runnerId, "homelab-runner");
  const row = (yield* postgresRead(runnerConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("runner_tasks").select("pid").where("taskId", "=", worker.taskId))))[0]; workerPid = Schema.decodeUnknownSync(ProcessId, { onExcessProperty: "error" })(row?.pid);
  assert.ok(workerPid > 0);
  yield* rejects(rpc(runnerUrl, runnerToken, "acceptTask", worker), 401);
  assert.equal((yield* rpc(runnerUrl, runnerControl, "acceptTask", worker)).taskId, worker.taskId);
  const corrected = { ...accepted.address!, revision: 2 };
  const correctedTask = yield* rpc(brokerUrl, coordinator, "task.address", { taskId: worker.taskId, address: corrected });
  assert.deepEqual(correctedTask.readback, corrected);
  yield* rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 1 }), 409);
  const full = yield* rpc(runnerUrl, runnerControl, "acceptTask", { ...worker, taskId: TaskId.make("full-capacity") }); assert.equal(full.accepted, false); assert.equal(full.reason, "full");
  yield* rejects(rpc(brokerUrl, outsider, "task.get", { taskId: worker.taskId }), 403);
  yield* rejects(rpc(brokerUrl, writer, "task.attach", { taskId: worker.taskId }), 403);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath); yield* ready(brokerUrl);
  const attached = yield* rpc(brokerUrl, coordinator, "task.attach", { taskId: worker.taskId }); assert.equal(attached.taskId, worker.taskId); // Effect has no arbitrary-PID liveness API; this PID is owned by the runner process.
  yield* Effect.try(() => process.kill(workerPid!, 0));
  const controlledState = Schema.decodeUnknownSync(RunnerNativeState, { onExcessProperty: "error" })(yield* rpc(brokerUrl, coordinator, "task.control", { taskId: worker.taskId, type: "get_state" }));
  assert.equal(controlledState.binding.sessionId, accepted.address!.sessionId);
  assert.equal(controlledState.isStreaming, false);
  assert.equal(controlledState.currentRunId, null);
  const historyMessages: Schema.Json[] = [];
  let historyCursor: string | undefined;
  for (;;) {
    const history = Schema.decodeUnknownSync(NativeMessagesPage, { onExcessProperty: "error" })(yield* rpc(brokerUrl, coordinator, "task.control", { taskId: worker.taskId, type: "get_messages", ...(historyCursor ? { cursor: historyCursor } : {}), limit: 256 }));
    assert.deepEqual(history.binding, controlledState.binding);
    historyMessages.push(...history.messages);
    if (!history.more) break;
    assert.ok(history.nextCursor && history.nextCursor !== historyCursor);
    historyCursor = history.nextCursor;
  }
  assert.equal(Schema.encodeSync(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" })(historyMessages).includes(worker.prompt), false);
  yield* rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 2 });
  const finished = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => ["SUCCEEDED", "FAILED", "HELD"].includes(task.state), 180000);
  assert.equal(finished.state, "SUCCEEDED", `Real host final state ${finished.state}: ${Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(finished.result ?? null)}`);
  const one = yield* postgresRead(runnerConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("runner_tasks").select("taskId").where("taskId", "=", worker.taskId)));
  const settledEvents = yield* runnerEvents(runnerConfig.databaseUrlFile, worker.taskId);
  assert.equal(one.length, 1); assert.equal(settledEvents.filter((event) => event.state === "LAUNCHING").length, 1);
  assert.equal(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(settledEvents).includes("COORDINATOR_SMOKE_OK"), false);
  yield* evidence("real-owned-omp-child-duplicate-full-and-coordinator-exit", { sha, workerPid, nativeSession: accepted.address!.sessionId, duplicateConflict: true, fullSlotsRejectedWithoutAcceptance: true, brokerCrashAttachPreserved: true, finalState: finished.state, acceptanceRows: 1, launchEvents: 1, evidenceVerified: false });
  yield* rpc(brokerUrl, coordinator, "evidence.local", { taskId: worker.taskId, sha, commands: ["real socket smoke"], passed: true });
  yield* rpc(brokerUrl, coordinator, "evidence.deployment", { taskId: worker.taskId, sha: Sha.make("a".repeat(40)), environment: "independent-demo-environment", url: "https://example.invalid", observedState: "not deployed", provenance: "smoke distinct negative observation" });
  yield* rejects(rpc(brokerUrl, coordinator, "evidence.review", { taskId: worker.taskId, sha, decision: "approved", note: "unauthorized" }), 403);
  yield* rpc(brokerUrl, reviewer, "evidence.review", { taskId: worker.taskId, sha, decision: "changes-requested", note: "Smoke credential provenance; not user approval" });
  yield* rpc(brokerUrl, coordinator, "evidence.target", { taskId: worker.taskId, sha: Sha.make("b".repeat(40)), ref: "main", requiredLanes: ["CI/verify"] });
  const observed = yield* rpc(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }); assert.equal(observed.state, "SUCCEEDED"); assert.ok(observed.evidence); assert.ok(observed.evidence.every((item) => item.stale));
  yield* evidence("independent-evidence-and-new-SHA-staleness", { runnerStateUnchanged: observed.state, oldLocalCiDeploymentReviewStale: true, reviewerAuthorizationSeparate: true });
  yield* until(() => postgresRead(runnerConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("runner_tasks").select("occupied"))).pipe(Effect.map((rows) => rows.reduce((total, row) => total + row.occupied, 0))), (occupied) => occupied === 0, 30000);
  yield* Effect.sleep(5500);
  obstructJournal = true;
  const uncertainWork = { ...worker, taskId: TaskId.make(`uncertain-${(yield* uuid)}`) };
  yield* rpc(brokerUrl, coordinator, "task.submit", uncertainWork);
  yield* until(() => Effect.sync(() => acceptanceCommitted), Boolean, 30000);
  alternate = yield* processFor("runner", alternatePath);
  yield* until(() => health(alternateUrl, alternateToken), Boolean, 30000);
  const ambiguous = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: uncertainWork.taskId }), (task) => task.state === "HELD", 45000);
  assert.equal(ambiguous.runnerId, "homelab-runner");
  yield* Effect.sleep(1500);
  assert.deepEqual(yield* postgresRead(alternateConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("runner_tasks").select("taskId").where("taskId", "=", uncertainWork.taskId))), []);
  obstructJournal = false;
  const reconciled = yield* until(() => rpc(brokerUrl, coordinator, "task.get", { taskId: uncertainWork.taskId }), (task) => task.state === "ACCEPTED" && Boolean(task.readback), 90000);
  assert.equal(reconciled.runnerId, "homelab-runner");
  uncertainPid = Schema.decodeUnknownSync(ProcessId, { onExcessProperty: "error" })((yield* postgresRead(runnerConfig.databaseUrlFile, (db) => db.execute(db.client.selectFrom("runner_tasks").select("pid").where("taskId", "=", uncertainWork.taskId))))[0]?.pid); assert.ok(uncertainPid > 0);
  const acceptedEvents = yield* runnerEvents(runnerConfig.databaseUrlFile, uncertainWork.taskId);
  assert.equal(acceptedEvents.filter((event) => event.state === "LAUNCHING").length, 1);
  assert.equal(acceptedEvents.filter((event) => event.type === "prompt_intent").length, 0);
  yield* exit(runner!, "SIGKILL"); runner = yield* processFor("runner", runnerPath); yield* ready(runnerUrl);
  const recovered = yield* rpc(runnerUrl, runnerControl, "journal", { taskId: uncertainWork.taskId }); assert.equal(recovered.state, "HELD");
  assert.equal((yield* runnerEvents(runnerConfig.databaseUrlFile, uncertainWork.taskId)).filter((event) => event.state === "LAUNCHING").length, 1);
  yield* evidence("ambiguous-socket-acceptance-and-runner-crash", { acceptanceCommittedBeforeReplyTimeout: true, journalOutageHeldSameRunner: true, freeAlternateHadNoAcceptance: true, reconciledOriginalAcceptance: true, launchEvents: 1, promptIntents: 0, restartState: recovered.state, automaticRelaunch: false });
  yield* (fileSystem.writeFileString(join(state, "transcript.json"), Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ brokerRuntime: process.version, host: (yield* exec(ChildProcess.make(launcher, ["--version"]))).trim(), privateRepository: identity.private, repositoryId: identity.id, sha, scenarios: transcript }), { mode: 0o600 }));
  yield* print(`Evidence: ${join(state, "transcript.json")}`);
 });
const runIsolationSmoke = () => runSmoke({ "--runner-isolation-only": true });
const program = Effect.gen(function*() {
  const args = [...(yield* (yield* Stdio.Stdio).args)];
  if (args.length === 1 && args[0] === "--runner-isolation-only") {
    return yield* Commands["program.smoke:isolation"].execute({ runIsolationSmoke }, {});
  }
  return yield* Commands["program.smoke"].executeDecoded({ runSmoke }, Object.fromEntries(args.map(flag => [flag, true])));
});
NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer))));
function sessionKey(session: SessionKey): SessionKey { return { sessionId: session.sessionId, incarnation: session.incarnation }; }
