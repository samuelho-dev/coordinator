import { RoleEnum, DispositionEnum, OfferDecisionEnum, ReviewDecisionEnum } from "../src/contracts.ts";
import { TaskStateEnum, RunnerStateEnum } from "../src/generated/database/enums.ts";
// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Cause, Clock, Config, Context, Effect, Exit, Formatter, Layer, Redacted, Schema, Scope, Stdio, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, HttpServer, HttpServerResponse } from "effect/http";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { DatabaseService, type DatabaseClient } from "../src/database.ts";

import { dropPostgres, postgresAdmin, postgresConfig, provisionPostgres } from "../test/postgres-fixture.ts";
import { restoreBrokerDatabase } from "../src/backup.ts";
import { BrokerConfig, RunnerConfig, RunnerRequest, RunnerControlEnum, RunnerEvent, ModelProfile, MachineId, WorkerId, RunnerId, GitHubRepositoryId, UserId, Repo, Json, JsonValue, HttpError, Commands, command, CommandError, type CommandInput, type Work, type Address, type SessionKey } from "../src/contracts.ts";
import { TaskId, PeerId, SessionId, Archive, Evidence, Peer, RunnerTask } from "../src/generated/database/types.ts";
import { Platform } from "../src/platform.ts";
import { inspectGithubRepository } from "../src/broker.ts";
import { Mcp, serverLayer } from "../src/mcp.ts";
import { CHILD_UID_BASE, requireIsolation, isolationArgs, childEnvironment, ownChildDirectory } from "../src/runner.ts";
import { connectAgentClient } from "../src/agent/client.ts";
import { AgentTools, AgentHostEnum, AgentConfig } from "../src/agent/protocol.ts";
import { Tool } from "effect/ai";

const smokeCommand = command("program.smoke", "program", "runSmoke", "Run the integration smoke with configured real host and GitHub read credential.", Schema.Struct({ "--runner-isolation-only": Schema.optional(Schema.Boolean).annotate({ description: "Run only the real Linux agent-child isolation proof." }) }), Schema.Void, { exposure: "private" });
const isolationSmokeCommand = command("program.smoke:isolation", "program", "runIsolationSmoke", "Run only the real Linux setpriv and agent-child isolation proof.", Tool.EmptyParams, Schema.Void, { exposure: "private" });

// Effect has no executable-path, runtime-version or own-PID service; process is used only for these boundary facts.
const runSmoke = (flags: CommandInput<typeof smokeCommand>) => Effect.gen(function*() {
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
    let error: Cause.Cause<E> | undefined;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const result = yield* Effect.exit(Effect.suspend(read));
      if (Exit.isSuccess(result)) { if (check(result.value)) return result.value; }
      else error = result.cause;
      yield* Effect.sleep(200);
    }
    return yield* Effect.fail(new CommandError({ message: `Scenario deadline expired${error === undefined ? "" : `: ${Cause.pretty(error).slice(0, 200)}`}` }));
  });
  const exit = Effect.fnUntraced(function*(child: ChildProcessSpawner.ChildProcessHandle, signal: "SIGTERM" | "SIGKILL") {
    if (yield* child.isRunning) yield* child.kill({ killSignal: signal });
    yield* until(() => child.isRunning, (running) => !running, 10000);
  });
  const postgresRead = Effect.fnUntraced(function*<A, E, R>(url: Redacted.Redacted<string>, query: (db: DatabaseClient) => Effect.Effect<A, E, R>) {
    return yield* Effect.gen(function*() {
      const database = yield* DatabaseService;
      return yield* query(database);
    }).pipe(Effect.provide(postgresAdmin(url)), Effect.scoped);
  });
  const runnerEvents = (url: Redacted.Redacted<string>, taskId: TaskId) => postgresRead(url, (db) =>
    db.query((client) => client.selectFrom("runner_events").select("payload").where("taskId", "=", taskId).orderBy("seq"))).pipe(
    Effect.flatMap((rows) => Effect.forEach(rows, (row) => Schema.decodeEffect(Schema.fromJsonString(RunnerEvent))(row.payload))));
  if (flags["--runner-isolation-only"]) {
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-isolation-" });
    yield* fileSystem.chmod(directory, 0o711);
    const credentials = join(directory, "credentials"), children = join(directory, "children"), model = join(directory, "model");
    const ownRoot = join(children, "own"), sibling = join(children, "sibling");
    const own = join(ownRoot, "repo"), sessions = join(ownRoot, "sessions"), home = join(ownRoot, "home");
    for (const path of [credentials, children, model, ownRoot, own, sessions, home, sibling]) yield* fileSystem.makeDirectory(path, { mode: 0o700 });
    // ponytail: isolated controls need a selectable local model, never external provider credentials.
    const provider = Context.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
    let providerRequests = 0;
    yield* provider.serve(Effect.sync(() => { providerRequests++; return HttpServerResponse.empty({ status: 500 }); }));
    const models = { providers: { "coordinator-isolation": { api: "openai-completions", baseUrl: `${HttpServer.formatAddress(provider.address)}/v1`, apiKey: "local-isolation",
      models: [{ id: "scripted", name: "Isolation fixture", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 512,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } };
    const token = join(credentials, "runner.token"), controlPath = join(credentials, "control.token"), ledger = join(directory, "private.key");
    for (const path of [token, controlPath, ledger, join(sibling, "private")]) yield* fileSystem.writeFileString(path, "ISOLATION_SECRET", { mode: 0o600 });
    yield* fileSystem.makeDirectory(join(home, ".omp/agent"), { recursive: true, mode: 0o700 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/config.yml"), Schema.encodeSync(Schema.fromJsonString(ModelProfile), { onExcessProperty: "error" })({ modelRoles: { default: "coordinator-isolation/scripted" } }), { mode: 0o600 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/auth.json"), "{}", { mode: 0o600 });
    yield* fileSystem.writeFileString(join(home, ".omp/agent/models.yml"), Schema.encodeSync(Schema.fromJsonString(Schema.Json))(models), { mode: 0o600 });
    yield* fileSystem.makeDirectory(join(model, ".omp/agent"), { recursive: true, mode: 0o700 });
    const parentProfile = join(model, ".omp/agent/auth.json");
    yield* fileSystem.writeFileString(parentProfile, "ISOLATION_SECRET", { mode: 0o600 });
    const launcher = yield* setting("COORDINATOR_TEST_OMP", "/usr/local/bin/omp");
    const config: RunnerConfig = { id: RunnerId.make("isolation-probe"), brokerUrl: "http://127.0.0.1", tokenFile: token, controlTokenHash: yield* digest("control"),
      stateDirectory: join(directory, "state"), host: "127.0.0.1", port: 0, slots: 1, repositories: {}, launchers: { omp: launcher },
      modelDirectory: model, childDirectory: children, childUidBase: UserId.make(CHILD_UID_BASE) };
    yield* requireIsolation(config);
    const ownUid = UserId.make(CHILD_UID_BASE + 1), siblingUid = UserId.make(CHILD_UID_BASE + 2);
    const agentConfig = join(ownRoot, "agent-config.json"), endpointFile = join(ownRoot, "agent-endpoint.json"), agentTokenFile = join(ownRoot, "agent.token");
    const agentToken = yield* secret();
    yield* fileSystem.writeFileString(agentTokenFile, agentToken, { flag: "wx", mode: 0o600 });
    yield* fileSystem.writeFileString(agentConfig, Schema.encodeSync(Schema.fromJsonString(AgentConfig))({ host: AgentHostEnum.omp, endpointFile, tokenFile: agentTokenFile }), { flag: "wx", mode: 0o600 });
    yield* ownChildDirectory(ownRoot, ownUid); yield* ownChildDirectory(sibling, siblingUid);
    const agentScope = yield* Scope.fork(yield* Effect.scope);
    // Stock RPC stdio remains private; the extension owns its separate MCP endpoint.
    const agent = yield* connectAgentClient({
      host: AgentHostEnum.omp, command: "/usr/bin/env",
      args: [`--chdir=${own}`, "/usr/bin/setpriv", ...isolationArgs(ownUid), launcher, "--mode", "rpc", "--session-dir", sessions, "--no-extensions", "-e", resolve(import.meta.dirname, "../dist/plugins/agent-session.js")],
      cwd: directory, env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", HOME: home, COORDINATOR_AGENT_CONFIG: agentConfig }, connectTimeoutMs: 60000,
      plugin: { endpointFile, token: agentToken },
    }).pipe(Effect.provideService(Scope.Scope, agentScope));
    assert.ok(agent.pid > 0);
    const agentState = yield* agent.call(AgentTools.getState, { binding: agent.binding }, { timeoutMs: 60000 });
    assert.equal(agentState.cwd, own);
    const agentMessages = yield* agent.call(AgentTools.getMessages, { binding: agent.binding, limit: 256 }, { timeoutMs: 60000 });
    assert.deepEqual(agentMessages.messages, []);
    const quote = (path: string) => `'${path.replace(/'/g, "'\\''")}'`;
    // Effect cannot inspect another process's environ; /proc access itself is the isolation proof.
    const denied = [token, controlPath, ledger, parentProfile, join(sibling, "private"), `/proc/${process.pid}/environ`];
    const command = `set -eu; ${denied.map((path) => `if cat ${quote(path)} >/dev/null 2>&1; then exit 41; fi`).join("; ")}; ` +
      `test -z "\${GITHUB_PAT-}\${GITHUB_TOKEN-}\${GH_TOKEN-}\${OP_SERVICE_ACCOUNT_TOKEN-}\${COORDINATOR_CONFIG-}\${OPENAI_API_KEY-}\${ANTHROPIC_API_KEY-}\${GEMINI_API_KEY-}\${OMP_AUTH_BROKER_TOKEN-}"; ` +
      `test "$(id -u)" = "${CHILD_UID_BASE + 1}"; test "$PWD" = ${quote(own)}; test "$(stat -c %a "$HOME")" = 700; ` +
      `touch approved-write; test -r "$HOME/.omp/agent/config.yml"; test -r "$HOME/.omp/agent/auth.json"; test "$(stat -c %a "$HOME/.omp/agent/auth.json")" = 600; touch ${quote(join(sessions, "approved-session-write"))}; ` +
      `checked=0; while IFS=: read -r key value; do set -- $value; case "$key" in Groups) test "$#" = 0; checked=$((checked+1));; CapInh|CapPrm|CapEff|CapBnd|CapAmb) test "$1" = 0000000000000000; checked=$((checked+1));; NoNewPrivs) test "$1" = 1; checked=$((checked+1));; esac; done </proc/self/status; test "$checked" = 7; printf ISOLATION_OK`;
    const response = yield* agent.call(AgentTools.bash, { binding: agent.binding, command }, { timeoutMs: 60000 });
    assert.equal(response.exitCode, 0);
    assert.equal(response.killed, false);
    assert.ok(response.output.includes("ISOLATION_OK"));
    yield* Scope.close(agentScope, Exit.void);
    yield* agent.observedProcessClosed.pipe(Effect.timeout(10000));
    assert.equal(providerRequests, 0, "Isolation controls must not contact a model provider");
    yield* print(Formatter.formatJson({ scenario: "real-agent-os-isolation", childUid: CHILD_UID_BASE + 1, forbiddenPathsDenied: denied.length,
      ownWorkspaceAndSessionWritable: true, modelProfileReadable: true, privateAuthProfileReadable: true, privateHomeMode: "0700", parentProfileDenied: true, parentEnvironDenied: true, siblingUidDistinct: true,
      capabilitiesZero: true, supplementalGroupsEmpty: true, noNewPrivileges: true, agentStateAndMessagesUsable: true, forbiddenEnvironmentAbsent: true, sdkPid: agent.pid, physicalScopedCloseProved: true, modelProviderRequests: providerRequests }, { space: 2 }));
    return;
  }
const root = resolve(import.meta.dirname, "..");
const state = (yield* setting("COORDINATOR_SMOKE_DIRECTORY", "")) || (yield* fileSystem.makeTempDirectory({ prefix: "coordinator-sockets-" }));
yield* (fileSystem.makeDirectory(state, { recursive: true, mode: 0o711 })); yield* (fileSystem.chmod(state, 0o711));
const repo = Repo.make("github.com/samuelho-dev/coordinator");
const sha = Schema.decodeUnknownSync(Evidence.fields.sha, { onExcessProperty: "error" })((yield* exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root, env: { PATH: "/usr/local/bin:/usr/bin:/bin" }, extendEnv: false }))).trim());
const identity = yield* inspectGithubRepository(repo, yield* Config.String("GITHUB_READ_TOKEN"));
const coordinator = (yield* secret()), recipient = (yield* secret()), runnerToken = (yield* secret()), runnerControl = (yield* secret()), reviewer = (yield* secret()), writer = (yield* secret()), outsider = (yield* secret());
const tokenDirectory = join(state, "tokens"); yield* (fileSystem.makeDirectory(tokenDirectory, { mode: 0o700 }));
yield* (fileSystem.writeFileString(join(tokenDirectory, yield* digest("homelab-runner")), runnerControl, { mode: 0o600 }));
const runnerEnrollmentPath = join(tokenDirectory, "runner-enrollment.token"); yield* (fileSystem.writeFileString(runnerEnrollmentPath, runnerToken, { mode: 0o600 }));
const alternateToken = (yield* secret()), alternateControl = (yield* secret());
const alternateEnrollmentPath = join(tokenDirectory, "alternate-enrollment.token"); yield* (fileSystem.writeFileString(alternateEnrollmentPath, alternateToken, { mode: 0o600 }));
yield* (fileSystem.writeFileString(join(tokenDirectory, yield* digest("alternate-runner")), alternateControl, { mode: 0o600 }));
const port = () => Effect.scoped(Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer;
  const address = server.address;
  assert.equal(address._tag, "InetAddressV4");
  if (address._tag !== "InetAddressV4") return yield* Effect.fail(new CommandError({ message: "No TCP port" }));
  return address.port;
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0))));
const brokerPort = yield* port(), runnerPort = yield* port(), proxyPort = yield* port(), alternatePort = yield* port();
const brokerUrl = `http://127.0.0.1:${brokerPort}`, runnerUrl = `http://127.0.0.1:${runnerPort}`, proxyUrl = `http://127.0.0.1:${proxyPort}`, alternateUrl = `http://127.0.0.1:${alternatePort}`;
const brokerDatabaseUrl = yield* provisionPostgres();
const brokerConfig: BrokerConfig = { host: "127.0.0.1", port: brokerPort,
  backupDirectory: join(state, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000, githubTokenEnv: "GITHUB_READ_TOKEN",
  repositoryIds: { [repo]: identity.id },
  principals: [
    { id: Peer.fields.sender.make(RoleEnum.coordinator), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.coordinator], repos: [repo], tokenHash: (yield* digest(coordinator)) },
    { id: Peer.fields.sender.make("recipient"), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.session], repos: [repo], tokenHash: (yield* digest(recipient)) },
    { id: Peer.fields.sender.make("homelab-runner"), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.runner], repos: [repo], tokenHash: (yield* digest(runnerToken)), controlTokenHash: (yield* digest(runnerControl)), runnerUrl: proxyUrl },
    { id: Peer.fields.sender.make("alternate-runner"), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.runner], repos: [repo], tokenHash: (yield* digest(alternateToken)), controlTokenHash: (yield* digest(alternateControl)), runnerUrl: alternateUrl },
    { id: Peer.fields.sender.make(RoleEnum.reviewer), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.reviewer], repos: [repo], tokenHash: (yield* digest(reviewer)) },
    { id: Peer.fields.sender.make("writer"), owner: Peer.fields.owner.make("owner"), roles: [RoleEnum.archiveWriter], repos: [repo], tokenHash: (yield* digest(writer)) },
    { id: Peer.fields.sender.make("outsider"), owner: Peer.fields.owner.make("outsider"), roles: [RoleEnum.coordinator, RoleEnum.session], repos: [repo], tokenHash: (yield* digest(outsider)) },
  ] };
const launcher = yield* setting("COORDINATOR_TEST_OMP", "/home/admin/.bun/bin/omp");
const runnerDatabaseUrl = yield* provisionPostgres(), alternateDatabaseUrl = yield* provisionPostgres();
const runnerConfig: RunnerConfig = { id: RunnerId.make("homelab-runner"), brokerUrl, tokenFile: runnerEnrollmentPath, host: "127.0.0.1", port: runnerPort,
  stateDirectory: join(state, "runner"), slots: 1, repositories: { [repo]: root }, launchers: { omp: launcher }, controlTokenHash: (yield* digest(runnerControl)),
  modelDirectory: yield* setting("COORDINATOR_MODEL_DIRECTORY", "/work/home"), childDirectory: join(state, "children"), childUidBase: UserId.make(1000000) };
const brokerPath = join(state, "broker.json"), runnerPath = join(state, "runner.json");
yield* (fileSystem.writeFileString(brokerPath, Schema.encodeSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(brokerConfig), { mode: 0o600 })); yield* (fileSystem.writeFileString(runnerPath, Schema.encodeSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(runnerConfig), { mode: 0o600 }));
const alternateConfig: RunnerConfig = { ...runnerConfig, id: RunnerId.make("alternate-runner"), port: alternatePort, tokenFile: alternateEnrollmentPath, stateDirectory: join(state, "alternate-runner"), childDirectory: join(state, "alternate-children"), childUidBase: UserId.make(2000000), controlTokenHash: (yield* digest(alternateControl)) };
const alternatePath = join(state, "alternate.json"); yield* (fileSystem.writeFileString(alternatePath, Schema.encodeSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(alternateConfig), { mode: 0o600 }));
const transcript: JsonValue[] = [];
const evidence = Effect.fnUntraced(function*(scenario: string, observed: JsonValue) { const entry: JsonValue = { scenario, observed, time: new Date(yield* Clock.currentTimeMillis).toISOString() }; transcript.push(entry); yield* print(Formatter.formatJson(entry, { space: 2 })); });
const processFor = Effect.fnUntraced(function*(mode: "broker" | "runner", config: string, databaseUrl: Redacted.Redacted<string>) {
  const env = { ...(yield* childEnvironment(yield* setting("HOME", "/tmp"))), COORDINATOR_CONFIG: config, COORDINATOR_RUNNER_TOKEN_DIRECTORY: tokenDirectory, COORDINATOR_DATABASE_URL: Redacted.value(databaseUrl) };
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
const health = (url: string, token: string) => rpc(url, token, Commands.health, {}).pipe(Effect.map((response) => response.status === "ok"));
const ready = (url: string) => until(() => health(url, url === runnerUrl ? runnerToken : coordinator), Boolean, 30000);
let obstructJournal = false, acceptanceCommitted = false;
const proxyContext = yield* Layer.build(serverLayer("127.0.0.1", proxyPort));
const proxy = Context.get(proxyContext, HttpServer.HttpServer);
yield* serve(proxy, { target: "runner",
  authorize: (token) => token === runnerControl ? Effect.succeed(token) : Effect.fail(new HttpError({ status: 401, message: "Unauthorized" })),
  handle: (_principal, request) => RunnerRequest.match(request, {
    [Commands.journalTask.name]: ({ params }: typeof Commands.journalTask.request.Type) => obstructJournal
      ? Effect.fail(new HttpError({ status: 503, message: "Intentional journal outage" }))
      : rpc(runnerUrl, runnerControl, Commands.journalTask, params),
    [Commands.acceptTask.name]: ({ params }: typeof Commands.acceptTask.request.Type) => Effect.gen(function*() {
      const response = yield* rpc(runnerUrl, runnerControl, Commands.acceptTask, params);
      if (obstructJournal && response.accepted) { acceptanceCommitted = true; yield* Effect.sleep(20000); }
      return response;
    }),
    [Commands.inspectTask.name]: ({ params }: typeof Commands.inspectTask.request.Type) => rpc(runnerUrl, runnerControl, Commands.inspectTask, params),
    [Commands.commandTask.name]: ({ params }: typeof Commands.commandTask.request.Type) => rpc(runnerUrl, runnerControl, Commands.commandTask, params),
    [Commands.prepareTask.name]: ({ params }: typeof Commands.prepareTask.request.Type) => rpc(runnerUrl, runnerControl, Commands.prepareTask, params),
    [Commands.releaseTask.name]: ({ params }: typeof Commands.releaseTask.request.Type) => rpc(runnerUrl, runnerControl, Commands.releaseTask, params),
  }),
});
let broker = yield* processFor("broker", brokerPath, brokerDatabaseUrl); let runner: ChildProcessSpawner.ChildProcessHandle | undefined, alternate: ChildProcessSpawner.ChildProcessHandle | undefined;
let workerPid: NonNullable<typeof RunnerTask.fields.pid.Type> | undefined, uncertainPid: NonNullable<typeof RunnerTask.fields.pid.Type> | undefined;
yield* Effect.addFinalizer(() => Effect.gen(function*() {
  for (const child of [alternate, runner, broker]) if (child) yield* exit(child, "SIGTERM").pipe(Effect.ignore);
  // Effect cannot signal arbitrary PIDs recovered from a different supervisor's journal.
  for (const pid of [workerPid, uncertainPid]) if (pid) yield* Effect.try(() => process.kill(pid, "SIGTERM")).pipe(Effect.ignore);
  if (!(yield* broker.isRunning)) yield* dropPostgres(brokerDatabaseUrl).pipe(Effect.orDie);
  for (const url of [runnerDatabaseUrl, alternateDatabaseUrl]) yield* dropPostgres(url).pipe(Effect.orDie);
}).pipe(Effect.orDie));
  yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, (yield* secret()), Commands.getTask, { taskId: TaskId.make("missing") }), 401);
  const session = { sessionId: SessionId.make("agent-owner-session"), incarnation: RunnerTask.fields.incarnation.make(yield* uuid), machineId: MachineId.make("homelab"), host: AgentHostEnum.omp, repo, workerId: WorkerId.make("existing-omp-smoke-worker") };
  yield* rpc(brokerUrl, recipient, Commands.heartbeatSession, session); yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session));
  const work: Work = { taskId: TaskId.make(`smoke-${(yield* uuid)}`), repo, ref: "refs/heads/main", sha, launcher: AgentHostEnum.omp, prompt: "Respond exactly COORDINATOR_SMOKE_OK. Do not call tools, change files, merge or deploy.", scope: "Socket lifecycle smoke only", restrictions: "No file modifications, merging or deployment", expectedEvidence: ["one owned real omp child", "exact-SHA result"], deadline: (yield* Clock.currentTimeMillis) + 600000,
    workerId: session.workerId, githubRepoId: identity.id, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
  yield* rejects(rpc(brokerUrl, writer, Commands.submitTask, work), 403);
  yield* evidence("authentication-and-operation-authorization", { invalidCredential: 401, archiveWriterCannotDispatch: 403 });
  yield* rejects(rpc(brokerUrl, coordinator, Commands.submitTask, { ...work, taskId: TaskId.make("wrong-enrolled-repository"), githubRepoId: GitHubRepositoryId.make(identity.id + 1) }), 409);
  yield* evidence("inspected-enrollment-without-CI-credential", { repositoryId: identity.id, mismatchedIdRejected: true, broadGithubCredentialNotInherited: true });
  const peerTask = yield* rpc(brokerUrl, coordinator, Commands.submitTask, work);
  assert.equal(peerTask.offerDecision, OfferDecisionEnum.pending); assert.equal(peerTask.runnerId, undefined);
  assert.ok(peerTask.offerDecision !== undefined);
  yield* evidence("peer-first-owner-offer", { taskId: work.taskId, state: peerTask.state, decision: peerTask.offerDecision, runnerAssigned: false });
  const message = { messageId: PeerId.make(yield* uuid), ...sessionKey(session), payload: { note: "addressed owner notification" }, ttlMs: 60000 };
  yield* rpc(brokerUrl, coordinator, Commands.sendPeer, message);
  const before = yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session)); assert.equal(before.messages.length, 1);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath, brokerDatabaseUrl); yield* ready(brokerUrl);
  yield* rejects(rpc(brokerUrl, coordinator, Commands.sendPeer, { ...message, messageId: PeerId.make(yield* uuid) }), 409);
  yield* rpc(brokerUrl, recipient, Commands.heartbeatSession, session);
  const replay = yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session)); assert.deepEqual(replay.messages, before.messages);
  yield* rpc(brokerUrl, recipient, Commands.acknowledgePeer, { ...sessionKey(session), messageId: message.messageId });
  assert.equal((yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session))).messages.length, 0);
  yield* evidence("broker-crash-same-incarnation-replay", { replayed: 1, newSendBeforeHeartbeatRejected: true, handledAckDeletes: true });
  yield* rpc(brokerUrl, coordinator, Commands.sendPeer, { ...message, messageId: PeerId.make("expires"), ttlMs: 100 }); yield* Effect.sleep(150);
  assert.equal((yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session))).messages.length, 0);
  const address: Address = { messageId: PeerId.make(work.taskId), revision: 1, workerId: work.workerId, ...sessionKey(session), repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  yield* rpc(brokerUrl, recipient, Commands.replyPeer, { taskId: work.taskId, ...sessionKey(session), decision: OfferDecisionEnum.accepted });
  yield* rpc(brokerUrl, coordinator, Commands.addressTask, { taskId: work.taskId, address });
  yield* rejects(rpc(brokerUrl, recipient, Commands.readbackTask, { taskId: work.taskId, address: { ...address, ref: "wrong-ref" }, disposition: DispositionEnum.ready }), 409);
  yield* rejects(rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, Commands.readbackTask, { taskId: work.taskId, address, disposition: DispositionEnum.hold, reason: "yield* correction", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
  yield* rejects(rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 1 }), 409);
  yield* rpc(brokerUrl, recipient, Commands.readbackTask, { taskId: work.taskId, address, disposition: DispositionEnum.ready }); yield* rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 1 });
  const second = { ...session, sessionId: SessionId.make("handoff-session"), incarnation: RunnerTask.fields.incarnation.make(yield* uuid) }; yield* rpc(brokerUrl, recipient, Commands.heartbeatSession, second); yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(second));
  yield* rejects(rpc(brokerUrl, coordinator, Commands.handoffTask, { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, Commands.reportTask, { taskId: work.taskId, ...sessionKey(session), state: TaskStateEnum.SUCCEEDED, result: { syntheticPeerGate: true } });
  yield* rpc(brokerUrl, coordinator, Commands.handoffTask, { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 });
  yield* rejects(rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 2 }), 409);
  const handoffAddress = { ...address, ...sessionKey(second), revision: 2 };
  yield* rpc(brokerUrl, recipient, Commands.readbackTask, { taskId: work.taskId, address: handoffAddress, disposition: DispositionEnum.ready }); yield* rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 2 });
  yield* evidence("expiry-correction-hold-and-handoff-gate", { expiredPeerNotDelivered: true, wrongRefRejected: true, heldConfirmationRejected: true, handoffRequiresNewReadback: true });
  const instructionAddress = { ...handoffAddress, messageId: PeerId.make("later-owned-message"), revision: 3, scope: "A separately approved later instruction" };
  yield* rpc(brokerUrl, coordinator, Commands.instructTask, { taskId: work.taskId, address: instructionAddress, prompt: "Later approved instruction; do not repeat the launch prompt", approved: true });
  assert.equal((yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(second))).releases.length, 0);
  yield* rejects(rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 2 }), 409);
  yield* rpc(brokerUrl, recipient, Commands.readbackTask, { taskId: work.taskId, address: instructionAddress, disposition: DispositionEnum.ready });
  yield* rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: work.taskId, revision: 3 });
  const later = (yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(second))).releases[0]!;
  assert.equal(later.instruction?.prompt, "Later approved instruction; do not repeat the launch prompt");
  yield* rpc(brokerUrl, recipient, Commands.reportTask, { taskId: work.taskId, ...sessionKey(second), state: TaskStateEnum.SUCCEEDED, result: { syntheticPeerGate: true } });
  yield* evidence("later-agent-instruction-gate", { sameTaskId: later.taskId, previousRevisionCannotRelease: true, newReadbackRequired: true, approvedPromptDistinct: true });
  yield* rpc(brokerUrl, recipient, Commands.consentArchive, { sessionId: session.sessionId, participants: [Peer.fields.sender.make("recipient")], enabled: true });
  const payload: CommandInput<typeof Commands.appendArchive>["payload"] = { id: Archive.fields.entry.make("entry-one"), parentId: null, type: "message", contentOmitted: true };
  const archive = { sessionId: session.sessionId, entryId: Archive.fields.entry.make("entry-one"), digest: (yield* fingerprint(payload)), payload, gaps: ["content-omitted", "unlinked"] };
  yield* rpc(brokerUrl, writer, Commands.appendArchive, archive); yield* rpc(brokerUrl, writer, Commands.appendArchive, archive);
  const unsafePayload = { ...payload, text: "unsafe-raw-content-must-not-persist" };
  // Send malformed arguments directly to MCP: the typed client deliberately rejects them before transport.
  const archiveId = Commands.appendArchive.name;
  const archiveMethod = Commands.appendArchive.name;
  const unsafeResponse = yield* client.execute(HttpClientRequest.post(new URL("/mcp", brokerUrl)).pipe(
    HttpClientRequest.setHeaders({ authorization: `Bearer ${writer}`, accept: "application/json, text/event-stream",
      "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": archiveMethod }),
    HttpClientRequest.bodyText(Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ jsonrpc: "2.0", id: "unsafe-archive", method: "tools/call",
      params: { name: archiveMethod, arguments: { ...archive, entryId: "unsafe", payload: unsafePayload, digest: yield* fingerprint(unsafePayload) },
        _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} } } }), "application/json"),
  ));
  assert.equal(unsafeResponse.status, 200);
  const unsafeText = yield* unsafeResponse.text;
  const unsafeMessage = Schema.decodeSync(Schema.fromJsonString(Json), { onExcessProperty: "error" })(unsafeResponse.headers["content-type"]?.includes("text/event-stream")
    ? unsafeText.split(/\r?\n/).filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n")
    : unsafeText);
  assert.ok(typeof unsafeMessage === "object" && unsafeMessage !== null && "result" in unsafeMessage);
  const unsafeResult = Schema.decodeUnknownSync(Json, { onExcessProperty: "error" })(unsafeMessage.result);
  assert.ok(typeof unsafeResult === "object" && unsafeResult !== null && "isError" in unsafeResult);
  assert.equal(unsafeResult.isError, true);
  const archived = yield* rpc(brokerUrl, coordinator, Commands.listArchive, { sessionId: session.sessionId });
  assert.equal(archived.length, 1);
  assert.equal(Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(archived).includes(unsafePayload.text), false);
  assert.deepEqual(yield* postgresRead(brokerDatabaseUrl, (db) => db.query((client) => client.selectFrom("archive").select("sequence").where("payload", "like", `%${unsafePayload.text}%`))), []);
  yield* rejects(rpc(brokerUrl, writer, Commands.deleteArchive, { sessionId: session.sessionId }), 403);
  assert.deepEqual(yield* rpc(brokerUrl, outsider, Commands.listArchive, { sessionId: session.sessionId }), []);
  const snapshotNames = new Set(yield* (fileSystem.readDirectory(brokerConfig.backupDirectory)));
  const snapshotName = yield* until(() => fileSystem.readDirectory(brokerConfig.backupDirectory).pipe(Effect.map((names) => names.find((name) => name.endsWith(".pgdump") && !snapshotNames.has(name)))), (name) => Boolean(name), 75000);
  const snapshot = join(brokerConfig.backupDirectory, snapshotName!);
  const snapshotDatabaseUrl = yield* provisionPostgres();
  yield* restoreBrokerDatabase(snapshot, true).pipe(Effect.provide(postgresConfig(snapshotDatabaseUrl)));
  for (const table of Array.of<"archive" | "consents" | "participant_consents">("archive", "consents", "participant_consents")) assert.deepEqual(yield* postgresRead(snapshotDatabaseUrl, (db) => db.query((client) => client.selectFrom(table).select("owner").limit(1))), []);
  assert.ok((yield* postgresRead(snapshotDatabaseUrl, (db) => db.query((client) => client.selectFrom("tasks").select("id").where("id", "=", work.taskId))))[0]);
  yield* dropPostgres(snapshotDatabaseUrl);
  assert.equal((yield* rpc(brokerUrl, coordinator, Commands.listArchive, { sessionId: session.sessionId })).length, 1);
  yield* rejects(rpc(brokerUrl, outsider, Commands.consentParticipant, { owner: Peer.fields.owner.make("owner"), sessionId: session.sessionId, enabled: false }), 403);
  yield* rpc(brokerUrl, outsider, Commands.consentParticipant, { owner: Peer.fields.owner.make("owner"), sessionId: session.sessionId, enabled: true });
  yield* rpc(brokerUrl, recipient, Commands.consentArchive, { sessionId: session.sessionId, participants: [Peer.fields.sender.make("recipient"), Peer.fields.sender.make("outsider")], enabled: true });
  yield* rpc(brokerUrl, outsider, Commands.consentParticipant, { owner: Peer.fields.owner.make("owner"), sessionId: session.sessionId, enabled: false });
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, Commands.listArchive, { sessionId: session.sessionId }), []);
  yield* rpc(brokerUrl, recipient, Commands.heartbeatSession, session);
  assert.equal((yield* rpc(brokerUrl, recipient, Commands.pollSession, sessionKey(session))).archiveEnabled, false);
  yield* rpc(brokerUrl, recipient, Commands.consentArchive, { sessionId: session.sessionId, participants: [Peer.fields.sender.make("recipient")], enabled: true });
  yield* rpc(brokerUrl, writer, Commands.appendArchive, archive);
  yield* rpc(brokerUrl, coordinator, Commands.deleteArchive, { sessionId: session.sessionId });
  yield* rejects(rpc(brokerUrl, writer, Commands.appendArchive, archive), 403);
  yield* exit(broker, "SIGTERM"); broker = yield* processFor("broker", brokerPath, brokerDatabaseUrl); yield* ready(brokerUrl);
  assert.deepEqual(yield* rpc(brokerUrl, coordinator, Commands.listArchive, { sessionId: session.sessionId }), []);
  yield* evidence("archive-privacy-idempotency-revocation-and-restart", { uniqueEntries: 1, appendOnlyCannotDelete: true, otherOwnerSeesNoRows: true, optoutRejectsAppendAfterRestart: true, snapshotsExcludeArchiveAndConsent: true });
  runner = yield* processFor("runner", runnerPath, runnerDatabaseUrl); yield* ready(runnerUrl); yield* Effect.sleep(300);
  const worker = { ...work, taskId: TaskId.make(`worker-${(yield* uuid)}`), bypassPeer: true };
  const submitted = yield* rpc(brokerUrl, coordinator, Commands.submitTask, worker); const duplicate = yield* rpc(brokerUrl, coordinator, Commands.submitTask, worker); assert.equal(submitted.taskId, duplicate.taskId);
  yield* rejects(rpc(brokerUrl, coordinator, Commands.submitTask, { ...worker, prompt: "different" }), 409);
  const accepted = yield* until(() => rpc(brokerUrl, coordinator, Commands.getTask, { taskId: worker.taskId }), (task) => Boolean(task.readback), 120000);
  assert.equal(accepted.address?.sha, sha); assert.equal(accepted.runnerId, "homelab-runner");
  const row = (yield* postgresRead(runnerDatabaseUrl, (db) => db.query((client) => client.selectFrom("runner_tasks").select("pid").where("taskId", "=", worker.taskId))))[0]; workerPid = Schema.decodeUnknownSync(RunnerTask.fields.pid.members[0], { onExcessProperty: "error" })(row?.pid);
  assert.ok(workerPid > 0);
  yield* rejects(rpc(runnerUrl, runnerToken, Commands.acceptTask, worker), 401);
  assert.equal((yield* rpc(runnerUrl, runnerControl, Commands.acceptTask, worker)).taskId, worker.taskId);
  const corrected = { ...accepted.address!, revision: 2 };
  const correctedTask = yield* rpc(brokerUrl, coordinator, Commands.addressTask, { taskId: worker.taskId, address: corrected });
  assert.deepEqual(correctedTask.readback, corrected);
  yield* rejects(rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: worker.taskId, revision: 1 }), 409);
  const full = yield* rpc(runnerUrl, runnerControl, Commands.acceptTask, { ...worker, taskId: TaskId.make("full-capacity") }); assert.equal(full.accepted, false); assert.equal(full.reason, "full");
  yield* rejects(rpc(brokerUrl, outsider, Commands.getTask, { taskId: worker.taskId }), 403);
  yield* rejects(rpc(brokerUrl, writer, Commands.attachTask, { taskId: worker.taskId }), 403);
  yield* exit(broker, "SIGKILL"); broker = yield* processFor("broker", brokerPath, brokerDatabaseUrl); yield* ready(brokerUrl);
  const attached = yield* rpc(brokerUrl, coordinator, Commands.attachTask, { taskId: worker.taskId }); assert.equal(attached.taskId, worker.taskId); // Effect has no arbitrary-PID liveness API; this PID is owned by the runner process.
  yield* Effect.try(() => process.kill(workerPid!, 0));
  const controlledState = yield* rpc(brokerUrl, coordinator, Commands.controlTask, { taskId: worker.taskId, type: RunnerControlEnum.getState });
  assert.ok("isStreaming" in controlledState, "State control returns the canonical state projection");
  assert.equal(controlledState.binding.sessionId, accepted.address!.sessionId);
  assert.equal(controlledState.isStreaming, false);
  assert.equal(controlledState.currentRunId, null);
  const historyMessages: Schema.Json[] = [];
  let historyCursor: string | undefined;
  for (;;) {
    const history = yield* rpc(brokerUrl, coordinator, Commands.controlTask, { taskId: worker.taskId, type: RunnerControlEnum.getMessages, ...(historyCursor ? { cursor: historyCursor } : {}), limit: 256 });
    assert.ok("messages" in history, "Message control returns the canonical message page");
    assert.deepEqual(history.binding, controlledState.binding);
    historyMessages.push(...history.messages);
    if (!history.more) break;
    assert.ok(history.nextCursor && history.nextCursor !== historyCursor);
    historyCursor = history.nextCursor;
  }
  assert.equal(Schema.encodeSync(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" })(historyMessages).includes(worker.prompt), false);
  yield* rpc(brokerUrl, coordinator, Commands.confirmTask, { taskId: worker.taskId, revision: 2 });
  const finished = yield* until(() => rpc(brokerUrl, coordinator, Commands.getTask, { taskId: worker.taskId }), (task) => [TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED, TaskStateEnum.HELD].includes(task.state), 180000);
  assert.equal(finished.state, TaskStateEnum.SUCCEEDED, `Real host final state ${finished.state}: ${Formatter.formatJson(finished.result ?? null, { space: 2 })}`);
  const one = yield* postgresRead(runnerDatabaseUrl, (db) => db.query((client) => client.selectFrom("runner_tasks").select("taskId").where("taskId", "=", worker.taskId)));
  const settledEvents = yield* runnerEvents(runnerDatabaseUrl, worker.taskId);
  assert.equal(one.length, 1); assert.equal(settledEvents.filter((event) => event.state === RunnerStateEnum.LAUNCHING).length, 1);
  assert.equal(Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(settledEvents).includes("COORDINATOR_SMOKE_OK"), false);
  yield* evidence("real-owned-omp-child-duplicate-full-and-coordinator-exit", { sha, workerPid, agentSession: accepted.address!.sessionId, duplicateConflict: true, fullSlotsRejectedWithoutAcceptance: true, brokerCrashAttachPreserved: true, finalState: finished.state, acceptanceRows: 1, launchEvents: 1, evidenceVerified: false });
  yield* rpc(brokerUrl, coordinator, Commands.recordLocalEvidence, { taskId: worker.taskId, sha, commands: ["real socket smoke"], passed: true });
  yield* rpc(brokerUrl, coordinator, Commands.recordDeploymentEvidence, { taskId: worker.taskId, sha: Evidence.fields.sha.make("a".repeat(40)), environment: "independent-demo-environment", url: "https://example.invalid", observedState: "not deployed", provenance: "smoke distinct negative observation" });
  yield* rejects(rpc(brokerUrl, coordinator, Commands.recordReviewEvidence, { taskId: worker.taskId, sha, decision: ReviewDecisionEnum.approved, note: "unauthorized" }), 403);
  yield* rpc(brokerUrl, reviewer, Commands.recordReviewEvidence, { taskId: worker.taskId, sha, decision: ReviewDecisionEnum.changesRequested, note: "Smoke credential provenance; not user approval" });
  yield* rpc(brokerUrl, coordinator, Commands.targetEvidence, { taskId: worker.taskId, sha: Evidence.fields.sha.make("b".repeat(40)), ref: "main", requiredLanes: ["CI/verify"] });
  const observed = yield* rpc(brokerUrl, coordinator, Commands.getTask, { taskId: worker.taskId }); assert.equal(observed.state, TaskStateEnum.SUCCEEDED); assert.ok(observed.evidence); assert.ok(observed.evidence.every((item) => item.stale));
  yield* evidence("independent-evidence-and-new-SHA-staleness", { runnerStateUnchanged: observed.state, oldLocalCiDeploymentReviewStale: true, reviewerAuthorizationSeparate: true });
  yield* until(() => postgresRead(runnerDatabaseUrl, (db) => db.query((client) => client.selectFrom("runner_tasks").select("occupied"))).pipe(Effect.map((rows) => rows.reduce((total, row) => total + row.occupied, 0))), (occupied) => occupied === 0, 30000);
  yield* Effect.sleep(5500);
  obstructJournal = true;
  const uncertainWork = { ...worker, taskId: TaskId.make(`uncertain-${(yield* uuid)}`) };
  yield* rpc(brokerUrl, coordinator, Commands.submitTask, uncertainWork);
  yield* until(() => Effect.sync(() => acceptanceCommitted), Boolean, 30000);
  alternate = yield* processFor("runner", alternatePath, alternateDatabaseUrl);
  yield* until(() => health(alternateUrl, alternateToken), Boolean, 30000);
  const ambiguous = yield* until(() => rpc(brokerUrl, coordinator, Commands.getTask, { taskId: uncertainWork.taskId }), (task) => task.state === TaskStateEnum.HELD, 45000);
  assert.equal(ambiguous.runnerId, "homelab-runner");
  yield* Effect.sleep(1500);
  assert.deepEqual(yield* postgresRead(alternateDatabaseUrl, (db) => db.query((client) => client.selectFrom("runner_tasks").select("taskId").where("taskId", "=", uncertainWork.taskId))), []);
  obstructJournal = false;
  const reconciled = yield* until(() => rpc(brokerUrl, coordinator, Commands.getTask, { taskId: uncertainWork.taskId }), (task) => task.state === TaskStateEnum.ACCEPTED && Boolean(task.readback), 90000);
  assert.equal(reconciled.runnerId, "homelab-runner");
  uncertainPid = Schema.decodeUnknownSync(RunnerTask.fields.pid.members[0], { onExcessProperty: "error" })((yield* postgresRead(runnerDatabaseUrl, (db) => db.query((client) => client.selectFrom("runner_tasks").select("pid").where("taskId", "=", uncertainWork.taskId))))[0]?.pid); assert.ok(uncertainPid > 0);
  const acceptedEvents = yield* runnerEvents(runnerDatabaseUrl, uncertainWork.taskId);
  assert.equal(acceptedEvents.filter((event) => event.state === RunnerStateEnum.LAUNCHING).length, 1);
  assert.equal(acceptedEvents.filter((event) => event.type === "prompt_intent").length, 0);
  yield* exit(runner!, "SIGKILL"); runner = yield* processFor("runner", runnerPath, runnerDatabaseUrl); yield* ready(runnerUrl);
  const recovered = yield* rpc(runnerUrl, runnerControl, Commands.journalTask, { taskId: uncertainWork.taskId }); assert.equal(recovered.state, RunnerStateEnum.HELD);
  assert.equal((yield* runnerEvents(runnerDatabaseUrl, uncertainWork.taskId)).filter((event) => event.state === RunnerStateEnum.LAUNCHING).length, 1);
  yield* evidence("ambiguous-socket-acceptance-and-runner-crash", { acceptanceCommittedBeforeReplyTimeout: true, journalOutageHeldSameRunner: true, freeAlternateHadNoAcceptance: true, reconciledOriginalAcceptance: true, launchEvents: 1, promptIntents: 0, restartState: recovered.state, automaticRelaunch: false });
  yield* (fileSystem.writeFileString(join(state, "transcript.json"), Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ brokerRuntime: process.version, host: (yield* exec(ChildProcess.make(launcher, ["--version"], { env: yield* childEnvironment(yield* setting("HOME", "/tmp")), extendEnv: false }))).trim(), privateRepository: identity.private, repositoryId: identity.id, sha, scenarios: transcript }), { mode: 0o600 }));
  yield* print(`Evidence: ${join(state, "transcript.json")}`);
 });
const runIsolationSmoke = () => runSmoke({ "--runner-isolation-only": true });
const program = Effect.gen(function*() {
  const args = [...(yield* (yield* Stdio.Stdio).args)];
  if (args.length === 1 && args[0] === "--runner-isolation-only") {
    return yield* isolationSmokeCommand.executeDecoded({ runIsolationSmoke }, {});
  }
  return yield* smokeCommand.execute({ runSmoke }, Object.fromEntries(args.map(flag => [flag, true])));
});
NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer))));
function sessionKey(session: SessionKey): SessionKey { return { sessionId: session.sessionId, incarnation: session.incarnation }; }
