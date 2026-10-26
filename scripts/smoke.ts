import { Clock, Config, Console, Context, Crypto, Deferred, Effect, Fiber, FileSystem, Layer, Option, Path, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { Hex } from "effect/encoding";
import { Host, serverLayer } from "../src/platform.ts";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { NodeHttpClient, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Inspector } from "../src/inspector.ts";
import { ConnectorAccepted, ConnectorAnnounce, InspectionTools, InspectorConfig, SessionPresence, createInspection } from "../src/inspection.ts";
import { Mcp } from "../src/mcp.ts";
import { connectNative, withProcessResources } from "../src/native.ts";
import { identifyProcess } from "../src/resources.ts";

const verify = {
  equal: (actual: unknown, expected: unknown, message = "Smoke equality failed") => Object.is(actual, expected) ? Effect.void : Effect.die(new Error(message)),
  notEqual: (actual: unknown, expected: unknown, message = "Smoke inequality failed") => !Object.is(actual, expected) ? Effect.void : Effect.die(message),
  deepEqual: (actual: unknown, expected: unknown, message = "Smoke structure mismatch") => JSON.stringify(actual) === JSON.stringify(expected) ? Effect.void : Effect.die(message),
  ok: (condition: unknown, message = "Smoke condition failed") => condition ? Effect.void : Effect.die(message),
  match: (value: string, pattern: RegExp) => pattern.test(value) ? Effect.void : Effect.die("Smoke pattern mismatch"),
  fail: (message: string) => Effect.die(message),
};
const CoordinatorListMessage = Schema.Struct({
  role: Schema.Literal("custom"), customType: Schema.Literal("coordinator-list"), content: Schema.String,
  details: Schema.Struct({ sessions: Schema.Array(SessionPresence) }),
});
const NativeStartupMessage = Schema.fromJsonString(Schema.Union([
  Schema.Struct({ id: Schema.Literal("coordinator-startup"), type: Schema.Literal("response"), command: Schema.Literal("get_state"), success: Schema.Boolean }),
  Schema.Struct({ id: Schema.Literal("coordinator-list"), type: Schema.Literal("response"), success: Schema.Boolean }),
  Schema.Struct({ id: Schema.Literal("coordinator-list"), type: Schema.Literal("prompt_result"), status: Schema.String, agentInvoked: Schema.Boolean }),
  Schema.Struct({
    id: Schema.Literal("coordinator-list-messages"), type: Schema.Literal("response"), success: Schema.Boolean,
    data: Schema.Struct({ messages: Schema.Array(Schema.Unknown) }),
  }),
  Schema.Struct({ type: Schema.Literal("agent_start") }),
  Schema.Struct({ type: Schema.Literal("ready") }),
  Schema.Struct({ type: Schema.Literal("extension_ui_request"), method: Schema.Literal("notify"), message: Schema.String }),
]));


const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const host = yield* Host;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const { join, resolve, dirname } = path;
  const scriptDirectory = dirname(yield* path.fromFileUrl(new URL(import.meta.url)));
  const searchPath = yield* Config.String("PATH").pipe(Config.withDefault("/usr/bin:/bin"));
  const mcp = yield* Mcp;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "inspector-smoke-" });
  yield* fs.chmod(root, 0o700);
  const token = yield* crypto.randomUUIDv4(), siblingToken = yield* crypto.randomUUIDv4(), outsiderToken = yield* crypto.randomUUIDv4();
  const machineId = `smoke-${yield* crypto.randomUUIDv4()}`;
  const principal = (id: string, owner: string, credential: string) => crypto.digest("SHA-256", new TextEncoder().encode(credential)).pipe(Effect.map((digest) => ({ id, owner, tokenHash: Hex.encode(digest) })));
  const inspector = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [
    yield* principal("local", "smoke-owner", token),
    yield* principal("sibling", "smoke-owner", siblingToken),
    yield* principal("outsider", "other-owner", outsiderToken),
  ] });
  const server = Context.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  let blockedStartup: {
    readonly machineId: string;
    readonly entered: Deferred.Deferred<void>;
    readonly resume: Deferred.Deferred<void>;
    readonly finished: Deferred.Deferred<void>;
  } | undefined;
  yield* mcp.serve(server, {
    ...inspector,
    announce: (principal, input) => {
      const gate = blockedStartup;
      if (!gate || input.machineId !== gate.machineId) return inspector.announce(principal, input);
      return Effect.gen(function*() {
        yield* Deferred.succeed(gate.entered, undefined);
        yield* Effect.interruptible(Deferred.await(gate.resume)).pipe(Effect.ensuring(Deferred.succeed(gate.finished, undefined)));
        return yield* inspector.announce(principal, input);
      });
    },
  });
  const inspectorUrl = HttpServer.formatAddress(server.address);
  for (const credential of ["", yield* crypto.randomUUIDv4()]) {
    yield* mcp.call(inspectorUrl, credential, InspectionTools.list, { limit: 1 }).pipe(Effect.matchEffect({
      onFailure: (error) => error._tag === "HttpError"
        ? verify.equal(error.status, credential === "" ? 400 : 401, "Empty credentials fail local validation; invalid bearer fails HTTP authentication")
        : Effect.fail(error),
      onSuccess: () => verify.fail("Unauthenticated MCP accepted"),
    }));
  }
  for (const credential of [undefined, yield* crypto.randomUUIDv4(), token]) {
    const ready = yield* Effect.scoped(Effect.gen(function*() {
      const request = HttpClientRequest.get(new URL("/readyz", inspectorUrl));
      const response = yield* HttpClient.execute(credential === undefined ? request : HttpClientRequest.setHeader(request, "authorization", `Bearer ${credential}`));
      yield* response.text;
      return { status: response.status, sha: response.headers["x-coordinator-source-sha"] };
    }));
    yield* verify.equal(ready.status, credential === token ? 200 : 401, "Readiness requires an authorized owner key");
    const expectedSource = Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_EXPECTED_SOURCE_SHA"))) ?? Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_IMAGE_SHA")));
    if (credential === token && expectedSource !== undefined) yield* verify.equal(ready.sha, expectedSource);
  }
  const tokenFile = join(root, "token");
  yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
  const identity = yield* identifyProcess(host.pid);
  if (identity.status !== "available") return yield* Effect.die("Process identity unavailable");
  const presence: SessionPresence = {
    ref: { machineId, harness: "pi", nativeSessionId: `fixture-${yield* crypto.randomUUIDv4()}` }, observedAt: yield* Clock.currentTimeMillis,
    processes: [{ pid: host.pid, startIdentity: identity.startTime, ownership: "owned" }],
    activity: { state: "idle" },
  };
  let detailRequests = 0;
  yield* connectNative({ inspectorUrl, tokenFile, machineId, stateDirectory: root }, {
    presence: () => Effect.succeed([presence]),
    inspect: () => Effect.gen(function*() {
      detailRequests++;
      return yield* withProcessResources(createInspection(presence, (yield* Clock.currentTimeMillis)));
    }),
  }).pipe(Effect.forkScoped);
  const waitForSession = (harness: "pi" | "omp" | "hermes", sessionId?: string, installationId = machineId) => Effect.gen(function*() {
    for (let attempt = 0; attempt < 300; attempt++) {
      const result = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId: installationId, harness, limit: 100 });
      const found = result.sessions.find((session) => sessionId === undefined ? session.ref.nativeSessionId !== presence.ref.nativeSessionId : session.ref.nativeSessionId === sessionId);
      if (found !== undefined) return found;
      yield* Effect.sleep(100);
    }
    return yield* Effect.die(`Disposable ${harness} connector presence deadline expired`);
  });
  yield* waitForSession("pi", presence.ref.nativeSessionId);
  yield* verify.equal(detailRequests, 0, "Presence/list must not sample native details or process resources");
  const sameOwner = yield* mcp.call(inspectorUrl, siblingToken, InspectionTools.list, { machineId, limit: 100 });
  yield* verify.equal(sameOwner.sessions.length, 1);
  yield* verify.equal((yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.list, { limit: 100 })).sessions.length, 0);
  yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.inspect, { ref: presence.ref }).pipe(Effect.matchEffect({
    onFailure: (error) => error._tag === "HttpError"
      ? verify.ok(error.status === 403 || error.status === 404, "Expected HTTP cross-owner denial")
      : Effect.fail(error),
    onSuccess: () => verify.fail("Cross-owner inspection accepted"),
  }));
  yield* mcp.post(inspectorUrl, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted,
    { connectorId: yield* crypto.randomUUIDv4(), incarnation: yield* crypto.randomUUIDv4(), machineId: "mismatched-machine", sessions: [presence] }).pipe(Effect.matchEffect({
      onFailure: (error) => error._tag === "HttpError"
        ? verify.equal(error.status, 403, "Expected session machine fence")
        : Effect.fail(error),
      onSuccess: () => verify.fail("Mixed-machine publication accepted"),
    }));
  const observation = yield* mcp.call(inspectorUrl, siblingToken, InspectionTools.inspect, { ref: presence.ref });
  yield* verify.equal(detailRequests, 1);
  const processCount = observation.resources.ownedProcessCount;
  yield* verify.equal(processCount.status, "available", processCount.status === "unavailable"
    ? `Native owned-process count unavailable: ${processCount.reason}; partial=${processCount.provenance.partial}; shared=${processCount.provenance.shared}`
    : "Expected an available owned-process count");
  yield* verify.equal(observation.resources.ownedRssBytes.status, "available");
  yield* verify.equal(observation.resources.ownedCpuPercent.status, "available");
  if (observation.resources.ownedRssBytes.status === "available") yield* verify.ok(observation.resources.ownedRssBytes.value > 0);
  yield* verify.equal(observation.context.contextUsedTokens.status, "unavailable");
  yield* verify.equal(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
  yield* verify.equal(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  const nativeHarnesses = yield* Config.Boolean("COORDINATOR_SMOKE_NATIVE").pipe(Config.withDefault(true));
  const nativeResults: Array<{ harness: string; status: string }> = [];
  // Synthetic native envelopes exercise the built CLI, not a live Hermes provider request.
  const hermesHome = join(root, "hermes space's");
  yield* fs.makeDirectory(hermesHome, { mode: 0o700 });
  const repositoryRoot = join(hermesHome, "repository");
  const worktreeRoot = join(hermesHome, "linked workspace");
  yield* fs.makeDirectory(repositoryRoot, { mode: 0o700 });
  const git = (args: ReadonlyArray<string>) => Effect.gen(function*() {
    const child = yield* spawner.spawn(ChildProcess.make("git", args, {
      cwd: repositoryRoot, env: { HOME: hermesHome, PATH: searchPath, GIT_CONFIG_NOSYSTEM: "1" },
      extendEnv: false, stdin: Stream.empty, forceKillAfter: "1 second",
    }));
    const output = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const errors = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    yield* verify.equal(yield* child.exitCode, 0, yield* Fiber.join(errors));
    return (yield* Fiber.join(output)).trim();
  });
  let gitHead: string | undefined;
  if (nativeHarnesses) {
    yield* git(["init", "--initial-branch=main"]);
    yield* git(["-c", "user.name=Smoke", "-c", "user.email=smoke@example.invalid", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "local smoke fixture"]);
    gitHead = yield* git(["rev-parse", "HEAD"]);
    yield* git(["worktree", "add", "-b", "smoke-linked", worktreeRoot]);
  } else {
    // The API-only production image intentionally has no Git executable.
    yield* fs.makeDirectory(worktreeRoot, { mode: 0o700 });
  }
  const project = { root: hermesHome, source: "configured", id: "smoke-project", name: "Explicit smoke scope" } as const;
  const verifyWorkspace = (session: SessionPresence, cwd = worktreeRoot, branch = "smoke-linked") => Effect.gen(function*() {
    yield* verify.equal(session.workspace?.cwd, cwd, "Native cwd must survive workspace grouping");
    yield* verify.deepEqual(session.workspace?.project, project, "Private configured project must survive list/inspect");
    const association = session.workspace?.git;
    yield* verify.ok(association !== undefined && association.observedAt > 0 && association.observedAt <= (yield* Clock.currentTimeMillis));
    if (nativeHarnesses) {
      yield* verify.equal(association?.state, "available", "Actual local Git association must be available");
      if (association?.state !== "available") return;
      yield* verify.equal(association.worktreeRoot, cwd);
      yield* verify.equal(association.commonDir, join(repositoryRoot, ".git"), "Linked worktrees must share the actual common Git directory");
      yield* verify.equal(association.branch, branch);
      yield* verify.equal(association.head, gitHead);
    } else {
      yield* verify.ok(association?.state === "not-repository" || association?.state === "unavailable", "API-image metadata must report an honest negative Git observation");
    }
    for (const key of ["machineName", "os", "kind", "provider", "profile", "cwd", "repository"]) {
      yield* verify.ok(!Object.hasOwn(session, key), `Legacy flat metadata must be absent: ${key}`);
    }
  });
  // Synthetic Rex references verify native metadata transport, not a live Rex server.
  const rex = { serverAddress: "unix:/tmp/coordinator-smoke-rex.sock", sessionId: `session:${yield* crypto.randomUUIDv4()}`, blockId: `block:${yield* crypto.randomUUIDv4()}` };
  const hostEnvironment = (home: string) => ({
    HOME: home, PATH: searchPath,
    XDG_CONFIG_HOME: join(home, "config"), XDG_STATE_HOME: join(home, "state"), XDG_CACHE_HOME: join(home, "cache"),
    PI_CODING_AGENT_DIR: join(home, ".pi", "agent"),
    PI_OFFLINE: "1",
    COORDINATOR_API_URL: inspectorUrl, COORDINATOR_API_TOKEN: token,
    REX_SERVER: rex.serverAddress, REX_SESSION: rex.sessionId, REX_BLOCK: rex.blockId,
  });
  const hermesEnv = hostEnvironment(hermesHome);
  const compiledMain = resolve(scriptDirectory, "../src/main.js");
  const main = (yield* fs.exists(compiledMain)) ? compiledMain : resolve(scriptDirectory, "../dist/src/main.js");
  const setup = (args: ReadonlyArray<string>) => Effect.gen(function*() {
    const child = yield* spawner.spawn(ChildProcess.make(host.executable, [main, ...args], {
      cwd: hermesHome, env: hostEnvironment(hermesHome), extendEnv: false, stdin: Stream.empty, forceKillAfter: "1 second",
    }));
    const output = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const errors = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    yield* verify.equal(yield* child.exitCode, 0, yield* Fiber.join(errors));
    return yield* Fiber.join(output);
  });
  const serverDirectory = join(root, "self-hosted");
  const initialized = yield* setup(["init", "--directory", serverDirectory, "--owner", "smoke"]);
  const ownerToken = (yield* fs.readFileString(join(serverDirectory, "owner.token"))).trim();
  const initializedConfig = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(InspectorConfig))(yield* fs.readFileString(join(serverDirectory, "server.json")));
  yield* verify.equal(initializedConfig.principals[0]?.tokenHash, Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(ownerToken))));
  yield* verify.ok(!initialized.includes(ownerToken), "Bootstrap must print a credential path, never its value");
  yield* verify.equal((yield* fs.stat(serverDirectory)).mode & 0o777, 0o700);
  yield* verify.equal((yield* fs.stat(join(serverDirectory, "owner.token"))).mode & 0o777, 0o600);
  const connected = yield* setup(["connect", "--endpoint", inspectorUrl, "--token-file", tokenFile]);
  const clientFile = join(hermesHome, "config", "coordinator", "inspector.json");
  yield* verify.deepEqual(JSON.parse(yield* fs.readFileString(clientFile)), { inspectorUrl, tokenFile });
  yield* verify.ok(!connected.includes(token), "Connection setup cannot print credentials");
  yield* verify.equal((yield* fs.stat(clientFile)).mode & 0o777, 0o600);
  yield* fs.writeFileString(clientFile, JSON.stringify({
    inspectorUrl, tokenFile, project: { root: project.root, id: project.id, name: project.name },
  }), { mode: 0o600 });
  const hermesId = `synthetic-hermes-${yield* crypto.randomUUIDv4()}`;
  const sensitive = `ignored-sensitive-${yield* crypto.randomUUIDv4()}`;
  const hermesHook = (event: string, extra: object = {}, sessionId = hermesId, home = hermesHome, cwd = home === hermesHome ? repositoryRoot : home) => Effect.gen(function*() {
    // Synthetic shell=False envelope; only the local built hook program executes.
    const child = yield* spawner.spawn(ChildProcess.make(host.executable, [main, "hook", "hermes"], {
      cwd, env: hostEnvironment(home), extendEnv: false,
      stdin: Stream.make(new TextEncoder().encode(JSON.stringify({ hook_event_name: event, session_id: sessionId, cwd, profile: "synthetic-profile", extra }))),
      forceKillAfter: "1 second",
    }));
    const stdout = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    yield* verify.equal(yield* child.exitCode, 0);
    yield* verify.equal(yield* Fiber.join(stdout), "", "Metadata hooks must never write stdout");
    yield* verify.equal(yield* Fiber.join(stderr), "", "Valid synthetic envelopes must not fail or disclose input");
  });
  yield* hermesHook("on_session_start", { model: "synthetic-model", api_key: sensitive, messages: [{ content: sensitive }] });
  yield* hermesHook("post_api_request", {
    model: "synthetic-model", provider: "synthetic-provider",
    usage: { input_tokens: 31, output_tokens: 0, total_tokens: 31, cache_read_tokens: 7, reasoning_tokens: 0, api_key: sensitive },
    response: { content: sensitive }, messages: [{ content: sensitive }], tool_input: sensitive,
  });
  yield* hermesHook("on_session_end");
  const hermesMachineId = (yield* fs.readFileString(join(hermesHome, "state", "coordinator", "machine-id"))).trim();
  yield* verify.match(hermesMachineId, /^[a-f0-9-]{36}$/);
  const hermesConnector = yield* spawner.spawn(ChildProcess.make(host.executable, [main, "connector"], {
    cwd: hermesHome, env: hermesEnv, extendEnv: false, stdin: Stream.never, forceKillAfter: "1 second",
  }));
  let connectorOutput = "";
  const connectorStdout = yield* hermesConnector.stdout.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
    connectorOutput += chunk;
  })), Effect.forkScoped);
  const connectorStderr = yield* hermesConnector.stderr.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
    connectorOutput += chunk;
  })), Effect.forkScoped);
  const hermes = yield* waitForSession("hermes", hermesId, hermesMachineId);
  yield* verify.equal(hermes.activity?.state, "idle", "Turn end retains Hermes identity as idle");
  yield* verifyWorkspace(hermes, repositoryRoot, "main");
  const hermesUsage = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: hermes.ref });
  yield* verify.equal(hermesUsage.presence.agent?.profile, "synthetic-profile");
  yield* verify.equal(hermesUsage.presence.model?.provider, "synthetic-provider");
  yield* verify.equal(hermesUsage.presence.model?.id, "synthetic-model");
  yield* verifyWorkspace(hermesUsage.presence, repositoryRoot, "main");
  yield* verify.deepEqual(hermesUsage.presence.processes, [{ pid: host.pid, startIdentity: identity.startTime, ownership: "shared" }]);
  yield* verify.equal(hermesUsage.resources.processes[0]?.process.pid, host.pid);
  yield* verify.equal(hermesUsage.resources.processes[0]?.rssBytes.status, "available");
  yield* verify.equal(hermesUsage.resources.processes[0]?.cpuPercent.status, "available");
  yield* verify.equal(hermesUsage.resources.processes[0]?.rssBytes.provenance.shared, true);
  yield* verify.equal(hermesUsage.resources.processes[0]?.process.ownership, "shared");
  yield* verify.equal(hermesUsage.resources.ownedProcessCount.status, "unavailable");
  yield* verify.equal(hermesUsage.resources.ownedRssBytes.status, "unavailable");
  yield* verify.equal(hermesUsage.resources.ownedCpuPercent.status, "unavailable");
  yield* verify.equal(hermesUsage.tokens.latestNativeUpdate.inputTokens.value, 31);
  yield* verify.equal(hermesUsage.tokens.latestNativeUpdate.outputTokens.value, 0);
  yield* verify.equal(hermesUsage.tokens.latestNativeUpdate.cacheReadTokens.value, 7);
  yield* verify.equal(hermesUsage.tokens.latestNativeUpdate.cacheWriteTokens.status, "unavailable");
  yield* verify.equal(hermesUsage.tokens.latestNativeUpdate.reasoningOutputTokens.value, 0);
  yield* verify.equal(hermesUsage.tokens.nativeAggregate.totalTokens.status, "unavailable");
  yield* verify.equal(hermesUsage.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  yield* Effect.sleep(25);
  yield* hermesHook("on_session_end");
  for (let attempt = 0; attempt < 300; attempt++) {
    const current = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId: hermesMachineId, harness: "hermes", limit: 100 });
    const updated = current.sessions.find((session) => session.ref.nativeSessionId === hermesId);
    if (updated?.lastActivityAt !== undefined && updated.lastActivityAt !== hermes.lastActivityAt) break;
    yield* verify.ok(attempt < 299, "Idle presence refresh deadline expired");
    yield* Effect.sleep(100);
  }
  const hermesIdle = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: hermes.ref });
  yield* verify.deepEqual(hermesIdle.tokens.latestNativeUpdate, hermesUsage.tokens.latestNativeUpdate, "Idle presence cannot rewrite native counter age");
  yield* verify.deepEqual(hermesIdle.presence.processes, hermesUsage.presence.processes);
  yield* verify.equal(hermesIdle.presence.workspace?.git?.observedAt, hermesUsage.presence.workspace?.git?.observedAt, "Hook heartbeats reuse the 30-second Git observation");
  const cacheDirectory = join(hermesHome, "state", "coordinator", "native-observations");
  const cacheEntries = yield* fs.readDirectory(cacheDirectory);
  for (const entry of cacheEntries) {
    const file = join(cacheDirectory, entry);
    yield* verify.ok(entry.startsWith("v3-"), "Ephemeral native cache must use the incompatible v3 namespace");
    const text = yield* fs.readFileString(file);
    yield* verify.ok(!text.includes(sensitive) && !text.includes(token), "Sensitive wire extras and credentials must not enter cache");
    yield* verify.equal((yield* fs.stat(file)).mode & 0o077, 0, "Cache must remain private");
  }
  yield* verify.ok(!JSON.stringify(hermesIdle).includes(sensitive), "Authenticated MCP must not disclose ignored wire extras");
  yield* hermesHook("on_session_finalize", { old_session_id: hermesId, response: sensitive }, "");
  yield* hermesHook("post_api_request", { usage: { input_tokens: 999 }, response: sensitive });
  yield* hermesHook("on_session_end");
  for (let attempt = 0; attempt < 300; attempt++) {
    const current = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId: hermesMachineId, harness: "hermes", limit: 100 });
    if (!current.sessions.some((session) => session.ref.nativeSessionId === hermesId)) break;
    yield* verify.ok(attempt < 299, "Finalized Hermes identity must disappear and late callbacks cannot resurrect it");
    yield* Effect.sleep(100);
  }
  const outsideRoot = join(root, "unrelated workspace");
  yield* fs.makeDirectory(outsideRoot, { mode: 0o700 });
  const outsideSessionId = `synthetic-outside-${yield* crypto.randomUUIDv4()}`;
  yield* hermesHook("on_session_start", {}, outsideSessionId, hermesHome, outsideRoot);
  const outsideSession = yield* waitForSession("hermes", outsideSessionId, hermesMachineId);
  yield* verify.equal(outsideSession.workspace?.cwd, outsideRoot);
  yield* verify.equal(outsideSession.workspace?.project, undefined, "Configured project must not attach outside its root on the same installation");
  const outsideGitState = outsideSession.workspace?.git?.state;
  yield* verify.ok(outsideGitState === "not-repository" || (!nativeHarnesses && outsideGitState === "unavailable"), "Changing cwd cannot retain a stale repository");
  yield* hermesHook("on_session_finalize", { old_session_id: outsideSessionId }, "", hermesHome, outsideRoot);
  yield* hermesConnector.kill();
  yield* hermesConnector.exitCode;
  yield* Fiber.join(connectorStdout);
  yield* Fiber.join(connectorStderr);
  yield* verify.ok(!connectorOutput.includes(sensitive) && !connectorOutput.includes(token), "Connector output must not expose wire secrets or credentials");
  nativeResults.push({ harness: "hermes", status: "synthetic shell envelopes through actual built CLI/connector/authenticated MCP; shared direct caller verified; no live provider execution" });
  const otherHome = join(root, "other-installation");
  yield* fs.makeDirectory(otherHome, { mode: 0o700 });
  const otherSessionId = `synthetic-hermes-${yield* crypto.randomUUIDv4()}`;
  yield* hermesHook("on_session_start", {}, otherSessionId, otherHome);
  const otherMachineId = (yield* fs.readFileString(join(otherHome, "state", "coordinator", "machine-id"))).trim();
  yield* verify.notEqual(otherMachineId, hermesMachineId, "Separate installation roots generate distinct identities under one owner key");
  const otherConnector = yield* spawner.spawn(ChildProcess.make(host.executable, [main, "connector"], {
    cwd: otherHome, env: hostEnvironment(otherHome), extendEnv: false, stdin: Stream.never, forceKillAfter: "1 second",
  }));
  yield* otherConnector.stdout.pipe(Stream.runDrain, Effect.forkScoped);
  yield* otherConnector.stderr.pipe(Stream.runDrain, Effect.forkScoped);
  const otherSession = yield* waitForSession("hermes", otherSessionId, otherMachineId);
  yield* verify.equal(otherSession.workspace?.cwd, otherHome);
  yield* verify.equal(otherSession.workspace?.project, undefined, "Configured project cannot leak to another installation");
  const otherGitState = otherSession.workspace?.git?.state;
  yield* verify.ok(otherGitState === "not-repository" || (!nativeHarnesses && otherGitState === "unavailable"), "Another installation must report its own negative Git observation");
  yield* verify.equal((yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.list, { limit: 100 })).sessions.length, 0);
  yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.inspect, { ref: otherSession.ref }).pipe(Effect.matchEffect({
    onFailure: (error) => error._tag === "HttpError"
      ? verify.ok(error.status === 403 || error.status === 404)
      : Effect.fail(error),
    onSuccess: () => verify.fail("Other-owner installation inspection accepted"),
  }));
  const harnesses: ReadonlyArray<"pi" | "omp"> = nativeHarnesses ? ["pi", "omp"] : [];
  const startupRequest = new TextEncoder().encode('{"id":"coordinator-startup","type":"get_state"}\n');
  for (const harness of harnesses) {
    const explicit = Option.getOrUndefined(yield* Config.option(Config.String(harness === "pi" ? "COORDINATOR_TEST_PI" : "COORDINATOR_TEST_OMP")));
    const candidates = explicit === undefined ? searchPath.split(host.platform === "win32" ? ";" : ":").map((directory) => join(directory, harness)) : [explicit];
    let executable: string | undefined;
    for (const candidate of candidates) if (yield* fs.exists(candidate)) { executable = resolve(candidate); break; }
    if (executable === undefined) return yield* Effect.die(`Actual ${harness} executable is required for native smoke`);
    const home = hermesHome;
    const modelArgs: string[] = [];
    if (harness === "omp") {
      const agentDirectory = join(home, ".omp", "agent");
      yield* fs.makeDirectory(agentDirectory, { recursive: true, mode: 0o700 });
      // Native ModelsConfigFile accepts JSON in models.yml: auth:none, no discovery, no prompt sent.
      yield* fs.writeFileString(join(agentDirectory, "models.yml"), JSON.stringify({
        providers: {
          "inspector-smoke": {
            baseUrl: "http://127.0.0.1:9/v1", api: "openai-completions", auth: "none",
            models: [{ id: "inspector-smoke", contextWindow: 8192, maxTokens: 1024 }],
          },
        },
      }), { mode: 0o600 });
      modelArgs.push("--provider", "inspector-smoke", "--model", "inspector-smoke");
      modelArgs.push("--no-tools", "--no-rules", "--no-skills", "--no-lsp", "--no-ui");
    }
    const nativeEnv = hostEnvironment(home);
    // Both CLIs honor this override; their native settings must stay separate.
    if (harness === "omp") nativeEnv.PI_CODING_AGENT_DIR = join(home, ".omp", "agent");
    const install = yield* spawner.spawn(ChildProcess.make(host.executable, [resolve(dirname(main), "../scripts/install.js"), "--target", harness, "--destination", join(home, `${harness}-native-host`), "--source"], {
      cwd: home, env: nativeEnv, extendEnv: false,
    }));
    const instructions = yield* install.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const installError = yield* install.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    yield* verify.equal(yield* install.exitCode, 0, yield* Fiber.join(installError));
    const installerOutput = yield* Fiber.join(instructions);
    yield* verify.ok(!installerOutput.includes(token), "Generated launchers and instructions cannot disclose ambient credentials");
    const destination = join(home, `${harness}-native-host`);
    for (const entry of yield* fs.readDirectory(destination, { recursive: true })) {
      const artifact = join(destination, entry);
      if ((yield* fs.stat(artifact)).type === "File") {
        yield* verify.ok(!(yield* fs.readFileString(artifact)).includes(token), "Generated installer artifacts cannot disclose ambient credentials");
      }
    }
    const activation = installerOutput.split("\n").find((line) => line.startsWith(`${harness} `));
    if (activation === undefined) return yield* Effect.die("Installer must supply a native activation command");
    const activated = yield* spawner.spawn(ChildProcess.make("/bin/sh", ["-c", `exec '${executable.replaceAll("'", "'\\''")}'${activation.slice(harness.length)}`], {
      cwd: home, env: nativeEnv, extendEnv: false,
    }));
    yield* activated.stdout.pipe(Stream.runDrain, Effect.forkScoped);
    const activationError = yield* activated.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    yield* verify.equal(yield* activated.exitCode, 0, yield* Fiber.join(activationError));
    const startup = { machineId: hermesMachineId, entered: yield* Deferred.make<void>(), resume: yield* Deferred.make<void>(), finished: yield* Deferred.make<void>() };
    blockedStartup = startup;
    const rpcReady = yield* Deferred.make<void>();
    const runList = yield* Deferred.make<void>();
    const listHandled = yield* Deferred.make<void>();
    const listVerified = yield* Deferred.make<void>();
    const listRequest = new TextEncoder().encode('{"id":"coordinator-list","type":"prompt","message":"/coordinator list"}\n');
    const messagesRequest = new TextEncoder().encode('{"id":"coordinator-list-messages","type":"get_messages"}\n');
    const child = yield* spawner.spawn(ChildProcess.make(executable, ["--mode", "rpc", "--session-dir", home, ...modelArgs], {
      cwd: worktreeRoot, env: nativeEnv, extendEnv: false,
      stdin: Stream.make(startupRequest).pipe(
        Stream.concat(Stream.fromEffect(Deferred.await(runList).pipe(Effect.as(listRequest)))),
        Stream.concat(Stream.fromEffect(Deferred.await(listHandled).pipe(Effect.as(messagesRequest)))),
        Stream.concat(Stream.never),
      ), forceKillAfter: "1 second",
    }));
    let sawNativeReady = false;
    let startupNotice = "";
    const stdout = yield* child.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runForEach((line) => Effect.gen(function*() {
      yield* verify.ok(!line.includes("\u001b]7501"), "Rex OSC reports must never corrupt native RPC stdout");
      yield* Schema.decodeEffect(NativeStartupMessage)(line).pipe(Effect.matchEffect({
        onFailure: () => Effect.void,
        onSuccess: (message) => Effect.gen(function*() {
          if (message.type === "ready") { sawNativeReady = true; return; }
          if (message.type === "agent_start") return yield* verify.fail("The native list command must not start a model turn");
          if (message.type === "extension_ui_request") {
            if (message.message.startsWith("Native inspector unavailable (")) startupNotice = message.message.slice(0, 1024);
            return;
          }
          if (message.type === "prompt_result") {
            yield* verify.equal(message.status, "completed");
            yield* verify.equal(message.agentInvoked, false);
            return yield* Deferred.succeed(listHandled, undefined);
          }
          yield* verify.ok(message.success, `${harness} native RPC command failed`);
          if (message.id === "coordinator-list") {
            if (harness === "pi") yield* Deferred.succeed(listHandled, undefined);
            return;
          }
          if (message.id === "coordinator-list-messages") {
            const result = message.data.messages.find(Schema.is(CoordinatorListMessage));
            yield* verify.ok(result !== undefined, "Native history must contain the displayed Coordinator list");
            if (result === undefined) return;
            yield* verify.ok(result.details.sessions.some((entry) => entry.ref.machineId === hermesMachineId && entry.ref.harness === harness));
            yield* verify.ok(result.details.sessions.some((entry) => entry.ref.machineId === otherMachineId));
            const content = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(CoordinatorListMessage.fields.details))(result.content);
            yield* verify.deepEqual(content.sessions, result.details.sessions, "Displayed JSON must preserve every listed session");
            const displayedNative = content.sessions.find((entry) => entry.ref.machineId === hermesMachineId && entry.ref.harness === harness);
            if (displayedNative === undefined) return yield* verify.fail("Displayed native session missing");
            yield* verifyWorkspace(displayedNative);
            yield* verify.ok(!result.content.includes(token));
            yield* Deferred.succeed(listVerified, undefined);
            return;
          }
          yield* Deferred.succeed(rpcReady, undefined);
        }),
      }));
    })), Effect.forkScoped);
    let nativeStderr = "";
    const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
      nativeStderr = (nativeStderr + chunk).slice(-4096);
    })), Effect.forkScoped);
    yield* Effect.raceFirst(
      Effect.all([Deferred.await(startup.entered), Deferred.await(rpcReady)]),
      Fiber.join(stdout).pipe(Effect.andThen(verify.fail(`${harness} RPC output ended before startup`))),
    ).pipe(
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () => Effect.gen(function*() {
          return yield* Effect.die(`Disposable ${harness} startup timed out: announceEntered=${yield* Deferred.isDone(startup.entered)}, announceFinished=${yield* Deferred.isDone(startup.finished)}, rpcReady=${yield* Deferred.isDone(rpcReady)}, sawNativeReady=${sawNativeReady}, pid=${child.pid}, running=${yield* child.isRunning}, executable=${executable}, notice=${JSON.stringify(startupNotice.replaceAll(token, "[redacted]"))}; stderr (last 4096 characters): ${JSON.stringify(nativeStderr.replaceAll(token, "[redacted]"))}`);
        }),
      }),
    );
    yield* verify.equal(yield* Deferred.isDone(startup.finished), false, `${harness} startup waited for the inspector response`);
    blockedStartup = undefined;
    yield* Deferred.succeed(startup.resume, undefined);
    const native = yield* Effect.raceFirst(waitForSession(harness, undefined, hermesMachineId), Effect.gen(function*() {
      const code = yield* child.exitCode;
      yield* Fiber.join(stderr);
      return yield* Effect.die(`Disposable ${harness} exited ${code} before presence; stderr (last 4096 characters): ${JSON.stringify(nativeStderr)}`);
    }));
    yield* verify.equal(native.rex?.serverAddress, rex.serverAddress);
    yield* verify.equal(native.rex?.sessionId, rex.sessionId);
    yield* verify.equal(native.rex?.blockId, rex.blockId);
    yield* verify.deepEqual(native.programStatus, { state: "idle" }, "Native startup has a known idle status");
    yield* verifyWorkspace(native);
    yield* verify.ok(native.machine?.name !== undefined && native.machine.os?.platform !== undefined, "Native machine facts must use the grouped shape");
    if (harness === "omp") yield* verify.deepEqual(native.model, { provider: "inspector-smoke", id: "inspector-smoke" });
    const beforeList = detailRequests;
    yield* Deferred.succeed(runList, undefined);
    yield* Effect.raceFirst(Deferred.await(listVerified), Fiber.join(stdout).pipe(Effect.andThen(verify.fail("Native RPC ended before list verification")))).pipe(Effect.timeout("30 seconds"));
    yield* verify.equal(detailRequests, beforeList, "The list command must not sample per-session details");
    const inspected = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: native.ref });
    yield* verify.equal(inspected.presence.ref.harness, harness);
    yield* verifyWorkspace(inspected.presence);
    yield* verify.equal(native.ref.machineId, hermesMachineId, "Pi, OMP and hooks share one local installation identity");
    yield* verify.equal((yield* fs.readFileString(join(home, "state", "coordinator", "machine-id"))).trim(), hermesMachineId);
    const discovered = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { limit: 100 });
    yield* verify.ok(discovered.sessions.some((entry) => entry.ref.machineId === otherMachineId));
    yield* verify.ok(discovered.sessions.some((entry) => entry.ref.machineId === hermesMachineId));
    const rootProcess = inspected.resources.processes.find((entry) => entry.process.pid === child.pid);
    yield* verify.ok(rootProcess?.rssBytes.status === "available" && rootProcess.rssBytes.value > 0, "Inspection measures the actual disposable native CLI");
    if (harness === "pi") yield* Effect.scoped(Effect.gen(function*() {
      // A separate SDK process inherits only the disposable native settings and owner key.
      // Package discovery is the same as the activated CLI: no explicit extension paths.
      const piSdk = Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_TEST_PI_SDK"))) ?? import.meta.resolve("@earendil-works/pi-coding-agent");
      const sdkSource = `
        import { createAgentSession, DefaultResourceLoader, SessionManager } from ${JSON.stringify(piSdk)};
        import { Config, Console, Effect, FileSystem, Path, Schema } from ${JSON.stringify(import.meta.resolve("effect"))};
        import { Stdio } from ${JSON.stringify(import.meta.resolve("effect/Stdio"))};
        import { NodeRuntime, NodeServices } from ${JSON.stringify(import.meta.resolve("@effect/platform-node"))};
        import { InspectionTools, SessionRef } from ${JSON.stringify((yield* path.toFileUrl(resolve(dirname(main), "inspection.js"))).href)};
        const guard = (condition, message) => condition ? Effect.void : Effect.die(message);
        const program = Effect.gen(function*() {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const stdio = yield* Stdio;
          const home = yield* Config.String("HOME");
          const agentDir = yield* Config.String("PI_CODING_AGENT_DIR");
          const stateHome = yield* Config.String("XDG_STATE_HOME");
          const [encodedRef, otherMachineId, encodedPid] = yield* stdio.args;
          if (encodedRef === undefined || otherMachineId === undefined || encodedPid === undefined) return yield* Effect.die("Missing SDK reader arguments");
          const ref = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SessionRef))(encodedRef);
          const cliPid = yield* Schema.decodeUnknownEffect(Schema.NumberFromString)(encodedPid);
          const loader = new DefaultResourceLoader({ cwd: home, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
          yield* Effect.promise(() => loader.reload());
          const options = { cwd: home, agentDir, resourceLoader: loader, sessionManager: SessionManager.inMemory(home), tools: ["coordinator_session_list", "coordinator_session_inspect"] };
          const { session, extensionsResult } = yield* Effect.acquireRelease(
            Effect.promise(() => createAgentSession(options)),
            ({ session }) => Effect.promise(() => session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" })).pipe(Effect.ensuring(Effect.sync(() => session.dispose()))),
          );
          yield* guard(extensionsResult.errors.length === 0, "Activated package must load without extension errors");
          yield* Effect.promise(() => session.bindExtensions({}));
          const list = session.agent.state.tools.find((tool) => tool.name === "coordinator_session_list");
          const inspect = session.agent.state.tools.find((tool) => tool.name === "coordinator_session_inspect");
          if (list === undefined || inspect === undefined) return yield* Effect.die("Activated built package must register native readers");
          const listed = yield* Effect.promise(() => list.execute("native-smoke-list", { limit: 100 }));
          const listing = yield* Schema.decodeUnknownEffect(InspectionTools.list.output)(listed.details);
          const rawListing = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(InspectionTools.list.output))(listed.content.find((entry) => entry.type === "text")?.text);
          yield* guard(JSON.stringify(rawListing) === JSON.stringify(listing), "Raw native list JSON must preserve complete structured details");
          yield* guard(listing.sessions.some((entry) => entry.ref.machineId === ref.machineId && entry.ref.harness === "pi" && entry.ref.nativeSessionId === ref.nativeSessionId), "Native reader must discover the independent disposable Pi CLI");
          const listedNative = listing.sessions.find((entry) => entry.ref.nativeSessionId === ref.nativeSessionId && entry.ref.machineId === ref.machineId && entry.ref.harness === ref.harness);
          yield* guard(listedNative?.workspace?.cwd === ${JSON.stringify(worktreeRoot)} && listedNative.workspace.git?.state === "available" && listedNative.workspace.git.worktreeRoot === ${JSON.stringify(worktreeRoot)} && listedNative.workspace.git.head === ${JSON.stringify(gitHead)}, "Native list JSON must retain real linked-worktree association");
          yield* guard(listedNative?.workspace?.project?.root === ${JSON.stringify(project.root)} && listedNative.workspace.project.id === "smoke-project", "Native list JSON must retain explicit project scope");
          yield* guard(listing.sessions.some((entry) => entry.ref.machineId === otherMachineId), "Native owner-key reader must discover separate installation");
          const inspected = yield* Effect.promise(() => inspect.execute("native-smoke-inspect", { ref }));
          const detail = yield* Schema.decodeUnknownEffect(InspectionTools.inspect.output)(inspected.details);
          const rawDetail = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(InspectionTools.inspect.output))(inspected.content.find((entry) => entry.type === "text")?.text);
          yield* guard(JSON.stringify(rawDetail) === JSON.stringify(detail), "Raw native inspect JSON must preserve complete grouped details");
          yield* guard(JSON.stringify(detail.presence.ref) === JSON.stringify(ref), "Native detail must match independent CLI");
          yield* guard(detail.presence.workspace?.git?.state === "available", "Native inspect JSON must retain actual Git metadata");
          yield* guard(detail.presence.workspace?.project?.id === "smoke-project" && detail.presence.workspace.project.source === "configured", "Native inspect JSON must retain explicit project");
          yield* guard(detail.presence.workspace?.git?.state === "available" && detail.presence.workspace.git.commonDir === ${JSON.stringify(join(repositoryRoot, ".git"))}, "Native inspect must retain shared Git identity");
          const rootProcess = detail.resources.processes.find((entry) => entry.process.pid === cliPid);
          if (rootProcess === undefined) return yield* Effect.die("Independent CLI process missing");
          yield* guard(rootProcess.rssBytes.status === "available" && rootProcess.rssBytes.value > 0, "Native reader must measure independent CLI, not SDK process");
          yield* guard(rootProcess.cpuTimeMs.status === "available", "Independent CLI CPU must be measured");
          const machineId = (yield* fs.readFileString(path.join(stateHome, "coordinator", "machine-id"))).trim();
          yield* guard(machineId === ref.machineId, "SDK and CLI must preserve same installation identity");
          yield* Console.log("coordinator-native-pi-reader:ok");
        });
        NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
      `;
      const sdkArguments = [JSON.stringify(native.ref), otherMachineId, String(child.pid)];
      // Node --eval omits the script argv entry; reserve it before Stdio.args slices argv.
      const reader = yield* spawner.spawn(ChildProcess.make(host.executable,
        ["--input-type=module", "--eval", sdkSource, "coordinator-native-reader", ...sdkArguments], {
        cwd: home, env: { ...nativeEnv, COORDINATOR_API_TOKEN: siblingToken }, extendEnv: false,
        stdin: Stream.empty, forceKillAfter: "1 second",
      }));
      const output = yield* reader.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
      const errors = yield* reader.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
      yield* verify.equal(yield* reader.exitCode, 0, yield* Fiber.join(errors));
      yield* verify.ok((yield* Fiber.join(output)).includes("coordinator-native-pi-reader:ok"),
        "Actual Pi SDK reader guards must complete without a model prompt");
      nativeResults.push({ harness: "pi-native-readers", status: "actual SDK and activated built-package discovery; registered list/inspect tools read independent CLI resources and same-owner installations; no model prompt sent" });
    }));
    yield* child.kill();
    yield* child.exitCode;
    nativeResults.push({ harness, status: "actual disposable CLI; startup is nonblocking; /coordinator list displays same-owner machines without a model turn or detail sampling; Rex references/status round-trip and no OSC on RPC" });
  }
  yield* otherConnector.kill();
  yield* otherConnector.exitCode;
  yield* Console.log(JSON.stringify({ scenario: "ephemeral-inspector", status: "ok", authenticatedHttpMcp: true, outboundConnector: true,
    ownerIsolation: true, sharedOwnerMultipleInstallations: true, sameInstallationNativeIdentity: true, groupedMetadata: true, realGitWorktree: nativeHarnesses, explicitProjectScope: true, selfHostBootstrap: true, authenticatedClientSetup: true, onDemandOsResources: true, fixtureNativeCounters: "unavailable, not fabricated", nativeHarnesses, nativePrograms: nativeResults }));
});

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("120 seconds"), Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer, NodeHttpClient.layerUndici, Mcp.layer))));
