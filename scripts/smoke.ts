import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { Context, Effect, Fiber, FileSystem, Layer, Schema, Stream } from "effect";
import { HttpServer } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Inspector } from "../src/inspector.ts";
import { ConnectorAccepted, ConnectorAnnounce, HttpError, InspectionTools, createInspection, type SessionPresence } from "../src/inspection.ts";
import { Mcp, serverLayer } from "../src/mcp.ts";
import { connectNative, withProcessResources } from "../src/native.ts";
import { identifyProcess } from "../src/resources.ts";

const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const mcp = yield* Mcp;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "inspector-smoke-" });
  yield* fs.chmod(root, 0o700);
  const token = randomUUID(), siblingToken = randomUUID(), outsiderToken = randomUUID();
  const machineId = `smoke-${randomUUID()}`;
  const principal = (id: string, owner: string, machine: string, credential: string) => ({ id, owner, machineId: machine, tokenHash: createHash("sha256").update(credential).digest("hex") });
  const inspector = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [
    principal("local", "smoke-owner", machineId, token),
    principal("sibling", "smoke-owner", "sibling-machine", siblingToken),
    principal("outsider", "other-owner", "other-machine", outsiderToken),
  ] });
  const server = Context.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  yield* mcp.serve(server, inspector);
  const inspectorUrl = HttpServer.formatAddress(server.address);
  for (const credential of ["", randomUUID()]) {
    yield* mcp.call(inspectorUrl, credential, InspectionTools.list, { limit: 1 }).pipe(Effect.matchEffect({
      onFailure: (error) => error._tag === "HttpError"
        ? Effect.sync(() => assert.equal(error.status, credential === "" ? 400 : 401, "Empty credentials fail local validation; invalid bearer fails HTTP authentication"))
        : Effect.fail(error),
      onSuccess: () => Effect.sync(() => assert.fail("Unauthenticated MCP accepted")),
    }));
  }
  const ready = yield* Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(new URL("/readyz", inspectorUrl), { signal, redirect: "error", headers: { authorization: `Bearer ${token}` } });
      await response.body?.cancel();
      return { status: response.status, sha: response.headers.get("x-coordinator-source-sha") };
    },
    catch: () => new HttpError({ status: 502, message: "Ephemeral readiness request failed" }),
  });
  assert.equal(ready.status, 200);
  const expectedSource = process.env.COORDINATOR_EXPECTED_SOURCE_SHA ?? process.env.COORDINATOR_IMAGE_SHA;
  if (expectedSource !== undefined) assert.equal(ready.sha, expectedSource);
  const tokenFile = join(root, "token");
  yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
  const identity = yield* identifyProcess(process.pid);
  assert.equal(identity.status, "available", "Real process identity must be observable");
  if (identity.status !== "available") return yield* Effect.die("Process identity unavailable");
  const presence: SessionPresence = {
    ref: { machineId, harness: "pi", nativeSessionId: `fixture-${randomUUID()}` }, observedAt: Date.now(),
    processes: [{ pid: process.pid, startIdentity: identity.startTime, ownership: "owned" }],
    activity: { state: "idle" },
  };
  let detailRequests = 0;
  yield* connectNative({ inspectorUrl, tokenFile, machineId, stateDirectory: root }, {
    presence: () => Effect.succeed([presence]),
    inspect: () => Effect.gen(function*() {
      detailRequests++;
      return yield* withProcessResources(createInspection(presence, Date.now()));
    }),
  }).pipe(Effect.forkScoped);
  const waitForSession = (harness: "pi" | "omp" | "hermes", sessionId?: string) => Effect.gen(function*() {
    for (let attempt = 0; attempt < 300; attempt++) {
      const result = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId, harness, limit: 100 });
      const found = result.sessions.find((session) => sessionId === undefined ? session.ref.nativeSessionId !== presence.ref.nativeSessionId : session.ref.nativeSessionId === sessionId);
      if (found !== undefined) return found;
      yield* Effect.sleep(100);
    }
    return yield* Effect.die(`Disposable ${harness} connector presence deadline expired`);
  });
  yield* waitForSession("pi", presence.ref.nativeSessionId);
  assert.equal(detailRequests, 0, "Presence/list must not sample native details or process resources");
  const sameOwner = yield* mcp.call(inspectorUrl, siblingToken, InspectionTools.list, { machineId, limit: 100 });
  assert.equal(sameOwner.sessions.length, 1);
  assert.equal((yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.list, { limit: 100 })).sessions.length, 0);
  yield* mcp.call(inspectorUrl, outsiderToken, InspectionTools.inspect, { ref: presence.ref }).pipe(Effect.matchEffect({
    onFailure: (error) => error._tag === "HttpError"
      ? Effect.sync(() => assert.ok(error.status === 403 || error.status === 404, "Expected HTTP cross-owner denial"))
      : Effect.fail(error),
    onSuccess: () => Effect.sync(() => assert.fail("Cross-owner inspection accepted")),
  }));
  yield* mcp.post(inspectorUrl, siblingToken, "/connector/announce", ConnectorAnnounce, ConnectorAccepted,
    { connectorId: randomUUID(), incarnation: randomUUID(), machineId, sessions: [presence] }).pipe(Effect.matchEffect({
      onFailure: (error) => error._tag === "HttpError"
        ? Effect.sync(() => assert.ok(error.status === 403 || error.status === 400, "Expected HTTP cross-machine denial"))
        : Effect.fail(error),
      onSuccess: () => Effect.sync(() => assert.fail("Cross-machine publication accepted")),
    }));
  const observation = yield* mcp.call(inspectorUrl, siblingToken, InspectionTools.inspect, { ref: presence.ref });
  assert.equal(detailRequests, 1);
  assert.equal(observation.resources.ownedProcessCount.status, "available");
  assert.equal(observation.resources.ownedRssBytes.status, "available");
  assert.equal(observation.resources.ownedCpuPercent.status, "available");
  if (observation.resources.ownedRssBytes.status === "available") assert.ok(observation.resources.ownedRssBytes.value > 0);
  assert.equal(observation.context.contextUsedTokens.status, "unavailable");
  assert.equal(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.equal(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  const nativeResults: Array<{ harness: string; status: string }> = [];
  // Synthetic native envelopes exercise the built CLI, not a live Hermes provider request.
  const hermesHome = join(root, "hermes space's");
  yield* fs.makeDirectory(hermesHome, { mode: 0o700 });
  const hermesConfig = join(hermesHome, "inspector.json");
  yield* fs.writeFileString(hermesConfig, JSON.stringify({ inspectorUrl, tokenFile, machineId, stateDirectory: hermesHome }), { mode: 0o600 });
  const compiledMain = resolve(import.meta.dirname, "../src/main.js");
  const main = existsSync(compiledMain) ? compiledMain : resolve(import.meta.dirname, "../dist/src/main.js");
  const hermesId = `synthetic-hermes-${randomUUID()}`;
  const sensitive = `ignored-sensitive-${randomUUID()}`;
  const hermesHook = (event: string, extra: object = {}, sessionId = hermesId) => Effect.gen(function*() {
    // Same direct argv as Hermes shell=False: env execs Node; this smoke process is the shared hook host.
    const child = yield* spawner.spawn(ChildProcess.make("/usr/bin/env", [`COORDINATOR_CONNECTOR_CONFIG=${hermesConfig}`, process.execPath, main, "hook", "hermes"], {
      stdin: Stream.make(Buffer.from(JSON.stringify({ hook_event_name: event, session_id: sessionId, cwd: hermesHome, profile: "synthetic-profile", extra }))),
      forceKillAfter: "1 second",
    }));
    const stdout = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    assert.equal(Number(yield* child.exitCode), 0);
    assert.equal(yield* Fiber.join(stdout), "", "Metadata hooks must never write stdout");
    assert.equal(yield* Fiber.join(stderr), "", "Valid synthetic envelopes must not fail or disclose input");
  });
  yield* hermesHook("on_session_start", { model: "synthetic-model", api_key: sensitive, messages: [{ content: sensitive }] });
  yield* hermesHook("post_api_request", {
    model: "synthetic-model", provider: "synthetic-provider",
    usage: { input_tokens: 31, output_tokens: 0, total_tokens: 31, cache_read_tokens: 7, reasoning_tokens: 0, api_key: sensitive },
    response: { content: sensitive }, messages: [{ content: sensitive }], tool_input: sensitive,
  });
  yield* hermesHook("on_session_end");
  const hermesConnector = yield* spawner.spawn(ChildProcess.make("/usr/bin/env", [`COORDINATOR_CONNECTOR_CONFIG=${hermesConfig}`, process.execPath, main, "connector"], {
    stdin: Stream.never, forceKillAfter: "1 second",
  }));
  let connectorOutput = "";
  const connectorStdout = yield* hermesConnector.stdout.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
    connectorOutput += chunk;
  })), Effect.forkScoped);
  const connectorStderr = yield* hermesConnector.stderr.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
    connectorOutput += chunk;
  })), Effect.forkScoped);
  const hermes = yield* waitForSession("hermes", hermesId);
  assert.equal(hermes.activity?.state, "idle", "Turn end retains Hermes identity as idle");
  const hermesUsage = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: hermes.ref });
  assert.equal(hermesUsage.presence.profile, "synthetic-profile");
  assert.equal(hermesUsage.presence.provider, "synthetic-provider");
  assert.equal(hermesUsage.presence.model, "synthetic-model");
  assert.deepEqual(hermesUsage.presence.processes, [{ pid: process.pid, startIdentity: identity.startTime, ownership: "shared" }]);
  assert.equal(hermesUsage.resources.processes[0]?.process.pid, process.pid);
  assert.equal(hermesUsage.resources.processes[0]?.rssBytes.status, "available");
  assert.equal(hermesUsage.resources.processes[0]?.cpuPercent.status, "available");
  assert.equal(hermesUsage.resources.processes[0]?.rssBytes.provenance.shared, true);
  assert.equal(hermesUsage.resources.processes[0]?.process.ownership, "shared");
  assert.equal(hermesUsage.resources.ownedProcessCount.status, "unavailable");
  assert.equal(hermesUsage.resources.ownedRssBytes.status, "unavailable");
  assert.equal(hermesUsage.resources.ownedCpuPercent.status, "unavailable");
  assert.equal(hermesUsage.tokens.latestNativeUpdate.inputTokens.value, 31);
  assert.equal(hermesUsage.tokens.latestNativeUpdate.outputTokens.value, 0);
  assert.equal(hermesUsage.tokens.latestNativeUpdate.cacheReadTokens.value, 7);
  assert.equal(hermesUsage.tokens.latestNativeUpdate.cacheWriteTokens.status, "unavailable");
  assert.equal(hermesUsage.tokens.latestNativeUpdate.reasoningOutputTokens.value, 0);
  assert.equal(hermesUsage.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.equal(hermesUsage.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  yield* Effect.sleep(25);
  yield* hermesHook("on_session_end");
  for (let attempt = 0; attempt < 300; attempt++) {
    const current = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId, harness: "hermes", limit: 100 });
    const updated = current.sessions.find((session) => session.ref.nativeSessionId === hermesId);
    if (updated?.lastActivityAt !== undefined && updated.lastActivityAt !== hermes.lastActivityAt) break;
    assert.ok(attempt < 299, "Idle presence refresh deadline expired");
    yield* Effect.sleep(100);
  }
  const hermesIdle = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: hermes.ref });
  assert.deepEqual(hermesIdle.tokens.latestNativeUpdate, hermesUsage.tokens.latestNativeUpdate, "Idle presence cannot rewrite native counter age");
  assert.deepEqual(hermesIdle.presence.processes, hermesUsage.presence.processes);
  const cacheDirectory = join(hermesHome, "native-observations");
  const cacheEntries = yield* fs.readDirectory(cacheDirectory);
  assert.ok(cacheEntries.some((name) => name.startsWith("hermes-")), "Native hook must persist its canonical private cache");
  for (const entry of cacheEntries) {
    const file = join(cacheDirectory, entry);
    const text = yield* fs.readFileString(file);
    assert.ok(!text.includes(sensitive) && !text.includes(token), "Sensitive wire extras and credentials must not enter cache");
    assert.equal((yield* fs.stat(file)).mode & 0o077, 0, "Cache must remain private");
  }
  assert.ok(!JSON.stringify(hermesIdle).includes(sensitive), "Authenticated MCP must not disclose ignored wire extras");
  yield* hermesHook("on_session_finalize", { old_session_id: hermesId, response: sensitive }, "");
  yield* hermesHook("post_api_request", { usage: { input_tokens: 999 }, response: sensitive });
  yield* hermesHook("on_session_end");
  for (let attempt = 0; attempt < 300; attempt++) {
    const current = yield* mcp.call(inspectorUrl, token, InspectionTools.list, { machineId, harness: "hermes", limit: 100 });
    if (!current.sessions.some((session) => session.ref.nativeSessionId === hermesId)) break;
    assert.ok(attempt < 299, "Finalized Hermes identity must disappear and late callbacks cannot resurrect it");
    yield* Effect.sleep(100);
  }
  yield* hermesConnector.kill();
  yield* hermesConnector.exitCode;
  yield* Fiber.join(connectorStdout);
  yield* Fiber.join(connectorStderr);
  assert.ok(!connectorOutput.includes(sensitive) && !connectorOutput.includes(token), "Connector output must not expose wire secrets or credentials");
  nativeResults.push({ harness: "hermes", status: "synthetic shell envelopes through actual built CLI/connector/authenticated MCP; shared direct caller verified; no live provider execution" });
  const harnesses: ReadonlyArray<"pi" | "omp"> = ["pi", "omp"];
  for (const harness of harnesses) {
    const explicit = harness === "pi" ? process.env.COORDINATOR_TEST_PI : process.env.COORDINATOR_TEST_OMP;
    const executable = explicit ?? (process.env.PATH ?? "").split(delimiter).map((directory) => join(directory, harness)).find(existsSync);
    if (executable === undefined) { nativeResults.push({ harness, status: "unavailable: native executable not installed" }); continue; }
    const home = join(root, harness);
    yield* fs.makeDirectory(home, { mode: 0o700 });
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
    }
    const configFile = join(home, "inspector.json");
    yield* fs.writeFileString(configFile, JSON.stringify({ inspectorUrl, tokenFile, machineId, stateDirectory: home }), { mode: 0o600 });
    const nativeEnv = { HOME: home, PATH: process.env.PATH ?? "/usr/bin:/bin", COORDINATOR_CONNECTOR_CONFIG: configFile };
    const install = yield* spawner.spawn(ChildProcess.make(process.execPath, [resolve(dirname(main), "../scripts/install.js"), "--target", harness, "--destination", join(home, "native-host"), "--config", configFile], {
      cwd: home, env: nativeEnv, extendEnv: false,
    }));
    const instructions = yield* install.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const installError = yield* install.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    assert.equal(yield* install.exitCode, 0, yield* Fiber.join(installError));
    const activation = (yield* Fiber.join(instructions)).split("\n").find((line) => line.startsWith(`${harness} `));
    assert.ok(activation, "Installer must supply a native activation command");
    const activated = yield* spawner.spawn(ChildProcess.make("/bin/sh", ["-c", `exec '${executable.replaceAll("'", "'\\''")}'${activation.slice(harness.length)}`], {
      cwd: home, env: nativeEnv, extendEnv: false,
    }));
    yield* activated.stdout.pipe(Stream.runDrain, Effect.forkScoped);
    const activationError = yield* activated.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    assert.equal(yield* activated.exitCode, 0, yield* Fiber.join(activationError));
    const extension = resolve(import.meta.dirname, "../..", "dist/plugins", `${harness}.js`);
    const sourceExtension = resolve(import.meta.dirname, "..", "dist/plugins", `${harness}.js`);
    const child = yield* spawner.spawn(ChildProcess.make(executable, ["--mode", "rpc", "--session-dir", home, ...modelArgs], {
      cwd: home, env: nativeEnv, extendEnv: false, stdin: Stream.never, forceKillAfter: "1 second",
    }));
    yield* child.stdout.pipe(Stream.runDrain, Effect.forkScoped);
    let nativeStderr = "";
    const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runForEach((chunk) => Effect.sync(() => {
      nativeStderr = (nativeStderr + chunk).slice(-4096);
    })), Effect.forkScoped);
    const native = yield* Effect.raceFirst(waitForSession(harness), Effect.gen(function*() {
      const code = yield* child.exitCode;
      yield* Fiber.join(stderr);
      return yield* Effect.die(`Disposable ${harness} exited ${code} before presence; stderr (last 4096 characters): ${JSON.stringify(nativeStderr)}`);
    }));
    const inspected = yield* mcp.call(inspectorUrl, token, InspectionTools.inspect, { ref: native.ref });
    assert.equal(inspected.presence.ref.harness, harness);
    if (harness === "pi") yield* Effect.promise(async () => {
      // Host-only SDK: the production inspector image deliberately omits native CLI/SDK packages.
      const { createAgentSession, DefaultResourceLoader, SessionManager } = await import("@earendil-works/pi-coding-agent");
      const previousConfig = process.env.COORDINATOR_CONNECTOR_CONFIG;
      process.env.COORDINATOR_CONNECTOR_CONFIG = configFile;
      try {
        const loader = new DefaultResourceLoader({ cwd: home, agentDir: home, additionalExtensionPaths: [existsSync(extension) ? extension : sourceExtension], noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
        await loader.reload();
        const { session } = await createAgentSession({ cwd: home, agentDir: home, resourceLoader: loader, sessionManager: SessionManager.inMemory(home), tools: ["coordinator_session_list", "coordinator_session_inspect"] });
        try {
          await session.bindExtensions({});
          const list = session.agent.state.tools.find((tool) => tool.name === "coordinator_session_list");
          const inspect = session.agent.state.tools.find((tool) => tool.name === "coordinator_session_inspect");
          assert.ok(list, "Native Pi must expose its authenticated session reader");
          assert.ok(inspect, "Native Pi must expose its authenticated detail reader");
          const listing = Schema.decodeUnknownSync(InspectionTools.list.output)((await list.execute("native-smoke-list", { machineId, harness, limit: 100 })).details);
          assert.ok(listing.sessions.some((entry) => entry.ref.nativeSessionId === native.ref.nativeSessionId), "Native reader must discover the real CLI session");
          const detail = Schema.decodeUnknownSync(InspectionTools.inspect.output)((await inspect.execute("native-smoke-inspect", { ref: native.ref })).details);
          assert.deepEqual(detail.presence.ref, native.ref);
          const rootProcess = detail.resources.processes.find((entry) => entry.process.pid === child.pid);
          assert.ok(rootProcess?.rssBytes.status === "available" && rootProcess.rssBytes.value > 0, "Native reader must measure the actual spawned CLI root");
          assert.equal(rootProcess.cpuTimeMs.status, "available");
        } finally {
          await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
          session.dispose();
        }
      } finally {
        if (previousConfig === undefined) delete process.env.COORDINATOR_CONNECTOR_CONFIG;
        else process.env.COORDINATOR_CONNECTOR_CONFIG = previousConfig;
      }
    });
    yield* child.kill();
    yield* child.exitCode;
    nativeResults.push({ harness, status: harness === "pi" ? "actual disposable CLI inspected through native SDK reader tools; no model prompt sent" : "actual disposable CLI extension inspected; no prompt sent" });
  }
  console.log(JSON.stringify({ scenario: "ephemeral-inspector", status: "ok", authenticatedHttpMcp: true, outboundConnector: true,
    ownerIsolation: true, machineIsolation: true, onDemandOsResources: true, fixtureNativeCounters: "unavailable, not fabricated", nativePrograms: nativeResults }));
});

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("120 seconds"), Effect.provide(Layer.mergeAll(NodeServices.layer, Mcp.layer))));
