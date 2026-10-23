import { assert, it } from "@effect/vitest";
import { Cause, Clock, ConfigProvider, Crypto, Deferred, Effect, Fiber, FileSystem, Layer, Logger, Path, PlatformError, Queue, Random, References, Schedule, Schema, Stream } from "effect";
import type { Scope } from "effect";
import { Hex } from "effect/encoding";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { FetchHttpClient, HttpClient, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeServices } from "@effect/platform-node";
import { connectNative, projectHook, projectClaudeStatus, projectCodexNotification, writeNativeCache, writeHermesHook, readNativeCache, readPrivateFile, loadConnectorConfig, readConnectorToken, resolveNativeConfig, withProcessResources } from "../src/native.ts";
import type { NativeConfig, NativeNotice } from "../src/native.ts";
import type { ConnectorConfig, InspectorPrincipal } from "../src/inspection.ts";
import { INSPECTION_LIMIT, HttpError, decodeConnectorResult, decodeSessionInspection, sessionRefKey } from "../src/inspection.ts";
import { Inspector } from "../src/inspector.ts";
import { Mcp } from "../src/mcp.ts";
import { Host, checkExecutable, serverLayer } from "../src/platform.ts";
import { cli } from "../src/main.ts";

const fixture = <A, E>(effect: Effect.Effect<A, E, NodeServices.NodeServices | Host | Scope.Scope | HttpClient.HttpClient>) => Effect.scoped(effect.pipe(Effect.provide(FetchHttpClient.layer), Effect.provide(NodeServices.layer), Effect.provide(Host.layer)));
const expectFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.gen(function*() {
  const exit = yield* Effect.exit(effect);
  assert.strictEqual(exit._tag, "Failure");
});
// Real File handles expose effects on their prototype; spread only explicit overrides.
const delegateFile = (handle: FileSystem.File, overrides: Partial<Pick<FileSystem.File, "read" | "writeAll">>): FileSystem.File => ({
  [FileSystem.FileTypeId]: FileSystem.FileTypeId,
  stat: handle.stat,
  sync: handle.sync,
  seek: (offset, from) => handle.seek(offset, from),
  read: (buffer, options) => handle.read(buffer, options),
  readAlloc: (size, options) => handle.readAlloc(size, options),
  truncate: (length) => handle.truncate(length),
  write: (buffer) => handle.write(buffer),
  writeAll: (buffer) => handle.writeAll(buffer),
  ...overrides,
});
const runNativeProcess = (args: readonly string[], environment: Record<string, string | undefined>, input = "") => Effect.scoped(Effect.gen(function*() {
  const host = yield* Host;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(ChildProcess.make(host.executable, ["--experimental-strip-types", ...args], {
    env: environment, extendEnv: true, stdin: Stream.make(new TextEncoder().encode(input)),
  }));
  const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (all, chunk) => all + chunk)).pipe(Effect.forkChild);
  const stdout = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (all, chunk) => all + chunk));
  const code = yield* child.exitCode;
  const diagnostic = yield* Fiber.join(stderr);
  assert.strictEqual(Number(code), 0, diagnostic + stdout);
  return stdout;
}));

it.live("merges environment over private config, keeps file selection explicit and reads rotating credentials", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const settings: Record<string, string | undefined> = {};

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-config-" }));
    settings.XDG_CONFIG_HOME = directory;
    settings.XDG_STATE_HOME = path.join(directory, "untouched");
    settings.COORDINATOR_CONNECTOR_CONFIG = undefined;
    settings.COORDINATOR_API_URL = undefined;
    settings.COORDINATOR_API_TOKEN = undefined;
    assert.strictEqual((yield* (loadConnectorConfig().pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))), undefined);
    settings.COORDINATOR_API_URL = "https://env.example";
    settings.COORDINATOR_API_TOKEN = "environment-credential";
    const environment: ConnectorConfig = { inspectorUrl: "https://env.example", apiToken: "environment-credential" };
    assert.deepStrictEqual((yield* (loadConnectorConfig().pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))), environment);
    const tokenFile = path.join(directory, "token");
    (yield* fs.writeFileString(tokenFile, "file-credential-first\n", { mode: 0o600 }));
    const file = path.join(directory, "client.json");
    (yield* fs.writeFileString(file, JSON.stringify({ inspectorUrl: "https://file.example", apiToken: "configured-credential", tokenFile }), { mode: 0o600 }));
    const other = path.join(directory, "other.json");
    (yield* fs.writeFileString(other, JSON.stringify({ inspectorUrl: "https://other.example", tokenFile }), { mode: 0o600 }));
    settings.COORDINATOR_CONNECTOR_CONFIG = other;
    assert.deepStrictEqual((yield* (loadConnectorConfig(file).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))), { ...environment, tokenFile });
    settings.COORDINATOR_API_URL = undefined;
    settings.COORDINATOR_API_TOKEN = undefined;
    assert.deepStrictEqual((yield* (loadConnectorConfig().pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))), { inspectorUrl: "https://other.example", tokenFile });
    const configured = (yield* (loadConnectorConfig(file).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings)))));
    if (!configured) return yield* Effect.die("Missing expected fixture result");
    assert.strictEqual((yield* (readConnectorToken(configured))), "configured-credential");
    const rotating: ConnectorConfig = { inspectorUrl: configured.inspectorUrl, tokenFile };
    assert.strictEqual((yield* (readConnectorToken(rotating))), "file-credential-first");
    const replacement = path.join(directory, "replacement");
    (yield* fs.writeFileString(replacement, "file-credential-second", { mode: 0o600 }));
    (yield* fs.rename(replacement, tokenFile));
    assert.strictEqual((yield* (readConnectorToken(rotating))), "file-credential-second");
    (yield* expectFailure((readConnectorToken({ inspectorUrl: configured.inspectorUrl }))));
    (yield* expectFailure((loadConnectorConfig(path.join(directory, "missing")).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    (yield* fs.writeFileString(file, JSON.stringify({ inspectorUrl: "https://file.example", machineId: "obsolete" }), { mode: 0o600 }));
    (yield* expectFailure((loadConnectorConfig(file).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    (yield* expectFailure(fs.stat(path.join(directory, "untouched"))));
})));

it.live("publishes one complete installation UUID across processes and preserves it across endpoint, key and profile changes", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-identity-" }));
  const state = path.join(directory, "state");
  const script = `import { Effect, Console, Layer } from "effect"; import { NodeServices, NodeRuntime } from "@effect/platform-node"; import { Host } from ${JSON.stringify(new URL("../src/platform.ts", import.meta.url).href)}; import { loadConnectorConfig, resolveNativeConfig } from ${JSON.stringify(new URL("../src/native.ts", import.meta.url).href)}; NodeRuntime.runMain(Effect.gen(function*() { const input = yield* loadConnectorConfig(); if (!input) return yield* Effect.die("Missing configuration"); const config = yield* resolveNativeConfig(input); yield* Console.log(config.machineId); }).pipe(Effect.provide(Layer.merge(NodeServices.layer, Host.layer))));`;
  const environment = { XDG_STATE_HOME: state, XDG_CONFIG_HOME: path.join(directory, "config"), COORDINATOR_CONNECTOR_CONFIG: undefined, COORDINATOR_API_URL: "https://first.example", COORDINATOR_API_TOKEN: "first-owner-credential" };
    const identities = (yield* Effect.all(Array.from({ length: 4 }, () => runNativeProcess(["--input-type=module", "-e", script], environment)), { concurrency: "unbounded" }));
    const identity = identities.at(0)?.trim();
    assert.match(identity ?? "", /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    for (const value of identities) assert.strictEqual(value, identity + "\n");
    assert.strictEqual((yield* runNativeProcess(["--input-type=module", "-e", script], { ...environment, COORDINATOR_API_URL: "https://second.example", COORDINATOR_API_TOKEN: "rotated-owner-credential", HERMES_PROFILE: "another-profile" })), identity + "\n");
    assert.notStrictEqual(((yield* runNativeProcess(["--input-type=module", "-e", script], { ...environment, XDG_STATE_HOME: path.join(directory, "other") }))).trim(), identity);
    const root = path.join(state, "coordinator");
    assert.strictEqual(((yield* fs.stat(root))).mode & 0o777, 0o700);
    assert.strictEqual(((yield* fs.stat(path.join(root, "machine-id")))).mode & 0o777, 0o600);
    assert.deepStrictEqual((yield* fs.readDirectory(root)), ["machine-id"]);
})), 30000);

it.live("cleans acquired identity temporaries on write or close failure without removing failed exclusive acquisitions", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const settings: Record<string, string | undefined> = {};

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-identity-failure-" }));
  for (const failure of ["write", "close", "exclusive", "link", "winner"]) {
    const base = path.join(directory, failure);
    const root = path.join(base, "coordinator");
    settings.XDG_STATE_HOME = base;
    const injected = FileSystem.make({ ...fs, open: (file, options) => {
      if (!path.basename(file).startsWith(".machine-id-")) return fs.open(file, options);
      if (failure === "exclusive") return fs.writeFileString(file, "another creator", { mode: 0o600 }).pipe(Effect.andThen(Effect.fail(PlatformError.systemError({ _tag: "AlreadyExists", module: "FileSystem", method: "open" }))));
      return Effect.gen(function*() {
        const handle = yield* fs.open(file, options);
        if (failure === "close") yield* Effect.addFinalizer(() => Effect.die(PlatformError.systemError({ _tag: "BadResource", module: "FileSystem", method: "close", pathOrDescriptor: "secret-identity-path" })));
        return failure === "write" ? delegateFile(handle, { writeAll: () => Effect.fail(PlatformError.systemError({ _tag: "BadResource", module: "FileSystem", method: "writeAll" })) }) : handle;
      });
    }, link: (from, to) => {
      if (failure === "link") return Effect.fail(PlatformError.systemError({ _tag: "PermissionDenied", module: "FileSystem", method: "link" }));
      if (failure === "winner") return fs.writeFileString(to, "corrupt winner\n", { mode: 0o600 }).pipe(Effect.andThen(Effect.fail(PlatformError.systemError({ _tag: "AlreadyExists", module: "FileSystem", method: "link" }))));
      return fs.link(from, to);
    } });
    const exit = yield* Effect.exit(resolveNativeConfig({ inspectorUrl: "https://inspector.example" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings)), Effect.provideService(FileSystem.FileSystem, injected)));
    if (exit._tag !== "Failure") return yield* Effect.die("Expected identity publication failure");
    assert.isTrue(exit.cause.reasons.every((reason) => reason._tag === "Fail" && reason.error._tag === "NativeError" && reason.error.operation === "Resolve private native installation identity"));
    const remaining = (yield* fs.readDirectory(root));
    if (failure === "exclusive") {
      assert.strictEqual(remaining.length, 1);
      assert.match(remaining.at(0) ?? "", /^\.machine-id-/);
      assert.strictEqual((yield* fs.readFileString(path.join(root, remaining.at(0) ?? ""))), "another creator");
    } else if (failure === "winner") {
      assert.deepStrictEqual(remaining, ["machine-id"]);
      assert.strictEqual(yield* fs.readFileString(path.join(root, "machine-id")), "corrupt winner\n");
    } else assert.deepStrictEqual(remaining, []);
    assert.strictEqual((yield* fs.exists(path.join(root, "machine-id"))), failure === "winner");
  }
})));

it.live("rejects corrupt, public or symlinked identity and unsafe state roots without replacing them", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const settings: Record<string, string | undefined> = {};

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-unsafe-identity-" }));
  const root = path.join(directory, "coordinator");
  const file = path.join(root, "machine-id");
  const config: ConnectorConfig = { inspectorUrl: "https://inspector.example" };
    settings.XDG_STATE_HOME = directory;
    (yield* fs.makeDirectory(root, { mode: 0o700 }));
    (yield* fs.writeFileString(file, "corrupt\n", { mode: 0o600 }));
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    assert.strictEqual((yield* fs.readFileString(file)), "corrupt\n");
    (yield* fs.remove(file));
    const valid = "ebcbb06d-92c4-4df9-973d-f2f47399a2ed\n";
    (yield* fs.writeFileString(file, valid, { mode: 0o644 }));
    (yield* fs.chmod(file, 0o644));
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    assert.strictEqual((yield* fs.readFileString(file)), valid);
    (yield* fs.remove(file));
    const linked = path.join(directory, "linked-id");
    (yield* fs.writeFileString(linked, valid, { mode: 0o600 }));
    (yield* fs.symlink(linked, file));
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    assert.strictEqual((yield* fs.readLink(file)), linked);
    (yield* fs.remove(file));
    (yield* fs.chmod(root, 0o755));
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    (yield* expectFailure(fs.stat(file)));
    settings.XDG_STATE_HOME = directory + "/../noncanonical";
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    settings.XDG_STATE_HOME = "relative-state";
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
    (yield* fs.chmod(root, 0o700));
    settings.XDG_STATE_HOME = path.join(directory, "alias");
    (yield* fs.symlink(directory, path.join(directory, "alias")));
    (yield* expectFailure((resolveNativeConfig(config).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))))));
})));

it.live("starts authenticated readers without creating or requiring writable installation state", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const settings: Record<string, string | undefined> = {};

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-reader-" }));
  const blocked = path.join(directory, "not-a-directory");
    (yield* fs.writeFileString(blocked, "unchanged", { mode: 0o600 }));
    settings.XDG_CONFIG_HOME = directory;
    settings.XDG_STATE_HOME = blocked;
    settings.COORDINATOR_CONNECTOR_CONFIG = undefined;
    settings.COORDINATOR_API_URL = "https://reader.example";
    settings.COORDINATOR_API_TOKEN = "reader-owner-credential";
    let calls = 0;
    const transport = (yield* (Mcp.make));
    const call: typeof transport.call = (url, token) => {
      calls++;
      assert.strictEqual(url, "https://reader.example");
      assert.strictEqual(token, "reader-owner-credential");
      return Effect.fail(new HttpError({ status: 503, message: "Reader reached authenticated transport" }));
    };
    yield* expectFailure(cli(["list"]).pipe(Effect.provideService(Mcp, { ...transport, call }), Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(settings))));
    assert.strictEqual(calls, 1);
    assert.strictEqual((yield* fs.readFileString(blocked)), "unchanged");
    assert.deepStrictEqual(((yield* fs.readDirectory(directory))).sort(), ["not-a-directory"]);
})));

it.live("startup hooks capture all three native harnesses without waiting for a stalled inspector", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const server = yield* HttpServer.HttpServer;
  let requests = 0;
  yield* server.serve(Effect.sync(() => { requests++; }).pipe(Effect.andThen(Effect.interruptible(Effect.never))));
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-offline-hook-" });
  const url = HttpServer.formatAddress(server.address);
  const configFile = path.join(directory, "client.json");
  yield* fs.writeFileString(configFile, JSON.stringify({ inspectorUrl: url }), { mode: 0o600 });
  const environment: Record<string, string | undefined> = {
    HOME: directory, XDG_CONFIG_HOME: path.join(directory, "config"), XDG_STATE_HOME: path.join(directory, "state"),
    COORDINATOR_CONNECTOR_CONFIG: configFile, COORDINATOR_API_URL: undefined, COORDINATOR_API_TOKEN: undefined,
  };
  const entry = yield* path.fromFileUrl(new URL("../src/main.ts", import.meta.url));
  for (const harness of ["claude", "codex", "hermes"]) {
    yield* runNativeProcess([entry, "hook", harness], environment, JSON.stringify({
      session_id: `${harness}-offline`, hook_event_name: harness === "hermes" ? "on_session_start" : "SessionStart", extra: {},
    })).pipe(Effect.timeout("5 seconds"));
  }
  const machineFile = path.join(directory, "state", "coordinator", "machine-id");
  const machineId = (yield* fs.readFileString(machineFile)).trim();
  const config: NativeConfig = { inspectorUrl: url, machineId, stateDirectory: path.dirname(machineFile) };
  const cached = yield* readNativeCache(config);
  assert.deepStrictEqual(cached.map((item) => item.presence.ref.harness).sort(), ["claude", "codex", "hermes"]);
  for (const item of cached) assert.strictEqual(item.presence.ref.machineId, machineId);
  assert.strictEqual(requests, 0, "Native startup hooks must only capture locally");
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0)))), 20000);

it.live("stdio startup advertises tools before inspector availability and closes a pending inspection on shutdown", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const server = yield* HttpServer.HttpServer;
  const requested = yield* Deferred.make<void>();
  const disconnected = yield* Deferred.make<void>();
  yield* server.serve(Deferred.succeed(requested, undefined).pipe(
    Effect.andThen(Effect.interruptible(Effect.never)),
    Effect.ensuring(Deferred.succeed(disconnected, undefined)),
  ));
  const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-stdio-startup-" });
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const listed = yield* Deferred.make<void>();
  const decodeReply = Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({
    id: Schema.optional(Schema.Number),
    result: Schema.optional(Schema.Struct({
      tools: Schema.optional(Schema.Array(Schema.Struct({ name: Schema.String }))),
    })),
    error: Schema.optional(Schema.Struct({ code: Schema.Number })),
  })));
  const child = yield* spawner.spawn(ChildProcess.make(host.executable, [
    "--experimental-strip-types", yield* path.fromFileUrl(new URL("../src/main.ts", import.meta.url)), "mcp",
  ], {
    env: {
      HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
      COORDINATOR_API_URL: HttpServer.formatAddress(server.address), COORDINATOR_API_TOKEN: "private-startup-credential",
    },
    extendEnv: false, stdin: Stream.fromQueue(input), forceKillAfter: "1 second",
  }));
  const output = yield* child.stdout.pipe(
    Stream.decodeText(), Stream.splitLines,
    Stream.runForEach((line) => Effect.gen(function*() {
      const response = yield* decodeReply(line);
      if (response.id === 1) {
        assert.strictEqual(response.error, undefined);
        assert.deepEqual(response.result?.tools?.map((tool) => tool.name).sort(), ["session.inspect", "session.list"]);
        yield* Deferred.succeed(listed, undefined);
      }
    })),
    Effect.forkScoped,
  );
  const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
  const encoder = new TextEncoder();
  const prematureRequest = Effect.raceFirst(
    Deferred.await(requested).pipe(Effect.andThen(Effect.die(new Error("Stdio startup awaited the inspector")))),
    Fiber.join(output).pipe(Effect.andThen(Effect.die(new Error("Stdio closed before tool discovery")))),
  );
  const metadata = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientInfo": { name: "startup-regression", version: "1.0.0" },
    "io.modelcontextprotocol/clientCapabilities": {},
  };
  yield* Queue.offer(input, encoder.encode(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: { _meta: metadata } })}\n`));
  yield* Effect.raceFirst(Deferred.await(listed), prematureRequest);
  yield* Queue.offer(input, encoder.encode(`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { _meta: metadata, name: "session.list", arguments: { limit: 1 } } })}\n`));
  yield* Deferred.await(requested);
  yield* Queue.end(input);
  const code = yield* child.exitCode;
  yield* Fiber.join(output);
  assert.strictEqual(Number(code), 0, yield* Fiber.join(stderr));
  yield* Deferred.await(disconnected);
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0)), Effect.timeout("8 seconds"))), 15000);

it.live("projects Claude metadata without prompt, transcript, result or invented PID", () => fixture(Effect.gen(function*() {

  const observation = (yield* (projectHook("claude", "machine", { session_id: "session", hook_event_name: "PermissionRequest", cwd: "/workspace", tool_name: "Bash", prompt: "secret-prompt", transcript_path: "/secret/transcript", tool_input: { command: "secret-command" }, tool_response: "secret-result", pid: 123, ppid: 456 }, 100)));
  (yield* (decodeSessionInspection(observation)));
  assert.strictEqual(observation.presence.activity?.approval, "pending");
  assert.strictEqual(observation.presence.processes, undefined);
  assert.strictEqual(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.strictEqual(JSON.stringify(observation).includes("secret"), false);
  assert.strictEqual(((yield* (withProcessResources(observation).pipe(Effect.provide(NodeServices.layer))))).resources.ownedProcessCount.status, "unavailable");
})));

it.live("keeps Claude current context input-only, cached and distinct from aggregate spend", () => fixture(Effect.gen(function*() {

  const observation = (yield* (projectClaudeStatus("machine", { session_id: "session", transcript_path: "/secret", context_window: { total_input_tokens: 400, total_output_tokens: 80, context_window_size: 2000, current_usage: { input_tokens: 100, output_tokens: 80, cache_read_input_tokens: 200, cache_creation_input_tokens: 100 }, used_percentage: 20, remaining_percentage: 80 }, cost: { total_cost_usd: 0.3, total_duration_ms: 1500, total_api_duration_ms: 400 } }, 300)));
  (yield* (decodeSessionInspection(observation)));
  assert.strictEqual(observation.context.contextUsedTokens.value, 400);
  assert.strictEqual(observation.context.contextUsedPercent.value, 20);
  assert.strictEqual(observation.context.contextRemainingTokens.value, 1600);
  assert.strictEqual(observation.tokens.latestNativeUpdate.outputTokens.value, 80);
  assert.strictEqual(observation.tokens.latestNativeUpdate.totalTokens.status, "unavailable");
  assert.strictEqual(observation.tokens.nativeAggregate.inputTokens.status, "unavailable");
  assert.strictEqual(observation.cost.nativeAggregate.totalCostUsd.provenance.estimated, true);
  assert.strictEqual(observation.activity.sessionRunningMs.value, 1500);
  assert.strictEqual(observation.activity.apiWaitMs.value, 400);
  assert.strictEqual(observation.context.contextUsedTokens.provenance.nativeUpdatedAt, null);
  assert.strictEqual(observation.context.contextUsedTokens.provenance.stale, false);
  const empty = (yield* (projectClaudeStatus("machine", { session_id: "session", context_window: { current_usage: null, context_window_size: 2000, used_percentage: null } }, 400)));
  assert.strictEqual(empty.context.contextUsedTokens.value, null);
  assert.strictEqual(empty.context.contextUsedPercent.status, "unavailable");
  const partial = (yield* (projectClaudeStatus("machine", { session_id: "session", context_window: { context_window_size: 2000, current_usage: { input_tokens: 100, cache_read_input_tokens: 30 }, used_percentage: 12 } }, 500)));
  assert.strictEqual(partial.context.contextUsedTokens.status, "unavailable");
  assert.strictEqual(partial.context.contextRemainingTokens.status, "unavailable");
  assert.strictEqual(partial.context.contextUsedPercent.value, 12);
  const overlimit = (yield* (projectClaudeStatus("machine", { session_id: "overlimit", context_window: { used_percentage: 105, remaining_percentage: -5 } })));
  (yield* (decodeSessionInspection(overlimit)));
  assert.strictEqual(overlimit.context.contextUsedPercent.value, 105);
  assert.strictEqual(overlimit.context.contextRemainingPercent.value, -5);
})));

it.live("accepts versioned attached Codex token notifications without summing subset counters or inventing cost", () => fixture(Effect.gen(function*() {

  const breakdown = { inputTokens: 100, cachedInputTokens: 50, outputTokens: 20, reasoningOutputTokens: 10, totalTokens: 120 };
  const envelope = { version: 2, source: "attached-app-server", method: "thread/tokenUsage/updated", params: { threadId: "thread", turnId: "turn", tokenUsage: { last: breakdown, total: { ...breakdown, totalTokens: 600 }, modelContextWindow: null } } };
  const observation = (yield* (projectCodexNotification("machine", envelope, 100)));
  (yield* (decodeSessionInspection(observation)));
  assert.strictEqual(observation.tokens.latestNativeUpdate.totalTokens.value, 120);
  assert.strictEqual(observation.tokens.nativeAggregate.totalTokens.value, 600);
  assert.strictEqual(observation.tokens.latestNativeUpdate.cacheWriteTokens.value, null);
  assert.strictEqual(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  assert.strictEqual(observation.context.contextUsedTokens.status, "unavailable");
  assert.strictEqual(observation.context.contextWindowTokens.value, null);
  (yield* expectFailure((projectCodexNotification("machine", { ...envelope, source: "other-tui" }))));
  (yield* expectFailure((projectCodexNotification("machine", { ...envelope, version: 1 }))));
})));

it.live("keeps private cached usage aged across metadata updates and expires stopped identities", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-native-" }));
  (yield* fs.chmod(directory, 0o700));
  const config: NativeConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: path.join(directory, "metadata") };
    const at = (yield* Clock.currentTimeMillis);
    (yield* (writeNativeCache(config, (yield* (projectClaudeStatus("machine", { session_id: "session", context_window: { total_input_tokens: 0 } }, at))))));
    (yield* (writeNativeCache(config, (yield* (projectHook("claude", "machine", { session_id: "session", hook_event_name: "UserPromptSubmit", prompt: "private" }, at + 1))))));
    const cached = (yield* (readNativeCache(config).pipe(Effect.provide(NodeServices.layer))));
    assert.strictEqual(cached.length, 1);
    assert.strictEqual(cached.at(0)?.tokens.latestNativeUpdate.inputTokens.value, 0);
    assert.strictEqual(cached.at(0)?.tokens.latestNativeUpdate.inputTokens.provenance.observedAt, at);
    assert.strictEqual(cached.at(0)?.presence.activity?.state, "working");
    assert.strictEqual(((yield* fs.stat(config.stateDirectory))).mode & 0o077, 0);
    const namespace = path.join(config.stateDirectory, "native-observations");
    for (const file of (yield* fs.readDirectory(namespace))) assert.strictEqual(((yield* fs.stat(path.join(namespace, file)))).mode & 0o077, 0);
    (yield* (writeNativeCache(config, (yield* (projectHook("claude", "machine", { session_id: "session", hook_event_name: "SessionEnd" }, at + 2))))));
    assert.strictEqual(((yield* (readNativeCache(config).pipe(Effect.provide(NodeServices.layer))))).length, 0);
    const configFile = path.join(directory, "inspector.json");
    (yield* fs.writeFileString(configFile, "{}", { mode: 0o600 }));
    (yield* (readPrivateFile(configFile)));
    (yield* fs.chmod(configFile, 0o644));
    (yield* expectFailure((readPrivateFile(configFile))));
    const link = path.join(directory, "link");
    (yield* fs.symlink(configFile, link));
    (yield* expectFailure((readPrivateFile(link))));
})));

it.live("isolates native observations from legacy journals and reclaims only expired owned typed files", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-cache-" }));
  const config: NativeConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const namespace = path.join(directory, "native-observations");
  const unsupported = "f".repeat(64) + ".json";
  const legacy = "e".repeat(64) + ".json";
  const original = JSON.stringify({ version: 1, journal: "user-owned-rollback" });
    (yield* fs.chmod(directory, 0o700));
    (yield* fs.writeFileString(path.join(directory, legacy), original, { mode: 0o600 }));
    (yield* fs.makeDirectory(path.join(directory, "backups"), { mode: 0o700 }));
    (yield* fs.writeFileString(path.join(directory, "backups", "journal"), "retained", { mode: 0o600 }));
    (yield* fs.makeDirectory(namespace, { mode: 0o700 }));
    (yield* fs.writeFileString(path.join(namespace, unsupported), original, { mode: 0o600 }));
    const old = (yield* (projectHook("codex", "machine", { session_id: "expired", hook_event_name: "Stop" }, 1)));
    (yield* Effect.forEach(Array.from({ length: 1024 }, (_, index) => index), (index) => Effect.gen(function*() {
      const ref = { ...old.presence.ref, nativeSessionId: `expired-${index}` };
      const file = Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(sessionRefKey(ref)))) + ".json";
      (yield* fs.writeFileString(path.join(namespace, file), JSON.stringify({ version: 2, kind: "inspection", expiresAt: 2, inspection: { ...old, presence: { ...old.presence, ref } } }), { mode: 0o600 }));
    }), { concurrency: "unbounded" }));
    const current = (yield* (projectHook("codex", "machine", { session_id: "new", hook_event_name: "Stop" })));
    (yield* (writeNativeCache(config, current)));
    assert.strictEqual(((yield* (readNativeCache(config).pipe(Effect.provide(NodeServices.layer))))).at(0)?.presence.ref.nativeSessionId, "new");
    assert.strictEqual(((yield* fs.readDirectory(namespace))).length, 2);
    assert.strictEqual((yield* fs.readFileString(path.join(namespace, unsupported))), original);
    assert.strictEqual((yield* fs.readFileString(path.join(directory, legacy))), original);
    assert.strictEqual((yield* fs.readFileString(path.join(directory, "backups", "journal"))), "retained");
})));

it.live("keeps native Hermes idle presence tied to the shared host without refreshing native usage or activity", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;

  if (host.platform !== "linux" && host.platform !== "darwin") return;
  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-hermes-" }));
  const config: NativeConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const at = (yield* Clock.currentTimeMillis) - 240000;
  const envelope = { session_id: "hermes-session", cwd: "/workspace", profile: "native-profile", extra: { model: "native-model", provider: "native-provider" } };
    (yield* fs.chmod(directory, 0o700));
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "on_session_start" }, at, host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).at(0)?.presence.activity?.state, "unknown");
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", prompt: "secret-prompt", response: "secret-response", extra: { ...envelope.extra, usage: { prompt_tokens: 100, completion_tokens: 0, cache_read_tokens: 20, cache_write_tokens: 0, reasoning_tokens: 0, total_tokens: 100, secret: "secret-usage" }, response: "secret-provider", messages: ["secret-message"], args: { secret: "secret-tool" } } }, at + 1, host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).at(0)?.presence.activity?.state, "unknown");
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "on_session_end" }, at + 2, host.pid)));
    const cached = (yield* (readNativeCache(config)));
    assert.strictEqual(cached.length, 1);
    const observation = cached.at(0);
    if (!observation) return yield* Effect.die("Missing expected fixture result");
    (yield* (decodeSessionInspection(observation)));
    assert.strictEqual(observation.presence.profile, "native-profile");
    assert.strictEqual(observation.presence.model, "native-model");
    assert.strictEqual(observation.presence.provider, "native-provider");
    assert.strictEqual(observation.presence.cwd, "/workspace");
    assert.strictEqual(observation.presence.activity?.state, "idle");
    assert.strictEqual(observation.presence.lastActivityAt, at + 2);
    assert.ok(observation.presence.observedAt > at + 120000);
    assert.deepStrictEqual(observation.presence.processes?.map((binding) => [binding.pid, binding.ownership]), [[host.pid, "shared"]]);
    const usage = observation.tokens.latestNativeUpdate;
    assert.deepStrictEqual([usage.inputTokens.value, usage.outputTokens.value, usage.cacheReadTokens.value, usage.cacheWriteTokens.value, usage.reasoningOutputTokens.value, usage.totalTokens.value], [100, 0, 20, 0, 0, 100]);
    for (const counter of Object.values(usage)) {
      assert.strictEqual(counter.provenance.observedAt, at + 1);
      assert.strictEqual(counter.provenance.nativeUpdatedAt, null);
    }
    assert.strictEqual(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
    assert.strictEqual(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
    assert.strictEqual(observation.context.contextUsedTokens.status, "unavailable");
    const resources = (yield* (withProcessResources(observation)));
    assert.strictEqual(resources.resources.processes.at(0)?.process.ownership, "shared");
    assert.strictEqual(resources.resources.ownedRssBytes.status, "unavailable");
    assert.strictEqual(resources.resources.ownedCpuPercent.status, "unavailable");
    assert.ok(new TextEncoder().encode(JSON.stringify(resources)).length < INSPECTION_LIMIT);
    const namespace = path.join(directory, "native-observations");
    for (const file of (yield* fs.readDirectory(namespace))) assert.strictEqual(((yield* fs.readFileString(path.join(namespace, file)))).includes("secret"), false);
    (yield* (writeHermesHook(config, { ...envelope, session_id: "", hook_event_name: "on_session_finalize", extra: { old_session_id: "hermes-session" } }, (yield* Clock.currentTimeMillis), host.pid)));
    for (const name of ["post_api_request", "on_session_end"]) (yield* (writeHermesHook(config, { ...envelope, hook_event_name: name, extra: { usage: { total_tokens: 9 } } }, (yield* Clock.currentTimeMillis), host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).length, 0);
})));

it.live("validates native Hermes counters and event whitelist while retaining legacy files", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const crypto = yield* Crypto.Crypto;

  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-hermes-boundary-" }));
  const config: NativeConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const envelope = { session_id: "session", profile: "default", extra: {} };
    (yield* fs.chmod(directory, 0o700));
    const namespace = path.join(directory, "native-observations");
    (yield* fs.makeDirectory(namespace, { mode: 0o700 }));
    const legacy = path.join(namespace, Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(sessionRefKey({ machineId: "machine", harness: "hermes", nativeSessionId: "session" })))) + ".json");
    const original = JSON.stringify({ version: 2, kind: "hermes", expiresAt: 1, native: { machineId: "machine", sessionId: "session", profile: "default", observedAt: 1, lastActivityAt: 1, state: "idle", usage: {} } });
    (yield* fs.writeFileString(legacy, original, { mode: 0o600 }));
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request" }, (yield* Clock.currentTimeMillis), host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).length, 0);
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "on_session_start" }, (yield* Clock.currentTimeMillis), host.pid)));
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: { input_tokens: 0, prompt_tokens: 5, output_tokens: 0, completion_tokens: 8 } } }, (yield* Clock.currentTimeMillis), host.pid)));
    const usage = ((yield* (readNativeCache(config)))).at(0)?.tokens.latestNativeUpdate;
    assert.strictEqual(usage?.inputTokens.value, 0);
    assert.strictEqual(usage?.outputTokens.value, 0);
    assert.strictEqual(usage?.totalTokens.status, "unavailable");
    assert.strictEqual(usage?.reasoningOutputTokens.status, "unavailable");
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, "3", Number.NaN]) {
      (yield* expectFailure((writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: { total_tokens: value } } }, (yield* Clock.currentTimeMillis), host.pid))));
    }
    for (const name of ["pre_api_request", "pre_llm_call", "post_tool_call", "on_conversation_start"]) {
      (yield* expectFailure((writeHermesHook(config, { ...envelope, hook_event_name: name }, (yield* Clock.currentTimeMillis), host.pid))));
    }
    (yield* (writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: null } }, (yield* Clock.currentTimeMillis), host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).at(0)?.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
    assert.strictEqual((yield* fs.readFileString(legacy)), original);
    assert.strictEqual(((yield* fs.readDirectory(namespace))).length, 2);
})));

it.live("does not promote a dead, reused or unavailable Hermes host to live or owned presence", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;

  if (host.platform !== "linux" && host.platform !== "darwin") return;
  const directory = (yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-hermes-host-" }));
  const config: NativeConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(ChildProcess.make(host.executable, ["--input-type=module", "-e", 'import { Effect } from "effect"; import { NodeRuntime } from "@effect/platform-node"; NodeRuntime.runMain(Effect.forever(Effect.sleep("1 hour")));'], { stdin: "ignore", stdout: "ignore", stderr: "ignore" }));
    (yield* fs.chmod(directory, 0o700));
    assert.ok(child.pid);
    (yield* (writeHermesHook(config, { session_id: "dead", hook_event_name: "on_session_start", extra: {} }, (yield* Clock.currentTimeMillis), child.pid)));
    (yield* child.kill());
    yield* child.isRunning.pipe(Effect.repeat({ while: (running) => running, schedule: Schedule.spaced("1 millis") }), Effect.timeout("5 seconds"));
    assert.strictEqual((yield* readNativeCache(config)).length, 0);
    const missingAt = (yield* Clock.currentTimeMillis) - 1000;
    (yield* (writeHermesHook(config, { session_id: "missing", hook_event_name: "on_session_start", extra: {} }, missingAt, child.pid)));
    const missing = ((yield* (readNativeCache(config)))).at(0);
    assert.strictEqual(missing?.presence.ref.nativeSessionId, "missing");
    assert.strictEqual(missing?.presence.observedAt, missingAt);
    assert.strictEqual(missing?.presence.processes?.length, 0);
    assert.strictEqual(missing?.resources.ownedRssBytes.status, "unavailable");
    (yield* (writeHermesHook(config, { session_id: "expired-missing", hook_event_name: "on_session_start", extra: {} }, (yield* Clock.currentTimeMillis) - 240000, child.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).some((item) => item.presence.ref.nativeSessionId === "expired-missing"), false);
    const reused = (yield* (projectHook("claude", "machine", { session_id: "reused", hook_event_name: "Stop" })));
    (yield* (writeNativeCache(config, { ...reused, presence: { ...reused.presence, ref: { ...reused.presence.ref, harness: "hermes" }, processes: [{ pid: host.pid, startIdentity: "different-birth", ownership: "shared" }] } })));
    assert.strictEqual(((yield* (readNativeCache(config)))).some((item) => item.presence.ref.nativeSessionId === "reused"), false);
    (yield* (writeHermesHook(config, { session_id: "reused", hook_event_name: "on_session_end", extra: {} }, (yield* Clock.currentTimeMillis), host.pid)));
    assert.strictEqual(((yield* (readNativeCache(config)))).some((item) => item.presence.ref.nativeSessionId === "reused"), false);
})));

it.live("bounds encoded process details without losing aggregate scope or claiming complete rows", () => fixture(Effect.gen(function*() {
  const host = yield* Host;
  const crypto = yield* Crypto.Crypto;

  if (host.platform !== "linux") return;
  const boot = "11111111-1111-4111-8111-111111111111";
  const files = FileSystem.makeNoop({
    readDirectory: () => Effect.succeed(Array.from({ length: 256 }, (_, index) => String(index + 1))),
    readFileString: (file) => Effect.sync(() => {
      if (file === "/proc/sys/kernel/random/boot_id") return boot;
      const pid = Number(file.split("/").at(2));
      const fields = Array.from({ length: 22 }, (_, index) => {
        if (index === 0) return "S";
        if (index === 1) return pid === 1 ? "0" : "1";
        if (index === 11) return "10";
        if (index === 12) return "2";
        if (index === 19) return String(pid);
        if (index === 21) return "3";
        return "0";
      });
      return `${pid} (native fixture) ${fields.join(" ")}`;
    }),
  });
  const native = (yield* (projectHook("claude", "machine", { session_id: "large-tree", hook_event_name: "Stop" })));
  const observation = (yield* (withProcessResources({ ...native, presence: { ...native.presence, processes: [{ pid: 1, startIdentity: `linux:${boot}:1`, ownership: "owned" }] } }).pipe(Effect.provideService(FileSystem.FileSystem, files), Effect.provide(NodeServices.layer))));
  (yield* (decodeSessionInspection(observation)));
  assert.ok(observation.resources.processes.length > 0);
  assert.ok(observation.resources.processes.length < 256);
  assert.ok(new TextEncoder().encode(JSON.stringify(observation)).length <= INSPECTION_LIMIT - 2 * 1024);
  assert.strictEqual(observation.resources.ownedProcessCount.value, 256);
  assert.strictEqual(observation.resources.ownedProcessCount.provenance.scope, "owned-processes");
  assert.strictEqual(observation.resources.ownedProcessCount.provenance.partial, true);
  assert.strictEqual(observation.resources.processes.at(0)?.rssBytes.provenance.partial, true);
  const actual = (yield* (Effect.scoped(Effect.gen(function*() {
    const publisher: InspectorPrincipal = { id: "native", owner: "owner", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode("native-secret"))) };
    const registry = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [publisher] });
    const identity = { connectorId: "native-tree", incarnation: "first" };
    yield* registry.announce(publisher, { ...identity, machineId: "machine", sessions: [observation.presence] });
    const query = yield* Effect.forkChild(registry.inspect(publisher, { ref: observation.presence.ref }), { startImmediately: true });
    const request = (yield* registry.poll(publisher, identity)).requests.at(0);
    if (!request) return yield* Effect.die("Missing expected fixture result");
    yield* registry.complete(publisher, { ...identity, requestId: request.requestId, observation });
    return yield* Fiber.join(query);
  }))));
  assert.ok(new TextEncoder().encode(JSON.stringify(actual)).length <= INSPECTION_LIMIT);
  assert.strictEqual(actual.resources.processes.length, observation.resources.processes.length);
  assert.strictEqual(actual.resources.ownedProcessCount.value, 256);
  assert.strictEqual(actual.resources.ownedProcessCount.provenance.scope, "owned-processes");
  assert.strictEqual(actual.resources.ownedProcessCount.provenance.partial, true);
  assert.strictEqual(actual.resources.processes.at(0)?.rssBytes.provenance.partial, true);
  const firstRow = actual.resources.processes.at(0);
  if (!firstRow) return yield* Effect.die("Missing expected fixture result");
  if (firstRow.rssBytes.value === null) return yield* Effect.die("Missing RSS");
  assert.strictEqual(actual.resources.ownedRssBytes.value, firstRow.rssBytes.value * 256);
  assert.strictEqual(actual.resources.ownedRssBytes.provenance.scope, "owned-processes");
  assert.strictEqual(actual.resources.ownedRssBytes.provenance.partial, true);
})));

it.live("keeps actual CLI auth and native diagnostics safe on stderr, hooks exit-safe and cancellation quiet", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-cli-errors-" });
  yield* fs.chmod(home, 0o700);
  yield* fs.writeFileString(path.join(home, "claude"), "#!/bin/sh\nprintf '[]\\n'\n", { mode: 0o700 });
  const polled = yield* Deferred.make<void>();
  let status = 401;
  let disconnects = 0;
  const server = yield* HttpServer.HttpServer;
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.url === "/connector/disconnect") {
      disconnects++;
      return HttpServerResponse.jsonUnsafe({ accepted: true });
    }
    if (status !== 200) return HttpServerResponse.text("PRIVATE_AUTH_REJECTION", { status });
    if (request.url === "/connector/poll") {
      yield* Deferred.succeed(polled, undefined);
      return HttpServerResponse.jsonUnsafe({ requests: [] });
    }
    return HttpServerResponse.jsonUnsafe({ accepted: true });
  }));
  for (const scenario of ["unauthorized", "forbidden", "missing", "hook", "cancel"]) {
    status = scenario === "forbidden" ? 403 : scenario === "cancel" ? 200 : 401;
    const beforeDisconnects = disconnects;
    const child = yield* spawner.spawn(ChildProcess.make(host.executable, [
      "--experimental-strip-types", new URL("../src/main.ts", import.meta.url).pathname,
      ...(scenario === "hook" ? ["hook", "claude"] : scenario === "missing" ? ["list"] : ["connector"]),
    ], {
      env: {
        HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
        PATH: home,
        COORDINATOR_CONNECTOR_CONFIG: undefined,
        COORDINATOR_API_URL: scenario === "missing" ? undefined : HttpServer.formatAddress(server.address),
        COORDINATOR_API_TOKEN: scenario === "missing" ? undefined : "PRIVATE_CLI_CREDENTIAL",
      },
      extendEnv: true, stdin: Stream.make(new TextEncoder().encode(scenario === "hook" ? "{" : "")),
    }));
    const captured = yield* Effect.all([
      child.stdout.pipe(Stream.decodeText(), Stream.mkString),
      child.stderr.pipe(Stream.decodeText(), Stream.mkString),
    ], { concurrency: "unbounded" }).pipe(Effect.forkChild);
    if (scenario === "cancel") {
      yield* Deferred.await(polled);
      yield* child.kill();
    }
    const code = Number(yield* child.exitCode);
    const [stdout, stderr] = yield* Fiber.join(captured);
    assert.strictEqual(stdout, "", "CLI diagnostics must not contaminate stdout");
    for (const secret of ["PRIVATE_AUTH_REJECTION", "PRIVATE_CLI_CREDENTIAL", home]) assert.strictEqual(stderr.includes(secret), false);
    if (scenario === "cancel") {
      assert.strictEqual(stderr, "");
      assert.strictEqual(disconnects - beforeDisconnects, 1, "Cancellation must complete advisory disconnect");
    } else {
      assert.strictEqual(code === 0, scenario === "hook", stderr);
      const diagnostic = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({
        reasons: Schema.Array(Schema.Struct({
          kind: Schema.String, code: Schema.String, status: Schema.optional(Schema.Number), operation: Schema.optional(Schema.String),
        })),
      })))(stderr);
      assert.strictEqual(diagnostic.reasons.length, 1);
      const reason = diagnostic.reasons.at(0);
      assert.strictEqual(reason?.kind, "Fail");
      assert.strictEqual(reason?.code, scenario === "missing" || scenario === "hook" ? "NativeError" : "HttpError");
      if (scenario === "unauthorized" || scenario === "forbidden") assert.strictEqual(reason?.status, status);
      else assert.isNotEmpty(reason?.operation);
      if (scenario === "unauthorized" || scenario === "forbidden") assert.strictEqual(disconnects - beforeDisconnects, 1);
    }
  }
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0)))), 30000);

it.effect("posts detail errors without actionable notices and keeps cancellation and advisory disconnect quiet", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-detail-errors-" });
  const enteredTimeout = yield* Deferred.make<void>();
  const completed = yield* Deferred.make<void>();
  const notices: NativeNotice[] = [];
  const results: Array<{ requestId: string; code: string }> = [];
  const diagnostics: Array<{ level: string; message: unknown }> = [];
  let polls = 0;
  let disconnects = 0;
  let cancelledDetails = 0;
  const server = yield* HttpServer.HttpServer;
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.url === "/connector/disconnect") {
      disconnects++;
      return HttpServerResponse.text("PRIVATE_DISCONNECT_REJECTION", { status: 503 });
    }
    if (request.url === "/connector/poll") {
      polls++;
      return HttpServerResponse.jsonUnsafe({ requests: polls === 1
        ? ["missing-one", "missing-two", "unavailable", "timeout"].map((id) => ({
          requestId: id, ref: { machineId: "machine", harness: "pi", nativeSessionId: id },
        })) : [] });
    }
    if (request.url === "/connector/result") {
      const result = yield* decodeConnectorResult(yield* request.json);
      if (!("code" in result.observation)) return yield* Effect.die("Expected error observation");
      results.push({ requestId: result.requestId, code: result.observation.code });
      if (results.length === 4) yield* Deferred.succeed(completed, undefined);
    }
    return HttpServerResponse.jsonUnsafe({ accepted: true });
  }));
  const connector = yield* connectNative({
    inspectorUrl: HttpServer.formatAddress(server.address), apiToken: "isolated-detail-credential", machineId: "machine", stateDirectory: directory,
  }, {
    presence: () => Effect.succeed([]),
    inspect: (ref) => ref.nativeSessionId === "timeout"
      ? Deferred.succeed(enteredTimeout, undefined).pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sync(() => { cancelledDetails++; })))
      : Effect.fail(new HttpError({ status: ref.nativeSessionId === "unavailable" ? 503 : 404, message: "PRIVATE_DETAIL_REJECTION" })),
    report: (notice) => Effect.sync(() => { notices.push(notice); }),
  }).pipe(
    Effect.scoped,
    Effect.provide(Layer.mergeAll(Mcp.layer, Logger.layer([Logger.make((entry) => { diagnostics.push({ level: entry.logLevel, message: entry.message }); })]))),
    Effect.provideService(References.MinimumLogLevel, "Debug"),
    Effect.forkChild,
  );
  yield* Deferred.await(enteredTimeout);
  yield* TestClock.adjust("8 seconds");
  yield* Deferred.await(completed);
  yield* Fiber.interrupt(connector);
  const exit = yield* Fiber.await(connector);
  assert.strictEqual(exit._tag, "Failure");
  if (exit._tag === "Failure") assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true);
  assert.deepStrictEqual(results, [
    { requestId: "missing-one", code: "not-found" },
    { requestId: "missing-two", code: "not-found" },
    { requestId: "unavailable", code: "unavailable" },
    { requestId: "timeout", code: "timeout" },
  ]);
  assert.strictEqual(polls, 1, "Detail failures must not restart the connector");
  assert.strictEqual(cancelledDetails, 1, "Timed-out detail getter must be interrupted");
  assert.strictEqual(disconnects, 1);
  assert.deepStrictEqual(notices, []);
  for (const entry of diagnostics) assert.strictEqual(entry.level, "Debug");
  const diagnosticText = JSON.stringify(diagnostics);
  for (const secret of ["PRIVATE_DETAIL_REJECTION", "PRIVATE_DISCONNECT_REJECTION", "isolated-detail-credential"]) {
    assert.strictEqual(diagnosticText.includes(secret), false);
  }
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0)))));

it.effect("terminates the connection and its polling peer when the presence getter defects", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-presence-failure-" });
  const tokenFile = path.join(directory, "token");
  yield* fs.writeFileString(tokenFile, "isolated-presence-credential", { mode: 0o600 });
  const firstPoll = yield* Deferred.make<void>();
  let disconnects = 0;
  const server = yield* HttpServer.HttpServer;
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.url === "/connector/disconnect") disconnects++;
    if (request.url === "/connector/poll") yield* Deferred.succeed(firstPoll, undefined);
    return HttpServerResponse.jsonUnsafe(request.url === "/connector/poll" ? { requests: [] } : { accepted: true });
  }));
  let presenceCalls = 0;
  const connector = yield* connectNative({
    inspectorUrl: HttpServer.formatAddress(server.address), tokenFile, machineId: "machine", stateDirectory: directory,
  }, {
    presence: () => Effect.sync(() => {
      if (++presenceCalls === 2) throw new Error("Native presence getter failed");
      return [];
    }),
    inspect: () => Effect.die("No detail requests expected"),
  }).pipe(Effect.scoped, Effect.provide(Mcp.layer), Effect.forkChild);
  yield* Deferred.await(firstPoll);
  yield* TestClock.adjust("10 seconds");
  const exit = yield* Fiber.await(connector);
  assert.strictEqual(exit._tag, "Failure");
  if (exit._tag === "Failure") assert.strictEqual(Cause.hasDies(exit.cause), true);
  assert.strictEqual(presenceCalls, 2);
  assert.strictEqual(disconnects, 1);
}).pipe(Effect.provide(serverLayer("127.0.0.1", 0)))));

it.effect("backs off with bounded jitter and keeps intermittent connections quiet until stable recovery", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-quiet-retry-" });
  const tokenFile = path.join(directory, "token");
  yield* fs.writeFileString(tokenFile, "isolated-retry-credential", { mode: 0o600 });
  const clock = yield* Clock.Clock;
  const now = () => Number(clock.monotonicTimeNanosUnsafe() / 1_000_000n);
  const epoch = now();
  const started = yield* Deferred.make<void>();
  const transport = yield* Mcp.make;
  const attempts: number[] = [];
  const successfulPolls: number[] = [];
  const notices: NativeNotice[] = [];
  let isFailing = true;
  let status = 503;
  let random = 0.5;
  const post: typeof transport.post = (_url, _token, route, _input, output) => Effect.gen(function*() {
    if (route !== "/connector/disconnect" && isFailing) {
      attempts.push(now() - epoch);
      yield* Deferred.succeed(started, undefined);
      return yield* new HttpError({ status, message: "Peer unavailable" });
    }
    if (route === "/connector/poll") successfulPolls.push(now() - epoch);
    return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(output))(route === "/connector/poll" ? { requests: [] } : { accepted: true }).pipe(
      Effect.mapError(() => new HttpError({ status: 400, message: "Invalid peer response" })),
    );
  });
  const connector = yield* connectNative({
    inspectorUrl: "http://127.0.0.1:8080", tokenFile, machineId: "machine", stateDirectory: directory,
  }, {
    presence: () => Effect.succeed([]),
    inspect: () => Effect.die("No detail requests expected"),
    report: (notice) => Effect.sync(() => { notices.push(notice); }),
  }).pipe(
    Effect.scoped,
    Effect.provideService(Mcp, { ...transport, post }),
    Effect.provideService(Random.Random, { nextDoubleUnsafe: () => random, nextIntUnsafe: () => 0 }),
    Effect.forkChild,
  );
  yield* Deferred.await(started);
  assert.strictEqual(notices.length, 0);
  yield* TestClock.adjust(1999);
  assert.deepStrictEqual(attempts, [0]);
  yield* TestClock.adjust(1);
  assert.deepStrictEqual(attempts, [0, 2000]);
  yield* TestClock.adjust(2999);
  assert.strictEqual(notices.length, 0);
  yield* TestClock.adjust(1001);
  assert.deepStrictEqual(notices.map((notice) => notice.state), ["unavailable"]);
  assert.strictEqual(notices.at(0)?.status, 503);
  for (const delay of [8000, 16000, 30000, 30000]) yield* TestClock.adjust(delay);
  assert.deepStrictEqual(attempts, [0, 2000, 6000, 14000, 30000, 60000, 90000]);
  assert.strictEqual(notices.length, 1);

  isFailing = false;
  yield* TestClock.adjust(30000);
  yield* TestClock.adjust(9000);
  assert.strictEqual(notices.length, 1);
  isFailing = true;
  yield* TestClock.adjust(1000);
  assert.strictEqual(notices.length, 1, "One successful poll must not reset the warning latch");
  isFailing = false;
  yield* TestClock.adjust(30000);
  yield* TestClock.adjust(9000);
  assert.strictEqual(notices.length, 1);
  yield* TestClock.adjust(1000);
  assert.deepStrictEqual(notices.map((notice) => notice.state), ["unavailable", "recovered"]);

  isFailing = true;
  random = 0;
  yield* TestClock.adjust(1000);
  const briefAttempts = attempts.length;
  yield* TestClock.adjust(1599);
  assert.strictEqual(attempts.length, briefAttempts);
  isFailing = false;
  yield* TestClock.adjust(1);
  assert.strictEqual(successfulPolls.at(-1), 172600, "Stable recovery must reset backoff and retain negative jitter");
  yield* TestClock.adjust(10000);
  assert.strictEqual(notices.length, 2, "A brief outage must emit neither warning nor recovery");

  isFailing = true;
  random = 0.75;
  yield* TestClock.adjust(1000);
  const jitterStart = attempts.length - 1;
  const jitterAt = attempts.at(-1);
  if (jitterAt === undefined) return yield* Effect.die("Missing jitter attempt");
  yield* TestClock.adjust(2199);
  assert.strictEqual(attempts.length, jitterStart + 1);
  for (const delay of [1, 4400, 8800, 17600, 30000, 30000]) yield* TestClock.adjust(delay);
  assert.deepStrictEqual(attempts.slice(jitterStart), [
    jitterAt, jitterAt + 2200, jitterAt + 6600, jitterAt + 15400,
    jitterAt + 33000, jitterAt + 63000, jitterAt + 93000,
  ]);
  assert.strictEqual(notices.length, 3);
  isFailing = false;
  yield* TestClock.adjust(30000);
  yield* TestClock.adjust(10000);
  assert.deepStrictEqual(notices.map((notice) => notice.state), ["unavailable", "recovered", "unavailable", "recovered"]);

  status = 400;
  isFailing = true;
  yield* TestClock.adjust(1000);
  const exit = yield* Fiber.await(connector);
  assert.strictEqual(exit._tag, "Failure");
  if (exit._tag === "Failure") {
    const error = Cause.findErrorOption(exit.cause);
    assert.strictEqual(error._tag, "Some");
    if (error._tag === "Some" && error.value._tag === "HttpError") assert.strictEqual(error.value.status, 400);
  }
})));

it.live("rejects dangling default configuration symlinks instead of falling back to environment", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-default-link-" });
  const root = path.join(directory, "coordinator");
  yield* fs.makeDirectory(root, { mode: 0o700 });
  yield* fs.symlink(path.join(directory, "missing"), path.join(root, "inspector.json"));
  yield* expectFailure(loadConnectorConfig().pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord({ XDG_CONFIG_HOME: directory, COORDINATOR_API_URL: "https://env.example", COORDINATOR_API_TOKEN: "environment-credential" }))));
})));

it.live("loops over partial private reads and rejects growth beyond the bound", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-partial-read-" });
  const target = path.join(directory, "private");
  const text = "private metadata ".repeat(20);
  yield* fs.writeFileString(target, text, { mode: 0o600 });
  const partial = FileSystem.make({ ...fs, open: (file, options) => fs.open(file, options).pipe(Effect.map((handle) => delegateFile(handle, { read: (buffer, options) => handle.read(buffer.subarray(0, 3), options) }))) });
  assert.strictEqual(yield* readPrivateFile(target).pipe(Effect.provideService(FileSystem.FileSystem, partial)), text);
  const growing = FileSystem.make({ ...fs, open: (file, options) => fs.open(file, options).pipe(Effect.map((handle) => delegateFile(handle, { read: (buffer) => Effect.sync(() => { buffer.fill(97); return buffer.length; }) }))) });
  yield* expectFailure(readPrivateFile(target).pipe(Effect.provideService(FileSystem.FileSystem, growing)));
})));

it.live("cleans identity and cache temporaries when interrupted at exclusive acquisition or during write", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-interrupted-write-" });
  for (const writer of ["identity", "cache"]) {
    for (const boundary of ["open", "write"]) {
      const base = path.join(directory, writer + "-" + boundary);
      const settings = ConfigProvider.fromEnvRecord({ XDG_STATE_HOME: base });
      const config = yield* resolveNativeConfig({ inspectorUrl: "https://inspector.example" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, settings));
      if (writer === "identity") yield* fs.remove(path.join(config.stateDirectory, "machine-id"));
      const acquired = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const injected = FileSystem.make({ ...fs, open: (file, options) => Effect.gen(function*() {
        const handle = yield* fs.open(file, options);
        if (options?.flag !== "wx") return handle;
        if (boundary === "open") {
          yield* Deferred.succeed(acquired, undefined);
          yield* Deferred.await(resume);
        }
        return delegateFile(handle, { writeAll: (bytes) => boundary === "write"
          ? Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Effect.never))
          : handle.writeAll(bytes) });
      }) });
      const operation = writer === "identity"
        ? resolveNativeConfig({ inspectorUrl: "https://inspector.example" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, settings))
        : projectHook("claude", config.machineId, { session_id: "cancelled", hook_event_name: "SessionStart" }).pipe(Effect.flatMap((inspection) => writeNativeCache(config, inspection)));
      const fiber = yield* operation.pipe(Effect.provideService(FileSystem.FileSystem, injected), Effect.forkChild);
      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
      // The interrupt request must arrive while the acquired handle is still masked.
      yield* Effect.yieldNow;
      yield* Deferred.succeed(resume, undefined);
      const exit = yield* Fiber.await(fiber);
      assert.strictEqual(exit._tag, "Failure");
      if (exit._tag === "Failure") assert.isTrue(Cause.hasInterrupts(exit.cause));
      assert.deepStrictEqual(yield* fs.readDirectory(writer === "identity" ? config.stateDirectory : path.join(config.stateDirectory, "native-observations")), []);
    }
  }
})));

it.live("sanitizes filesystem close failures, recovers cache reads, and preserves programmer defects", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-close-failure-" });
  const config = yield* resolveNativeConfig({ inspectorUrl: "https://inspector.example" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord({ XDG_STATE_HOME: directory })));
  const inspection = yield* projectHook("claude", config.machineId, { session_id: "close", hook_event_name: "SessionStart" });
  yield* writeNativeCache(config, inspection);
  const cacheDirectory = path.join(config.stateDirectory, "native-observations");
  const entry = (yield* fs.readDirectory(cacheDirectory)).at(0);
  if (entry === undefined) return yield* Effect.die("Missing cache fixture");
  const file = path.join(cacheDirectory, entry);
  const programmerDefect = new Error("programmer defect");
  for (const defect of [PlatformError.systemError({ _tag: "BadResource", module: "FileSystem", method: "close", pathOrDescriptor: "secret-close-path" }), programmerDefect]) {
    const injected = FileSystem.make({ ...fs, open: (target, options) => Effect.gen(function*() {
      const handle = yield* fs.open(target, options);
      yield* Effect.addFinalizer(() => Effect.die(defect));
      return handle;
    }) });
    const exit = yield* Effect.exit(readPrivateFile(file).pipe(Effect.provideService(FileSystem.FileSystem, injected)));
    if (exit._tag !== "Failure") return yield* Effect.die("Expected close failure");
    if (defect === programmerDefect) {
      assert.isTrue(exit.cause.reasons.some((reason) => reason._tag === "Die" && reason.defect === programmerDefect));
      const cacheExit = yield* Effect.exit(readNativeCache(config).pipe(Effect.provideService(FileSystem.FileSystem, injected)));
      assert.strictEqual(cacheExit._tag, "Failure");
      if (cacheExit._tag === "Failure") assert.isTrue(cacheExit.cause.reasons.some((reason) => reason._tag === "Die" && reason.defect === programmerDefect));
    } else {
      assert.isTrue(exit.cause.reasons.every((reason) => reason._tag === "Fail" && reason.error._tag === "NativeError" && reason.error.operation === "Read private file"));
      assert.deepStrictEqual(yield* readNativeCache(config).pipe(Effect.provideService(FileSystem.FileSystem, injected)), []);
      const failingWriter = FileSystem.make({ ...fs, open: (target, options) => Effect.gen(function*() {
        const handle = yield* fs.open(target, options);
        if (options?.flag === "wx") yield* Effect.addFinalizer(() => Effect.die(defect));
        return handle;
      }) });
      const writeExit = yield* Effect.exit(writeNativeCache(config, inspection).pipe(Effect.provideService(FileSystem.FileSystem, failingWriter)));
      if (writeExit._tag !== "Failure") return yield* Effect.die("Expected cache close failure");
      assert.isTrue(writeExit.cause.reasons.every((reason) => reason._tag === "Fail" && reason.error._tag === "NativeError" && reason.error.operation === "Write private metadata cache"));
      assert.deepStrictEqual(yield* fs.readDirectory(cacheDirectory), [entry]);
    }
  }
})));

it.live("rejects non-executable runtimes without executing the selected file", () => fixture(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-executable-" });
  const runtime = path.join(directory, "runtime");
  yield* fs.writeFileString(runtime, '#!/bin/sh\nprintf executed > "$0.executed"\n', { mode: 0o600 });
  const error = yield* Effect.flip(checkExecutable(runtime));
  assert.strictEqual(error.reason._tag, "PermissionDenied");
  yield* fs.chmod(runtime, 0o700);
  yield* checkExecutable(runtime);
  assert.isFalse(yield* fs.exists(runtime + ".executed"));
})));
