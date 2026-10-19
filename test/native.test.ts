import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import { once } from "node:events";
import * as path from "node:path";
import { Effect, Fiber, FileSystem } from "effect";
import { it } from "@effect/vitest";
import { ChildProcessSpawner } from "effect/process";
import { NodeServices } from "@effect/platform-node";
import { projectHook, projectClaudeStatus, projectCodexNotification, writeNativeCache, writeHermesHook, readNativeCache, readPrivateFile, loadConnectorConfig, withProcessResources } from "../src/native.ts";
import { INSPECTION_LIMIT, decodeSessionInspection, sessionRefKey, type ConnectorConfig, type InspectorPrincipal } from "../src/inspection.ts";
import { Inspector } from "../src/inspector.ts";

const runNative = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner>) => Effect.runPromise(effect.pipe(Effect.provide(NodeServices.layer)));

it("projects Claude metadata without prompt, transcript, result or invented PID", async () => {
  const observation = await Effect.runPromise(projectHook("claude", "machine", { session_id: "session", hook_event_name: "PermissionRequest", cwd: "/workspace", tool_name: "Bash", prompt: "secret-prompt", transcript_path: "/secret/transcript", tool_input: { command: "secret-command" }, tool_response: "secret-result", pid: 123, ppid: 456 }, 100));
  await Effect.runPromise(decodeSessionInspection(observation));
  assert.equal(observation.presence.activity?.approval, "pending");
  assert.equal(observation.presence.processes, undefined);
  assert.equal(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.equal(JSON.stringify(observation).includes("secret"), false);
  assert.equal((await Effect.runPromise(withProcessResources(observation).pipe(Effect.provide(NodeServices.layer)))).resources.ownedProcessCount.status, "unavailable");
});

it("keeps Claude current context input-only, cached and distinct from aggregate spend", async () => {
  const observation = await Effect.runPromise(projectClaudeStatus("machine", { session_id: "session", transcript_path: "/secret", context_window: { total_input_tokens: 400, total_output_tokens: 80, context_window_size: 2000, current_usage: { input_tokens: 100, output_tokens: 80, cache_read_input_tokens: 200, cache_creation_input_tokens: 100 }, used_percentage: 20, remaining_percentage: 80 }, cost: { total_cost_usd: 0.3, total_duration_ms: 1500, total_api_duration_ms: 400 } }, 300));
  await Effect.runPromise(decodeSessionInspection(observation));
  assert.equal(observation.context.contextUsedTokens.value, 400);
  assert.equal(observation.context.contextUsedPercent.value, 20);
  assert.equal(observation.context.contextRemainingTokens.value, 1600);
  assert.equal(observation.tokens.latestNativeUpdate.outputTokens.value, 80);
  assert.equal(observation.tokens.latestNativeUpdate.totalTokens.status, "unavailable");
  assert.equal(observation.tokens.nativeAggregate.inputTokens.status, "unavailable");
  assert.equal(observation.cost.nativeAggregate.totalCostUsd.provenance.estimated, true);
  assert.equal(observation.activity.sessionRunningMs.value, 1500);
  assert.equal(observation.activity.apiWaitMs.value, 400);
  assert.equal(observation.context.contextUsedTokens.provenance.nativeUpdatedAt, null);
  assert.equal(observation.context.contextUsedTokens.provenance.stale, false);
  const empty = await Effect.runPromise(projectClaudeStatus("machine", { session_id: "session", context_window: { current_usage: null, context_window_size: 2000, used_percentage: null } }, 400));
  assert.equal(empty.context.contextUsedTokens.value, null);
  assert.equal(empty.context.contextUsedPercent.status, "unavailable");
  const partial = await Effect.runPromise(projectClaudeStatus("machine", { session_id: "session", context_window: { context_window_size: 2000, current_usage: { input_tokens: 100, cache_read_input_tokens: 30 }, used_percentage: 12 } }, 500));
  assert.equal(partial.context.contextUsedTokens.status, "unavailable");
  assert.equal(partial.context.contextRemainingTokens.status, "unavailable");
  assert.equal(partial.context.contextUsedPercent.value, 12);
  const overlimit = await Effect.runPromise(projectClaudeStatus("machine", { session_id: "overlimit", context_window: { used_percentage: 105, remaining_percentage: -5 } }));
  await Effect.runPromise(decodeSessionInspection(overlimit));
  assert.equal(overlimit.context.contextUsedPercent.value, 105);
  assert.equal(overlimit.context.contextRemainingPercent.value, -5);
});

it("accepts versioned attached Codex token notifications without summing subset counters or inventing cost", async () => {
  const breakdown = { inputTokens: 100, cachedInputTokens: 50, outputTokens: 20, reasoningOutputTokens: 10, totalTokens: 120 };
  const envelope = { version: 2, source: "attached-app-server", method: "thread/tokenUsage/updated", params: { threadId: "thread", turnId: "turn", tokenUsage: { last: breakdown, total: { ...breakdown, totalTokens: 600 }, modelContextWindow: null } } };
  const observation = await Effect.runPromise(projectCodexNotification("machine", envelope, 100));
  await Effect.runPromise(decodeSessionInspection(observation));
  assert.equal(observation.tokens.latestNativeUpdate.totalTokens.value, 120);
  assert.equal(observation.tokens.nativeAggregate.totalTokens.value, 600);
  assert.equal(observation.tokens.latestNativeUpdate.cacheWriteTokens.value, null);
  assert.equal(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
  assert.equal(observation.context.contextUsedTokens.status, "unavailable");
  assert.equal(observation.context.contextWindowTokens.value, null);
  await assert.rejects(Effect.runPromise(projectCodexNotification("machine", { ...envelope, source: "other-tui" })));
  await assert.rejects(Effect.runPromise(projectCodexNotification("machine", { ...envelope, version: 1 })));
});

it("keeps private cached usage aged across metadata updates and expires stopped identities", async () => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), "coordinator-native-"));
  await fs.chmod(directory, 0o700);
  const config: ConnectorConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: path.join(directory, "metadata") };
  try {
    const at = Date.now();
    await runNative(writeNativeCache(config, await Effect.runPromise(projectClaudeStatus("machine", { session_id: "session", context_window: { total_input_tokens: 0 } }, at))));
    await runNative(writeNativeCache(config, await Effect.runPromise(projectHook("claude", "machine", { session_id: "session", hook_event_name: "UserPromptSubmit", prompt: "private" }, at + 1))));
    const cached = await Effect.runPromise(readNativeCache(config).pipe(Effect.provide(NodeServices.layer)));
    assert.equal(cached.length, 1);
    assert.equal(cached.at(0)?.tokens.latestNativeUpdate.inputTokens.value, 0);
    assert.equal(cached.at(0)?.tokens.latestNativeUpdate.inputTokens.provenance.observedAt, at);
    assert.equal(cached.at(0)?.presence.activity?.state, "working");
    assert.equal((await fs.stat(config.stateDirectory)).mode & 0o077, 0);
    const namespace = path.join(config.stateDirectory, "native-observations");
    for (const file of await fs.readdir(namespace)) assert.equal((await fs.stat(path.join(namespace, file))).mode & 0o077, 0);
    await runNative(writeNativeCache(config, await Effect.runPromise(projectHook("claude", "machine", { session_id: "session", hook_event_name: "SessionEnd" }, at + 2))));
    assert.equal((await Effect.runPromise(readNativeCache(config).pipe(Effect.provide(NodeServices.layer)))).length, 0);
    const configFile = path.join(directory, "inspector.json");
    await fs.writeFile(configFile, JSON.stringify(config), { mode: 0o600 });
    assert.deepEqual(await Effect.runPromise(loadConnectorConfig(configFile)), config);
    await fs.chmod(configFile, 0o644);
    await assert.rejects(Effect.runPromise(readPrivateFile(configFile)));
    const link = path.join(directory, "link");
    await fs.symlink(configFile, link);
    await assert.rejects(Effect.runPromise(readPrivateFile(link)));
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

it("isolates native observations from legacy journals and reclaims only expired owned typed files", async () => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), "coordinator-cache-"));
  const config: ConnectorConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const namespace = path.join(directory, "native-observations");
  const unsupported = "f".repeat(64) + ".json";
  const legacy = "e".repeat(64) + ".json";
  const original = JSON.stringify({ version: 1, journal: "user-owned-rollback" });
  try {
    await fs.chmod(directory, 0o700);
    await fs.writeFile(path.join(directory, legacy), original, { mode: 0o600 });
    await fs.mkdir(path.join(directory, "backups"), { mode: 0o700 });
    await fs.writeFile(path.join(directory, "backups", "journal"), "retained", { mode: 0o600 });
    await fs.mkdir(namespace, { mode: 0o700 });
    await fs.writeFile(path.join(namespace, unsupported), original, { mode: 0o600 });
    const old = await Effect.runPromise(projectHook("codex", "machine", { session_id: "expired", hook_event_name: "Stop" }, 1));
    await Promise.all(Array.from({ length: 1024 }, async (_, index) => {
      const ref = { ...old.presence.ref, nativeSessionId: `expired-${index}` };
      const file = createHash("sha256").update(sessionRefKey(ref)).digest("hex") + ".json";
      await fs.writeFile(path.join(namespace, file), JSON.stringify({ version: 2, kind: "inspection", expiresAt: 2, inspection: { ...old, presence: { ...old.presence, ref } } }), { mode: 0o600 });
    }));
    const current = await Effect.runPromise(projectHook("codex", "machine", { session_id: "new", hook_event_name: "Stop" }));
    await runNative(writeNativeCache(config, current));
    assert.equal((await Effect.runPromise(readNativeCache(config).pipe(Effect.provide(NodeServices.layer)))).at(0)?.presence.ref.nativeSessionId, "new");
    assert.equal((await fs.readdir(namespace)).length, 2);
    assert.equal(await fs.readFile(path.join(namespace, unsupported), "utf8"), original);
    assert.equal(await fs.readFile(path.join(directory, legacy), "utf8"), original);
    assert.equal(await fs.readFile(path.join(directory, "backups", "journal"), "utf8"), "retained");
    assert.equal((await fs.readdir(directory)).length, 3);
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

it("keeps native Hermes idle presence tied to the shared host without refreshing native usage or activity", async () => {
  if (process.platform !== "linux" && process.platform !== "darwin") return;
  const directory = await fs.mkdtemp(path.join(tmpdir(), "coordinator-hermes-"));
  const config: ConnectorConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const at = Date.now() - 240000;
  const envelope = { session_id: "hermes-session", cwd: "/workspace", profile: "native-profile", extra: { model: "native-model", provider: "native-provider" } };
  try {
    await fs.chmod(directory, 0o700);
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "on_session_start" }, at, process.pid));
    assert.equal((await runNative(readNativeCache(config))).at(0)?.presence.activity?.state, "unknown");
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", prompt: "secret-prompt", response: "secret-response", extra: { ...envelope.extra, usage: { prompt_tokens: 100, completion_tokens: 0, cache_read_tokens: 20, cache_write_tokens: 0, reasoning_tokens: 0, total_tokens: 100, secret: "secret-usage" }, response: "secret-provider", messages: ["secret-message"], args: { secret: "secret-tool" } } }, at + 1, process.pid));
    assert.equal((await runNative(readNativeCache(config))).at(0)?.presence.activity?.state, "unknown");
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "on_session_end" }, at + 2, process.pid));
    const cached = await runNative(readNativeCache(config));
    assert.equal(cached.length, 1);
    const observation = cached.at(0);
    assert.ok(observation);
    await Effect.runPromise(decodeSessionInspection(observation));
    assert.equal(observation.presence.profile, "native-profile");
    assert.equal(observation.presence.model, "native-model");
    assert.equal(observation.presence.provider, "native-provider");
    assert.equal(observation.presence.cwd, "/workspace");
    assert.equal(observation.presence.activity?.state, "idle");
    assert.equal(observation.presence.lastActivityAt, at + 2);
    assert.ok(observation.presence.observedAt > at + 120000);
    assert.deepEqual(observation.presence.processes?.map((binding) => [binding.pid, binding.ownership]), [[process.pid, "shared"]]);
    const usage = observation.tokens.latestNativeUpdate;
    assert.deepEqual([usage.inputTokens.value, usage.outputTokens.value, usage.cacheReadTokens.value, usage.cacheWriteTokens.value, usage.reasoningOutputTokens.value, usage.totalTokens.value], [100, 0, 20, 0, 0, 100]);
    for (const counter of Object.values(usage)) {
      assert.equal(counter.provenance.observedAt, at + 1);
      assert.equal(counter.provenance.nativeUpdatedAt, null);
    }
    assert.equal(observation.tokens.nativeAggregate.totalTokens.status, "unavailable");
    assert.equal(observation.cost.nativeAggregate.totalCostUsd.status, "unavailable");
    assert.equal(observation.context.contextUsedTokens.status, "unavailable");
    const resources = await runNative(withProcessResources(observation));
    assert.equal(resources.resources.processes.at(0)?.process.ownership, "shared");
    assert.equal(resources.resources.ownedRssBytes.status, "unavailable");
    assert.equal(resources.resources.ownedCpuPercent.status, "unavailable");
    assert.ok(Buffer.byteLength(JSON.stringify(resources)) < INSPECTION_LIMIT);
    const namespace = path.join(directory, "native-observations");
    for (const file of await fs.readdir(namespace)) assert.equal((await fs.readFile(path.join(namespace, file), "utf8")).includes("secret"), false);
    await runNative(writeHermesHook(config, { ...envelope, session_id: "", hook_event_name: "on_session_finalize", extra: { old_session_id: "hermes-session" } }, Date.now(), process.pid));
    for (const name of ["post_api_request", "on_session_end"]) await runNative(writeHermesHook(config, { ...envelope, hook_event_name: name, extra: { usage: { total_tokens: 9 } } }, Date.now(), process.pid));
    assert.equal((await runNative(readNativeCache(config))).length, 0);
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

it("validates native Hermes counters and event whitelist while retaining legacy files", async () => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), "coordinator-hermes-boundary-"));
  const config: ConnectorConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const envelope = { session_id: "session", profile: "default", extra: {} };
  try {
    await fs.chmod(directory, 0o700);
    const namespace = path.join(directory, "native-observations");
    await fs.mkdir(namespace, { mode: 0o700 });
    const legacy = path.join(namespace, createHash("sha256").update(sessionRefKey({ machineId: "machine", harness: "hermes", nativeSessionId: "session" })).digest("hex") + ".json");
    const original = JSON.stringify({ version: 2, kind: "hermes", expiresAt: 1, native: { machineId: "machine", sessionId: "session", profile: "default", observedAt: 1, lastActivityAt: 1, state: "idle", usage: {} } });
    await fs.writeFile(legacy, original, { mode: 0o600 });
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request" }, Date.now(), process.pid));
    assert.equal((await runNative(readNativeCache(config))).length, 0);
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "on_session_start" }, Date.now(), process.pid));
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: { input_tokens: 0, prompt_tokens: 5, output_tokens: 0, completion_tokens: 8 } } }, Date.now(), process.pid));
    const usage = (await runNative(readNativeCache(config))).at(0)?.tokens.latestNativeUpdate;
    assert.equal(usage?.inputTokens.value, 0);
    assert.equal(usage?.outputTokens.value, 0);
    assert.equal(usage?.totalTokens.status, "unavailable");
    assert.equal(usage?.reasoningOutputTokens.status, "unavailable");
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, "3", Number.NaN]) {
      await assert.rejects(runNative(writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: { total_tokens: value } } }, Date.now(), process.pid)));
    }
    for (const name of ["pre_api_request", "pre_llm_call", "post_tool_call", "on_conversation_start"]) {
      await assert.rejects(runNative(writeHermesHook(config, { ...envelope, hook_event_name: name }, Date.now(), process.pid)));
    }
    await runNative(writeHermesHook(config, { ...envelope, hook_event_name: "post_api_request", extra: { usage: null } }, Date.now(), process.pid));
    assert.equal((await runNative(readNativeCache(config))).at(0)?.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
    assert.equal(await fs.readFile(legacy, "utf8"), original);
    assert.equal((await fs.readdir(namespace)).length, 2);
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

it("does not promote a dead, reused or unavailable Hermes host to live or owned presence", async () => {
  if (process.platform !== "linux" && process.platform !== "darwin") return;
  const directory = await fs.mkdtemp(path.join(tmpdir(), "coordinator-hermes-host-"));
  const config: ConnectorConfig = { inspectorUrl: "http://127.0.0.1:8080", machineId: "machine", tokenFile: path.join(directory, "token"), stateDirectory: directory };
  const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: ["pipe", "ignore", "ignore"] });
  const exited = once(child, "exit");
  try {
    await fs.chmod(directory, 0o700);
    assert.ok(child.pid);
    await runNative(writeHermesHook(config, { session_id: "dead", hook_event_name: "on_session_start", extra: {} }, Date.now(), child.pid));
    child.kill();
    await exited;
    assert.equal((await runNative(readNativeCache(config))).length, 0);
    const missingAt = Date.now() - 1000;
    await runNative(writeHermesHook(config, { session_id: "missing", hook_event_name: "on_session_start", extra: {} }, missingAt, child.pid));
    const missing = (await runNative(readNativeCache(config))).at(0);
    assert.equal(missing?.presence.ref.nativeSessionId, "missing");
    assert.equal(missing?.presence.observedAt, missingAt);
    assert.equal(missing?.presence.processes?.length, 0);
    assert.equal(missing?.resources.ownedRssBytes.status, "unavailable");
    await runNative(writeHermesHook(config, { session_id: "expired-missing", hook_event_name: "on_session_start", extra: {} }, Date.now() - 240000, child.pid));
    assert.equal((await runNative(readNativeCache(config))).some((item) => item.presence.ref.nativeSessionId === "expired-missing"), false);
    const reused = await Effect.runPromise(projectHook("claude", "machine", { session_id: "reused", hook_event_name: "Stop" }));
    await runNative(writeNativeCache(config, { ...reused, presence: { ...reused.presence, ref: { ...reused.presence.ref, harness: "hermes" }, processes: [{ pid: process.pid, startIdentity: "different-birth", ownership: "shared" }] } }));
    assert.equal((await runNative(readNativeCache(config))).some((item) => item.presence.ref.nativeSessionId === "reused"), false);
    await runNative(writeHermesHook(config, { session_id: "reused", hook_event_name: "on_session_end", extra: {} }, Date.now(), process.pid));
    assert.equal((await runNative(readNativeCache(config))).some((item) => item.presence.ref.nativeSessionId === "reused"), false);
  } finally {
    if (child.exitCode === null && child.signalCode === null) { child.kill(); await exited; }
    await fs.rm(directory, { recursive: true, force: true });
  }
});

it("bounds encoded process details without losing aggregate scope or claiming complete rows", async () => {
  if (process.platform !== "linux") return;
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
  const native = await Effect.runPromise(projectHook("claude", "machine", { session_id: "large-tree", hook_event_name: "Stop" }));
  const observation = await Effect.runPromise(withProcessResources({ ...native, presence: { ...native.presence, processes: [{ pid: 1, startIdentity: `linux:${boot}:1`, ownership: "owned" }] } }).pipe(Effect.provideService(FileSystem.FileSystem, files), Effect.provide(NodeServices.layer)));
  await Effect.runPromise(decodeSessionInspection(observation));
  assert.ok(observation.resources.processes.length > 0);
  assert.ok(observation.resources.processes.length < 256);
  assert.ok(Buffer.byteLength(JSON.stringify(observation)) <= INSPECTION_LIMIT - 2 * 1024);
  assert.equal(observation.resources.ownedProcessCount.value, 256);
  assert.equal(observation.resources.ownedProcessCount.provenance.scope, "owned-processes");
  assert.equal(observation.resources.ownedProcessCount.provenance.partial, true);
  assert.equal(observation.resources.processes.at(0)?.rssBytes.provenance.partial, true);
  const actual = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const publisher: InspectorPrincipal = { id: "native", owner: "owner", machineId: "machine", tokenHash: createHash("sha256").update("native-secret").digest("hex") };
    const registry = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [publisher] });
    const identity = { connectorId: "native-tree", incarnation: "first" };
    yield* registry.announce(publisher, { ...identity, machineId: "machine", sessions: [observation.presence] });
    const query = yield* Effect.forkChild(registry.inspect(publisher, { ref: observation.presence.ref }), { startImmediately: true });
    const request = (yield* registry.poll(publisher, identity)).requests.at(0);
    assert.ok(request);
    yield* registry.complete(publisher, { ...identity, requestId: request.requestId, observation });
    return yield* Fiber.join(query);
  })));
  assert.ok(Buffer.byteLength(JSON.stringify(actual)) <= INSPECTION_LIMIT);
  assert.equal(actual.resources.processes.length, observation.resources.processes.length);
  assert.equal(actual.resources.ownedProcessCount.value, 256);
  assert.equal(actual.resources.ownedProcessCount.provenance.scope, "owned-processes");
  assert.equal(actual.resources.ownedProcessCount.provenance.partial, true);
  assert.equal(actual.resources.processes.at(0)?.rssBytes.provenance.partial, true);
  const firstRow = actual.resources.processes.at(0);
  assert.ok(firstRow);
  assert.ok(firstRow.rssBytes.value !== null);
  assert.equal(actual.resources.ownedRssBytes.value, firstRow.rssBytes.value * 256);
  assert.equal(actual.resources.ownedRssBytes.provenance.scope, "owned-processes");
  assert.equal(actual.resources.ownedRssBytes.provenance.partial, true);
});
