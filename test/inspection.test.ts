import { Effect } from "effect";
import type { Path } from "effect";
import { NodeServices } from "@effect/platform-node";
import { assert, expect, it } from "@effect/vitest";
import {
  createInspection, decodeConnectorAnnounce, decodeConnectorConfig, decodeConnectorResult, decodeInspectorConfig,
  decodeSessionInspection, decodeSessionListInput, decodeSessionListOutput, decodeSessionPresence, metric, sessionRefKey,
  type MachineResources, type MetricProvenance, type SessionGit, type SessionPresence,
} from "../src/inspection.ts";

const run = <A, E>(effect: Effect.Effect<A, E, Path.Path>) => Effect.runPromise(effect.pipe(Effect.provide(NodeServices.layer)));

const presence: SessionPresence = {
  ref: { machineId: "mac", harness: "claude", nativeSessionId: "native-session" }, observedAt: 200,
};
const grouped: SessionPresence = {
  ...presence,
  machine: { name: "Mac", os: { platform: "darwin", release: "25", architecture: "arm64" } },
  agent: { kind: "primary", name: "reviewer", profile: "careful" },
  model: { provider: "provider", id: "model" },
  workspace: {
    cwd: "/work/project/subdir",
    project: { root: "/work/project", source: "configured", id: "project-id", name: "Project" },
    git: { state: "available", observedAt: 190, worktreeRoot: "/work/project", commonDir: "/work/main/.git", branch: "feature", head: "a".repeat(40) },
  },
};

const machineProvenance: MetricProvenance = { source: "linux-cgroup-v2", scope: "cgroup", scopeDescription: "Shared cgroup boundary", observedAt: 100, nativeUpdatedAt: null, estimated: false, derived: false, shared: true, partial: false, stale: false };
const machineResources: MachineResources = {
  scope: "cgroup", scopeId: "boot-boundary", source: "linux-cgroup-v2", observedAt: 100,
  cpu: { usagePercent: metric("percent-one-core", 75, machineProvenance), capacityCores: metric("cores", 0.5, machineProvenance), usedPercent: metric("percent", 150, machineProvenance) },
  memory: { usedBytes: metric("bytes", 10, machineProvenance), capacityBytes: metric("bytes", 100, machineProvenance), availableBytes: { status: "unavailable", value: null, unit: "bytes", reason: "No MemAvailable measurement", provenance: machineProvenance }, usedPercent: metric("percent", 10, machineProvenance) },
};

it("round trips fractional CPU capacity and quota bursts while rejecting malformed machine telemetry", async () => {
  const value = { ...grouped, machine: { ...grouped.machine, resources: machineResources } };
  assert.deepStrictEqual(await run(decodeSessionPresence(value)), value);
  assert.deepStrictEqual((await run(decodeSessionInspection(createInspection(value, 200)))).presence, value);
  for (const resources of [
    { ...machineResources, scopeId: "" },
    { ...machineResources, observedAt: -1 },
    { ...machineResources, privatePath: "/sys/fs/cgroup/private" },
    { ...machineResources, cpu: { ...machineResources.cpu, capacityCores: metric("cores", -1, machineProvenance) } },
    { ...machineResources, cpu: { ...machineResources.cpu, capacityCores: metric("count", 1, machineProvenance) } },
    { ...machineResources, memory: { ...machineResources.memory, usedBytes: metric("bytes", 0.5, machineProvenance) } },
  ]) await expect(run(decodeSessionPresence({ ...value, machine: { resources } }))).rejects.toThrow();
});

it("validates per-session capacity units, fractional quotas and nonexclusive RSS fractions", async () => {
  const resources: NonNullable<SessionPresence["resources"]> = {
    observedAt: 100, machineScopeId: "boot-boundary", attribution: "shared",
    cpuPercent: metric("percent-one-core", 75, machineProvenance), cpuCapacityPercent: metric("percent", 150, machineProvenance),
    rssBytes: metric("bytes", 110, machineProvenance), rssCapacityPercent: metric("percent", 110, machineProvenance),
  };
  const value = { ...grouped, resources };
  assert.deepStrictEqual((await run(decodeSessionPresence(value))).resources, resources);
  for (const invalid of [
    { ...resources, attribution: "proportional" },
    { ...resources, cpuCapacityPercent: metric("percent-one-core", 50, machineProvenance) },
    { ...resources, rssCapacityPercent: metric("percent", -1, machineProvenance) },
    { ...resources, cpuCapacityPercent: metric("percent", Infinity, machineProvenance) },
    { ...resources, rssBytes: metric("bytes", 0.5, machineProvenance) },
    { ...resources, privatePath: "/proc/private" },
  ]) await expect(run(decodeSessionPresence({ ...value, resources: invalid }))).rejects.toThrow();
});

it("round trips grouped metadata through public list, inspection and connector boundaries", async () => {
  const copy = JSON.parse(JSON.stringify(grouped));
  assert.deepStrictEqual(await run(decodeSessionPresence(copy)), grouped);
  const list = { sessions: [copy], nextCursor: null };
  assert.deepStrictEqual(await run(decodeSessionListOutput(list)), list);
  const observation = createInspection(copy, 300);
  assert.deepStrictEqual(await run(decodeSessionInspection(observation)), observation);
  const connector = { connectorId: "connector", incarnation: "incarnation" };
  const announce = { ...connector, machineId: grouped.ref.machineId, sessions: [copy] };
  assert.deepStrictEqual(await run(decodeConnectorAnnounce(announce)), announce);
  const result = { ...connector, requestId: "request", observation };
  assert.deepStrictEqual(await run(decodeConnectorResult(result)), result);
});

it("accepts SHA-1, SHA-256, detached, unborn and explicit negative Git observations", async () => {
  const available = { state: "available", observedAt: 190, worktreeRoot: "/work/project", commonDir: "/work/main/.git" } satisfies Omit<Extract<SessionGit, { state: "available" }>, "branch" | "head">;
  for (const git of [
    ...["a".repeat(40), "0123456789abcdef".repeat(4), null].map((head) => ({ ...available, branch: "main", head })),
    { ...available, branch: null, head: "b".repeat(40) },
    { state: "not-repository", observedAt: 190 },
    { state: "unavailable", observedAt: 190 },
  ] satisfies SessionGit[]) {
    const value = { ...presence, workspace: { cwd: "/work/project", git } };
    assert.deepStrictEqual(await run(decodeSessionPresence(value)), value);
  }
  for (const head of ["", "A".repeat(40), "g".repeat(40), "a".repeat(39), "a".repeat(41), "a".repeat(63), "a".repeat(65), `${"a".repeat(40)}\n`, `${"a".repeat(64)}\r\n`]) {
    await expect(run(decodeSessionPresence({ ...presence, workspace: { cwd: "/work/project", git: { ...available, branch: "main", head } } }))).rejects.toThrow();
  }
  for (const git of [
    { ...available, head: null },
    { ...available, branch: null },
    { state: "not-repository", observedAt: 190, worktreeRoot: "/work/project" },
    { state: "unavailable", observedAt: -1 },
  ]) {
    await expect(run(decodeSessionPresence({ ...presence, workspace: { cwd: "/work/project", git } }))).rejects.toThrow();
  }
});

it("rejects obsolete flat metadata and unknown or private fields within every metadata group", async () => {
  for (const obsolete of [
    { machineName: "Mac" }, { os: { platform: "darwin" } }, { kind: "primary" }, { provider: "provider" },
    { model: "model" }, { profile: "careful" }, { agent: "reviewer" }, { cwd: "/work/project" }, { repository: "/work/project" },
  ]) {
    await expect(run(decodeSessionPresence({ ...presence, ...obsolete }))).rejects.toThrow();
  }
  for (const extra of [
    { machine: { name: "Mac", transcript: "private" } },
    { machine: { os: { platform: "darwin", prompt: "private" } } },
    { agent: { name: "reviewer", instructions: "private" } },
    { model: { id: "model", messages: ["private"] } },
    { workspace: { cwd: "/work/project", transcript: "private" } },
    { workspace: { cwd: "/work/project", project: { root: "/work/project", source: "native", prompt: "private" } } },
    { workspace: { cwd: "/work/project", git: { state: "unavailable", observedAt: 190, stderr: "private" } } },
    { workspace: { project: { root: "/work/project", source: "native" } } },
    { workspace: { cwd: "/work/project", project: { root: "/work/project", source: "inferred" } } },
  ]) {
    const invalid = { ...presence, ...extra };
    await expect(run(decodeSessionPresence(invalid))).rejects.toThrow();
    await expect(run(decodeSessionInspection({ ...createInspection(presence, 300), presence: invalid }))).rejects.toThrow();
  }
});
const provenance: MetricProvenance = {
  source: "claude.statusline", scope: "latest-native-update", scopeDescription: "Latest response input, not lifetime session usage",
  observedAt: 150, nativeUpdatedAt: 100, estimated: false, derived: false, shared: false, partial: true, stale: true,
};

it("keeps missing counters distinct from measured zero and accepts native percentages above 100", async () => {
  const empty = createInspection(presence, 300);
  const inspection = await run(decodeSessionInspection({
    ...empty,
    context: { ...empty.context, contextUsedPercent: metric("percent", 101, { ...provenance, scope: "current-context", estimated: true }) },
    tokens: {
      ...empty.tokens,
      latestNativeUpdate: { ...empty.tokens.latestNativeUpdate, inputTokens: metric("tokens", 0, provenance) },
    },
    resources: { ...empty.resources, ownedCpuPercent: metric("percent-one-core", 250, { ...provenance, scope: "owned-processes" }) },
  }));
  assert.strictEqual(empty.tokens.latestNativeUpdate.inputTokens.value, null);
  assert.strictEqual(empty.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
  assert.strictEqual(inspection.tokens.latestNativeUpdate.inputTokens.status, "available");
  assert.strictEqual(inspection.tokens.latestNativeUpdate.inputTokens.value, 0);
  assert.strictEqual(inspection.resources.ownedCpuPercent.value, 250);
  assert.strictEqual(inspection.context.contextUsedPercent.value, 101);
});

it("rejects forbidden content recursively, invalid units and invalid numeric bounds at the decode boundary", async () => {
  const empty = createInspection(presence, 300);
  const invalid = [
    { ...empty, transcript: [] },
    { ...empty, presence: { ...presence, queuedPrompt: "private" } },
    { ...empty, presence: { ...presence, activity: { state: "working", toolInput: "private" } } },
    { ...empty, presence: { ...presence, programStatus: { state: "blocked" } } },
    { ...empty, presence: { ...presence, programStatus: { state: "working", kind: "permission" } } },
    { ...empty, presence: { ...presence, programStatus: { state: "blocked", kind: "tool" } } },
    { ...empty, presence: { ...presence, rex: { serverId: "invented" } } },
    { ...empty, presence: { ...presence, rex: { serverAddress: "socket\u0085bad" } } },
    { ...empty, presence: { ...presence, rex: { sessionId: "session\nbad" } } },
    { ...empty, context: { ...empty.context, contextUsedPercent: metric("percent", Infinity, provenance) } },
    { ...empty, tokens: { ...empty.tokens, latestNativeUpdate: { ...empty.tokens.latestNativeUpdate, inputTokens: metric("tokens", 0.5, provenance) } } },
    { ...empty, tokens: { ...empty.tokens, latestNativeUpdate: { ...empty.tokens.latestNativeUpdate, inputTokens: metric("bytes", 1, provenance) } } },
    { ...empty, cost: { ...empty.cost, nativeAggregate: { ...empty.cost.nativeAggregate, totalCostUsd: metric("usd", -1, provenance) } } },
    { ...empty, activity: { ...empty.activity, apiWaitMs: metric("milliseconds", Infinity, provenance) } },
    { ...empty, resources: { ...empty.resources, ownedCpuPercent: metric("percent-one-core", NaN, provenance) } },
    { ...empty, activity: { ...empty.activity, apiWaitMs: metric("milliseconds", 1, { ...provenance, sampleWindow: { startedAt: 20, endedAt: 10 } }) } },
  ];
  for (const value of invalid) await expect(run(decodeSessionInspection(value))).rejects.toThrow();
  await expect(run(decodeSessionPresence({ ...presence, ref: { ...presence.ref, harness: "other" } }))).rejects.toThrow();
});

it("requires bounded pagination and TLS outside loopback", async () => {
  const config = { inspectorUrl: "https://inspector.example", tokenFile: "/private/token" };
  await run(decodeConnectorConfig({ ...config, inspectorUrl: "http://127.0.0.1:8080" }));
  for (const inspectorUrl of ["http://inspector.example", "https://user:secret@inspector.example", "https://inspector.example/?token=secret", "https://inspector.example/#secret", "file:///tmp/inspector"]) {
    await expect(run(decodeConnectorConfig({ ...config, inspectorUrl }))).rejects.toThrow();
  }
  for (const limit of [0, 101, 1.5]) await expect(run(decodeSessionListInput({ limit }))).rejects.toThrow();
  await expect(run(decodeSessionListInput({ limit: 1, cursor: "x".repeat(257) }))).rejects.toThrow();
});

it("requires canonical private token paths without restricting native metadata", async () => {
  const config = { inspectorUrl: "https://inspector.example", tokenFile: "/private/token" };
  for (const tokenFile of ["private/token", "", "/private/\0token", "/private/token\r", "/private/token\n", "/private/../token", "/private//token", "/private/./token", "/private/token/"]) {
    await expect(run(decodeConnectorConfig({ ...config, tokenFile }))).rejects.toThrow();
  }
  const metadata: SessionPresence = { ...presence, workspace: { cwd: "relative/native", project: { root: "native\nmetadata", source: "native" } } };
  assert.deepStrictEqual(await run(decodeSessionPresence(metadata)), metadata);
});

it("validates explicitly configured project roots with private path rules and bounded labels", async () => {
  const config = { inspectorUrl: "https://inspector.example", project: { root: "/work/project", id: "stable-project", name: "Project" } };
  assert.deepStrictEqual(await run(decodeConnectorConfig(config)), config);
  for (const root of ["work/project", "", "/work/\0project", "/work/project\r", "/work/project\n", "/work/../project", "/work//project", "/work/./project", "/work/project/"]) {
    await expect(run(decodeConnectorConfig({ ...config, project: { ...config.project, root } }))).rejects.toThrow();
  }
  for (const project of [
    { root: "/work/project", id: "" }, { root: "/work/project", id: "x".repeat(257) },
    { root: "/work/project", name: "" }, { root: "/work/project", name: "x".repeat(513) },
    { root: "/work/project", source: "configured" }, { root: "/work/project", transcript: "private" },
    { id: "missing-root" },
  ]) await expect(run(decodeConnectorConfig({ ...config, project }))).rejects.toThrow();
});

it("allows local capture without credentials but rejects invalid credentials and obsolete client settings", async () => {
  const config = { inspectorUrl: "https://inspector.example" };
  await run(decodeConnectorConfig(config));
  await run(decodeConnectorConfig({ ...config, apiToken: "valid-owner-token", tokenFile: "/private/token" }));
  for (const apiToken of ["", "short", "x".repeat(4097), " valid-owner-token", "valid-owner-token\n", "invalid owner token", "invalid:owner:token"]) {
    await expect(run(decodeConnectorConfig({ ...config, apiToken }))).rejects.toThrow();
  }
  for (const obsolete of [{ machineId: "mac" }, { stateDirectory: "/private/state" }, { token: "valid-owner-token" }]) {
    await expect(run(decodeConnectorConfig({ ...config, ...obsolete }))).rejects.toThrow();
  }
});

it("accepts owner credentials without a machine binding and rejects obsolete server bindings", async () => {
  const principal = { id: "owner-key", owner: "alice", tokenHash: "a".repeat(64) };
  const config = { host: "127.0.0.1", port: 0, principals: [principal] };
  await run(decodeInspectorConfig(config));
  await expect(run(decodeInspectorConfig({ ...config, principals: [{ ...principal, machineId: "mac" }] }))).rejects.toThrow();
  await expect(run(decodeInspectorConfig({ ...config, principals: [{ ...principal, tokenHash: "not-a-hash" }] }))).rejects.toThrow();
});

it("keys native actors by their full composite identity without delimiter collisions", () => {
  assert.notStrictEqual(sessionRefKey(presence.ref), sessionRefKey({ ...presence.ref, nativeAgentId: "child" }));
  assert.notStrictEqual(
    sessionRefKey({ ...presence.ref, machineId: "a:b", nativeSessionId: "c" }),
    sessionRefKey({ ...presence.ref, machineId: "a", nativeSessionId: "b:c" }),
  );
});
