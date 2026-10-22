import { Effect } from "effect";
import type { Path } from "effect";
import { NodeServices } from "@effect/platform-node";
import { assert, expect, it } from "@effect/vitest";
import {
  createInspection, decodeConnectorConfig, decodeInspectorConfig, decodeSessionInspection, decodeSessionListInput,
  decodeSessionPresence, metric, sessionRefKey,
  type MetricProvenance, type SessionPresence,
} from "../src/inspection.ts";

const run = <A, E>(effect: Effect.Effect<A, E, Path.Path>) => Effect.runPromise(effect.pipe(Effect.provide(NodeServices.layer)));

const presence: SessionPresence = {
  ref: { machineId: "mac", harness: "claude", nativeSessionId: "native-session" }, observedAt: 200,
};
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
  const metadata = { ...presence, cwd: "relative/native", repository: "native\nmetadata" };
  assert.deepStrictEqual(await run(decodeSessionPresence(metadata)), metadata);
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
