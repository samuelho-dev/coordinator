import assert from "node:assert/strict";
import { Effect } from "effect";
import { it } from "@effect/vitest";
import {
  createInspection, decodeConnectorConfig, decodeSessionInspection, decodeSessionListInput,
  decodeSessionPresence, metric, sessionRefKey,
  type MetricProvenance, type SessionPresence,
} from "../src/inspection.ts";

const presence: SessionPresence = {
  ref: { machineId: "mac", harness: "claude", nativeSessionId: "native-session" }, observedAt: 200,
};
const provenance: MetricProvenance = {
  source: "claude.statusline", scope: "latest-native-update", scopeDescription: "Latest response input, not lifetime session usage",
  observedAt: 150, nativeUpdatedAt: 100, estimated: false, derived: false, shared: false, partial: true, stale: true,
};

it("keeps missing counters distinct from measured zero and accepts native percentages above 100", async () => {
  const empty = createInspection(presence, 300);
  const inspection = await Effect.runPromise(decodeSessionInspection({
    ...empty,
    context: { ...empty.context, contextUsedPercent: metric("percent", 101, { ...provenance, scope: "current-context", estimated: true }) },
    tokens: {
      ...empty.tokens,
      latestNativeUpdate: { ...empty.tokens.latestNativeUpdate, inputTokens: metric("tokens", 0, provenance) },
    },
    resources: { ...empty.resources, ownedCpuPercent: metric("percent-one-core", 250, { ...provenance, scope: "owned-processes" }) },
  }));
  assert.equal(empty.tokens.latestNativeUpdate.inputTokens.value, null);
  assert.equal(empty.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
  assert.equal(inspection.tokens.latestNativeUpdate.inputTokens.status, "available");
  assert.equal(inspection.tokens.latestNativeUpdate.inputTokens.value, 0);
  assert.equal(inspection.resources.ownedCpuPercent.value, 250);
  assert.equal(inspection.context.contextUsedPercent.value, 101);
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
  for (const value of invalid) await assert.rejects(Effect.runPromise(decodeSessionInspection(value)));
  await assert.rejects(Effect.runPromise(decodeSessionPresence({ ...presence, ref: { ...presence.ref, harness: "other" } })));
});

it("requires bounded pagination and TLS outside loopback", async () => {
  const config = { inspectorUrl: "https://inspector.example", tokenFile: "/private/token", machineId: "mac", stateDirectory: "/private/state" };
  await Effect.runPromise(decodeConnectorConfig({ ...config, inspectorUrl: "http://127.0.0.1:8080" }));
  for (const inspectorUrl of ["http://inspector.example", "https://user:secret@inspector.example", "https://inspector.example/?token=secret", "file:///tmp/inspector"]) {
    await assert.rejects(Effect.runPromise(decodeConnectorConfig({ ...config, inspectorUrl })));
  }
  for (const limit of [0, 101, 1.5]) await assert.rejects(Effect.runPromise(decodeSessionListInput({ limit })));
  await assert.rejects(Effect.runPromise(decodeSessionListInput({ limit: 1, cursor: "x".repeat(257) })));
});

it("requires absolute private config paths without NUL, CR or LF without restricting native metadata", async () => {
  const config = { inspectorUrl: "https://inspector.example", tokenFile: "/private/token", machineId: "mac", stateDirectory: "/private/state" };
  assert.deepEqual(await Effect.runPromise(decodeConnectorConfig(config)), config);
  for (const path of ["private/token", "", "/private/\0token", "/private/token\r", "/private/token\n"]) {
    await assert.rejects(Effect.runPromise(decodeConnectorConfig({ ...config, tokenFile: path })));
    await assert.rejects(Effect.runPromise(decodeConnectorConfig({ ...config, stateDirectory: path })));
  }
  const metadata = { ...presence, cwd: "relative/native", repository: "native\nmetadata" };
  assert.deepEqual(await Effect.runPromise(decodeSessionPresence(metadata)), metadata);
});

it("keys native actors by their full composite identity without delimiter collisions", () => {
  assert.notEqual(sessionRefKey(presence.ref), sessionRefKey({ ...presence.ref, nativeAgentId: "child" }));
  assert.notEqual(
    sessionRefKey({ ...presence.ref, machineId: "a:b", nativeSessionId: "c" }),
    sessionRefKey({ ...presence.ref, machineId: "a", nativeSessionId: "b:c" }),
  );
});
