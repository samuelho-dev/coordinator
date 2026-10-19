import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { it } from "vitest";
import { createNativeObserver, isCoordinateInput, transformCoordinateInput } from "../src/plugin.ts";
import type { Context } from "../src/plugin.ts";
import type { SessionListOutput } from "../src/inspection.ts";

const context = (id = "surrounding-session"): Context => ({
  cwd: "/workspace", mode: "rpc", hasUI: false,
  model: { provider: "native-provider", id: "native-model" },
  sessionManager: { getSessionId: () => id },
  getContextUsage: () => ({ tokens: null, contextWindow: 1000, percent: null }),
  isIdle: () => true, ui: { notify: () => {} },
});
const usage = { input: 10, output: 3, cacheRead: 5, cacheWrite: 2, totalTokens: 20, cost: { input: 0.1, output: 0.2, cacheRead: 0.01, cacheWrite: 0.02, total: 0.33 } };

it("publishes surrounding native identity and clears cached counters only on real switches", () => {
  const observer = createNativeObserver("omp", "machine");
  const native: Context = { ...context(), agent: { kind: "main", id: "Main", name: "main" } };
  observer.update(native, { message: { role: "assistant", timestamp: 12, usage } }, 20);
  assert.deepEqual(observer.presence(21)?.ref, { machineId: "machine", harness: "omp", nativeSessionId: "surrounding-session", nativeAgentId: "Main" });
  observer.update(native, undefined, 22);
  assert.equal(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.value, 10);
  assert.equal(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.provenance.observedAt, 20);
  assert.equal(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.provenance.nativeUpdatedAt, 12);
  assert.equal(observer.inspect(3000)?.tokens.latestNativeUpdate.inputTokens.provenance.stale, false);
  assert.equal(typeof observer.presence()?.machineName, "string");
  observer.update({ ...native, agent: { kind: "sub", id: "child" } }, undefined, 31);
  assert.equal(observer.inspect(32)?.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
  observer.update(context("next-session"), undefined, 33);
  assert.equal(observer.presence(34)?.ref.nativeSessionId, "next-session");
  observer.clear();
  assert.equal(observer.inspect(35), undefined);
});

it("queries context only on demand, preserving unknown and over-limit native estimates", () => {
  const observer = createNativeObserver("pi", "machine");
  let queries = 0;
  const ctx: Context = { ...context(), getContextUsage: () => { queries++; return { tokens: null, contextWindow: 1000, percent: null }; } };
  observer.update(ctx, undefined, 1);
  observer.presence(2);
  assert.equal(queries, 0);
  const unknown = observer.inspect(3);
  assert.equal(queries, 1);
  assert.equal(unknown?.context.contextUsedTokens.value, null);
  assert.equal(unknown?.context.contextRemainingTokens.status, "unavailable");
  assert.equal(unknown?.activity.pendingMessageCount.status, "unavailable");
  observer.update({ ...ctx, getContextUsage: () => ({ tokens: 1200, contextWindow: 1000, percent: 120 }) }, undefined, 4);
  const over = observer.inspect(5);
  assert.equal(over?.context.contextUsedTokens.value, 1200);
  assert.equal(over?.context.contextUsedPercent.value, 120);
  assert.equal(over?.context.contextRemainingPercent.value, -20);
});

it("projects native cached aggregate and async counts without adding child costs or leaking payloads", () => {
  const observer = createNativeObserver("omp", "machine");
  const aggregate = { input: 50, output: 20, cacheRead: 15, cacheWrite: 5, totalTokens: 90, premiumRequests: 2, cost: 1.5, subagentCost: 0.7 };
  const job = { command: "PRIVATE_COMMAND", label: "PRIVATE_LABEL" };
  const ctx: Context = {
    ...context(), sessionManager: { getSessionId: () => "native", getUsageStatistics: () => aggregate },
    getAsyncJobSnapshot: () => ({ running: [job], delivery: { queued: 1, delivering: true, pendingJobIds: ["PRIVATE_JOB_ID"] } }),
  };
  const privateMessage = { role: "assistant", timestamp: 5, usage: { ...usage, cost: { ...usage.cost }, reasoningTokens: 2, premiumRequests: 0, credits: { cost: 0.5, committedCost: 0.4, acuCost: 0 } }, content: "PRIVATE_MESSAGE", toolResults: ["PRIVATE_TOOL_RESULT"] };
  observer.update(ctx, { message: privateMessage }, 6);
  const result = observer.inspect(10);
  assert.equal(result?.cost.nativeAggregate.totalCostUsd.value, 1.5);
  assert.equal(result?.tokens.nativeAggregate.totalTokens.value, 90);
  assert.equal(result?.tokens.nativeAggregate.inputTokens.provenance.nativeUpdatedAt, null);
  assert.equal(result?.activity.activeAsyncJobCount.value, 1);
  assert.equal(result?.activity.pendingAsyncWorkCount.value, 1);
  assert.equal(result?.activity.activeAsyncJobCount.provenance.derived, true);
  assert.equal(result?.activity.pendingAsyncWorkCount.provenance.derived, false);
  assert.equal(result?.tokens.latestNativeUpdate.reasoningOutputTokens.value, 2);
  assert.equal(result?.cost.latestNativeUpdate.premiumRequests.value, 0);
  assert.equal(result?.cost.latestNativeUpdate.creditCost.value, 0.5);
  assert.equal(result?.cost.latestNativeUpdate.committedCreditCost.value, 0.4);
  assert.equal(result?.cost.latestNativeUpdate.acuCost.value, 0);
  assert.equal(result?.tokens.nativeAggregate.reasoningOutputTokens.status, "unavailable");
  assert.equal(result?.cost.nativeAggregate.creditCost.status, "unavailable");
  assert.equal(result?.presence.profile, undefined);
  const serialized = JSON.stringify(result);
  for (const secret of ["PRIVATE_COMMAND", "PRIVATE_LABEL", "PRIVATE_JOB_ID", "PRIVATE_MESSAGE", "PRIVATE_TOOL_RESULT"]) assert.equal(serialized.includes(secret), false);
  // Later mutations of the native payload cannot change the retained scalar snapshot.
  privateMessage.usage.input = 999;
  privateMessage.usage.credits.cost = 999;
  assert.equal(observer.inspect(11)?.tokens.latestNativeUpdate.inputTokens.value, 10);
  assert.equal(observer.inspect(11)?.cost.latestNativeUpdate.creditCost.value, 0.5);
  observer.update(ctx, { message: { role: "assistant", usage } }, 12);
  assert.equal(observer.inspect(13)?.tokens.latestNativeUpdate.reasoningOutputTokens.status, "unavailable");
  assert.equal(observer.inspect(13)?.cost.latestNativeUpdate.creditCost.status, "unavailable");
});

it("does not infer Pi aggregate or async counts from native activity", () => {
  const observer = createNativeObserver("pi", "machine");
  observer.update({ ...context(), isIdle: () => false });
  const result = observer.inspect();
  assert.equal(result?.presence.activity?.state, "working");
  assert.equal(result?.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.equal(result?.activity.activeAsyncJobCount.status, "unavailable");
  assert.equal(result?.activity.pendingAsyncWorkCount.status, "unavailable");
});

it("preserves Pi native reasoning subsets without inventing unsupported provider counters", () => {
  const observer = createNativeObserver("pi", "machine");
  observer.update(context(), { message: { role: "assistant", usage: { ...usage, reasoning: 0 } } }, 1);
  const result = observer.inspect(2);
  assert.equal(result?.tokens.latestNativeUpdate.reasoningOutputTokens.value, 0);
  assert.equal(result?.tokens.latestNativeUpdate.outputTokens.value, 3);
  assert.equal(result?.cost.latestNativeUpdate.premiumRequests.status, "unavailable");
});

it("retains only active tool IDs and names across concurrent executions and native switches", () => {
  const observer = createNativeObserver("omp", "machine");
  const ctx = context();
  const start = { type: "tool_execution_start", toolCallId: "a", toolName: "read", args: "PRIVATE_ARGS" };
  observer.update(ctx, start, 1);
  observer.update(ctx, { type: "tool_execution_start", toolCallId: "b", toolName: "search" }, 2);
  assert.equal(observer.presence()?.activity?.currentToolName, "read");
  assert.equal(observer.presence()?.activity?.wait, "tool");
  const end = { type: "tool_execution_end", toolCallId: "a", toolName: "read", result: "PRIVATE_RESULT" };
  observer.update(ctx, end, 3);
  assert.equal(observer.presence()?.activity?.currentToolName, "search");
  const serialized = JSON.stringify(observer.inspect(4));
  assert.equal(serialized.includes("PRIVATE_ARGS"), false);
  assert.equal(serialized.includes("PRIVATE_RESULT"), false);
  observer.update(ctx, { type: "tool_execution_end", toolCallId: "b" }, 5);
  assert.equal(observer.presence()?.activity?.currentToolName, undefined);
  observer.update(ctx, start, 6);
  observer.update(context("switched"), undefined, 7);
  assert.equal(observer.presence()?.activity?.currentToolName, undefined);
});

it("matches only a leading bounded coordinate token and preserves native input result shapes and images", () => {
  for (const text of ["coordinate", "coordinate inspect", "coordinate\ninspect"]) assert.equal(isCoordinateInput(text), true);
  for (const text of ["/coordinate", "please coordinate", " coordinate", "coordinates", "coordinate_task", "coordinate/file", "Coordinate"]) {
    assert.equal(isCoordinateInput(text), false);
    assert.equal(transformCoordinateInput("omp", { text }, { sessions: [], nextCursor: null }), undefined);
  }
  const images: Array<{ type: "image"; data: string; mimeType: string }> = [{ type: "image", data: "IMAGE", mimeType: "image/png" }];
  const snapshot: SessionListOutput = { sessions: [{ ref: { machineId: "remote", harness: "omp", nativeSessionId: "actual-fleet-session" }, observedAt: 1 }], nextCursor: null };
  const omp = transformCoordinateInput("omp", { text: "coordinate inspect", images }, snapshot);
  const pi = transformCoordinateInput("pi", { text: "coordinate inspect", images }, snapshot);
  assert.equal(omp?.images, images);
  assert.equal(pi?.images, images);
  assert.equal(omp?.text.includes("actual-fleet-session"), true);
  assert.equal(omp && "action" in omp, false);
  assert.equal(pi && "action" in pi ? pi.action : undefined, "transform");
});

it("adds the real native coordinate shortcut without enabling disabled core keywords", () => {
  // Native keyword definitions import text assets and require OMP's Bun runtime.
  execFileSync("bun", ["--eval", `
    import assert from "node:assert/strict";
    import { Effect } from "effect";
    import { registerCoordinateKeyword } from "./src/plugin.ts";
    import { MAGIC_KEYWORDS } from "@oh-my-pi/pi-coding-agent/modes/magic-keywords";
    import { isMagicKeyword, magicKeywordRanges, setMagicKeywords } from "@oh-my-pi/pi-tui/prompt/magic-keywords";
    for (const enabled of [[], MAGIC_KEYWORDS.slice(0, 1), MAGIC_KEYWORDS]) {
      setMagicKeywords(enabled);
      const before = MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word));
      await Effect.runPromise(registerCoordinateKeyword);
      assert.deepEqual(MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word)), before);
      assert.equal(isMagicKeyword("coordinate"), true);
      assert.deepEqual(magicKeywordRanges("coordinate"), [{ from: 0, to: 10 }]);
      assert.deepEqual(magicKeywordRanges("coordinate_task"), []);
      await Effect.runPromise(registerCoordinateKeyword);
      assert.deepEqual(MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word)), before);
      assert.deepEqual(magicKeywordRanges("coordinate"), [{ from: 0, to: 10 }]);
    }
  `], { stdio: "pipe" });
});
