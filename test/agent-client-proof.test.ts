import { AgentAdmissionDispositionEnum } from "../src/agent/contracts.ts";
import { AgentRunOutcomeEnum } from "../src/agent/contracts.ts";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Config, Effect, FileSystem, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { NodeCrypto, NodeHttpServer, NodeRuntime, NodeServices, NodeStdio } from "@effect/platform-node";
import { connectAgentClient } from "../src/agent/client.ts";
import { AgentHostEnum, AgentBinding, AgentError, AgentEventSequence, AgentMcpWireBytes, AgentPromptResult, AgentRunId, AgentState, AgentSubmissionId, AgentTools, agentEventsUri, type AgentTool, type AgentLifecycleEmission } from "../src/agent/contracts.ts";
import { AgentLifecycleJournal } from "../src/agent/lifecycle.ts";
import { AgentMcpLifetime, agentHttpLayer, agentStdioLayer, agentTool } from "../src/agent/server.ts";
import { createAgentPresentation, withAgentMcpRequest } from "../src/agent/interaction.ts";

const binding = Schema.decodeUnknownSync(AgentBinding)({ host: AgentHostEnum.pi, sessionId: "proof-test", incarnation: "retained", generation: 0 });
const privateSystemPrompt = "private-system-prompt-" + "λ".repeat(40000);
const privateTools = [{ name: "proof-tool", description: "private-tool-description", parameters: { type: "object", properties: { token: { type: "string" } }, required: ["token"], additionalProperties: false } }];

if (process.env.AGENT_CLIENT_PROOF_SERVER === "1") {
  const journal = new AgentLifecycleJournal();
  const presentation = createAgentPresentation(() => ({ agentBinding: binding }));
  let finishPrompt: (() => void) | undefined;
  let pendingRunId: AgentRunId | null = null;
  const tools = [
    agentTool(AgentTools.getState, () => Effect.succeed({ binding, sessionFile: undefined, sessionName: "private-session", cwd: process.cwd(), model: null, systemPrompt: privateSystemPrompt, dumpTools: privateTools, currentRunId: null, isStreaming: false, isCompacting: false, pendingMessageCount: 2, pendingAsyncWorkCount: 1, messageCount: 3, queuedMessages: { steering: ["private-steering"], followUp: ["private-follow-up"] }, steeringMode: "all", followUpMode: "all", interruptMode: "wait", autoCompactionEnabled: true, fastModeEnabled: true, fastModeActive: true, slowModeSupported: true, slowModeEnabled: true, slowModeScope: "session", usageLimit: { stage: "wrap_up", resetsAtSec: 42, extraUsage: false }, tokensPerSecond: 12.5, contextUsage: { tokens: 7, contextWindow: 11, percent: 63.6 }, goal: { enabled: true, mode: "active", goal: { id: "private-goal", objective: "private-objective", status: "active", tokenBudget: 100, tokensUsed: 7, timeUsedSeconds: 2.5, createdAt: 1, updatedAt: 2 } } })),
    agentTool(AgentTools.prompt, (input) => withAgentMcpRequest(async () => {
      if (input.message === "observer") {
        presentation.write("observation/text_delta", { text: "first" });
        presentation.write("observation/text_delta", { text: "second" });
        await presentation.flush();
      }
      const handled = input.message === "missing-admission" || input.message === "handled";
      const runId = handled ? null : Schema.decodeSync(AgentRunId)(randomUUID());
      const emission: AgentLifecycleEmission = { type: "agent_lifecycle", binding, runId, at: Date.now(), kind: "submission_admitted", submissionId: input.submissionId, disposition: handled ? AgentAdmissionDispositionEnum.handled : AgentAdmissionDispositionEnum.started };
      const seq = input.message === "missing-admission" ? journal.highWater : journal.append(emission).seq;
      const admission = { binding, submissionId: input.submissionId, runId, disposition: emission.disposition, eventsUri: agentEventsUri(binding), seq };
      if (runId === null) {
        const result: AgentPromptResult = { admission, disposition: "handled" };
        return result;
      }
      journal.append({ type: "agent_lifecycle", binding, runId, at: Date.now(), kind: "run_started" });
      if (input.message === "http-wait") {
        pendingRunId = runId;
        await new Promise<void>(resolve => { finishPrompt = resolve; });
      }
      const outcome = input.message === "http-wait" ? AgentRunOutcomeEnum.aborted : AgentRunOutcomeEnum.completed;
      if (input.message !== "missing-terminal") journal.append({ type: "agent_lifecycle", binding, runId, at: Date.now(), kind: "run_settled", outcome });
      const result: AgentPromptResult = { admission, disposition: "settled", final: { binding, runId, outcome } };
      return result;
    })),
    agentTool(AgentTools.abort, () => Effect.sync(() => {
      const runId = pendingRunId;
      finishPrompt?.();
      finishPrompt = undefined;
      pendingRunId = null;
      return { binding, runId, disposition: runId === null ? "already_settled" : "requested" };
    })),
    ...Object.values(AgentTools).filter(descriptor => descriptor !== AgentTools.getState && descriptor !== AgentTools.prompt && descriptor !== AgentTools.abort && descriptor.hosts.some(host => host === AgentHostEnum.pi)).map(descriptor => agentTool<AgentTool["input"], AgentTool["output"], never>(descriptor, () => Effect.fail(new AgentError({ code: "unsupported_operation", message: "This resource proof server implements only local prompt operations" })))),
  ];
  if (process.env.AGENT_CLIENT_PROOF_HTTP_ENDPOINT !== undefined) {
    NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
      const endpoint = yield* Config.String("AGENT_CLIENT_PROOF_HTTP_ENDPOINT");
      const app = yield* HttpRouter.toHttpEffect(agentHttpLayer({
        host: AgentHostEnum.pi, binding, journal, tools: tools.filter(tool => tool.descriptor.name === AgentTools.getState.name || tool.descriptor.name === AgentTools.prompt.name || tool.descriptor.name === AgentTools.abort.name), name: "retained-http-proof", version: "1",
      }));
      const server = yield* NodeHttpServer.make(() => createServer(), { host: "127.0.0.1", port: 0 });
      yield* server.serve(app);
      if (server.address._tag !== "InetAddressV4") throw new Error("HTTP fixture requires loopback IPv4");
      process.stdout.write(JSON.stringify({ type: "ready", agentRpcPayload: "λ".repeat(AgentMcpWireBytes) }) + "\n");
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(`${endpoint}.pending`, JSON.stringify({ binding, url: `http://127.0.0.1:${server.address.port}/mcp` }), { mode: 0o600 });
      yield* fs.rename(`${endpoint}.pending`, endpoint);
      return yield* Effect.never;
    }).pipe(Effect.provide(NodeServices.layer))));
  } else {
    NodeRuntime.runMain(Effect.scoped(AgentMcpLifetime.use(owner => owner.closed).pipe(Effect.provide(agentStdioLayer({ host: AgentHostEnum.pi, binding, journal, tools, presentation, name: "retained-client-proof", version: "1" }).pipe(Layer.provide(Layer.merge(NodeStdio.layer, NodeCrypto.layer)))))));
  }
} else {
  // Static Vitest imports cannot load in the directly launched Node fixture process.
  const { it } = await import("@effect/vitest");
  it.live("prompt requires retained admission and terminal proof over standard MCP resources", () => Effect.scoped(Effect.gen(function*() {
    const client = yield* connectAgentClient({ host: AgentHostEnum.pi, command: process.execPath, args: ["--import", "tsx", fileURLToPath(import.meta.url)], cwd: fileURLToPath(new URL("..", import.meta.url)), env: { AGENT_CLIENT_PROOF_SERVER: "1" }, connectTimeoutMs: 10000 });
    const state = yield* client.call(AgentTools.getState, { binding }, { timeoutMs: 3000 });
    assert.equal(state.systemPrompt, privateSystemPrompt, "Private state survives SDK resource transfer above the wire budget");
    assert.deepEqual(state.dumpTools, privateTools);
    assert.equal(state.todoPhases, undefined, "Pi does not invent OMP todo phases");
    assert.deepEqual(state.queuedMessages, { steering: ["private-steering"], followUp: ["private-follow-up"] });
    assert.equal(state.sessionFile, undefined, "Optional undefined state survives native SDK encoding and private chunk decoding");
    const decodeState = Schema.decodeUnknownSync(AgentState, { onExcessProperty: "error" });
    assert.throws(() => decodeState({ ...state, systemPrompt: [privateSystemPrompt] }));
    assert.throws(() => decodeState({ ...state, dumpTools: [{ ...privateTools[0], rawModelConfig: { apiKey: "not-state" } }] }));
    assert.throws(() => decodeState({ ...state, obsoleteQueuedMessageCount: 0 }));
    assert.throws(() => decodeState({ ...state, queuedMessages: { ...state.queuedMessages, durableJournal: [] } }));
    const ompState = { ...state, binding: { ...binding, host: AgentHostEnum.omp }, systemPrompt: [privateSystemPrompt],
      dumpTools: [{ ...privateTools[0], examples: [{ call: { token: "private-token" } }, { bad: {}, good: { token: "private-token" } }, { caption: "private-caption", note: "private-note" }] }],
      todoPhases: [{ name: "private-phase", tasks: [{ content: "private-task", status: "blocked", blocker: "private-blocker", details: "private-details", notes: ["private-note"] }] }] };
    assert.deepEqual(decodeState(ompState), ompState);
    let after = Schema.decodeUnknownSync(AgentEventSequence)(0);
    const admissions: string[] = [];
    const invoke = (message: string) => client.prompt({ binding, submissionId: Schema.decodeSync(AgentSubmissionId)(message), message }, { after, timeoutMs: message === "missing-terminal" ? 300 : 3000, onAdmission: async admission => { await Promise.resolve(); admissions.push(admission.submissionId); }, onLifecycle: event => { after = event.seq; } });
    const missingAdmission = yield* invoke("missing-admission").pipe(Effect.result);
    assert.equal(missingAdmission._tag, "Failure");
    if (missingAdmission._tag === "Failure") assert.equal(missingAdmission.failure.code, "resource_gap");
    assert.deepEqual(admissions, [], "A response receipt must never call onAdmission");
    const missingTerminal = yield* invoke("missing-terminal").pipe(Effect.result);
    assert.equal(missingTerminal._tag, "Failure");
    if (missingTerminal._tag === "Failure") assert.equal(missingTerminal.failure.code, "deadline_exceeded");
    const complete = yield* invoke("complete");
    assert.equal(complete.disposition, "settled");
    assert.equal(complete.admission.submissionId, "complete");
    assert.deepEqual(admissions, ["missing-terminal", "complete"]);
    const handled = yield* invoke("handled");
    assert.equal(handled.disposition, "handled");
    assert.equal(handled.admission.runId, null);
  })), { timeout: 15000 });
  it.live("a prompt deadline cancels an immortal observer callback without poisoning independent subscriptions", () => Effect.scoped(Effect.gen(function*() {
    const client = yield* connectAgentClient({ host: AgentHostEnum.pi, command: process.execPath, args: ["--import", "tsx", fileURLToPath(import.meta.url)], cwd: fileURLToPath(new URL("..", import.meta.url)), env: { AGENT_CLIENT_PROOF_SERVER: "1" }, connectTimeoutMs: 10000 });
    let entered = false;
    const startedAt = performance.now();
    const cancelled = yield* client.prompt({ binding, submissionId: Schema.decodeSync(AgentSubmissionId)("blocked-callback"), message: "complete" }, {
      after: Schema.decodeSync(AgentEventSequence)(0), timeoutMs: 300,
      onAdmission() { entered = true; return new Promise<void>(() => {}); },
    }).pipe(Effect.result);
    assert.equal(entered, true, "The deadline must interrupt a real retained-admission callback");
    assert.equal(cancelled._tag, "Failure");
    if (cancelled._tag === "Failure") assert.equal(cancelled.failure.code, "deadline_exceeded");
    assert.ok(performance.now() - startedAt < 2000, "Cleanup stays within the prompt's budget rather than the connection timeout");
    const delivered: number[] = [];
    const watch = yield* client.watchLifecycle({ binding, after: Schema.decodeSync(AgentEventSequence)(0) }, event => { delivered.push(event.seq); });
    yield* watch.close;
    assert.deepEqual(delivered, [1, 2, 3]);
    const resumed = yield* client.prompt({ binding, submissionId: Schema.decodeSync(AgentSubmissionId)("after-blocked-callback"), message: "complete" }, {
      after: Schema.decodeSync(AgentEventSequence)(3), timeoutMs: 3000, onAdmission() {},
    });
    assert.equal(resumed.disposition, "settled");
  })), { timeout: 15000 });
  it.live("HTTP partial capabilities retain admission without push while oversized native RPC stays private", () => Effect.gen(function*() {
    const directory = yield* Effect.acquireRelease(
      Effect.promise(() => mkdtemp(join(tmpdir(), "agent-http-proof-"))),
      (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
    );
      const endpointFile = join(directory, "endpoint.json");
      const client = yield* Effect.scoped(Effect.gen(function*() {
        const client = yield* connectAgentClient({
          host: AgentHostEnum.pi, command: process.execPath, args: ["--import", "tsx", fileURLToPath(import.meta.url)], cwd: fileURLToPath(new URL("..", import.meta.url)),
          env: { AGENT_CLIENT_PROOF_SERVER: "1", AGENT_CLIENT_PROOF_HTTP_ENDPOINT: endpointFile }, connectTimeoutMs: 10000, plugin: { endpointFile, token: "private-http-proof-token-0000000000" },
        });
        const state = yield* client.call(AgentTools.getState, { binding }, { timeoutMs: 3000 });
        assert.equal(state.systemPrompt, privateSystemPrompt, "Private HTTP result chunks must preserve Unicode bytes and their owner");
        const admissions: string[] = [];
        const result = yield* client.prompt({ binding, submissionId: AgentSubmissionId.make("http-wait"), message: "http-wait" }, {
          after: AgentEventSequence.make(0), timeoutMs: 3000,
          onAdmission: async admission => {
            admissions.push(admission.submissionId);
            await Effect.runPromise(client.call(AgentTools.abort, { binding }, { timeoutMs: 1000 }));
          },
        });
        assert.deepEqual(admissions, ["http-wait"], "Admission must arrive from the journal while the HTTP prompt is still pending");
        assert.equal(result.disposition, "settled");
        if (result.disposition === "settled") assert.equal(result.final.outcome, AgentRunOutcomeEnum.aborted);
        return client;
      }));
      yield* client.observedProcessClosed.pipe(Effect.timeout("5 seconds"));
      assert.throws(() => process.kill(client.pid, 0), { code: "ESRCH" }, "Logical HTTP close must also reap the physical CLI owner");
  }), { timeout: 15000 });
}
