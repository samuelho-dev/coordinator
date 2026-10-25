import { assert, it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Context as EffectContext, Crypto, Effect, FileSystem, Layer, Path, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { Host, serverLayer } from "../src/platform.ts";
import { createNativeObserver, isCoordinateInput, transformCoordinateInput } from "../src/plugin.ts";
import type { Context } from "../src/plugin.ts";
import type { SessionListOutput } from "../src/inspection.ts";
import { decodeConnectorResult } from "../src/inspection.ts";
import { Hex } from "effect/encoding";
import { Inspector } from "../src/inspector.ts";
import { Mcp } from "../src/mcp.ts";
import { createRexReporter, rexSequence } from "../src/rex.ts";

const context = (id = "surrounding-session"): Context => ({
  cwd: "/workspace", mode: "rpc", hasUI: false,
  model: { provider: "native-provider", id: "native-model" },
  sessionManager: { getSessionId: () => id },
  getContextUsage: () => ({ tokens: null, contextWindow: 1000, percent: null }),
  isIdle: () => true, ui: { notify: () => {} },
});
const usage = { input: 10, output: 3, cacheRead: 5, cacheWrite: 2, totalTokens: 20, cost: { input: 0.1, output: 0.2, cacheRead: 0.01, cacheWrite: 0.02, total: 0.33 } };


it("projects native lifecycle outcomes without treating tools, retries or unknown stops as failures", async () => {
  for (const harness of ["omp", "pi"] as const) {
    const observer = await Effect.runPromise(createNativeObserver(harness, "machine").pipe(Effect.provide(Host.layer)));
    const ctx = { ...context(), isIdle: () => false, agent: { kind: "main" as const } };
    observer.update(ctx, { type: "agent_start" });
    assert.deepStrictEqual(observer.presence()?.programStatus, { state: "working" });
    observer.update(ctx, { type: "tool_execution_start", toolCallId: "a", toolName: "read" });
    assert.deepStrictEqual(observer.presence()?.programStatus, { state: "working" });
    assert.strictEqual(observer.presence()?.activity?.wait, "tool");
    assert.strictEqual(observer.presence()?.kind, "main");
    observer.update(ctx, { type: "tool_execution_end", toolCallId: "a" });
    if (harness === "pi") {
      observer.update(ctx, { type: "ui_prompt_start" });
      observer.update(ctx, { type: "tool_execution_start", toolCallId: "b", toolName: "read" });
      assert.deepStrictEqual(observer.presence()?.programStatus, { state: "blocked", kind: "question" });
      assert.strictEqual(observer.presence()?.activity?.wait, "user");
      observer.update(ctx, { type: "ui_prompt_end" });
      assert.deepStrictEqual(observer.presence()?.programStatus, { state: "working" });
    }
    for (const [stopReason, state] of [["stop", "done"], ["error", "error"], ["aborted", "idle"]] as const) {
      observer.update(ctx, { type: "agent_start" });
      observer.update(ctx, { type: "message_end", message: { role: "assistant", stopReason } });
      observer.update(ctx, { type: "agent_end" });
      if (harness === "pi") {
        assert.deepStrictEqual(observer.presence()?.programStatus, { state: "working" }, "A loop end is not final settlement");
        observer.update(ctx, { type: "agent_before_settle", outcome: stopReason === "stop" ? "completed" : stopReason });
        observer.update(ctx, { type: "agent_settled" });
      }
      assert.deepStrictEqual(observer.presence()?.programStatus, { state });
    }
    observer.update(ctx, { type: "agent_end", willContinue: true });
    assert.deepStrictEqual(observer.presence()?.programStatus, { state: "working" });
    observer.update(ctx, { type: "agent_start" });
    observer.update(ctx, { type: "agent_end" });
    if (harness === "pi") observer.update(ctx, { type: "agent_settled" });
    assert.strictEqual(observer.presence()?.programStatus, undefined);
    observer.clear();
    assert.strictEqual(observer.presence(), undefined);
  }
});

it("emits complete scoped Rex records only on terminal status changes", () => {
  const output: string[] = [];
  const reporter = createRexReporter((sequence) => output.push(sequence));
  reporter.update({ state: "working" }, false);
  reporter.update({ state: "done" }, false);
  assert.deepStrictEqual(output, []);
  reporter.update({ state: "working" }, true);
  reporter.update({ state: "working" }, true);
  reporter.update({ state: "blocked", kind: "permission" }, true);
  reporter.update({ state: "working" }, true);
  assert.strictEqual(output.length, 3);
  const id = /id=([^:]+)/u.exec(output[0]!)?.[1];
  assert.match(id!, /^coordinator\/[a-f0-9]{32}$/u);
  for (const sequence of output) {
    assert.match(sequence, /^\u001b\]7501;id=coordinator\/[a-f0-9]{32}:state=/u);
    assert.match(sequence, /:app=coordinator:title=Q29vcmRpbmF0b3I=/u);
    assert.ok(Buffer.byteLength(sequence) < 4096);
  }
  assert.ok(!output[2]!.includes(":kind="));
  reporter.close(true);
  assert.ok(output.at(-1)!.includes(":state=clear:"));
  assert.strictEqual(rexSequence("550e8400-e29b-41d4-a716-446655440000", { state: "idle" }), "");
  assert.strictEqual(rexSequence("bad\u001b", { state: "idle" }), "");
  const completed: string[] = [];
  const done = createRexReporter((sequence) => completed.push(sequence));
  done.update({ state: "done" }, true);
  done.close(true);
  assert.strictEqual(completed.length, 1);
});

it("publishes surrounding native identity and clears cached counters only on real switches", async () => {
  const observer = await Effect.runPromise(createNativeObserver("omp", "machine").pipe(Effect.provide(Host.layer)));
  const native: Context = { ...context(), agent: { kind: "main", id: "Main", name: "main" } };
  observer.update(native, { message: { role: "assistant", timestamp: 12, usage } }, 20);
  assert.deepStrictEqual(observer.presence(21)?.ref, { machineId: "machine", harness: "omp", nativeSessionId: "surrounding-session", nativeAgentId: "Main" });
  observer.update(native, undefined, 22);
  assert.strictEqual(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.value, 10);
  assert.strictEqual(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.provenance.observedAt, 20);
  assert.strictEqual(observer.inspect(30)?.tokens.latestNativeUpdate.inputTokens.provenance.nativeUpdatedAt, 12);
  observer.update({ ...native, agent: { kind: "sub", id: "child" } }, undefined, 31);
  assert.strictEqual(observer.inspect(32)?.tokens.latestNativeUpdate.inputTokens.status, "unavailable");
  observer.update(context("next-session"), undefined, 33);
  assert.strictEqual(observer.presence(34)?.ref.nativeSessionId, "next-session");
  observer.clear();
  assert.strictEqual(observer.inspect(35), undefined);
});

it("queries context only on demand, preserving unknown and over-limit native estimates", async () => {
  const observer = await Effect.runPromise(createNativeObserver("pi", "machine").pipe(Effect.provide(Host.layer)));
  let queries = 0;
  const ctx: Context = { ...context(), getContextUsage: () => { queries++; return { tokens: null, contextWindow: 1000, percent: null }; } };
  observer.update(ctx, undefined, 1);
  observer.presence(2);
  assert.strictEqual(queries, 0);
  const unknown = observer.inspect(3);
  assert.strictEqual(queries, 1);
  assert.strictEqual(unknown?.context.contextUsedTokens.value, null);
  assert.strictEqual(unknown?.context.contextRemainingTokens.status, "unavailable");
  assert.strictEqual(unknown?.activity.pendingMessageCount.status, "unavailable");
  observer.update({ ...ctx, getContextUsage: () => ({ tokens: 1200, contextWindow: 1000, percent: 120 }) }, undefined, 4);
  const over = observer.inspect(5);
  assert.strictEqual(over?.context.contextUsedTokens.value, 1200);
  assert.strictEqual(over?.context.contextUsedPercent.value, 120);
  assert.strictEqual(over?.context.contextRemainingPercent.value, -20);
});

it("projects native cached aggregate and async counts without adding child costs or leaking payloads", async () => {
  const observer = await Effect.runPromise(createNativeObserver("omp", "machine").pipe(Effect.provide(Host.layer)));
  const aggregate = { input: 50, output: 20, cacheRead: 15, cacheWrite: 5, totalTokens: 90, premiumRequests: 2, cost: 1.5, subagentCost: 0.7 };
  const job = { command: "PRIVATE_COMMAND", label: "PRIVATE_LABEL" };
  const ctx: Context = {
    ...context(), sessionManager: { getSessionId: () => "native", getUsageStatistics: () => aggregate },
    getAsyncJobSnapshot: () => ({ running: [job], delivery: { queued: 1, delivering: true, pendingJobIds: ["PRIVATE_JOB_ID"] } }),
  };
  const privateMessage = { role: "assistant", timestamp: 5, usage: { ...usage, cost: { ...usage.cost }, reasoningTokens: 2, premiumRequests: 0, credits: { cost: 0.5, committedCost: 0.4, acuCost: 0 } }, content: "PRIVATE_MESSAGE", toolResults: ["PRIVATE_TOOL_RESULT"] };
  observer.update(ctx, { message: privateMessage }, 6);
  const result = observer.inspect(10);
  assert.strictEqual(result?.cost.nativeAggregate.totalCostUsd.value, 1.5);
  assert.strictEqual(result?.tokens.nativeAggregate.totalTokens.value, 90);
  assert.strictEqual(result?.tokens.nativeAggregate.inputTokens.provenance.nativeUpdatedAt, null);
  assert.strictEqual(result?.activity.activeAsyncJobCount.value, 1);
  assert.strictEqual(result?.activity.pendingAsyncWorkCount.value, 1);
  assert.strictEqual(result?.activity.activeAsyncJobCount.provenance.derived, true);
  assert.strictEqual(result?.activity.pendingAsyncWorkCount.provenance.derived, false);
  assert.strictEqual(result?.tokens.latestNativeUpdate.reasoningOutputTokens.value, 2);
  assert.strictEqual(result?.cost.latestNativeUpdate.premiumRequests.value, 0);
  assert.strictEqual(result?.cost.latestNativeUpdate.acuCost.value, 0);
  assert.strictEqual(result?.tokens.nativeAggregate.reasoningOutputTokens.status, "unavailable");
  assert.strictEqual(result?.cost.nativeAggregate.creditCost.status, "unavailable");
  const serialized = JSON.stringify(result);
  for (const secret of ["PRIVATE_COMMAND", "PRIVATE_LABEL", "PRIVATE_JOB_ID", "PRIVATE_MESSAGE", "PRIVATE_TOOL_RESULT"]) assert.strictEqual(serialized.includes(secret), false);
  // Later mutations of the native payload cannot change the retained scalar snapshot.
  privateMessage.usage.input = 999;
  privateMessage.usage.credits.cost = 999;
  assert.strictEqual(observer.inspect(11)?.tokens.latestNativeUpdate.inputTokens.value, 10);
  assert.strictEqual(observer.inspect(11)?.cost.latestNativeUpdate.creditCost.value, 0.5);
  observer.update(ctx, { message: { role: "assistant", usage } }, 12);
  assert.strictEqual(observer.inspect(13)?.tokens.latestNativeUpdate.reasoningOutputTokens.status, "unavailable");
  assert.strictEqual(observer.inspect(13)?.cost.latestNativeUpdate.creditCost.status, "unavailable");
});

it("does not infer Pi aggregate or async counts from native activity", async () => {
  const observer = await Effect.runPromise(createNativeObserver("pi", "machine").pipe(Effect.provide(Host.layer)));
  observer.update({ ...context(), isIdle: () => false });
  const result = observer.inspect();
  assert.strictEqual(result?.presence.activity?.state, "working");
  assert.strictEqual(result?.tokens.nativeAggregate.totalTokens.status, "unavailable");
  assert.strictEqual(result?.activity.activeAsyncJobCount.status, "unavailable");
  assert.strictEqual(result?.activity.pendingAsyncWorkCount.status, "unavailable");
});

it("preserves Pi native reasoning subsets without inventing unsupported provider counters", async () => {
  const observer = await Effect.runPromise(createNativeObserver("pi", "machine").pipe(Effect.provide(Host.layer)));
  observer.update(context(), { message: { role: "assistant", usage: { ...usage, reasoning: 0 } } }, 1);
  const result = observer.inspect(2);
  assert.strictEqual(result?.tokens.latestNativeUpdate.reasoningOutputTokens.value, 0);
  assert.strictEqual(result?.tokens.latestNativeUpdate.outputTokens.value, 3);
  assert.strictEqual(result?.cost.latestNativeUpdate.premiumRequests.status, "unavailable");
});

it("retains only active tool IDs and names across concurrent executions and native switches", async () => {
  const observer = await Effect.runPromise(createNativeObserver("omp", "machine").pipe(Effect.provide(Host.layer)));
  const ctx = context();
  const start = { type: "tool_execution_start", toolCallId: "a", toolName: "read", args: "PRIVATE_ARGS" };
  observer.update(ctx, start, 1);
  observer.update(ctx, { type: "tool_execution_start", toolCallId: "b", toolName: "search" }, 2);
  assert.strictEqual(observer.presence()?.activity?.currentToolName, "read");
  assert.strictEqual(observer.presence()?.activity?.wait, "tool");
  const end = { type: "tool_execution_end", toolCallId: "a", toolName: "read", result: "PRIVATE_RESULT" };
  observer.update(ctx, end, 3);
  assert.strictEqual(observer.presence()?.activity?.currentToolName, "search");
  const serialized = JSON.stringify(observer.inspect(4));
  assert.strictEqual(serialized.includes("PRIVATE_ARGS"), false);
  assert.strictEqual(serialized.includes("PRIVATE_RESULT"), false);
  observer.update(ctx, { type: "tool_execution_end", toolCallId: "b" }, 5);
  assert.strictEqual(observer.presence()?.activity?.currentToolName, undefined);
  observer.update(ctx, start, 6);
  observer.update(context("switched"), undefined, 7);
  assert.strictEqual(observer.presence()?.activity?.currentToolName, undefined);
});

it("matches only a leading bounded coordinate token and preserves native input result shapes", () => {
  for (const text of ["coordinate", "coordinate inspect", "coordinate\ninspect"]) assert.strictEqual(isCoordinateInput(text), true);
  for (const text of ["/coordinate", "please coordinate", " coordinate", "coordinates", "coordinate_task", "coordinate/file", "Coordinate"]) {
    assert.strictEqual(isCoordinateInput(text), false);
    assert.strictEqual(transformCoordinateInput("omp", { text }, { sessions: [], nextCursor: null }), undefined);
  }
  const snapshot: SessionListOutput = { sessions: [{ ref: { machineId: "remote", harness: "omp", nativeSessionId: "actual-fleet-session" }, observedAt: 1 }], nextCursor: null };
  const omp = transformCoordinateInput("omp", { text: "coordinate inspect" }, snapshot);
  const pi = transformCoordinateInput("pi", { text: "coordinate inspect" }, snapshot);
  assert.strictEqual(omp?.text.includes("actual-fleet-session"), true);
  assert.strictEqual(omp && "action" in omp, false);
  assert.strictEqual(pi && "action" in pi ? pi.action : undefined, "transform");
});

const runScript = (executable: string, args: ReadonlyArray<string>, options: ChildProcess.CommandOptions, diagnostics: "debug" | "headless" | undefined = undefined) => Effect.scoped(Effect.gen(function*() {
  const child = yield* ChildProcess.make(executable, args, { stdin: "ignore", ...options });
  const [stdout, stderr, code] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.stdout)),
    Stream.mkString(Stream.decodeText(child.stderr)),
    child.exitCode,
  ], { concurrency: "unbounded" });
  assert.strictEqual(code, 0, `Native fixture ${options.env?.FIXTURE_CASE ?? args.at(0) ?? executable} exited ${code}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  assert.strictEqual(stdout, "", "Native integrations must not write into host stdout");
  if (diagnostics) {
    if (diagnostics === "debug") {
      assert.include(stderr, "DEBUG");
      assert.include(stderr, "native detail");
    }
    if (diagnostics === "headless") {
      const warnings = stderr.split("\n").filter((line) => line.includes("Native inspector unavailable"));
      assert.lengthOf(warnings, 2, "Headless actionable warnings must be independent and deduplicated");
      assert.include(warnings.at(0) ?? "", "401");
      assert.include(warnings.at(1) ?? "", "Defect");
    }
    for (const secret of ["isolated-original-credential", "isolated-rotated-credential", "PRIVATE_REJECTION_BODY", "PRIVATE_NATIVE_DEFECT"]) assert.notInclude(stderr, secret);
  } else assert.strictEqual(stderr, "", "UI-backed native integrations must not write into host stderr");
}));

it("adds the real native coordinate shortcut without enabling disabled core keywords", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-plugin-keyword-" });
    yield* runScript("bun", ["--eval", `
      import { NodeRuntime, NodeServices } from "@effect/platform-node";
      import { Effect } from "effect";
      import { registerCoordinateKeyword } from "./src/plugin.ts";
      import { MAGIC_KEYWORDS } from "@oh-my-pi/pi-coding-agent/modes/magic-keywords";
      import { isMagicKeyword, magicKeywordRanges, setMagicKeywords } from "@oh-my-pi/pi-tui/prompt/magic-keywords";
      const check = (condition, message) => { if (!condition) throw new Error(message); };
      NodeRuntime.runMain(Effect.gen(function*() {
        for (const enabled of [[], MAGIC_KEYWORDS.slice(0, 1), MAGIC_KEYWORDS]) {
          setMagicKeywords(enabled);
          const before = JSON.stringify(MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word)));
          yield* registerCoordinateKeyword;
          check(JSON.stringify(MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word))) === before, "Core keyword settings changed");
          check(isMagicKeyword("coordinate"), "Coordinate keyword missing");
          check(JSON.stringify(magicKeywordRanges("coordinate")) === JSON.stringify([{ from: 0, to: 10 }]), "Coordinate highlight missing");
          check(magicKeywordRanges("coordinate_task").length === 0, "Unbounded coordinate highlight");
          yield* registerCoordinateKeyword;
          check(JSON.stringify(MAGIC_KEYWORDS.map(({ word }) => isMagicKeyword(word))) === before, "Duplicate registration changed core settings");
          check(JSON.stringify(magicKeywordRanges("coordinate")) === JSON.stringify([{ from: 0, to: 10 }]), "Duplicate registration changed highlight");
        }
      }).pipe(Effect.provide(NodeServices.layer)));
    `], { extendEnv: true, env: {
      HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
      COORDINATOR_CONNECTOR_CONFIG: undefined, COORDINATOR_API_URL: undefined, COORDINATOR_API_TOKEN: undefined,
    } });
  })).pipe(Effect.provide(NodeServices.layer)));
});

it("repairs startup and rotated credentials without leaking host hooks or shutdown work", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const host = yield* Host;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-plugin-errors-" });
    const tokenA = "isolated-original-credential";
    const tokenB = "isolated-rotated-credential";
    let activeHome = "";
    let activeCase = "";
    let polls = 0;
    let readerRequests = 0;
    let results = 0;
    const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
    const server = EffectContext.get(serverContext, HttpServer.HttpServer);
    if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP fixture server");
    const url = `http://127.0.0.1:${server.address.port}`;
    yield* server.serve(Effect.gen(function*() {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.url === "/mcp") {
        assert.strictEqual(request.headers.authorization, `Bearer ${tokenA}`);
        readerRequests++;
        return HttpServerResponse.text("PRIVATE_REJECTION_BODY", { status: 401 });
      }
      // HttpEffect masks handlers; this deliberately stalled response must still honor client cancellation.
      if (request.url === "/connector/disconnect") {
        if (activeCase.startsWith("startup-")) yield* fs.writeFileString(path.join(activeHome, "disconnect"), "yes");
        return yield* Effect.interruptible(Effect.never);
      }
      if (activeCase.startsWith("startup-") && request.url === "/connector/announce") {
        const home = activeHome;
        yield* fs.writeFileString(path.join(home, "announce"), "yes");
        return yield* Effect.interruptible(Effect.never).pipe(Effect.ensuring(fs.writeFileString(path.join(home, "announce-cancelled"), "yes").pipe(Effect.orDie)));
      }
      if (activeCase === "background" && polls > 0 && request.url === "/connector/poll"
        && !(yield* fs.exists(path.join(activeHome, "outage-handled")))) {
        return HttpServerResponse.text("PRIVATE_REJECTION_BODY", { status: 503 });
      }
      if (request.url === "/connector/result") {
        const result = yield* decodeConnectorResult(yield* request.json);
        if (!("code" in result.observation)) return yield* Effect.die("Expected stale-session error observation");
        assert.strictEqual(result.observation.code, "not-found");
        assert.strictEqual(result.requestId, `stale-${results + 1}`);
        results++;
        yield* fs.writeFileString(path.join(activeHome, "results"), `${results}`);
      }
      if (activeCase === "main" && polls > 0 && request.headers.authorization !== `Bearer ${tokenB}`) {
        yield* fs.writeFileString(path.join(activeHome, "inspector.token"), tokenB, { mode: 0o600 });
        return HttpServerResponse.text("PRIVATE_REJECTION_BODY", { status: 401 });
      }
      assert.strictEqual(request.headers.authorization, `Bearer ${activeCase === "main" && polls > 0 ? tokenB : tokenA}`);
      if (request.url === "/connector/poll") {
        polls++;
        yield* fs.writeFileString(path.join(activeHome, "polls"), `${polls}`);
        if (request.headers.authorization === `Bearer ${tokenB}`) yield* fs.writeFileString(path.join(activeHome, "rotated"), "yes");
      }
      return HttpServerResponse.jsonUnsafe(request.url === "/connector/poll" ? {
        requests: polls === 1 && ["background", "debug", "headless"].includes(activeCase) ? [1, 2].map((id) => ({
          requestId: `stale-${id}`, ref: { machineId: "stale-machine", harness: "omp", nativeSessionId: `stale-session-${id}` },
        })) : [],
      } : { accepted: true });
    }));
    const script = `
      import { Clock, Config, Effect, FileSystem, Layer, Logger, Path, References } from "effect";
      globalThis.COORDINATOR_HARNESS = process.env.FIXTURE_CASE === "startup-pi" ? "pi" : "omp";
      const check = (condition, message) => { if (!condition) throw new Error(message); };
      // Isolated child defaults enable diagnostics without changing the plugin's public API.
      const connectionStates = [];
      const diagnosticLogs = [];
      if (["background", "debug"].includes(process.env.FIXTURE_CASE)) References.MinimumLogLevel.defaultValue = () => "Debug";
      if (process.env.FIXTURE_CASE === "background") {
        Logger.CurrentLoggers.defaultValue = () => new Set([Logger.make((options) => {
          const { message, logLevel } = options;
          if (logLevel !== "Debug") Logger.defaultLogger.log(options);
          diagnosticLogs.push({ message, logLevel });
          if (message[0] === "Native inspector connection") connectionStates.push(message[1].state);
        })]);
      }
      // Static platform imports cache Effect defaults before this isolated fixture configures them.
      const { NodeRuntime, NodeServices } = await import("@effect/platform-node");
      const { Host } = await import("./src/platform.ts");
      const { default: plugin } = await import("./src/plugin.ts");
      NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* Config.String("HOME");
        const scenario = yield* Config.String("FIXTURE_CASE");
        const url = yield* Config.String("FIXTURE_URL");
        const configFile = path.join(home, "inspector.json");
        const config = { inspectorUrl: url, tokenFile: path.join(home, "inspector.token") };
        if (scenario !== "ambient") {
          yield* fs.writeFileString(configFile, scenario === "main" ? "{invalid" : JSON.stringify(config), { mode: 0o600 });
          if (scenario !== "missing") yield* fs.writeFileString(config.tokenFile, ${JSON.stringify(tokenA)}, { mode: 0o600 });
        }
        const handlers = new Map();
        const notices = [];
        plugin({ on: (name, handler) => handlers.set(name, handler), registerTool: () => {}, registerCommand: () => {}, sendMessage: () => {} });
        const context = { cwd: home, mode: "rpc", hasUI: scenario !== "headless",
          sessionManager: { getSessionId: () => scenario }, isIdle: () => true,
          getContextUsage: () => undefined, ui: { notify: (message, level) => notices.push({ message, level }) } };
        const event = name => Effect.promise(() => handlers.get(name)({ type: name }, context));
        let stopped = false;
        const stop = Effect.gen(function*() {
          if (stopped) return;
          stopped = true;
          yield* event("session_shutdown");
        });
        yield* Effect.addFinalizer(() => stop);
        const waitFor = (ready, label) => Effect.gen(function*() {
          const deadline = (yield* Clock.currentTimeMillis) + 30000;
          while (!(yield* ready())) {
            check((yield* Clock.currentTimeMillis) < deadline, "Inspector behavior deadline expired: " + label);
            yield* Effect.sleep("10 millis");
          }
        });
        const waitForFile = (name, value) => waitFor(() => Effect.gen(function*() {
          const file = path.join(home, name);
          if (!(yield* fs.exists(file))) return false;
          return value === undefined || (yield* fs.readFileString(file)) === value;
        }), name + (value === undefined ? "" : "=" + value));
        if (scenario === "ambient") {
          const result = yield* Effect.promise(() => handlers.get("input")({ text: "coordinate inspect" }, context));
          check(result === undefined, "Rejected coordinate reader returned input");
          check(!(yield* fs.exists(path.join(home, "state"))), "Read-only coordinate created identity");
        }
        if (scenario === "broken") context.sessionManager.getSessionId = () => { throw new Error("PRIVATE_NATIVE_DEFECT"); };
        if (scenario.startsWith("startup-")) {
          yield* event("session_start").pipe(Effect.timeout("2 seconds"));
          yield* waitForFile("announce");
          check(notices.length === 0, "Pending startup connection produced a warning");
          const before = yield* Clock.monotonicTimeNanos;
          yield* stop;
          check((yield* Clock.monotonicTimeNanos) - before < 1500000000n, "Pending announce stalled native shutdown");
          yield* waitForFile("announce-cancelled");
          yield* waitForFile("disconnect");
          yield* event("turn_start");
          check(notices.length === 0, "Startup cancellation produced a warning");
          return;
        }
        yield* event("session_start");
        if (scenario === "broken") {
          check(notices.at(0)?.message.includes("Defect"), "Native defect did not produce safe warning");
        } else {
          if (scenario === "main") {
            check(notices.at(0)?.level === "warning", "Invalid configuration did not warn");
            yield* event("turn_start");
            check(notices.length === 1, "Repeated startup failure repeated warning");
            yield* fs.writeFileString(configFile, JSON.stringify(config), { mode: 0o600 });
            yield* event("turn_start");
          }
          if (scenario === "missing") {
            const deadline = (yield* Clock.currentTimeMillis) + 9000;
            while (notices.length === 0) {
              check((yield* Clock.currentTimeMillis) < deadline, "Missing-token warning deadline expired");
              yield* Effect.sleep("10 millis");
            }
            check(notices.at(0)?.level === "warning", "Missing token did not warn");
            yield* fs.writeFileString(config.tokenFile, ${JSON.stringify(tokenA)}, { mode: 0o600 });
            yield* event("turn_start");
          }
          yield* waitForFile("polls");
          if (scenario === "background" || scenario === "debug" || scenario === "headless") {
            yield* waitForFile("results", "2");
            check((yield* fs.readFileString(path.join(home, "results"))) === "2", "Repeated stale details were not both answered");
            check(notices.length === 0, "Background connection or stale detail produced a native warning");
            yield* event("turn_start");
            if (scenario === "background") {
              yield* waitFor(() => Effect.sync(() => connectionStates.includes("unavailable")), "observed unavailable diagnostic");
              check(!connectionStates.includes("recovered"), "Connection recovered before terminal failure injection");
              check(notices.length === 0, "Reported outage produced a native warning");
            }
            const read = Effect.promise(() => handlers.get("input")({ text: "coordinate inspect" }, context));
            check((yield* read) === undefined, "Rejected reader returned input");
            check((yield* read) === undefined, "Repeated rejected reader returned input");
            if (scenario !== "headless") {
              check(notices.length === 1 && notices.at(0)?.message.includes("401"), "Actionable auth failure was masked or repeated");
            }
            if (scenario === "background") {
              check(!connectionStates.includes("recovered"), "Terminal auth failure was not exercised inside the outage");
              check(notices.at(0)?.level === "warning", "Outage auth failure was not actionable");
              yield* fs.writeFileString(path.join(home, "outage-handled"), "yes");
              yield* waitFor(() => Effect.sync(() => connectionStates.includes("recovered")), "observed stable recovery diagnostic");
              yield* event("turn_end");
              check(notices.length === 1, "Transient outage or stable recovery produced a native warning");
              yield* read;
              yield* read;
              check(notices.length === 2, "Stable recovery did not reset actionable error deduplication");
            }
            const sessionId = context.sessionManager.getSessionId;
            context.sessionManager.getSessionId = () => { throw new Error("PRIVATE_NATIVE_DEFECT"); };
            yield* event("turn_start");
            yield* event("turn_start");
            context.sessionManager.getSessionId = sessionId;
            if (scenario !== "headless") {
              check(notices.length === (scenario === "background" ? 3 : 2) && notices.at(-1)?.message.includes("Defect"), "Changed fatal failure was hidden or repeated");
            }
          }
          const identityFile = path.join(home, "state/coordinator/machine-id");
          const machineId = yield* fs.readFileString(identityFile);
          if (scenario === "main") {
            yield* waitForFile("rotated");
            check((yield* fs.readFileString(identityFile)) === machineId, "Rotation changed installation identity");
          }
          const noticeCount = notices.length;
          const before = yield* Clock.monotonicTimeNanos;
          yield* stop;
          check((yield* Clock.monotonicTimeNanos) - before < 1500000000n, "Disconnect stalled native shutdown");
          const stoppedPolls = yield* fs.readFileString(path.join(home, "polls"));
          yield* event("turn_start");
          yield* Effect.sleep("25 millis");
          check((yield* fs.readFileString(path.join(home, "polls"))) === stoppedPolls, "Stopped plugin restarted");
          check(notices.length === noticeCount, "Normal shutdown or stopped hook emitted a warning");
        }
        check(diagnosticLogs.every(({ logLevel }) => logLevel === "Debug"), "Background fixture emitted a non-debug diagnostic");
        for (const secret of [${JSON.stringify(tokenA)}, ${JSON.stringify(tokenB)}, "PRIVATE_REJECTION_BODY", "PRIVATE_NATIVE_DEFECT"]) {
          check(!JSON.stringify(notices).includes(secret), "Native warning leaked private data");
          check(!JSON.stringify(diagnosticLogs).includes(secret), "Captured native diagnostic leaked private data");
        }
      })).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer))));
    `;
    const identities = new Set<string>();
    for (const scenario of ["startup-pi", "startup-omp", "main", "missing", "broken", "ambient", "background", "debug", "headless"]) {
      activeCase = scenario;
      polls = 0;
      results = 0;
      activeHome = path.join(directory, scenario);
      yield* fs.makeDirectory(activeHome, { mode: 0o700 });
      yield* runScript(host.executable, ["--input-type=module", "--eval", script], {
        extendEnv: true, env: {
          HOME: activeHome, XDG_CONFIG_HOME: path.join(activeHome, "config"), XDG_STATE_HOME: path.join(activeHome, "state"),
          FIXTURE_CASE: scenario, FIXTURE_URL: url,
          COORDINATOR_CONNECTOR_CONFIG: scenario === "ambient" ? undefined : path.join(activeHome, "inspector.json"),
          COORDINATOR_API_URL: scenario === "ambient" ? url : undefined,
          COORDINATOR_API_TOKEN: scenario === "ambient" ? tokenA : undefined,
        },
      }, scenario === "debug" ? "debug" : scenario === "headless" ? "headless" : undefined).pipe(Effect.timeout("60 seconds"));
      if (scenario.startsWith("startup-")) assert.strictEqual(polls, 0, "Startup must not require a completed announce, and shutdown must not restart it");
      if (scenario !== "broken") {
        const identity = yield* fs.readFileString(path.join(activeHome, "state/coordinator/machine-id"));
        assert.strictEqual(identities.has(identity), false, "Independent installations must have independent identities");
        identities.add(identity);
      }
    }
    assert.isAbove(readerRequests, 0, "Coordinate authenticates without starting observation");
  })).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer))));
}, 120000);

it("rejects oversized hook input promptly while its stdin pipe remains open", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const host = yield* Host;
    const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-hook-open-pipe-" });
    const secret = "PRIVATE_OPEN_PIPE_PAYLOAD";
    const child = yield* ChildProcess.make(host.executable, [path.resolve("src/main.ts"), "hook", "claude"], {
      stdin: { stream: "pipe", endOnDone: false },
      extendEnv: true,
      env: {
        HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
        COORDINATOR_CONNECTOR_CONFIG: undefined,
        COORDINATOR_API_URL: "http://127.0.0.1:1", COORDINATOR_API_TOKEN: "isolated-open-pipe-credential",
      },
    });
    const payload = new TextEncoder().encode(JSON.stringify({ session_id: secret, padding: "x".repeat(65536) }));
    const [stdout, stderr, code] = yield* Effect.all([
      Stream.mkString(Stream.decodeText(child.stdout)),
      Stream.mkString(Stream.decodeText(child.stderr)),
      Stream.run(Stream.make(payload), child.stdin).pipe(Effect.andThen(child.exitCode)),
    ], { concurrency: "unbounded" }).pipe(Effect.timeout("5 seconds"));
    assert.strictEqual(code, 0, stderr);
    assert.strictEqual(stdout, "");
    assert.strictEqual(stderr.includes(secret), false);
    assert.strictEqual(stderr.includes("isolated-open-pipe-credential"), false);
    assert.strictEqual(yield* fs.exists(path.join(home, "state/coordinator/machine-id")), true, "Hook must reach native input processing");
  })).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer))));
}, 10000);

it("authenticates the OMP stdio reader from the native environment without creating installation state", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const host = yield* Host;
    const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-omp-reader-" });
    const token = "isolated-omp-reader-credential";
    let requests = 0;
    const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
    const server = EffectContext.get(serverContext, HttpServer.HttpServer);
    if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP fixture server");
    yield* server.serve(Effect.gen(function*() {
      const request = yield* HttpServerRequest.HttpServerRequest;
      assert.strictEqual(request.headers.authorization, `Bearer ${token}`);
      requests++;
      return HttpServerResponse.text("PRIVATE_REJECTION_BODY", { status: 401 });
    }));
    yield* runScript("bun", ["--eval", `
      import { NodeRuntime, NodeServices } from "@effect/platform-node";
      import { Config, Effect, FileSystem, Path } from "effect";
      import { StdioTransport } from "@oh-my-pi/pi-coding-agent/mcp/transports/stdio";
      const check = (condition, message) => { if (!condition) throw new Error(message); };
      NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* Config.String("HOME");
        const token = yield* Config.String("COORDINATOR_API_TOKEN");
        const transport = new StdioTransport({
          command: ${JSON.stringify(host.executable)},
          args: [path.resolve("src/main.ts"), "mcp"], env: {},
        });
        yield* Effect.addFinalizer(() => Effect.promise(() => transport.close()));
        yield* Effect.promise(() => transport.connect());
        yield* Effect.promise(() => transport.request("initialize", {
          protocolVersion: "2025-11-25", capabilities: {},
          clientInfo: { name: "coordinator-env-regression", version: "1" },
        }));
        transport.notify("notifications/initialized");
        const result = yield* Effect.promise(() => transport.request("tools/call", { name: "session.list", arguments: { limit: 1 } }));
        check(result.isError === true, "Rejected credentials must not trigger another credential source");
        check(!JSON.stringify(result).includes(token), "Reader leaked token");
        check(!JSON.stringify(result).includes("PRIVATE_REJECTION_BODY"), "Reader leaked response body");
        check(!(yield* fs.exists(path.join(home, "state"))), "Reader created native state");
      })).pipe(Effect.provide(NodeServices.layer)));
    `], { extendEnv: true, env: {
      HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
      COORDINATOR_API_URL: `http://127.0.0.1:${server.address.port}`, COORDINATOR_API_TOKEN: token,
      COORDINATOR_CONNECTOR_CONFIG: undefined,
    } }).pipe(Effect.timeout("20 seconds"));
    assert.isAbove(requests, 0, "The actual native stdio child must receive the owner's token");
  })).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer))));
}, 25000);

it("lists every authorized session across pages and machines without native capture or credential leakage", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const home = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-command-" });
    yield* fs.chmod(home, 0o700);
    const token = "private-list-credential", outsiderToken = "private-outsider-credential", emptyToken = "private-empty-credential";
    const principal = (id: string, owner: string, credential: string) => crypto.digest("SHA-256", new TextEncoder().encode(credential)).pipe(
      Effect.map((hash) => ({ id, owner, tokenHash: Hex.encode(hash) })),
    );
    const reader = yield* principal("reader", "reader-owner", token);
    const outsider = yield* principal("outsider", "outsider-owner", outsiderToken);
    const empty = yield* principal("empty", "empty-owner", emptyToken);
    const inspector = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [reader, outsider, empty] });
    for (let batch = 0; batch < 4; batch++) {
      const machineId = batch === 3 ? "remote-machine" : "local-machine";
      const sessions = Array.from({ length: batch === 3 ? 5 : 32 }, (_, index) => ({
        ref: { machineId, harness: "omp" as const, nativeSessionId: `native-${batch * 32 + index}`, nativeAgentId: `agent-${batch * 32 + index}` },
        observedAt: 10, lastActivityAt: 0, machineName: "same-hostname",
        provider: "test-provider", model: "test-model", cwd: `/workspace/${batch * 32 + index}`,
        os: { platform: "linux" as const, architecture: "x64" }, activity: { state: "idle" as const },
      }));
      yield* inspector.announce(reader, { connectorId: `connector-${batch}`, incarnation: `incarnation-${batch}`, machineId, sessions });
      if (batch === 3) yield* inspector.disconnect(reader, { connectorId: `connector-${batch}`, incarnation: `incarnation-${batch}` });
    }
    yield* inspector.announce(outsider, {
      connectorId: "hidden-connector", incarnation: "hidden-incarnation", machineId: "hidden-machine",
      sessions: [{ ref: { machineId: "hidden-machine", harness: "pi", nativeSessionId: "hidden-session" }, observedAt: 10 }],
    });
    const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
    const server = EffectContext.get(serverContext, HttpServer.HttpServer);
    if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP fixture server");
    yield* (yield* Mcp).serve(server, inspector);
    const tokenFile = path.join(home, "token");
    const configFile = path.join(home, "inspector.json");
    yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
    yield* fs.writeFileString(configFile, JSON.stringify({ inspectorUrl: `http://127.0.0.1:${server.address.port}`, tokenFile }), { mode: 0o600 });
    const script = `
      import { NodeRuntime, NodeServices } from "@effect/platform-node";
      import { Config, Effect, FileSystem, Path } from "effect";
      import plugin from "./src/plugin.ts";
      globalThis.COORDINATOR_HARNESS = process.env.FIXTURE_HARNESS;
      const check = (condition, message) => { if (!condition) throw new Error(message); };
      NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* Config.String("HOME");
        const handlers = new Map(), commands = new Map(), messages = [];
        plugin({
          on: (name, handler) => handlers.set(name, handler), registerTool: () => {},
          registerCommand: (name, command) => commands.set(name, command),
          sendMessage: message => messages.push(message),
        });
        yield* Effect.addFinalizer(() => Effect.promise(() => handlers.get("session_shutdown")()));
        const list = () => Effect.promise(() => commands.get("coordinator").handler("list", {}));
        yield* list();
        const message = messages.pop();
        const sessions = message.details.sessions;
        check(sessions.length === 101, "List silently stopped at the first API page");
        check(sessions.some(session => session.ref.machineId === "local-machine"), "Local machine missing");
        check(sessions.some(session => session.ref.machineId === "remote-machine"), "Remote machine missing");
        check(!message.content.includes("hidden-session") && !message.content.includes("hidden-machine"), "List exposed another owner's sessions");
        for (const value of ["local-machine", "remote-machine", "native-100", "agent-100", "test-provider", "test-model", "/workspace/100", "stale", "live"]) {
          check(message.content.includes(value), "Visible session metadata missing: " + value);
        }
        yield* fs.writeFileString(path.join(home, "token"), "invalid-credential", { mode: 0o600 });
        yield* list();
        const rejected = messages.pop();
        check(rejected.details === undefined && rejected.content.includes("401"), "Rejected credentials produced a session list");
        for (const credential of [${JSON.stringify(token)}, "invalid-credential"]) check(!rejected.content.includes(credential), "Command disclosed credentials");
        yield* fs.writeFileString(path.join(home, "token"), ${JSON.stringify(emptyToken)}, { mode: 0o600 });
        yield* list();
        check(messages.pop().details.sessions.length === 0, "Empty owner did not receive an empty listing");
        check(!(yield* fs.exists(path.join(home, "state"))), "Read-only command initialized native capture");
      })).pipe(Effect.provide(NodeServices.layer)));
    `;
    for (const harness of ["pi", "omp"]) {
      yield* runScript("bun", ["--eval", script], { extendEnv: true, env: {
        HOME: home, XDG_CONFIG_HOME: path.join(home, "config"), XDG_STATE_HOME: path.join(home, "state"),
        COORDINATOR_CONNECTOR_CONFIG: configFile, COORDINATOR_API_URL: undefined, COORDINATOR_API_TOKEN: undefined,
        FIXTURE_HARNESS: harness,
      } });
      yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
    }
  })).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer, Mcp.layer))));
}, 30000);
