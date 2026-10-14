import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { Effect, Fiber } from "effect";
import { NativeControlError, NativeSubmissionId } from "@samuelho/coordinator-native-control/contracts";
import { makeNativeSession } from "../src/native-session.ts";
import { Platform } from "../src/platform.ts";

test("automatic retirement refuses accepted native work and fences new admission once idle", { timeout: 15000 }, () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-retirement-" });
  yield* platform.fs.chmod(directory, 0o700);
  const agent = platform.path.join(directory, "agent");
  yield* platform.fs.makeDirectory(agent, { mode: 0o700 });
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agent;
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previous;
  }));
  const requested = Promise.withResolvers<void>();
  let complete: (() => void) | undefined;
  let calls = 0;
  const provider = createServer((request, response) => {
    calls++;
    request.resume();
    request.once("end", () => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      const chunk = (delta: object, finish_reason: string | null) => response.write(`data: ${JSON.stringify({ id: "retirement", object: "chat.completion.chunk", created: 1, model: "local", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
      chunk({ role: "assistant", content: "accepted" }, null);
      complete = () => { chunk({}, "stop"); response.end("data: [DONE]\n\n"); };
      requested.resolve();
    });
  });
  yield* Effect.acquireRelease(Effect.promise(() => new Promise<void>(resolve => provider.listen(0, "127.0.0.1", resolve))), () => Effect.promise(() => new Promise<void>(resolve => {
    provider.closeAllConnections(); provider.close(() => resolve());
  })));
  const address = provider.address();
  assert.ok(address !== null && typeof address !== "string");
  yield* platform.fs.writeFileString(platform.path.join(agent, "models.json"), JSON.stringify({ providers: { local: {
    api: "openai-completions", baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "fixture",
    models: [{ id: "local", name: "Local", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }), { mode: 0o600 });
  const owner = yield* makeNativeSession("pi", directory, platform.path.join(directory, "sessions"), { provider: "local", id: "local" });
  const running = yield* Effect.forkChild(owner.prompt({ binding: owner.binding, submissionId: NativeSubmissionId.make("accepted"), message: "finish once" }));
  yield* Effect.promise(() => requested.promise);
  const runId = owner.currentRunId;
  assert.ok(runId);
  assert.equal(owner.retireIfIdle(), false, "Accepted streaming work must prevent automatic retirement");
  yield* owner.bound(owner.binding);
  assert.ok(complete);
  let allowed = true;
  const queued = yield* Effect.forkChild(owner.prompt({
    binding: owner.binding, submissionId: NativeSubmissionId.make("queued-broker"), message: "must remain fenced",
  }, undefined, () => allowed).pipe(Effect.match({
    onSuccess: () => assert.fail("Queued work must honor a subsequent admission fence"),
    onFailure: error => { assert.equal(error.code, "invalid_input"); },
  })));
  while (owner.pendingSubmissions < 2) yield* Effect.yieldNow;
  allowed = false;
  complete();
  const result = yield* Fiber.join(running);
  assert.equal(result.disposition, "settled");
  if (result.disposition === "settled") {
    assert.equal(result.final.runId, runId);
    assert.equal(result.final.outcome, "completed");
  }
  yield* Fiber.join(queued);
  const shellStarted = Promise.withResolvers<void>();
  const shell = yield* Effect.forkChild(owner.operation(async () => {
    shellStarted.resolve();
    return owner.session.executeBash("sleep 0.2; exit 0", undefined, { excludeFromContext: true });
  }));
  yield* Effect.promise(() => shellStarted.promise);
  assert.equal(owner.retireIfIdle(), false, "Standalone accepted native shell work also prevents retirement");
  const shellResult = yield* Fiber.join(shell);
  assert.equal(shellResult.exitCode, 0);
  assert.equal(shellResult.cancelled, false);
  assert.equal(owner.retireIfIdle(), true);
  yield* owner.prompt({ binding: owner.binding, submissionId: NativeSubmissionId.make("after-retirement"), message: "must not run" }).pipe(Effect.match({
    onSuccess: () => assert.fail("Retired owner must refuse admission"),
    onFailure: error => { assert.ok(error instanceof NativeControlError); assert.equal(error.code, "binding_mismatch"); },
  }));
  assert.equal(calls, 1, "Retirement must not replay or admit another provider call");
})).pipe(Effect.provide(Platform.layer))));
