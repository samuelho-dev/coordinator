import { CiVerdictEnum } from "../src/contracts.ts";
import { AgentHostEnum } from "../src/agent/contracts.ts";
import { it } from "@effect/vitest"
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { NodeHttpClient } from "@effect/platform-node";
import { Clock, Effect, Fiber, Layer, ManagedRuntime, Schema } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { pollCi } from "../src/evidence.ts";
import { JsonValue, Work } from "../src/contracts.ts";
import { Evidence } from "../src/generated/database/types.ts";

function response(value: JsonValue): Response { return new Response(Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(value), { headers: { "content-type": "application/json" } }); }

const sha = Evidence.fields.sha.make("1".repeat(40));
const work = Schema.decodeUnknownSync(Work, { onExcessProperty: "error" })({ taskId: "evidence-task", repo: "github.com/owner/repo", ref: "main", sha, launcher: AgentHostEnum.omp, prompt: "approved", scope: "scope", restrictions: "no merge", expectedEvidence: ["CI/verify"], deadline: (await Effect.runPromise(Clock.currentTimeMillis)) + 60000, workerId: "worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: true, approved: true });

it("exact-SHA latest-attempt CI refuses prior green, missing/skipped/cancelled lanes and polling races", async () => {
  let respond = (_url: string): Response => response({});
  const client = HttpClient.make((request, url) => Effect.sync(() => HttpClientResponse.fromWeb(request, respond(url.toString()))));
  const runtime = ManagedRuntime.make(Layer.succeed(HttpClient.HttpClient, client));
  try {
    for (const scenario of ["green", "heads-green", "rerun-pending", "skipped", "cancelled", "missing", "wrong-sha", "attempt-race", "api-loss"]) {
      const current = { id: 200, workflow_id: 1000, name: "CI", head_sha: scenario === "wrong-sha" ? "2".repeat(40) : sha, head_branch: "main", run_attempt: 2, status: scenario === "rerun-pending" ? "in_progress" : "completed", conclusion: scenario === "rerun-pending" ? null : "success", html_url: "https://github.com/owner/repo/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
      const old = { ...current, id: 100, run_attempt: 1, status: "completed", conclusion: "success", head_sha: sha };
      respond = (input) => {
        const url = String(input);
        if (scenario === "api-loss") return new Response("{}", { status: 503 });
        if (url.includes("/jobs?")) return response({ jobs: scenario === "missing" ? [] : [{ id: 900, name: "verify", status: "completed", conclusion: ["skipped", "cancelled"].includes(scenario) ? scenario : "success", started_at: "2026-10-06T01:00:00Z", completed_at: "2026-10-06T01:00:01Z" }] });
        if (url.includes("/attempts/")) return response(url.includes("/200/") ? current : old);
        if (url.includes("/actions/runs?")) return response({ workflow_runs: [old, current] });
        if (url.endsWith("/actions/runs/200")) return response({ ...current, run_attempt: scenario === "attempt-race" ? 3 : 2 });
        if (url.endsWith("/actions/runs/100")) return response(old);
        return response({ id: 42, full_name: "owner/repo", private: true });
      };
      const result = await runtime.runPromise(pollCi({ ...work, ref: scenario === "heads-green" ? "refs/heads/main" : work.ref }));
      const expected = ["green", "heads-green"].includes(scenario) ? CiVerdictEnum.green : ["rerun-pending", "attempt-race"].includes(scenario) ? CiVerdictEnum.pending : ["skipped", "cancelled"].includes(scenario) ? CiVerdictEnum.failed : CiVerdictEnum.unknown;
      assert.equal(result.verdict, expected, scenario);
      if (scenario === "green" || scenario === "heads-green") { assert.equal(result.lanes[0]?.runId, 200); assert.equal(result.lanes[0]?.attempt, 2); }
    }
  } finally { await runtime.dispose(); }
});

it("PR merge-ref CI requires the exact current merge SHA and authenticated matching PR association", async () => {
  let respond = (_url: string): Response => response({});
  const client = HttpClient.make((request, url) => Effect.sync(() => HttpClientResponse.fromWeb(request, respond(url.toString()))));
  const runtime = ManagedRuntime.make(Layer.succeed(HttpClient.HttpClient, client));
  const headSha = Evidence.fields.sha.make("2".repeat(40));
  try {
    for (const scenario of ["green", "wrong-pr", "head-sha-run", "head-sha-request", "merge-mismatch", "ref-race", "association-race", "api-loss", "unauthenticated"]) {
      let refReads = 0;
      const current = { id: 200, workflow_id: 1000, name: "CI", head_sha: scenario === "head-sha-run" || scenario === "head-sha-request" ? headSha : sha, head_branch: "feature/review", pull_requests: [{ number: scenario === "wrong-pr" ? 8 : 7, url: "https://api.github.com/repos/owner/repo/pulls/7" }], run_attempt: 2, status: "completed", conclusion: "success", html_url: "https://github.com/owner/repo/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
      respond = (input) => {
        const url = String(input);
        if (url.endsWith("/git/ref/pull/7/merge")) {
          refReads++;
          if (scenario === "api-loss") return new Response("{}", { status: 503 });
          return response({ ref: "refs/pull/7/merge", object: { type: "commit", sha: scenario === "merge-mismatch" || (scenario === "ref-race" && refReads > 1) ? "3".repeat(40) : sha } });
        }
        if (url.includes("/jobs?")) return response({ jobs: [{ id: 900, name: "verify", status: "completed", conclusion: "success", started_at: "2026-10-06T01:00:00Z", completed_at: "2026-10-06T01:00:01Z" }] });
        if (url.includes("/attempts/")) return response(current);
        if (url.includes("/actions/runs?")) return response({ workflow_runs: [current] });
        if (url.endsWith("/actions/runs/200")) return response({ ...current, pull_requests: [{ number: scenario === "association-race" ? 8 : 7 }] });
        return response({ id: 42, full_name: "owner/repo", private: true });
      };
      const result = await runtime.runPromise(pollCi({ ...work, ref: "refs/pull/7/merge", sha: scenario === "head-sha-request" ? headSha : sha }, scenario === "unauthenticated" ? undefined : "github-token"));
      assert.equal(result.verdict, scenario === "green" ? CiVerdictEnum.green : scenario === "association-race" ? CiVerdictEnum.pending : CiVerdictEnum.unknown, scenario);
      if (scenario === "green") {
        assert.equal(result.lanes[0]?.runId, 200);
        assert.equal(result.lanes[0]?.attempt, 2);
      }
    }
  } finally { await runtime.dispose(); }
});

it("GitHub response body deadline returns unknown and closes the stalled request scope", async () => {
  let headersReceived!: () => void;
  let requestClosed!: () => void;
  const headers = new Promise<void>((resolve) => { headersReceived = resolve; });
  const closed = new Promise<void>((resolve) => { requestClosed = resolve; });
  const server = createServer((_request, response) => {
    response.on("close", requestClosed);
    response.writeHead(200, { "content-type": "application/json" });
    response.write('{"id":42');
  });
  await new Promise<void>((resolve, reject) => server.listen(0, "127.0.0.1", resolve).once("error", reject));
  const address = server.address();
  assert(address && typeof address === "object");
  const runtime = ManagedRuntime.make(Layer.merge(NodeHttpClient.layerNodeHttp, TestClock.layer()));
  try {
    const result = await runtime.runPromise(Effect.gen(function*() {
      const native = yield* HttpClient.HttpClient;
      const client = HttpClient.transform(HttpClient.mapRequest(native, (request) => {
        const target = new URL(request.url);
        return HttpClientRequest.setUrl(request, `http://127.0.0.1:${address.port}${target.pathname}${target.search}`);
      }), (response) => response.pipe(Effect.tap(() => Effect.sync(headersReceived))));
      const fiber = yield* pollCi(work).pipe(Effect.provideService(HttpClient.HttpClient, client), Effect.forkChild);
      yield* TestClock.withLive(Effect.promise(() => headers));
      yield* Effect.yieldNow;
      yield* TestClock.adjust("15 seconds");
      return yield* Fiber.join(fiber);
    }));
    assert.equal(result.verdict, CiVerdictEnum.unknown);
    await closed;
  } finally {
    await runtime.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}, 5000);
