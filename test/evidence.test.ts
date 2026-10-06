import test from "node:test";
import assert from "node:assert/strict";
import { pollCi } from "../src/evidence.ts";
import type { Work } from "../src/protocol.ts";

const sha = "1".repeat(40);
const work: Work = { taskId: "evidence-task", repo: "github.com/owner/repo", ref: "main", sha, launcher: "omp", prompt: "approved", scope: "scope", restrictions: "no merge", expectedEvidence: ["CI/verify"], deadline: Date.now() + 60000, workerId: "worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: true, approved: true };

test("exact-SHA latest-attempt CI refuses prior green, missing/skipped/cancelled lanes and polling races", async () => {
  const original = globalThis.fetch;
  try {
    for (const scenario of ["green", "heads-green", "rerun-pending", "skipped", "cancelled", "missing", "wrong-sha", "attempt-race", "api-loss"] as const) {
      const current = { id: 200, workflow_id: 1000, name: "CI", head_sha: scenario === "wrong-sha" ? "2".repeat(40) : sha, head_branch: "main", run_attempt: 2, status: scenario === "rerun-pending" ? "in_progress" : "completed", conclusion: scenario === "rerun-pending" ? null : "success", html_url: "https://github.com/owner/repo/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
      const old = { ...current, id: 100, run_attempt: 1, status: "completed", conclusion: "success", head_sha: sha };
      globalThis.fetch = async (input) => {
        const url = String(input);
        if (scenario === "api-loss") return new Response("{}", { status: 503 });
        if (url.includes("/jobs?")) return Response.json({ jobs: scenario === "missing" ? [] : [{ id: 900, name: "verify", status: "completed", conclusion: ["skipped", "cancelled"].includes(scenario) ? scenario : "success", started_at: "2026-10-06T01:00:00Z", completed_at: "2026-10-06T01:00:01Z" }] });
        if (url.includes("/attempts/")) return Response.json(url.includes("/200/") ? current : old);
        if (url.includes("/actions/runs?")) return Response.json({ workflow_runs: [old, current] });
        if (url.endsWith("/actions/runs/200")) return Response.json({ ...current, run_attempt: scenario === "attempt-race" ? 3 : 2 });
        if (url.endsWith("/actions/runs/100")) return Response.json(old);
        return Response.json({ id: 42, full_name: "owner/repo", private: true });
      };
      const result = await pollCi({ ...work, ref: scenario === "heads-green" ? "refs/heads/main" : work.ref });
      const expected = ["green", "heads-green"].includes(scenario) ? "green" : ["rerun-pending", "attempt-race"].includes(scenario) ? "pending" : ["skipped", "cancelled"].includes(scenario) ? "failed" : "unknown";
      assert.equal(result.verdict, expected, scenario);
      if (scenario === "green" || scenario === "heads-green") { assert.equal(result.lanes[0]?.runId, 200); assert.equal(result.lanes[0]?.attempt, 2); }
    }
  } finally { globalThis.fetch = original; }
});

test("PR merge-ref CI requires the exact current merge SHA and authenticated matching PR association", async () => {
  const original = globalThis.fetch;
  const headSha = "2".repeat(40);
  try {
    for (const scenario of ["green", "wrong-pr", "head-sha-run", "head-sha-request", "merge-mismatch", "ref-race", "association-race", "api-loss", "unauthenticated"] as const) {
      let refReads = 0;
      let requests = 0;
      const current = { id: 200, workflow_id: 1000, name: "CI", head_sha: scenario === "head-sha-run" || scenario === "head-sha-request" ? headSha : sha, head_branch: "feature/review", pull_requests: [{ number: scenario === "wrong-pr" ? 8 : 7, url: "https://api.github.com/repos/owner/repo/pulls/7" }], run_attempt: 2, status: "completed", conclusion: "success", html_url: "https://github.com/owner/repo/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
      globalThis.fetch = async (input, init) => {
        const url = String(input);
        requests++;
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer github-token");
        if (url.endsWith("/git/ref/pull/7/merge")) {
          refReads++;
          if (scenario === "api-loss") return new Response("{}", { status: 503 });
          return Response.json({ ref: "refs/pull/7/merge", object: { type: "commit", sha: scenario === "merge-mismatch" || (scenario === "ref-race" && refReads > 1) ? "3".repeat(40) : sha } });
        }
        if (url.includes("/jobs?")) return Response.json({ jobs: [{ id: 900, name: "verify", status: "completed", conclusion: "success", started_at: "2026-10-06T01:00:00Z", completed_at: "2026-10-06T01:00:01Z" }] });
        if (url.includes("/attempts/")) return Response.json(current);
        if (url.includes("/actions/runs?")) return Response.json({ workflow_runs: [current] });
        if (url.endsWith("/actions/runs/200")) return Response.json({ ...current, pull_requests: [{ number: scenario === "association-race" ? 8 : 7 }] });
        return Response.json({ id: 42, full_name: "owner/repo", private: true });
      };
      const result = await pollCi({ ...work, ref: "refs/pull/7/merge", sha: scenario === "head-sha-request" ? headSha : sha }, scenario === "unauthenticated" ? undefined : "github-token");
      assert.equal(result.verdict, scenario === "green" ? "green" : scenario === "association-race" ? "pending" : "unknown", scenario);
      if (scenario === "unauthenticated") assert.equal(requests, 0);
      if (scenario === "green") {
        assert.equal(result.lanes[0]?.runId, 200);
        assert.equal(result.lanes[0]?.attempt, 2);
        assert.equal(refReads, 2);
      }
    }
  } finally { globalThis.fetch = original; }
});
