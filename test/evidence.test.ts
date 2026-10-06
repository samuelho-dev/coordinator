import test from "node:test";
import assert from "node:assert/strict";
import { pollCi } from "../src/evidence.ts";
import type { Work } from "../src/protocol.ts";

const sha = "1".repeat(40);
const work: Work = { taskId: "evidence-task", repo: "github.com/owner/repo", ref: "main", sha, launcher: "omp", prompt: "approved", scope: "scope", restrictions: "no merge", expectedEvidence: ["CI/verify"], deadline: Date.now() + 60000, workerId: "worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: true, approved: true };

test("exact-SHA latest-attempt CI refuses prior green, missing/skipped/cancelled lanes and polling races", async () => {
  const original = globalThis.fetch;
  try {
    for (const scenario of ["green", "rerun-pending", "skipped", "cancelled", "missing", "wrong-sha", "attempt-race", "api-loss"] as const) {
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
      const result = await pollCi(work);
      const expected = scenario === "green" ? "green" : ["rerun-pending", "attempt-race"].includes(scenario) ? "pending" : ["skipped", "cancelled"].includes(scenario) ? "failed" : "unknown";
      assert.equal(result.verdict, expected, scenario);
      if (scenario === "green") { assert.equal(result.lanes[0]?.runId, 200); assert.equal(result.lanes[0]?.attempt, 2); }
    }
  } finally { globalThis.fetch = original; }
});
