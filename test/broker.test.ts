import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker.ts";
import { digest, rpc, type Address, type BrokerConfig, type Task, type Work } from "../src/protocol.ts";

const repo = "github.com/test/broker", sha = "1".repeat(40);
const coordinator = "coordinator-regression-only-credential", peer = "native-peer-regression-only-credential", reviewer = "reviewer-regression-only-credential";

test("native release fences and independent evidence survive stale peers and CI polling", async () => {
  const root = mkdtempSync(join(tmpdir(), "coordinator-broker-"));
  const config: BrokerConfig = { database: join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0, backupDirectory: join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: { [repo]: 42 }, principals: [
      { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: digest(coordinator) },
      { id: "peer", owner: "owner", roles: ["session"], repos: [repo], tokenHash: digest(peer) },
      { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: digest(reviewer) },
    ] };
  const broker = new Broker(config), originalFetch = globalThis.fetch;
  try {
    const server = await broker.start(), bound = server.address(); assert.ok(bound && typeof bound !== "string");
    const url = `http://127.0.0.1:${bound.port}`;
    const session = { sessionId: "owner-session", incarnation: "native-incarnation", machineId: "machine", host: "omp", repo, workerId: "wrong-worker" };
    const key = { sessionId: session.sessionId, incarnation: session.incarnation };
    await rpc(url, peer, "session.heartbeat", session); await rpc(url, peer, "session.poll", key);
    const work: Work = { taskId: "wrong-worker-offer", repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "approved bounded work", scope: "regression", restrictions: "no deployment", expectedEvidence: ["CI/verify"], deadline: Date.now() + 60000,
      workerId: "native-worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
    const unavailable = await rpc<Task>(url, coordinator, "task.submit", work);
    assert.equal(unavailable.offerDecision, "expired"); assert.equal(unavailable.peerSession, undefined);
    session.workerId = work.workerId;
    await rpc(url, peer, "session.heartbeat", session); await rpc(url, peer, "session.poll", key);
    const acceptedWork = { ...work, taskId: "bound-native-work" };
    const offered = await rpc<Task>(url, coordinator, "task.submit", acceptedWork); assert.equal(offered.offerDecision, "pending");
    const report = (state: string) => rpc(url, peer, "task.report", { ...key, taskId: acceptedWork.taskId, state, result: { completed: true } });
    await assert.rejects(report("SUCCEEDED"), /RPC 409/);
    await rpc(url, peer, "peer.reply", { ...key, taskId: acceptedWork.taskId, decision: "accepted" });
    const address: Address = { ...key, messageId: acceptedWork.taskId, revision: 1, workerId: work.workerId, repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
    await rpc(url, coordinator, "task.address", { taskId: acceptedWork.taskId, address });
    await rpc(url, peer, "task.readback", { taskId: acceptedWork.taskId, address, disposition: "ready" });
    await assert.rejects(report("FAILED"), /RPC 409/);
    await rpc(url, coordinator, "task.confirm", { taskId: acceptedWork.taskId, revision: 1 });
    await assert.rejects(rpc(url, peer, "task.readback", { taskId: acceptedWork.taskId, address, disposition: "hold", reason: "too late", nextCheck: Date.now() + 1000 }), /RPC 409/);
    await assert.rejects(rpc(url, coordinator, "task.address", { taskId: acceptedWork.taskId, address: { ...address, revision: 2 } }), /RPC 409/);
    const next = { ...session, sessionId: "next-session", incarnation: "next-incarnation" };
    await rpc(url, peer, "session.heartbeat", next); await rpc(url, peer, "session.poll", { sessionId: next.sessionId, incarnation: next.incarnation });
    const handoff = { taskId: acceptedWork.taskId, oldSessionId: key.sessionId, oldIncarnation: key.incarnation, newSessionId: next.sessionId, newIncarnation: next.incarnation, revision: 2 };
    await assert.rejects(rpc(url, coordinator, "task.handoff", handoff), /RPC 409/);
    assert.equal((await rpc<Task>(url, coordinator, "task.get", { taskId: acceptedWork.taskId })).state, "HELD");
    await report("SUCCEEDED");
    await assert.rejects(report("RUNNING"), /RPC 409/);
    const stored = await rpc<Task>(url, coordinator, "task.get", { taskId: acceptedWork.taskId });
    broker.db.prepare("UPDATE tasks SET data=? WHERE id=?").run(JSON.stringify({ ...stored, runnerId: "assigned-runner" }), stored.taskId);
    await assert.rejects(report("SUCCEEDED"), /RPC 409/);
    broker.db.prepare("UPDATE tasks SET data=? WHERE id=?").run(JSON.stringify(stored), stored.taskId);
    await rpc(url, coordinator, "task.handoff", handoff);
    const payload = { taskId: acceptedWork.taskId, sha };
    await rpc(url, coordinator, "evidence.local", { ...payload, commands: ["consumer-visible scenario"], passed: true });
    await rpc(url, coordinator, "evidence.deployment", { ...payload, environment: "regression", url: "https://example.invalid", observedState: "not deployed", provenance: "explicit negative observation" });
    await rpc(url, reviewer, "evidence.review", { ...payload, decision: "changes-requested", note: "not deployment approval" });
    const run = { id: 200, workflow_id: 1000, name: "CI", head_sha: sha, head_branch: "main", run_attempt: 2, status: "completed", conclusion: "success", html_url: "https://github.com/test/broker/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
    globalThis.fetch = async (input) => {
      const path = String(input);
      if (path.includes("/jobs?")) return Response.json({ jobs: [{ id: 900, name: "verify", status: "completed", conclusion: "success", started_at: null, completed_at: null }] });
      if (path.includes("/actions/runs?")) return Response.json({ workflow_runs: [run] });
      if (path.includes("/actions/runs/")) return Response.json(run);
      return Response.json({ id: 42, full_name: "test/broker", private: true });
    };
    for (let i = 0; i < 12; i++) await rpc(url, coordinator, "evidence.ci", { taskId: acceptedWork.taskId });
    type Observed = Task & { evidence: { kind: string; stale: boolean }[] };
    const current = await rpc<Observed>(url, coordinator, "task.get", { taskId: acceptedWork.taskId });
    assert.deepEqual(current.evidence.map((item) => item.kind).sort(), ["ci", "deployment", "human-review", "local"]);
    assert.ok(current.evidence.every((item) => !item.stale));
    await rpc(url, coordinator, "evidence.target", { taskId: acceptedWork.taskId, sha, ref: work.ref, requiredLanes: ["CI/security"] });
    const newLanes = await rpc<Observed>(url, coordinator, "task.get", { taskId: acceptedWork.taskId });
    assert.equal(newLanes.evidence.find((item) => item.kind === "ci")?.stale, true);
    assert.ok(newLanes.evidence.filter((item) => item.kind !== "ci").every((item) => !item.stale));
    await rpc(url, coordinator, "evidence.target", { taskId: acceptedWork.taskId, sha, ref: "release", requiredLanes: work.requiredLanes });
    assert.equal((await rpc<Observed>(url, coordinator, "task.get", { taskId: acceptedWork.taskId })).evidence.find((item) => item.kind === "ci")?.stale, true);
  } finally { globalThis.fetch = originalFetch; await broker.close(); rmSync(root, { recursive: true, force: true }); }
});
