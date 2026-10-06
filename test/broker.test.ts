import test from "node:test";
import assert from "node:assert/strict";
import { Clock, Deferred, Effect, Fiber, Layer, ManagedRuntime } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { Broker } from "../src/broker.ts";
import { Commands, HttpError, JsonValue, Task, type Address, type BrokerConfig, type Work } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { Mcp, decode, encodeJson } from "../src/http.ts";

const repo = "github.com/test/broker", sha = "1".repeat(40);
const coordinator = "coordinator-regression-only-credential", peer = "native-peer-regression-only-credential", reviewer = "reviewer-regression-only-credential";
function response(value: JsonValue): Response { return new Response(encodeJson(JsonValue, value), { headers: { "content-type": "application/json" } }); }

test("native release fences and independent evidence survive stale peers and CI polling", async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-broker-" }));
  const now = await setup.runPromise(Clock.currentTimeMillis);
  let respond = (_url: string): Response => response({});
  const client = HttpClient.make((request, url) => Effect.sync(() => HttpClientResponse.fromWeb(request, respond(url.toString()))));
  const config: BrokerConfig = { database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0, backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: { [repo]: 42 }, principals: [
      { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(coordinator)) },
      { id: "peer", owner: "owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(peer)) },
      { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(reviewer)) },
    ] };
  const runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provide(Layer.merge(base, Layer.succeed(HttpClient.HttpClient, client)))));
  const broker = await runtime.runPromise(Broker);
  const mcp = await setup.runPromise(Mcp);
  try {
    const bound = broker.server.address;
    if (bound._tag !== "InetAddressV4") throw new Error("Expected IPv4 TCP server");
    const url = `http://127.0.0.1:${bound.port}`;
    const conflict = (effect: Effect.Effect<unknown, unknown>) => setup.runPromise(effect.pipe(Effect.match({
      onFailure: (error) => { assert.ok(error instanceof HttpError); assert.equal(error.status, 409); },
      onSuccess: () => assert.fail("Expected rejected conflict"),
    })));
    const session = { sessionId: "owner-session", incarnation: "native-incarnation", machineId: "machine", host: "omp", repo, workerId: "wrong-worker" };
    const key = { sessionId: session.sessionId, incarnation: session.incarnation };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session)); await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    const receiver = await setup.runPromise(Journal.make(platform.path.join(root, "receiver"), session.sessionId));
    const messageIds = Array.from({ length: 5 }, (_, index) => `large-peer-${index}`);
    const largePayload = "x".repeat(7900);
    for (const messageId of messageIds) await setup.runPromise(mcp.call(url, coordinator, "peer.send", { ...key, messageId, payload: largePayload, ttlMs: 60000 }));
    const delivered = (await setup.runPromise(mcp.call(url, peer, "session.poll", key))).messages;
    assert.deepEqual(delivered.map((message) => message.messageId).sort(), messageIds);
    assert.deepEqual(delivered.map((message) => message.payload), messageIds.map(() => largePayload));
    for (const message of delivered) {
      await setup.runPromise(receiver.receive(message.messageId, message.payload));
      await setup.runPromise(mcp.call(url, peer, "peer.ack", { ...key, messageId: message.messageId }));
    }
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).messages, []);
    const reopenedReceiver = await setup.runPromise(Journal.make(platform.path.join(root, "receiver"), session.sessionId));
    assert.deepEqual(Object.keys(reopenedReceiver.state.inbox).sort(), messageIds);
    const work: Work = { taskId: "wrong-worker-offer", repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "approved bounded work", scope: "s".repeat(16000), restrictions: "no deployment", expectedEvidence: ["CI/verify"], deadline: now + 60000,
      workerId: "native-worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
    assert.throws(() => decode(Commands["task.submit"].input, { ...work, deadline: JSON.parse("1e400") }));
    const unavailable = await setup.runPromise(mcp.call(url, coordinator, "task.submit", work));
    assert.equal(unavailable.offerDecision, "expired"); assert.equal(unavailable.peerSession, undefined);
    session.workerId = work.workerId;
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session)); await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    const expiring = await setup.runPromise(mcp.call(url, coordinator, "task.submit", { ...work, taskId: "expires-while-queued", scope: "bounded queued offer" }));
    assert.equal(expiring.offerDecision, "pending");
    assert.ok(expiring.offerExpires !== undefined);
    const offerExpires = expiring.offerExpires;
    const peerPrincipal = await runtime.runPromise(broker.authorize(peer));
    await runtime.runPromise(Effect.gen(function*() {
      const locked = yield* Deferred.make<void>();
      const unlock = yield* Deferred.make<void>();
      const holder = yield* broker.sql.withTransaction(Effect.gen(function*() {
        yield* Deferred.succeed(locked, undefined);
        yield* Deferred.await(unlock);
      })).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(locked);
      const clock = yield* Clock.Clock;
      let commandTime = offerExpires - 1;
      // Immediate start reaches the held SQLite semaphore before the clock advances.
      const reply = yield* broker.handle(peerPrincipal, { method: "peer.reply", params: { ...key, taskId: expiring.taskId, decision: "accepted" } }).pipe(
        Effect.provideService(Clock.Clock, {
          currentTimeMillis: Effect.sync(() => commandTime), currentTimeMillisUnsafe: () => commandTime,
          currentTimeNanos: clock.currentTimeNanos, currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
          monotonicTimeNanos: clock.monotonicTimeNanos, monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
          sleep: (duration) => clock.sleep(duration),
        }),
        Effect.match({
          onFailure: (error) => { assert.ok(error instanceof HttpError); assert.equal(error.status, 409); },
          onSuccess: () => assert.fail("Offer expiring while queued must not be accepted"),
        }),
        Effect.forkChild({ startImmediately: true }),
      );
      commandTime = offerExpires + 1;
      yield* Deferred.succeed(unlock, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(reply);
    }));
    const expiredStored = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: expiring.taskId }));
    assert.equal(expiredStored.offerDecision, "pending");
    assert.equal(expiredStored.state, "QUEUED");
    const acceptedWork = { ...work, taskId: "bound-native-work" };
    const offered = await setup.runPromise(mcp.call(url, coordinator, "task.submit", acceptedWork)); assert.equal(offered.offerDecision, "pending");
    const report = (state: string) => mcp.call(url, peer, "task.report", { ...key, taskId: acceptedWork.taskId, state, result: { completed: true } });
    await conflict(report("SUCCEEDED"));
    const address: Address = { ...key, messageId: acceptedWork.taskId, revision: 1, workerId: work.workerId, repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
    await conflict(mcp.call(url, coordinator, "task.address", { taskId: acceptedWork.taskId, address }));
    await conflict(mcp.call(url, peer, "task.readback", { taskId: acceptedWork.taskId, address, disposition: "ready" }));
    await conflict(mcp.call(url, coordinator, "task.confirm", { taskId: acceptedWork.taskId, revision: 1 }));
    await setup.runPromise(mcp.call(url, peer, "peer.reply", { ...key, taskId: acceptedWork.taskId, decision: "accepted" }));
    await setup.runPromise(mcp.call(url, coordinator, "task.address", { taskId: acceptedWork.taskId, address }));
    await setup.runPromise(mcp.call(url, peer, "task.readback", { taskId: acceptedWork.taskId, address, disposition: "ready" }));
    await conflict(report("FAILED"));
    await setup.runPromise(mcp.call(url, coordinator, "task.confirm", { taskId: acceptedWork.taskId, revision: 1 }));
    const released = (await setup.runPromise(mcp.call(url, peer, "session.poll", key))).releases;
    assert.deepEqual(released.map((task) => task.taskId), [acceptedWork.taskId]);
    assert.deepEqual(released[0]?.readback, address);
    await conflict(mcp.call(url, peer, "task.readback", { taskId: acceptedWork.taskId, address, disposition: "hold", reason: "too late", nextCheck: now + 1000 }));
    await conflict(mcp.call(url, coordinator, "task.address", { taskId: acceptedWork.taskId, address: { ...address, revision: 2 } }));
    const next = { ...session, sessionId: "next-session", incarnation: "next-incarnation", workerId: "replacement-native-worker" };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", next)); await setup.runPromise(mcp.call(url, peer, "session.poll", { sessionId: next.sessionId, incarnation: next.incarnation }));
    const handoff = { taskId: acceptedWork.taskId, oldSessionId: key.sessionId, oldIncarnation: key.incarnation, newSessionId: next.sessionId, newIncarnation: next.incarnation, revision: 2 };
    await conflict(mcp.call(url, coordinator, "task.handoff", handoff));
    assert.equal((await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }))).state, "HELD");
    await setup.runPromise(report("SUCCEEDED"));
    await conflict(report("RUNNING"));
    const stored = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [encodeJson(Task, { ...stored, runnerId: "assigned-runner" }), stored.taskId]));
    await conflict(report("SUCCEEDED"));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [encodeJson(Task, stored), stored.taskId]));
    // Existing pre-cutover pending confirmations cannot become native releases after decline/runner acceptance.
    const legacyPending: Task = { ...stored, state: "QUEUED", offerDecision: "pending", offerExpires: now + 15000 };
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [encodeJson(Task, legacyPending), stored.taskId]));
    await conflict(mcp.call(url, coordinator, "task.address", { taskId: stored.taskId, address }));
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address, disposition: "ready" }));
    await conflict(mcp.call(url, coordinator, "task.confirm", { taskId: stored.taskId, revision: 1 }));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).releases, []);
    await setup.runPromise(mcp.call(url, peer, "peer.reply", { ...key, taskId: stored.taskId, decision: "declined" }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [encodeJson(Task, { ...legacyPending, state: "ACCEPTED", offerDecision: "declined", runnerId: "assigned-runner" }), stored.taskId]));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).releases, []);
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address, disposition: "ready" }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [encodeJson(Task, stored), stored.taskId]));
    const transferred = await setup.runPromise(mcp.call(url, coordinator, "task.handoff", handoff));
    assert.equal(transferred.address?.workerId, next.workerId);
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address: { ...transferred.address, workerId: work.workerId }, disposition: "ready" }));
    await setup.runPromise(mcp.call(url, coordinator, "task.address", { taskId: stored.taskId, address: transferred.address }));
    await setup.runPromise(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address: transferred.address, disposition: "ready" }));
    await setup.runPromise(mcp.call(url, coordinator, "task.confirm", { taskId: stored.taskId, revision: 2 }));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", { sessionId: next.sessionId, incarnation: next.incarnation }))).releases, []);
    const payload = { taskId: acceptedWork.taskId, sha };
    await setup.runPromise(mcp.call(url, coordinator, "evidence.local", { ...payload, commands: ["consumer-visible scenario"], passed: true }));
    await setup.runPromise(mcp.call(url, coordinator, "evidence.deployment", { ...payload, environment: "regression", url: "https://example.invalid", observedState: "not deployed", provenance: "explicit negative observation" }));
    await setup.runPromise(mcp.call(url, reviewer, "evidence.review", { ...payload, decision: "changes-requested", note: "not deployment approval" }));
    const run = { id: 200, workflow_id: 1000, name: "CI", head_sha: sha, head_branch: "main", run_attempt: 2, status: "completed", conclusion: "success", html_url: "https://github.com/test/broker/actions/runs/200", created_at: "2026-10-06T01:00:00Z" };
    respond = (input) => {
      const path = String(input);
      if (path.includes("/jobs?")) return response({ jobs: [{ id: 900, name: "verify", status: "completed", conclusion: "success", started_at: null, completed_at: null }] });
      if (path.includes("/actions/runs?")) return response({ workflow_runs: [run] });
      if (path.includes("/actions/runs/")) return response(run);
      return response({ id: 42, full_name: "test/broker", private: true });
    };
    for (let i = 0; i < 12; i++) await setup.runPromise(mcp.call(url, coordinator, "evidence.ci", { taskId: acceptedWork.taskId }));
    const current = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }));
    if (!current.evidence) throw new Error("Expected persisted evidence");
    assert.deepEqual(current.evidence.map((item) => item.kind).sort(), ["ci", "deployment", "human-review", "local"]);
    assert.ok(current.evidence.every((item) => !item.stale));
    await setup.runPromise(mcp.call(url, coordinator, "evidence.target", { taskId: acceptedWork.taskId, sha, ref: work.ref, requiredLanes: ["CI/security"] }));
    const newLanes = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }));
    if (!newLanes.evidence) throw new Error("Expected persisted evidence");
    assert.equal(newLanes.evidence.find((item) => item.kind === "ci")?.stale, true);
    assert.ok(newLanes.evidence.filter((item) => item.kind !== "ci").every((item) => !item.stale));
    await setup.runPromise(mcp.call(url, coordinator, "evidence.target", { taskId: acceptedWork.taskId, sha, ref: "release", requiredLanes: work.requiredLanes }));
    const newRef = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }));
    if (!newRef.evidence) throw new Error("Expected persisted evidence");
    assert.equal(newRef.evidence.find((item) => item.kind === "ci")?.stale, true);
    await runtime.runPromise(broker.sql.unsafe("INSERT INTO consents VALUES (?,?,?,?)", ["owner", session.sessionId, 1, '["owner"]']));
    await runtime.runPromise(broker.sql.unsafe("INSERT INTO participant_consents VALUES (?,?,?,?)", ["owner", session.sessionId, "owner", 1]));
    await runtime.runPromise(broker.sql.unsafe("INSERT INTO archive(owner,session,entry,digest,payload,gaps,created) VALUES (?,?,?,?,?,?,?)", ["owner", session.sessionId, "private-entry", sha, '{"text":"private transcript"}', "[]", now]));
    const snapshot = await runtime.runPromise(broker.snapshot());
    const backup = ManagedRuntime.make(SqliteClient.layer({ filename: snapshot, readonly: true }));
    try {
      const sql = await backup.runPromise(SqliteClient.SqliteClient);
      for (const table of ["archive", "consents", "participant_consents"]) assert.deepEqual(await backup.runPromise(sql.unsafe(`SELECT * FROM ${table}`)), []);
      assert.equal((await setup.runPromise(platform.fs.stat(snapshot))).mode & 0o777, 0o600);
      assert.ok((await setup.runPromise(platform.fs.readDirectory(config.backupDirectory))).every((file) => !file.endsWith(".partial")));
    } finally { await backup.dispose(); }
  } finally { await runtime.dispose(); await setup.runPromise(platform.fs.remove(root, { recursive: true, force: true })); await setup.dispose(); }
});
