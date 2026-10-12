import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { once } from "node:events";
import { Clock, Deferred, Effect, Fiber, Layer, ManagedRuntime, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { Broker } from "../src/broker.ts";
import { Commands, HttpError, JsonValue, Task, Address, BrokerConfig, Work, Repo, Sha, TaskId, MessageId, RunnerId, SessionId, IncarnationId, MachineId, WorkerId, PrincipalId, OwnerId, ReportState, type BrokerConfig as BrokerConfigType, type ArchiveEntry, type ArchiveRow, type CommandInput, type CommandOutput } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { Mcp } from "../src/http.ts";

const repo = Repo.make("github.com/test/broker"), sha = Sha.make("1".repeat(40));
const coordinator = "coordinator-regression-only-credential", peer = "native-peer-regression-only-credential", reviewer = "reviewer-regression-only-credential";
function response(value: JsonValue): Response { return new Response(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(value), { headers: { "content-type": "application/json" } }); }

test("native release fences and independent evidence survive stale peers and CI polling", async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-broker-" }));
  const now = await setup.runPromise(Clock.currentTimeMillis);
  let respond = (_url: string): Response => response({});
  const client = HttpClient.make((request, url) => Effect.sync(() => HttpClientResponse.fromWeb(request, respond(url.toString()))));
  const config = Schema.decodeUnknownSync(BrokerConfig, { onExcessProperty: "error" })({ database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0, backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: { [repo]: 42 }, principals: [
      { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(coordinator)) },
      { id: "peer", owner: "owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(peer)) },
      { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(reviewer)) },
    ] });
  const runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provide(Layer.merge(base, Layer.succeed(HttpClient.HttpClient, client)))));
  const broker = await runtime.runPromise(Broker);
  const mcp = await setup.runPromise(Mcp);
  // Metadata only: distinguish the original call timeout from transaction or scoped teardown stalls.
  let phase = "peer delivery";
  console.error("[BROKER-AWAIT] phase.start", phase);
  try {
    const bound = broker.server.address;
    if (bound._tag !== "InetAddressV4") throw new Error("Expected IPv4 TCP server");
    const url = `http://127.0.0.1:${bound.port}`;
    const conflict = (effect: Effect.Effect<unknown, unknown>) => setup.runPromise(effect.pipe(Effect.match({
      onFailure: (error) => { assert.ok(error instanceof HttpError); assert.equal(error.status, 409); },
      onSuccess: () => assert.fail("Expected rejected conflict"),
    })));
    let session: CommandInput<"session.heartbeat"> = { sessionId: SessionId.make("owner-session"), incarnation: IncarnationId.make("native-incarnation"), machineId: MachineId.make("machine"), host: "omp", repo, workerId: WorkerId.make("wrong-worker") };
    const key = { sessionId: session.sessionId, incarnation: session.incarnation };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session)); await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    const receiver = await setup.runPromise(Journal.make(platform.path.join(root, "receiver"), session.sessionId));
    const messageIds = Array.from({ length: 5 }, (_, index) => MessageId.make(`large-peer-${index}`));
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
    const work = Schema.decodeUnknownSync(Work, { onExcessProperty: "error" })({ taskId: "wrong-worker-offer", repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "approved bounded work", scope: "s".repeat(16000), restrictions: "no deployment", expectedEvidence: ["CI/verify"], deadline: now + 60000,
      workerId: "native-worker", githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true });
    assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(Commands["task.submit"].input), { onExcessProperty: "error" })({ ...work, deadline: JSON.parse("1e400") }));
    const unavailable = await setup.runPromise(mcp.call(url, coordinator, "task.submit", work));
    assert.equal(unavailable.offerDecision, "expired"); assert.equal(unavailable.peerSession, undefined);
    session = { ...session, workerId: work.workerId };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session)); await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    const expiring = await setup.runPromise(mcp.call(url, coordinator, "task.submit", { ...work, taskId: TaskId.make("expires-while-queued"), scope: "bounded queued offer" }));
    assert.equal(expiring.offerDecision, "pending");
    assert.ok(expiring.offerExpires !== undefined);
    const offerExpires = expiring.offerExpires;
    phase = "transaction-owned offer expiry";
    console.error("[BROKER-AWAIT] transaction.start");
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
    console.error("[BROKER-AWAIT] transaction.end");
    const expiredStored = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: expiring.taskId }));
    assert.equal(expiredStored.offerDecision, "pending");
    assert.equal(expiredStored.state, "QUEUED");
    phase = "native release fences";
    console.error("[BROKER-AWAIT] phase.start", phase);
    const acceptedWork = { ...work, taskId: TaskId.make("bound-native-work") };
    const offered = await setup.runPromise(mcp.call(url, coordinator, "task.submit", acceptedWork)); assert.equal(offered.offerDecision, "pending");
    const report = (state: typeof ReportState.Type) => mcp.call(url, peer, "task.report", { ...key, taskId: acceptedWork.taskId, state, result: { completed: true } });
    await conflict(report("SUCCEEDED"));
    const address: Address = { ...key, messageId: MessageId.make(acceptedWork.taskId), revision: 1, workerId: work.workerId, repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
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
    const next = { ...session, sessionId: SessionId.make("next-session"), incarnation: IncarnationId.make("next-incarnation"), workerId: WorkerId.make("replacement-native-worker") };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", next)); await setup.runPromise(mcp.call(url, peer, "session.poll", { sessionId: next.sessionId, incarnation: next.incarnation }));
    const handoff = { taskId: acceptedWork.taskId, oldSessionId: key.sessionId, oldIncarnation: key.incarnation, newSessionId: next.sessionId, newIncarnation: next.incarnation, revision: 2 };
    await conflict(mcp.call(url, coordinator, "task.handoff", handoff));
    assert.equal((await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }))).state, "HELD");
    await setup.runPromise(report("SUCCEEDED"));
    await conflict(report("RUNNING"));
    const stored = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: acceptedWork.taskId }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })({ ...stored, runnerId: RunnerId.make("assigned-runner") }), stored.taskId]));
    await conflict(report("SUCCEEDED"));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(stored), stored.taskId]));
    // Existing pre-cutover pending confirmations cannot become native releases after decline/runner acceptance.
    const legacyPending: Task = { ...stored, state: "QUEUED", offerDecision: "pending", offerExpires: now + 15000 };
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(legacyPending), stored.taskId]));
    await conflict(mcp.call(url, coordinator, "task.address", { taskId: stored.taskId, address }));
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address, disposition: "ready" }));
    await conflict(mcp.call(url, coordinator, "task.confirm", { taskId: stored.taskId, revision: 1 }));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).releases, []);
    await setup.runPromise(mcp.call(url, peer, "peer.reply", { ...key, taskId: stored.taskId, decision: "declined" }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })({ ...legacyPending, state: "ACCEPTED", offerDecision: "declined", runnerId: RunnerId.make("assigned-runner") }), stored.taskId]));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).releases, []);
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address, disposition: "ready" }));
    await runtime.runPromise(broker.sql.unsafe("UPDATE tasks SET data=? WHERE id=?", [Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(stored), stored.taskId]));
    const transferred = await setup.runPromise(mcp.call(url, coordinator, "task.handoff", handoff));
    assert.equal(transferred.address?.workerId, next.workerId);
    assert.ok(transferred.address);
    await conflict(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address: { ...transferred.address, workerId: work.workerId }, disposition: "ready" }));
    await setup.runPromise(mcp.call(url, coordinator, "task.address", { taskId: stored.taskId, address: transferred.address }));
    await setup.runPromise(mcp.call(url, peer, "task.readback", { taskId: stored.taskId, address: transferred.address, disposition: "ready" }));
    await setup.runPromise(mcp.call(url, coordinator, "task.confirm", { taskId: stored.taskId, revision: 2 }));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", { sessionId: next.sessionId, incarnation: next.incarnation }))).releases, []);
    phase = "independent evidence";
    console.error("[BROKER-AWAIT] phase.start", phase);
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
    phase = "sanitized snapshot";
    console.error("[BROKER-AWAIT] phase.start", phase);
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
    } finally {
      console.error("[BROKER-AWAIT] backup.dispose.start");
      await backup.dispose();
      console.error("[BROKER-AWAIT] backup.dispose.end");
    }
    phase = "assertions complete";
  } catch (error) {
    console.error("[BROKER-AWAIT] consumer.failure", phase);
    throw error;
  } finally {
    console.error("[BROKER-AWAIT] runtime.dispose.start", phase);
    await runtime.dispose();
    console.error("[BROKER-AWAIT] runtime.dispose.end");
    await setup.runPromise(platform.fs.remove(root, { recursive: true, force: true }));
    console.error("[BROKER-AWAIT] setup.dispose.start");
    await setup.dispose();
    console.error("[BROKER-AWAIT] setup.dispose.end");
  }
});

test("broker restart preserves RPC delivery, release fences and consented JSONL metadata", async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-durable-" }));
  let runtime: ManagedRuntime.ManagedRuntime<Broker, unknown> | undefined;
  try {
    await setup.runPromise(platform.fs.chmod(root, 0o700));
    const clock = await setup.runPromise(Clock.Clock);
    const now = await setup.runPromise(Clock.currentTimeMillis);
    // Fixed transaction time keeps live/offer/retention fences independent of machine speed.
    const services = Layer.merge(base, Layer.succeed(Clock.Clock, {
      currentTimeMillis: Effect.succeed(now), currentTimeMillisUnsafe: () => now,
      currentTimeNanos: clock.currentTimeNanos, currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
      monotonicTimeNanos: clock.monotonicTimeNanos, monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
      sleep: (duration) => clock.sleep(duration),
    }));
    const participantToken = "archive-participant-regression-only-credential";
    const config = Schema.decodeUnknownSync(BrokerConfig, { onExcessProperty: "error" })({
      database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0,
      backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
      repositoryIds: { [repo]: 42 }, principals: [
        { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(coordinator)) },
        { id: "peer", owner: "owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(peer)) },
        { id: "participant", owner: "participant-owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(participantToken)) },
      ],
    });
    const open = async () => {
      runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provide(services)));
      const broker = await runtime.runPromise(Broker);
      const bound = broker.server.address;
      if (bound._tag !== "InetAddressV4") throw new Error("Expected IPv4 TCP server");
      return `http://127.0.0.1:${bound.port}`;
    };
    let url = await open();
    const restart = async () => { await runtime?.dispose(); runtime = undefined; url = await open(); };
    const mcp = await setup.runPromise(Mcp);
    const reject = (status: number, effect: Effect.Effect<unknown, unknown>) => setup.runPromise(effect.pipe(Effect.match({
      onFailure: (error) => { assert.ok(error instanceof HttpError); assert.equal(error.status, status); },
      onSuccess: () => assert.fail(`Expected HTTP ${status}`),
    })));
    const session: CommandInput<"session.heartbeat"> = {
      sessionId: SessionId.make("durable-session"), incarnation: IncarnationId.make("durable-incarnation"),
      machineId: MachineId.make("durable-machine"), host: "omp", repo, workerId: WorkerId.make("durable-worker"),
    };
    const key = { sessionId: session.sessionId, incarnation: session.incarnation };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session));
    await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    const message: CommandInput<"peer.send"> = { ...key, messageId: MessageId.make("durable-message"), payload: { instruction: "bounded private peer work" }, ttlMs: 60000 };
    const sent = await setup.runPromise(mcp.call(url, coordinator, "peer.send", message));
    assert.deepEqual(sent, { delivered: false, committed: true, messageId: message.messageId });
    const work = Schema.decodeUnknownSync(Work, { onExcessProperty: "error" })({
      taskId: "durable-task", repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "approved bounded work",
      scope: "restart acceptance only", restrictions: "no deployment", expectedEvidence: ["CI/verify"],
      deadline: now + 60000, workerId: session.workerId, githubRepoId: 42, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true,
    });
    const offered = await setup.runPromise(mcp.call(url, coordinator, "task.submit", work));
    assert.equal(offered.offerDecision, "pending");
    await setup.runPromise(mcp.call(url, peer, "peer.reply", { ...key, taskId: work.taskId, decision: "accepted" }));
    const address: Address = { ...key, messageId: MessageId.make("durable-release"), revision: 1, workerId: work.workerId, repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
    await setup.runPromise(mcp.call(url, coordinator, "task.address", { taskId: work.taskId, address }));
    await setup.runPromise(mcp.call(url, peer, "task.readback", { taskId: work.taskId, address, disposition: "ready" }));
    const confirmed = await setup.runPromise(mcp.call(url, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }));
    const before = await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    assert.deepEqual(before.releases.map((task) => task.taskId), [work.taskId]);
    assert.equal(before.messages.length, 1);

    const journal = await setup.runPromise(Journal.make(platform.path.join(root, "journal"), session.sessionId));
    const file = platform.path.join(root, "host.jsonl");
    const lines = [
      { id: "root", parentId: null, type: "message", message: { role: "user", content: "TOKEN=private-capture-secret" } },
      { id: "left", parentId: "root", type: "message", message: { role: "assistant", content: [{ type: "thinking", thinking: "private-branch-thought" }] } },
      { id: "right", parentId: "root", type: "message", message: { role: "toolResult", content: "private-tool-output" } },
    ];
    const text = lines.map((line) => Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue))(line)).join("\n") + "\n";
    await setup.runPromise(platform.fs.writeFileString(file, text, { mode: 0o600 }));
    await setup.runPromise(journal.scan(file));
    assert.equal(journal.state.spool.length, 0, "Scanning without consent must retain no transcript metadata");
    const participants = [PrincipalId.make("peer"), PrincipalId.make("participant")];
    const enrollment = { sessionId: session.sessionId, participants, enabled: true };
    await reject(403, mcp.call(url, peer, "archive.consent", enrollment));
    for (const token of [peer, participantToken]) {
      const consent = await setup.runPromise(mcp.call(url, token, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: true }));
      assert.deepEqual(consent, { participant: token === peer ? "peer" : "participant", enabled: true });
    }
    assert.deepEqual(await setup.runPromise(mcp.call(url, peer, "archive.consent", enrollment)), { enabled: true });
    await setup.runPromise(journal.consent(participants));
    await setup.runPromise(journal.scan(file));
    const captured = [...journal.state.spool];
    assert.equal(captured.length, 4);
    assert.deepEqual(captured.slice(0, 3).map((entry) => entry.payload), [
      { id: "root", parentId: null, type: "message", role: "user", contentOmitted: true },
      { id: "left", parentId: "root", type: "message", role: "assistant", contentOmitted: true },
      { id: "right", parentId: "root", type: "message", role: "toolResult", contentOmitted: true },
    ]);
    assert.deepEqual(captured[3]?.payload, { coverageOnly: true, gaps: ["content-omitted", "unlinked"] });
    const append = (entry: ArchiveEntry) => mcp.call(url, peer, "archive.append", { sessionId: session.sessionId, ...entry });
    await setup.runPromise(journal.upload(append));
    assert.equal(journal.state.spool.length, 0, "The local spool clears only after broker archive acknowledgements");
    const archive = async () => {
      const rows: ArchiveRow[] = [];
      let cursor = 0;
      for (;;) {
        const page = await setup.runPromise(mcp.call(url, coordinator, "archive.list", { sessionId: session.sessionId, cursor }));
        if (!page.length) return rows;
        assert.ok(page.every((row) => row.sequence > cursor));
        rows.push(...page);
        cursor = page[page.length - 1]!.sequence;
      }
    };
    const archived = await archive();
    assert.deepEqual(archived.map(({ sequence: _sequence, ...entry }) => entry), captured.map((entry) => ({ ...entry, createdAt: now })));
    const first = captured[0];
    if (!first) throw new Error("Expected captured root metadata");
    await setup.runPromise(append(first));
    await reject(409, append({ ...first, digest: await setup.runPromise(platform.digest("incorrect digest")) }));
    assert.deepEqual(await archive(), archived);
    for (const path of [root, config.backupDirectory, platform.path.dirname(journal.path)]) assert.equal((await setup.runPromise(platform.fs.stat(path))).mode & 0o777, 0o700);
    for (const path of [file, config.database, journal.path]) assert.equal((await setup.runPromise(platform.fs.stat(path))).mode & 0o777, 0o600);
    assert.equal((await setup.runPromise(platform.fs.readFileString(journal.path))).includes("private-"), false);
    const beforeRestartTask = await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: work.taskId }));

    await restart();
    assert.deepEqual(await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: work.taskId })), beforeRestartTask);
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions, [{ ...session, status: "stale", liveUntil: 0 }]);
    await reject(409, mcp.call(url, peer, "session.poll", key));
    await reject(409, mcp.call(url, coordinator, "peer.send", message));
    // Startup invalidates liveness, not incarnation; explicitly restore and poll the same binding.
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", session));
    const after = await setup.runPromise(mcp.call(url, peer, "session.poll", key));
    assert.deepEqual(after, { ...before, archiveEnabled: true });
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions, [{ ...session, status: "live", liveUntil: now + 30000 }]);
    assert.deepEqual(await setup.runPromise(mcp.call(url, coordinator, "peer.send", message)), sent);
    await reject(409, mcp.call(url, coordinator, "peer.send", { ...message, payload: "conflicting replay" }));
    assert.deepEqual(await setup.runPromise(mcp.call(url, coordinator, "task.submit", work)), confirmed);
    await reject(409, mcp.call(url, coordinator, "task.submit", { ...work, prompt: "conflicting replay" }));
    assert.deepEqual(await setup.runPromise(mcp.call(url, peer, "task.readback", { taskId: work.taskId, address, disposition: "ready" })), confirmed);
    await reject(409, mcp.call(url, coordinator, "task.address", { taskId: work.taskId, address: { ...address, revision: 2 } }));
    await reject(409, mcp.call(url, peer, "task.readback", { taskId: work.taskId, address, disposition: "hold", reason: "cannot reset release" }));
    await setup.runPromise(mcp.call(url, peer, "peer.ack", { ...key, messageId: message.messageId }));
    await setup.runPromise(mcp.call(url, peer, "peer.ack", { ...key, messageId: message.messageId }));
    assert.deepEqual((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).messages, []);
    assert.deepEqual(await archive(), archived);
    await setup.runPromise(append(first));
    assert.deepEqual(await archive(), archived);
    const rewrittenPayload = Schema.decodeUnknownSync(Commands["archive.append"].input.fields.payload)({ id: "root", parentId: null, type: "message", role: "assistant", contentOmitted: true });
    const rewritten = { ...first, payload: rewrittenPayload, digest: await setup.runPromise(platform.fingerprint(rewrittenPayload)) };
    await setup.runPromise(append(rewritten));
    const rewrittenRows = await archive();
    assert.deepEqual(rewrittenRows.slice(0, archived.length), archived);
    assert.deepEqual(rewrittenRows.slice(archived.length).map(({ sequence: _sequence, ...entry }) => entry), [{ ...rewritten, gaps: [...first.gaps, "entry-rewrite"], createdAt: now }]);
    assert.equal(rewrittenRows.length, archived.length + 1);
    await setup.runPromise(mcp.call(url, participantToken, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: false }));
    assert.deepEqual(await archive(), []);
    assert.equal((await setup.runPromise(mcp.call(url, peer, "session.poll", key))).archiveEnabled, false);
    await reject(403, append(first));
    await reject(403, mcp.call(url, peer, "archive.consent", enrollment));
    await setup.runPromise(mcp.call(url, participantToken, "archive.participantConsent", { owner: OwnerId.make("owner"), sessionId: session.sessionId, enabled: true }));
    await setup.runPromise(mcp.call(url, peer, "archive.consent", enrollment));
    await setup.runPromise(append(first));
    assert.equal((await archive()).length, 1);
    const deleted = await setup.runPromise(mcp.call(url, peer, "archive.delete", { sessionId: session.sessionId }));
    assert.equal(deleted.revoked, true); assert.equal(deleted.deleted, true);
    await setup.runPromise(journal.erase());
    const optedOut = await setup.runPromise(Journal.make(platform.path.join(root, "journal"), session.sessionId));
    await setup.runPromise(optedOut.scan(file));
    await setup.runPromise(optedOut.upload(append));
    assert.equal(optedOut.state.consent, false);
    assert.deepEqual(optedOut.state.seen, {}); assert.deepEqual(optedOut.state.spool, []);
    assert.deepEqual(await archive(), []);
    await reject(403, append(first));

    await restart();
    assert.deepEqual(await archive(), []);
    await reject(403, append(first));
    const replacement = { ...session, incarnation: IncarnationId.make("replacement-incarnation") };
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", replacement));
    const replacementKey = { sessionId: replacement.sessionId, incarnation: replacement.incarnation };
    const replacementPoll = await setup.runPromise(mcp.call(url, peer, "session.poll", replacementKey));
    assert.deepEqual(replacementPoll.messages, []); assert.deepEqual(replacementPoll.releases, []);
    await reject(403, mcp.call(url, peer, "session.poll", key));
    await reject(403, mcp.call(url, peer, "peer.ack", { ...key, messageId: message.messageId }));
    await reject(403, mcp.call(url, peer, "task.report", { ...replacementKey, taskId: work.taskId, state: "SUCCEEDED", result: { completed: true } }));
    assert.deepEqual(await setup.runPromise(mcp.call(url, coordinator, "task.get", { taskId: work.taskId })), beforeRestartTask);
  } finally {
    try { await runtime?.dispose(); }
    finally {
      try { await setup.runPromise(platform.fs.remove(root, { recursive: true, force: true })); }
      finally { await setup.dispose(); }
    }
  }
});

test("session discovery admits unlinked repositories, isolates owners, and pages live and stale participants", async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-sessions-" }));
  const clock = await setup.runPromise(Clock.Clock);
  let now = await setup.runPromise(Clock.currentTimeMillis);
  const services = Layer.merge(base, Layer.succeed(Clock.Clock, {
    currentTimeMillis: Effect.sync(() => now), currentTimeMillisUnsafe: () => now,
    currentTimeNanos: clock.currentTimeNanos, currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
    monotonicTimeNanos: clock.monotonicTimeNanos, monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
    sleep: (duration) => clock.sleep(duration),
  }));
  const unlinkedRepo = Repo.make("github.com/test/private");
  const config = Schema.decodeUnknownSync(BrokerConfig)({
    database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0,
    backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: { [repo]: 42 },
    principals: [
      { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(coordinator)) },
      { id: "peer", owner: "owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(peer)) },
      { id: "foreign", owner: "other-owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest("foreign")) },
      { id: "unlinked", owner: "owner", roles: ["session", "coordinator"], repos: [], tokenHash: await setup.runPromise(platform.digest("unlinked")) },
      { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(reviewer)) },
    ],
  });
  const runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provideMerge(services)));
  try {
    const broker = await runtime.runPromise(Broker);
    const bound = broker.server.address;
    if (bound._tag !== "InetAddressV4") throw new Error("Expected IPv4 TCP server");
    const url = `http://127.0.0.1:${bound.port}`, mcp = await setup.runPromise(Mcp);
    const principal = await runtime.runPromise(broker.authorize(peer));
    const expected: CommandOutput<"plugin.sessions"> = [];
    for (let index = 0; index < 110; index++) {
      const session: CommandInput<"session.heartbeat"> = {
        sessionId: SessionId.make(`session-${index.toString().padStart(3, "0")}-${"s".repeat(140)}`),
        incarnation: IncarnationId.make("i".repeat(160)), machineId: MachineId.make(`machine-${index}-${"m".repeat(140)}`),
        workerId: WorkerId.make(`worker-${index}-${"w".repeat(140)}`), host: index % 2 === 0 ? "omp" : "pi", repo,
      };
      await runtime.runPromise(broker.handle(principal, { method: "session.heartbeat", params: session }));
      await runtime.runPromise(broker.handle(principal, { method: "session.poll", params: { sessionId: session.sessionId, incarnation: session.incarnation } }));
      expected.push({ ...session, status: "live", liveUntil: now + 30000 });
    }
    const first = expected[0];
    assert.ok(first);
    const discovered: CommandInput<"session.heartbeat"> = {
      sessionId: SessionId.make("unlinked"), incarnation: IncarnationId.make("unlinked"), machineId: MachineId.make("unlinked"),
      host: "pi", repo: unlinkedRepo, workerId: WorkerId.make("unlinked-worker"),
    };
    await setup.runPromise(mcp.call(url, "unlinked", "session.heartbeat", discovered));
    await setup.runPromise(mcp.call(url, "unlinked", "session.poll", { sessionId: discovered.sessionId, incarnation: discovered.incarnation }));
    expected.push({ ...discovered, status: "live", liveUntil: now + 30000 });
    await setup.runPromise(mcp.call(url, "foreign", "session.heartbeat", {
      sessionId: SessionId.make("foreign"), incarnation: IncarnationId.make("foreign"), machineId: MachineId.make("foreign"),
      host: "pi", repo: unlinkedRepo,
    }));
    const listed: CommandOutput<"plugin.sessions"> = [];
    let cursor: SessionId | undefined, pages = 0;
    do {
      const page = await setup.runPromise(mcp.call(url, coordinator, "session.list", cursor === undefined ? {} : { cursor }));
      listed.push(...page.sessions);
      cursor = page.nextCursor;
      pages++;
    } while (cursor !== undefined);
    assert.ok(pages > 1, "A registry exceeding the MCP byte budget must be completely paginated");
    assert.deepEqual(listed, expected, "Same-owner sessions remain discoverable without repository enrollment");
    assert.deepEqual((await setup.runPromise(mcp.call(url, "foreign", "session.list", {}))).sessions.map((session) => session.sessionId), ["foreign"]);
    assert.equal(await setup.runPromise(mcp.call(url, peer, "session.heartbeat", discovered).pipe(Effect.catchTag("HttpError", (error) => Effect.succeed(error.status)))), 403);
    const unauthorizedWork = Schema.decodeUnknownSync(Work)({
      taskId: "unlinked-execution", repo: unlinkedRepo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "approved bounded work",
      scope: "execution must remain denied", restrictions: "no changes", expectedEvidence: ["CI/verify"], deadline: now + 60000,
      workerId: "unlinked-worker", githubRepoId: 43, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true,
    });
    assert.equal(await setup.runPromise(mcp.call(url, "unlinked", "task.submit", unauthorizedWork).pipe(Effect.catchTag("HttpError", (error) => Effect.succeed(error.status)))), 403);
    assert.equal(await setup.runPromise(mcp.call(url, reviewer, "session.list", {}).pipe(Effect.catchTag("HttpError", (error) => Effect.succeed(error.status)))), 403);
    now += 10001;
    assert.equal((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions[0]?.status, "stale");
    await setup.runPromise(mcp.call(url, peer, "session.heartbeat", {
      sessionId: first.sessionId, incarnation: first.incarnation, machineId: first.machineId, host: first.host, repo: first.repo, workerId: first.workerId,
    }));
    assert.equal((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions[0]?.status, "stale", "Heartbeat alone must not imply active polling");
    await setup.runPromise(mcp.call(url, peer, "session.poll", { sessionId: first.sessionId, incarnation: first.incarnation }));
    assert.equal((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions[0]?.status, "live");
    now += 30000;
    assert.equal((await setup.runPromise(mcp.call(url, peer, "session.list", {}))).sessions[0]?.status, "stale");
  } finally {
    await runtime.dispose();
    await setup.runPromise(platform.fs.remove(root, { recursive: true, force: true }));
    await setup.dispose();
  }
});

test("snapshot cadence expires local files even when remote put or list fails", async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-retention-" }));
  const accessKeyFile = platform.path.join(root, "access"), secretKeyFile = platform.path.join(root, "secret");
  await setup.runPromise(platform.fs.writeFileString(accessKeyFile, "FixtureAccess", { mode: 0o600 }));
  await setup.runPromise(platform.fs.writeFileString(secretKeyFile, "FixtureSecret", { mode: 0o600 }));
  let failing = "PUT";
  const requests: string[] = [];
  const client = HttpClient.make((request) => Effect.sync(() => {
    requests.push(request.method);
    return HttpClientResponse.fromWeb(request, new Response("", { status: request.method === failing ? 503 : 200 }));
  }));
  const config: BrokerConfigType = {
    database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0,
    backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    principals: [], repositoryIds: {}, objectBackup: { endpoint: "http://127.0.0.1:1", region: "fixture", bucket: "fixture-backups", accessKeyFile, secretKeyFile },
  };
  const runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provide(Layer.merge(base, Layer.succeed(HttpClient.HttpClient, client)))));
  try {
    const broker = await runtime.runPromise(Broker);
    for (const method of ["PUT", "GET"]) {
      failing = method;
      const expired = platform.path.join(config.backupDirectory, `${method}.sqlite`);
      const recent = platform.path.join(config.backupDirectory, `recent-${method}.sqlite`);
      await setup.runPromise(platform.fs.writeFileString(expired, "", { mode: 0o600 }));
      await setup.runPromise(platform.fs.utimes(expired, new Date(0), new Date(0)));
      await setup.runPromise(platform.fs.writeFileString(recent, "", { mode: 0o600 }));
      await runtime.runPromise(broker.maintain());
      assert.equal(await setup.runPromise(platform.fs.exists(expired)), true, "One-second maintenance must not walk or expire snapshots");
      requests.length = 0;
      assert.equal((await runtime.runPromise(Effect.exit(broker.snapshot())))._tag, "Failure");
      assert.ok(requests.includes(method), "Exercise the actual remote failure stage");
      assert.equal(await setup.runPromise(platform.fs.exists(expired)), false);
      assert.equal(await setup.runPromise(platform.fs.exists(recent)), true, "Expiry uses mtime, not the file name");
      assert.ok((await setup.runPromise(platform.fs.readDirectory(config.backupDirectory))).every((file) => !file.endsWith(".partial")));
    }
  } finally {
    await runtime.dispose();
    await setup.runPromise(platform.fs.remove(root, { recursive: true }));
    await setup.dispose();
  }
});

test("broker health stays responsive while SQLite waits for a writer", { timeout: 20000 }, async () => {
  const base = Layer.merge(Platform.layer, Mcp.layer);
  const setup = ManagedRuntime.make(base);
  const platform = await setup.runPromise(Platform);
  const root = await setup.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-http-lock-" }));
  const token = `health-lock-${root}`;
  const config = Schema.decodeUnknownSync(BrokerConfig)({
    database: platform.path.join(root, "ledger.sqlite"), host: "127.0.0.1", port: 0,
    backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 60000, backupRetentionMs: 60000,
    repositoryIds: { [repo]: 42 },
    principals: [{ id: "health-peer", owner: "health-owner", roles: ["session"], repos: [repo], tokenHash: await setup.runPromise(platform.digest(token)) }],
  });
  const runtime = ManagedRuntime.make(Broker.layer(config).pipe(Layer.provide(base)));
  const children: ChildProcess[] = [];
  const message = (child: ChildProcess) => new Promise<unknown>((resolve, reject) => {
    const cleanup = () => { child.off("message", received); child.off("exit", exited); child.off("error", failed); };
    const received = (value: unknown) => { cleanup(); resolve(value); };
    const exited = (code: number | null) => { cleanup(); reject(new Error(`SQLite contention helper exited before IPC (code ${code})`)); };
    const failed = (error: Error) => { cleanup(); reject(error); };
    child.once("message", received); child.once("exit", exited); child.once("error", failed);
  });
  const start = (script: string) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", script], { stdio: ["ignore", "ignore", "inherit", "ipc"] });
    children.push(child);
    return child;
  };
  try {
    const broker = await runtime.runPromise(Broker);
    const bound = broker.server.address;
    if (bound._tag !== "InetAddressV4") throw new Error("Expected IPv4 TCP server");
    const url = `http://127.0.0.1:${bound.port}`;
    const session: CommandInput<"session.heartbeat"> = {
      sessionId: SessionId.make("health-session"), incarnation: IncarnationId.make("health-incarnation"),
      machineId: MachineId.make("health-machine"), host: "omp", repo, workerId: WorkerId.make("health-worker"),
    };
    // The observer's clock and HTTP abort remain live even when the broker's Node loop is blocked.
    // Real platform time is intentional: fake timers cannot detect starvation of a different process's HTTP loop.
    const observer = start(`
      import assert from "node:assert/strict";
      import { once } from "node:events";
      import { setTimeout as sleep } from "node:timers/promises";
      import { Layer, ManagedRuntime } from "effect";
      import { Mcp } from ${JSON.stringify(new URL("../src/http.ts", import.meta.url).href)};
      import { Platform } from ${JSON.stringify(new URL("../src/platform.ts", import.meta.url).href)};
      const runtime = ManagedRuntime.make(Layer.merge(Platform.layer, Mcp.layer));
      try {
        const mcp = await runtime.runPromise(Mcp);
        const url = ${JSON.stringify(url)}, token = ${JSON.stringify(token)}, session = ${JSON.stringify(session)};
        const key = { sessionId: session.sessionId, incarnation: session.incarnation };
        const health = async () => {
          const response = await fetch(url + "/healthz", { headers: { authorization: "Bearer " + token }, signal: AbortSignal.timeout(500) });
          return { status: response.status, body: await response.json() };
        };
        const initial = await runtime.runPromise(mcp.call(url, token, "session.heartbeat", session));
        const before = await runtime.runPromise(mcp.call(url, token, "session.poll", key));
        assert.deepEqual(await health(), { status: 200, body: { status: "ok" } });
        const start = once(process, "message");
        process.send("warm");
        await start;
        let settled = false;
        const heartbeat = runtime.runPromise(mcp.call(url, token, "session.heartbeat", session));
        heartbeat.then(() => { settled = true; }, () => { settled = true; });
        await sleep(200);
        const waitingBeforeHealth = !settled;
        let observed;
        try { observed = await health(); }
        catch (error) { observed = { error: error.name }; }
        const waitingAfterHealth = !settled;
        const renewed = await heartbeat;
        const after = await runtime.runPromise(mcp.call(url, token, "session.poll", key));
        process.send({ waitingBeforeHealth, waitingAfterHealth, observed, initial, renewed, before, after });
      } finally {
        await runtime.dispose();
        process.disconnect();
      }
    `);
    assert.equal(await message(observer), "warm");
    // Start the lock only after imports, MCP discovery and both bound-session requests are warm.
    const locker = start(`
      import { DatabaseSync } from "node:sqlite";
      const database = new DatabaseSync(${JSON.stringify(config.database)});
      database.exec("BEGIN IMMEDIATE");
      const release = () => { database.exec("ROLLBACK"); database.close(); process.disconnect(); };
      process.once("disconnect", () => process.exit());
      setTimeout(release, 2500);
      process.send("locked");
    `);
    assert.equal(await message(locker), "locked");
    const result = message(observer);
    observer.send("observe");
    const observed = Schema.decodeUnknownSync(Schema.Struct({
      waitingBeforeHealth: Schema.Boolean, waitingAfterHealth: Schema.Boolean, observed: Schema.Unknown,
      initial: Commands["session.heartbeat"].output, renewed: Commands["session.heartbeat"].output,
      before: Commands["session.poll"].output, after: Commands["session.poll"].output,
    }))(await result);
    assert.equal(observed.waitingBeforeHealth, true, "The bound heartbeat must still be waiting for the external writer");
    assert.equal(observed.waitingAfterHealth, true, "Health must finish before the blocked heartbeat");
    assert.deepEqual(observed.observed, { status: 200, body: { status: "ok" } }, "Authenticated health must meet its independent 500ms deadline during the 2500ms writer lock");
    assert.ok(observed.renewed.liveUntil > observed.initial.liveUntil, "The bound session lease must renew after the writer releases");
    assert.deepEqual(observed.after, observed.before, "The same bound session must poll successfully after the writer releases");
  } finally {
    await Promise.all(children.map(async (child) => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = once(child, "exit");
      child.kill();
      await exited;
    }));
    await runtime.dispose();
    await setup.runPromise(platform.fs.remove(root, { recursive: true, force: true }));
    await setup.dispose();
  }
});
