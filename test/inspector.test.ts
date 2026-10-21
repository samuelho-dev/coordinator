import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { it } from "@effect/vitest";
import { Effect, Exit, Fiber, Scope } from "effect";
import { TestClock } from "effect/testing";
import { Inspector } from "../src/inspector.ts";
import {
  createInspection, metric, type InspectorConfig, type InspectorPrincipal,
  type HttpError, type MetricProvenance, type SessionInspection, type SessionListOutput, type SessionPresence,
} from "../src/inspection.ts";

const publisher: InspectorPrincipal = {
  id: "publisher", owner: "alice", machineId: "mac", tokenHash: createHash("sha256").update("publisher-secret").digest("hex"),
};
const reader: InspectorPrincipal = {
  id: "reader", owner: "alice", machineId: "linux", tokenHash: createHash("sha256").update("reader-secret").digest("hex"),
};
const outsider: InspectorPrincipal = {
  id: "outsider", owner: "bob", machineId: "mac", tokenHash: createHash("sha256").update("outsider-secret").digest("hex"),
};
const config: InspectorConfig = { host: "127.0.0.1", port: 0, principals: [publisher, reader, outsider] };
const identity = { connectorId: "native-connector", incarnation: "first" };
const presence: SessionPresence = {
  ref: { machineId: "mac", harness: "omp", nativeSessionId: "actual-native-session" }, observedAt: 9_999_999_999_999,
  connection: { state: "live", receivedAt: 9_999_999_999_999, liveUntil: 9_999_999_999_999 },
};
const provenance: MetricProvenance = {
  source: "native", scope: "process", scopeDescription: "Native process sample", observedAt: 5, nativeUpdatedAt: 4,
  estimated: false, derived: false, shared: false, partial: false, stale: false,
};

it.effect("authenticates credentials and isolates owners and machine-bound publishers", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  assert.deepEqual(yield* inspector.authorize("publisher-secret"), publisher);
  assert.equal((yield* Effect.flip(inspector.authorize("wrong-secret"))).status, 401);
  assert.equal((yield* Effect.flip(inspector.list({ ...reader, owner: "bob" }, { limit: 10 }))).status, 401);
  assert.equal((yield* Effect.flip(inspector.announce(reader, { ...identity, machineId: "mac", sessions: [presence] }))).status, 403);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  assert.equal((yield* inspector.list(reader, { limit: 10 })).sessions.length, 1);
  assert.deepEqual((yield* inspector.list(outsider, { limit: 10 })).sessions, []);
  assert.equal((yield* Effect.flip(inspector.inspect(outsider, { ref: presence.ref }))).status, 404);
  assert.equal((yield* Effect.flip(inspector.poll(reader, identity))).status, 409);
  const duplicateId = yield* Effect.flip(Inspector.make({ ...config, principals: [publisher, { ...reader, id: publisher.id }] }));
  const duplicateHash = yield* Effect.flip(Inspector.make({ ...config, principals: [publisher, { ...reader, tokenHash: publisher.tokenHash }] }));
  assert.equal(duplicateId.status, 500);
  assert.equal(duplicateHash.status, 500);
})));

it.effect("correlates an actual read-only query with its publisher, incarnation and native ref", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
  const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
  assert.ok(request);
  assert.deepEqual(request.ref, presence.ref);
  assert.deepEqual((yield* inspector.poll(publisher, identity)).requests, []);
  const observation = createInspection(presence, 123);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, { ...identity, incarnation: "forged", requestId: request.requestId, observation }))).status, 409);
  assert.equal((yield* Effect.flip(inspector.complete(reader, { ...identity, requestId: request.requestId, observation }))).status, 409);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, {
    ...identity, requestId: request.requestId,
    observation: { ...observation, presence: { ...presence, ref: { ...presence.ref, nativeSessionId: "another" } } },
  }))).status, 409);
  yield* inspector.complete(publisher, { ...identity, requestId: request.requestId, observation });
  const actual = yield* Fiber.join(query);
  assert.notEqual(actual.presence.connection?.receivedAt, presence.connection?.receivedAt);
  assert.notEqual(actual.queriedAt, 123);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: request.requestId, observation }))).status, 409);
})));

it.effect("disposes timed-out and cancelled reads and frees bounded request capacity", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const timed = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  const timedRequest = (yield* inspector.poll(publisher, identity)).requests.at(0);
  assert.ok(timedRequest);
  yield* TestClock.adjust(10_001);
  assert.equal((yield* Fiber.join(timed)).status, 504);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: timedRequest.requestId, observation: createInspection(presence, 0) }))).status, 409);
  const reads: Array<Fiber.Fiber<SessionInspection, HttpError>> = [];
  for (let index = 0; index < 32; index++) {
    const read = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
    reads.push(read);
  }
  assert.equal((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 429);
  const cancelledRequest = (yield* inspector.poll(publisher, identity)).requests.at(0);
  assert.ok(cancelledRequest);
  yield* Fiber.interruptAll(reads);
  assert.deepEqual((yield* inspector.poll(publisher, identity)).requests, []);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: cancelledRequest.requestId, observation: createInspection(presence, 0) }))).status, 409);
  const replacement = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
  assert.equal((yield* inspector.poll(publisher, identity)).requests.length, 1);
  yield* Fiber.interrupt(replacement);
})));

it.effect("uses receiver freshness and fences reconnects, duplicate publishers and native session switches", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const listed = (yield* inspector.list(reader, { limit: 10 })).sessions.at(0);
  assert.equal(listed?.connection?.state, "live");
  assert.notEqual(listed?.connection?.liveUntil, presence.connection?.liveUntil);
  assert.equal((yield* Effect.flip(inspector.announce(publisher, { ...identity, incarnation: "second", machineId: "mac", sessions: [presence] }))).status, 409);
  assert.equal((yield* Effect.flip(inspector.announce(publisher, { ...identity, connectorId: "other", machineId: "mac", sessions: [presence] }))).status, 409);
  const switched = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [{ ...presence, ref: { ...presence.ref, nativeSessionId: "switched-session" } }] });
  assert.equal((yield* Fiber.join(switched)).status, 409);
  assert.equal((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 404);
  yield* TestClock.adjust(30_001);
  assert.equal((yield* inspector.list(reader, { limit: 10 })).sessions.at(0)?.connection?.state, "stale");
  yield* inspector.announce(publisher, { ...identity, incarnation: "second", machineId: "mac", sessions: [presence] });
  assert.equal((yield* Effect.flip(inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] }))).status, 409);
  yield* inspector.disconnect(publisher, { ...identity, incarnation: "second" });
  assert.equal((yield* inspector.list(reader, { limit: 10 })).sessions.at(0)?.connection?.state, "stale");
  assert.equal((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 409);
  yield* TestClock.adjust(120_001);
  assert.deepEqual((yield* inspector.list(reader, { limit: 10 })).sessions, []);
})));

it.effect("marks a process shared across native sessions without charging either session owned totals", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  const process = { pid: 123, startIdentity: "birth", ownership: "owned" };
  const first: SessionPresence = { ...presence, processes: [{ ...process, ownership: "owned" }] };
  const second: SessionPresence = { ...first, ref: { ...first.ref, nativeSessionId: "second-session" } };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first, second] });
  assert.ok((yield* inspector.list(reader, { limit: 10 })).sessions.every((session) => session.processes?.at(0)?.ownership === "shared"));
  for (const native of [first, second]) {
    const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: native.ref }), { startImmediately: true });
    const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
    assert.ok(request);
    const empty = createInspection(native, 0);
    yield* inspector.complete(publisher, {
      ...identity, requestId: request.requestId,
      observation: { ...empty, resources: {
        processes: [{ process: { ...process, ownership: "owned" }, cpuPercent: metric("percent-one-core", 100, provenance), cpuTimeMs: metric("milliseconds", 50, provenance), rssBytes: metric("bytes", 1024, provenance) }],
        ownedProcessCount: metric("count", 1, provenance), ownedCpuPercent: metric("percent-one-core", 100, provenance), ownedRssBytes: metric("bytes", 1024, provenance),
      } },
    });
    const actual = yield* Fiber.join(query);
    assert.equal(actual.resources.processes.at(0)?.process.ownership, "shared");
    assert.equal(actual.resources.processes.at(0)?.rssBytes.value, 1024);
    assert.equal(actual.resources.processes.at(0)?.rssBytes.provenance.shared, true);
    assert.equal(actual.resources.ownedRssBytes.status, "unavailable");
    assert.equal(actual.resources.ownedCpuPercent.status, "unavailable");
    assert.equal(actual.resources.ownedProcessCount.status, "unavailable");
  }
  const nextBirth: SessionPresence = { ...second, processes: [{ ...process, startIdentity: "new-birth", ownership: "owned" }] };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first, nextBirth] });
  assert.ok((yield* inspector.list(reader, { limit: 10 })).sessions.every((session) => session.processes?.at(0)?.ownership === "owned"));
})));

it.effect("paginates by actual response bytes using opaque owner-bound cursors", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  const large = Array.from({ length: 5 }, (_, index): SessionPresence => ({
    ...presence, ref: { ...presence.ref, nativeSessionId: `session-${index}` },
    activity: { state: "idle", goals: Array.from({ length: 100 }, (_, goal): { id: string; state: "pending" } => ({ id: `${goal}`.padEnd(256, "x"), state: "pending" })) },
  }));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: large });
  const first = yield* inspector.list(reader, { limit: 100 });
  assert.ok(Buffer.byteLength(JSON.stringify(first), "utf8") <= 48 * 1024);
  assert.ok(first.nextCursor);
  assert.equal((yield* Effect.flip(inspector.list(outsider, { limit: 100, cursor: first.nextCursor }))).status, 400);
  assert.equal((yield* Effect.flip(inspector.list(reader, { limit: 100, cursor: first.nextCursor, machineId: "other" }))).status, 400);
  const found = new Set(first.sessions.map((session) => session.ref.nativeSessionId));
  let cursor: string | null = first.nextCursor;
  while (cursor !== null) {
    const page: SessionListOutput = yield* inspector.list(reader, { limit: 100, cursor });
    assert.ok(Buffer.byteLength(JSON.stringify(page), "utf8") <= 48 * 1024);
    for (const session of page.sessions) {
      assert.equal(found.has(session.ref.nativeSessionId), false);
      found.add(session.ref.nativeSessionId);
    }
    cursor = page.nextCursor;
  }
  assert.equal(found.size, large.length);
})));

it.effect("closes pending readers with the registry scope and rejects late results", () => Effect.gen(function*() {
  const scope = yield* Scope.make();
  const inspector = yield* Inspector.make(config).pipe(Scope.provide(scope));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const read = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
  assert.ok(request);
  yield* Scope.close(scope, Exit.void);
  assert.equal((yield* Fiber.join(read)).status, 503);
  assert.equal((yield* Effect.flip(inspector.complete(publisher, {
    ...identity, requestId: request.requestId, observation: createInspection(presence, 0),
  }))).status, 401);
}));

it.effect("bounds connectors and reclaims expired presence rather than retaining registrations", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config);
  for (let index = 0; index < 256; index++) {
    yield* inspector.announce(publisher, { ...identity, connectorId: `connector-${index}`, machineId: "mac", sessions: [] });
  }
  assert.equal((yield* Effect.flip(inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [] }))).status, 429);
  yield* TestClock.adjust(120_001);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  assert.equal((yield* inspector.list(reader, { limit: 10 })).sessions.length, 1);
})));
