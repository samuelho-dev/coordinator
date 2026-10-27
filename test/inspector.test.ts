import { assert, it } from "@effect/vitest";
import { Clock, Crypto, Effect, Exit, Fiber, Scope } from "effect";
import { Hex } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { TestClock } from "effect/testing";
import { Inspector } from "../src/inspector.ts";
import {
  createInspection, metric, type InspectorConfig, type InspectorPrincipal,
  type HttpError, type MetricProvenance, type SessionInspection, type SessionListOutput, type SessionPresence, type SessionRef,
} from "../src/inspection.ts";

const { publisher, reader, outsider, config } = await Effect.runPromise(Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto;
  const publisher: InspectorPrincipal = {
    id: "publisher", owner: "alice", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode("publisher-secret"))),
  };
  const reader: InspectorPrincipal = {
    id: "reader", owner: "alice", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode("reader-secret"))),
  };
  const outsider: InspectorPrincipal = {
    id: "outsider", owner: "bob", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode("outsider-secret"))),
  };
  const config: InspectorConfig = { host: "127.0.0.1", port: 0, principals: [publisher, reader, outsider] };
  return { publisher, reader, outsider, config };
}).pipe(Effect.provide(NodeServices.layer)));
const identity = { connectorId: "native-connector", incarnation: "first" };
const presence: SessionPresence = {
  ref: { machineId: "mac", harness: "omp", nativeSessionId: "actual-native-session" }, observedAt: 9_999_999_999_999,
  connection: { state: "live", receivedAt: 9_999_999_999_999, liveUntil: 9_999_999_999_999 },
  machine: { name: "Mac", os: { platform: "darwin", architecture: "arm64" } },
  agent: { kind: "primary", name: "reviewer", profile: "careful" },
  model: { provider: "provider", id: "model" },
  workspace: {
    cwd: "/work/linked/subdir",
    project: { root: "/work/linked", source: "configured", id: "project" },
    git: { state: "available", observedAt: 5, worktreeRoot: "/work/linked", commonDir: "/work/main/.git", branch: null, head: "a".repeat(64) },
  },
};
const provenance: MetricProvenance = {
  source: "native", scope: "process", scopeDescription: "Native process sample", observedAt: 5, nativeUpdatedAt: 4,
  estimated: false, derived: false, shared: false, partial: false, stale: false,
};

it.effect("marks disconnected and aged machine readings stale without rewriting source times", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  const at = yield* Clock.currentTimeMillis;
  const p: MetricProvenance = { ...provenance, scope: "host", observedAt: at, sampleWindow: { startedAt: at, endedAt: at }, shared: true };
  const resources = {
    scope: "host" as const, scopeId: "boot", source: "linux-proc", observedAt: at,
    cpu: { usagePercent: metric("percent-one-core", 50, p), capacityCores: metric("cores", 2, p), usedPercent: metric("percent", 25, p) },
    memory: { usedBytes: metric("bytes", 20, p), capacityBytes: metric("bytes", 100, p), availableBytes: metric("bytes", 80, p), usedPercent: metric("percent", 20, p) },
  };
  const source = { ...presence, machine: { ...presence.machine, resources } };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [source] });
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions[0]?.machine?.resources?.cpu.usagePercent.provenance.stale, false);
  yield* TestClock.adjust("31 seconds");
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [source] });
  const aged = (yield* inspector.list(reader, { limit: 10 })).sessions[0]?.machine?.resources;
  assert.strictEqual(aged?.cpu.usagePercent.provenance.stale, true);
  assert.strictEqual(aged?.memory.availableBytes.provenance.stale, true);
  assert.strictEqual(aged?.observedAt, at);
  assert.strictEqual(aged?.cpu.usagePercent.provenance.observedAt, at);
  assert.deepStrictEqual(aged?.cpu.usagePercent.provenance.sampleWindow, p.sampleWindow);
  assert.deepStrictEqual((yield* inspector.list(outsider, { limit: 10 })).sessions, []);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [{ ...source, machine: { ...source.machine, resources: { ...resources, observedAt: yield* Clock.currentTimeMillis } } }] });
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions[0]?.machine?.resources?.cpu.capacityCores.provenance.stale, false);
  yield* inspector.disconnect(publisher, identity);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions[0]?.machine?.resources?.cpu.capacityCores.provenance.stale, true);
})));

it.effect("preserves per-session percentages while demoting duplicate process claims and aging source readings", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  const at = yield* Clock.currentTimeMillis;
  const p = { ...provenance, observedAt: at };
  const resources: NonNullable<SessionPresence["resources"]> = {
    observedAt: at, machineScopeId: "boot", attribution: "owned",
    cpuPercent: metric("percent-one-core", 250, p), cpuCapacityPercent: metric("percent", 62.5, p),
    rssBytes: metric("bytes", 800, p), rssCapacityPercent: metric("percent", 80, p),
  };
  const first: SessionPresence = { ...presence, resources, processes: [{ pid: 123, startIdentity: "birth", ownership: "owned" }] };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first] });
  const exclusive = (yield* inspector.list(reader, { limit: 10 })).sessions[0]?.resources;
  assert.strictEqual(exclusive?.attribution, "owned");
  assert.strictEqual(exclusive?.cpuCapacityPercent.value, 62.5);
  assert.strictEqual(exclusive?.rssCapacityPercent.provenance.shared, false);
  const second: SessionPresence = { ...first, ref: { ...first.ref, nativeSessionId: "second" } };
  const otherIdentity = { connectorId: "second-connector", incarnation: "first" };
  yield* inspector.announce(publisher, { ...otherIdentity, machineId: "mac", sessions: [second] });
  const shared = (yield* inspector.list(reader, { limit: 10 })).sessions;
  for (const row of shared) {
    assert.strictEqual(row.resources?.attribution, "shared");
    assert.strictEqual(row.resources?.cpuCapacityPercent.value, 62.5);
    assert.strictEqual(row.resources?.rssCapacityPercent.value, 80);
    assert.ok(row.resources);
    for (const value of [row.resources.cpuPercent, row.resources.cpuCapacityPercent, row.resources.rssBytes, row.resources.rssCapacityPercent]) {
      assert.strictEqual(value.provenance.shared, true);
    }
  }
  const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: first.ref }), { startImmediately: true });
  const request = (yield* inspector.poll(publisher, identity)).requests[0];
  assert.ok(request);
  yield* inspector.complete(publisher, { ...identity, requestId: request.requestId, observation: createInspection(first, at) });
  assert.strictEqual((yield* Fiber.join(query)).presence.resources?.attribution, "shared");
  yield* inspector.disconnect(publisher, otherIdentity);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.find((row) => row.ref.nativeSessionId === first.ref.nativeSessionId)?.resources?.attribution, "owned");
  yield* TestClock.adjust("31 seconds");
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first] });
  const aged = (yield* inspector.list(reader, { limit: 10 })).sessions.find((row) => row.ref.nativeSessionId === first.ref.nativeSessionId)?.resources;
  assert.strictEqual(aged?.cpuCapacityPercent.provenance.stale, true);
  assert.strictEqual(aged?.rssCapacityPercent.provenance.stale, true);
  assert.strictEqual(aged?.observedAt, at);
  assert.strictEqual(aged?.cpuCapacityPercent.value, 62.5);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [{ ...first, resources: { ...resources, observedAt: yield* Clock.currentTimeMillis } }] });
  yield* inspector.disconnect(publisher, identity);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.find((row) => row.ref.nativeSessionId === first.ref.nativeSessionId)?.resources?.rssCapacityPercent.provenance.stale, true);
})));

it.effect("authenticates owner keys across machines while isolating owners and connector principals", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  assert.deepStrictEqual(yield* inspector.authorize("publisher-secret"), publisher);
  assert.strictEqual((yield* Effect.flip(inspector.authorize("wrong-secret"))).status, 401);
  assert.strictEqual((yield* Effect.flip(inspector.list({ ...reader, owner: "bob" }, { limit: 10 }))).status, 401);
  assert.strictEqual((yield* Effect.flip(inspector.list({ ...reader, tokenHash: publisher.tokenHash }, { limit: 10 }))).status, 401);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const linuxIdentity = { connectorId: "linux-connector", incarnation: "first" };
  const linux: SessionPresence = { ...presence, ref: { ...presence.ref, machineId: "linux" } };
  yield* inspector.announce(publisher, { ...linuxIdentity, machineId: "linux", sessions: [linux] });
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.length, 2);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10, machineId: "linux" })).sessions.at(0)?.ref.machineId, "linux");
  assert.deepStrictEqual((yield* inspector.list(outsider, { limit: 10 })).sessions, []);
  assert.deepStrictEqual((yield* inspector.list(outsider, { limit: 10, machineId: "linux" })).sessions, []);
  for (const native of [presence, linux]) {
    assert.strictEqual((yield* Effect.flip(inspector.inspect(outsider, { ref: native.ref }))).status, 404);
  }
  assert.strictEqual((yield* Effect.flip(inspector.poll(reader, identity))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.poll(outsider, identity))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.disconnect(reader, identity))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.disconnect(outsider, identity))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.announce(reader, { ...identity, machineId: "mac", sessions: [presence] }))).status, 403);
  for (const native of [presence, linux]) {
    const connector = native.ref.machineId === "mac" ? identity : linuxIdentity;
    const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: native.ref }), { startImmediately: true });
    const request = (yield* inspector.poll(publisher, connector)).requests.at(0);
    if (!(request)) throw new Error("Expected truthy value");
    const otherConnector = native.ref.machineId === "mac" ? linuxIdentity : identity;
    assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, {
      ...otherConnector, requestId: request.requestId, observation: createInspection(native, 0),
    }))).status, 409);
    assert.strictEqual((yield* Effect.flip(inspector.complete(outsider, {
      ...connector, requestId: request.requestId, observation: createInspection(native, 0),
    }))).status, 409);
    yield* inspector.complete(publisher, { ...connector, requestId: request.requestId, observation: createInspection(native, 0) });
    assert.strictEqual((yield* Fiber.join(query)).presence.ref.machineId, native.ref.machineId);
  }
  const duplicateId = yield* Effect.flip(Inspector.make({ ...config, principals: [publisher, { ...reader, id: publisher.id }] }).pipe(Effect.provide(NodeServices.layer)));
  const duplicateHash = yield* Effect.flip(Inspector.make({ ...config, principals: [publisher, { ...reader, tokenHash: publisher.tokenHash }] }).pipe(Effect.provide(NodeServices.layer)));
  assert.strictEqual(duplicateId.status, 500);
  assert.strictEqual(duplicateHash.status, 500);
})));

it.effect("rejects mixed-machine refs and connector rebinding without changing published presence", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  const linux: SessionPresence = { ...presence, ref: { ...presence.ref, machineId: "linux" } };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, {
    ...identity, connectorId: "mixed", machineId: "mac", sessions: [presence, linux],
  }))).status, 403);
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, {
    ...identity, machineId: "mac", sessions: [{ ...presence, parent: linux.ref }],
  }))).status, 403);
  for (const incarnation of [identity.incarnation, "second"]) {
    assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, {
      ...identity, incarnation, machineId: "linux", sessions: [linux],
    }))).status, 403);
  }
  yield* TestClock.adjust(30_001);
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, {
    ...identity, incarnation: "second", machineId: "linux", sessions: [linux],
  }))).status, 403);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.at(0)?.ref.machineId, "mac");
  assert.strictEqual((yield* Effect.flip(inspector.inspect(reader, { ref: linux.ref }))).status, 404);
})));

it.effect("correlates an actual read-only query with its publisher, incarnation and native ref", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
  const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
  if (!(request)) throw new Error("Expected truthy value");
  assert.deepStrictEqual(request.ref, presence.ref);
  assert.deepStrictEqual((yield* inspector.poll(publisher, identity)).requests, []);
  const observation = createInspection(presence, 123);
  assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, { ...identity, incarnation: "forged", requestId: request.requestId, observation }))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.complete(reader, { ...identity, requestId: request.requestId, observation }))).status, 409);
  for (const ref of [
    { ...presence.ref, nativeSessionId: "another" },
    { ...presence.ref, machineId: "linux" },
    { ...presence.ref, harness: "pi" },
    { ...presence.ref, nativeAgentId: "child" },
  ] satisfies ReadonlyArray<SessionRef>) {
    assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, {
      ...identity, requestId: request.requestId,
      observation: { ...observation, presence: { ...presence, ref } },
    }))).status, 409);
  }
  yield* inspector.complete(publisher, { ...identity, requestId: request.requestId, observation });
  const actual = yield* Fiber.join(query);
  assert.notStrictEqual(actual.presence.connection?.receivedAt, presence.connection?.receivedAt);
  assert.notStrictEqual(actual.queriedAt, 123);
  assert.deepStrictEqual(actual.presence.machine, presence.machine);
  assert.deepStrictEqual(actual.presence.agent, presence.agent);
  assert.deepStrictEqual(actual.presence.model, presence.model);
  assert.deepStrictEqual(actual.presence.workspace, presence.workspace);
  assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: request.requestId, observation }))).status, 409);
})));

it.effect("disposes timed-out and cancelled reads and frees bounded request capacity", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const timed = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  const timedRequest = (yield* inspector.poll(publisher, identity)).requests.at(0);
  if (!(timedRequest)) throw new Error("Expected truthy value");
  yield* TestClock.adjust(10_001);
  assert.strictEqual((yield* Fiber.join(timed)).status, 504);
  assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: timedRequest.requestId, observation: createInspection(presence, 0) }))).status, 409);
  const reads: Array<Fiber.Fiber<SessionInspection, HttpError>> = [];
  for (let index = 0; index < 32; index++) {
    const read = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
    reads.push(read);
  }
  assert.strictEqual((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 429);
  const cancelledRequest = (yield* inspector.poll(publisher, identity)).requests.at(0);
  if (!(cancelledRequest)) throw new Error("Expected truthy value");
  yield* Fiber.interruptAll(reads);
  assert.deepStrictEqual((yield* inspector.poll(publisher, identity)).requests, []);
  assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, { ...identity, requestId: cancelledRequest.requestId, observation: createInspection(presence, 0) }))).status, 409);
  const replacement = yield* Effect.forkChild(inspector.inspect(reader, { ref: presence.ref }), { startImmediately: true });
  assert.strictEqual((yield* inspector.poll(publisher, identity)).requests.length, 1);
  yield* Fiber.interrupt(replacement);
})));

it.effect("uses receiver freshness and fences reconnects, duplicate publishers and native session switches", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const listed = (yield* inspector.list(reader, { limit: 10 })).sessions.at(0);
  assert.strictEqual(listed?.connection?.state, "live");
  assert.notStrictEqual(listed?.connection?.liveUntil, presence.connection?.liveUntil);
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, { ...identity, incarnation: "second", machineId: "mac", sessions: [presence] }))).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, { ...identity, connectorId: "other", machineId: "mac", sessions: [presence] }))).status, 409);
  const switched = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [{ ...presence, ref: { ...presence.ref, nativeSessionId: "switched-session" } }] });
  assert.strictEqual((yield* Fiber.join(switched)).status, 409);
  assert.strictEqual((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 404);
  yield* TestClock.adjust(30_001);
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.at(0)?.connection?.state, "stale");
  yield* inspector.announce(publisher, { ...identity, incarnation: "second", machineId: "mac", sessions: [presence] });
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] }))).status, 409);
  yield* inspector.disconnect(publisher, { ...identity, incarnation: "second" });
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.at(0)?.connection?.state, "stale");
  assert.strictEqual((yield* Effect.flip(inspector.inspect(reader, { ref: presence.ref }))).status, 409);
  yield* TestClock.adjust(120_001);
  assert.deepStrictEqual((yield* inspector.list(reader, { limit: 10 })).sessions, []);
})));

it.effect("marks a process shared across native sessions without charging either session owned totals", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  const process = { pid: 123, startIdentity: "birth", ownership: "owned" };
  const first: SessionPresence = { ...presence, processes: [{ ...process, ownership: "owned" }] };
  const second: SessionPresence = { ...first, ref: { ...first.ref, nativeSessionId: "second-session" } };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first, second] });
  if (!((yield* inspector.list(reader, { limit: 10 })).sessions.every((session) => session.processes?.at(0)?.ownership === "shared"))) throw new Error("Expected truthy value");
  for (const native of [first, second]) {
    const query = yield* Effect.forkChild(inspector.inspect(reader, { ref: native.ref }), { startImmediately: true });
    const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
    if (!(request)) throw new Error("Expected truthy value");
    const empty = createInspection(native, 0);
    yield* inspector.complete(publisher, {
      ...identity, requestId: request.requestId,
      observation: { ...empty, resources: {
        processes: [{ process: { ...process, ownership: "owned" }, cpuPercent: metric("percent-one-core", 100, provenance), cpuTimeMs: metric("milliseconds", 50, provenance), rssBytes: metric("bytes", 1024, provenance) }],
        ownedProcessCount: metric("count", 1, provenance), ownedCpuPercent: metric("percent-one-core", 100, provenance), ownedRssBytes: metric("bytes", 1024, provenance),
      } },
    });
    const actual = yield* Fiber.join(query);
    assert.strictEqual(actual.resources.processes.at(0)?.process.ownership, "shared");
    assert.strictEqual(actual.resources.processes.at(0)?.rssBytes.value, 1024);
    assert.strictEqual(actual.resources.processes.at(0)?.rssBytes.provenance.shared, true);
    assert.strictEqual(actual.resources.ownedRssBytes.status, "unavailable");
    assert.strictEqual(actual.resources.ownedCpuPercent.status, "unavailable");
    assert.strictEqual(actual.resources.ownedProcessCount.status, "unavailable");
  }
  const nextBirth: SessionPresence = { ...second, processes: [{ ...process, startIdentity: "new-birth", ownership: "owned" }] };
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [first, nextBirth] });
  if (!((yield* inspector.list(reader, { limit: 10 })).sessions.every((session) => session.processes?.at(0)?.ownership === "owned"))) throw new Error("Expected truthy value");
})));

it.effect("paginates by actual response bytes using opaque owner-bound cursors", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  const large = Array.from({ length: 5 }, (_, index): SessionPresence => ({
    ...presence, ref: { ...presence.ref, nativeSessionId: `session-${index}` },
    activity: { state: "idle", goals: Array.from({ length: 100 }, (_, goal): { id: string; state: "pending" } => ({ id: `${goal}`.padEnd(256, "x"), state: "pending" })) },
  }));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: large });
  const first = yield* inspector.list(reader, { limit: 100 });
  if (!(new TextEncoder().encode(JSON.stringify(first)).byteLength <= 48 * 1024)) throw new Error("Expected truthy value");
  if (!(first.nextCursor)) throw new Error("Expected truthy value");
  assert.strictEqual((yield* Effect.flip(inspector.list(outsider, { limit: 100, cursor: first.nextCursor }))).status, 400);
  assert.strictEqual((yield* Effect.flip(inspector.list(reader, { limit: 100, cursor: first.nextCursor, machineId: "other" }))).status, 400);
  const found = new Set(first.sessions.map((session) => session.ref.nativeSessionId));
  let cursor: string | null = first.nextCursor;
  while (cursor !== null) {
    const page: SessionListOutput = yield* inspector.list(reader, { limit: 100, cursor });
    if (!(new TextEncoder().encode(JSON.stringify(page)).byteLength <= 48 * 1024)) throw new Error("Expected truthy value");
    for (const session of page.sessions) {
      assert.strictEqual(found.has(session.ref.nativeSessionId), false);
      found.add(session.ref.nativeSessionId);
    }
    cursor = page.nextCursor;
  }
  assert.strictEqual(found.size, large.length);
})));

it.effect("closes pending readers with the registry scope and rejects late results", () => Effect.gen(function*() {
  const scope = yield* Scope.make();
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer)).pipe(Scope.provide(scope));
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  const read = yield* Effect.forkChild(Effect.flip(inspector.inspect(reader, { ref: presence.ref })), { startImmediately: true });
  const request = (yield* inspector.poll(publisher, identity)).requests.at(0);
  if (!(request)) throw new Error("Expected truthy value");
  yield* Scope.close(scope, Exit.void);
  assert.strictEqual((yield* Fiber.join(read)).status, 503);
  assert.strictEqual((yield* Effect.flip(inspector.complete(publisher, {
    ...identity, requestId: request.requestId, observation: createInspection(presence, 0),
  }))).status, 401);
}));

it.effect("bounds connectors and reclaims expired presence rather than retaining registrations", () => Effect.scoped(Effect.gen(function*() {
  const inspector = yield* Inspector.make(config).pipe(Effect.provide(NodeServices.layer));
  for (let index = 0; index < 256; index++) {
    yield* inspector.announce(publisher, { ...identity, connectorId: `connector-${index}`, machineId: "mac", sessions: [] });
  }
  assert.strictEqual((yield* Effect.flip(inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [] }))).status, 429);
  yield* TestClock.adjust(120_001);
  yield* inspector.announce(publisher, { ...identity, machineId: "mac", sessions: [presence] });
  assert.strictEqual((yield* inspector.list(reader, { limit: 10 })).sessions.length, 1);
})));
