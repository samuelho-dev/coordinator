import { it } from "@effect/vitest";
import assert from "node:assert/strict";
import { canonicalRemote } from "../src/platform.ts";
import { BrokerRequest, Commands, RunnerConfig, RunnerRequest } from "../src/contracts.ts";
import { Schema } from "effect";

it("HTTPS and SCP Git remotes share the canonical routing key without authorizing ambiguous paths", () => {
  const key = "github.com/samuelho-dev/coordinator";
  assert.equal(canonicalRemote("https://github.com/samuelho-dev/coordinator.git"), key);
  assert.equal(canonicalRemote("git@github.com:samuelho-dev/coordinator.git"), key);
  assert.equal(canonicalRemote("ssh://git@github.com/samuelho-dev/coordinator.git"), key);
  assert.throws(() => canonicalRemote("file:///tmp/coordinator"));
  assert.throws(() => canonicalRemote("https://github.com/owner/repo/extra"));
  assert.throws(() => canonicalRemote("https://github.com/owner/repo?token=unsafe"));
});

it("catalog command schemas reject malformed roots and excess arguments at the boundary", () => {
  for (const root of [null, [], "archive", 1]) {
    assert.throws(() => Schema.decodeUnknownSync(Commands.listArchive.input, { onExcessProperty: "error" })(root));
  }
  assert.throws(() => Schema.decodeUnknownSync(Commands.listArchive.input, { onExcessProperty: "error" })({ sessionId: "owned-session", principal: "forged" }));
  assert.throws(() => Schema.decodeUnknownSync(Commands.appendArchive.input, { onExcessProperty: "error" })({
    sessionId: "owned-session", entryId: "unsafe", digest: "a".repeat(64),
    payload: { id: "unsafe", parentId: null, type: "message", contentOmitted: true, text: "private-content" }, gaps: [],
  }));
  assert.throws(() => Schema.decodeUnknownSync(Commands.health.input, { onExcessProperty: "error" })(undefined));
});

it("request schemas route only the catalog's target operations", () => {
  const archive = { method: Commands.listArchive.name, params: { sessionId: "owned-session" } };
  assert.throws(() => Schema.decodeUnknownSync(RunnerRequest, { onExcessProperty: "error" })(archive));
  const journal = { method: Commands.journalTask.name, params: { taskId: "owned-task" } };
  assert.throws(() => Schema.decodeUnknownSync(BrokerRequest, { onExcessProperty: "error" })(journal));
  assert.throws(() => Schema.decodeUnknownSync(BrokerRequest, { onExcessProperty: "error" })({ method: "program.smoke:backup", params: {} }));
  assert.throws(() => Schema.decodeUnknownSync(RunnerRequest, { onExcessProperty: "error" })({ method: "agent.abort", params: { id: "one", type: "abort" } }));
});

it("runner configuration and heartbeat share the supported capacity and reject obsolete journal payloads", () => {
  const config = { id: "capacity-proof", brokerUrl: "http://127.0.0.1:1", tokenFile: "/private/token", host: "127.0.0.1", port: 0,
    controlTokenHash: "a".repeat(64), stateDirectory: "/private/state", slots: 64, repositories: {}, launchers: { omp: "/usr/local/bin/omp" },
    modelDirectory: "/private/model", childDirectory: "/work/children", childUidBase: 1000000 };
  const heartbeat = { runnerId: config.id, slots: 64, load: 0 };
  const decodeConfig = Schema.decodeUnknownSync(RunnerConfig, { onExcessProperty: "error" });
  const decodeHeartbeat = Schema.decodeUnknownSync(Commands.heartbeatRunner.input, { onExcessProperty: "error" });
  assert.equal(decodeConfig(config).slots, 64);
  assert.equal(decodeHeartbeat(heartbeat).slots, 64);
  for (const slots of [65, 128]) {
    assert.throws(() => decodeConfig({ ...config, slots }));
    assert.throws(() => decodeHeartbeat({ ...heartbeat, slots }));
  }
  assert.throws(() => decodeHeartbeat({ ...heartbeat, journal: [] }));
});
