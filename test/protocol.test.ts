import test from "node:test";
import assert from "node:assert/strict";
import { canonicalRemote } from "../src/protocol.ts";

test("HTTPS and SCP Git remotes share the canonical routing key without authorizing ambiguous paths", () => {
  const key = "github.com/samuelho-dev/coordinator";
  assert.equal(canonicalRemote("https://github.com/samuelho-dev/coordinator.git"), key);
  assert.equal(canonicalRemote("git@github.com:samuelho-dev/coordinator.git"), key);
  assert.equal(canonicalRemote("ssh://git@github.com/samuelho-dev/coordinator.git"), key);
  assert.throws(() => canonicalRemote("file:///tmp/coordinator"));
  assert.throws(() => canonicalRemote("https://github.com/owner/repo/extra"));
  assert.throws(() => canonicalRemote("https://github.com/owner/repo?token=unsafe"));
});
