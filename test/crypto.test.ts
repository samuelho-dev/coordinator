import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { Hex } from "effect/encoding";
import { Platform } from "../src/platform.ts";
import { RunnerTask } from "../src/generated/database/types.ts";
import { Principal } from "../src/contracts.ts";

it.live("RFC 4231 HMAC uses exact key views and stored SHA-256 credentials stay unchanged", () => Effect.gen(function*() {
  const platform = yield* Platform;
  // RFC 4231 section 4.2: twenty 0x0b bytes; surrounding bytes must not enter the key.
  const backing = new Uint8Array(22).fill(0xff);
  const key = backing.subarray(1, 21).fill(0x0b);
  const mac = yield* platform.hmac(key, "Hi There");
  assert.equal(Hex.encode(mac), "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
  assert.equal(backing[0], 0xff);
  assert.equal(backing[21], 0xff);

  const hash = RunnerTask.fields.fingerprint.make("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  assert.equal(yield* platform.digest("abc"), hash);
  assert.equal(yield* platform.checkToken("abc", hash), true);
  assert.equal(yield* platform.checkToken("counterfeit", hash), false);
  assert.equal(yield* platform.checkToken("", hash), false);
  assert.equal(yield* platform.checkToken("abc", hash), true);
  for (const malformed of ["", hash.slice(1), `${hash}0`, `${hash}\n`, hash.toUpperCase(), "g".repeat(64)]) {
    assert.throws(() => Principal.make({ id: Principal.fields.id.make("owner"), owner: Principal.fields.owner.make("owner"), tokenHash: RunnerTask.fields.fingerprint.make(malformed), roles: [], repos: [] }));
    assert.equal(yield* platform.checkToken("abc", RunnerTask.fields.fingerprint.make(malformed)), false);
  }
}).pipe(Effect.provide(Platform.layer)));
