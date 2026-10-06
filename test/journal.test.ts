import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Context, Effect, Layer } from "effect";
import { Journal } from "../src/journal.ts";
import { Platform } from "../src/platform.ts";
import { canonicalJson } from "../src/http.ts";
import { CommandError } from "../src/contracts.ts";

test("complete host lines across branches, metadata-only consent spool, durable delivery intent and coverage", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const fileSystem = platform.fs;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-journal-" });
    const file = platform.path.join(directory, "host.jsonl");
    const first = { id: "a", type: "message", parentId: null, message: { role: "user", content: "TOKEN=supersecret" } };
    const branch = { id: "b", type: "message", parentId: "a", message: { role: "assistant", content: [{ type: "thinking", thinking: "private" }] } };
    yield* fileSystem.writeFileString(file, `${canonicalJson(first)}\n${canonicalJson(branch)}`);
    const journal = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.length, 0);
    yield* journal.consent(["owner", "participant"]);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "a").length, 1);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 0);
    yield* fileSystem.writeFileString(file, `${canonicalJson(first)}\n${canonicalJson(branch)}\n`);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 1);
    const persisted = yield* fileSystem.readFileString(journal.path);
    assert.equal(persisted.includes("supersecret"), false);
    assert.equal(persisted.includes("private"), false);
    assert.equal((yield* fileSystem.stat(journal.path)).mode & 0o777, 0o600);
    yield* journal.handle("peer:message");
    assert.deepEqual(yield* Effect.all([journal.releaseIntent("task:1"), journal.releaseIntent("task:1")], { concurrency: "unbounded" }), [true, false]);
    const resumed = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
    assert.equal((yield* resumed.handled("peer:message")), true);
    assert.equal(yield* resumed.releaseIntent("task:1"), false);
    assert.equal((yield* Effect.exit(resumed.upload(() => Effect.fail(new CommandError({ message: "offline" })))))._tag, "Failure");
    yield* fileSystem.writeFileString(file, `${canonicalJson(branch)}\n`);
    yield* resumed.scan(file);
    assert.ok(resumed.state.gaps.includes("rewrite"));
    yield* resumed.scan(undefined);
    assert.ok(resumed.state.spool.some((entry) => entry.gaps.includes("ephemeral")));
    yield* resumed.upload(() => Effect.void);
    assert.equal((Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal)).state.spool.length, 0);
    yield* resumed.erase();
    const optedOut = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
    assert.equal(optedOut.state.consent, false);
    assert.equal(yield* optedOut.releaseIntent("task:1"), false);
    assert.deepEqual(optedOut.state.seen, {});
})).pipe(Effect.provide(Platform.layer))));

test("concurrent inbox delivery persists exactly once before acknowledgement and rejects conflicting payloads", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const fileSystem = platform.fs;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-inbox-" });
    const journal = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
    assert.deepEqual(yield* Effect.all([journal.receive("message", { text: "private peer message" }), journal.receive("message", { text: "private peer message" })], { concurrency: "unbounded" }), [true, false]);
    const resumed = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
    assert.equal((yield* resumed.handled("peer:message")), true);
    assert.deepEqual(resumed.state.inbox.message?.payload, { text: "private peer message" });
    assert.equal((yield* Effect.exit(resumed.receive("message", { text: "different" })) )._tag, "Failure");
    yield* resumed.erase();
    assert.equal(yield* resumed.receive("message", { text: "private peer message" }), false);
})).pipe(Effect.provide(Platform.layer))));

test("failed durable writes withhold inbox acknowledgement and poison later mutations", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-fail-closed-" });
  const journal = Context.get(yield* Layer.build(Journal.layer(directory, "session")), Journal);
  yield* platform.fs.remove(directory, { recursive: true });
  const results = yield* Effect.all([
    Effect.exit(journal.receive("message", { text: "must not acknowledge" })),
    Effect.exit(journal.releaseIntent("task:1")),
  ], { concurrency: "unbounded" });
  assert.ok(results.every((result) => result._tag === "Failure"));
  yield* platform.fs.makeDirectory(directory, { mode: 0o700 });
  assert.equal((yield* Effect.exit(journal.save()))._tag, "Failure");
  assert.equal(yield* platform.fs.exists(journal.path), false);
})).pipe(Effect.provide(Platform.layer))));
