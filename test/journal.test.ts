import { strict as assert } from "node:assert";
import { it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import { Journal } from "../src/journal.ts";
import { Platform } from "../src/platform.ts";
import { CommandError, JournalHandledKey, JournalReleaseKey } from "../src/contracts.ts";
import { SessionId, PeerId, Peer } from "../src/generated/database/types.ts";

it.live("complete host lines across branches, metadata-only consent spool, durable delivery intent and coverage", () => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const fileSystem = platform.fs;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-journal-" });
    const file = platform.path.join(directory, "host.jsonl");
    const first = { id: "a", type: "message", parentId: null, message: { role: "user", content: "TOKEN=supersecret" } };
    const branch = { id: "b", type: "message", parentId: "a", message: { role: "assistant", content: [{ type: "thinking", thinking: "private" }] } };
    yield* fileSystem.writeFileString(file, `${Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(first)}\n${Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(branch)}`);
    const journal = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.length, 0);
    yield* journal.consent([Peer.fields.sender.make("owner"), Peer.fields.sender.make("participant")]);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "a").length, 1);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 0);
    yield* fileSystem.writeFileString(file, `${Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(first)}\n${Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(branch)}\n`);
    yield* journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 1);
    const persisted = yield* fileSystem.readFileString(journal.path);
    assert.equal(persisted.includes("supersecret"), false);
    assert.equal(persisted.includes("private"), false);
    assert.equal((yield* fileSystem.stat(journal.path)).mode & 0o777, 0o600);
    yield* journal.handle(JournalHandledKey.make("peer:message"));
    assert.deepEqual(yield* Effect.all([journal.releaseIntent(JournalReleaseKey.make("task:1")), journal.releaseIntent(JournalReleaseKey.make("task:1"))], { concurrency: "unbounded" }), [true, false]);
    const resumed = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
    assert.equal((yield* resumed.handled(JournalHandledKey.make("peer:message"))), true);
    assert.equal(yield* resumed.releaseIntent(JournalReleaseKey.make("task:1")), false);
    assert.equal((yield* Effect.exit(resumed.upload(() => Effect.fail(new CommandError({ message: "offline" })))))._tag, "Failure");
    yield* fileSystem.writeFileString(file, `${Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(branch)}\n`);
    yield* resumed.scan(file);
    assert.ok(resumed.state.gaps.includes("rewrite"));
    yield* resumed.scan(undefined);
    assert.ok(resumed.state.spool.some((entry) => entry.gaps.includes("ephemeral")));
    yield* resumed.upload(() => Effect.void);
    assert.equal((Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal)).state.spool.length, 0);
    yield* resumed.erase();
    const optedOut = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
    assert.equal(optedOut.state.consent, false);
    assert.equal(yield* optedOut.releaseIntent(JournalReleaseKey.make("task:1")), false);
    assert.deepEqual(optedOut.state.seen, {});
})).pipe(Effect.provide(Platform.layer)));

it.live("capture marks missing parents but resolves complete out-of-order branch metadata without content", () => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-branch-coverage-" });
  const file = platform.path.join(directory, "host.jsonl");
  const child = { id: "child", parentId: "parent", type: "message", message: { role: "assistant", content: "private branch content" } };
  const parent = { id: "parent", parentId: null, type: "message", message: { role: "user", content: "private root content" } };
  const line = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
  yield* platform.fs.writeFileString(file, `${line(child)}\n`);
  const missing = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("missing-parent"))), Journal);
  yield* missing.consent([Peer.fields.sender.make("participant")]);
  yield* missing.scan(file);
  assert.ok(missing.state.spool.find(entry => entry.entryId === "child")?.gaps.includes("unlinked"), "A syntactically valid parent ID is not proof that its complete entry exists");
  yield* platform.fs.writeFileString(file, `${line(child)}\n${line(parent)}\n`);
  const complete = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("complete-branch"))), Journal);
  yield* complete.consent([Peer.fields.sender.make("participant")]);
  yield* complete.scan(file);
  const captured = complete.state.spool.find(entry => entry.entryId === "child");
  assert.deepEqual(captured?.payload, { id: "child", parentId: "parent", type: "message", role: "assistant", contentOmitted: true });
  assert.equal(captured?.gaps.includes("unlinked"), false, "Parent resolution covers all complete lines, not just earlier lines");
  assert.equal((yield* platform.fs.readFileString(complete.path)).includes("private"), false);
})).pipe(Effect.provide(Platform.layer)));

it.live("concurrent inbox delivery persists exactly once before acknowledgement and rejects conflicting payloads", () => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const fileSystem = platform.fs;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "coordinator-inbox-" });
    const journal = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
    assert.deepEqual(yield* Effect.all([journal.receive(PeerId.make("message"), { text: "private peer message" }), journal.receive(PeerId.make("message"), { text: "private peer message" })], { concurrency: "unbounded" }), [true, false]);
    const resumed = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
    assert.equal((yield* resumed.handled(JournalHandledKey.make("peer:message"))), true);
    assert.deepEqual(resumed.state.inbox[PeerId.make("message")]?.payload, { text: "private peer message" });
    assert.equal((yield* Effect.exit(resumed.receive(PeerId.make("message"), { text: "different" })) )._tag, "Failure");
    yield* resumed.erase();
    assert.equal(yield* resumed.receive(PeerId.make("message"), { text: "private peer message" }), false);
})).pipe(Effect.provide(Platform.layer)));

it.live("failed durable writes withhold inbox acknowledgement and poison later mutations", () => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-fail-closed-" });
  const journal = Context.get(yield* Layer.build(Journal.layer(directory, SessionId.make("session"))), Journal);
  yield* platform.fs.remove(directory, { recursive: true });
  const results = yield* Effect.all([
    Effect.exit(journal.receive(PeerId.make("message"), { text: "must not acknowledge" })),
    Effect.exit(journal.releaseIntent(JournalReleaseKey.make("task:1"))),
  ], { concurrency: "unbounded" });
  assert.ok(results.every((result) => result._tag === "Failure"));
  yield* platform.fs.makeDirectory(directory, { mode: 0o700 });
  assert.equal((yield* Effect.exit(journal.save()))._tag, "Failure");
  assert.equal(yield* platform.fs.exists(journal.path), false);
})).pipe(Effect.provide(Platform.layer)));
