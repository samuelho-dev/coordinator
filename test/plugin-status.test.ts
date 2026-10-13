import assert from "node:assert/strict";
import test from "node:test";
import { Deferred, Effect, Exit, Scope, Sink, Stdio, Stream } from "effect";
import { makeProgramStatus } from "../src/plugin-status.ts";
import type { Context } from "../src/contracts.ts";

const query = "\x1b]7501;?\x1b\\";
type Handler = (data: string) => { consume?: boolean; data?: string } | undefined;
const fixture = (options: { replies?: string[]; terminal?: boolean; hasUI?: boolean; mode?: Context["mode"]; sub?: boolean; callback?: boolean; waitForProbe?: boolean } = {}) => Effect.gen(function*() {
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const queried = yield* Deferred.make<void>();
  const probeStopped = yield* Deferred.make<void>();
  const output: string[] = [], passed: string[] = [];
  const records = new Map<string, string>([["", "host-root"], ["another", "another-child"]]);
  const statuses = new Map<string, string>([["host", "host-status"]]);
  const statusUpdates: [string, string | undefined][] = [];
  let handler: Handler | undefined;
  let registrations = 0, unsubscriptions = 0;
  const input = (data: string) => {
    const result = handler?.(data);
    if (!result?.consume) passed.push(result?.data ?? data);
  };
  const ctx: Context = { cwd: "/fixture", mode: options.mode ?? "tui", hasUI: options.hasUI ?? true,
    ...(options.sub ? { agent: { kind: "sub" as const } } : {}),
    sessionManager: { getSessionId: () => "host-session" },
    ui: { notify: () => {}, confirm: async () => false,
      setStatus: (key, text) => {
        statusUpdates.push([key, text]);
        if (text === undefined) statuses.delete(key);
        else statuses.set(key, text);
      },
      ...(options.callback === false ? {} : { onTerminalInput: (listener: Handler) => {
        registrations++;
        handler = listener;
        return () => {
          unsubscriptions++;
          handler = undefined;
          Deferred.doneUnsafe(probeStopped, Effect.void);
        };
      } }),
    },
  };
  const status = yield* makeProgramStatus(ctx).pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(Stdio.layerTest({
    stdoutIsTerminal: Effect.succeed(options.terminal ?? true),
    // stdin must never be read; only the host's supported callback owns input.
    stdin: Stream.die("Program status must use the native terminal callback"),
    stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.gen(function*() {
      assert.equal(typeof chunk, "string");
      const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
      output.push(text);
      if (text === query) {
        for (const response of options.replies ?? []) input(response);
        yield* Deferred.succeed(queried, undefined);
      } else {
        const body = text.slice("\x1b]7501;".length, -2);
        const pairs = new Map(body.split(":").map((pair) => { const i = pair.indexOf("="); return [pair.slice(0, i), pair.slice(i + 1)] as const; }));
        const id = pairs.get("id") ?? "";
        if (pairs.get("state") === "clear") {
          for (const key of records.keys()) if (id === "" || key === id || key.startsWith(`${id}/`)) records.delete(key);
        } else records.set(id, body);
      }
    })),
  })));
  if (options.waitForProbe !== false && (options.terminal ?? true) && ctx.hasUI && ctx.mode === "tui" && !options.sub && options.callback !== false) yield* Deferred.await(probeStopped);
  return { status, statuses, statusUpdates, output, passed, records, queried, probeStopped, input, close: Scope.close(scope, Exit.void), counts: () => ({ registrations, unsubscriptions }) };
});

const run = (body: Effect.Effect<void, unknown, Scope.Scope>) => Effect.runPromise(Effect.scoped(body));

test("PSP is gated by interactive main TTY, native callback, and authoritative reply", () => run(Effect.gen(function*() {
  for (const options of [{ terminal: false }, { hasUI: false }, { mode: "rpc" as const }, { mode: "print" as const }, { mode: "json" as const }, { sub: true }, { callback: false }]) {
    const f = yield* fixture(options);
    yield* f.status.report("idle");
    yield* f.status.clear();
    yield* f.close;
    assert.deepEqual(f.output, []);
    assert.equal(f.counts().registrations, 0);
  }
  for (const replies of [[], ["\x1b]7501;state=idle\x07"], ["\x1b]7501;?" + "x".repeat(4096) + "\x07"]]) {
    const f = yield* fixture({ replies });
    yield* f.status.report("idle");
    yield* f.status.clear();
    yield* f.close;
    assert.deepEqual(f.output, [query]);
    assert.deepEqual(f.counts(), { registrations: 1, unsubscriptions: 1 });
  }
})));

test("complete BEL/ST replies accept future pairs and preserve unrelated input", () => run(Effect.gen(function*() {
  for (const end of ["\x07", "\x1b\\"]) {
    const unrelated = "key\x1b]10;rgb:ff/ff/ff\x07";
    const f = yield* fixture({ replies: [unrelated + "\x1b]7501;?:future=value" + end + "tail"] });
    yield* f.status.report("idle");
    assert.deepEqual(f.passed, [unrelated + "tail"]);
    assert.equal(f.records.get("coordinator"), "id=coordinator:app=coordinator:state=idle");
    yield* f.close;
    assert.deepEqual(f.counts(), { registrations: 1, unsubscriptions: 1 });
  }
})));

test("torn relevant replies are observed without withholding keyboard/terminal bytes", () => run(Effect.gen(function*() {
  for (const fragments of [["\x1b]75", "01;?", "\x07"], ["\x1b", "]7501;?:future=value", "\x1b", "\\"]]) {
    const f = yield* fixture({ replies: fragments });
    yield* f.status.report("working");
    assert.deepEqual(f.passed, fragments, "the host has no reinjection API, so torn bytes must not be withheld");
    assert.ok(f.records.has("coordinator"));
    yield* f.close;
  }
  const unrelated = ["\x1b", "[A", "typing", "\x1b]10;?\x07"];
  const f = yield* fixture({ replies: unrelated });
  yield* f.status.report("working");
  assert.deepEqual(f.passed, unrelated);
  assert.deepEqual(f.output, [query]);
})));

test("owned transitions replace only coordinator and use bounded safe constants", () => run(Effect.gen(function*() {
  const f = yield* fixture({ replies: [query] });
  yield* f.status.report("idle");
  yield* f.status.report("idle");
  assert.equal(f.output.length, 2, "unchanged states have no heartbeat");
  yield* f.status.report("working", "permission");
  assert.equal(f.records.get("coordinator"), `id=coordinator:app=coordinator:state=blocked:kind=permission:msg=${Buffer.from("Waiting for permission").toString("base64")}`);
  yield* f.status.report("working", "question");
  assert.equal(f.records.get("coordinator"), `id=coordinator:app=coordinator:state=blocked:kind=question:msg=${Buffer.from("Waiting for an answer").toString("base64")}`);
  yield* f.status.report("working");
  assert.equal(f.records.get("coordinator"), "id=coordinator:app=coordinator:state=working", "replacement drops obsolete blocked fields");
  for (const sequence of f.output) assert.ok(Buffer.byteLength(sequence) < 4096);
  yield* f.status.report(undefined, "permission");
  assert.deepEqual([...f.records], [["", "host-root"], ["another", "another-child"]]);
  const count = f.output.length;
  yield* f.status.clear();
  assert.equal(f.output.length, count, "clear emits nothing if this helper has no record");
  yield* f.close;
})));

test("scope cancels detection and clears live ownership but preserves settled results", () => run(Effect.gen(function*() {
  const pending = yield* fixture({ waitForProbe: false });
  yield* Deferred.await(pending.queried);
  yield* pending.close;
  yield* pending.status.report("working");
  assert.deepEqual(pending.output, [query]);
  assert.deepEqual(pending.counts(), { registrations: 1, unsubscriptions: 1 });
  for (const state of ["idle", "working", "done", "error"] as const) {
    const f = yield* fixture({ replies: [query] });
    yield* f.status.report(state);
    yield* f.close;
    assert.equal(f.records.has("coordinator"), state === "done" || state === "error");
    assert.equal(f.records.get(""), "host-root");
    assert.equal(f.records.get("another"), "another-child");
    const count = f.output.length;
    yield* f.status.report("idle");
    yield* f.status.clear();
    assert.equal(f.output.length, count, "retired helper cannot resurrect or erase a record");
  }
  const replacement = yield* fixture({ replies: [query] });
  yield* replacement.status.report("done");
  yield* replacement.status.clear();
  yield* replacement.close;
  assert.equal(replacement.records.has("coordinator"), false, "deliberate replacement can clear a settled record");
})));

test("network updates deduplicate and compose without replacing task ownership", () => run(Effect.gen(function*() {
  const f = yield* fixture({ replies: [query] });
  yield* f.status.report("working", "permission");
  yield* f.status.network("retrying");
  const record = f.records.get("coordinator");
  assert.ok(record?.includes("state=blocked:kind=permission:"));
  assert.ok(record?.includes(Buffer.from("Waiting for permission; Coordinator reconnecting").toString("base64")));
  assert.ok(f.statuses.has("coordinator.network"));
  assert.equal(f.statuses.has("coordinator.paused"), false);
  const count = f.output.length, updates = f.statusUpdates.length;
  yield* f.status.network("retrying");
  assert.equal(f.output.length, count);
  assert.equal(f.statusUpdates.length, updates);
  yield* f.status.network("paused");
  assert.equal(f.statuses.has("coordinator.network"), false);
  assert.ok(f.statuses.has("coordinator.paused"));
  assert.ok(f.records.get("coordinator")?.includes("state=blocked:kind=permission:"));
  yield* f.status.report("done");
  assert.ok(f.records.get("coordinator")?.includes("state=done:"));
  yield* f.status.network(undefined);
  assert.equal(f.records.get("coordinator"), "id=coordinator:app=coordinator:state=done");
  assert.deepEqual([...f.statuses], [["host", "host-status"]]);
  yield* f.status.network("retrying");
  yield* f.status.report(undefined);
  assert.ok(f.records.has("coordinator"), "task clear preserves live network ownership");
  yield* f.status.clear();
  assert.deepEqual([...f.records], [["", "host-root"], ["another", "another-child"]]);
  assert.deepEqual([...f.statuses], [["host", "host-status"]]);
  yield* f.close;
  assert.deepEqual(f.counts(), { registrations: 1, unsubscriptions: 1 });
})));

test("pending detection never blocks state updates and retirement removes transient ownership", () => run(Effect.gen(function*() {
  const pending = yield* fixture({ waitForProbe: false });
  yield* Deferred.await(pending.queried);
  yield* pending.status.report("working");
  yield* pending.status.network("retrying");
  yield* pending.status.clear();
  yield* pending.close;
  assert.deepEqual(pending.output, [query]);
  assert.deepEqual([...pending.statuses], [["host", "host-status"]]);
  for (const state of ["working", "done"] as const) {
    const f = yield* fixture({ replies: [query] });
    yield* f.status.report(state);
    yield* f.status.network("paused");
    yield* f.close;
    assert.deepEqual([...f.statuses], [["host", "host-status"]]);
    assert.equal(f.records.has("coordinator"), state === "done");
    const count = f.output.length, updates = f.statusUpdates.length;
    yield* f.status.network("retrying");
    yield* f.status.report("working");
    yield* f.status.clear();
    assert.equal(f.output.length, count);
    assert.equal(f.statusUpdates.length, updates);
    assert.equal(f.records.get(""), "host-root");
    assert.equal(f.records.get("another"), "another-child");
    assert.deepEqual(f.counts(), { registrations: 1, unsubscriptions: 1 });
  }
})));

test("a delayed probe flushes only the latest composed state", () => run(Effect.gen(function*() {
  const f = yield* fixture({ waitForProbe: false });
  yield* Deferred.await(f.queried);
  yield* f.status.report("working", "permission");
  yield* f.status.network("retrying");
  yield* f.status.network("paused");
  assert.deepEqual(f.output, [query]);
  f.input(query);
  yield* Deferred.await(f.probeStopped);
  assert.equal(f.output.length, 2, "detection replays the latest state rather than stale transitions");
  assert.ok(f.records.get("coordinator")?.includes("state=blocked:kind=permission:"));
  assert.ok(f.statuses.has("coordinator.paused"));
  assert.equal(f.statuses.has("coordinator.network"), false);
  yield* f.close;
  assert.deepEqual([...f.statuses], [["host", "host-status"]]);
  assert.deepEqual([...f.records], [["", "host-root"], ["another", "another-child"]]);
})));
