import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import type { Transport } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { Cause, Effect, Schema, Scope } from "effect";
import { connectNativeTransport, type NativeClient } from "../packages/native-control/src/client.js";
import { NativeControlError, NativeEventSequence, NativeGeneration, NativeSubmissionId, NativeTools } from "../packages/native-control/src/contracts.js";

// Reuse the real SDK server/resource fixture, not an echo transport or copied schemas.
const source = fileURLToPath(new URL("native-client-proof.test.ts", import.meta.url));
const cwd = fileURLToPath(new URL("..", import.meta.url));

test("attached SDK transport retains private state/proof and distinguishes logical close from physical exit", async () => {
  const stdio = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", source], cwd, env: { NATIVE_CLIENT_PROOF_SERVER: "1" }, stderr: "ignore", maxBufferSize: 65536 });
  let resolvePhysical!: () => void;
  let physicallyClosed = false;
  const observedProcessClosed = new Promise<void>(resolve => { resolvePhysical = resolve; });
  // The attached transport's logical connection and the actual SDK child owner
  // deliberately have independent lifetimes, just as a remote process does.
  const transport: Transport = {
    async start() {
      stdio.onmessage = message => {
        if ("result" in message && corruptNextPrompt !== undefined) {
          const decoded = Schema.decodeUnknownOption(Schema.toCodecJson(NativeTools["native.prompt"].structuredOutput))(message.result.structuredContent);
          if (decoded._tag === "Some" && "result" in decoded.value && decoded.value.result.disposition === "settled") {
            const original = decoded.value.result;
            const corrupted = corruptNextPrompt === "admission"
              ? { ...original, admission: { ...original.admission, binding: { ...original.admission.binding, generation: NativeGeneration.make(original.admission.binding.generation + 1) } } }
              : { ...original, final: { ...original.final, binding: { ...original.final.binding, generation: NativeGeneration.make(original.final.binding.generation + 1) } } };
            corruptNextPrompt = undefined;
            transport.onmessage?.({ ...message, result: { ...message.result, structuredContent: Schema.encodeSync(Schema.toCodecJson(NativeTools["native.prompt"].structuredOutput))({ result: corrupted }) } });
            return;
          }
        }
        transport.onmessage?.(message);
      };
      stdio.onerror = error => transport.onerror?.(error);
      stdio.onclose = () => { physicallyClosed = true; resolvePhysical(); transport.onclose?.(); };
      await stdio.start();
    },
    send: message => stdio.send(message),
    async close() { transport.onclose?.(); },
  };
  let ownedCloseCalls = 0;
  let observedPhysical = false;
  let physicalObservation: Promise<void> | undefined;
  let corruptNextPrompt: "admission" | "final" | undefined;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const client: NativeClient<null> = yield* connectNativeTransport({ host: "pi", connectTimeoutMs: 10000 }, transport, {
      pid: null,
      observedProcessClosed,
      async close() { ownedCloseCalls++; await stdio.close(); await observedProcessClosed; },
    });
    assert.equal(client.pid, null);
    physicalObservation = Effect.runPromise(client.observedProcessClosed).then(() => { observedPhysical = true; });
    void physicalObservation.catch(() => undefined);
    for (const timeoutMs of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      const invalidCall = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs }).pipe(Effect.result);
      assert.equal(invalidCall._tag, "Failure");
      if (invalidCall._tag === "Failure") assert.equal(invalidCall.failure.code, "invalid_input");
      const invalidPrompt = yield* client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make("invalid-budget"), message: "complete" }, {
        after: NativeEventSequence.make(0), timeoutMs, onAdmission() { assert.fail("Invalid budget must not admit native work"); },
      }).pipe(Effect.result);
      assert.equal(invalidPrompt._tag, "Failure");
      if (invalidPrompt._tag === "Failure") assert.equal(invalidPrompt.failure.code, "invalid_input");
    }
    const state = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 3000 });
    assert.equal(state.systemPrompt, "private-system-prompt-" + "λ".repeat(40000));
    assert.equal(state.dumpTools?.at(0)?.name, "proof-tool");
    let admissions = 0;
    const missing = yield* client.prompt({ binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("missing-admission"), message: "missing-admission" }, {
      after: Schema.decodeSync(NativeEventSequence)(0), timeoutMs: 3000, onAdmission() { admissions++; },
    }).pipe(Effect.result);
    assert.equal(missing._tag, "Failure");
    if (missing._tag === "Failure") assert.equal(missing.failure.code, "resource_gap");
    assert.equal(admissions, 0);
    const complete = yield* client.prompt({ binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("complete"), message: "complete" }, {
      after: Schema.decodeSync(NativeEventSequence)(0), timeoutMs: 3000, onAdmission() { admissions++; },
    });
    assert.equal(complete.disposition, "settled");
    assert.equal(admissions, 1);
    // A request deadline cancels the request, not the physical owner.
    let independentProgress = false;
    const cancelled = yield* client.prompt({ binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("missing-terminal"), message: "missing-terminal" }, {
      after: complete.admission.seq, timeoutMs: 1000,
      async onAdmission() {
        const concurrentState = await Effect.runPromise(client.call("native.get_state", { binding: client.binding }, { timeoutMs: 500 }));
        assert.equal(concurrentState.binding.generation, client.binding.generation);
        independentProgress = true;
      },
    }).pipe(Effect.result);
    assert.equal(cancelled._tag, "Failure");
    if (cancelled._tag === "Failure") assert.equal(cancelled.failure.code, "deadline_exceeded");
    assert.equal(independentProgress, true, "A real SDK state request completes while the independently admitted prompt remains unsettled");
    assert.equal(physicallyClosed, false);
    const failed = yield* client.call("native.bash", { binding: client.binding, command: "printf ordinary" }, { timeoutMs: 3000 }).pipe(Effect.result);
    assert.equal(failed._tag, "Failure");
    if (failed._tag === "Failure") assert.equal(failed.failure.code, "unsupported_operation");
    yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 3000 });
    const resumed = yield* client.prompt({ binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("complete-after-deadline"), message: "complete" }, {
      after: complete.admission.seq, timeoutMs: 3000, onAdmission() {},
    });
    assert.equal(resumed.disposition === "settled" && resumed.final.outcome, "completed", "A request deadline must not poison later lifecycle proof");
    assert.equal(ownedCloseCalls, 0, "Valid tool failure and deadline leave the owner usable");
    let after = NativeEventSequence.make(0);
    for (const ownerField of ["admission", "final"]) {
      // Corrupt an actual decoded SDK reply after the real server's proof checks, not its framing or transport.
      corruptNextPrompt = ownerField === "admission" ? "admission" : "final";
      const result = yield* client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make(`wrong-${ownerField}-owner`), message: "complete" }, {
        after, timeoutMs: 3000, onAdmission() {}, onLifecycle(event) { after = event.seq; },
      }).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.code, "binding_mismatch");
      assert.equal(corruptNextPrompt, undefined, "The owner fault must reach the client's decoded SDK response boundary");
    }
    const recovered = yield* client.prompt({ binding: client.binding, submissionId: NativeSubmissionId.make("after-owner-mismatch"), message: "complete" }, { after, timeoutMs: 3000, onAdmission() {} });
    assert.equal(recovered.disposition === "settled" && recovered.final.outcome, "completed");
    yield* Effect.promise(() => transport.close());
    const logical = yield* client.connectionClosed.pipe(Effect.result);
    assert.equal(logical._tag, "Failure");
    assert.equal(physicallyClosed, false, "Logical close must not resolve the independently observed process exit");
    yield* Effect.yieldNow;
    assert.equal(observedPhysical, false, "The public process-close observer must retain occupied physical ownership after logical close");
    assert.equal(ownedCloseCalls, 0);
  })));
  assert.equal(ownedCloseCalls, 1);
  assert.equal(physicallyClosed, true, "Scope release waits for the real SDK child close callback");
  await physicalObservation;
  assert.equal(observedPhysical, true, "The public observer resolves only after the real child close callback");
});

for (const uncertain of [false, true]) test(`terminal official SDK message validation fault closes ownership once and retains cleanup failures (${uncertain})`, async () => {
  // Install the corruption producer before the fixture's module initialization.
  const producer = `
    const { writeSync } = await import("node:fs");
    process.on("SIGUSR1", () => { writeSync(1, "null\\n"); });
    await import(${JSON.stringify(new URL("native-client-proof.test.ts", import.meta.url).href)});
  `;
  const stdio = new StdioClientTransport({
    command: process.execPath,
    args: ["--import", "tsx", "--input-type=module", "--eval", producer],
    cwd, env: { NATIVE_CLIENT_PROOF_SERVER: "1" }, stderr: "ignore", maxBufferSize: 65536,
  });
  let sentFaultSignal = false;
  let sdkTransportFaults = 0;
  let resolvePhysical!: () => void;
  let physicallyClosed = false;
  const observedProcessClosed = new Promise<void>(resolve => { resolvePhysical = resolve; });
  let logicalCloseCalls = 0;
  let ownedCloseCalls = 0;
  const logicalFailure = new NativeControlError({ code: "transport_closed", message: "Logical SDK close failed" });
  const physicalFailure = new NativeControlError({ code: "transport_closed", message: "Physical owner cleanup failed" });
  const transport: Transport = {
    async start() {
      stdio.onmessage = message => transport.onmessage?.(message);
      stdio.onerror = error => { sdkTransportFaults++; transport.onerror?.(error); };
      stdio.onclose = () => { physicallyClosed = true; resolvePhysical(); transport.onclose?.(); };
      await stdio.start();
    },
    send: message => stdio.send(message),
    async close() {
      logicalCloseCalls++;
      transport.onclose?.();
      if (uncertain) throw logicalFailure;
    },
  };
  const result = await Effect.runPromiseExit(Effect.scoped(Effect.gen(function*() {
    const client = yield* connectNativeTransport({ host: "pi", connectTimeoutMs: 10000 }, transport, {
      pid: null,
      observedProcessClosed,
      async close() {
        ownedCloseCalls++;
        await stdio.close();
        await observedProcessClosed;
        if (uncertain) throw physicalFailure;
      },
    });
    const pending = yield* client.prompt({
      binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("missing-terminal"), message: "missing-terminal",
    }, {
      after: Schema.decodeSync(NativeEventSequence)(0), timeoutMs: 3000,
      onAdmission() {
        const pid = stdio.pid;
        if (pid === null) assert.fail("The real SDK child must be running");
        process.kill(pid, "SIGUSR1");
        sentFaultSignal = true;
      },
    }).pipe(Effect.result);
    assert.equal(pending._tag, "Failure");
    assert.equal(sentFaultSignal, true, "Retained admission precedes the actual OS fault stimulus");
    assert.equal(sdkTransportFaults, 1, "The official SDK rejected the producer's actual OS stdout envelope");
    if (pending._tag === "Failure") assert.equal(pending.failure.code, "transport_closed", JSON.stringify({ sdkTransportFaults, logicalCloseCalls, ownedCloseCalls }));
    const later = yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 3000 }).pipe(Effect.result);
    assert.equal(later._tag, "Failure");
    if (later._tag === "Failure") assert.equal(later.failure.code, "transport_closed");
    yield* client.observedProcessClosed;
    assert.equal(physicallyClosed, true, "Physical exit is observed before the caller scope closes");
    assert.equal(logicalCloseCalls, 1);
    assert.equal(ownedCloseCalls, 1);
  })));
  assert.equal(logicalCloseCalls, 1, "Scope release reuses terminal SDK cleanup");
  assert.equal(ownedCloseCalls, 1);
  assert.equal(result._tag, uncertain ? "Failure" : "Success", result._tag === "Failure" ? Cause.pretty(result.cause) : undefined);
  if (result._tag === "Failure") {
    assert.ok(result.cause.reasons.some(reason => reason._tag === "Die" && reason.defect === logicalFailure));
    assert.ok(result.cause.reasons.some(reason => reason._tag === "Die" && reason.defect === physicalFailure), Cause.pretty(result.cause));
    assert.ok(result.cause.reasons.every(reason => reason._tag === "Die" && (reason.defect === logicalFailure || reason.defect === physicalFailure)), Cause.pretty(result.cause));
  }
});

for (const connectTimeoutMs of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) test(`invalid attached timeout ${connectTimeoutMs} still closes ownership and preserves uncertain physical-exit failure`, async () => {
  let closed = 0;
  const uncertain = new NativeControlError({ code: "transport_closed", message: "Physical owner exit is unproven" });
  const stdio = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", source], cwd, env: { NATIVE_CLIENT_PROOF_SERVER: "1" }, stderr: "ignore" });
  const observedProcessClosed = new Promise<void>(() => {});
  const result = await Effect.runPromiseExit(Effect.scoped(connectNativeTransport({ host: "pi", connectTimeoutMs }, stdio, {
    pid: null,
    observedProcessClosed,
    async close() { closed++; throw uncertain; },
  })));
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") {
    assert.ok(result.cause.reasons.some(reason => reason._tag === "Die" && reason.defect === uncertain), "Uncertain physical closure must remain in the complete failure cause");
    assert.ok(result.cause.reasons.some(reason => reason._tag === "Fail" && reason.error.code === "invalid_input"));
  }
  assert.equal(closed, 1);
  assert.equal(stdio.pid, null, "Invalid connection never starts a substitute local process");
});

test("genuine SDK observers close by draining retained lifecycle and immutable presentation callbacks", async () => {
  const stdio = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", source], cwd, env: { NATIVE_CLIENT_PROOF_SERVER: "1" }, stderr: "ignore", maxBufferSize: 131072 });
  let resolvePhysical!: () => void;
  const observedProcessClosed = new Promise<void>(resolve => { resolvePhysical = resolve; });
  let unsubscribes = 0;
  let failUnsubscribe = false;
  let holdNextRead = false;
  let heldReadId: string | number | undefined;
  let releaseRead!: () => void;
  let readArrived!: () => void;
  let heldRead: Promise<void> = Promise.resolve();
  let readResponse: Promise<void> = Promise.resolve();
  const transport: Transport = {
    async start() {
      stdio.onmessage = message => {
        if ("id" in message && message.id === heldReadId) {
          heldReadId = undefined;
          readArrived();
          void heldRead.then(() => transport.onmessage?.(message));
        } else transport.onmessage?.(message);
      };
      stdio.onerror = error => transport.onerror?.(error);
      stdio.onclose = () => { resolvePhysical(); transport.onclose?.(); };
      await stdio.start();
    },
    send(message) {
      if ("method" in message && message.method === "resources/unsubscribe") unsubscribes++;
      if (failUnsubscribe && "method" in message && message.method === "resources/unsubscribe") return Promise.reject(new Error("Owned subscription teardown failed"));
      if (holdNextRead && "method" in message && message.method === "resources/read" && "id" in message) {
        holdNextRead = false;
        heldReadId = message.id;
      }
      return stdio.send(message);
    },
    close: () => stdio.close(),
  };
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const client = yield* connectNativeTransport({ host: "pi", connectTimeoutMs: 10000 }, transport, {
      pid: null, observedProcessClosed, close: () => stdio.close(),
    });
    const scope = yield* Effect.scope;
    let releaseLifecycle!: () => void;
    let enteredLifecycle!: () => void;
    const lifecycleEntered = new Promise<void>(resolve => { enteredLifecycle = resolve; });
    const lifecycleGate = new Promise<void>(resolve => { releaseLifecycle = resolve; });
    let releasePresentation!: () => void;
    let enteredPresentation!: () => void;
    const presentationEntered = new Promise<void>(resolve => { enteredPresentation = resolve; });
    const presentationGate = new Promise<void>(resolve => { releasePresentation = resolve; });
    const events: number[] = [];
    const values: unknown[] = [];
    const lifecycle = yield* client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(0) }, async event => {
      events.push(event.seq);
      if (events.length === 1) { enteredLifecycle(); await lifecycleGate; }
    });
    const presentation = yield* client.watchPresentation({ binding: client.binding, after: 0 }, async value => {
      values.push(value.value);
      if (values.length === 1) { enteredPresentation(); await presentationGate; }
    });
    yield* client.call("native.prompt", { binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("observer-close"), message: "observer" }, { timeoutMs: 3000 });
    yield* Effect.promise(() => Promise.all([lifecycleEntered, presentationEntered]));
    const closingLifecycle = Effect.runPromise(lifecycle.close);
    const closingPresentation = Effect.runPromise(presentation.close);
    yield* Effect.yieldNow;
    assert.equal(unsubscribes, 0, "Neither observer may unsubscribe ahead of retained callbacks");
    releaseLifecycle();
    releasePresentation();
    yield* Effect.promise(() => Promise.all([closingLifecycle, closingPresentation]));
    assert.deepEqual(events, [1, 2, 3]);
    assert.deepEqual(values, [{ text: "first" }, { text: "second" }]);
    assert.equal(unsubscribes, 2);
    const callbackFailure = new NativeControlError({ code: "native_failure", message: "Original observer callback failure" });
    for (const kind of ["lifecycle", "presentation"] as const) {
      let release!: () => void;
      let entered!: () => void;
      const gate = new Promise<void>(resolve => { release = resolve; });
      const started = new Promise<void>(resolve => { entered = resolve; });
      const callback = async () => { entered(); await gate; throw callbackFailure; };
      // Acquisition replays actual retained records, and its cleanup must retain
      // the same callback error rather than returning success or a replacement.
      const acquisition = kind === "lifecycle"
        ? client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(0) }, callback)
        : client.watchPresentation({ binding: client.binding, after: 0 }, callback);
      const failed = Effect.runPromise(acquisition.pipe(Effect.provideService(Scope.Scope, scope), Effect.result));
      yield* Effect.promise(() => started);
      release();
      const result = yield* Effect.promise(() => failed);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure, callbackFailure);
    }
    const retainedAfterRead: number[] = [];
    const next = yield* client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(3) }, event => { retainedAfterRead.push(event.seq); });
    heldRead = new Promise<void>(resolve => { releaseRead = resolve; });
    readResponse = new Promise<void>(resolve => { readArrived = resolve; });
    holdNextRead = true;
    const inFlightRead = Effect.runPromise(next.drain);
    yield* Effect.promise(() => readResponse);
    yield* client.call("native.prompt", { binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("observer-read-close"), message: "observer" }, { timeoutMs: 3000 });
    const closingRead = Effect.runPromise(next.close);
    yield* Effect.yieldNow;
    releaseRead();
    yield* Effect.promise(() => Promise.all([inFlightRead, closingRead]));
    assert.deepEqual(retainedAfterRead, [4, 5, 6], "Close reads a fresh high-water after the older in-flight resource reply");
    const teardownWatch = yield* client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(6) }, () => {});
    failUnsubscribe = true;
    const teardown = yield* teardownWatch.close.pipe(Effect.result);
    assert.equal(teardown._tag, "Failure", "A genuine SDK unsubscribe send failure is not suppressed");
    failUnsubscribe = false;
    const independent = yield* client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(6) }, () => {});
    yield* independent.close;
    for (const kind of ["lifecycle", "presentation"] as const) {
      let rejectCallback!: () => void;
      let callbackEntered!: () => void;
      const gate = new Promise<void>(resolve => { rejectCallback = resolve; });
      const entered = new Promise<void>(resolve => { callbackEntered = resolve; });
      const callback = async () => { callbackEntered(); await gate; throw callbackFailure; };
      const watcher = kind === "lifecycle"
        ? yield* client.watchLifecycle({ binding: client.binding, after: Schema.decodeSync(NativeEventSequence)(6) }, callback)
        : yield* client.watchPresentation({ binding: client.binding, after: 6 }, callback);
      yield* client.call("native.prompt", { binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)(`observer-close-error-${kind}`), message: "observer" }, { timeoutMs: 3000 });
      yield* Effect.promise(() => entered);
      const closing = Effect.runPromise(watcher.close.pipe(Effect.result));
      yield* Effect.yieldNow;
      rejectCallback();
      const failed = yield* Effect.promise(() => closing);
      assert.equal(failed._tag, "Failure", "Close must not suppress an in-flight callback rejection");
      if (failed._tag === "Failure") assert.equal(failed.failure, callbackFailure);
    }
    for (const kind of ["lifecycle", "presentation"]) {
      const observer = kind === "lifecycle"
        ? yield* client.watchLifecycle({ binding: client.binding, after: NativeEventSequence.make(0) }, () => {})
        : yield* client.watchPresentation({ binding: client.binding, after: 0 }, () => {});
      heldRead = new Promise<void>(resolve => { releaseRead = resolve; });
      readResponse = new Promise<void>(resolve => { readArrived = resolve; });
      holdNextRead = true;
      const pending = Effect.runPromise(observer.drain.pipe(Effect.result));
      yield* Effect.promise(() => readResponse);
      try {
        const expired = yield* Effect.promise(() => pending);
        assert.equal(expired._tag, "Failure");
        if (expired._tag === "Failure") assert.equal(expired.failure.code, "deadline_exceeded");
        const disconnected = yield* observer.connectionClosed.pipe(Effect.result);
        assert.equal(disconnected._tag, "Failure");
        if (disconnected._tag === "Failure") assert.equal(disconnected.failure.code, "deadline_exceeded");
        const closed = yield* observer.close.pipe(Effect.result);
        assert.equal(closed._tag, "Failure");
        if (closed._tag === "Failure") assert.equal(closed.failure.code, "deadline_exceeded");
      } finally {
        releaseRead();
      }
      yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 3000 });
      assert.notEqual(stdio.pid, null, "A resource request timeout must not close the physical owner");
    }
    yield* client.call("native.get_state", { binding: client.binding }, { timeoutMs: 3000 });
  })));
});

test("a presentation close deadline cancels an immortal callback before physical SDK ownership teardown", async () => {
  const stdio = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", source], cwd, env: { NATIVE_CLIENT_PROOF_SERVER: "1" }, stderr: "ignore", maxBufferSize: 131072 });
  let physicallyClosed = false;
  let resolvePhysical!: () => void;
  const observedProcessClosed = new Promise<void>(resolve => { resolvePhysical = resolve; });
  const transport: Transport = {
    async start() {
      stdio.onmessage = message => transport.onmessage?.(message);
      stdio.onerror = error => transport.onerror?.(error);
      stdio.onclose = () => { physicallyClosed = true; resolvePhysical(); transport.onclose?.(); };
      await stdio.start();
    },
    send: message => stdio.send(message),
    close: () => stdio.close(),
  };
  const result = await Effect.runPromiseExit(Effect.scoped(Effect.gen(function*() {
    const client = yield* connectNativeTransport({ host: "pi", connectTimeoutMs: 10000 }, transport, {
      pid: null, observedProcessClosed,
      async close() { await stdio.close(); await observedProcessClosed; },
    });
    let entered!: () => void;
    const callbackEntered = new Promise<void>(resolve => { entered = resolve; });
    yield* client.watchPresentation({ binding: client.binding, after: 0 }, () => {
      entered();
      return new Promise<void>(() => {});
    });
    yield* client.call("native.prompt", { binding: client.binding, submissionId: Schema.decodeSync(NativeSubmissionId)("immortal-presentation"), message: "observer" }, { timeoutMs: 3000 });
    yield* Effect.promise(() => callbackEntered);
  })));
  assert.equal(result._tag, "Failure", "The actual presentation drain deadline remains observable");
  if (result._tag === "Failure") assert.ok(result.cause.reasons.some(reason => reason._tag === "Die" && reason.defect instanceof NativeControlError && reason.defect.code === "deadline_exceeded"), Cause.pretty(result.cause));
  assert.equal(physicallyClosed, true, "An unresolved user promise must not retain the real SDK child process");
  assert.equal(stdio.pid, null);
});
