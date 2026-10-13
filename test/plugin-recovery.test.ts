import assert from "node:assert/strict";
import { createServer } from "node:http";
import { pipeline } from "node:stream/promises";
import test from "node:test";
import { Clock, ConfigProvider, Context as EffectContext, Effect, Fiber, Layer, Schema, Sink, Stdio } from "effect";
import type { Scope } from "effect";
import { TestClock } from "effect/testing";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { NativePluginEndpoint } from "@samuelho/coordinator-native-control/contracts";
import { CommandError, Delivery, PluginConfig, Principal } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
import { Mcp, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Plugin } from "../src/plugin.ts";

interface RecoveryFixture {
  plugin: NonNullable<Plugin["Service"]>;
  ctx: Context;
  traffic: { method: string; at: number; sessionId?: string; incarnation?: string }[];
  notifications: { text: string; level: string | undefined }[];
  statuses: Map<string, string>;
  frames: string[];
  advance: (milliseconds: number) => Effect.Effect<void>;
  settle: () => Effect.Effect<void>;
  nativeState: () => Effect.Effect<{ messageCount: number; isStreaming: boolean }, unknown, Scope.Scope>;
  clock: TestClock.TestClock;
  eligible: () => boolean;
  fail: (value: { status: number; retryAfter?: string; method?: string } | undefined) => void;
  hang: (value: boolean) => void;
  holdPoll: () => { reached: Promise<void>; release: () => void };
  cancelled: () => number;
  conflict: () => void;
  replace: () => void;
}

// Native startup mutates Pi's documented agent directory; node:test runs these top-level tests serially.
const fixture = Effect.fn(function*(): Effect.fn.Return<RecoveryFixture, unknown, Platform | Mcp | Scope.Scope> {
  const platform = yield* Platform, mcp = yield* Mcp;
  const clock = yield* TestClock.make({ warningDelay: "1 minute" });
  yield* clock.setTime(Date.now());
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-recovery-" });
  yield* platform.fs.chmod(directory, 0o700);
  const agentDir = platform.path.join(directory, "agent");
  yield* platform.fs.makeDirectory(agentDir, { mode: 0o700 });
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previous;
  }));
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const tokenFile = platform.path.join(directory, "token"), stateDirectory = platform.path.join(directory, "state");
  yield* platform.fs.writeFileString(tokenFile, "fixture-token", { mode: 0o600 });
  const notifications: { text: string; level: string | undefined }[] = [], statuses = new Map<string, string>();
  const frames: string[] = [];
  let input: ((data: string) => { consume?: boolean; data?: string } | undefined) | undefined;
  let parentSession = "first-host-session";
  const ctx: Context = { cwd: directory, mode: "tui", hasUI: true,
    sessionManager: { getSessionId: () => parentSession },
    ui: { notify: (text, level) => { notifications.push({ text, level }); }, confirm: async () => false,
      setStatus: (key, value) => { if (value === undefined) statuses.delete(key); else statuses.set(key, value); },
      onTerminalInput: (handler) => { input = handler; return () => { input = undefined; }; } } };
  const traffic: { method: string; at: number; sessionId?: string; incarnation?: string }[] = [];
  let liveUntil = 0, polledAt = 0, conflict = false;
  const server = EffectContext.get(yield* Layer.build(serverLayer("127.0.0.1", 0)), HttpServer.HttpServer);
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed(Schema.decodeUnknownSync(Principal)({ id: "fixture", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] })),
    handle: (_principal, request) => Effect.gen(function*() {
      const now = clock.currentTimeMillisUnsafe();
      if (request.method === "session.heartbeat") { liveUntil = now + 30000; return { liveUntil }; }
      if (request.method === "session.poll") {
        polledAt = now;
        return { messages: conflict ? [Schema.decodeUnknownSync(Delivery)({ messageId: "same-message", incarnation: request.params.incarnation,
          payload: now, expiresAt: now + 60000 })] : [], offers: [], releases: [] };
      }
      if (request.method === "peer.ack") return { handled: true };
      if (request.method === "session.stop") return { stopped: true };
      return yield* new CommandError({ message: "Unexpected fixture command" });
    }),
  });
  assert.equal(server.address._tag, "InetAddressV4");
  if (server.address._tag !== "InetAddressV4") throw new Error("Loopback required");
  const upstream = `http://127.0.0.1:${server.address.port}`;
  let failure: { status: number; retryAfter?: string; method?: string } | undefined;
  let hanging = false, cancelled = 0;
  let pollHold: { reached: ReturnType<typeof Promise.withResolvers<void>>; released: ReturnType<typeof Promise.withResolvers<void>> } | undefined;
  const proxyFailures: string[] = [];
  const forwardingHeaders = (headers: Headers) => {
    const connection = headers.get("connection")?.split(",").map(name => name.trim()) ?? [];
    for (const name of ["host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "content-length", ...connection]) {
      if (name) headers.delete(name);
    }
    return headers;
  };
  // Real HTTP fault injection preserves status/header semantics before the MCP SDK decodes them.
  const proxy = createServer(async (request, response) => {
    const abort = new AbortController();
    response.on("close", () => abort.abort());
    request.on("aborted", () => abort.abort());
    let phase = "request body";
    try {
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of request) {
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        size += bytes.length;
        if (size > 1024 * 1024) throw new Error("Fixture request body limit");
        chunks.push(bytes);
      }
      const body = Buffer.concat(chunks);
      const rpc = body.length ? JSON.parse(body.toString("utf8")) : undefined;
      const method: string | undefined = rpc?.method === "tools/call" ? rpc.params.name : undefined;
      if (method) traffic.push({ method, at: clock.currentTimeMillisUnsafe(), sessionId: rpc.params.arguments?.sessionId, incarnation: rpc.params.arguments?.incarnation });
      if (method && hanging && method !== "session.stop") { response.on("close", () => { cancelled++; }); return; }
      if (method && failure && (failure.method === undefined || failure.method === method)) {
        response.writeHead(failure.status, { "content-type": "application/json", ...(failure.retryAfter ? { "retry-after": failure.retryAfter } : {}) });
        response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, error: { code: failure.status === 501 ? -32601 : -32000, message: "PRIVATE_FAULT_DETAIL" } }));
        return;
      }
      if (method === "session.poll" && pollHold) {
        const hold = pollHold;
        pollHold = undefined;
        hold.reached.resolve();
        await hold.released.promise;
      }
      const headers = new Headers();
      for (const [key, value] of Object.entries(request.headers)) if (value !== undefined) headers.set(key, Array.isArray(value) ? value.join(",") : value);
      phase = "upstream request";
      const result = await fetch(upstream + request.url, { method: request.method, headers: forwardingHeaders(headers), ...(body.length ? { body } : {}), signal: abort.signal });
      const responseHeaders = forwardingHeaders(new Headers(result.headers));
      // fetch decodes content encodings; outgoing framing belongs to this response.
      responseHeaders.delete("content-encoding");
      phase = "upstream response";
      response.writeHead(result.status, Object.fromEntries(responseHeaders));
      response.flushHeaders();
      if (result.body) {
        const body = result.body;
        const chunks = async function*() {
          const reader = body.getReader();
          try {
            for (;;) {
              const chunk = await reader.read();
              if (chunk.done) return;
              yield chunk.value;
            }
          } finally {
            try { await reader.cancel(); }
            finally { reader.releaseLock(); }
          }
        };
        await pipeline(chunks(), response, { signal: abort.signal });
      } else response.end();
    } catch (error) {
      if (request.aborted || abort.signal.aborted) return;
      proxyFailures.push(phase);
      if (!response.destroyed) {
        if (response.headersSent) response.destroy();
        else { response.writeHead(502, { "content-type": "text/plain" }); response.end(`Fixture transport failure: ${phase}`); }
      }
    }
  });
  yield* Effect.promise(() => new Promise<void>(resolve => proxy.listen(0, "127.0.0.1", resolve)));
  yield* Effect.addFinalizer(() => Effect.promise(async () => {
    await new Promise<void>(resolve => { proxy.closeAllConnections(); proxy.close(() => resolve()); });
    assert.deepEqual(proxyFailures, [], "Loopback fixture transport failed (controlled categories only)");
  }));
  const address = proxy.address();
  assert.ok(address && typeof address === "object");
  const config = platform.path.join(directory, "config.json");
  yield* platform.fs.writeFileString(config, Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({ brokerUrl: `http://127.0.0.1:${address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const plugin = EffectContext.get(yield* Layer.build(Plugin.layer.pipe(
    Layer.provide(Layer.succeed(Platform, platform)),
    Layer.provide(Layer.succeed(Clock.Clock, clock)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
    Layer.provide(Stdio.layerTest({ stdoutIsTerminal: Effect.succeed(true), stdout: () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => {
      if (typeof chunk !== "string") return;
      frames.push(chunk);
      if (chunk.includes("7501;?")) queueMicrotask(() => input?.("\x1b]7501;?\x1b\\"));
    })) })),
  )), Plugin);
  assert.ok(plugin);
  // Only Effect time is virtual. Give the actual socket and SDK microtasks time to settle at every step.
  const settle = () => Effect.promise(() => new Promise<void>(resolve => setTimeout(resolve, 25)));
  const advance = Effect.fn(function*(milliseconds: number) {
    for (let remaining = milliseconds; remaining > 0; remaining -= 250) { yield* clock.adjust(Math.min(remaining, 250)); yield* settle(); }
  });
  const nativeState = Effect.fn(function*() {
    const nativeDirectory = platform.path.join(stateDirectory, `native-${yield* platform.digest(parentSession)}`);
    const endpoint = Schema.decodeUnknownSync(Schema.fromJsonString(NativePluginEndpoint))(yield* platform.privateFile(platform.path.join(nativeDirectory, "endpoint.json")));
    const token = yield* platform.privateFile(platform.path.join(nativeDirectory, "control.token"));
    const client = yield* Effect.acquireRelease(Effect.sync(() => new Client({ name: "recovery-fixture", version: "1" })), client => Effect.promise(() => client.close()));
    yield* Effect.promise(() => client.connect(new StreamableHTTPClientTransport(new URL(endpoint.url), { requestInit: { headers: { authorization: `Bearer ${token}` } } })));
    const result = yield* Effect.promise(() => client.callTool({ name: "native.get_state", arguments: { binding: endpoint.binding } }));
    assert.notEqual(result.isError, true, "Paused broker traffic must not dispose the owned native endpoint");
    return Schema.decodeUnknownSync(Schema.Struct({ result: Schema.Struct({ messageCount: Schema.Number, isStreaming: Schema.Boolean }) }))(result.structuredContent).result;
  });
  return { plugin, ctx, traffic, notifications, statuses, frames, advance, settle, nativeState, clock,
    eligible: () => clock.currentTimeMillisUnsafe() < liveUntil && clock.currentTimeMillisUnsafe() - polledAt < 10000,
    fail: (value: typeof failure) => { failure = value; }, hang: (value: boolean) => { hanging = value; }, cancelled: () => cancelled,
    holdPoll: () => {
      const hold = { reached: Promise.withResolvers<void>(), released: Promise.withResolvers<void>() };
      pollHold = hold;
      return { reached: hold.reached.promise, release: () => hold.released.resolve() };
    },
    conflict: () => { conflict = true; }, replace: () => { parentSession = "replacement-host-session"; } };
});

const run = (body: (f: RecoveryFixture) => Effect.Effect<void, unknown, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const f = yield* fixture();
    yield* body(f).pipe(Effect.provideService(Clock.Clock, f.clock));
  })).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer))));

const warnings = (f: RecoveryFixture) => f.notifications.filter(item => item.level === "warning");

test("idle presence survives one outage epoch, partial recovery, capped jitter and fresh reenrollment", { timeout: 60000 }, () => run(f => Effect.gen(function*() {
  yield* f.plugin.start(f.ctx);
  yield* f.advance(1250);
  assert.ok(f.traffic.filter(call => call.method === "session.poll").length >= 2, "Idle enrollment continues without native prompts");
  f.fail({ status: 503, method: "session.poll" });
  yield* f.advance(1500);
  assert.equal(warnings(f).length, 1, "A failed poll emits one outage transition notice");
  const notices = f.notifications.length;
  yield* f.advance(6000);
  assert.equal(f.notifications.length, notices, "Successful heartbeat alone cannot claim recovery or repeat outage notices");
  assert.equal((yield* f.nativeState()).messageCount, 0, "Presence maintenance never admits a native prompt");
  assert.ok(f.statuses.has("coordinator.network"), "Connectivity remains in the native keyed footer");
  assert.ok(f.frames.some(frame => frame === `\x1b]7501;id=coordinator:app=coordinator:state=idle:msg=${Buffer.from("Coordinator reconnecting").toString("base64")}\x1b\\`), "Retrying connectivity composes the idle public coordinator PSP record");
  assert.ok(f.frames.filter(frame => frame.includes("id=")).every(frame => frame.startsWith("\x1b]7501;id=coordinator:")), "Task and network status share one PSP record identity");
  f.fail({ status: 503 });
  const started = f.traffic.length;
  yield* f.advance(150000);
  const attempts = f.traffic.slice(started).filter(call => call.method === "session.heartbeat");
  assert.ok(attempts.length >= 4 && attempts.length < 14, "An offline idle owner retains bounded low-rate recovery");
  const gaps = attempts.slice(1).map((call, index) => call.at - attempts[index]!.at);
  assert.ok(gaps.every(gap => gap >= 750 && gap <= 30500), "Jitter is capped after application, not before");
  assert.ok(gaps.some(gap => gap >= 12500), "Changing failed methods must not reset the retained outage schedule");
  assert.equal(f.eligible(), false, "Lease and poll eligibility really become stale; no deadline inflation");
  assert.equal(warnings(f).length, 1);
  assert.ok(f.statuses.has("coordinator.network"));
  f.fail(undefined);
  yield* f.advance(31000);
  assert.equal(f.eligible(), true, "Fresh heartbeat and poll restore enrollment after lease expiry");
  assert.equal(f.statuses.has("coordinator.network"), false);
  assert.equal(f.notifications.length, notices + 1, "Only the full successful pair announces recovery once");
  const nextOutage = f.traffic.length;
  f.fail({ status: 503, method: "session.poll" });
  yield* f.advance(1500);
  const first = f.traffic.slice(nextOutage).find(call => call.method === "session.poll")!.at;
  yield* f.advance(1500);
  assert.ok(f.traffic.some(call => call.method === "session.heartbeat" && call.at > first && call.at - first <= 1500), "A new outage resets to the initial jittered target");
  assert.equal(warnings(f).length, 2);
  yield* f.plugin.stop();
  const stopped = f.traffic.length;
  yield* f.advance(35000);
  assert.equal(f.traffic.length, stopped, "Stop interrupts recovery backoff");
})));

test("server delay exceeds the local cap while static authorization and configuration failures pause only broker traffic", { timeout: 60000 }, () => run(f => Effect.gen(function*() {
  yield* f.plugin.start(f.ctx);
  f.fail({ status: 429, retryAfter: "45", method: "session.poll" });
  yield* f.advance(1500);
  const failureAt = f.traffic.at(-1)!.at, before = f.traffic.length;
  yield* f.advance(43000);
  assert.equal(f.traffic.length, before, "Retry-After seconds are a minimum, even beyond the 30-second cap");
  f.fail(undefined);
  yield* f.advance(3000);
  assert.ok(f.traffic.some(call => call.method === "session.heartbeat" && call.at - failureAt >= 45000));
  for (const status of [401, 403, 400, 404, 501]) {
    f.fail({ status });
    yield* f.advance(1500);
    const paused = f.traffic.length;
    yield* f.advance(35000);
    assert.equal(f.traffic.length, paused, `${status} cannot resend unchanged credentials/configuration`);
    yield* f.nativeState();
    assert.ok(f.statuses.has("coordinator.paused"), `${status} exposes the permanent broker pause`);
    assert.equal(f.statuses.has("coordinator.network"), false, `${status} clears the retrying connectivity status`);
    f.fail(undefined);
    yield* f.plugin.start(f.ctx);
  }
  yield* f.plugin.stop();
})));

test("retirement cancels an in-flight poll and never resumes the old binding in a replacement host session", { timeout: 30000 }, () => run(f => Effect.gen(function*() {
  yield* f.plugin.start(f.ctx);
  const old = f.traffic.find(call => call.method === "session.heartbeat")!;
  f.hang(true);
  yield* f.advance(1500);
  const stop = yield* f.plugin.stop().pipe(Effect.forkChild);
  yield* f.settle();
  assert.ok(f.cancelled() > 0, "Actual scoped retirement cancels the open HTTP request, not just future ticks");
  yield* Fiber.join(stop);
  f.replace();
  f.hang(false);
  yield* f.plugin.start(f.ctx);
  const replacement = f.traffic.filter(call => call.method === "session.heartbeat").at(-1)!;
  assert.notEqual(replacement.sessionId, old.sessionId);
  assert.notEqual(replacement.incarnation, old.incarnation);
  const before = f.traffic.length;
  yield* f.advance(35000);
  assert.ok(f.traffic.length > before, "The new idle owner maintains its own presence");
  assert.ok(f.traffic.slice(before).every(call => call.incarnation !== old.incarnation));
  yield* f.plugin.stop();
})));

test("a structured heartbeat conflict obtains a fresh incarnation and a journal conflict pauses further admission", { timeout: 30000 }, () => run(f => Effect.gen(function*() {
  f.fail({ status: 409, method: "session.heartbeat" });
  yield* Effect.exit(f.plugin.start(f.ctx));
  assert.equal(f.traffic.filter(call => call.method === "session.poll").length, 0);
  const binding = f.traffic.find(call => call.method === "session.heartbeat")!;
  f.fail(undefined);
  yield* f.advance(10000);
  const replacement = f.traffic.filter(call => call.method === "session.heartbeat").at(-1)!;
  assert.notEqual(replacement.incarnation, binding.incarnation, "A structured conflict retires the fenced native incarnation");
  const stopped = f.traffic.findIndex(call => call.method === "session.stop" && call.incarnation === binding.incarnation);
  const enrolled = f.traffic.findIndex(call => call.method === "session.heartbeat" && call.incarnation === replacement.incarnation);
  assert.ok(stopped >= 0 && stopped < enrolled, "Conflict stops the old enrollment before reenrollment");
  assert.ok(f.traffic.filter(call => call.method === "session.poll").every(call => call.incarnation === replacement.incarnation), "Only the fresh incarnation polls after a heartbeat fence");
  assert.equal(f.eligible(), true);
  f.conflict();
  yield* f.advance(3000);
  const paused = f.traffic.length;
  yield* f.advance(35000);
  assert.equal(f.traffic.length, paused, "A real repeated message id with different durable content latches admission/recovery");
  yield* f.nativeState();
  assert.ok(!f.notifications.some(item => /PRIVATE_FAULT_DETAIL|fixture-token/.test(item.text)));
  yield* f.plugin.stop();
})));

test("poll and heartbeat deadlines share one outage notice and partial heartbeat success retains offline status", { timeout: 30000 }, () => run(f => Effect.gen(function*() {
  yield* f.plugin.start(f.ctx);
  f.hang(true);
  yield* f.advance(12500);
  assert.equal(warnings(f).length, 1);
  assert.ok(f.statuses.has("coordinator.network"));
  yield* f.advance(13000);
  assert.ok(f.cancelled() >= 2, "The unchanged ten-second command deadline closes poll and heartbeat requests");
  assert.equal(warnings(f).length, 1, "Changing failed method never creates a second outage epoch");
  f.hang(false);
  f.fail({ status: 503, method: "session.poll" });
  const before = f.notifications.length;
  yield* f.advance(12000);
  assert.equal(f.notifications.length, before, "Successful heartbeat followed by failed poll is still offline");
  assert.ok(f.statuses.has("coordinator.network"));
  f.fail(undefined);
  yield* f.advance(31000);
  assert.equal(f.statuses.has("coordinator.network"), false);
  assert.equal(f.notifications.length, before + 1);
  yield* f.plugin.stop();
})));

for (const status of [403, 503]) {
  test(`an older successful poll cannot clear a concurrent ${status} outage`, { timeout: 30000 }, () => run(f => Effect.gen(function*() {
    yield* f.plugin.start(f.ctx);
    const hold = f.holdPoll();
    yield* f.advance(1000);
    yield* Effect.promise(() => hold.reached);
    f.fail({ status, method: "session.list" });
    yield* Effect.exit(f.plugin.command("sessions", f.ctx));
    hold.release();
    yield* f.settle();
    assert.equal(f.notifications.filter(item => item.text === "Coordinator connection recovered.").length, 0);
    assert.ok(f.statuses.has(status === 403 ? "coordinator.paused" : "coordinator.network"));
    const boundary = f.traffic.length;
    f.fail(undefined);
    yield* f.advance(5000);
    if (status === 403) {
      assert.equal(f.traffic.length, boundary, "Permanent pause survives the earlier successful poll");
      assert.ok(f.statuses.has("coordinator.paused"));
    } else {
      assert.deepEqual(f.traffic.slice(boundary, boundary + 2).map(call => call.method), ["session.heartbeat", "session.poll"]);
      assert.equal(f.statuses.has("coordinator.network"), false);
    }
    yield* f.plugin.stop();
  })));
}

test("a permanent command fault prevents a queued conflict restart", { timeout: 30000 }, () => run(f => Effect.gen(function*() {
  f.fail({ status: 409, method: "session.heartbeat" });
  yield* f.plugin.start(f.ctx);
  f.fail({ status: 403, method: "session.list" });
  yield* Effect.exit(f.plugin.command("sessions", f.ctx));
  const boundary = f.traffic.length;
  f.fail(undefined);
  yield* f.advance(15000);
  assert.equal(f.traffic.length, boundary, "Conflict recovery cannot bypass unchanged permanent authorization failure");
  assert.ok(f.statuses.has("coordinator.paused"));
  yield* f.nativeState();
  yield* f.plugin.stop();
})));
