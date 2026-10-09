import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { mkdtemp, realpath, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import nativeControl from "../src/native-plugin.js";
import { Client, ProtocolError, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { NodeCrypto, NodeHttpServer, NodeRuntime, NodeServices, NodeStdio } from "@effect/platform-node";
import { Effect, Layer, Record, Schema } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { connectNativeClient } from "../packages/native-control/src/client.js";
import { NativeBinding, NativeControlError, NativeMcpProtocolVersion, NativeMcpResultBytes, NativeMcpWireBytes, NativeTools, type NativeHost } from "../packages/native-control/src/contracts.js";
import { NativeLifecycleJournal } from "../packages/native-control/src/lifecycle.js";
import { NativeMcpLifetime, nativeControlHttpLayer, nativeControlServerLayer, nativeTool } from "../packages/native-control/src/server.js";
import { createNativePresentation } from "../packages/native-control/src/interaction.js";

type RawServer = {
  readonly child: ChildProcessWithoutNullStreams;
  readonly next: () => Promise<unknown>;
  readonly exit: Promise<unknown[]>;
  readonly stderr: () => string;
};

const path = fileURLToPath(import.meta.url);
const cwd = fileURLToPath(new URL("..", import.meta.url));
const host = Schema.decodeUnknownSync(Schema.UndefinedOr(Schema.Literals(["pi", "omp"])))(process.env.NATIVE_BYTE_LIMIT_SERVER);
if (host !== undefined) {
  const binding = Schema.decodeUnknownSync(NativeBinding)({ host, sessionId: "byte-limits", incarnation: "sdk", generation: 0 });
  const presentation = createNativePresentation(() => ({ nativeBinding: binding, nativeRunId: null, emitNativeInput() {} }));
  const tools = Record.keys(NativeTools)
    .filter(name => NativeTools[name].hosts.some(value => value === host))
    .map(name => name === "native.get_state"
      ? nativeTool(name, () => Effect.succeed({ binding, cwd, model: null, currentRunId: null, isStreaming: false, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: 0, queuedMessages: { steering: [], followUp: [] }, steeringMode: "all", followUpMode: "all", interruptMode: "immediate", autoCompactionEnabled: true, fastModeEnabled: false, fastModeActive: false, slowModeSupported: false, slowModeEnabled: false, tokensPerSecond: null, goal: null, systemPrompt: host === "pi" ? "😀".repeat(20000) : ["😀".repeat(20000)], dumpTools: [], ...(host === "omp" ? { todoPhases: [] } : {}) }))
      : nativeTool(name, () => Effect.fail(new NativeControlError({ code: "unsupported_operation", message: "Boundary regression only reads canonical metadata and state" }))));
  const cleanup = Layer.effectDiscard(Effect.addFinalizer(() => Effect.sync(() => process.stderr.write("native-byte-cleanup\n"))));
  if (process.env.NATIVE_BYTE_LIMIT_HTTP === "1") {
    NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
      const app = yield* HttpRouter.toHttpEffect(nativeControlHttpLayer({ host, binding, journal: new NativeLifecycleJournal(), presentation, tools, name: "native-http-pages", version: "1" }));
      const server = yield* NodeHttpServer.make(() => createServer(), { host: "127.0.0.1", port: 0 });
      yield* server.serve(app);
      console.log(JSON.stringify({ url: `${HttpServer.formatAddress(server.address)}/mcp` }));
      return yield* Effect.never;
    }).pipe(Effect.provide(NodeServices.layer))));
  } else {
    NodeRuntime.runMain(Effect.scoped(NativeMcpLifetime.use(owner => owner.closed).pipe(Effect.provide(nativeControlServerLayer({ host, binding, journal: new NativeLifecycleJournal(), presentation, tools, name: "native-byte-regression", version: "1" })
      .pipe(Layer.provide(Layer.mergeAll(NodeStdio.layer, NodeCrypto.layer, cleanup)))))));
  }
} else {
  const args = ["--import", "tsx", path];
  const initialize = (id: number | string = 1) => ({ jsonrpc: "2.0", id, method: "initialize", params: { protocolVersion: NativeMcpProtocolVersion, capabilities: {}, clientInfo: { name: "😀real-standard-client", version: "1" } } });
  const raw = (t: TestContext, isHttp = false): RawServer => {
    const child = spawn(process.execPath, args, { cwd, env: { ...process.env, NATIVE_BYTE_LIMIT_SERVER: "omp", NATIVE_BYTE_LIMIT_HTTP: isHttp ? "1" : "0" }, stdio: "pipe" });
    t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
    let pending = "";
    const messages: unknown[] = [];
    const waiters: ((value: unknown) => void)[] = [];
    let stderr = "";
    child.stderr.on("data", data => { stderr += data.toString(); });
    child.stdin.on("error", () => {});
    child.stdout.on("data", data => {
      pending += data.toString();
      for (;;) {
        const end = pending.indexOf("\n");
        if (end < 0) break;
        const line = pending.slice(0, end + 1);
        pending = pending.slice(end + 1);
        assert.ok(Buffer.byteLength(line) <= NativeMcpWireBytes, "actual encoded SDK response exceeds the wire cap");
        const value: unknown = JSON.parse(line);
        const waiter = waiters.shift();
        if (waiter) waiter(value); else messages.push(value);
      }
    });
    const next = () => messages.length ? Promise.resolve(messages.shift()) : new Promise<unknown>(resolve => waiters.push(resolve));
    const exit = once(child, "exit");
    return { child, next, exit, stderr: () => stderr };
  };
  const send = (child: ChildProcessWithoutNullStreams, value: unknown) => child.stdin.write(`${JSON.stringify(value)}\n`);
  const ready = async (server: RawServer) => {
    send(server.child, initialize());
    const response = await server.next();
    assert.ok(response && typeof response === "object" && "result" in response);
    send(server.child, { jsonrpc: "2.0", method: "notifications/initialized" });
  };

  test("public transcript JSON serialization preserves data and fails closed at the real HTTP boundary", { timeout: 15000 }, async t => {
    const directory = await realpath(await mkdtemp(join(tmpdir(), "native-public-json-")));
    const token = "private-transcript-boundary-token-".repeat(2);
    const tokenFile = join(directory, "token"), endpointFile = join(directory, "endpoint"), configFile = join(directory, "config");
    await writeFile(tokenFile, token, { mode: 0o600 });
    await writeFile(configFile, JSON.stringify({ host: "pi", tokenFile, endpointFile }), { mode: 0o600 });
    const previousConfig = process.env.COORDINATOR_NATIVE_CONFIG;
    process.env.COORDINATOR_NATIVE_CONFIG = configFile;
    type PublicHost = Parameters<typeof nativeControl>[0];
    const handlers = new Map<string, Parameters<PublicHost["on"]>[1]>();
    const sessionManager = SessionManager.inMemory(directory);
    const ctx: Parameters<Parameters<PublicHost["on"]>[1]>[1] = { cwd: directory, mode: "rpc", hasUI: false, sessionManager, model: undefined,
      isIdle: () => true, hasPendingMessages: () => false, abort: () => {}, getSystemPrompt: () => "" };
    const client = new Client({ name: "public-transcript-consumer", version: "1" }, { supportedProtocolVersions: [NativeMcpProtocolVersion], versionNegotiation: { mode: "legacy" } });
    t.after(async () => {
      await client.close();
      await handlers.get("session_shutdown")?.({}, ctx);
      if (previousConfig === undefined) delete process.env.COORDINATOR_NATIVE_CONFIG;
      else process.env.COORDINATOR_NATIVE_CONFIG = previousConfig;
      await rm(directory, { recursive: true, force: true });
    });
    await nativeControl({ on(name, handler) { handlers.set(name, handler); }, sendUserMessage() {},
      exec: async () => ({ code: 0, stdout: "", stderr: "", killed: false }) });
    const start = handlers.get("session_start");
    assert.ok(start);
    await start({}, ctx);
    const endpoint = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Struct({ url: Schema.String, binding: NativeBinding })))(await readFile(endpointFile, "utf8"));
    await client.connect(new StreamableHTTPClientTransport(new URL(endpoint.url), { requestInit: { headers: { authorization: `Bearer ${token}` } } }));
    sessionManager.appendCustomEntry("optional-data");
    sessionManager.appendCustomEntry("complete-data", { date: new Date("2026-01-02T03:04:05Z"), values: [undefined, "😀"], optional: undefined, nested: { retained: true } });
    sessionManager.appendMessage({ role: "user", content: [{ type: "text", text: "raw public message 😀" }], timestamp: 123 });
    sessionManager.appendMessage({ role: "custom", customType: "raw-details", content: "public custom message", display: false,
      details: { date: new Date("2026-02-03T04:05:06Z"), optional: undefined, retained: ["😀", 42] }, timestamp: 124 });
    const entries = await client.callTool({ name: "native.get_entries", arguments: { binding: endpoint.binding } });
    assert.equal(entries.isError, undefined);
    assert.deepEqual(entries.structuredContent, { result: JSON.parse(JSON.stringify({ entries: sessionManager.getEntries(), leafId: sessionManager.getLeafId() })) });
    const messages = await client.callTool({ name: "native.get_messages", arguments: { binding: endpoint.binding } });
    assert.equal(messages.isError, undefined);
    assert.deepEqual(messages.structuredContent, { result: JSON.parse(JSON.stringify({ binding: endpoint.binding, messages: sessionManager.getBranch().filter(entry => entry.type === "message").map(entry => entry.message), nextCursor: null, more: false })) });
    const since = sessionManager.getLeafId();
    sessionManager.appendCustomEntry("nonserializable", { bigint: 1n });
    const unrepresentable = await client.callTool({ name: "native.get_entries", arguments: { binding: endpoint.binding, since } });
    const failure = Schema.Struct({ error: NativeControlError });
    assert.equal(unrepresentable.isError, true);
    assert.equal(Schema.decodeUnknownSync(failure)(unrepresentable.structuredContent).error.message, "Public transcript entry is not JSON serializable");
    const oversizedSince = sessionManager.getLeafId();
    sessionManager.appendCustomEntry("oversized", { text: "😀".repeat(17 * 1024 * 1024) });
    const oversized = await client.callTool({ name: "native.get_entries", arguments: { binding: endpoint.binding, since: oversizedSince } });
    assert.equal(oversized.isError, true);
    assert.equal(Schema.decodeUnknownSync(failure)(oversized.structuredContent).error.message, "Public transcript entry exceeds the native contract");
  });

  const profiles: ReadonlyArray<NativeHost> = ["pi", "omp"];
  for (const profile of profiles) {
    test(`shared MCP server rejects invalid DTOs and preserves private UTF-8 state for ${profile}`, { timeout: 20000 }, async () => {
      const transport = new StdioClientTransport({ command: process.execPath, args, cwd, env: { ...process.env, NATIVE_BYTE_LIMIT_SERVER: profile }, maxBufferSize: NativeMcpWireBytes, stderr: "pipe" });
      const client = new Client({ name: "byte-regression", version: "1" }, { supportedProtocolVersions: [NativeMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
      transport.stderr?.on("data", () => {});
      try {
        await client.connect(transport);
        const invalid = await client.callTool({ name: "native.get_state", arguments: { unexpected: "valid JSON, invalid native DTO" } });
        assert.equal(invalid.isError, true);
        const failure = Schema.decodeUnknownSync(Schema.toCodecJson(Schema.Struct({ error: NativeControlError })), { onExcessProperty: "error" })(invalid.structuredContent);
        assert.equal(failure.error.code, "invalid_input");
      } finally { await client.close(); }
      await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        const native = yield* connectNativeClient({ host: profile, command: process.execPath, args, cwd, env: { ...process.env, NATIVE_BYTE_LIMIT_SERVER: profile }, maxBufferSize: NativeMcpWireBytes, connectTimeoutMs: 10000 });
        const state = yield* native.call("native.get_state", {}, { timeoutMs: 10000 });
        assert.deepEqual(state.systemPrompt, profile === "pi" ? "😀".repeat(20000) : ["😀".repeat(20000)]);
      })));
    });
  }

  test("actual SDK list pages fit the result budget and reject foreign cursors", { timeout: 15000 }, async t => {
    const transport = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", path], cwd,
      env: { NATIVE_BYTE_LIMIT_SERVER: "omp" }, stderr: "ignore", maxBufferSize: NativeMcpWireBytes });
    const client = new Client({ name: "native-page-consumer", version: "1" },
      { capabilities: {}, supportedProtocolVersions: [NativeMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
    t.after(() => client.close());
    await client.connect(transport);
    let pages = 0;
    const deliver = transport.onmessage;
    transport.onmessage = (message) => {
      if ("result" in message && typeof message.result === "object" && message.result !== null && "tools" in message.result) {
        assert.ok(Buffer.byteLength(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(message.result)) <= NativeMcpResultBytes);
        pages++;
      }
      deliver?.(message);
    };
    const { tools } = await client.listTools();
    assert.ok(pages > 1, "full canonical metadata must exercise standard SDK pagination");
    assert.equal(tools.length, 65);
    assert.equal(new Set(tools.map(tool => tool.name)).size, 65);
    await assert.rejects(client.listTools({ cursor: "foreign:0:1" }), error => ProtocolError.isInstance(error) && error.code === -32602);
    await client.close();
  });

  test("HTTP catalog pagination stays session-bound across transient RPC clients", { timeout: 15000 }, async t => {
    const server = raw(t, true);
    const endpoint = Schema.decodeUnknownSync(Schema.Struct({ url: Schema.String }), { onExcessProperty: "error" })(await server.next());
    const client = new Client({ name: "native-http-pages", version: "1" }, { supportedProtocolVersions: [NativeMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
    const foreign = new Client({ name: "foreign-native-http-pages", version: "1" }, { supportedProtocolVersions: [NativeMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
    t.after(async () => { await client.close(); await foreign.close(); });
    const transport = new StreamableHTTPClientTransport(new URL(endpoint.url));
    await client.connect(transport);
    let cursor: unknown;
    let pages = 0;
    const deliver = transport.onmessage;
    transport.onmessage = message => {
      if ("result" in message && typeof message.result === "object" && message.result !== null && "tools" in message.result) {
        const page = Schema.decodeUnknownSync(Schema.Struct({ tools: Schema.Array(Schema.Unknown), nextCursor: Schema.optional(Schema.String) }))(message.result);
        assert.ok(Buffer.byteLength(JSON.stringify(message.result)) <= NativeMcpResultBytes);
        if (page.nextCursor !== undefined) cursor = page.nextCursor;
        pages++;
      }
      deliver?.(message);
    };
    const { tools } = await client.listTools();
    assert.deepEqual(tools.map(tool => tool.name).sort(), Record.keys(NativeTools).filter(name => NativeTools[name].hosts.some(host => host === "omp")).sort());
    assert.ok(pages > 1, "HTTP discovery must cross a real catalog page boundary");
    await foreign.connect(new StreamableHTTPClientTransport(new URL(endpoint.url)));
    await assert.rejects(foreign.listTools({ cursor: Schema.decodeUnknownSync(Schema.String)(cursor) }), error => ProtocolError.isInstance(error) && error.code === -32602);
    await assert.rejects(client.listTools({ cursor: "foreign:0:1" }), error => ProtocolError.isInstance(error) && error.code === -32602);
  });


  const invalid: ReadonlyArray<readonly [string, Buffer]> = [
    ["raw whitespace", Buffer.from(" ".repeat(NativeMcpWireBytes) + "\n")],
    ["non-ASCII bytes", Buffer.from("😀".repeat(NativeMcpWireBytes / 4 + 1))],
    ["long unterminated input", Buffer.from("x".repeat(NativeMcpWireBytes + 1))],
    ["fatal UTF-8", Buffer.from([0xc3, 0x28, 0x0a])],
  ];
  for (const [name, bytes] of invalid) test(`${name} closes the actual SDK source and its physical scope`, { timeout: 15000 }, async t => {
    const server = raw(t);
    await ready(server);
    server.child.stdin.write(bytes);
    await server.exit;
    assert.match(server.stderr(), /native-byte-cleanup/);
  });

  test("incomplete UTF-8 at EOF closes the source scope", { timeout: 15000 }, async t => {
    const server = raw(t);
    await ready(server);
    server.child.stdin.end(Buffer.from([0xf0, 0x9f]));
    await server.exit;
    assert.match(server.stderr(), /native-byte-cleanup/);
  });

  test("SDK output with a huge echoed request ID is closed before oversized bytes escape", { timeout: 15000 }, async t => {
    const server = raw(t);
    await ready(server);
    send(server.child, { jsonrpc: "2.0", id: "i".repeat(63000), method: "tools/list", params: {} });
    await server.exit;
    assert.match(server.stderr(), /native-byte-cleanup/);
  });
}
