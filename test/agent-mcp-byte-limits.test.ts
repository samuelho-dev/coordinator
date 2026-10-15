import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import type { TestContext } from "vitest";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { mkdtemp, realpath, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeAgentSessionServer } from "../src/runner-plugin.ts";
import { Platform } from "../src/platform.ts";
import { Client, ProtocolError, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { NodeCrypto, NodeHttpServer, NodeRuntime, NodeServices, NodeStdio } from "@effect/platform-node";
import { Effect, Exit, Layer, ManagedRuntime, Schema, Scope } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { connectAgentClient } from "../src/agent/client.ts";
import { AgentHostEnum, AgentBinding, AgentError, AgentMcpProtocolVersion, AgentMcpResultBytes, AgentMcpWireBytes, AgentTools, AgentHost } from "../src/agent/contracts.ts";
import { AgentLifecycleJournal } from "../src/agent/lifecycle.ts";
import { AgentMcpLifetime, agentHttpLayer, agentStdioLayer, agentTool } from "../src/agent/server.ts";
import { createAgentPresentation } from "../src/agent/interaction.ts";

type RawServer = {
  readonly child: ChildProcessWithoutNullStreams;
  readonly next: () => Promise<unknown>;
  readonly exit: Promise<unknown[]>;
  readonly stderr: () => string;
};

const path = fileURLToPath(import.meta.url);
const cwd = fileURLToPath(new URL("..", import.meta.url));
const host = Schema.decodeUnknownSync(Schema.UndefinedOr(AgentHost))(process.env.AGENT_BYTE_LIMIT_SERVER);
if (host !== undefined) {
  const binding = Schema.decodeUnknownSync(AgentBinding)({ host, sessionId: "byte-limits", incarnation: "sdk", generation: 0 });
  const presentation = createAgentPresentation(() => ({ agentBinding: binding }));
  const tools = Object.values(AgentTools)
    .filter(name => name.hosts.some(value => value === host))
    .map(name => name.name === AgentTools.getState.name
      ? agentTool(AgentTools.getState, () => Effect.succeed({ binding, cwd, model: null, currentRunId: null, isStreaming: false, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: 0, queuedMessages: { steering: [], followUp: [] }, steeringMode: "all", followUpMode: "all", interruptMode: "immediate", autoCompactionEnabled: true, fastModeEnabled: false, fastModeActive: false, slowModeSupported: false, slowModeEnabled: false, tokensPerSecond: null, goal: null, systemPrompt: host === AgentHostEnum.pi ? "😀".repeat(20000) : ["😀".repeat(20000)] }))
      : agentTool(name, () => Effect.fail(new AgentError({ code: "unsupported_operation", message: "Boundary regression only reads canonical metadata and state" }))));
  const cleanup = Layer.effectDiscard(Effect.addFinalizer(() => Effect.sync(() => process.stderr.write("agent-byte-cleanup\n"))));
  if (process.env.AGENT_BYTE_LIMIT_HTTP === "1") {
    NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
      const app = yield* HttpRouter.toHttpEffect(agentHttpLayer({ host, binding, journal: new AgentLifecycleJournal(), presentation, tools, name: "agent-http-pages", version: "1" }));
      const server = yield* NodeHttpServer.make(() => createServer(), { host: "127.0.0.1", port: 0 });
      yield* server.serve(app);
      console.log(JSON.stringify({ url: `${HttpServer.formatAddress(server.address)}/mcp` }));
      return yield* Effect.never;
    }).pipe(Effect.provide(NodeServices.layer))));
  } else {
    NodeRuntime.runMain(Effect.scoped(AgentMcpLifetime.use(owner => owner.closed).pipe(Effect.provide(agentStdioLayer({ host, binding, journal: new AgentLifecycleJournal(), presentation, tools, name: "agent-byte-regression", version: "1" })
      .pipe(Layer.provide(Layer.mergeAll(NodeStdio.layer, NodeCrypto.layer, cleanup)))))));
  }
} else {
  // Static Vitest imports cannot load in the directly launched Node fixture process.
  const { it } = await import("@effect/vitest");
  const args = ["--import", "tsx", path];
  const initialize = (id: number | string = 1) => ({ jsonrpc: "2.0", id, method: "initialize", params: { protocolVersion: AgentMcpProtocolVersion, capabilities: {}, clientInfo: { name: "😀real-standard-client", version: "1" } } });
  const raw = (t: TestContext, isHttp = false): RawServer => {
    const child = spawn(process.execPath, args, { cwd, env: { ...process.env, AGENT_BYTE_LIMIT_SERVER: AgentHostEnum.omp, AGENT_BYTE_LIMIT_HTTP: isHttp ? "1" : "0" }, stdio: "pipe" });
    t.onTestFinished(() => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
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
        assert.ok(Buffer.byteLength(line) <= AgentMcpWireBytes, "actual encoded SDK response exceeds the wire cap");
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

  it("public transcript JSON serialization preserves data and fails closed at the real HTTP boundary", async (t: TestContext) => { const directory = await realpath(await mkdtemp(join(tmpdir(), "agent-public-json-")));
  const token = "private-transcript-boundary-token-".repeat(2);
  const tokenFile = join(directory, "token"), endpointFile = join(directory, "endpoint"), agentDir = join(directory, "agent");
  await writeFile(tokenFile, token, { mode: 0o600 });
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const runtime = ManagedRuntime.make(Layer.merge(Platform.layer, NodeServices.layer));
  const scope = await runtime.runPromise(Scope.make());
  const client = new Client({ name: "public-transcript-consumer", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" } });
  t.onTestFinished(async () => {
    await client.close();
    await runtime.runPromise(Scope.close(scope, Exit.void));
    await runtime.dispose();
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await rm(directory, { recursive: true, force: true });
  });
  const owner = await runtime.runPromise(makeAgentSessionServer({ host: AgentHostEnum.pi, tokenFile, endpointFile }, { cwd: directory }).pipe(Effect.provideService(Scope.Scope, scope)));
  const sessionManager = owner.manager;
  const endpoint = Schema.decodeSync(Schema.fromJsonString(Schema.Struct({ url: Schema.String, binding: AgentBinding })))(await readFile(endpointFile, "utf8"));
  await client.connect(new StreamableHTTPClientTransport(new URL(endpoint.url), { requestInit: { headers: { authorization: `Bearer ${token}` } } }));
  sessionManager.appendCustomEntry("optional-data");
  sessionManager.appendCustomEntry("complete-data", { date: new Date("2026-01-02T03:04:05Z"), values: [undefined, "😀"], optional: undefined, nested: { retained: true } });
  sessionManager.appendMessage({ role: "user", content: [{ type: "text", text: "raw public message 😀" }], timestamp: 123 });
  sessionManager.appendMessage({ role: "custom", customType: "raw-details", content: "public custom message", display: false,
    details: { date: new Date("2026-02-03T04:05:06Z"), optional: undefined, retained: ["😀", 42] }, timestamp: 124 });
  const state = await client.callTool({ name: "agent.get_state", arguments: { binding: endpoint.binding } });
  assert.notEqual(state.isError, true);
  const stateWire = Schema.decodeUnknownSync(Schema.Struct({ result: Schema.Struct({ messageCount: Schema.Number, sessionFile: Schema.String }) }))(state.structuredContent);
  assert.equal(stateWire.result.sessionFile, sessionManager.getSessionFile(), "The renamed endpoint reads the same manager used by transcript operations");
  assert.equal(stateWire.result.messageCount, owner.session.messages.length, "State reads the live session, not a reconstructed transcript");
  const entries = await client.callTool({ name: "agent.get_entries", arguments: { binding: endpoint.binding } });
  assert.equal(entries.isError, undefined);
  const entryWire = Schema.decodeUnknownSync(Schema.Struct({ result: Schema.Struct({ entries: Schema.Array(Schema.Struct({ type: Schema.String, customType: Schema.optional(Schema.String), data: Schema.optional(Schema.Json) })) }) }))(entries.structuredContent);
  assert.deepEqual(entryWire.result.entries.find(entry => entry.customType === "complete-data")?.data, { date: "2026-01-02T03:04:05.000Z", values: [null, "😀"], nested: { retained: true } });
  const messages = await client.callTool({ name: "agent.get_messages", arguments: { binding: endpoint.binding } });
  assert.equal(messages.isError, undefined);
  const messageWire = Schema.decodeUnknownSync(Schema.Struct({ result: Schema.Struct({ messages: Schema.Array(Schema.Json) }) }))(messages.structuredContent);
  assert.deepEqual(messageWire.result.messages, [
    { role: "user", content: [{ type: "text", text: "raw public message 😀" }], timestamp: 123 },
    { role: "custom", customType: "raw-details", content: "public custom message", display: false, details: { date: "2026-02-03T04:05:06.000Z", retained: ["😀", 42] }, timestamp: 124 },
  ]);
  const since = sessionManager.getLeafId();
  assert.throws(() => sessionManager.appendCustomEntry("nonserializable", { bigint: 1n }), TypeError);
  const unrepresentable = await client.callTool({ name: "agent.get_entries", arguments: { binding: endpoint.binding, since } });
  const failure = Schema.Struct({ error: AgentError });
  assert.equal(unrepresentable.isError, true);
  assert.equal(Schema.decodeUnknownSync(failure)(unrepresentable.structuredContent).error.code, "agent_failure");
  const oversizedSince = sessionManager.getLeafId();
  sessionManager.appendCustomEntry("oversized", { text: "😀".repeat(17 * 1024 * 1024) });
  const oversized = await client.callTool({ name: "agent.get_entries", arguments: { binding: endpoint.binding, since: oversizedSince } });
  assert.equal(oversized.isError, true);
  assert.equal(Schema.decodeUnknownSync(failure)(oversized.structuredContent).error.code, "agent_failure"); }, 15000);

  const profiles = Object.values(AgentHostEnum);
  for (const profile of profiles) {
    it.live(`shared MCP server rejects invalid DTOs and preserves private UTF-8 state for ${profile}`, () => Effect.gen(function*() {
      const transport = new StdioClientTransport({ command: process.execPath, args, cwd, env: { ...process.env, AGENT_BYTE_LIMIT_SERVER: profile }, maxBufferSize: AgentMcpWireBytes, stderr: "pipe" });
      const client = new Client({ name: "byte-regression", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
      transport.stderr?.on("data", () => {});
      yield* Effect.promise(async () => {
        try {
          await client.connect(transport);
          const invalid = await client.callTool({ name: "agent.get_state", arguments: { unexpected: "valid JSON, invalid native DTO" } });
          assert.equal(invalid.isError, true);
          const failure = Schema.decodeUnknownSync(Schema.toCodecJson(Schema.Struct({ error: AgentError })), { onExcessProperty: "error" })(invalid.structuredContent);
          assert.equal(failure.error.code, "invalid_input");
        } finally { await client.close(); }
      });
      yield* Effect.scoped(Effect.gen(function*() {
        const agent = yield* connectAgentClient({ host: profile, command: process.execPath, args, cwd, env: { ...process.env, AGENT_BYTE_LIMIT_SERVER: profile }, maxBufferSize: AgentMcpWireBytes, connectTimeoutMs: 10000 });
        const state = yield* agent.call(AgentTools.getState, {}, { timeoutMs: 10000 });
        assert.deepEqual(state.systemPrompt, profile === AgentHostEnum.pi ? "😀".repeat(20000) : ["😀".repeat(20000)]);
      }));
    }), { timeout: 20000 });
  }

  it("actual SDK list pages fit the result budget and reject foreign cursors", async (t: TestContext) => { const transport = new StdioClientTransport({ command: process.execPath, args: ["--import", "tsx", path], cwd,
    env: { AGENT_BYTE_LIMIT_SERVER: AgentHostEnum.omp }, stderr: "ignore", maxBufferSize: AgentMcpWireBytes });
  const client = new Client({ name: "agent-page-consumer", version: "1" },
    { capabilities: {}, supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
  t.onTestFinished(() => client.close());
  await client.connect(transport);
  let pages = 0;
  const deliver = transport.onmessage;
  transport.onmessage = (message) => {
    if ("result" in message && typeof message.result === "object" && message.result !== null && "tools" in message.result) {
      assert.ok(Buffer.byteLength(Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(message.result)) <= AgentMcpResultBytes);
      pages++;
    }
    deliver?.(message);
  };
  const { tools } = await client.listTools();
  assert.ok(pages > 1, "full canonical metadata must exercise standard SDK pagination");
  assert.equal(tools.length, 65);
  assert.equal(new Set(tools.map(tool => tool.name)).size, 65);
  await assert.rejects(client.listTools({ cursor: "foreign:0:1" }), error => ProtocolError.isInstance(error) && error.code === -32602);
  await client.close(); }, 15000);

  it("HTTP catalog pagination stays session-bound across transient RPC clients", async (t: TestContext) => { const server = raw(t, true);
  const endpoint = Schema.decodeUnknownSync(Schema.Struct({ url: Schema.String }), { onExcessProperty: "error" })(await server.next());
  const client = new Client({ name: "agent-http-pages", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
  const foreign = new Client({ name: "foreign-native-http-pages", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" }, listMaxPages: 16 });
  t.onTestFinished(async () => { await client.close(); await foreign.close(); });
  const transport = new StreamableHTTPClientTransport(new URL(endpoint.url));
  await client.connect(transport);
  let cursor: unknown;
  let pages = 0;
  const deliver = transport.onmessage;
  transport.onmessage = message => {
    if ("result" in message && typeof message.result === "object" && message.result !== null && "tools" in message.result) {
      const page = Schema.decodeUnknownSync(Schema.Struct({ tools: Schema.Array(Schema.Unknown), nextCursor: Schema.optional(Schema.String) }))(message.result);
      assert.ok(Buffer.byteLength(JSON.stringify(message.result)) <= AgentMcpResultBytes);
      if (page.nextCursor !== undefined) cursor = page.nextCursor;
      pages++;
    }
    deliver?.(message);
  };
  const { tools } = await client.listTools();
  assert.deepEqual(tools.map(tool => tool.name).sort(), Object.values(AgentTools).filter(name => name.hosts.some(host => host === AgentHostEnum.omp)).map(descriptor => descriptor.name).sort());
  assert.ok(pages > 1, "HTTP discovery must cross a real catalog page boundary");
  await foreign.connect(new StreamableHTTPClientTransport(new URL(endpoint.url)));
  await assert.rejects(foreign.listTools({ cursor: Schema.decodeUnknownSync(Schema.String)(cursor) }), error => ProtocolError.isInstance(error) && error.code === -32602);
  await assert.rejects(client.listTools({ cursor: "foreign:0:1" }), error => ProtocolError.isInstance(error) && error.code === -32602); }, 15000);


  const invalid: ReadonlyArray<readonly [string, Buffer]> = [
    ["raw whitespace", Buffer.from(" ".repeat(AgentMcpWireBytes) + "\n")],
    ["non-ASCII bytes", Buffer.from("😀".repeat(AgentMcpWireBytes / 4 + 1))],
    ["long unterminated input", Buffer.from("x".repeat(AgentMcpWireBytes + 1))],
    ["fatal UTF-8", Buffer.from([0xc3, 0x28, 0x0a])],
  ];
  for (const [name, bytes] of invalid) it(`${name} closes the actual SDK source and its physical scope`, async (t: TestContext) => { const server = raw(t);
  await ready(server);
  server.child.stdin.write(bytes);
  await server.exit;
  assert.match(server.stderr(), /agent-byte-cleanup/); }, 15000);

  it("incomplete UTF-8 at EOF closes the source scope", async (t: TestContext) => { const server = raw(t);
  await ready(server);
  server.child.stdin.end(Buffer.from([0xf0, 0x9f]));
  await server.exit;
  assert.match(server.stderr(), /agent-byte-cleanup/); }, 15000);

  it("SDK output with a huge echoed request ID is closed before oversized bytes escape", async (t: TestContext) => { const server = raw(t);
  await ready(server);
  send(server.child, { jsonrpc: "2.0", id: "i".repeat(63000), method: "tools/list", params: {} });
  await server.exit;
  assert.match(server.stderr(), /agent-byte-cleanup/); }, 15000);
}
