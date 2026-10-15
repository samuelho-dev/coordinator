import assert from "node:assert/strict";
import { createServer } from "node:http";
import { it } from "@effect/vitest";
import { Context, Crypto, Effect, Layer, Schema, Scope } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { NodeCrypto, NodeHttpServer } from "@effect/platform-node";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { McpProtocol, McpSchema, McpServer, Tool } from "effect/ai";
import { AgentHostEnum, AgentBinding, AgentMcpProtocolVersion } from "../src/agent/contracts.ts";
import { createAgentPresentation, AgentPresentationPage, agentPresentationUri, withAgentMcpRequest } from "../src/agent/interaction.ts";

const WriteInput = Schema.Struct({ values: Schema.Array(Schema.String), rotate: Schema.optional(Schema.Boolean) });

it.live("private presentation replays ordered immutable snapshots through standard MCP and exposes eviction gaps", () => Effect.scoped(Effect.gen(function*() {
  let binding = Schema.decodeSync(AgentBinding)({ host: AgentHostEnum.pi, sessionId: "presentation-test", incarnation: "one", generation: 0 });
  const session = { get agentBinding() { return binding; } };
  const presentation = createAgentPresentation(() => session);
  const agent = McpServer.layerHttp({ name: "presentation-resource-proof", version: "1", path: "/mcp", protocols: [McpProtocol.v2025_11_25], allowedOrigins: [] });
  const registration = Layer.effectDiscard(Effect.gen(function*() {
    const registry = yield* McpServer.McpServer;
    const crypto = yield* Crypto.Crypto;
    const registrationScope = yield* Effect.scope;
    yield* presentation.register(binding);
    yield* registry.addTool({
      tool: new McpSchema.Tool({ name: "presentation.write", inputSchema: Schema.decodeUnknownSync(McpSchema.ToolJson)(Tool.getJsonSchemaFromSchema(Schema.toCodecJson(WriteInput))) }),
      annotations: Context.empty(),
      handle: (payload: unknown) => Effect.gen(function*() {
        const invocation = yield* Effect.context<McpSchema.McpRequestContext>();
        const client = Context.getOrUndefined(invocation, McpSchema.McpServerClient);
        if (client === undefined) return yield* new McpSchema.InternalError({ message: "Presentation write requires an initialized MCP client" });
        const input = yield* Schema.decodeUnknownEffect(WriteInput)(payload, { onExcessProperty: "error" }).pipe(Effect.mapError(() => new McpSchema.InvalidParams({ message: "Invalid presentation write" })));
        if (input.rotate) {
          binding = yield* Schema.decodeEffect(AgentBinding)({ ...binding, generation: binding.generation + 1 }).pipe(Effect.mapError(() => new McpSchema.InvalidParams({ message: "Invalid generation" })));
          yield* presentation.register(binding).pipe(Effect.provideService(McpServer.McpServer, registry), Effect.provideService(Scope.Scope, registrationScope));
        }
        yield* withAgentMcpRequest(async () => {
          for (const text of input.values) {
            await Promise.resolve();
            presentation.write("observation/text_delta", { text });
          }
          await presentation.flush();
        }).pipe(
          Effect.provideService(McpSchema.McpServerClient, client),
          Effect.provideService(McpServer.McpServer, registry),
          Effect.provideService(Crypto.Crypto, crypto),
        );
        return new McpSchema.CallToolResult({ content: [] });
      }).pipe(Effect.mapError(() => new McpSchema.InternalError({ message: "Presentation write failed" }))),
    });
  }));
  const app = yield* HttpRouter.toHttpEffect(registration.pipe(Layer.provideMerge(agent)));
  const server = yield* HttpServer.HttpServer;
  yield* server.serve(app);
  const endpoint = new URL("/mcp", HttpServer.formatAddress(server.address));
  yield* Effect.promise(async () => {
    const owner = new Client({ name: "presentation-owner", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" } });
    const stranger = new Client({ name: "presentation-stranger", version: "1" }, { supportedProtocolVersions: [AgentMcpProtocolVersion], versionNegotiation: { mode: "legacy" } });
    try {
      await owner.connect(new StreamableHTTPClientTransport(endpoint));
      await stranger.connect(new StreamableHTTPClientTransport(endpoint));
      assert.equal(owner.getNegotiatedProtocolVersion(), AgentMcpProtocolVersion);
      assert.equal(stranger.getNegotiatedProtocolVersion(), AgentMcpProtocolVersion);
      assert.throws(() => presentation.write("observation/text_delta", { text: "idle-local" }));
      const read = (uri: string, caller = owner) => caller.readResource({ uri }, { cacheMode: "bypass" });
      const page = async (uri: string, caller = owner) => {
        const result = await read(uri, caller);
        const content = result.contents[0];
        assert.ok(content && "text" in content);
        return Schema.decodeSync(Schema.fromJsonString(AgentPresentationPage))(content.text, { onExcessProperty: "error" });
      };
      const write = async (values: readonly string[], rotate = false) => {
        const result = await owner.callTool({ name: "presentation.write", arguments: { values: [...values], rotate } });
        assert.notEqual(result.isError, true);
      };
      const snapshot = async (uri: string) => {
        const result = await read(uri);
        const content = result.contents[0];
        assert.ok(content && "blob" in content);
        return Schema.decodeSync(Schema.fromJsonString(Schema.Struct({ text: Schema.String })))(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.from(content.blob, "base64")), { onExcessProperty: "error" });
      };
      const uri = agentPresentationUri(binding);
      assert.ok((await owner.listResources()).resources.some(entry => entry.uri === uri));
      const templates = (await owner.listResourceTemplates()).resourceTemplates.map(entry => entry.uriTemplate);
      assert.ok(templates.includes(`${uri}{?after,limit}`));
      assert.ok(templates.includes(`${uri}/snapshots/{snapshot}{?offset,limit}`));
      assert.equal((await page(uri)).highWater, 0);
      await write(["a", "b", "c"]);
      const first = await page(`${uri}?after=0&limit=2`);
      assert.deepEqual(first.entries.map(entry => entry.seq), [1, 2]);
      assert.equal(first.more, true);
      const second = await page(`${uri}?after=${first.nextAfter}`);
      assert.deepEqual(second.entries.map(entry => entry.seq), [3]);
      const decoded: string[] = [];
      for (const entry of [...first.entries, ...second.entries]) decoded.push((await snapshot(entry.snapshot.uri)).text);
      assert.deepEqual(decoded, ["a", "b", "c"]);
      const originalEntry = first.entries[0];
      assert.ok(originalEntry);
      const original = originalEntry.snapshot.uri;
      await write(["d"]);
      assert.deepEqual(await snapshot(original), { text: "a" }, "A later delta must not rewrite retained snapshot bytes");
      const firstChunk = (await read(`${original}?offset=0&limit=1`)).contents[0];
      const remainingChunk = (await read(`${original}?offset=1&limit=32768`)).contents[0];
      assert.ok(firstChunk && "blob" in firstChunk && remainingChunk && "blob" in remainingChunk);
      const immutableBytes = Buffer.concat([Buffer.from(firstChunk.blob, "base64"), Buffer.from(remainingChunk.blob, "base64")]);
      assert.equal(immutableBytes.byteLength, originalEntry.snapshot.byteLength);
      assert.equal(new TextDecoder("utf-8", { fatal: true }).decode(immutableBytes), '{"text":"a"}');
      await assert.rejects(read(original, stranger));
      assert.equal((await page(uri, stranger)).entries.length, 0);
      await assert.rejects(read(`${original}?offset=0&offset=1`));
      await assert.rejects(read(original.replace("/snapshots/", "/snapshots/../snapshots/")));
      await assert.rejects(page(`${uri}?after=0&after=1`));
      await write([], true);
      const nextUri = agentPresentationUri(binding);
      await assert.rejects(read(original.replace(uri, nextUri)));
      assert.equal((await page(nextUri)).highWater, 0);
      await write(Array.from({ length: 65 }, (_, index) => `delta-${index}`));
      const retained = await page(nextUri);
      assert.deepEqual(retained.gap, { from: 1, through: 1 });
      assert.equal(retained.highWater, 65);
      assert.deepEqual(retained.entries.map(entry => entry.seq), Array.from({ length: 64 }, (_, index) => index + 2));
      assert.equal((await page(nextUri, stranger)).highWater, 0);
      await assert.rejects(read(original));
    } finally {
      await Promise.all([owner.close(), stranger.close()]);
    }
  });
})).pipe(Effect.provide(Layer.merge(NodeHttpServer.layerServer(() => createServer(), { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" }), NodeCrypto.layer))));
