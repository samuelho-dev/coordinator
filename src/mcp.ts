import { ByteSize, Cause, Config, Context, Effect, Formatter, Layer, Option, Schema, Stream } from "effect";
import { HttpBody, HttpIncomingMessage, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpServer } from "@effect/platform-node";
import { createServer } from "node:http";
import { McpProtocol, McpSchema, McpServer } from "effect/ai";
import { Client, StreamableHTTPClientTransport, isJSONRPCErrorResponse } from "@modelcontextprotocol/client";
import { ConnectorAccepted, ConnectorAnnounce, ConnectorDisconnect, ConnectorPoll, ConnectorPollResult, ConnectorResult, HttpError, InspectionTools, SessionInspectInput, SessionInspection, SessionListInput, SessionListOutput } from "./inspection.ts";
import type { Inspector } from "./inspector.ts";

export const LIMIT = 64 * 1024;
export const MCP_RESPONSE_LIMIT = 3 * LIMIT;
export const MCP_RESULT_LIMIT = MCP_RESPONSE_LIMIT - 1024;
const protocols: readonly [McpProtocol.ProtocolAdapter, ...McpProtocol.ProtocolAdapter[]] = [McpProtocol.v2026_07_28, McpProtocol.v2025_11_25, McpProtocol.v2025_06_18, McpProtocol.v2025_03_26, McpProtocol.v2024_11_05];
const encoder = new TextEncoder();
const ToolFailure = Schema.Struct({ error: HttpError });
const encodeToolResult = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(McpSchema.CallToolResult)));
const toolResult = (structuredContent: Schema.Json, isError = false) => new McpSchema.CallToolResult({
  isError, content: [McpSchema.TextContent.make({ type: "text", text: Formatter.formatJson(structuredContent, { space: 2 }) })], structuredContent,
});
export const serverLayer = (host: string, port: number) => NodeHttpServer.layerServer(() => createServer({ requestTimeout: 15000 }), { host, port, gracefulShutdownTimeout: "1 second" });
const invalid = () => new HttpError({ status: 400, message: "Invalid inspection request or response" });
const unavailable = () => new HttpError({ status: 502, message: "Inspection transport unavailable" });
const boundaryFailure = <E>(cause: Cause.Cause<E>) => {
  const error = Cause.findErrorOption(cause);
  if (Option.isSome(error)) {
    const decoded = Schema.decodeUnknownOption(HttpError)(error.value);
    if (Option.isSome(decoded)) return new HttpError({ status: decoded.value.status, message: "Inspection request unavailable" });
  }
  return invalid();
};
const endpoint = (url: string, path: string, token: string) => Effect.try({
  try: () => {
    const base = new URL(url);
    if (base.username || base.password || base.search || base.hash || !token || token.length > 505 || /[\r\n]/.test(token)) throw invalid();
    if (base.protocol !== "https:" && !(base.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(base.hostname))) throw invalid();
    const target = new URL(path, base);
    if (target.origin !== base.origin) throw invalid();
    return target;
  }, catch: invalid,
});
// Byte accounting only: native MCP owns JSON and SSE framing, including split UTF-8.
const eventByteGate = (limit: number) => {
  let size = 0;
  let lineBytes = 0;
  let previousCR = false;
  return (chunk: Uint8Array) => {
    for (const byte of chunk) {
      if (++size > limit) return Effect.fail(new HttpError({ status: 413, message: "MCP event exceeds bound" }));
      if (byte === 13 || (byte === 10 && !previousCR)) {
        if (lineBytes === 0) size = 0;
        lineBytes = 0;
      } else if (byte !== 10) lineBytes++;
      previousCR = byte === 13;
    }
    return Effect.succeed(chunk);
  };
};
const byteGate = (limit: number) => {
  let size = 0;
  return (chunk: Uint8Array) => (size += chunk.byteLength) > limit
    ? Effect.fail(new HttpError({ status: 413, message: "Response exceeds bound" })) : Effect.succeed(chunk);
};
const boundedResponse = Effect.fnUntraced(function*(response: HttpServerResponse.HttpServerResponse, limit: number) {
  const body = response.body;
  if (body._tag === "Empty") return response;
  const sse = response.headers["content-type"]?.split(";")[0]?.trim() === "text/event-stream";
  const gate = sse ? eventByteGate(limit) : byteGate(limit);
  if (body._tag === "Uint8Array") { yield* gate(body.body); return response; }
  if (body._tag !== "Stream") return yield* unavailable();
  return HttpServerResponse.setBody(response, HttpBody.stream(body.stream.pipe(Stream.mapEffect(gate)), body.contentType));
});
const resultResponse = <O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(schema: O, value: O["Type"]) => Effect.gen(function*() {
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(Effect.mapError(invalid));
  if (encoder.encode(JSON.stringify(encoded)).byteLength > LIMIT - 1024) return yield* new HttpError({ status: 413, message: "Inspection result exceeds bound" });
  const response = toolResult({ result: encoded });
  if (encoder.encode(encodeToolResult(response)).byteLength > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Inspection result exceeds bound" });
  return response;
});
const registerTool = <I extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, E, R>(
  descriptor: { readonly name: string; readonly description: string; readonly input: I; readonly output: O },
  handle: (input: I["Type"]) => Effect.Effect<O["Type"], E, R>,
) => Layer.effectDiscard(Effect.gen(function*() {
  const services = yield* Effect.context<R>();
  const registry = yield* McpServer.McpServer;
  const input = Schema.toJsonSchemaDocument(Schema.toCodecJson(descriptor.input), { onExcessProperty: "error" });
  const output = Schema.toJsonSchemaDocument(Schema.toCodecJson(Schema.Struct({ result: descriptor.output })));
  const inputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolJson)({ ...input.schema, type: "object", $defs: input.definitions });
  const outputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolOutputJson)({ ...output.schema, $defs: output.definitions });
  yield* registry.addTool({
    tool: new McpSchema.Tool({ name: descriptor.name, description: descriptor.description, inputSchema, outputSchema,
      annotations: McpSchema.ToolAnnotations.make({ readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }) }),
    annotations: Context.empty(),
    handle: (payload: unknown) => Effect.gen(function*() {
      const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(descriptor.input), { onExcessProperty: "error" })(payload).pipe(Effect.mapError(invalid));
      return yield* resultResponse(descriptor.output, yield* handle(decoded).pipe(Effect.provideContext(services)));
    }).pipe(Effect.catchCause((cause) => Effect.succeed(toolResult(Schema.encodeSync(Schema.toCodecJson(ToolFailure))({ error: boundaryFailure(cause) }), true)))),
  });
}));

export class Mcp extends Context.Service<Mcp>()("@samuelho/coordinator/Mcp", {
  make: Effect.gen(function*() {
    const post = <I extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(
      url: string, token: string, path: string, inputSchema: I, outputSchema: O, params: I["Type"], timeoutMs = 10000,
    ) => Effect.scoped(Effect.gen(function*() {
      const target = yield* endpoint(url, path, token);
      const input = yield* Schema.encodeEffect(Schema.toCodecJson(inputSchema))(params).pipe(Effect.mapError(invalid));
      const body = JSON.stringify(input);
      if (encoder.encode(body).byteLength > LIMIT) return yield* new HttpError({ status: 413, message: "Request exceeds bound" });
      const controller = yield* Effect.acquireRelease(Effect.sync(() => new AbortController()), (controller) => Effect.sync(() => controller.abort()));
      const response = yield* Effect.tryPromise({ try: (signal) => fetch(target, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body, redirect: "error", signal: AbortSignal.any([signal, controller.signal]) }), catch: unavailable });
      if (!response.ok) { yield* Effect.promise(() => response.body?.cancel() ?? Promise.resolve()); return yield* new HttpError({ status: Math.min(599, Math.max(400, response.status)), message: "Inspection request rejected" }); }
      if (!response.body) return yield* unavailable();
      const bytes = yield* Stream.fromReadableStream({ evaluate: () => response.body!, onError: unavailable }).pipe(Stream.mapEffect(byteGate(MCP_RESPONSE_LIMIT)), Stream.runCollect);
      const text = new TextDecoder().decode(Buffer.concat(bytes));
      return yield* Schema.decodeEffect(Schema.fromJsonString(Schema.toCodecJson(outputSchema)), { onExcessProperty: "error" })(text).pipe(Effect.mapError(invalid));
    })).pipe(Effect.timeout(timeoutMs));

    const call = <I extends Schema.Top & Schema.ConstraintCodec<unknown, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(
      url: string, token: string, descriptor: { readonly name: string; readonly input: I; readonly output: O }, params: I["Type"], timeoutMs = 10000,
    ) => Effect.scoped(Effect.gen(function*() {
      const target = yield* endpoint(url, "/mcp", token);
      if (descriptor.name !== InspectionTools.list.name && descriptor.name !== InspectionTools.inspect.name) return yield* invalid();
      let failure: HttpError | undefined;
      const controller = yield* Effect.acquireRelease(Effect.sync(() => new AbortController()), (controller) => Effect.sync(() => controller.abort()));
      const sdkFetch: typeof fetch = async (input, init) => {
        if (failure) throw failure;
        const request = new Request(input, init);
        if (new URL(request.url).origin !== target.origin) { failure = invalid(); throw failure; }
        const body = request.body ? await request.arrayBuffer() : undefined;
        if (body && body.byteLength > LIMIT) { failure = new HttpError({ status: 413, message: "Request exceeds bound" }); throw failure; }
        return Effect.runPromise(Effect.gen(function*() {
          const response = yield* Effect.tryPromise({ try: (signal) => fetch(request.url, { method: request.method, headers: request.headers, ...(body ? { body } : {}), redirect: "error", signal: AbortSignal.any([request.signal, signal, controller.signal]) }), catch: unavailable });
          if (!response.ok) { yield* Effect.promise(() => response.body?.cancel() ?? Promise.resolve()); return yield* new HttpError({ status: Math.min(599, Math.max(400, response.status)), message: "MCP request rejected" }); }
          if (!response.body) return response;
          const gate = response.headers.get("content-type")?.split(";")[0]?.trim() === "text/event-stream" ? eventByteGate(MCP_RESPONSE_LIMIT) : byteGate(512 * 1024);
          const stream = Stream.fromReadableStream({ evaluate: () => response.body!, onError: unavailable }).pipe(Stream.mapEffect(gate), Stream.tapError((error) => Effect.sync(() => { failure = error; controller.abort(); })));
          return new Response(Stream.toReadableStream(stream), { status: response.status, headers: response.headers });
        }).pipe(Effect.tapError((error) => Effect.sync(() => { failure = error; controller.abort(); }))));
      };
      const client = yield* Effect.acquireRelease(Effect.sync(() => new Client({ name: "coordinator-inspector", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } })), (client) => Effect.promise(() => client.close()));
      const transport = new StreamableHTTPClientTransport(target, { authProvider: { token: async () => token }, fetch: sdkFetch, requestInit: { redirect: "error" } });
      const send = transport.send.bind(transport);
      let observedMessage: typeof transport.onmessage;
      // Observe native decoded errors before the SDK can attempt a corrective POST.
      transport.send = (message, options) => {
        if (transport.onmessage !== observedMessage) {
          const onmessage = transport.onmessage;
          observedMessage = (reply) => {
            if (isJSONRPCErrorResponse(reply)) failure ??= unavailable();
            onmessage?.(reply);
          };
          transport.onmessage = observedMessage;
        }
        return send(message, options);
      };
      yield* Effect.addFinalizer(() => Effect.promise(() => transport.close()));
      client.onerror = () => { failure ??= unavailable(); controller.abort(); void client.close(); };
      yield* Effect.tryPromise({ try: () => client.connect(transport), catch: () => failure ?? unavailable() });
      const args = yield* Schema.encodeEffect(descriptor.input)(params).pipe(Effect.mapError(invalid));
      const result = yield* Effect.tryPromise({ try: (signal) => client.callTool({ name: descriptor.name, arguments: args }, { signal, timeout: timeoutMs }), catch: () => failure ?? unavailable() });
      if (encoder.encode(JSON.stringify(result)).byteLength > MCP_RESPONSE_LIMIT) return yield* new HttpError({ status: 413, message: "MCP result exceeds bound" });
      if (result.isError) {
        const error = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolFailure), { onExcessProperty: "error" })(result.structuredContent).pipe(Effect.mapError(invalid));
        return yield* error.error;
      }
      const output = yield* Schema.decodeUnknownEffect(Schema.Struct({ result: Schema.Unknown }), { onExcessProperty: "error" })(result.structuredContent).pipe(Effect.mapError(invalid));
      return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(descriptor.output), { onExcessProperty: "error" })(output.result).pipe(Effect.mapError(invalid));
    })).pipe(Effect.timeout(timeoutMs));

    const serve = (server: HttpServer.HttpServer["Service"], inspector: Inspector["Service"]) => Effect.gen(function*() {
      const sha = yield* Config.option(Config.String("COORDINATOR_IMAGE_SHA"));
      const readinessHeaders = { "cache-control": "no-store", ...(Option.isSome(sha) ? { "x-coordinator-source-sha": sha.value } : {}) };
      const authorize = Effect.gen(function*() {
        const current = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
        if (Option.isNone(current)) return yield* new HttpError({ status: 401, message: "Request context required" });
        const request = current.value;
        if (request.headers.origin !== undefined) return yield* new HttpError({ status: 403, message: "Origin prohibited" });
        const auth = request.headers.authorization;
        if (!auth?.startsWith("Bearer ") || auth.length > 512) return yield* new HttpError({ status: 401, message: "Credential required" });
        return yield* inspector.authorize(auth.slice(7));
      });
      const protocol = McpServer.layerHttp({ name: "coordinator-inspector", version: "1.0.0", path: "/mcp", protocols, allowedOrigins: [] });
      const tools = Layer.merge(registerTool(InspectionTools.list, (input) => authorize.pipe(Effect.flatMap((principal) => inspector.list(principal, input)))), registerTool(InspectionTools.inspect, (input) => authorize.pipe(Effect.flatMap((principal) => inspector.inspect(principal, input))))).pipe(Layer.provide(protocol));
      const app = yield* HttpRouter.toHttpEffect(Layer.merge(protocol, tools));
      yield* server.serve(Effect.gen(function*() {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.method === "GET" && request.url === "/livez") return HttpServerResponse.jsonUnsafe({ status: "ok" }, { headers: { "cache-control": "no-store" } });
        const principal = yield* authorize;
        if (request.method === "GET" && (request.url === "/readyz" || request.url === "/healthz")) return HttpServerResponse.jsonUnsafe({ status: "ok" }, { headers: readinessHeaders });
        if (Number(request.headers["content-length"] ?? 0) > LIMIT) return yield* new HttpError({ status: 413, message: "Request exceeds bound" });
        const body: unknown = request.method === "POST" ? yield* request.json.pipe(Effect.interruptible, Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(LIMIT)), Effect.mapError(invalid)) : undefined;
        const decode = <S extends Schema.Top>(schema: S) => Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(body).pipe(Effect.mapError(invalid));
        if (request.method === "POST") {
          switch (request.url) {
            case "/connector/announce": return yield* HttpServerResponse.schemaJson(ConnectorAccepted)(yield* inspector.announce(principal, yield* decode(ConnectorAnnounce)));
            case "/connector/poll": return yield* HttpServerResponse.schemaJson(ConnectorPollResult)(yield* inspector.poll(principal, yield* decode(ConnectorPoll)));
            case "/connector/result": return yield* HttpServerResponse.schemaJson(ConnectorAccepted)(yield* inspector.complete(principal, yield* decode(ConnectorResult)));
            case "/connector/disconnect": return yield* HttpServerResponse.schemaJson(ConnectorAccepted)(yield* inspector.disconnect(principal, yield* decode(ConnectorDisconnect)));
          }
        }
        return yield* app;
      }).pipe(Effect.flatMap((response) => boundedResponse(response, 512 * 1024)), Effect.catchCause((cause) => {
        const error = boundaryFailure(cause);
        return Effect.succeed(HttpServerResponse.jsonUnsafe({ error: error.message }, { status: error.status, headers: readinessHeaders }));
      })));
    });
    const serveStdio = <LE, LR, IE, IR>(reader: {
      readonly list: (input: SessionListInput) => Effect.Effect<SessionListOutput, LE, LR>;
      readonly inspect: (input: SessionInspectInput) => Effect.Effect<SessionInspection, IE, IR>;
    }) => {
      const protocol = McpServer.layerStdio({ name: "coordinator-inspector", version: "1.0.0", protocols });
      return Layer.launch(Layer.merge(protocol, Layer.merge(registerTool(InspectionTools.list, reader.list), registerTool(InspectionTools.inspect, reader.inspect)).pipe(Layer.provide(protocol))));
    };
    return { call, post, serve, serveStdio };
  }),
}) {
  static readonly layer = Layer.effect(this, this.make);
}
