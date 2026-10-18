import { ByteSize, Cause, Config, Context, Effect, Exit, Formatter, Layer, Option, Schema, Scope, Stream } from "effect";
import { HttpBody, HttpClient, HttpClientError, HttpClientRequest, HttpIncomingMessage, HttpRouter, HttpServer, HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { createServer } from "node:http";
import { lookup } from "node:dns";
import type { LookupFunction } from "node:net";
import { McpProtocol, McpSchema, McpServer } from "effect/ai";
import { Client, ProtocolError, SdkError, SdkErrorCode, StreamableHTTPClientTransport, isJSONRPCErrorResponse, parseJSONRPCMessage } from "@modelcontextprotocol/client";
import { BrokerRequest, RunnerRequest, ConnectHosts, HttpError, CommandError, BoundaryError, Json, Commands, McpResult, type CommandDescriptor, type CommandOutput, type McpEndpoint } from "./contracts.ts";

export const LIMIT = 64 * 1024;
export const MCP_RESPONSE_LIMIT = 3 * LIMIT;
export const MCP_RESULT_LIMIT = MCP_RESPONSE_LIMIT - 1024;
const commandProtocol = McpProtocol.v2026_07_28;

const encodeToolResult = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(McpSchema.CallToolResult)));
const encodeStructuredResult = Schema.encodeSync(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" });
const toolResult = (structuredContent: Schema.Json, isError = false) =>
  new McpSchema.CallToolResult({ isError, content: [McpSchema.TextContent.make({ type: "text", text: Formatter.formatJson(structuredContent, { space: 2 }) })], structuredContent });
export const mcpResultText = (structuredContent: Schema.Json) => encodeToolResult(toolResult(structuredContent));

const clientLayer = Layer.unwrap(Effect.gen(function*() {
  const configuredHosts = yield* Config.String("COORDINATOR_CONNECT_HOSTS_JSON").pipe(Config.withDefault("{}"));
  const hosts = yield* Schema.decodeEffect(Schema.fromJsonString(ConnectHosts), { onExcessProperty: "error" })(configuredHosts);
  // The Node agent requires a DNS callback; Effect has no DNS service. The socket override leaves TLS hostname verification intact.
  const connectLookup: LookupFunction = (hostname, options, callback) => lookup(hosts[hostname] ?? hostname, options, callback);
  return NodeHttpClient.layerNodeHttpNoAgent.pipe(Layer.provide(NodeHttpClient.layerAgentOptions({ lookup: connectLookup })));
}));

// NodeHttpServer explicitly requires this native server factory; all serving/lifecycle is the Effect adapter.
export const serverLayer = (host: string, port: number) => NodeHttpServer.layerServer(() => createServer({ requestTimeout: 15000 }), { host, port, gracefulShutdownTimeout: "1 second" });

const ToolFailure = Schema.Struct({ error: HttpError });
const boundaryFailure = <E>(cause: Cause.Cause<E>) => {
  const error = Cause.squash(cause);
  return new HttpError({
    status: error instanceof HttpError ? error.status : 400,
    message: error instanceof HttpError ? error.message.slice(0, 512) : "Invalid request or unavailable operation",
    ...(error instanceof HttpError ? {
      ...(error.retryAfter === undefined ? {} : { retryAfter: error.retryAfter.slice(0, 512) }),
      ...(error.wwwAuthenticate === undefined ? {} : { wwwAuthenticate: error.wwwAuthenticate.slice(0, 512) }),
      ...(error.protocolCode === undefined ? {} : { protocolCode: error.protocolCode }),
    } : {}),
  });
};
// Byte-only SSE resource guard; the native transport still owns framing/JSON.
// Reset at blank lines, not at response EOF: legitimate subscriptions are unbounded in duration.
const eventByteGate = (limit: number) => {
  let size = 0;
  let lineBytes = 0;
  let previousCR = false;
  return (chunk: Uint8Array) => Effect.try({
    try: () => {
      for (const byte of chunk) {
        if (++size > limit) throw new HttpError({ status: 413, message: "MCP event exceeds bound" });
        if (byte === 13 || (byte === 10 && !previousCR)) {
          if (lineBytes === 0) size = 0;
          lineBytes = 0;
        } else if (byte !== 10) lineBytes++;
        previousCR = byte === 13;
      }
      return chunk;
    },
    catch: (error) => error instanceof HttpError ? error : new HttpError({ status: 500, message: "MCP response unavailable" }),
  });
};
const boundedResponse = Effect.fnUntraced(function*(response: HttpServerResponse.HttpServerResponse, limit: number) {
  const body = response.body;
  if (body._tag === "Empty") return response;
  const sse = response.headers["content-type"]?.split(";")[0]?.trim() === "text/event-stream";
  if (body._tag === "Uint8Array") {
    if (sse) yield* eventByteGate(limit)(body.body);
    else if (body.contentLength > limit) return yield* new HttpError({ status: 413, message: "MCP response exceeds bound" });
    return sse ? HttpServerResponse.setHeader(response, "x-accel-buffering", "no") : response;
  }
  if (body._tag !== "Stream") return yield* new HttpError({ status: 500, message: "Unsupported MCP response body" });
  let size = 0;
  const gate = sse ? eventByteGate(limit) : (chunk: Uint8Array) => {
    size += chunk.byteLength;
    return size > limit ? Effect.fail(new HttpError({ status: 413, message: "MCP response exceeds bound" })) : Effect.succeed(chunk);
  };
  const streamed = HttpServerResponse.setBody(response, HttpBody.stream(body.stream.pipe(Stream.mapEffect(gate)), body.contentType));
  return sse ? HttpServerResponse.setHeader(streamed, "x-accel-buffering", "no") : streamed;
});

// fromWeb uses Raw for Web streams; NodeHttpClient needs an Effect byte stream.
function fromWebRequest(request: Request): HttpClientRequest.HttpClientRequest {
  const outbound = HttpClientRequest.fromWeb(request);
  if (request.body === null || outbound.body._tag !== "Raw") return outbound;
  const body = outbound.body;
  const stream = Stream.suspend(() => {
    let size = 0;
    return Stream.fromReadableStream({ evaluate: () => request.body!, onError: () => new HttpError({ status: 400, message: "Request body unavailable" }) }).pipe(
      Stream.mapEffect((chunk) => {
        size += chunk.byteLength;
        return size > LIMIT ? Effect.fail(new HttpError({ status: 413, message: "Request too large" })) : Effect.succeed(chunk);
      }),
    );
  });
  const adapted = HttpClientRequest.setBody(outbound, HttpBody.stream(stream, body.contentType, body.contentLength));
  return request.headers.has("content-type") ? adapted : HttpClientRequest.removeHeader(adapted, "content-type");
}

export class Mcp extends Context.Service<Mcp>()("@samuelho/coordinator/Mcp", {
  make: Effect.gen(function*() {
    const http = yield* HttpClient.HttpClient;
    // Each call owns its SDK connection: bearer contexts never share state or an authenticated session.
    const call = <I extends Schema.ConstraintCodec<unknown, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, S extends Schema.Top & Schema.ConstraintCodec<{ readonly result: O["Type"] }, unknown>>(url: string, token: string, descriptor: CommandDescriptor<I, O> & { readonly structuredOutput: S; readonly target: McpEndpoint<never, never, never, never>["target"] | typeof Commands.health.target }, params: I["Type"], timeoutMs = 10000): Effect.Effect<O["Type"], HttpError | CommandError | Cause.TimeoutError> => Effect.scoped(Effect.gen(function*() {
      const callScope = yield* Scope.Scope;
      const input = params;
      const endpoint = yield* Effect.try({ try: () => new URL("/mcp", url), catch: () => new HttpError({ status: 400, message: "Invalid MCP endpoint" }) });
      if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) return yield* new HttpError({ status: 400, message: "TLS required except loopback" });
      // Small call-local evidence survives native Promise/SDK wrapping; never infer network failure from text.
      let httpFailure: HttpError | undefined;
      let commandFailure: CommandError | undefined;
      let responseMetadata: { status: number; retryAfter?: string; wwwAuthenticate?: string; protocolCode?: number } | undefined;
      const captureTransportFailure = (error: unknown, responseStream = false) => {
        if (error instanceof HttpError) httpFailure = error;
        else if (HttpClientError.isHttpClientError(error)) {
          const cause = "cause" in error.reason ? error.reason.cause : undefined;
          if (cause instanceof HttpError) httpFailure = cause;
          else if (error.reason._tag === "TransportError" || (responseStream && error.reason._tag === "DecodeError")) {
            commandFailure = new CommandError({ reason: "transport", message: "MCP transport unavailable" });
          }
        }
        return error;
      };
      const sdkFailure = (error: unknown) => {
        let protocolCode: number | undefined;
        if (error instanceof ProtocolError) protocolCode = error.code;
        else protocolCode = responseMetadata?.protocolCode;
        if (httpFailure) return httpFailure;
        if (responseMetadata && (responseMetadata.status < 200 || responseMetadata.status >= 300 || protocolCode !== undefined)) {
          return new HttpError({ ...responseMetadata, message: protocolCode === undefined ? "MCP request rejected" : "MCP protocol rejected request", ...(protocolCode === undefined ? {} : { protocolCode }) });
        }
        if (commandFailure) return commandFailure;
        if (error instanceof HttpError || error instanceof CommandError) return error;
        if (error instanceof SyntaxError || (error instanceof SdkError && [
          SdkErrorCode.InvalidResult, SdkErrorCode.UnsupportedResultType, SdkErrorCode.CapabilityNotSupported,
          SdkErrorCode.MethodNotSupportedByProtocolVersion, SdkErrorCode.ClientHttpUnexpectedContent,
        ].includes(error.code))) return new CommandError({ reason: "protocol", message: "Invalid or incompatible MCP protocol" });
        return new CommandError({ message: "MCP operation unavailable" });
      };
      return yield* Effect.gen(function*() {
      const sdkFetch: typeof globalThis.fetch = async (input, init) => {
        const request = new Request(input, init);
        if (new URL(request.url).origin !== endpoint.origin) {
          httpFailure = new HttpError({ status: 400, message: "Cross-origin MCP request prohibited" });
          throw httpFailure;
        }
        // A pinned/static-credential call cannot correct a rejection; prohibit SDK corrective POST replay too.
        if (request.method === "POST" && (httpFailure || commandFailure || (responseMetadata && (responseMetadata.status < 200 || responseMetadata.status >= 300 || responseMetadata.protocolCode !== undefined)))) {
          throw sdkFailure(new CommandError({ message: "MCP request already failed" }));
        }
        return Effect.runPromise(Effect.gen(function*() {
          const responseScope = yield* Scope.fork(callScope);
          const close = Scope.close(responseScope, Exit.void);
          const abort = () => { void Effect.runPromise(close); };
          request.signal.addEventListener("abort", abort, { once: true });
          yield* Scope.addFinalizer(responseScope, Effect.sync(() => request.signal.removeEventListener("abort", abort)));
          return yield* Effect.gen(function*() {
          let outbound = fromWebRequest(request);
          let body = "";
          const decoder = new TextDecoder();
          if (outbound.body._tag === "Stream") {
            const streamed = outbound.body;
            outbound = HttpClientRequest.setBody(outbound, HttpBody.stream(streamed.stream.pipe(
              Stream.tap((chunk) => Effect.sync(() => { body += decoder.decode(chunk, { stream: true }); })),
            ), streamed.contentType, streamed.contentLength));
            if (!request.headers.has("content-type")) outbound = HttpClientRequest.removeHeader(outbound, "content-type");
          }
          const response = yield* HttpClient.withScope(http).execute(outbound).pipe(Effect.mapError((error) => {
            captureTransportFailure(error);
            return httpFailure ?? commandFailure ?? error;
          }));
          body += decoder.decode();
          // Discovery contains the full catalog; command responses include structured JSON and readable text.
          const method = body ? (yield* Schema.decodeEffect(Schema.fromJsonString(Json), { onExcessProperty: "error" })(body)).method : undefined;
          const discovery = method === "server/discover" || method === "tools/list";
          const rejected = response.status < 200 || response.status >= 300;
          const limit = rejected ? LIMIT : discovery ? 512 * 1024 : MCP_RESPONSE_LIMIT;
          responseMetadata = {
            status: response.status,
            ...(response.headers["retry-after"] === undefined ? {} : { retryAfter: response.headers["retry-after"].slice(0, 512) }),
            ...(response.headers["www-authenticate"] === undefined ? {} : { wwwAuthenticate: response.headers["www-authenticate"].slice(0, 512) }),
          };
          let size = 0;
          let rejectedBody = "";
          const rejectedDecoder = rejected ? new TextDecoder() : undefined;
          const gate = !rejected && response.headers["content-type"]?.split(";")[0]?.trim() === "text/event-stream"
            ? eventByteGate(limit)
            : (chunk: Uint8Array) => {
              size += chunk.byteLength;
              return size > limit ? Effect.fail(new HttpError({ ...(rejected ? responseMetadata! : { status: 413 }), message: "HTTP body exceeds bound" })) : Effect.succeed(chunk);
            };
          const reader = Stream.toReadableStream(response.stream.pipe(
            Stream.mapEffect(gate),
            Stream.tap((chunk) => Effect.sync(() => {
              if (rejectedDecoder) rejectedBody += rejectedDecoder.decode(chunk, { stream: true });
            })),
            Stream.mapError((error) => {
              captureTransportFailure(error, true);
              return error;
            }),
          ), { strategy: { highWaterMark: 0 } }).getReader();
          yield* Scope.addFinalizer(responseScope, Effect.promise(() => reader.cancel()));
          const readable = new ReadableStream<Uint8Array>({
            async pull(controller) {
              try {
                const next = await reader.read();
                if (next.done) {
                  if (rejectedDecoder) {
                    rejectedBody += rejectedDecoder.decode();
                    const decoded = Effect.runSync(Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(rejectedBody).pipe(Effect.flatMap((value) => Effect.try(() => parseJSONRPCMessage(value))), Effect.option));
                    if (Option.isSome(decoded) && isJSONRPCErrorResponse(decoded.value)) responseMetadata = { ...responseMetadata!, protocolCode: decoded.value.error.code };
                  }
                  controller.close();
                  await Effect.runPromise(close);
                } else controller.enqueue(next.value);
              } catch (error) {
                if (error instanceof HttpError) httpFailure = error;
                controller.error(error);
                await Effect.runPromise(close);
              }
            },
            cancel: () => Effect.runPromise(close),
          }, { highWaterMark: 0 });
          return new Response(readable, { status: response.status, headers: response.headers });
          }).pipe(
            Scope.provide(responseScope),
            Effect.onExit((exit) => exit._tag === "Failure" ? close : Effect.void),
          );
        }).pipe(Effect.catchTag("HttpError", (error) => {
          httpFailure = error;
          return Effect.fail(error);
        })), { signal: request.signal });
      };
      const client = yield* Effect.acquireRelease(
        Effect.sync(() => new Client({ name: "coordinator", version: "0.1.0" }, { versionNegotiation: { mode: { pin: commandProtocol.protocolVersion } } })),
        (client) => Effect.tryPromise({ try: () => client.close(), catch: () => new CommandError({ message: "MCP connection cleanup failed" }) }).pipe(Effect.orDie),
      );
      client.onerror = (error) => {
        if (error instanceof SyntaxError) commandFailure = new CommandError({ reason: "protocol", message: "Invalid or incompatible MCP protocol" });
        else commandFailure ??= new CommandError({ message: "MCP operation unavailable" });
        // A failed native stream/parser cannot produce a valid final reply; close, don't wait for its deadline.
        if (commandFailure || httpFailure) void client.close();
      };
      yield* Effect.tryPromise({
        try: (signal) => {
          const transport = new StreamableHTTPClientTransport(endpoint, {
            authProvider: { token: async () => token },
            fetch: (input, init) => sdkFetch(input, { ...init, signal: init?.signal ? AbortSignal.any([signal, init.signal]) : signal }),
            requestInit: { redirect: "error" },
          });
          Effect.runSync(Scope.addFinalizer(callScope, Effect.promise(() => transport.close())));
          const send = transport.send.bind(transport);
          let observedMessage: typeof transport.onmessage;
          let observedError: typeof transport.onerror;
          // ProbeWindow and Protocol replace handlers on handover. Observe their native decoded evidence.
          transport.send = (message, options) => {
            if (transport.onmessage !== observedMessage) {
              const onmessage = transport.onmessage;
              observedMessage = (reply) => {
                if (isJSONRPCErrorResponse(reply) && responseMetadata) responseMetadata = { ...responseMetadata, protocolCode: reply.error.code };
                onmessage?.(reply);
              };
              transport.onmessage = observedMessage;
            }
            if (transport.onerror !== observedError) {
              const onerror = transport.onerror;
              observedError = (error) => {
                if (error instanceof SyntaxError) commandFailure = new CommandError({ reason: "protocol", message: "Invalid or incompatible MCP protocol" });
                else commandFailure ??= new CommandError({ message: "MCP operation unavailable" });
                void transport.close();
                onerror?.(error);
              };
              transport.onerror = observedError;
            }
            return send(message, options);
          };
          return client.connect(transport);
        },
        catch: sdkFailure,
      });
      const arguments_ = yield* Schema.encodeEffect<I>(descriptor.input, { onExcessProperty: "error" })(input).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
      const result = yield* Effect.tryPromise({ try: (signal) => client.callTool({ name: descriptor.name, arguments: arguments_ }, { signal, timeout: timeoutMs }), catch: sdkFailure });
      if (result.isError) {
        const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolFailure), { onExcessProperty: "error" })(result.structuredContent).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
        return yield* decoded.error;
      }
      const output = yield* Schema.decodeUnknownEffect<Schema.toCodecJson<S>>(Schema.toCodecJson(descriptor.structuredOutput), { onExcessProperty: "error" })(result.structuredContent).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
      return output.result;
      }).pipe(
        Effect.timeout(timeoutMs),
        Effect.catchTag("TimeoutError", (error) => Effect.fail(
          responseMetadata && (responseMetadata.status < 200 || responseMetadata.status >= 300)
            ? sdkFailure(error)
            : commandFailure ?? error,
        )),
      );
    }));

    const serve = <AE, HE, AR = never, HR = never>(server: HttpServer.HttpServer["Service"], options: McpEndpoint<AE, AR, HE, HR>, probes?: {
      readonly live: Effect.Effect<void, HttpError>;
      readonly ready: Effect.Effect<void, HttpError>;
    }) => Effect.gen(function*() {
      const services = yield* Effect.context<AR | HR>();
      const sourceSha = probes ? Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_IMAGE_SHA"))) : undefined;
      const readinessHeaders = { "cache-control": "no-store", ...(sourceSha ? { "x-coordinator-source-sha": sourceSha } : {}) };
      const authorize = Effect.gen(function*() {
        const current = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
        if (Option.isNone(current)) return yield* new HttpError({ status: 401, message: "Request context required" });
        const request = current.value;
        const auth = request.headers.authorization;
        if (!auth?.startsWith("Bearer ") || auth.length > 512) return yield* new HttpError({ status: 401, message: "Credential required" });
        if (options.target === "broker") yield* options.authorize(auth.slice(7)).pipe(Effect.provideContext(services));
        else yield* options.authorize(auth.slice(7)).pipe(Effect.provideContext(services));
        return auth.slice(7);
      });
      const health: Effect.Effect<CommandOutput<typeof Commands.health>> = Commands.health.executeDecoded({ health: () => Effect.succeed({ status: "ok" }) }, {});
      // addTool supplies the actual raw MCP arguments. Admit the full keyed request once here.
      const handle = (name: string, params: unknown) => Effect.gen(function*() {
        const token = yield* authorize;
        if (name === Commands.health.name) {
          yield* Schema.decodeUnknownEffect(Commands.health.input, { onExcessProperty: "error" })(params);
          return yield* health;
        }
        if (options.target === "broker") {
          const principal = yield* options.authorize(token).pipe(Effect.provideContext(services));
          return yield* options.handle(principal, yield* Schema.decodeUnknownEffect(Schema.toCodecJson(BrokerRequest), { onExcessProperty: "error" })({ method: name, params }).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })))).pipe(Effect.provideContext(services));
        }
        const principal = yield* options.authorize(token).pipe(Effect.provideContext(services));
        return yield* options.handle(principal, yield* Schema.decodeUnknownEffect(Schema.toCodecJson(RunnerRequest), { onExcessProperty: "error" })({ method: name, params }).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })))).pipe(Effect.provideContext(services));
      });
      const tools = Object.values(Commands).flatMap((descriptor) => {
        if (descriptor.target !== "broker" && descriptor.target !== "runner" && descriptor.target !== "shared") return [];
        if (descriptor.target !== options.target && descriptor.target !== "shared") return [];
        return [Layer.effectDiscard(Effect.gen(function*() {
        const registry = yield* McpServer.McpServer;
        const inputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(descriptor.input), { onExcessProperty: "error" });
        const outputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(descriptor.structuredOutput));
        // MCP tool inputs require an object root, including object-only tagged unions.
        // Effect's JSON Schema document has unknown-valued properties; admit that library output once.
        const inputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolJson)({ ...inputDocument.schema, type: "object", $defs: inputDocument.definitions }).pipe(Effect.orDie);
        const outputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolOutputJson)({ ...outputDocument.schema, $defs: outputDocument.definitions }).pipe(Effect.orDie);
        yield* registry.addTool({
          tool: new McpSchema.Tool({ name: descriptor.name, description: descriptor.description, inputSchema, outputSchema }),
          annotations: Context.empty(),
          handle: (payload: unknown) => Effect.gen(function*() {
            const result = yield* handle(descriptor.name, payload ?? {});
            // MCP results require JSON values before serialization, including optional undefined fields.
            const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(McpResult), { onExcessProperty: "error" })({ result });
            if (new TextEncoder().encode(encodeStructuredResult(encoded)).byteLength > LIMIT - 1024) return yield* new HttpError({ status: 413, message: "Result too large; use cursor pagination" });
            const response = toolResult(encoded);
            if (new TextEncoder().encode(encodeToolResult(response)).byteLength > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Result too large; use cursor pagination" });
            return response;
          }).pipe(Effect.catchCause((cause) => Effect.sync(() => {
            const structuredContent = Schema.encodeSync(Schema.toCodecJson(ToolFailure), { onExcessProperty: "error" })({ error: boundaryFailure(cause) });
            return toolResult(structuredContent, true);
          }))),
        });
        }))];
      });
      const protocolLayer = McpServer.layerHttp({ name: "coordinator", version: "0.1.0", path: "/mcp", protocols: [commandProtocol], allowedOrigins: [] });
      const app = yield* HttpRouter.toHttpEffect(tools.reduce(
        (layer, tool) => Layer.merge(layer, tool.pipe(Layer.provide(protocolLayer))),
        protocolLayer,
      ));
      return yield* server.serve(Effect.gen(function*() {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (probes && request.url === "/livez" && request.method === "GET") {
          yield* probes.live;
          return yield* HttpServerResponse.schemaJson(Commands.health.output)({ status: "ok" }, { headers: { "cache-control": "no-store" } });
        }
        yield* authorize;
        if (probes && request.url === "/readyz" && request.method === "GET") {
          yield* probes.ready;
          return yield* HttpServerResponse.schemaJson(Commands.health.output)({ status: "ok" }, { headers: readinessHeaders });
        }
        if (request.url === "/healthz" && request.method === "GET") return yield* HttpServerResponse.schemaJson(Commands.health.output, { onExcessProperty: "error" })(yield* health, { headers: { "cache-control": "no-store" } });
        if (Number(request.headers["content-length"] ?? 0) > LIMIT) return yield* new HttpError({ status: 413, message: "Request too large" });
        // Node masks handlers; client abort must also cancel the cached request-body reader.
        const body = request.method === "POST" ? yield* request.json.pipe(Effect.interruptible, Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(LIMIT))) : undefined;
        const method = body && typeof body === "object" && !Array.isArray(body) && "method" in body ? body.method : undefined;
        const response = yield* app.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(LIMIT)));
        const rejected = response.status < 200 || response.status >= 300;
        return yield* boundedResponse(response, rejected ? LIMIT : method === "server/discover" || method === "tools/list" ? 512 * 1024 : MCP_RESPONSE_LIMIT);
      }).pipe(Effect.catchCause((cause) => {
        if (Cause.squash(cause) instanceof HttpServerError.HttpServerError) {
          return HttpServerError.causeResponse(cause).pipe(Effect.map(([response]) => HttpServerResponse.setHeader(response, "cache-control", "no-store")));
        }
        const error = boundaryFailure(cause);
        return Effect.gen(function*() {
          const request = yield* HttpServerRequest.HttpServerRequest;
          return yield* HttpServerResponse.schemaJson(BoundaryError, { onExcessProperty: "error" })({ error: error.message }, {
            status: error.status, headers: request.url === "/readyz" ? readinessHeaders : { "cache-control": "no-store" },
          });
        });
      })));
    }).pipe(Effect.orDie);
    return { call, serve };
  }),
}) {
  static readonly layer = Layer.effect(this, this.make).pipe(Layer.provideMerge(clientLayer));
}
