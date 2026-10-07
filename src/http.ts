import { ByteSize, Cause, Config, Context, Effect, Layer, Option, Schema, Stream, type Scope } from "effect";
import { HttpBody, HttpClient, HttpClientError, HttpClientRequest, HttpIncomingMessage, HttpRouter, HttpServer, HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { createServer } from "node:http";
import { lookup } from "node:dns";
import type { LookupFunction } from "node:net";
import { McpProtocol, McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { BackupError, SnapshotList, BrokerRequest, RunnerRequest, ConnectHosts, HttpError, CommandError, BoundaryError, Json, McpToolName, Commands, McpCommandName, type CommandInput, type McpEndpoint } from "./contracts.ts";

export const LIMIT = 64 * 1024;
export const MCP_RESULT_LIMIT = LIMIT - 1024;
export function mcpResultSizer<S extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(schema: S) {
  const encode = Schema.encodeUnknownSync(Schema.toCodecJson(schema), { onExcessProperty: "error" });
  return (value: unknown) => bytes(JSON.stringify(encode(value)));
}
export function decode<S extends Schema.ConstraintDecoder<unknown>>(schema: S, value: unknown): S["Type"] {
  return Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(value);
}
export function decodeEffect<S extends Schema.Constraint>(schema: S, value: unknown): Effect.Effect<S["Type"], HttpError, S["DecodingServices"]> {
  return Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(value).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
}
export function decodeJson<S extends Schema.ConstraintDecoder<unknown> & Schema.ConstraintEncoder<unknown>>(schema: S, text: string): S["Type"] {
  return decode(Schema.fromJsonString(Schema.toCodecJson(schema)), text);
}
export function encodeJson<S extends Schema.ConstraintDecoder<unknown> & Schema.ConstraintEncoder<unknown>>(schema: S, value: S["Type"], space?: number | string): string {
  // Storage keeps omitted optional fields; only MCP's object wire uses canonical JSON nulls.
  return Schema.encodeUnknownSync(Schema.fromJsonString(schema, { space }), { onExcessProperty: "error" })(value);
}
export function canonicalJson(value: unknown): string {
  const text = JSON.stringify(value, (_, item: unknown) => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
    return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
  });
  if (text === undefined) throw new Error("JSON value required");
  return text;
}
export function decodeSnapshotList(text: string): SnapshotList {
  // ponytail: S3's fixed ASCII snapshot names only; reject entities and foreign keys instead of adding a general XML parser.
  if (text.includes("<!") || !/^\s*(?:<\?xml\s[^<>]*\?>\s*)?<ListBucketResult(?:\s[^>]*)?>[\s\S]*<\/ListBucketResult>\s*$/.test(text)) throw new BackupError({ message: "Invalid S3 snapshot listing" });
  const tags: string[] = [];
  let through = 0;
  for (const token of text.matchAll(/<[^<>]*>/g)) {
    if (text.slice(through, token.index).includes("<")) throw new BackupError({ message: "Malformed S3 snapshot listing" });
    through = token.index + token[0].length;
    if (/^<\?xml\s[^<>]*\?>$/.test(token[0])) continue;
    const name = /^<\/?([A-Za-z_][\w.-]*)(?:\s[^<>]*)?\/?>$/.exec(token[0])?.[1];
    if (!name) throw new BackupError({ message: "Malformed S3 snapshot tag" });
    if (token[0].startsWith("</")) {
      if (tags.pop() !== name) throw new BackupError({ message: "Malformed S3 snapshot nesting" });
    } else if (!token[0].endsWith("/>")) tags.push(name);
  }
  if (tags.length || text.slice(through).includes("<")) throw new BackupError({ message: "Incomplete S3 snapshot nesting" });
  const contents = [...text.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)];
  const keys = contents.map((item) => {
    const matches = [...item[1]!.matchAll(/<Key>([^<]*)<\/Key>/g)];
    if (matches.length !== 1) throw new BackupError({ message: "Invalid S3 snapshot key" });
    return matches[0]![1]!;
  });
  const truncated = [...text.matchAll(/<IsTruncated>(true|false)<\/IsTruncated>/g)];
  const next = [...text.matchAll(/<NextContinuationToken>([^<]+)<\/NextContinuationToken>/g)];
  const count = [...text.matchAll(/<KeyCount>([0-9]+)<\/KeyCount>/g)], maximum = [...text.matchAll(/<MaxKeys>([0-9]+)<\/MaxKeys>/g)];
  if ((text.match(/<\/?Key\b/g)?.length ?? 0) !== contents.length * 2 || (text.match(/<\/?Contents\b/g)?.length ?? 0) !== contents.length * 2 || truncated.length !== 1 || next.length !== (truncated[0]![1] === "true" ? 1 : 0) || count.length !== 1 || Number(count[0]![1]) !== keys.length || maximum.length !== 1 || !Number.isSafeInteger(Number(maximum[0]![1])) || Number(maximum[0]![1]) < keys.length) throw new BackupError({ message: "Incomplete S3 snapshot listing" });
  return decode(SnapshotList, { keys, ...(next.length ? { next: next[0]![1]! } : {}) });
}
function operation(method: unknown, target?: "broker" | "runner") {
  if (typeof method !== "string" || !Object.hasOwn(Commands, method) || method === "health") throw new HttpError({ status: 404, message: "Unknown operation" });
  const descriptor = Commands[decode(McpToolName, method)];
  if (target !== undefined && descriptor.target !== target) throw new HttpError({ status: 404, message: "Unknown operation" });
  return descriptor;
}
export function decodeRequest(target: "broker", method: unknown, params: unknown): BrokerRequest;
export function decodeRequest(target: "runner", method: unknown, params: unknown): RunnerRequest;
export function decodeRequest(target: "broker" | "runner", method: unknown, params: unknown): BrokerRequest | RunnerRequest {
  operation(method, target);
  return target === "broker" ? decode(BrokerRequest, { method, params }) : decode(RunnerRequest, { method, params });
}

const clientLayer = Layer.unwrap(Effect.gen(function*() {
  const configuredHosts = yield* Config.String("COORDINATOR_CONNECT_HOSTS_JSON").pipe(Config.withDefault("{}"));
  const hosts = yield* Effect.try(() => decodeJson(ConnectHosts, configuredHosts));
  // The Node agent requires a DNS callback; Effect has no DNS service. The socket override leaves TLS hostname verification intact.
  const connectLookup: LookupFunction = (hostname, options, callback) => lookup(hosts[hostname] ?? hostname, options, callback);
  return NodeHttpClient.layerNodeHttpNoAgent.pipe(Layer.provide(NodeHttpClient.layerAgentOptions({ lookup: connectLookup })));
}));

// NodeHttpServer explicitly requires this native server factory; all serving/lifecycle is the Effect adapter.
export const serverLayer = (host: string, port: number) => NodeHttpServer.layerServer(() => createServer({ requestTimeout: 15000 }), { host, port, gracefulShutdownTimeout: "1 second" });

function boundedText<E>(message: HttpIncomingMessage.HttpIncomingMessage<E>, limit = LIMIT) {
  return message.text.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(limit)), Effect.mapError((error) => {
    const reason = error instanceof HttpClientError.HttpClientError || error instanceof HttpServerError.HttpServerError ? error.reason : undefined;
    const cause = reason !== undefined && "cause" in reason ? reason.cause : undefined;
    return cause instanceof Error && cause.message === "maxBytes exceeded" ? new HttpError({ status: 413, message: "HTTP body exceeds bound" }) : error;
  }));
}
const ToolFailure = Schema.Struct({ error: HttpError });
const boundaryFailure = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  return new HttpError({ status: error instanceof HttpError ? error.status : 400, message: error instanceof HttpError ? error.message.slice(0, 512) : "Invalid request or unavailable operation" });
};
const bytes = (text: string) => new TextEncoder().encode(text).byteLength;
const boundedResponse = Effect.fnUntraced(function*(response: HttpServerResponse.HttpServerResponse, limit: number) {
  const body = response.body;
  if (body._tag === "Empty") return response;
  if (body._tag === "Uint8Array") {
    if (body.contentLength > limit) return yield* new HttpError({ status: 413, message: "MCP response exceeds bound" });
    return response;
  }
  if (body._tag !== "Stream") return yield* new HttpError({ status: 500, message: "Unsupported MCP response body" });
  let size = 0;
  const chunks = yield* body.stream.pipe(
    Stream.mapEffect((chunk) => {
      size += chunk.byteLength;
      return size > limit ? Effect.fail(new HttpError({ status: 413, message: "MCP response exceeds bound" })) : Effect.succeed(chunk);
    }),
    Stream.runCollect,
    Effect.mapError((error) => error instanceof HttpError ? error : new HttpError({ status: 500, message: "MCP response unavailable" })),
  );
  const data = chunks.length === 1 ? chunks[0]! : new Uint8Array(size);
  if (chunks.length !== 1) {
    let offset = 0;
    for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.byteLength; }
  }
  return HttpServerResponse.setBody(response, HttpBody.uint8Array(data, body.contentType));
});

// fromWeb uses Raw for Web streams; NodeHttpClient needs an Effect byte stream.
export function fromWebRequest(request: Request): HttpClientRequest.HttpClientRequest {
  const outbound = HttpClientRequest.fromWeb(request);
  if (request.body === null || outbound.body._tag !== "Raw") return outbound;
  const body = outbound.body;
  const stream = Stream.suspend(() => {
    let size = 0;
    return Stream.fromReadableStream({ evaluate: () => request.body!, onError: (error) => error }).pipe(
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
    const call = <const M extends McpCommandName>(url: string, token: string, method: M, params: CommandInput<M>, timeoutMs = 10000): Effect.Effect<(typeof Commands)[M]["output"]["Type"], unknown> => Effect.scoped(Effect.gen(function*() {
      const descriptor = Commands[method];
      const input = yield* decodeEffect(descriptor.input, params);
      const endpoint = yield* Effect.try(() => new URL("/mcp", url));
      if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) return yield* new HttpError({ status: 400, message: "TLS required except loopback" });
      const sdkFetch: typeof globalThis.fetch = async (input, init) => {
        const request = new Request(input, init);
        if (new URL(request.url).origin !== endpoint.origin) throw new HttpError({ status: 400, message: "Cross-origin MCP request prohibited" });
        return Effect.runPromise(Effect.scoped(Effect.gen(function*() {
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
          const response = yield* http.execute(outbound).pipe(Effect.mapError((error) => {
            const cause = "cause" in error.reason ? error.reason.cause : undefined;
            return cause instanceof HttpError ? cause : error;
          }));
          body += decoder.decode();
          // Discovery contains the full catalog; command responses keep the existing 64 KiB bound.
          const method = body ? decodeJson(Json, body).method : undefined;
          const discovery = method === "server/discover" || method === "tools/list";
          const text = yield* boundedText(response, discovery ? 512 * 1024 : LIMIT);
          if (response.status < 200 || response.status >= 300) return yield* new HttpError({ status: response.status, message: "MCP request rejected" });
          return new Response(text || null, { status: response.status, headers: response.headers });
        })), { signal: request.signal });
      };
      const client = yield* Effect.acquireRelease(
        Effect.sync(() => new Client({ name: "coordinator", version: "0.1.0" })),
        (client) => Effect.tryPromise(() => client.close()).pipe(Effect.catchCause(() => Effect.void)),
      );
      yield* Effect.tryPromise({ try: (signal) => client.connect(new StreamableHTTPClientTransport(endpoint, { authProvider: { token: async () => token }, fetch: (input, init) => sdkFetch(input, { ...init, signal: init?.signal ? AbortSignal.any([signal, init.signal]) : signal }), requestInit: { redirect: "error" } })), catch: (error) => error instanceof HttpError || error instanceof CommandError ? error : new CommandError({ message: error instanceof Error ? error.message : String(error) }) });
      const result = yield* Effect.tryPromise({ try: (signal) => client.callTool({ name: descriptor.method ?? method, arguments: decode(Schema.Record(Schema.String, Schema.Unknown), Schema.encodeUnknownSync(Schema.toCodecJson(descriptor.input), { onExcessProperty: "error" })(input)) }, { signal, timeout: timeoutMs }), catch: (error) => error instanceof HttpError || error instanceof CommandError ? error : new CommandError({ message: error instanceof Error ? error.message : String(error) }) });
      if (result.isError) {
        const failure = result.structuredContent ?? (yield* Effect.try(() => {
          const text = result.content.find((item) => item.type === "text");
          return text?.type === "text" ? JSON.parse(text.text) : undefined;
        }));
        const decoded = yield* decodeEffect(Schema.toCodecJson(ToolFailure), failure);
        return yield* decoded.error;
      }
      const output = yield* decodeEffect<Schema.toCodecJson<(typeof Commands)[M]["structuredOutput"]>>(Schema.toCodecJson(descriptor.structuredOutput), result.structuredContent);
      return output.result;
    })).pipe(Effect.timeout(timeoutMs));

    const serve = (server: HttpServer.HttpServer["Service"], options: McpEndpoint): Effect.Effect<void, never, Scope.Scope> => Effect.gen(function*() {
      const authorize = Effect.gen(function*() {
        const current = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
        if (Option.isNone(current)) return yield* new HttpError({ status: 401, message: "Request context required" });
        const request = current.value;
        const auth = request.headers.authorization;
        if (!auth?.startsWith("Bearer ") || auth.length > 512) return yield* new HttpError({ status: 401, message: "Credential required" });
        if (options.target === "broker") yield* options.authorize(auth.slice(7));
        else yield* options.authorize(auth.slice(7));
        return auth.slice(7);
      });
      const handle = (name: McpCommandName, params: unknown) => Effect.gen(function*() {
        // This service is restored by native MCP for each request, never captured at registration.
        const token = yield* authorize;
        if (name === "health") return yield* Commands.health.execute({ health: () => Effect.succeed({ status: "ok" as const }) }, {});
        if (options.target === "broker") {
          const principal = yield* options.authorize(token);
          return yield* options.handle(principal, yield* decodeEffect(BrokerRequest, { method: name, params }));
        }
        const principal = yield* options.authorize(token);
        return yield* options.handle(principal, yield* decodeEffect(RunnerRequest, { method: name, params }));
      });
      const tools = Object.entries(Commands).filter(([, descriptor]) => descriptor.target === options.target || descriptor.target === "shared").map(([id, descriptor]) => {
        const commandName = decode(McpCommandName, id);
        // Runtime schemas remain context-free; the domain decodes its discriminated request.
        const makeTool = (name: string, input: Schema.Top & Schema.ConstraintCodec<unknown, unknown>, output: Schema.Top & Schema.ConstraintCodec<unknown, unknown>, description: string) => {
          const failureCodec = Schema.toCodecJson(ToolFailure);
          const resultSize = mcpResultSizer(output);
          const tool = Tool.make(name, { parameters: Schema.toCodecJson(input), success: Schema.toCodecJson(output), failure: failureCodec, description }).annotate(Tool.Strict, true);
          const toolkit = Toolkit.make(tool);
          const registration = Layer.effectDiscard(Effect.gen(function*() {
            const registry = yield* McpServer.McpServer;
            yield* McpServer.registerToolkit(toolkit).pipe(Effect.provideService(McpServer.McpServer, {
              ...registry,
              addTool: (registration) => registry.addTool({
                ...registration,
                handle: (payload) => registration.handle(payload).pipe(Effect.flatMap((result) => {
                  // ponytail: structured clients only; duplicating JSON text breaks the fixed wire bound.
                  if (!("isError" in result) || !result.isError) return Effect.succeed(new McpSchema.CallToolResult({ ...result, content: [] }));
                  const text = result.content.find((item) => item.type === "text");
                  return Effect.try(() => text?.type === "text" ? decodeJson(ToolFailure, text.text) : undefined).pipe(
                    Effect.catchCause(() => Effect.succeed(undefined)),
                    Effect.map((failure) => failure === undefined ? result : new McpSchema.CallToolResult({
                      ...result,
                      structuredContent: Schema.encodeUnknownSync(failureCodec)(failure),
                    })),
                  );
                })),
              }),
            }));
          }));
          return registration.pipe(Layer.provide(toolkit.toLayer({
            [name]: (params: unknown) => Effect.gen(function*() {
              const result = yield* handle(commandName, params);
              const value = yield* decodeEffect(output, { result });
              if (resultSize(value) > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Result too large; use cursor pagination" });
              return value;
            }).pipe(Effect.catchCause((cause) => Effect.fail({ error: boundaryFailure(cause) }))),
          })));
        };
        return makeTool(descriptor.method ?? commandName, descriptor.input, descriptor.structuredOutput, descriptor.description);
      });
      const native = McpServer.layerHttp({ name: "coordinator", version: "0.1.0", path: "/mcp", protocols: [McpProtocol.v2026_07_28, McpProtocol.v2025_11_25], allowedOrigins: [] });
      const app = yield* HttpRouter.toHttpEffect(tools.reduce(
        (layer, tool) => Layer.merge(layer, tool.pipe(Layer.provide(native))),
        native,
      ));
      return yield* server.serve(Effect.gen(function*() {
        const request = yield* HttpServerRequest.HttpServerRequest;
        yield* authorize;
        if (request.url === "/healthz" && request.method === "GET") return HttpServerResponse.text(encodeJson(Commands.health.output, yield* decodeEffect(Commands.health.output, yield* handle("health", {}))), { contentType: "application/json", headers: { "cache-control": "no-store" } });
        if (Number(request.headers["content-length"] ?? 0) > LIMIT) return yield* new HttpError({ status: 413, message: "Request too large" });
        const body = request.method === "POST" ? yield* request.json.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(LIMIT))) : undefined;
        const method = body && typeof body === "object" && !Array.isArray(body) && "method" in body ? body.method : undefined;
        const response = yield* app.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(LIMIT)));
        return yield* boundedResponse(response, method === "server/discover" || method === "tools/list" ? 512 * 1024 : LIMIT);
      }).pipe(Effect.catchCause((cause) => {
        if (Cause.squash(cause) instanceof HttpServerError.HttpServerError) {
          return HttpServerError.causeResponse(cause).pipe(Effect.map(([response]) => HttpServerResponse.setHeader(response, "cache-control", "no-store")));
        }
        const error = boundaryFailure(cause);
        return Effect.succeed(HttpServerResponse.text(encodeJson(BoundaryError, { error: error.message }), { status: error.status, contentType: "application/json", headers: { "cache-control": "no-store" } }));
      })));
    }).pipe(Effect.orDie);
    return { call, serve };
  }),
}) {
  static readonly layer = Layer.effect(this, this.make).pipe(Layer.provideMerge(clientLayer));
}
