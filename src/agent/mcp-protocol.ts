import { Cause, Deferred, Effect, Layer, Option, Sink, Stdio, Stream } from "effect";
import { McpProtocol, McpSchema } from "effect/ai";
import { Headers, HttpServerRequest } from "effect/http";
import type { Rpc, RpcGroup } from "effect/rpc";
import { AgentError, AgentMcpResultBytes, AgentMcpWireBytes, type AgentTool } from "./contracts.ts";

// Constrain the SDK's erased installation seam without replacing its RPC codecs.
type InstallationTarget = {
  readonly context: unknown;
  readonly install: <Rpcs extends Rpc.Any, Handlers extends RpcGroup.HandlersFrom<Rpcs>>(
    protocol: { readonly protocolVersion: string },
    rpcs: RpcGroup.RpcGroup<Rpcs>,
    handlers: Handlers,
  ) => Effect.Effect<void, never, RpcGroup.HandlersServices<Rpcs, Handlers>>;
};
type ListHandler<E extends McpSchema.McpError, R> = (
  request: typeof McpSchema.ListTools.payloadSchema.Type,
) => Effect.Effect<McpSchema.ListToolsResult, E, R>;

export const agentMcpProtocol = (tools: readonly Pick<AgentTool, "name">[]): McpProtocol.ProtocolAdapter<"2025-11-25", McpProtocol.StatefulRuntimeDescriptor> => {
  const sdk = McpProtocol.v2025_11_25;
  const fence = globalThis.crypto.randomUUID();
  const issued = new Map<string, number>();
  const names = tools.map((descriptor) => descriptor.name).sort();
  return {
    ...sdk,
    installHandlers: (core: unknown, lifecycle: unknown, target: InstallationTarget) => sdk.installHandlers(core, lifecycle, {
      context: target.context,
      install: <Rpcs extends Rpc.Any, E extends McpSchema.McpError, R, Handlers extends RpcGroup.HandlersFrom<Rpcs> & { readonly "tools/list": ListHandler<E, R> }>(
        protocol: { readonly protocolVersion: string },
        rpcs: RpcGroup.RpcGroup<Rpcs>,
        handlers: Handlers,
      ) => {
        const list = handlers["tools/list"];
        return target.install(protocol, rpcs, {
          ...handlers,
          "tools/list": (request: typeof McpSchema.ListTools.payloadSchema.Type) => Effect.gen(function*() {
            const client = yield* McpSchema.McpServerClient;
            const http = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
            // HTTP RPC ids change per POST; the validated MCP session owns its cursors.
            const owner = http._tag === "Some" ? Option.getOrUndefined(Headers.get(http.value.headers, "mcp-session-id")) : client.clientId;
            if (owner === undefined) return yield* new McpSchema.InvalidParams({ message: "Agent tools require an initialized MCP session" });
            const prefix = `${fence}:${owner}:`;
            const cursor = request?.cursor;
            const offset = cursor === undefined ? 0 : issued.get(cursor);
            if (offset === undefined || (cursor !== undefined && !cursor.startsWith(prefix))) {
              return yield* new McpSchema.InvalidParams({ message: "Invalid agent tools cursor" });
            }
            const result = yield* list(request);
            const actual = result.tools.map(tool => tool.name).sort();
            if (actual.length !== names.length || actual.some((name, index) => name !== names[index])) {
              return yield* new McpSchema.InternalError({ message: "Agent tool inventory changed" });
            }
            if (!Number.isSafeInteger(offset) || offset >= result.tools.length) {
              return yield* new McpSchema.InvalidParams({ message: "Invalid agent tools cursor" });
            }
            // Reserve the longest cursor up front, then account for exact UTF-8
            // JSON bytes. The outer JSON-RPC envelope has its separate wire cap.
            const encoder = new TextEncoder();
            let bytes = encoder.encode(JSON.stringify({ tools: [], nextCursor: `${prefix}${result.tools.length}` })).byteLength;
            let end = offset;
            while (end < result.tools.length) {
              const added = encoder.encode(JSON.stringify(result.tools[end])).byteLength + (end === offset ? 0 : 1);
              if (bytes + added > AgentMcpResultBytes) break;
              bytes += added;
              end++;
            }
            if (end === offset) return yield* new McpSchema.InternalError({ message: "Agent tool schema exceeds MCP page budget" });
            const nextCursor = end < result.tools.length ? `${prefix}${end}` : undefined;
            if (nextCursor !== undefined) issued.set(nextCursor, end);
            return new McpSchema.ListToolsResult({ ...result, tools: result.tools.slice(offset, end), nextCursor });
          }),
        });
      },
    }),
  };
};

/** Literal wire-byte and fatal UTF-8 boundary; the SDK still owns framing/JSON. */
export const agentMcpWireByteGate = () => {
  const utf8 = new TextDecoder("utf-8", { fatal: true });
  const encoder = new TextEncoder();
  let bytes = 0;
  let failed = false;
  return (data: Uint8Array | string, eof = false) => Effect.try({
    try: () => {
      if (failed) throw new Error("Closed agent MCP byte boundary");
      if (eof) {
        utf8.decode();
        if (bytes !== 0) throw new Error("Unterminated agent MCP frame");
      }
      const chunk = typeof data === "string" ? encoder.encode(data) : data;
      // Include whitespace and LF before SDK decoding or buffer allocation.
      for (const byte of chunk) {
        if (++bytes > AgentMcpWireBytes) throw new Error("Agent MCP wire byte limit exceeded");
        if (byte === 10) bytes = 0;
      }
      utf8.decode(chunk, { stream: true });
      return data;
    },
    catch: () => {
      failed = true;
      return new AgentError({ code: "transport_closed", message: "Agent MCP wire is oversized or invalid UTF-8" });
    },
  });
};

/** Byte-only trust middleware: no JSON parsing, framing, or replacement codec. */
export const agentMcpStdioLayer = (fatal: Deferred.Deferred<void, AgentError>) => Layer.effect(Stdio.Stdio, Effect.gen(function*() {
  const stdio = yield* Stdio.Stdio;
  const input = agentMcpWireByteGate();
  const output = agentMcpWireByteGate();
  return Stdio.make({
    ...stdio,
    stdin: stdio.stdin.pipe(
      Stream.mapEffect(data => input(data).pipe(Effect.tapError(error => Deferred.fail(fatal, error)), Effect.as(data), Effect.orDie)),
      Stream.concat(Stream.fromEffect(input(new Uint8Array(), true).pipe(Effect.tapError(error => Deferred.fail(fatal, error)), Effect.orDie)).pipe(Stream.drain)),
      // The SDK retries its reader on parser failure. A failed consumer scope
      // instead closes this stdio owner; ordinary EOF still drains SDK output.
      Stream.onExit(exit => exit._tag === "Failure" && !Cause.hasInterruptsOnly(exit.cause)
        ? Deferred.fail(fatal, new AgentError({ code: "transport_closed", message: "Agent MCP input processing failed" }))
        : Effect.void),
    ),
    stdout: options => stdio.stdout(options).pipe(
      Sink.mapInputEffect((data: Uint8Array | string) => output(data).pipe(Effect.tapError(error => Deferred.fail(fatal, error)), Effect.orDie)),
      Sink.onExit(exit => {
        const expected = exit._tag === "Success" || exit.cause.reasons.every(reason =>
          reason._tag === "Fail" && Cause.isDone(reason.error));
        if (expected) return output(new Uint8Array(), true).pipe(
          Effect.tapError(error => Deferred.fail(fatal, error)),
          Effect.andThen(Deferred.succeed(fatal, undefined)),
          Effect.asVoid,
          Effect.orDie,
        );
        if (Cause.hasInterruptsOnly(exit.cause)) return Effect.void;
        // Retain only causal shape, never transport payloads or exception messages.
        const reasons = exit.cause.reasons.map(reason => {
          if (reason._tag === "Interrupt") return "Interrupt";
          const value = reason._tag === "Fail" ? reason.error : reason.defect;
          return `${reason._tag}:${value instanceof Error ? value.name : typeof value}`;
        }).join(",");
        return Deferred.fail(fatal, new AgentError({
          code: "transport_closed", message: `Agent MCP output processing failed (${reasons})`,
        }));
      }),
    ),
  });
}));
