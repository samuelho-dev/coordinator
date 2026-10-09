import { Cause, Deferred, Effect, Layer, Option, Sink, Stdio, Stream } from "effect";
import { McpProtocol, McpSchema } from "effect/ai";
import { Headers, HttpServerRequest } from "effect/http";
import type { Rpc, RpcGroup } from "effect/rpc";
import { NativeControlError, NativeMcpResultBytes, NativeMcpWireBytes, type NativeToolName } from "./contracts.js";

// The SDK's public adapter erases its installation target. Narrow only the seam
// we intercept; all dated codecs, middleware and other handlers stay SDK-owned.
type InstallationTarget = {
  readonly context: unknown;
  readonly install: (protocol: { readonly protocolVersion: string }, rpcs: RpcGroup.RpcGroup<Rpc.Any>, handlers: Readonly<Record<string, unknown>>) => Effect.Effect<void, never, unknown>;
};
type ListHandler = (request: typeof McpSchema.ListTools.payloadSchema.Type) => Effect.Effect<McpSchema.ListToolsResult, McpSchema.McpError, McpSchema.McpServerClient>;

export const nativeMcpProtocol = (toolNames: readonly NativeToolName[]): McpProtocol.ProtocolAdapter<"2025-11-25", McpProtocol.StatefulRuntimeDescriptor> => {
  const sdk = McpProtocol.v2025_11_25;
  const fence = globalThis.crypto.randomUUID();
  const issued = new Map<string, number>();
  const names = [...toolNames].sort();
  return {
    ...sdk,
    installHandlers: (core: unknown, lifecycle: unknown, target: InstallationTarget) => sdk.installHandlers(core, lifecycle, {
      context: target.context,
      install: (protocol: { readonly protocolVersion: string }, rpcs: RpcGroup.RpcGroup<Rpc.Any>, handlers: Readonly<Record<string, unknown>>) => {
        const original = handlers["tools/list"];
        if (typeof original !== "function") return Effect.die("Native MCP SDK is missing tools/list");
        const list = original as ListHandler;
        return target.install(protocol, rpcs, {
          ...handlers,
          "tools/list": (request: typeof McpSchema.ListTools.payloadSchema.Type) => Effect.gen(function*() {
            const client = yield* McpSchema.McpServerClient;
            const http = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
            // HTTP RPC ids change per POST; the validated MCP session owns its cursors.
            const owner = http._tag === "Some" ? Option.getOrUndefined(Headers.get(http.value.headers, "mcp-session-id")) : client.clientId;
            if (owner === undefined) return yield* new McpSchema.InvalidParams({ message: "Native tools require an initialized MCP session" });
            const prefix = `${fence}:${owner}:`;
            const cursor = request?.cursor;
            const offset = cursor === undefined ? 0 : issued.get(cursor);
            if (offset === undefined || (cursor !== undefined && !cursor.startsWith(prefix))) {
              return yield* new McpSchema.InvalidParams({ message: "Invalid native tools cursor" });
            }
            const result = yield* list(request);
            const actual = result.tools.map(tool => tool.name).sort();
            if (actual.length !== names.length || actual.some((name, index) => name !== names[index])) {
              return yield* new McpSchema.InternalError({ message: "Native tool inventory changed" });
            }
            if (!Number.isSafeInteger(offset) || offset >= result.tools.length) {
              return yield* new McpSchema.InvalidParams({ message: "Invalid native tools cursor" });
            }
            // Reserve the longest cursor up front, then account for exact UTF-8
            // JSON bytes. The outer JSON-RPC envelope has its separate wire cap.
            const encoder = new TextEncoder();
            let bytes = encoder.encode(JSON.stringify({ tools: [], nextCursor: `${prefix}${result.tools.length}` })).byteLength;
            let end = offset;
            while (end < result.tools.length) {
              const added = encoder.encode(JSON.stringify(result.tools[end])).byteLength + (end === offset ? 0 : 1);
              if (bytes + added > NativeMcpResultBytes) break;
              bytes += added;
              end++;
            }
            if (end === offset) return yield* new McpSchema.InternalError({ message: "Native tool schema exceeds MCP page budget" });
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
export const nativeMcpWireByteGate = () => {
  const utf8 = new TextDecoder("utf-8", { fatal: true });
  const encoder = new TextEncoder();
  let bytes = 0;
  let failed = false;
  return (data: Uint8Array | string, eof = false) => Effect.try({
    try: () => {
      if (failed) throw new Error("Closed native MCP byte boundary");
      if (eof) {
        utf8.decode();
        if (bytes !== 0) throw new Error("Unterminated native MCP frame");
      }
      const chunk = typeof data === "string" ? encoder.encode(data) : data;
      // Include whitespace and LF before SDK decoding or buffer allocation.
      for (const byte of chunk) {
        if (++bytes > NativeMcpWireBytes) throw new Error("Native MCP wire byte limit exceeded");
        if (byte === 10) bytes = 0;
      }
      utf8.decode(chunk, { stream: true });
      return data;
    },
    catch: () => {
      failed = true;
      return new NativeControlError({ code: "transport_closed", message: "Native MCP wire is oversized or invalid UTF-8" });
    },
  });
};

/** Byte-only trust middleware: no JSON parsing, framing, or replacement codec. */
export const nativeMcpStdioLayer = (fatal: Deferred.Deferred<void, NativeControlError>) => Layer.effect(Stdio.Stdio, Effect.gen(function*() {
  const stdio = yield* Stdio.Stdio;
  const input = nativeMcpWireByteGate();
  const output = nativeMcpWireByteGate();
  return Stdio.make({
    ...stdio,
    stdin: stdio.stdin.pipe(
      Stream.mapEffect(data => input(data).pipe(Effect.tapError(error => Deferred.fail(fatal, error)), Effect.as(data), Effect.orDie)),
      Stream.concat(Stream.fromEffect(input(new Uint8Array(), true).pipe(Effect.tapError(error => Deferred.fail(fatal, error)), Effect.orDie)).pipe(Stream.drain)),
      // The SDK retries its reader on parser failure. A failed consumer scope
      // instead closes this stdio owner; ordinary EOF still drains SDK output.
      Stream.onExit(exit => exit._tag === "Failure" && !Cause.hasInterruptsOnly(exit.cause)
        ? Deferred.fail(fatal, new NativeControlError({ code: "transport_closed", message: "Native MCP input processing failed" }))
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
        return Deferred.fail(fatal, new NativeControlError({
          code: "transport_closed", message: `Native MCP output processing failed (${reasons})`,
        }));
      }),
    ),
  });
}));
