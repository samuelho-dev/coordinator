import { Cause, Context, Crypto, Deferred, Effect, Layer, Option, Queue, Schema, Scope } from "effect";
import { McpSchema, McpServer } from "effect/ai";
import {
  NativeControlError,
  NativeEventSequence,
  NativeEventPage,
  NativePromptResult,
  NativeTools,
  NativeBinding,
  NativeHost,
  NativeMcpResultBytes,
  NativePrivateResultBytes,
  NativePrivateResultId,
  NativePrivateResultReference,
  nativePrivateResultUri,
  nativeToolInputSchema,
  nativeToolOutputSchema,
  nativeEventsUri,
  type NativeLifecycleEvent,
  type NativeToolInput,
  type NativeToolOutput,
  type NativeToolName,
} from "./contracts.js";
import { NativeLifecycleJournal } from "./lifecycle.js";
import { NativeMcpOwnerScope, nativeMcpPrivateOwner, type NativePresentation } from "./interaction.js";
import { nativeMcpProtocol, nativeMcpStdioLayer } from "./mcp-protocol.js";

export type NativeMcpRequestContext = McpSchema.McpRequestContext["Service"];
export const nativeMcpRequestContext = McpSchema.McpRequestContext;

const failureSchema = Schema.Struct({ error: NativeControlError });
type CapturePrivateResult = (
  binding: NativeBinding,
  bytes: Uint8Array,
  owner: string,
) => Effect.Effect<NativePrivateResultReference, NativeControlError>;

const promptSettlements: {
  readonly [N in NativeToolName]?: (journal: NativeLifecycleJournal, result: NativeToolOutput<N>) => Effect.Effect<void, NativeControlError>;
} = {
  "native.prompt": (journal, result) => awaitPromptSettlement(journal, result),
  "native.abort_and_prompt": (journal, result) => awaitPromptSettlement(journal, result),
};
const resultBindings: { readonly [N in NativeToolName]?: (result: NativeToolOutput<N>) => NativeBinding } = {
  "native.get_state": (result) => result.binding,
};

export type NativeToolRegistration<R = never> = {
  readonly name: NativeToolName;
  readonly register: (
    journal: NativeLifecycleJournal,
    rememberClient: Effect.Effect<NativeMcpRequestContext, never, McpSchema.McpRequestContext>,
    capturePrivateResult: CapturePrivateResult,
    presentation?: NativePresentation,
  ) => Effect.Effect<void, never, R | McpServer.McpServer>;
};

/** The literal name ties a handler's input and output to the canonical descriptor. */
export const nativeTool = <N extends NativeToolName, R = never>(
  name: N,
  handler: (
    input: NativeToolInput<N>,
    context: NativeMcpRequestContext,
  ) => Effect.Effect<NativeToolOutput<N>, NativeControlError, R | McpSchema.McpRequestContext | McpSchema.McpServerClient>,
) => ({
  name,
  register: (
    journal: NativeLifecycleJournal,
    rememberClient: Effect.Effect<NativeMcpRequestContext, never, McpSchema.McpRequestContext>,
    capturePrivateResult: CapturePrivateResult,
    presentation?: NativePresentation,
  ) => Effect.gen(function*() {
    const registry = yield* McpServer.McpServer;
    const services = yield* Effect.context<R>();
    const descriptor = NativeTools[name];
    const inputCodec = Schema.toCodecJson(nativeToolInputSchema(name));
    const outputCodec = Schema.toCodecJson(nativeToolOutputSchema(name));
    const structuredCodec = Schema.toCodecJson(descriptor.structuredOutput);
    const inputDocument = Schema.toJsonSchemaDocument(inputCodec, { onExcessProperty: "error" });
    const outputDocument = Schema.toJsonSchemaDocument(structuredCodec, { onExcessProperty: "error" });
    yield* registry.addTool({
      tool: new McpSchema.Tool({
        name,
        description: descriptor.description,
        inputSchema: Schema.decodeUnknownSync(McpSchema.ToolJson)(
          Object.keys(inputDocument.definitions).length === 0 ? inputDocument.schema : { ...inputDocument.schema, $defs: inputDocument.definitions },
        ),
        outputSchema: Schema.decodeUnknownSync(McpSchema.ToolOutputJson)(
          Object.keys(outputDocument.definitions).length === 0 ? outputDocument.schema : { ...outputDocument.schema, $defs: outputDocument.definitions },
        ),
      }),
      annotations: Context.empty(),
      handle: (payload: unknown) => Effect.gen(function*() {
        const context = yield* rememberClient;
        const invocation = yield* Effect.context<McpSchema.McpRequestContext>();
        const client = Context.getOrUndefined(invocation, McpSchema.McpServerClient);
        if (client === undefined) {
          return yield* new McpSchema.InternalError({ message: "Native tool requires an initialized MCP client" });
        }
        const input = yield* Effect.try({
          try: () => Schema.decodeUnknownSync<Schema.toCodecJson<(typeof NativeTools)[N]["input"]>>(inputCodec)(payload, { onExcessProperty: "error" }),
          catch: () => new NativeControlError({ code: "invalid_input", message: "Invalid native tool input" }),
        });
        const result = yield* handler(input, context).pipe(
          Effect.provideContext(Context.add(Context.merge(services, invocation), McpSchema.McpServerClient, client)),
        );
        const settlement = promptSettlements[name];
        if (settlement !== undefined) {
          yield* settlement(journal, result);
          if (presentation !== undefined) yield* Effect.tryPromise({
            try: () => presentation.flush(),
            catch: () => new NativeControlError({ code: "native_failure", message: "Native presentation publication failed" }),
          });
        }
        const value = yield* Effect.try({
          try: () => Schema.encodeUnknownSync(outputCodec)(result, { onExcessProperty: "error" }),
          catch: () => new NativeControlError({ code: "native_failure", message: "Invalid or oversized native tool output" }),
        });
        const text = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(value, { onExcessProperty: "error" }).pipe(
          Effect.mapError(() => new NativeControlError({ code: "native_failure", message: "Invalid or oversized native tool output" })),
        );
        const bytes = new TextEncoder().encode(text);
        let envelope: Schema.JsonObject;
        if (bytes.byteLength + 11 <= NativeMcpResultBytes) {
          envelope = { result: value };
        } else {
          let binding = "binding" in input ? input.binding : undefined;
          if (binding === undefined) binding = resultBindings[name]?.(result);
          if (binding === undefined) return yield* new NativeControlError({ code: "binding_mismatch", message: "Native result is missing its invocation binding" });
          const resultResource = yield* capturePrivateResult(binding, bytes, yield* nativeMcpPrivateOwner);
          envelope = { resultResource: Schema.encodeSync(Schema.toCodecJson(NativePrivateResultReference))(resultResource) };
        }
        return new McpSchema.CallToolResult({ content: [], structuredContent: envelope });
      }).pipe(
        Effect.catchTag("NativeControlError", (error) =>
          Schema.encodeUnknownEffect(Schema.toCodecJson(failureSchema))({ error }, { onExcessProperty: "error" }).pipe(
            Effect.map((structuredContent) => new McpSchema.CallToolResult({ isError: true, content: [], structuredContent })),
            Effect.mapError(() => new McpSchema.InternalError({ message: "Invalid native failure envelope" })),
          )),
        Effect.catchCause((cause) => Cause.hasDies(cause)
          ? Effect.fail(new McpSchema.InternalError({ message: "Native tool execution failed" }))
          : Effect.failCause(cause)),
      ),
    });
  }),
});

export type NativeControlServerOptions<R = never> = {
  readonly host: NativeHost;
  readonly binding: NativeBinding;
  readonly journal: NativeLifecycleJournal;
  readonly tools: readonly NativeToolRegistration<R>[];
  readonly presentation?: NativePresentation;
};

const awaitPromptSettlement = (journal: NativeLifecycleJournal, result: typeof NativePromptResult.Type) => {
  if (result.disposition === "handled") {
    return result.admission.disposition === "handled" && result.admission.runId === null
      ? Effect.void
      : Effect.fail(new NativeControlError({ code: "native_failure", message: "Native handled result does not match its admission" }));
  }
  const admission = result.admission;
  if (admission.runId === null || admission.runId !== result.final.runId ||
    nativeEventsUri(admission.binding) !== nativeEventsUri(result.final.binding)) {
    return Effect.fail(new NativeControlError({ code: "native_failure", message: "Native prompt result does not match its admission" }));
  }
  return Effect.callback<void, NativeControlError>((resume) => {
    const matches = (event: NativeLifecycleEvent) =>
      event.kind === "run_settled" && event.runId === admission.runId &&
      nativeEventsUri(event.binding) === nativeEventsUri(admission.binding);
    const settle = (event: NativeLifecycleEvent) => {
      if (event.kind !== "run_settled") return;
      resume(event.outcome === result.final.outcome
        ? Effect.void
        : Effect.fail(new NativeControlError({ code: "native_failure", message: "Native prompt outcome disagrees with settlement" })));
    };
    const unsubscribe = journal.subscribe((event) => { if (matches(event)) settle(event); });
    try {
      let after = admission.seq;
      for (;;) {
        const page = journal.read(admission.binding, after);
        if (page.gap !== null) {
          resume(Effect.fail(new NativeControlError({ code: "resource_gap", message: "Native settlement history is unavailable" })));
          break;
        }
        const settled = page.entries.find(matches);
        if (settled !== undefined) { settle(settled); break; }
        if (!page.more) break;
        after = page.nextAfter;
      }
    } catch {
      resume(Effect.fail(new NativeControlError({ code: "resource_gap", message: "Native settlement history is unavailable" })));
    }
    return Effect.sync(unsubscribe);
  });
};

/** Registers implemented public capabilities; absent operations are never advertised. */
export const registerNativeControl = <R = never>(options: NativeControlServerOptions<R>) =>
  Effect.gen(function*() {
    if (options.binding.host !== options.host) {
      return yield* new NativeControlError({ code: "binding_mismatch", message: "Server binding host does not match" });
    }
    const allowed = Object.entries(NativeTools).filter(([, descriptor]) => descriptor.hosts.some((host) => host === options.host)).map(([name]) => name);
    const supplied = options.tools.map((tool) => tool.name);
    if (new Set(supplied).size !== supplied.length || !supplied.includes("native.get_state") ||
      supplied.some((name) => !allowed.includes(name))) {
      return yield* new NativeControlError({ code: "unsupported_operation", message: "Native handler inventory contains missing bootstrap, duplicate or foreign capabilities" });
    }
    const registry = yield* McpServer.McpServer;
    const registrationScope = yield* Scope.Scope;
    const wake = yield* Queue.sliding<void>(1);
    const pending = new Map<string, NativeBinding>();
    const registered = new Set<string>();
    let notificationContext: NativeMcpRequestContext | undefined;
    const rememberClient = Effect.map(McpSchema.McpRequestContext, (context) => {
      notificationContext = context;
      Queue.offerUnsafe(wake, undefined);
      return context;
    });
    const crypto = yield* Crypto.Crypto;
    const snapshots = yield* Effect.acquireRelease(
      Effect.sync(() => new Map<string, { readonly bytes: Uint8Array; readonly owner: string }>()),
      (store) => Effect.sync(() => store.clear()),
    );
    let retainedBytes = 0;
    const capturePrivateResult: CapturePrivateResult = (binding, bytes, owner) => Effect.gen(function*() {
      if (bytes.byteLength < 1 || bytes.byteLength > NativePrivateResultBytes) {
        return yield* new NativeControlError({ code: "native_failure", message: "Native private result exceeds snapshot capacity" });
      }
      const id = Schema.decodeUnknownSync(NativePrivateResultId)(yield* crypto.randomUUIDv4().pipe(
        Effect.mapError(() => new NativeControlError({ code: "native_failure", message: "Unable to allocate a private result identifier" })),
      ));
      const uri = nativePrivateResultUri(binding, id);
      while (retainedBytes + bytes.byteLength > NativePrivateResultBytes || snapshots.size >= 64) {
        const oldest = snapshots.entries().next().value;
        if (oldest === undefined) break;
        snapshots.delete(oldest[0]);
        retainedBytes -= oldest[1].bytes.byteLength;
      }
      const reference: NativePrivateResultReference = {
        binding, uri, mimeType: "application/json", byteLength: bytes.byteLength,
      };
      snapshots.set(uri, { bytes, owner });
      retainedBytes += bytes.byteLength;
      return reference;
    });
    yield* registry.addResourceTemplate({
      template: new McpSchema.ResourceTemplate({
        uriTemplate: "native://{host}/{session}/{incarnation}/{generation}/results/{resultId}{?offset,limit}",
        name: "Private native result bytes",
        description: "Immutable private output bytes; opaque references are supplied only by their originating tool call",
        mimeType: "application/json",
      }),
      routerPath: "native:://:0/:1/:2/:3/results/:4",
      annotations: Context.empty(),
      completions: {},
      handle: (uri) => Effect.gen(function*() {
        yield* rememberClient;
        const owner = yield* nativeMcpPrivateOwner;
        return yield* Effect.try({
          try: () => {
            const url = new URL(uri);
            if (url.hash) throw new McpSchema.InvalidParams({ message: "Invalid native result cursor" });
            for (const key of url.searchParams.keys()) {
              if ((key !== "offset" && key !== "limit") || url.searchParams.getAll(key).length !== 1) {
                throw new McpSchema.InvalidParams({ message: "Invalid native result cursor" });
              }
            }
            const base = `${url.protocol}//${url.host}${url.pathname}`;
            if (uri !== base + url.search) throw new McpSchema.InvalidParams({ message: "Invalid native result URI" });
            const snapshot = snapshots.get(base);
            if (snapshot === undefined || snapshot.owner !== owner) {
              throw new McpSchema.InvalidParams({
                message: "Native private result is unavailable",
                data: Schema.encodeUnknownSync(Schema.toCodecJson(failureSchema))({
                  error: new NativeControlError({ code: "resource_gap", message: "Native private result snapshot was evicted or is unavailable" }),
                }),
              });
            }
            const number = (key: string, fallback: number) => {
              const raw = url.searchParams.get(key);
              if (raw === null) return fallback;
              if (!/^(0|[1-9][0-9]*)$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
                throw new McpSchema.InvalidParams({ message: "Invalid native result cursor" });
              }
              return Number(raw);
            };
            const offset = number("offset", 0);
            const limit = number("limit", 32768);
            if (offset > snapshot.bytes.byteLength || limit < 1 || limit > 32768) {
              throw new McpSchema.InvalidParams({ message: "Invalid native result cursor" });
            }
            const result = { contents: [{ uri, mimeType: "application/json", blob: snapshot.bytes.subarray(offset, offset + limit) }] };
            const wire = Schema.encodeUnknownSync(Schema.toCodecJson(McpSchema.ReadResourceResult), { onExcessProperty: "error" })(result);
            if (new TextEncoder().encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > NativeMcpResultBytes) {
              throw new McpSchema.InvalidParams({ message: "Native result cursor exceeds the wire budget" });
            }
            return result;
          },
          catch: (error) => error instanceof McpSchema.InvalidParams
            ? error
            : new McpSchema.InvalidParams({ message: "Invalid native result resource or cursor" }),
        });
      }),
    });

    const readPage = (binding: NativeBinding, uri: string) => Effect.gen(function*() {
      yield* rememberClient;
      const page = yield* Effect.try({
        try: () => {
          const url = new URL(uri);
          if (url.hash || `${url.protocol}//${url.host}${url.pathname}` !== nativeEventsUri(binding)) throw new Error();
          for (const key of url.searchParams.keys()) if (key !== "after" && key !== "limit") throw new Error();
          if (url.searchParams.getAll("after").length > 1 || url.searchParams.getAll("limit").length > 1) throw new Error();
          const number = (key: string, fallback: number) => {
            const value = url.searchParams.get(key);
            if (value === null) return fallback;
            if (!/^(0|[1-9][0-9]*)$/.test(value)) throw new Error();
            const parsed = Number(value);
            if (!Number.isSafeInteger(parsed)) throw new Error();
            return parsed;
          };
          const after = Schema.decodeUnknownSync(NativeEventSequence)(number("after", 0));
          const limit = url.searchParams.has("limit") ? number("limit", 0) : undefined;
          if (limit !== undefined && limit < 1) throw new Error();
          return options.journal.read(binding, after, limit);
        },
        catch: () => new McpSchema.InvalidParams({ message: "Invalid native lifecycle resource cursor or binding" }),
      });
      const text = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(NativeEventPage))(page, { onExcessProperty: "error" }).pipe(
        Effect.mapError(() => new McpSchema.InternalError({ message: "Invalid or oversized native lifecycle page" })),
      );
      return { contents: [{ uri, mimeType: "application/json", text }] };
    });

    const registerBinding = (binding: NativeBinding) => Effect.gen(function*() {
      const uri = nativeEventsUri(binding);
      if (registered.has(uri)) return;
      if (options.presentation !== undefined) yield* options.presentation.register(binding).pipe(
        Effect.provideService(Scope.Scope, registrationScope),
      );
      // The closure belongs permanently to this incarnation, including after retirement.
      yield* McpServer.registerResource({
        uri,
        name: `${binding.host} native lifecycle`,
        description: "Canonical lifecycle journal; invalidation hints are not settlement receipts",
        mimeType: "application/json",
        content: readPage(binding, uri),
      });
      yield* registry.addResourceTemplate({
        template: new McpSchema.ResourceTemplate({
          uriTemplate: `${uri}{?after,limit}`,
          name: `${binding.host} native lifecycle cursor`,
          mimeType: "application/json",
        }),
        routerPath: uri.replaceAll(":", "::"),
        annotations: Context.empty(),
        completions: {},
        handle: (requestedUri) => readPage(binding, requestedUri),
      });
      registered.add(uri);
    });

    yield* registerBinding(options.binding);
    yield* Effect.acquireRelease(
      Effect.sync(() => options.journal.subscribe((event: NativeLifecycleEvent) => {
        if (event.binding.host !== options.host) return;
        pending.set(nativeEventsUri(event.binding), event.binding);
        Queue.offerUnsafe(wake, undefined);
      })),
      (unsubscribe) => Effect.sync(unsubscribe),
    );
    yield* Effect.forkScoped(Effect.forever(Effect.gen(function*() {
      yield* Queue.take(wake);
      for (const [uri, binding] of pending) {
        yield* registerBinding(binding);
        if (notificationContext === undefined) continue;
        pending.delete(uri);
        // One worker awaits delivery using an actual client's captured context, never a fabricated request.
        yield* registry.notifications["notifications/resources/updated"]({ uri }).pipe(
          Effect.provideService(McpSchema.McpRequestContext, notificationContext),
        );
      }
    })));

    for (const tool of options.tools) yield* tool.register(options.journal, rememberClient, capturePrivateResult, options.presentation);
  });

/** Plugin-owned HTTP transport leaves the installed host's RPC stdio untouched. */
export const nativeControlHttpLayer = <R = never>(
  options: NativeControlServerOptions<R> & { readonly name: string; readonly version: string },
) => Layer.unwrap(Effect.gen(function*() {
  const scope = yield* Scope.Scope;
  return Layer.effectDiscard(registerNativeControl(options)).pipe(
    Layer.provideMerge(McpServer.layerHttp({
      name: options.name, version: options.version, path: "/mcp",
      protocols: [nativeMcpProtocol(options.tools.map(tool => tool.name))], allowedOrigins: [], allowSessionTermination: true,
    })),
    Layer.provide(Layer.succeed(NativeMcpOwnerScope, scope)),
  );
}));

/** Completes at the physical stdout sink, not the SDK's layer-construction fiber. */
export class NativeMcpLifetime extends Context.Service<NativeMcpLifetime, {
  readonly closed: Effect.Effect<void, NativeControlError>;
}>()("coordinator/NativeMcpLifetime") {}

/** The host supplies its real Stdio and Crypto services and owns scoped EOF/physical cleanup. */
export const nativeControlServerLayer = <R = never>(
  options: NativeControlServerOptions<R> & { readonly name: string; readonly version: string },
) => Layer.unwrap(Effect.gen(function*() {
  const closed = yield* Deferred.make<void, NativeControlError>();
  const scope = yield* Scope.Scope;
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    const result = yield* Deferred.poll(closed);
    if (Option.isSome(result)) yield* Effect.orDie(result.value);
  }));
  return Layer.effectDiscard(registerNativeControl(options)).pipe(
    Layer.provideMerge(McpServer.layerStdio({
      name: options.name,
      version: options.version,
      protocols: [nativeMcpProtocol(options.tools.map(tool => tool.name))],
    }).pipe(Layer.provide(nativeMcpStdioLayer(closed)))),
    Layer.provideMerge(Layer.merge(
      Layer.succeed(NativeMcpLifetime, { closed: Deferred.await(closed) }),
      Layer.succeed(NativeMcpOwnerScope, scope),
    )),
  );
}));
