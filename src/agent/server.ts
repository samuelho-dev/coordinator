import { AsyncLocalStorage } from "node:async_hooks";
import { isDeepStrictEqual } from "node:util";
import { Cause, Context, Crypto, Deferred, Effect, Formatter, Layer, Option, Queue, Schema, Scope, Sink, Stdio, Stream } from "effect";
import { McpProtocol, McpSchema, McpServer } from "effect/ai";
import { Headers, HttpServerRequest } from "effect/http";
import type { Rpc, RpcGroup } from "effect/rpc";
import {
  AgentError,
  AgentEventSequence,
  AgentEventPage,
  AgentPromptResult,
  AgentTools,
  agentToolDescriptors,
  AgentBinding,
  AgentHost,
  AgentMcpResultBytes,
  AgentPrivateResultBytes,
  AgentPrivateResultId,
  AgentPrivateResultReference,
  agentPrivateResultUri,
  agentEventsUri,
  AgentMcpWireBytes,
  AgentLifecycleJournal,
  AgentPresentationMetadataKey,
  AgentPresentationPage,
  AgentPresentationValue,
  agentPresentationUri,
  type AgentLifecycleEvent,
  type AgentTool,
  type AgentToolDescriptor,
  type AgentToolResultMetadata,
} from "./protocol.ts";

export type AgentMcpRequestContext = McpSchema.McpRequestContext["Service"];

const failureSchema = Schema.Struct({ error: AgentError });
const encodeToolResult = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(McpSchema.CallToolResult)));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonEffect = Schema.encodeEffect(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" });
type CapturePrivateResult = (
  binding: AgentBinding,
  bytes: Uint8Array,
  owner: string,
) => Effect.Effect<AgentPrivateResultReference, AgentError>;


export type AgentToolRegistration<R = never> = {
  readonly descriptor: Pick<AgentTool, "name" | "hosts">;
  readonly register: (
    journal: AgentLifecycleJournal,
    rememberClient: Effect.Effect<AgentMcpRequestContext, never, McpSchema.McpRequestContext>,
    capturePrivateResult: CapturePrivateResult,
    presentation?: AgentPresentation,
  ) => Effect.Effect<void, never, R | McpServer.McpServer>;
};

/** The canonical descriptor ties the handler to its exact input and output codecs. */
export const agentTool = <I extends Schema.ConstraintCodec<{ readonly binding?: AgentBinding | undefined }, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, A extends O["Type"] = O["Type"], R = never>(
  descriptor: Omit<AgentToolDescriptor<I, O>, "inspectOutput" | "decodeOutput"> & {
    readonly inspectOutput: (output: NoInfer<A>) => AgentToolResultMetadata;
  },
  handler: (
    input: I["Type"],
    context: AgentMcpRequestContext,
  ) => Effect.Effect<A, AgentError, R | McpSchema.McpRequestContext | McpSchema.McpServerClient>,
) => ({
  descriptor,
  register: (
    journal: AgentLifecycleJournal,
    rememberClient: Effect.Effect<AgentMcpRequestContext, never, McpSchema.McpRequestContext>,
    capturePrivateResult: CapturePrivateResult,
    presentation?: AgentPresentation,
  ) => Effect.gen(function*() {
    const registry = yield* McpServer.McpServer;
    const services = yield* Effect.context<R>();
    const inputCodec = Schema.toCodecJson(descriptor.input);
    const outputCodec = Schema.toCodecJson(descriptor.output);
    const encodeOutput = Schema.encodeSync(outputCodec, { onExcessProperty: "error" });
    yield* registry.addTool({
      tool: new McpSchema.Tool({
        name: descriptor.name,
        description: descriptor.description,
        inputSchema: descriptor.inputSchema,
        outputSchema: descriptor.outputSchema,
      }),
      annotations: Context.empty(),
      handle: (payload: unknown) => Effect.gen(function*() {
        const context = yield* rememberClient;
        const invocation = yield* Effect.context<McpSchema.McpRequestContext>();
        const client = Context.getOrUndefined(invocation, McpSchema.McpServerClient);
        if (client === undefined) {
          return yield* new McpSchema.InternalError({ message: "Agent tool requires an initialized MCP client" });
        }
        const input = yield* Schema.decodeUnknownEffect<Schema.toCodecJson<I>>(inputCodec)(payload, { onExcessProperty: "error" }).pipe(
          Effect.mapError(() => new AgentError({ code: "invalid_input", message: "Invalid agent tool input" })),
        );
        const result = yield* handler(input, context).pipe(
          Effect.provideContext(Context.add(Context.merge(services, invocation), McpSchema.McpServerClient, client)),
        );
        const metadata = descriptor.inspectOutput(result);
        if (metadata.prompt !== undefined) {
          yield* awaitPromptSettlement(journal, metadata.prompt);
          if (presentation !== undefined) yield* Effect.tryPromise({
            try: () => presentation.flush(),
            catch: () => new AgentError({ code: "agent_failure", message: "Agent presentation publication failed" }),
          });
        }
        const value = yield* Effect.try({
          try: () => encodeOutput(result),
          catch: () => new AgentError({ code: "agent_failure", message: "Invalid or oversized agent tool output" }),
        });
        const text = yield* encodeJsonEffect(value).pipe(
          Effect.mapError(() => new AgentError({ code: "agent_failure", message: "Invalid or oversized agent tool output" })),
        );
        const bytes = new TextEncoder().encode(text);
        let response: McpSchema.CallToolResult | undefined;
        if (bytes.byteLength + 11 <= AgentMcpResultBytes) {
          const structuredContent = { result: value };
          const candidate = new McpSchema.CallToolResult({ content: [McpSchema.TextContent.make({ type: "text", text: Formatter.formatJson(structuredContent, { space: 2 }) })], structuredContent });
          if (new TextEncoder().encode(encodeToolResult(candidate)).byteLength <= AgentMcpResultBytes) response = candidate;
        }
        if (response === undefined) {
          let binding = input.binding;
          if (binding === undefined) binding = metadata.binding;
          if (binding === undefined) return yield* new AgentError({ code: "binding_mismatch", message: "Agent result is missing its invocation binding" });
          const resultResource = yield* capturePrivateResult(binding, bytes, yield* agentMcpPrivateOwner);
          const structuredContent = { resultResource: Schema.encodeSync(Schema.toCodecJson(AgentPrivateResultReference))(resultResource) };
          response = new McpSchema.CallToolResult({ content: [McpSchema.TextContent.make({ type: "text", text: Formatter.formatJson(structuredContent, { space: 2 }) })], structuredContent });
          if (new TextEncoder().encode(encodeToolResult(response)).byteLength > AgentMcpResultBytes) {
            return yield* new AgentError({ code: "agent_failure", message: "Agent result reference exceeds its wire bound" });
          }
        }
        return response;
      }).pipe(
        Effect.catchTag("AgentError", (error) =>
          Schema.encodeEffect(Schema.toCodecJson(failureSchema))({ error }, { onExcessProperty: "error" }).pipe(
            Effect.map((structuredContent) => new McpSchema.CallToolResult({ isError: true, content: [McpSchema.TextContent.make({ type: "text", text: Formatter.formatJson(structuredContent, { space: 2 }) })], structuredContent })),
            Effect.mapError(() => new McpSchema.InternalError({ message: "Invalid agent failure envelope" })),
          )),
        Effect.catchCause((cause) => Cause.hasDies(cause)
          ? Effect.fail(new McpSchema.InternalError({ message: "Agent tool execution failed" }))
          : Effect.failCause(cause)),
      ),
    });
  }),
});

export type AgentServerOptions<R = never> = {
  readonly host: AgentHost;
  readonly binding: AgentBinding;
  readonly journal: AgentLifecycleJournal;
  readonly tools: readonly AgentToolRegistration<R>[];
  readonly presentation?: AgentPresentation;
};

const awaitPromptSettlement = (journal: AgentLifecycleJournal, result: typeof AgentPromptResult.Type) => {
  if (result.disposition === "handled") {
    return result.admission.disposition === "handled" && result.admission.runId === null
      ? Effect.void
      : Effect.fail(new AgentError({ code: "agent_failure", message: "Agent handled result does not match its admission" }));
  }
  const admission = result.admission;
  if (admission.runId === null || admission.runId !== result.final.runId ||
    agentEventsUri(admission.binding) !== agentEventsUri(result.final.binding)) {
    return Effect.fail(new AgentError({ code: "agent_failure", message: "Agent prompt result does not match its admission" }));
  }
  return Effect.callback<void, AgentError>((resume) => {
    const matches = (event: AgentLifecycleEvent) =>
      event.kind === "run_settled" && event.runId === admission.runId &&
      agentEventsUri(event.binding) === agentEventsUri(admission.binding);
    const settle = (event: AgentLifecycleEvent) => {
      if (event.kind !== "run_settled") return;
      resume(event.outcome === result.final.outcome
        ? Effect.void
        : Effect.fail(new AgentError({ code: "agent_failure", message: "Agent prompt outcome disagrees with settlement" })));
    };
    const unsubscribe = journal.subscribe((event) => { if (matches(event)) settle(event); });
    try {
      let after = admission.seq;
      for (;;) {
        const page = journal.read(admission.binding, after);
        if (page.gap !== null) {
          resume(Effect.fail(new AgentError({ code: "resource_gap", message: "Agent settlement history is unavailable" })));
          break;
        }
        const settled = page.entries.find(matches);
        if (settled !== undefined) { settle(settled); break; }
        if (!page.more) break;
        after = page.nextAfter;
      }
    } catch {
      resume(Effect.fail(new AgentError({ code: "resource_gap", message: "Agent settlement history is unavailable" })));
    }
    return Effect.sync(unsubscribe);
  });
};

/** Registers implemented public capabilities; absent operations are never advertised. */
export const registerAgentControl = <R = never>(options: AgentServerOptions<R>) =>
  Effect.gen(function*() {
    if (options.binding.host !== options.host) {
      return yield* new AgentError({ code: "binding_mismatch", message: "Server binding host does not match" });
    }
    const allowed = agentToolDescriptors.filter((descriptor) => descriptor.hosts.some((host) => host === options.host)).map((descriptor) => descriptor.name);
    const supplied = options.tools.map((tool) => tool.descriptor.name);
    if (new Set(supplied).size !== supplied.length || !supplied.includes(AgentTools.getState.name) ||
      supplied.some((name) => !allowed.includes(name))) {
      return yield* new AgentError({ code: "unsupported_operation", message: "Agent handler inventory contains missing bootstrap, duplicate or foreign capabilities" });
    }
    const registry = yield* McpServer.McpServer;
    const registrationScope = yield* Scope.Scope;
    const wake = yield* Queue.sliding<void>(1);
    const pending = new Map<string, AgentBinding>();
    const registered = new Set<string>();
    let notificationContext: AgentMcpRequestContext | undefined;
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
      if (bytes.byteLength < 1 || bytes.byteLength > AgentPrivateResultBytes) {
        return yield* new AgentError({ code: "agent_failure", message: "Agent private result exceeds snapshot capacity" });
      }
      const id = Schema.decodeSync(AgentPrivateResultId)(yield* crypto.randomUUIDv4().pipe(
        Effect.mapError(() => new AgentError({ code: "agent_failure", message: "Unable to allocate a private result identifier" })),
      ));
      const uri = agentPrivateResultUri(binding, id);
      while (retainedBytes + bytes.byteLength > AgentPrivateResultBytes || snapshots.size >= 64) {
        const oldest = snapshots.entries().next().value;
        if (oldest === undefined) break;
        snapshots.delete(oldest[0]);
        retainedBytes -= oldest[1].bytes.byteLength;
      }
      const reference: AgentPrivateResultReference = {
        binding, uri, mimeType: "application/json", byteLength: bytes.byteLength,
      };
      snapshots.set(uri, { bytes, owner });
      retainedBytes += bytes.byteLength;
      return reference;
    });
    yield* registry.addResourceTemplate({
      template: new McpSchema.ResourceTemplate({
        uriTemplate: "agent://{host}/{session}/{incarnation}/{generation}/results/{resultId}{?offset,limit}",
        name: "Private agent result bytes",
        description: "Immutable private output bytes; opaque references are supplied only by their originating tool call",
        mimeType: "application/json",
      }),
      routerPath: "agent:://:0/:1/:2/:3/results/:4",
      annotations: Context.empty(),
      completions: {},
      handle: (uri) => Effect.gen(function*() {
        yield* rememberClient;
        const owner = yield* agentMcpPrivateOwner;
        return yield* Effect.try({
          try: () => {
            const url = new URL(uri);
            if (url.hash) throw new McpSchema.InvalidParams({ message: "Invalid agent result cursor" });
            for (const key of url.searchParams.keys()) {
              if ((key !== "offset" && key !== "limit") || url.searchParams.getAll(key).length !== 1) {
                throw new McpSchema.InvalidParams({ message: "Invalid agent result cursor" });
              }
            }
            const base = `${url.protocol}//${url.host}${url.pathname}`;
            if (uri !== base + url.search) throw new McpSchema.InvalidParams({ message: "Invalid agent result URI" });
            const snapshot = snapshots.get(base);
            if (snapshot === undefined || snapshot.owner !== owner) {
              throw new McpSchema.InvalidParams({
                message: "Agent private result is unavailable",
                data: Schema.encodeSync(Schema.toCodecJson(failureSchema))({
                  error: new AgentError({ code: "resource_gap", message: "Agent private result snapshot was evicted or is unavailable" }),
                }),
              });
            }
            const number = (key: string, fallback: number) => {
              const raw = url.searchParams.get(key);
              if (raw === null) return fallback;
              if (!/^(0|[1-9][0-9]*)$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
                throw new McpSchema.InvalidParams({ message: "Invalid agent result cursor" });
              }
              return Number(raw);
            };
            const offset = number("offset", 0);
            const limit = number("limit", 32768);
            if (offset > snapshot.bytes.byteLength || limit < 1 || limit > 32768) {
              throw new McpSchema.InvalidParams({ message: "Invalid agent result cursor" });
            }
            const result = { contents: [{ uri, mimeType: "application/json", blob: snapshot.bytes.subarray(offset, offset + limit) }] };
            const wire = Schema.encodeSync(Schema.toCodecJson(McpSchema.ReadResourceResult), { onExcessProperty: "error" })(result);
            if (new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > AgentMcpResultBytes) {
              throw new McpSchema.InvalidParams({ message: "Agent result cursor exceeds the wire budget" });
            }
            return result;
          },
          catch: (error) => error instanceof McpSchema.InvalidParams
            ? error
            : new McpSchema.InvalidParams({ message: "Invalid agent result resource or cursor" }),
        });
      }),
    });

    const readPage = (binding: AgentBinding, uri: string) => Effect.gen(function*() {
      yield* rememberClient;
      const page = yield* Effect.try({
        try: () => {
          const url = new URL(uri);
          if (url.hash || `${url.protocol}//${url.host}${url.pathname}` !== agentEventsUri(binding)) throw new Error();
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
          const after = Schema.decodeUnknownSync(AgentEventSequence)(number("after", 0));
          const limit = url.searchParams.has("limit") ? number("limit", 0) : undefined;
          if (limit !== undefined && limit < 1) throw new Error();
          return options.journal.read(binding, after, limit);
        },
        catch: () => new McpSchema.InvalidParams({ message: "Invalid agent lifecycle resource cursor or binding" }),
      });
      const text = yield* Schema.encodeEffect(Schema.fromJsonString(AgentEventPage))(page, { onExcessProperty: "error" }).pipe(
        Effect.mapError(() => new McpSchema.InternalError({ message: "Invalid or oversized agent lifecycle page" })),
      );
      return { contents: [{ uri, mimeType: "application/json", text }] };
    });

    const registerBinding = (binding: AgentBinding) => Effect.gen(function*() {
      const uri = agentEventsUri(binding);
      if (registered.has(uri)) return;
      if (options.presentation !== undefined) yield* options.presentation.register(binding).pipe(
        Effect.provideService(Scope.Scope, registrationScope),
      );
      // The closure belongs permanently to this incarnation, including after retirement.
      yield* McpServer.registerResource({
        uri,
        name: `${binding.host} agent lifecycle`,
        description: "Canonical lifecycle journal; invalidation hints are not settlement receipts",
        mimeType: "application/json",
        content: readPage(binding, uri),
      });
      yield* registry.addResourceTemplate({
        template: new McpSchema.ResourceTemplate({
          uriTemplate: `${uri}{?after,limit}`,
          name: `${binding.host} agent lifecycle cursor`,
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
      Effect.sync(() => options.journal.subscribe((event: AgentLifecycleEvent) => {
        if (event.binding.host !== options.host) return;
        pending.set(agentEventsUri(event.binding), event.binding);
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
export const agentHttpLayer = <R = never>(
  options: AgentServerOptions<R> & { readonly name: string; readonly version: string },
) => Layer.unwrap(Effect.gen(function*() {
  return Layer.effectDiscard(registerAgentControl(options)).pipe(
    Layer.provideMerge(McpServer.layerHttp({
      name: options.name, version: options.version, path: "/mcp",
      protocols: [agentMcpProtocol(options.tools.map(tool => tool.descriptor))], allowedOrigins: [], allowSessionTermination: true,
    })),
  );
}));

/** Completes at the physical stdout sink, not the SDK's layer-construction fiber. */
export class AgentMcpLifetime extends Context.Service<AgentMcpLifetime, {
  readonly closed: Effect.Effect<void, AgentError>;
}>()("coordinator/AgentMcpLifetime") {}

/** The host supplies its real Stdio and Crypto services and owns scoped EOF/physical cleanup. */
export const agentStdioLayer = <R = never>(
  options: AgentServerOptions<R> & { readonly name: string; readonly version: string },
) => Layer.unwrap(Effect.gen(function*() {
  const closed = yield* Deferred.make<void, AgentError>();
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    const result = yield* Deferred.poll(closed);
    if (Option.isSome(result)) yield* Effect.orDie(result.value);
  }));
  return Layer.effectDiscard(registerAgentControl(options)).pipe(
    Layer.provideMerge(McpServer.layerStdio({
      name: options.name,
      version: options.version,
      protocols: [agentMcpProtocol(options.tools.map(tool => tool.descriptor))],
    }).pipe(Layer.provide(agentMcpStdioLayer(closed)))),
    Layer.provideMerge(Layer.succeed(AgentMcpLifetime, { closed: Deferred.await(closed) })),
  );
}));

export const agentInteractionUnsupported = () => new AgentError({ code: "agent_failure", message: "This interaction is not supported by the associated MCP client" });
const safeFailure = () => new AgentError({ code: "agent_failure", message: "Agent MCP interaction failed" });
// HTTP transport client IDs belong to individual requests; the admitted MCP session owns private bytes.
export const agentMcpPrivateOwner = Effect.gen(function*() {
  const context = yield* McpSchema.McpRequestContext;
  const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
  const sessionId = Option.isSome(request) ? request.value.headers["mcp-session-id"] : undefined;
  return sessionId === undefined ? `connection:${context.clientId}` : `http:${sessionId}`;
});

type Invocation = {
  readonly context: McpSchema.McpRequestContext["Service"];
  readonly owner: string;
  readonly signal: AbortSignal;
  readonly uuid: () => Promise<string>;
  readonly registry: Option.Option<McpServer.McpServer["Service"]>;
};
const invocation = new AsyncLocalStorage<Invocation>();
export const agentMcpInvocation = () => {
  const current = invocation.getStore();
  if (!current) throw agentInteractionUnsupported();
  if (current.signal.aborted) throw new AgentError({ code: "transport_closed", message: "Agent MCP interaction owner closed" });
  return current;
};
export const withAgentMcpRequest = <A>(body: (signal: AbortSignal) => Promise<A>, connectionSignal?: AbortSignal): Effect.Effect<A, AgentError, McpSchema.McpRequestContext | Crypto.Crypto> => Effect.scoped(Effect.gen(function*() {
  const context = yield* McpSchema.McpRequestContext;
  const crypto = yield* Crypto.Crypto;
  const registry = yield* Effect.serviceOption(McpServer.McpServer);
  const owner = yield* agentMcpPrivateOwner;
  return yield* Effect.tryPromise({
    try: async (signal) => {
      const lifetime = new AbortController();
      const cancel = () => lifetime.abort();
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) cancel();
      const runSignal = connectionSignal === undefined ? lifetime.signal : AbortSignal.any([lifetime.signal, connectionSignal]);
      try {
        return await invocation.run({ context, owner, signal: runSignal, registry, uuid: () => Effect.runPromise(crypto.randomUUIDv4()) }, () => body(runSignal));
      } finally {
        // Successful receipts release their request signal, not any still-owned Source run.
        signal.removeEventListener("abort", cancel);
      }
    },
    catch: (error) => error instanceof AgentError ? error : safeFailure(),
  });
}));
export type AgentInteractionSession = {
  readonly agentBinding: AgentBinding;
};
export type AgentPresentation = {
  readonly write: (key: string, value: Schema.Json, expectedBinding?: AgentBinding) => void;
  readonly register: (binding: AgentBinding) => Effect.Effect<void, AgentError, McpServer.McpServer | Scope.Scope>;
  readonly flush: () => Promise<void>;
};
export const createAgentPresentation = (getSession: () => AgentInteractionSession): AgentPresentation => {
  type Record = { binding: AgentBinding; seq: number; key: string; bytes: Uint8Array; current: Invocation; uri?: string };
  type Ledger = { highWater: number; evictedThrough: number; records: Record[] };
  const ledgers = new Map<string, Map<string, Ledger>>();
  const snapshots = new Map<string, Record>();
  const retained = new Set<Record>();
  const pending = new Set<Record>();
  const hints = new Map<Ledger, { uri: string; current: Invocation }>();
  const registered = new Set<string>();
  const utf8 = new TextEncoder();
  let retainedBytes = 0;
  let wake: Queue.Queue<void> | undefined;
  let busy = false;
  let registering = 0;
  let failure: AgentError | undefined;
  const waiters = new Set<{ resolve: () => void; reject: (error: AgentError) => void }>();
  const flush = (): Promise<void> => {
    if (failure) return Promise.reject(failure);
    if (!busy && registering === 0 && pending.size === 0 && hints.size === 0) return Promise.resolve();
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    waiters.add({ resolve, reject });
    return promise;
  };
  const finish = () => {
    if (!failure && (busy || registering !== 0 || pending.size !== 0 || hints.size !== 0)) return;
    for (const waiter of waiters) failure ? waiter.reject(failure) : waiter.resolve();
    waiters.clear();
  };
  const unavailable = () => new McpSchema.InvalidParams({ message: "Private presentation unavailable or invalid cursor" });
  const register: AgentPresentation["register"] = binding => Effect.gen(function*() {
    if (failure) return yield* failure;
    const captured = Object.freeze({ ...binding });
    const uri = agentPresentationUri(captured);
    if (registered.has(uri)) return;
    const registry = yield* McpServer.McpServer;
    registered.add(uri);
    registering++;
    if (wake === undefined) {
      const scope = yield* Scope.Scope;
      wake = yield* Queue.sliding<void>(1);
      const queue = wake;
      yield* Effect.forkIn(Effect.forever(Effect.gen(function*() {
        yield* Queue.take(queue);
        busy = true;
        yield* Effect.gen(function*() {
          for (const record of pending) {
            const id = yield* Effect.tryPromise({ try: () => record.current.uuid(), catch: () => safeFailure() });
            Schema.decodeSync(AgentPrivateResultId)(id);
            pending.delete(record);
            if (!retained.has(record)) continue;
            record.uri = `${agentPresentationUri(record.binding)}/snapshots/${id}`;
            snapshots.set(record.uri, record);
          }
          for (const [ledger, hint] of hints) {
            hints.delete(ledger);
            yield* registry.notifications["notifications/resources/updated"]({ uri: hint.uri }).pipe(
              Effect.provideService(McpSchema.McpRequestContext, hint.current.context),
              Effect.mapError(() => safeFailure()),
            );
          }
        }).pipe(Effect.catchCause(() => Effect.sync(() => { failure = safeFailure(); })));
        busy = false;
        if (failure || (pending.size === 0 && hints.size === 0)) finish();
      })), scope);
      yield* Scope.addFinalizer(scope, Effect.sync(() => {
        failure = safeFailure();
        pending.clear(); hints.clear(); snapshots.clear(); retained.clear(); ledgers.clear();
        finish();
      }));
    }
    const clients = new Map<string, Ledger>();
    ledgers.set(uri, clients);
    const readPage = (requestedUri: string) => Effect.gen(function*() {
      const owner = yield* agentMcpPrivateOwner;
      yield* Effect.tryPromise({ try: flush, catch: unavailable });
      return yield* Effect.try({
        try: () => {
          const url = new URL(requestedUri);
          if (url.hash || `${url.protocol}//${url.host}${url.pathname}` !== uri || requestedUri !== uri + url.search) throw unavailable();
          for (const key of url.searchParams.keys()) if (!["after", "limit"].includes(key) || url.searchParams.getAll(key).length !== 1) throw unavailable();
          const rawAfter = url.searchParams.get("after") ?? "0";
          const rawLimit = url.searchParams.get("limit") ?? "256";
          if (!/^(0|[1-9][0-9]*)$/.test(rawAfter) || !/^[1-9][0-9]*$/.test(rawLimit)) throw unavailable();
          const after = Number(rawAfter), limit = Number(rawLimit);
          const ledger = clients.get(owner);
          const highWater = ledger?.highWater ?? 0;
          if (!Number.isSafeInteger(after) || after > highWater || !Number.isSafeInteger(limit) || limit > 256) throw unavailable();
          const page = {
            binding: captured, after, highWater, nextAfter: after, more: false,
            gap: ledger && after < ledger.evictedThrough ? { from: after + 1, through: ledger.evictedThrough } : null,
            entries: new Array<AgentPresentationPage["entries"][number]>(),
          };
          const candidates = ledger?.records.filter(record => record.seq > after) ?? [];
          const result = { contents: [{ uri: requestedUri, mimeType: "application/json", text: "" }] };
          for (let index = 0; index <= Math.min(limit, candidates.length); index++) {
            page.more = candidates.length > page.entries.length;
            result.contents[0]!.text = Schema.encodeSync(Schema.fromJsonString(AgentPresentationPage))(page, { onExcessProperty: "error" });
            const wire = Schema.encodeSync(Schema.toCodecJson(McpSchema.ReadResourceResult))(result);
            if (utf8.encode(Schema.encodeSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > AgentMcpResultBytes) {
              page.entries.pop();
              page.nextAfter = page.entries.at(-1)?.seq ?? after;
              page.more = true;
              result.contents[0]!.text = Schema.encodeSync(Schema.fromJsonString(AgentPresentationPage))(page);
              if (page.entries.length === 0) throw unavailable();
              return result;
            }
            if (index === Math.min(limit, candidates.length)) return result;
            const record = candidates[index]!;
            if (record.uri === undefined) throw unavailable();
            page.entries.push({ binding: captured, seq: record.seq, key: record.key, snapshot: { uri: record.uri, byteLength: record.bytes.byteLength, mimeType: "application/json" } });
            page.nextAfter = record.seq;
          }
          throw unavailable();
        }, catch: unavailable,
      });
    });
    yield* registry.addResource({
      resource: new McpSchema.Resource({ uri, name: `${captured.host} private presentation`, mimeType: "application/json", _meta: { [AgentPresentationMetadataKey]: { private: true, binding: captured } } }),
      annotations: Context.empty(), handle: readPage(uri).pipe(Effect.mapError(() => new McpSchema.InternalError({ message: "Private presentation unavailable" }))),
    });
    yield* registry.addResourceTemplate({
      template: new McpSchema.ResourceTemplate({ uriTemplate: `${uri}{?after,limit}`, name: "Private presentation cursor", mimeType: "application/json" }),
      routerPath: uri.replaceAll(":", "::"), annotations: Context.empty(), completions: {}, handle: readPage,
    });
    yield* registry.addResourceTemplate({
      template: new McpSchema.ResourceTemplate({ uriTemplate: `${uri}/snapshots/{snapshot}{?offset,limit}`, name: "Private presentation immutable bytes", mimeType: "application/json" }),
      routerPath: `${uri.replaceAll(":", "::")}/snapshots/:0`, annotations: Context.empty(), completions: {},
      handle: requestedUri => Effect.gen(function*() {
        const owner = yield* agentMcpPrivateOwner;
        return yield* Effect.try({
          try: () => {
            const url = new URL(requestedUri);
            const base = `${url.protocol}//${url.host}${url.pathname}`;
            const snapshot = snapshots.get(base);
            if (!snapshot || snapshot.current.owner !== owner || agentPresentationUri(snapshot.binding) !== uri || url.hash || requestedUri !== base + url.search) throw unavailable();
            for (const key of url.searchParams.keys()) if (!["offset", "limit"].includes(key) || url.searchParams.getAll(key).length !== 1) throw unavailable();
            const rawOffset = url.searchParams.get("offset") ?? "0", rawLimit = url.searchParams.get("limit") ?? "32768";
            if (!/^(0|[1-9][0-9]*)$/.test(rawOffset) || !/^[1-9][0-9]*$/.test(rawLimit)) throw unavailable();
            const offset = Number(rawOffset), limit = Number(rawLimit);
            if (!Number.isSafeInteger(offset) || offset > snapshot.bytes.byteLength || !Number.isSafeInteger(limit) || limit > 32768) throw unavailable();
            const result = { contents: [{ uri: requestedUri, mimeType: "application/json", blob: snapshot.bytes.subarray(offset, offset + limit) }] };
            const wire = Schema.encodeSync(Schema.toCodecJson(McpSchema.ReadResourceResult))(result);
            if (utf8.encode(Schema.encodeSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > AgentMcpResultBytes) throw unavailable();
            return result;
          }, catch: unavailable,
        });
      }),
    });
    registering--;
    finish();
  }).pipe(Effect.catchCause(() => {
    failure = safeFailure();
    finish();
    return Effect.fail(failure);
  }));
  return {
    register, flush,
    write: (key, value, expectedBinding) => {
      if (failure) throw failure;
      const current = agentMcpInvocation();
      const binding = Object.freeze({ ...getSession().agentBinding });
      if (expectedBinding !== undefined && !isDeepStrictEqual(expectedBinding, binding)) throw new AgentError({ code: "binding_mismatch", message: "Agent presentation owner changed" });
      const uri = agentPresentationUri(binding);
      const clients = ledgers.get(uri);
      if (!clients || wake === undefined || Option.isNone(current.registry)) throw agentInteractionUnsupported();
      Schema.decodeSync(AgentPresentationValue.fields.key)(key);
      const bytes = utf8.encode(Schema.encodeSync(Schema.fromJsonString(Schema.Json))(value, { onExcessProperty: "error" }));
      if (bytes.byteLength < 1 || bytes.byteLength > AgentPrivateResultBytes) throw safeFailure();
      let ledger = clients.get(current.owner);
      if (!ledger) { ledger = { highWater: 0, evictedThrough: 0, records: [] }; clients.set(current.owner, ledger); }
      if (ledger.highWater === Number.MAX_SAFE_INTEGER) { failure = safeFailure(); throw failure; }
      if (!hints.has(ledger) && hints.size >= 1024) { failure = safeFailure(); throw failure; }
      const record: Record = { binding, seq: ++ledger.highWater, key, bytes, current };
      while (retainedBytes + bytes.byteLength > AgentPrivateResultBytes || retained.size >= 64) {
        const oldest = retained.values().next().value;
        if (!oldest) break;
        retained.delete(oldest); pending.delete(oldest);
        if (oldest.uri) snapshots.delete(oldest.uri);
        retainedBytes -= oldest.bytes.byteLength;
        const owner = ledgers.get(agentPresentationUri(oldest.binding))!.get(oldest.current.owner)!;
        owner.evictedThrough = oldest.seq;
        owner.records.shift();
      }
      retained.add(record); pending.add(record); retainedBytes += bytes.byteLength; ledger.records.push(record);
      hints.set(ledger, { uri, current });
      Queue.offerUnsafe(wake, undefined);
    },
  };
};

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
            let bytes = encoder.encode(encodeJson({ tools: [], nextCursor: `${prefix}${result.tools.length}` })).byteLength;
            let end = offset;
            while (end < result.tools.length) {
              const added = encoder.encode(encodeJson(result.tools[end]!)).byteLength + (end === offset ? 0 : 1);
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
