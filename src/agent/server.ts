import { Cause, Context, Crypto, Deferred, Effect, Layer, Option, Queue, Schema, Scope } from "effect";
import { McpSchema, McpServer } from "effect/ai";
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
  type AgentLifecycleEvent,
  type AgentTool,
  type AgentToolDescriptor,
  type AgentToolResultMetadata,
} from "./contracts.ts";
import { AgentLifecycleJournal } from "./lifecycle.ts";
import { AgentMcpOwnerScope, agentMcpPrivateOwner, type AgentPresentation } from "./interaction.ts";
import { agentMcpProtocol, agentMcpStdioLayer } from "./mcp-protocol.ts";

export type AgentMcpRequestContext = McpSchema.McpRequestContext["Service"];

const failureSchema = Schema.Struct({ error: AgentError });
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
          try: () => Schema.encodeSync(outputCodec)(result, { onExcessProperty: "error" }),
          catch: () => new AgentError({ code: "agent_failure", message: "Invalid or oversized agent tool output" }),
        });
        const text = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(value, { onExcessProperty: "error" }).pipe(
          Effect.mapError(() => new AgentError({ code: "agent_failure", message: "Invalid or oversized agent tool output" })),
        );
        const bytes = new TextEncoder().encode(text);
        let envelope: Schema.JsonObject;
        if (bytes.byteLength + 11 <= AgentMcpResultBytes) {
          envelope = { result: value };
        } else {
          let binding = input.binding;
          if (binding === undefined) binding = metadata.binding;
          if (binding === undefined) return yield* new AgentError({ code: "binding_mismatch", message: "Agent result is missing its invocation binding" });
          const resultResource = yield* capturePrivateResult(binding, bytes, yield* agentMcpPrivateOwner);
          envelope = { resultResource: Schema.encodeSync(Schema.toCodecJson(AgentPrivateResultReference))(resultResource) };
        }
        return new McpSchema.CallToolResult({ content: [], structuredContent: envelope });
      }).pipe(
        Effect.catchTag("AgentError", (error) =>
          Schema.encodeEffect(Schema.toCodecJson(failureSchema))({ error }, { onExcessProperty: "error" }).pipe(
            Effect.map((structuredContent) => new McpSchema.CallToolResult({ isError: true, content: [], structuredContent })),
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
  const scope = yield* Scope.Scope;
  return Layer.effectDiscard(registerAgentControl(options)).pipe(
    Layer.provideMerge(McpServer.layerHttp({
      name: options.name, version: options.version, path: "/mcp",
      protocols: [agentMcpProtocol(options.tools.map(tool => tool.descriptor))], allowedOrigins: [], allowSessionTermination: true,
    })),
    Layer.provide(Layer.succeed(AgentMcpOwnerScope, scope)),
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
  const scope = yield* Scope.Scope;
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
    Layer.provideMerge(Layer.merge(
      Layer.succeed(AgentMcpLifetime, { closed: Deferred.await(closed) }),
      Layer.succeed(AgentMcpOwnerScope, scope),
    )),
  );
}));
