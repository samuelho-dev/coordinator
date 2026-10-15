import { AsyncLocalStorage } from "node:async_hooks";
import { isDeepStrictEqual } from "node:util";
import { Context, Crypto, Effect, Option, Queue, Schema, Scope } from "effect";
import { McpSchema, McpServer } from "effect/ai";
import { HttpServerRequest } from "effect/http";
import { AgentBinding, AgentError, AgentPrivateResultId, AgentMcpResultBytes, AgentPrivateResultBytes, agentEventsUri } from "./contracts.ts";

export const AgentPresentationMetadataKey = "io.coordinator.agent/presentation";
export const AgentHostResultReference = Schema.Struct({
  uri: Schema.String, mimeType: Schema.Literal("application/json"),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67108864 })),
});
export const agentInteractionUnsupported = () => new AgentError({ code: "agent_failure", message: "This interaction is not supported by the associated MCP client" });
const safeFailure = () => new AgentError({ code: "agent_failure", message: "Agent MCP interaction failed" });
// HTTP transport client IDs belong to individual requests; the admitted MCP session owns private bytes.
export const agentMcpPrivateOwner = Effect.gen(function*() {
  const context = yield* McpSchema.McpRequestContext;
  const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
  const sessionId = Option.isSome(request) ? request.value.headers["mcp-session-id"] : undefined;
  return sessionId === undefined ? `connection:${context.clientId}` : `http:${sessionId}`;
});
export class AgentMcpOwnerScope extends Context.Service<AgentMcpOwnerScope, Scope.Scope>()("coordinator/AgentMcpOwnerScope") {}

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
const presentationSequence = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const presentationKey = Schema.String.check(Schema.isMaxLength(4096));
export const AgentPresentationCursor = Schema.Struct({ binding: AgentBinding, after: presentationSequence });
export type AgentPresentationCursor = typeof AgentPresentationCursor.Type;
export const AgentPresentationPage = Schema.Struct({
  binding: AgentBinding, after: presentationSequence, highWater: presentationSequence,
  nextAfter: presentationSequence, more: Schema.Boolean,
  gap: Schema.NullOr(Schema.Struct({ from: presentationSequence, through: presentationSequence })),
  entries: Schema.Array(Schema.Struct({
    binding: AgentBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey,
    snapshot: AgentHostResultReference,
  })).check(Schema.isMaxLength(256)),
});
export type AgentPresentationPage = typeof AgentPresentationPage.Type;
export const AgentPresentationValue = Schema.Struct({
  binding: AgentBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey, value: Schema.Json,
});
export type AgentPresentationValue = typeof AgentPresentationValue.Type;
export const agentPresentationUri = (binding: AgentBinding) => agentEventsUri(binding).replace(/\/events$/, "/presentation");
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
      Schema.decodeSync(presentationKey)(key);
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
