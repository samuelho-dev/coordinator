import { AsyncLocalStorage } from "node:async_hooks";
import { isDeepStrictEqual } from "node:util";
import { Context, Crypto, Effect, FileSystem, Option, Queue, Schema, Scope } from "effect";
import { McpSchema, McpServer } from "effect/ai";
import { HttpServerRequest } from "effect/http";
import { NativeBinding, NativeControlError, NativeInputId, NativeRunId, NativePrivateResultId, NativeMcpResultBytes, NativePrivateResultBytes, nativeEventsUri, resultByteLimit } from "./contracts.js";

export const NativeHostMetadataKey = "io.coordinator.native/host";
export const NativePresentationMetadataKey = "io.coordinator.native/presentation";
export const NativeHostEndpoint = Schema.Struct({
  url: Schema.String.check(Schema.makeFilter((value) => {
    try { const url = new URL(value); return !url.username && !url.password && !url.hash && (url.protocol === "https:" || (url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))); } catch { return false; }
  })),
  authorizationFile: Schema.optional(Schema.String.check(Schema.isMinLength(1))),
});
export type NativeHostEndpoint = typeof NativeHostEndpoint.Type;
export const NativeHostToolMetadata = Schema.Struct({
  label: Schema.optional(Schema.String), hidden: Schema.optional(Schema.Boolean),
  loadMode: Schema.optional(Schema.Literals(["essential", "discoverable"])),
  readsSkillUris: Schema.optional(Schema.Boolean), progressUriTemplate: Schema.optional(Schema.String),
});
export const NativeHostResourceMetadata = Schema.Struct({
  immutable: Schema.optional(Schema.Boolean),
  contentType: Schema.optional(Schema.Literals(["text/plain", "application/json", "text/markdown"])),
  notes: Schema.optional(Schema.Array(Schema.String)),
});
export const NativeHostUriMetadata = Schema.Struct({
  scheme: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9+.-]*$/)),
  ...NativeHostResourceMetadata.fields,
  writable: Schema.optional(Schema.Boolean), writeTool: Schema.optional(Schema.String),
});
export const NativeInteractionImage = Schema.Struct({
  type: Schema.Literal("image"), data: Schema.String, mimeType: Schema.String,
  detail: Schema.optional(Schema.Literals(["auto", "low", "high", "original"])),
  url: Schema.optional(Schema.String),
  providerFile: Schema.optional(Schema.Struct({ provider: Schema.Literals(["openai", "anthropic", "google"]), id: Schema.optional(Schema.String), uri: Schema.optional(Schema.String), expiresAt: Schema.optional(Schema.Finite) })),
});
export const NativeInteractionImages = Schema.Array(NativeInteractionImage);
export const NativeHostAgentToolResult = Schema.Struct({
  content: Schema.Array(Schema.Union([
    Schema.Struct({ type: Schema.Literal("text"), text: Schema.String, textSignature: Schema.optional(Schema.String) }),
    NativeInteractionImage,
  ])),
  details: Schema.optional(Schema.Json),
  isError: Schema.optional(Schema.Boolean),
  useless: Schema.optional(Schema.Boolean),
  providerMetadata: Schema.optional(Schema.Struct({
    type: Schema.Literal("computer"),
    screenshot: Schema.Union([
      Schema.Struct({ type: Schema.Literal("computer_screenshot"), image_url: Schema.String }),
      Schema.Struct({ type: Schema.Literal("computer_screenshot"), file_id: Schema.String }),
    ]),
    acknowledgedSafetyChecks: Schema.Array(Schema.Struct({ id: Schema.String, code: Schema.optional(Schema.NullOr(Schema.String)), message: Schema.optional(Schema.NullOr(Schema.String)) })),
  })),
});
export const NativeHostResultReference = Schema.Struct({
  uri: Schema.String, mimeType: Schema.Literal("application/json"),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67108864 })),
});
export const NativeHostToolEnvelope = Schema.Union([Schema.Struct({ result: NativeHostAgentToolResult }), Schema.Struct({ resultResource: NativeHostResultReference })]).check(resultByteLimit);
export const NativeHostProgressPage = Schema.Struct({
  after: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  updates: Schema.Array(Schema.Struct({ seq: Schema.Int.check(Schema.isGreaterThan(0)), result: NativeHostAgentToolResult })),
  done: Schema.Boolean,
});
export const NativeHostProgressEnvelope = Schema.Union([Schema.Struct({ result: NativeHostProgressPage }), Schema.Struct({ resultResource: NativeHostResultReference })]).check(resultByteLimit);
export const NativeHostUriWriteResult = Schema.Struct({ written: Schema.Boolean });
export const nativeInteractionUnsupported = () => new NativeControlError({ code: "native_failure", message: "This interaction is not supported by the associated MCP client" });
const safeFailure = () => new NativeControlError({ code: "native_failure", message: "Native MCP interaction failed" });
// HTTP transport client IDs belong to individual requests; the admitted MCP session owns private bytes.
export const nativeMcpPrivateOwner = Effect.gen(function*() {
  const context = yield* McpSchema.McpRequestContext;
  const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
  const sessionId = Option.isSome(request) ? request.value.headers["mcp-session-id"] : undefined;
  return sessionId === undefined ? `connection:${context.clientId}` : `http:${sessionId}`;
});
/** Reverse clients are leased by the server scope, including detached Source runs. */
export class NativeMcpOwnerScope extends Context.Service<NativeMcpOwnerScope, Scope.Scope>()("coordinator/NativeMcpOwnerScope") {}

type Invocation = {
  readonly context: McpSchema.McpRequestContext["Service"];
  readonly owner: string;
  readonly client: McpSchema.McpReverseClient;
  readonly signal: AbortSignal;
  readonly scope: Scope.Scope;
  readonly uuid: () => Promise<string>;
  readonly registry: Option.Option<McpServer.McpServer["Service"]>;
};
const invocation = new AsyncLocalStorage<Invocation>();
export const nativeMcpInvocation = () => {
  const current = invocation.getStore();
  if (!current) throw nativeInteractionUnsupported();
  if (current.signal.aborted) throw new NativeControlError({ code: "transport_closed", message: "Native MCP interaction owner closed" });
  return current;
};
export const withNativeMcpRequest = <A>(body: (signal: AbortSignal) => Promise<A>, connectionSignal?: AbortSignal): Effect.Effect<A, NativeControlError, McpSchema.McpRequestContext | McpSchema.McpServerClient | Crypto.Crypto> => Effect.scoped(Effect.gen(function*() {
  const context = yield* McpSchema.McpRequestContext;
  const serverClient = yield* McpSchema.McpServerClient;
  const ownerScope = yield* Effect.serviceOption(NativeMcpOwnerScope);
  const scope = Option.isSome(ownerScope) ? ownerScope.value : yield* Scope.Scope;
  const client = yield* serverClient.getClient.pipe(Effect.provideService(Scope.Scope, scope));
  const crypto = yield* Crypto.Crypto;
  const registry = yield* Effect.serviceOption(McpServer.McpServer);
  const owner = yield* nativeMcpPrivateOwner;
  return yield* Effect.tryPromise({
    try: async (signal) => {
      const lifetime = new AbortController();
      const cancel = () => lifetime.abort();
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) cancel();
      const runSignal = connectionSignal === undefined ? lifetime.signal : AbortSignal.any([lifetime.signal, connectionSignal]);
      try {
        return await invocation.run({ context, owner, client, scope, signal: runSignal, registry, uuid: () => Effect.runPromise(crypto.randomUUIDv4()) }, () => body(runSignal));
      } finally {
        // Successful receipts release their request signal, not any still-owned Source run.
        signal.removeEventListener("abort", cancel);
      }
    },
    catch: (error) => error instanceof NativeControlError ? error : safeFailure(),
  });
}));
export type NativeInteractionSession = {
  readonly nativeBinding: NativeBinding;
  readonly nativeRunId: NativeRunId | null;
  emitNativeInput(kind: "input_requested" | "input_resolved", inputKind: "question" | "approval", inputId: NativeInputId, runId: NativeRunId | null, binding?: NativeBinding): unknown;
};
export type NativeDialogOptions = { signal?: AbortSignal; timeout?: number; onTimeout?: () => void; onTimeoutStart?: () => void };
export const nativeElicit = async (
  getSession: () => NativeInteractionSession,
  request: McpSchema.ElicitRequestParams,
  kind: "question" | "approval",
  options?: NativeDialogOptions,
): Promise<McpSchema.ElicitResult> => {
  const current = nativeMcpInvocation();
  const mode = request.mode ?? "form";
  const capability = current.context.clientCapabilities.elicitation;
  // Match the negotiated SDK adapter: legacy elicitation {} supports form, never URL.
  if (capability === undefined || (mode === "url" ? capability.url === undefined : capability.form === undefined && capability.url !== undefined)) throw nativeInteractionUnsupported();
  const session = getSession();
  const binding = Object.freeze({ ...Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" })(session.nativeBinding) });
  const runId = session.nativeRunId;
  const inputId = Schema.decodeUnknownSync(NativeInputId)(await current.uuid());
  const abort = new AbortController();
  const cancel = () => abort.abort();
  current.signal.addEventListener("abort", cancel, { once: true });
  options?.signal?.addEventListener("abort", cancel, { once: true });
  if (current.signal.aborted || options?.signal?.aborted) cancel();
  let timedOut = false;
  let requested = false;
  const timer = options?.timeout === undefined ? undefined : setTimeout(() => { timedOut = true; cancel(); }, options.timeout);
  try {
    session.emitNativeInput("input_requested", kind, inputId, runId, binding);
    requested = true;
    options?.onTimeoutStart?.();
    const result = await Effect.runPromise(current.client.elicit(request).pipe(Effect.mapError(() => safeFailure())), { signal: abort.signal });
    if (getSession() !== session || !isDeepStrictEqual(getSession().nativeBinding, binding)) throw new NativeControlError({ code: "binding_mismatch", message: "Native interaction owner changed" });
    return result;
  } catch (error) {
    if (abort.signal.aborted && !current.signal.aborted) {
      if (timedOut) options?.onTimeout?.();
      return new McpSchema.ElicitDeclineResult({ action: "cancel" });
    }
    throw error;
  } finally {
    clearTimeout(timer);
    current.signal.removeEventListener("abort", cancel);
    options?.signal?.removeEventListener("abort", cancel);
    if (requested) session.emitNativeInput("input_resolved", kind, inputId, runId, binding);
  }
};
export const nativeForm = (message: string, properties: Record<string, McpSchema.PrimitiveSchemaDefinition>, required = Object.keys(properties)) =>
  new McpSchema.ElicitRequestFormParams({ mode: "form", message, requestedSchema: { type: "object", properties, required } });
export const nativeOAuthUrl = async (getSession: () => NativeInteractionSession, url: string, message: string, options?: NativeDialogOptions) => {
  const elicitationId = await nativeMcpInvocation().uuid();
  const result = await nativeElicit(getSession, new McpSchema.ElicitRequestURLParams({ mode: "url", url, message, elicitationId }), "approval", options);
  return { ...result, elicitationId };
};
export const nativeOAuthComplete = async (elicitationId: string) => {
  const current = nativeMcpInvocation();
  if (Option.isNone(current.registry)) throw nativeInteractionUnsupported();
  await Effect.runPromise(current.registry.value.notifyElicitationComplete({ clientId: current.context.clientId, elicitationId }), { signal: current.signal });
};
export const readNativeHostAuthorization = (endpoint: NativeHostEndpoint) => Effect.gen(function*() {
  if (endpoint.authorizationFile === undefined) return undefined;
  const fs = yield* FileSystem.FileSystem;
  const stat = yield* fs.stat(endpoint.authorizationFile).pipe(Effect.mapError(() => safeFailure()));
  if (stat.type !== "File" || (stat.mode & 0o077) !== 0 || Option.isNone(stat.uid) || stat.uid.value !== process.getuid?.()) return yield* safeFailure();
  const value = (yield* fs.readFileString(endpoint.authorizationFile).pipe(Effect.mapError(() => safeFailure()))).trim();
  if (!value || /[\r\n]/.test(value)) return yield* safeFailure();
  return value;
});
const presentationSequence = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const presentationKey = Schema.String.check(Schema.isMaxLength(4096));
export const NativePresentationCursor = Schema.Struct({ binding: NativeBinding, after: presentationSequence });
export type NativePresentationCursor = typeof NativePresentationCursor.Type;
export const NativePresentationPage = Schema.Struct({
  binding: NativeBinding, after: presentationSequence, highWater: presentationSequence,
  nextAfter: presentationSequence, more: Schema.Boolean,
  gap: Schema.NullOr(Schema.Struct({ from: presentationSequence, through: presentationSequence })),
  entries: Schema.Array(Schema.Struct({
    binding: NativeBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey,
    snapshot: NativeHostResultReference,
  })).check(Schema.isMaxLength(256)),
});
export type NativePresentationPage = typeof NativePresentationPage.Type;
export const NativePresentationValue = Schema.Struct({
  binding: NativeBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey, value: Schema.Json,
});
export type NativePresentationValue = typeof NativePresentationValue.Type;
export const nativePresentationUri = (binding: NativeBinding) => nativeEventsUri(binding).replace(/\/events$/, "/presentation");
export type NativePresentation = {
  readonly write: (key: string, value: Schema.Json, expectedBinding?: NativeBinding) => void;
  readonly register: (binding: NativeBinding) => Effect.Effect<void, NativeControlError, McpServer.McpServer | Scope.Scope>;
  readonly flush: () => Promise<void>;
};
export const createNativePresentation = (getSession: () => NativeInteractionSession): NativePresentation => {
  type Record = { binding: NativeBinding; seq: number; key: string; bytes: Uint8Array; current: Invocation; uri?: string };
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
  let failure: NativeControlError | undefined;
  const waiters = new Set<{ resolve: () => void; reject: (error: NativeControlError) => void }>();
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
  const register: NativePresentation["register"] = binding => Effect.gen(function*() {
    if (failure) return yield* failure;
    const captured = Object.freeze({ ...Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" })(binding) });
    const uri = nativePresentationUri(captured);
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
            Schema.decodeUnknownSync(NativePrivateResultId)(id);
            pending.delete(record);
            if (!retained.has(record)) continue;
            record.uri = `${nativePresentationUri(record.binding)}/snapshots/${id}`;
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
      const owner = yield* nativeMcpPrivateOwner;
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
            entries: new Array<NativePresentationPage["entries"][number]>(),
          };
          const candidates = ledger?.records.filter(record => record.seq > after) ?? [];
          const result = { contents: [{ uri: requestedUri, mimeType: "application/json", text: "" }] };
          for (let index = 0; index <= Math.min(limit, candidates.length); index++) {
            page.more = candidates.length > page.entries.length;
            result.contents[0]!.text = Schema.encodeUnknownSync(Schema.fromJsonString(NativePresentationPage))(page, { onExcessProperty: "error" });
            const wire = Schema.encodeUnknownSync(Schema.toCodecJson(McpSchema.ReadResourceResult))(result);
            if (utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > NativeMcpResultBytes) {
              page.entries.pop();
              page.nextAfter = page.entries.at(-1)?.seq ?? after;
              page.more = true;
              result.contents[0]!.text = Schema.encodeUnknownSync(Schema.fromJsonString(NativePresentationPage))(page);
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
      resource: new McpSchema.Resource({ uri, name: `${captured.host} private presentation`, mimeType: "application/json", _meta: { [NativePresentationMetadataKey]: { private: true, binding: captured } } }),
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
        const owner = yield* nativeMcpPrivateOwner;
        return yield* Effect.try({
          try: () => {
            const url = new URL(requestedUri);
            const base = `${url.protocol}//${url.host}${url.pathname}`;
            const snapshot = snapshots.get(base);
            if (!snapshot || snapshot.current.owner !== owner || nativePresentationUri(snapshot.binding) !== uri || url.hash || requestedUri !== base + url.search) throw unavailable();
            for (const key of url.searchParams.keys()) if (!["offset", "limit"].includes(key) || url.searchParams.getAll(key).length !== 1) throw unavailable();
            const rawOffset = url.searchParams.get("offset") ?? "0", rawLimit = url.searchParams.get("limit") ?? "32768";
            if (!/^(0|[1-9][0-9]*)$/.test(rawOffset) || !/^[1-9][0-9]*$/.test(rawLimit)) throw unavailable();
            const offset = Number(rawOffset), limit = Number(rawLimit);
            if (!Number.isSafeInteger(offset) || offset > snapshot.bytes.byteLength || !Number.isSafeInteger(limit) || limit > 32768) throw unavailable();
            const result = { contents: [{ uri: requestedUri, mimeType: "application/json", blob: snapshot.bytes.subarray(offset, offset + limit) }] };
            const wire = Schema.encodeUnknownSync(Schema.toCodecJson(McpSchema.ReadResourceResult))(result);
            if (utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > NativeMcpResultBytes) throw unavailable();
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
      const current = nativeMcpInvocation();
      const binding = Object.freeze({ ...Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" })(getSession().nativeBinding) });
      if (expectedBinding !== undefined && !isDeepStrictEqual(expectedBinding, binding)) throw new NativeControlError({ code: "binding_mismatch", message: "Native presentation owner changed" });
      const uri = nativePresentationUri(binding);
      const clients = ledgers.get(uri);
      if (!clients || wake === undefined || Option.isNone(current.registry)) throw nativeInteractionUnsupported();
      Schema.decodeUnknownSync(presentationKey)(key);
      const bytes = utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Json))(value, { onExcessProperty: "error" }));
      if (bytes.byteLength < 1 || bytes.byteLength > NativePrivateResultBytes) throw safeFailure();
      let ledger = clients.get(current.owner);
      if (!ledger) { ledger = { highWater: 0, evictedThrough: 0, records: [] }; clients.set(current.owner, ledger); }
      if (ledger.highWater === Number.MAX_SAFE_INTEGER) { failure = safeFailure(); throw failure; }
      if (!hints.has(ledger) && hints.size >= 1024) { failure = safeFailure(); throw failure; }
      const record: Record = { binding, seq: ++ledger.highWater, key, bytes, current };
      while (retainedBytes + bytes.byteLength > NativePrivateResultBytes || retained.size >= 64) {
        const oldest = retained.values().next().value;
        if (!oldest) break;
        retained.delete(oldest); pending.delete(oldest);
        if (oldest.uri) snapshots.delete(oldest.uri);
        retainedBytes -= oldest.bytes.byteLength;
        const owner = ledgers.get(nativePresentationUri(oldest.binding))!.get(oldest.current.owner)!;
        owner.evictedThrough = oldest.seq;
        owner.records.shift();
      }
      retained.add(record); pending.add(record); retainedBytes += bytes.byteLength; ledger.records.push(record);
      hints.set(ledger, { uri, current });
      Queue.offerUnsafe(wake, undefined);
    },
  };
};
export const createNativeObservationPublisher = (getSession: () => NativeInteractionSession, presentation: NativePresentation) => {
  const current = nativeMcpInvocation();
  const session = getSession();
  const binding = Object.freeze({ ...Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" })(session.nativeBinding) });
  return (channel: string, value: Schema.Json): void => {
    if (getSession() !== session || !isDeepStrictEqual(session.nativeBinding, binding) || current.signal.aborted) throw nativeInteractionUnsupported();
    invocation.run(current, () => presentation.write(`observation/${channel}`, value, binding));
  };
};
export const publishNativeMcpObservation = (binding: NativeBinding, channel: string, value: Schema.Json, presentation: NativePresentation): void => {
  nativeMcpInvocation();
  presentation.write(`observation/${channel}`, value, binding);
};
