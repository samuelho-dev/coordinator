import { Client, ProtocolError, SdkError, SdkErrorCode, StreamableHTTPClientTransport, type Transport, type ClientCapabilities, type ClientContext, type ElicitRequest, type ElicitResult } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { Buffer } from "node:buffer";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { isDeepStrictEqual } from "node:util";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { Cause, Effect, Schema, Scope } from "effect";
import type { Exit } from "effect";
import { McpSchema } from "effect/ai";
import { NativePresentationCursor, NativePresentationPage, nativePresentationUri, type NativePresentationValue } from "./interaction.js";
export { nativeMcpWireByteGate } from "./mcp-protocol.js";
import {
  NativeBinding,
  NativeControlError,
  NativePluginEndpoint,
  NativeEventPage,
  NativeEventSequence,
  NativeHost,
  NativeLifecycleEvent,
  NativePromptAdmission,
  NativeMcpResultBytes,
  NativeMcpWireBytes,
  NativeMcpProtocolVersion,
  NativePrivateResultId,
  NativePrivateResultReference,
  nativePrivateResultUri,
  NativeTools,
  nativeEventsUri,
  nativeToolInputSchema,
  nativeToolOutputSchema,
  type NativeBinding as NativeBindingType,
  type NativeEventPage as NativeEventPageType,
  type NativeEventSequence as NativeEventSequenceType,
  type NativeHost as NativeHostType,
  type NativeLifecycleEvent as NativeLifecycleEventType,
  type NativePromptAdmission as NativePromptAdmissionType,
  type NativePromptInput as NativePromptInputType,
  type NativePromptResult as NativePromptResultType,
  type NativeToolInput,
  type NativeToolName,
  type NativeToolOutput,
} from "./contracts.js";
import { Record } from "effect";

const nonEmpty = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
const timeout = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 2_147_483_647 }));
export const NativeClientLaunch = Schema.Struct({
  host: NativeHost,
  command: nonEmpty,
  args: Schema.optional(Schema.Array(Schema.String.check(Schema.isMaxLength(16000))).check(Schema.isMaxLength(256))),
  env: Schema.Record(Schema.String.check(Schema.isPattern(/^[A-Za-z_][A-Za-z0-9_]*$/)), Schema.String.check(Schema.isMaxLength(262144))),
  cwd: nonEmpty,
  maxBufferSize: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 268435456 }))),
  connectTimeoutMs: timeout,
  plugin: Schema.optional(Schema.Struct({ endpointFile: nonEmpty, token: Schema.String.check(Schema.isMinLength(32), Schema.isMaxLength(480)) })),
});
export type NativeClientLaunch = typeof NativeClientLaunch.Type;

export const NativeClientConnection = Schema.Struct({ host: NativeHost, connectTimeoutMs: timeout });
export type NativeClientConnection = typeof NativeClientConnection.Type;

export const NativeCallBudget = Schema.Struct({ timeoutMs: timeout });
export type NativeCallBudget = typeof NativeCallBudget.Type;
export const NativeLifecycleCursor = Schema.Struct({ binding: NativeBinding, after: NativeEventSequence });
export type NativeLifecycleCursor = typeof NativeLifecycleCursor.Type;

export type NativeElicitation = Readonly<{
  capabilities: NonNullable<ClientCapabilities["elicitation"]>;
  handle: (request: ElicitRequest, context: ClientContext) => Promise<ElicitResult>;
}>;
export type NativePromptCallOptions = NativeCallBudget & Readonly<{
  after: NativeEventSequenceType;
  onAdmission: (admission: NativePromptAdmissionType) => void | Promise<void>;
  onLifecycle?: (event: NativeLifecycleEventType) => void | Promise<void>;
}>;
export type NativeLifecycleWatch = Readonly<{ drain: Effect.Effect<void, NativeControlError>; connectionClosed: Effect.Effect<never, NativeControlError>; close: Effect.Effect<void, NativeControlError> }>;
export type NativePresentationWatch = Readonly<{
  drain: Effect.Effect<void, NativeControlError>;
  close: Effect.Effect<void, NativeControlError>;
  connectionClosed: Effect.Effect<never, NativeControlError>;
}>;

const fail = (code: ConstructorParameters<typeof NativeControlError>[0]["code"], message: string) =>
  new NativeControlError({ code, message: message.slice(0, 1024) });
const transportFailure = () => fail("transport_closed", "Native MCP connection closed before the operation was proven complete");
const nativeFailure = () => fail("native_failure", "Native MCP operation failed");
const deadlineFailure = () => fail("deadline_exceeded", "Native MCP operation exceeded its caller-provided deadline");
const failureEnvelope = Schema.Struct({ error: NativeControlError });
const waitFor = <A>(promise: Promise<A>, signal: AbortSignal): Promise<A> => {
  if (signal.aborted) {
    // The caller may already have started a read/drain before observing abort.
    // Its eventual failure still needs an observer after this waiter detaches.
    void promise.catch(() => undefined);
    return Promise.reject(signal.reason);
  }
  return new Promise<A>((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
};
const queryEventsUri = (binding: NativeBindingType, after: NativeEventSequenceType, limit = 256) =>
  `${nativeEventsUri(binding)}?after=${after}&limit=${limit}`;

const transportEffect = <A>(run: (signal: AbortSignal) => Promise<A>, deadline = false): Effect.Effect<A, NativeControlError> =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => {
      if (cause instanceof NativeControlError) return cause;
      if (ProtocolError.isInstance(cause) && cause.data !== undefined) {
        const decoded = Schema.decodeUnknownOption(Schema.toCodecJson(failureEnvelope), { onExcessProperty: "error" })(cause.data);
        if (decoded._tag === "Some") return decoded.value.error;
      }
      if (SdkError.isInstance(cause) && cause.code === SdkErrorCode.RequestTimeout) return deadlineFailure();
      if (SdkError.isInstance(cause) && (cause.code === SdkErrorCode.ConnectionClosed || cause.code === SdkErrorCode.NotConnected || cause.code === SdkErrorCode.SendFailed)) return transportFailure();
      return deadline && cause instanceof Error && cause.name === "AbortError" ? deadlineFailure() : nativeFailure();
    },
  });

const assertBinding = (expected: NativeBindingType, actual: NativeBindingType) => {
  if (expected.host !== actual.host || expected.sessionId !== actual.sessionId || expected.incarnation !== actual.incarnation || expected.generation !== actual.generation) throw fail("binding_mismatch", "Native response binding does not match the requested owner generation");
};

export type NativeClient<ProcessID extends number | null = number> = Readonly<{
  host: NativeHostType;
  binding: NativeBindingType;
  pid: ProcessID;
  capabilities: readonly NativeToolName[];
  connectionClosed: Effect.Effect<void, NativeControlError>;
  connectionError: Effect.Effect<never, NativeControlError>;
  observedProcessClosed: Effect.Effect<void, NativeControlError>;
  call<N extends NativeToolName>(name: N, input: NativeToolInput<N>, budget: NativeCallBudget): Effect.Effect<NativeToolOutput<N>, NativeControlError>;
  /** Success requires retained admission and whole-run lifecycle proof; the deadline includes callbacks and closing drain. */
  prompt(input: NativePromptInputType, options: NativePromptCallOptions): Effect.Effect<NativePromptResultType, NativeControlError>;
  watchLifecycle(cursor: NativeLifecycleCursor, onEvent: (event: NativeLifecycleEventType) => void | Promise<void>): Effect.Effect<NativeLifecycleWatch, NativeControlError, Scope.Scope>;
  watchPresentation(cursor: NativePresentationCursor, onValue: (value: NativePresentationValue) => void | Promise<void>): Effect.Effect<NativePresentationWatch, NativeControlError, Scope.Scope>;
}>;

export const connectNativeClient = (
  untrustedLaunch: NativeClientLaunch,
  elicitation?: NativeElicitation,
): Effect.Effect<NativeClient, NativeControlError, Scope.Scope> => Effect.gen(function*() {
  const launch = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativeClientLaunch, { onExcessProperty: "error" })(untrustedLaunch), catch: () => fail("invalid_input", "Invalid native MCP launch configuration") });
  let resolveProcessClosed!: () => void;
  const processClosedPromise = new Promise<void>((resolve) => { resolveProcessClosed = resolve; });
  const envArgs = Object.entries(launch.env).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `${key}=${value}`);
  let startAttempted = false;
  let control: Transport;
  let readPid: () => number | null;
  let closeProcess: () => Promise<void>;
  let endpointBinding: NativeBindingType | undefined;
  if (launch.plugin === undefined) {
    const transport = new StdioClientTransport({
      command: "/usr/bin/env",
      args: ["-i", ...envArgs, launch.command, ...(launch.args ?? [])],
      env: {}, cwd: launch.cwd, stderr: "ignore",
      maxBufferSize: Math.min(launch.maxBufferSize ?? NativeMcpWireBytes, NativeMcpWireBytes),
    });
    const sdkStart = transport.start.bind(transport);
    transport.start = () => { startAttempted = true; return sdkStart(); };
    const sdkClose = transport.close.bind(transport);
    let closePromise: Promise<void> | undefined;
    transport.close = () => closePromise ??= sdkClose();
    transport.onclose = resolveProcessClosed;
    readPid = () => transport.pid;
    closeProcess = () => transport.close();
    control = transport;
  } else {
    const plugin = launch.plugin;
    let http: StreamableHTTPClientTransport | undefined;
    let closed = false;
    let processClosed = false;
    let child: ChildProcessWithoutNullStreams | undefined;
    const missing = Schema.Struct({ code: Schema.Literal("ENOENT") });
    const bridge: Transport = {
      hasPerRequestStream: true,
      get sessionId() { return http?.sessionId; },
      setProtocolVersion(version) {
        if (http === undefined) throw transportFailure();
        http.setProtocolVersion(version);
      },
      async start() {
        if (closed) throw transportFailure();
        startAttempted = true;
        const acquired = spawn("/usr/bin/env", ["-i", ...envArgs, launch.command, ...(launch.args ?? [])], {
          cwd: launch.cwd, env: {}, stdio: ["pipe", "pipe", "pipe"],
        });
        child = acquired;
        acquired.once("close", () => { processClosed = true; resolveProcessClosed(); bridge.onclose?.(); });
        acquired.on("error", (error) => { if (!closed) bridge.onerror?.(error); });
        acquired.stdin.on("error", (error) => { if (!closed) bridge.onerror?.(error); });
        acquired.stdout.on("error", (error) => { if (!closed) bridge.onerror?.(error); });
        acquired.stderr.on("error", (error) => { if (!closed) bridge.onerror?.(error); });
        // Native RPC is not MCP: drain bytes without a JSON-RPC decoder or buffer.
        acquired.stdout.resume();
        acquired.stderr.resume();
        await new Promise<void>((resolve, reject) => { acquired.once("spawn", () => resolve()); acquired.once("error", reject); });
        const deadline = performance.now() + launch.connectTimeoutMs;
        for (;;) {
          if (closed || processClosed) throw transportFailure();
          if (performance.now() >= deadline) throw deadlineFailure();
          let endpoint: typeof NativePluginEndpoint.Type | undefined;
          try {
            const file = await open(plugin.endpointFile, constants.O_RDONLY | constants.O_NOFOLLOW);
            try {
              const stat = await file.stat();
              if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.size > NativeMcpWireBytes) throw fail("invalid_input", "Private bounded plugin endpoint file required");
              endpoint = Schema.decodeUnknownSync(Schema.fromJsonString(NativePluginEndpoint), { onExcessProperty: "error" })(await file.readFile("utf8"));
            } finally { await file.close(); }
          } catch (error) {
            if (Schema.decodeUnknownOption(missing)(error)._tag === "None") throw fail("invalid_input", "Plugin endpoint is unavailable or invalid");
          }
          if (endpoint !== undefined) {
            if (endpoint.binding.host !== launch.host || closed || processClosed) throw fail("binding_mismatch", "Plugin endpoint owner changed during startup");
            endpointBinding = endpoint.binding;
            http = new StreamableHTTPClientTransport(new URL(endpoint.url), {
              requestInit: { headers: { Authorization: `Bearer ${plugin.token}` }, redirect: "error" },
            });
            http.onclose = () => bridge.onclose?.();
            http.onerror = (error) => bridge.onerror?.(error);
            http.onmessage = (message) => bridge.onmessage?.(message);
            await http.start();
            return;
          }
          await new Promise<void>((resolve) => setTimeout(resolve, 25));
        }
      },
      async send(message, options) {
        if (closed || http === undefined) throw transportFailure();
        await http.send(message, options);
      },
      async close() { closed = true; await http?.close(); },
    };
    readPid = () => child?.pid ?? null;
    let closePromise: Promise<void> | undefined;
    closeProcess = () => closePromise ??= (async () => {
      const owned = child;
      if (owned === undefined) { resolveProcessClosed(); return; }
      if (processClosed) return;
      owned.stdin.end();
      owned.kill("SIGTERM");
      await Promise.race([processClosedPromise, delay(1000, undefined, { ref: false })]);
      if (!processClosed) {
        owned.kill("SIGKILL");
        await Promise.race([processClosedPromise, delay(3000, undefined, { ref: false }).then(() => { throw transportFailure(); })]);
      }
    })();
    control = bridge;
  }
  const native = yield* connectNativeTransport(
    { host: launch.host, connectTimeoutMs: launch.connectTimeoutMs },
    control,
    {
      get pid() {
        const pid = readPid();
        if (pid === null) throw transportFailure();
        return pid;
      },
      observedProcessClosed: processClosedPromise,
      async close() {
        await closeProcess();
        // Wiring can fail before start; in that case no process was acquired.
        if (!startAttempted) resolveProcessClosed();
      },
    },
    elicitation,
  );
  if (launch.plugin !== undefined) {
    const announced = endpointBinding;
    if (announced === undefined) return yield* Effect.fail(fail("binding_mismatch", "Plugin endpoint did not announce its owner"));
    yield* Effect.try({ try: () => assertBinding(announced, native.binding), catch: () => fail("binding_mismatch", "Plugin endpoint and MCP readback owners differ") });
  }
  return native;
});

export const connectNativeTransport = <ProcessID extends number | null>(
  untrustedConnection: NativeClientConnection,
  transport: Transport,
  ownership: Readonly<{ pid: ProcessID; observedProcessClosed: Promise<void>; close: () => Promise<void> }>,
  elicitation?: NativeElicitation,
): Effect.Effect<NativeClient<ProcessID>, NativeControlError, Scope.Scope> => Effect.gen(function*() {
  const processClosedPromise = ownership.observedProcessClosed;
  // Observe rejection immediately, even if discovery fails before returning a client.
  void processClosedPromise.catch(() => undefined);
  let connectedClient: Client | undefined;
  let teardownPromise: Promise<Exit.Exit<void, NativeControlError>> | undefined;
  const teardown = () => teardownPromise ??= Promise.resolve().then(() => Effect.runPromiseExit(Effect.gen(function*() {
    const connection = yield* Effect.exit(transportEffect(async () => { await connectedClient?.close(); }));
    // SDK closure is logical; always attempt physical ownership cleanup.
    const physical = yield* Effect.exit(transportEffect(async () => {
      await ownership.close();
      await processClosedPromise;
    }));
    if (connection._tag === "Failure" && physical._tag === "Failure") return yield* Effect.failCause(Cause.combine(connection.cause, physical.cause));
    if (physical._tag === "Failure") return yield* Effect.failCause(physical.cause);
    if (connection._tag === "Failure") return yield* Effect.failCause(connection.cause);
  })));
  yield* Effect.acquireRelease(
    Effect.void,
    () => Effect.promise(teardown).pipe(
      Effect.flatMap(exit => exit._tag === "Failure" ? Effect.failCause(exit.cause) : Effect.void),
      Effect.catchCause(cause => Effect.failCause(Cause.fromReasons<never>(cause.reasons.map(reason =>
        reason._tag === "Fail" ? Cause.makeDieReason(reason.error) : reason)))),
    ),
  );
  const launch = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativeClientConnection, { onExcessProperty: "error" })(untrustedConnection), catch: () => fail("invalid_input", "Invalid native MCP connection configuration") });
  let resolveConnectionClosed!: () => void;
  const connectionClosedPromise = new Promise<void>((resolve) => { resolveConnectionClosed = resolve; });
  let connectionClosed = false;
  let rejectConnectionError!: (error: NativeControlError) => void;
  const connectionErrorPromise = new Promise<never>((_, reject) => { rejectConnectionError = reject; });
  void connectionErrorPromise.catch(() => undefined);
  const sdkEffect = <A>(run: (signal: AbortSignal) => Promise<A>, deadline = false) =>
    transportEffect(signal => connectionClosed
      ? Promise.reject(transportFailure())
      : Promise.race([run(signal), connectionClosedPromise.then(() => { throw transportFailure(); })]), deadline);
  const client = yield* Effect.try({ try: () => new Client(
    { name: "@samuelho/coordinator-native-control", version: "0.1.0" },
    {
      capabilities: elicitation ? { elicitation: elicitation.capabilities } : {},
      supportedProtocolVersions: [NativeMcpProtocolVersion],
      versionNegotiation: { mode: "legacy" },
      enforceStrictCapabilities: true,
      listMaxPages: 16,
    },
  ), catch: () => nativeFailure() });
  connectedClient = client;
  if (elicitation) client.setRequestHandler("elicitation/create", (request, context) => elicitation.handle(request, context));
  client.onclose = () => { connectionClosed = true; resolveConnectionClosed(); };
  // SDK onerror also reports late replies after a valid request timeout. Only
  // faults emitted by its transport own physical teardown.
  const previousTransportError = transport.onerror;
  transport.onerror = error => {
    connectionClosed = true;
    resolveConnectionClosed();
    rejectConnectionError(transportFailure());
    // Cache the complete exit: the finalizer must still report cleanup uncertainty.
    void teardown();
    previousTransportError?.(error);
  };

  yield* sdkEffect((signal) => client.connect(transport, { signal, timeout: launch.connectTimeoutMs }), true).pipe(Effect.withSpan("NativeClient.connect"));
  if (client.getNegotiatedProtocolVersion() !== NativeMcpProtocolVersion) return yield* Effect.fail(fail("unsupported_operation", "Native server negotiated an unsupported protocol version"));
  const pid = yield* Effect.try({ try: () => ownership.pid, catch: () => transportFailure() });

  const serverCapabilities = client.getServerCapabilities();
  if (!serverCapabilities?.tools || !serverCapabilities.resources) return yield* Effect.fail(fail("unsupported_operation", "Native server lacks required tools or resources"));
  const supportsSubscriptions = serverCapabilities.resources.subscribe === true;
  const allowedNames = Record.keys(NativeTools).filter((name) => NativeTools[name].hosts.some((host) => host === launch.host));
  const discovery = yield* sdkEffect((signal) => client.listTools(undefined, { signal, timeout: launch.connectTimeoutMs })).pipe(Effect.withSpan("NativeClient.discoverTools"));
  const expectedNames = discovery.tools.map(({ name }) => name);
  if (new Set(expectedNames).size !== expectedNames.length || !expectedNames.includes("native.get_state") ||
    expectedNames.some((name) => !allowedNames.some((allowed) => allowed === name))) {
    return yield* Effect.fail(fail("unsupported_operation", "Native server advertises duplicate, foreign or missing bootstrap capabilities"));
  }
  const capabilities = allowedNames.filter((name) => expectedNames.includes(name));
  for (const tool of discovery.tools) {
    const name = Schema.decodeUnknownSync(Schema.Literals(Record.keys(NativeTools)))(tool.name);
    const contract = NativeTools[name];
    const inputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(contract.input), { onExcessProperty: "error" });
    const outputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(contract.structuredOutput), { onExcessProperty: "error" });
    const inputSchema = Object.keys(inputDocument.definitions).length === 0 ? inputDocument.schema : { ...inputDocument.schema, $defs: inputDocument.definitions };
    const outputSchema = Object.keys(outputDocument.definitions).length === 0 ? outputDocument.schema : { ...outputDocument.schema, $defs: outputDocument.definitions };
    if (!isDeepStrictEqual(tool.inputSchema, inputSchema) || !isDeepStrictEqual(tool.outputSchema, outputSchema)) {
      return yield* Effect.fail(fail("unsupported_operation", `Native server schema discovery mismatch for ${name}`));
    }
  }
  const [resources, templates] = yield* Effect.all([
    sdkEffect((signal) => client.listResources(undefined, { signal, timeout: launch.connectTimeoutMs })),
    sdkEffect((signal) => client.listResourceTemplates(undefined, { signal, timeout: launch.connectTimeoutMs })),
  ]).pipe(Effect.withSpan("NativeClient.discoverResources"));
  if (resources.resources.length < 1 || !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate.endsWith("/events{?after,limit}"))) {
    return yield* Effect.fail(fail("unsupported_operation", "Native lifecycle resources are not discoverable"));
  }
  const lifecycleHints = new Map<string, Set<() => void>>();
  const lifecycleSubscriptions = new Map<string, number>();
  const subscriptionChanges = new Map<string, Promise<void>>();
  const changeSubscription = (uri: string, acquire: boolean, acquired?: () => void) => sdkEffect((signal) => {
    const previous = subscriptionChanges.get(uri) ?? Promise.resolve();
    const change = previous.catch(() => undefined).then(async () => {
      if (signal.aborted) throw signal.reason;
      const count = lifecycleSubscriptions.get(uri) ?? 0;
      if (acquire) {
        if (count === 0) await client.subscribeResource({ uri }, { signal, timeout: launch.connectTimeoutMs });
        lifecycleSubscriptions.set(uri, count + 1);
        acquired?.();
      } else if (count > 1) {
        lifecycleSubscriptions.set(uri, count - 1);
      } else if (count === 1) {
        lifecycleSubscriptions.delete(uri);
        await client.unsubscribeResource({ uri }, { signal, timeout: launch.connectTimeoutMs });
      }
    });
    subscriptionChanges.set(uri, change);
    void change.finally(() => { if (subscriptionChanges.get(uri) === change) subscriptionChanges.delete(uri); }).catch(() => undefined);
    return waitFor(change, signal);
  });
  client.setNotificationHandler("notifications/resources/updated", (notification) => {
    for (const hint of lifecycleHints.get(notification.params.uri) ?? []) hint();
  });

  const call = <N extends NativeToolName>(name: N, untrustedInput: NativeToolInput<N>, budget: NativeCallBudget) => Effect.gen(function*() {
    if (connectionClosed) return yield* Effect.fail(transportFailure());
    const input = yield* Effect.try({ try: () => Schema.decodeUnknownSync(nativeToolInputSchema(name), { onExcessProperty: "error" })(untrustedInput), catch: () => fail("invalid_input", "Invalid native tool input") });
    const checkedBudget = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativeCallBudget, { onExcessProperty: "error" })(budget), catch: () => fail("invalid_input", "Invalid native request budget") });
    const deadlineAt = performance.now() + checkedBudget.timeoutMs;
    if (!expectedNames.includes(name)) return yield* Effect.fail(fail("unsupported_operation", "Native tool is unavailable on the connected host"));
    const encoded = yield* Effect.try({
      try: () => Schema.encodeUnknownSync(Schema.toCodecJson(nativeToolInputSchema(name)), { onExcessProperty: "error" })(input),
      catch: () => fail("invalid_input", "Invalid native tool input"),
    });
    const arguments_ = yield* Effect.try({ try: () => Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown), { onExcessProperty: "error" })(encoded), catch: () => fail("invalid_input", "Native tool input is not an object") });
    const response = yield* sdkEffect((signal) => client.callTool({ name, arguments: arguments_ }, { signal, timeout: checkedBudget.timeoutMs, maxTotalTimeout: checkedBudget.timeoutMs }), true);
    if (response.isError) {
      if (response.structuredContent === undefined) return yield* Effect.fail(nativeFailure());
      return yield* Effect.fail(yield* Effect.try({ try: () => Schema.decodeUnknownSync(Schema.toCodecJson(failureEnvelope), { onExcessProperty: "error" })(response.structuredContent).error, catch: () => nativeFailure() }));
    }
    if (response.structuredContent === undefined) return yield* Effect.fail(nativeFailure());
    const envelope = yield* Effect.try({ try: () => Schema.decodeUnknownSync(Schema.toCodecJson(NativeTools[name].structuredOutput), { onExcessProperty: "error" })(response.structuredContent), catch: () => nativeFailure() });
    if ("result" in envelope) return yield* Effect.try({ try: () => Schema.decodeUnknownSync(nativeToolOutputSchema(name), { onExcessProperty: "error" })(envelope.result), catch: () => nativeFailure() });
    const reference = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativePrivateResultReference, { onExcessProperty: "error" })(envelope.resultResource), catch: () => nativeFailure() });
    const bytes = yield* sdkEffect(async (signal) => {
      if (reference.binding.host !== launch.host) throw fail("binding_mismatch", "Native result resource belongs to another host");
      if (arguments_.binding !== undefined) assertBinding(Schema.decodeUnknownSync(NativeBinding, { onExcessProperty: "error" })(arguments_.binding), reference.binding);
      const resultId = Schema.decodeUnknownSync(NativePrivateResultId, { onExcessProperty: "error" })(reference.uri.split("/").at(-1));
      if (reference.uri !== nativePrivateResultUri(reference.binding, resultId)) throw fail("binding_mismatch", "Native result resource URI does not match its owner binding");
      const bytes = new Uint8Array(reference.byteLength);
      let offset = 0;
      while (offset < bytes.length) {
        const remainingMs = Math.floor(deadlineAt - performance.now());
        if (remainingMs < 1) throw deadlineFailure();
        const limit = Math.min(32768, bytes.length - offset);
        const uri = `${reference.uri}?offset=${offset}&limit=${limit}`;
        const result = await client.readResource({ uri }, { signal, timeout: remainingMs, maxTotalTimeout: remainingMs, cacheMode: "bypass" });
        if (result.contents.length !== 1) throw fail("resource_gap", "Native private result chunk is unavailable");
        const content = result.contents[0];
        if (!content || content.uri !== uri || content.mimeType !== reference.mimeType || !("blob" in content)) throw fail("binding_mismatch", "Native private result chunk identity does not match");
        if (content.blob.length > 44000 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(content.blob)) throw fail("resource_gap", "Native private result chunk is invalid");
        const chunk = Buffer.from(content.blob, "base64");
        if (chunk.byteLength !== limit || chunk.toString("base64") !== content.blob) throw fail("resource_gap", "Native private result cursor or EOF is incomplete");
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return bytes;
    }, true);
    const output = yield* Effect.try({
      try: () => Schema.decodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(nativeToolOutputSchema(name))), { onExcessProperty: "error" })(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
      catch: () => nativeFailure(),
    });
    if (name === "native.get_state") {
      const state = yield* Effect.try({ try: () => Schema.decodeUnknownSync(nativeToolOutputSchema("native.get_state"), { onExcessProperty: "error" })(output), catch: () => nativeFailure() });
      yield* Effect.try({ try: () => assertBinding(reference.binding, state.binding), catch: () => fail("binding_mismatch", "Native state snapshot binding does not match its result resource") });
    }
    return output;
  });

  // The server, not launch arguments, owns the binding readback.
  const initialState = yield* call("native.get_state", {}, { timeoutMs: launch.connectTimeoutMs }).pipe(Effect.withSpan("NativeClient.discoverBinding"));
  if (initialState.binding.host !== launch.host) return yield* Effect.fail(fail("binding_mismatch", "Native readback host does not match the requested host"));
  if (!resources.resources.some(({ uri }) => uri === nativeEventsUri(initialState.binding)) ||
    !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${nativeEventsUri(initialState.binding)}{?after,limit}`) ||
    !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === "native://{host}/{session}/{incarnation}/{generation}/results/{resultId}{?offset,limit}")) {
    return yield* Effect.fail(fail("unsupported_operation", "Native lifecycle and private-result resources are not discoverable"));
  }
  const hasPresentation = resources.resources.some(({ uri }) => uri === nativePresentationUri(initialState.binding)) &&
    templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${nativePresentationUri(initialState.binding)}{?after,limit}`) &&
    templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${nativePresentationUri(initialState.binding)}/snapshots/{snapshot}{?offset,limit}`);

  const readPage = async (binding: NativeBindingType, after: NativeEventSequenceType, signal: AbortSignal): Promise<NativeEventPageType> => {
    const uri = queryEventsUri(binding, after);
    const response = await client.readResource({ uri }, { signal, timeout: launch.connectTimeoutMs, cacheMode: "bypass" });
    if (response.contents.length !== 1 || response.contents[0]?.uri !== uri || response.contents[0]?.mimeType !== "application/json") throw fail("resource_gap", "Native lifecycle page identity is invalid");
    const text = response.contents.find((content) => "text" in content)?.text;
    if (text === undefined) throw nativeFailure();
    const page = Schema.decodeUnknownSync(Schema.fromJsonString(NativeEventPage), { onExcessProperty: "error" })(text);
    assertBinding(binding, page.binding);
    if (page.gap !== null) throw fail("resource_gap", "Native lifecycle retention gap prevents proving run state");
    if (page.after !== after || page.highWater < after || page.nextAfter > page.highWater) throw fail("resource_gap", "Native lifecycle page cursor is invalid");
    return page;
  };

  const closeObserver = (drain: () => Promise<void>, cancellation: AbortController, unsubscribe: () => Effect.Effect<void, NativeControlError>, finish: () => void, deadlineAt: number) => Effect.gen(function*() {
    const drained = yield* sdkEffect(signal => waitFor(drain(), signal)).pipe(
      Effect.interruptible,
      Effect.timeoutOrElse({ duration: Math.max(0, deadlineAt - performance.now()), orElse: () => Effect.fail(deadlineFailure()) }),
      Effect.exit,
    );
    cancellation.abort();
    finish();
    const detached = yield* unsubscribe().pipe(
      Effect.interruptible,
      Effect.timeoutOrElse({ duration: Math.max(0, deadlineAt - performance.now()), orElse: () => Effect.fail(deadlineFailure()) }),
      Effect.exit,
    );
    if (drained._tag === "Failure" && detached._tag === "Failure") return yield* Effect.failCause(Cause.combine(drained.cause, detached.cause));
    if (drained._tag === "Failure") return yield* Effect.failCause(drained.cause);
    if (detached._tag === "Failure") return yield* Effect.failCause(detached.cause);
  }).pipe(Effect.uninterruptible);

  const watchLifecycle = (untrustedCursor: NativeLifecycleCursor, onEvent: (event: NativeLifecycleEventType) => void | Promise<void>, deadlineAt?: number) => Effect.gen(function*() {
    const cursor = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativeLifecycleCursor, { onExcessProperty: "error" })(untrustedCursor), catch: () => fail("invalid_input", "Invalid native lifecycle cursor") });
    const baseUri = nativeEventsUri(cursor.binding);
    const cancellation = new AbortController();
    let failure: NativeControlError | undefined;
    let after = cursor.after;
    let stopped = false;
    let closing = false;
    let closeHighWater: NativeEventSequenceType | undefined;
    let reading: Promise<void> | undefined;
    let hinted = false;
    let rejectWatch!: (error: NativeControlError) => void;
    const watchFailure = new Promise<never>((_, reject) => { rejectWatch = reject; });
    void watchFailure.catch(() => undefined);
    const drain = () => {
      if (failure) return Promise.reject(failure);
      if (stopped) return Promise.resolve();
      if (reading) { hinted = true; return reading; }
      reading = (async () => {
        do {
          hinted = false;
          let page: NativeEventPageType;
          do {
            page = await readPage(cursor.binding, after, cancellation.signal);
            if (closing && closeHighWater === undefined) closeHighWater = page.highWater;
            if (stopped) return;
            for (const event of page.entries) {
              if (closeHighWater !== undefined && event.seq > closeHighWater) break;
              assertBinding(cursor.binding, event.binding);
              if (event.seq <= after || event.seq > page.nextAfter) throw fail("resource_gap", "Native lifecycle events are duplicated or reordered");
              await waitFor(Promise.resolve(onEvent(Schema.decodeUnknownSync(NativeLifecycleEvent, { onExcessProperty: "error" })(event))), cancellation.signal);
              after = event.seq;
              if (stopped) return;
            }
            if (page.nextAfter < after || (page.more && (page.entries.length === 0 || page.nextAfter !== page.entries.at(-1)?.seq || page.nextAfter >= page.highWater)) || (!page.more && page.nextAfter !== page.highWater)) throw fail("resource_gap", "Native lifecycle page made no valid progress");
            after = closeHighWater === undefined ? page.nextAfter : Schema.decodeUnknownSync(NativeEventSequence)(Math.min(page.nextAfter, closeHighWater));
          } while (closeHighWater === undefined ? page.more : after < closeHighWater);
        } while (hinted && !closing && !stopped);
      })().catch((cause) => {
        failure = cause instanceof NativeControlError ? cause : SdkError.isInstance(cause) ? cause.code === SdkErrorCode.RequestTimeout ? deadlineFailure() : transportFailure() : nativeFailure();
        rejectWatch(failure);
        throw failure;
      }).finally(() => { reading = undefined; });
      return reading;
    };
    const hint = () => { if (!stopped) void drain().catch(() => undefined); };
    const hints = lifecycleHints.get(baseUri) ?? new Set<() => void>();
    hints.add(hint);
    lifecycleHints.set(baseUri, hints);
    const unregister = () => { hints.delete(hint); if (hints.size === 0) lifecycleHints.delete(baseUri); };
    let subscribed = false;
    let released = false;
    const release = Effect.suspend(() => {
      if (released) return Effect.void;
      released = true;
      closing = true;
      unregister();
      return closeObserver(async () => {
        await reading;
        // Capture a fresh retained high-water after any read already in flight.
        closeHighWater = undefined;
        await drain();
      }, cancellation, () => subscribed ? changeSubscription(baseUri, false) : Effect.void, () => { stopped = true; }, deadlineAt ?? performance.now() + launch.connectTimeoutMs);
    });
    if (deadlineAt === undefined) yield* Effect.addFinalizer(() => release.pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      if (supportsSubscriptions) yield* changeSubscription(baseUri, true, () => { subscribed = true; });
      // ponytail: HTTP without push reads retained events, never polls idle as settlement.
      else yield* Effect.forkScoped(Effect.gen(function*() {
        while (!closing && !stopped) {
          yield* Effect.sleep("100 millis");
          if (!closing && !stopped) yield* sdkEffect((signal) => waitFor(drain(), signal));
        }
      }));
      yield* sdkEffect((signal) => waitFor(drain(), signal));
    }).pipe(Effect.onExit(exit => exit._tag === "Failure" ? release : Effect.void));
    const connectionClosed = Effect.tryPromise({ try: (signal) => Promise.race([
      connectionClosedPromise.then(() => { throw transportFailure(); }),
      connectionErrorPromise,
      watchFailure,
      new Promise<never>((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true })),
    ]), catch: (cause) => cause instanceof NativeControlError ? cause : transportFailure() });
    return { drain: sdkEffect((signal) => waitFor(drain(), signal)), connectionClosed, close: release };
  });

  const watchPresentation = (untrustedCursor: NativePresentationCursor, onValue: (value: NativePresentationValue) => void | Promise<void>) => Effect.gen(function*() {
    const cursor = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativePresentationCursor, { onExcessProperty: "error" })(untrustedCursor), catch: () => fail("invalid_input", "Invalid native presentation cursor") });
    if (!hasPresentation) return yield* Effect.fail(fail("unsupported_operation", "Public plugin does not expose native presentation resources"));
    const baseUri = nativePresentationUri(cursor.binding);
    const cancellation = new AbortController();
    let after = cursor.after;
    let stopped = false;
    let closing = false;
    let closeHighWater: number | undefined;
    let hinted = false;
    let reading: Promise<void> | undefined;
    let failure: NativeControlError | undefined;
    let rejectWatch!: (error: NativeControlError) => void;
    const watchFailure = new Promise<never>((_, reject) => { rejectWatch = reject; });
    void watchFailure.catch(() => undefined);
    const drain = (): Promise<void> => {
      if (failure) return Promise.reject(failure);
      if (stopped) return Promise.resolve();
      if (reading) { hinted = true; return reading; }
      reading = (async () => {
        do {
          hinted = false;
          let more = false;
          do {
            const uri = `${baseUri}?after=${after}&limit=256`;
            const response = await client.readResource({ uri }, { signal: cancellation.signal, timeout: launch.connectTimeoutMs, cacheMode: "bypass" });
            if (stopped) return;
            if (response.contents.length !== 1) throw fail("resource_gap", "Native presentation page is unavailable");
            const content = response.contents[0];
            if (!content || content.uri !== uri || content.mimeType !== "application/json" || !("text" in content)) throw fail("binding_mismatch", "Native presentation page identity does not match");
            const wire = Schema.encodeUnknownSync(Schema.toCodecJson(McpSchema.ReadResourceResult), { onExcessProperty: "error" })(response);
            if (new TextEncoder().encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Json))({ result: wire })).byteLength > NativeMcpResultBytes) throw fail("resource_gap", "Native presentation page exceeds its wire bound");
            const page = Schema.decodeUnknownSync(Schema.fromJsonString(NativePresentationPage), { onExcessProperty: "error" })(content.text);
            assertBinding(cursor.binding, page.binding);
            if (page.gap !== null) throw fail("resource_gap", "Native presentation retention gap prevents complete delivery");
            if (page.after !== after || page.highWater < after || page.nextAfter > page.highWater || page.entries.length > 256) throw fail("resource_gap", "Native presentation page cursor is invalid");
            if (closing && closeHighWater === undefined) closeHighWater = page.highWater;
            for (const entry of page.entries) {
              if (closeHighWater !== undefined && entry.seq > closeHighWater) break;
              assertBinding(cursor.binding, entry.binding);
              if (entry.seq !== after + 1) throw fail("resource_gap", "Native presentation sequence is incomplete");
              const snapshotId = Schema.decodeUnknownSync(NativePrivateResultId)(entry.snapshot.uri.split("/").at(-1));
              if (entry.snapshot.uri !== `${baseUri}/snapshots/${snapshotId}`) throw fail("binding_mismatch", "Native presentation snapshot belongs to another owner");
              const bytes = new Uint8Array(entry.snapshot.byteLength);
              let offset = 0;
              while (offset < bytes.byteLength) {
                const limit = Math.min(32768, bytes.byteLength - offset);
                const chunkUri = `${entry.snapshot.uri}?offset=${offset}&limit=${limit}`;
                const result = await client.readResource({ uri: chunkUri }, { signal: cancellation.signal, timeout: launch.connectTimeoutMs, cacheMode: "bypass" });
                if (stopped) return;
                if (result.contents.length !== 1) throw fail("resource_gap", "Native presentation snapshot was evicted");
                const chunkContent = result.contents[0];
                if (!chunkContent || chunkContent.uri !== chunkUri || chunkContent.mimeType !== entry.snapshot.mimeType || !("blob" in chunkContent)) throw fail("binding_mismatch", "Native presentation chunk identity does not match");
                if (chunkContent.blob.length > 44000 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(chunkContent.blob)) throw fail("resource_gap", "Native presentation chunk encoding is invalid");
                const chunk = Buffer.from(chunkContent.blob, "base64");
                if (chunk.byteLength !== limit || chunk.toString("base64") !== chunkContent.blob) throw fail("resource_gap", "Native presentation snapshot cursor or EOF is incomplete");
                bytes.set(chunk, offset);
                offset += chunk.byteLength;
              }
              const value = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" })(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
              if (stopped) return;
              await waitFor(Promise.resolve(onValue({ binding: entry.binding, seq: entry.seq, key: entry.key, value })), cancellation.signal);
              after = entry.seq;
            }
            if (page.nextAfter !== (page.entries.at(-1)?.seq ?? page.after) || page.more !== (page.nextAfter < page.highWater) || (page.more && page.entries.length === 0)) throw fail("resource_gap", "Native presentation page made no valid progress");
            more = closeHighWater === undefined ? page.more : after < closeHighWater;
          } while (more && !stopped);
        } while (hinted && !closing && !stopped);
      })().catch((cause) => {
        failure = cause instanceof NativeControlError ? cause : SdkError.isInstance(cause) ? cause.code === SdkErrorCode.RequestTimeout ? deadlineFailure() : transportFailure() : nativeFailure();
        rejectWatch(failure);
        throw failure;
      }).finally(() => { reading = undefined; });
      return reading;
    };
    const hint = () => { if (!stopped) void drain().catch(() => undefined); };
    const hints = lifecycleHints.get(baseUri) ?? new Set<() => void>();
    hints.add(hint);
    lifecycleHints.set(baseUri, hints);
    let subscribed = false;
    let released = false;
    const close = Effect.suspend(() => {
      if (released) return Effect.void;
      released = true;
      closing = true;
      hints.delete(hint);
      if (hints.size === 0) lifecycleHints.delete(baseUri);
      return closeObserver(async () => {
        await reading;
        closeHighWater = undefined;
        await drain();
      }, cancellation, () => subscribed ? changeSubscription(baseUri, false) : Effect.void, () => { stopped = true; }, performance.now() + launch.connectTimeoutMs);
    });
    yield* Effect.addFinalizer(() => close.pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      if (supportsSubscriptions) yield* changeSubscription(baseUri, true, () => { subscribed = true; });
      else yield* Effect.forkScoped(Effect.gen(function*() {
        while (!closing && !stopped) {
          yield* Effect.sleep("100 millis");
          if (!closing && !stopped) yield* sdkEffect((signal) => waitFor(drain(), signal));
        }
      }));
      yield* sdkEffect((signal) => waitFor(drain(), signal));
    }).pipe(Effect.onExit(exit => exit._tag === "Failure" ? close : Effect.void));
    return {
      drain: sdkEffect((signal) => waitFor(drain(), signal)),
      close,
      connectionClosed: sdkEffect((signal) => waitFor(Promise.race([
        connectionClosedPromise.then(() => { throw transportFailure(); }), connectionErrorPromise, watchFailure,
      ]), signal)),
    } satisfies NativePresentationWatch;
  });

  const prompt = (input: NativePromptInputType, options: NativePromptCallOptions) => Effect.scoped(Effect.gen(function*() {
    const budget = yield* Effect.try({ try: () => Schema.decodeUnknownSync(NativeCallBudget, { onExcessProperty: "error" })({ timeoutMs: options.timeoutMs }), catch: () => fail("invalid_input", "Invalid native request budget") });
    const deadlineAt = performance.now() + budget.timeoutMs;
    let watch: NativeLifecycleWatch | undefined;
    return yield* Effect.gen(function*() {
      let admission: NativePromptAdmissionType | undefined;
      let settlement: Extract<NativeLifecycleEventType, { kind: "run_settled" }> | undefined;
      watch = yield* watchLifecycle({ binding: input.binding, after: options.after }, async (event) => {
        if (performance.now() >= deadlineAt) throw deadlineFailure();
        await options.onLifecycle?.(event);
        if (event.kind === "session_retired") throw fail("binding_mismatch", "Native owner generation retired while prompt state was unresolved");
        if (event.kind === "submission_admitted" && event.submissionId === input.submissionId) {
          if (admission !== undefined) throw fail("native_failure", "Native prompt submission was admitted more than once");
          admission = Schema.decodeUnknownSync(NativePromptAdmission, { onExcessProperty: "error" })({ binding: event.binding, submissionId: event.submissionId, disposition: event.disposition, runId: event.runId, eventsUri: nativeEventsUri(event.binding), seq: event.seq });
          if ((admission.disposition === "handled") !== (admission.runId === null)) throw fail("native_failure", "Native prompt admission has a contradictory run identity");
          if (performance.now() >= deadlineAt) throw deadlineFailure();
          await options.onAdmission(admission);
        }
        if (event.kind === "run_settled" && admission?.runId !== null && event.runId === admission?.runId) {
          if (settlement !== undefined) throw fail("native_failure", "Native prompt run settled more than once");
          settlement = event;
        }
      }, deadlineAt);
      const remainingMs = Math.floor(deadlineAt - performance.now());
      if (remainingMs < 1) return yield* Effect.fail(deadlineFailure());
      const result = yield* Effect.raceFirst(call("native.prompt", input, { timeoutMs: remainingMs }), watch.connectionClosed);
      yield* Effect.raceFirst(watch.drain, watch.connectionClosed);
      yield* watch.close;
      const retainedAdmission = admission;
      const retainedSettlement = settlement;
      yield* Effect.try({ try: () => assertBinding(input.binding, result.admission.binding), catch: () => fail("binding_mismatch", "Native prompt admission belongs to another owner generation") });
      if (result.admission.submissionId !== input.submissionId) return yield* Effect.fail(fail("binding_mismatch", "Native prompt result belongs to another submission"));
      if (retainedAdmission === undefined) return yield* Effect.fail(fail("resource_gap", "Native prompt has no retained admission proof"));
      if (!isDeepStrictEqual(retainedAdmission, result.admission)) return yield* Effect.fail(fail("native_failure", "Native prompt admission event disagrees with the final result"));
      if (result.disposition === "handled") {
        if (retainedAdmission.disposition !== "handled" || retainedAdmission.runId !== null) return yield* Effect.fail(fail("native_failure", "Native handled result contradicts its admission"));
      } else {
        yield* Effect.try({ try: () => assertBinding(retainedAdmission.binding, result.final.binding), catch: () => fail("binding_mismatch", "Native prompt final result belongs to another owner generation") });
        if (retainedAdmission.disposition === "handled" || retainedAdmission.runId === null || result.final.runId !== retainedAdmission.runId) return yield* Effect.fail(fail("native_failure", "Native final result contradicts its admitted run"));
        if (retainedSettlement === undefined) return yield* Effect.fail(fail("resource_gap", "Native prompt has no retained whole-run settlement proof"));
        yield* Effect.try({ try: () => assertBinding(result.final.binding, retainedSettlement.binding), catch: () => fail("binding_mismatch", "Native prompt settlement belongs to another owner generation") });
        if (retainedSettlement.seq <= retainedAdmission.seq || retainedSettlement.outcome !== result.final.outcome) return yield* Effect.fail(fail("native_failure", "Native final result contradicts its lifecycle settlement"));
      }
      return result;
    }).pipe(
      Effect.timeoutOrElse({ duration: budget.timeoutMs, orElse: () => Effect.fail(deadlineFailure()) }),
      Effect.onExit(() => watch?.close ?? Effect.void),
    );
  }));

  return {
    host: launch.host,
    binding: initialState.binding,
    pid,
    capabilities,
    connectionClosed: Effect.tryPromise({ try: (signal) => waitFor(connectionClosedPromise, signal).then(() => { throw transportFailure(); }), catch: (cause) => cause instanceof NativeControlError ? cause : transportFailure() }),
    connectionError: Effect.tryPromise({ try: (signal) => waitFor(connectionErrorPromise, signal), catch: (cause) => cause instanceof NativeControlError ? cause : transportFailure() }),
    observedProcessClosed: Effect.tryPromise({ try: (signal) => waitFor(processClosedPromise, signal), catch: (cause) => cause instanceof NativeControlError ? cause : transportFailure() }),
    call,
    prompt,
    watchLifecycle,
    watchPresentation,
  };
});
