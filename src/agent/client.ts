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
import {
  AgentPresentationPage,
  agentPresentationUri,
  type AgentPresentationCursor,
  type AgentPresentationValue,
  AgentError,
  AgentEndpoint,
  AgentEventPage,
  AgentEventSequence,
  AgentMcpResultBytes,
  AgentMcpWireBytes,
  AgentMcpProtocolVersion,
  AgentPrivateResultId,
  agentPrivateResultUri,
  AgentTools,
  agentToolDescriptors,
  agentToolDiscovery,
  agentEventsUri,
  type AgentBinding as AgentBindingType,
  type AgentEventPage as AgentEventPageType,
  type AgentEventSequence as AgentEventSequenceType,
  type AgentHost as AgentHostType,
  type AgentLifecycleEvent as AgentLifecycleEventType,
  type AgentPromptAdmission as AgentPromptAdmissionType,
  type AgentPromptInput as AgentPromptInputType,
  type AgentPromptResult as AgentPromptResultType,
  type AgentToolDescriptor,
  type AgentToolName,
} from "./protocol.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export type AgentClientLaunch = Readonly<{
  host: AgentHostType;
  command: string;
  args?: readonly string[];
  env: Readonly<Record<string, string>>;
  cwd: string;
  maxBufferSize?: number;
  connectTimeoutMs: number;
  plugin?: Readonly<{ endpointFile: string; token: string }>;
}>;
export type AgentClientConnection = Readonly<{ host: AgentHostType; connectTimeoutMs: number }>;
export type AgentCallBudget = Readonly<{ timeoutMs: number }>;
export type AgentLifecycleCursor = Readonly<{ binding: AgentBindingType; after: AgentEventSequenceType }>;

export type AgentElicitation = Readonly<{
  capabilities: NonNullable<ClientCapabilities["elicitation"]>;
  handle: (request: ElicitRequest, context: ClientContext) => Promise<ElicitResult>;
}>;
export type AgentPromptCallOptions = AgentCallBudget & Readonly<{
  after: AgentEventSequenceType;
  onAdmission: (admission: AgentPromptAdmissionType) => void | Promise<void>;
  onLifecycle?: (event: AgentLifecycleEventType) => void | Promise<void>;
}>;
export type AgentLifecycleWatch = Readonly<{ drain: Effect.Effect<void, AgentError>; connectionClosed: Effect.Effect<never, AgentError>; close: Effect.Effect<void, AgentError> }>;
export type AgentPresentationWatch = Readonly<{
  drain: Effect.Effect<void, AgentError>;
  close: Effect.Effect<void, AgentError>;
  connectionClosed: Effect.Effect<never, AgentError>;
}>;

const fail = (code: ConstructorParameters<typeof AgentError>[0]["code"], message: string) =>
  new AgentError({ code, message: message.slice(0, 1024) });
const transportFailure = () => fail("transport_closed", "Agent MCP connection closed before the operation was proven complete");
const agentFailure = () => fail("agent_failure", "Agent MCP operation failed");
const deadlineFailure = () => fail("deadline_exceeded", "Agent MCP operation exceeded its caller-provided deadline");
const validTimeout = (milliseconds: number) => Number.isInteger(milliseconds) && milliseconds >= 1 && milliseconds <= 2_147_483_647;
const failureEnvelope = Schema.Struct({ error: AgentError });
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
const queryEventsUri = (binding: AgentBindingType, after: AgentEventSequenceType, limit = 256) =>
  `${agentEventsUri(binding)}?after=${after}&limit=${limit}`;

// Bound wire resources before the SDK parser allocates; never cap a whole SSE subscription.
const boundedAgentFetch = (endpoint: URL): typeof fetch => async (input, init) => {
  const request = new Request(input, init);
  if (new URL(request.url).origin !== endpoint.origin) throw transportFailure();
  const response = await fetch(request);
  if (response.body === null) return response;
  const sse = response.headers.get("content-type")?.split(";")[0]?.trim() === "text/event-stream";
  let bytes = 0;
  let lineBytes = 0;
  let previousCR = false;
  const body = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      for (const byte of chunk) {
        if (++bytes > AgentMcpWireBytes) throw transportFailure();
        if (sse && (byte === 13 || (byte === 10 && !previousCR))) {
          if (lineBytes === 0) bytes = 0;
          lineBytes = 0;
        } else if (byte !== 10 && byte !== 13) lineBytes++;
        previousCR = byte === 13;
      }
      controller.enqueue(chunk);
    },
  }));
  return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
};

const transportEffect = <A>(run: (signal: AbortSignal) => Promise<A>, deadline = false): Effect.Effect<A, AgentError> =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => {
      if (cause instanceof AgentError) return cause;
      if (ProtocolError.isInstance(cause) && cause.data !== undefined) {
        const decoded = Schema.decodeUnknownOption(Schema.toCodecJson(failureEnvelope), { onExcessProperty: "error" })(cause.data);
        if (decoded._tag === "Some") return decoded.value.error;
      }
      if (SdkError.isInstance(cause) && cause.code === SdkErrorCode.RequestTimeout) return deadlineFailure();
      if (SdkError.isInstance(cause) && (cause.code === SdkErrorCode.ConnectionClosed || cause.code === SdkErrorCode.NotConnected || cause.code === SdkErrorCode.SendFailed)) return transportFailure();
      return deadline && cause instanceof Error && cause.name === "AbortError" ? deadlineFailure() : agentFailure();
    },
  });

const assertBinding = (expected: AgentBindingType, actual: AgentBindingType) => {
  if (expected.host !== actual.host || expected.sessionId !== actual.sessionId || expected.incarnation !== actual.incarnation || expected.generation !== actual.generation) throw fail("binding_mismatch", "Agent response binding does not match the requested owner generation");
};

export type AgentClient<ProcessID extends number | null = number> = Readonly<{
  host: AgentHostType;
  binding: AgentBindingType;
  pid: ProcessID;
  capabilities: readonly AgentToolName[];
  connectionClosed: Effect.Effect<void, AgentError>;
  connectionError: Effect.Effect<never, AgentError>;
  observedProcessClosed: Effect.Effect<void, AgentError>;
  call<I extends Schema.ConstraintCodec<{ readonly binding?: AgentBindingType | undefined }, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(descriptor: AgentToolDescriptor<I, O>, input: I["Type"], budget: AgentCallBudget): Effect.Effect<O["Type"], AgentError>;
  /** Success requires retained admission and whole-run lifecycle proof; the deadline includes callbacks and closing drain. */
  prompt(input: AgentPromptInputType, options: AgentPromptCallOptions): Effect.Effect<AgentPromptResultType, AgentError>;
  watchLifecycle(cursor: AgentLifecycleCursor, onEvent: (event: AgentLifecycleEventType) => void | Promise<void>): Effect.Effect<AgentLifecycleWatch, AgentError, Scope.Scope>;
  watchPresentation(cursor: AgentPresentationCursor, onValue: (value: AgentPresentationValue) => void | Promise<void>): Effect.Effect<AgentPresentationWatch, AgentError, Scope.Scope>;
}>;

export const connectAgentClient = (
  launch: AgentClientLaunch,
  elicitation?: AgentElicitation,
): Effect.Effect<AgentClient, AgentError, Scope.Scope> => Effect.gen(function*() {
  if (!validTimeout(launch.connectTimeoutMs)) return yield* Effect.fail(fail("invalid_input", "Invalid agent MCP launch timeout"));
  if (launch.maxBufferSize !== undefined && (!Number.isInteger(launch.maxBufferSize) || launch.maxBufferSize < 1024 || launch.maxBufferSize > 268435456)) return yield* Effect.fail(fail("invalid_input", "Invalid agent MCP launch buffer limit"));
  let resolveProcessClosed!: () => void;
  const processClosedPromise = new Promise<void>((resolve) => { resolveProcessClosed = resolve; });
  const envArgs = Object.entries(launch.env).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `${key}=${value}`);
  let startAttempted = false;
  let control: Transport;
  let readPid: () => number | null;
  let closeProcess: () => Promise<void>;
  let endpointBinding: AgentBindingType | undefined;
  if (launch.plugin === undefined) {
    const transport = new StdioClientTransport({
      command: "/usr/bin/env",
      args: ["-i", ...envArgs, launch.command, ...(launch.args ?? [])],
      env: {}, cwd: launch.cwd, stderr: "ignore",
      maxBufferSize: Math.min(launch.maxBufferSize ?? AgentMcpWireBytes, AgentMcpWireBytes),
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
        // Agent RPC is not MCP: drain bytes without a JSON-RPC decoder or buffer.
        acquired.stdout.resume();
        acquired.stderr.resume();
        await new Promise<void>((resolve, reject) => { acquired.once("spawn", () => resolve()); acquired.once("error", reject); });
        const deadline = performance.now() + launch.connectTimeoutMs;
        for (;;) {
          if (closed || processClosed) throw transportFailure();
          if (performance.now() >= deadline) throw deadlineFailure();
          let endpoint: typeof AgentEndpoint.Type | undefined;
          try {
            const file = await open(plugin.endpointFile, constants.O_RDONLY | constants.O_NOFOLLOW);
            try {
              const stat = await file.stat();
              if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.size > AgentMcpWireBytes) throw fail("invalid_input", "Private bounded plugin endpoint file required");
              endpoint = Schema.decodeSync(Schema.fromJsonString(AgentEndpoint), { onExcessProperty: "error" })(await file.readFile("utf8"));
            } finally { await file.close(); }
          } catch (error) {
            if (Schema.decodeUnknownOption(missing)(error)._tag === "None") throw fail("invalid_input", "Plugin endpoint is unavailable or invalid");
          }
          if (endpoint !== undefined) {
            if (endpoint.binding.host !== launch.host || closed || processClosed) throw fail("binding_mismatch", "Plugin endpoint owner changed during startup");
            endpointBinding = endpoint.binding;
            http = new StreamableHTTPClientTransport(new URL(endpoint.url), {
              requestInit: { headers: { Authorization: `Bearer ${plugin.token}` }, redirect: "error" },
              fetch: boundedAgentFetch(new URL(endpoint.url)),
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
  const agent = yield* connectAgentTransport(
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
    yield* Effect.try({ try: () => assertBinding(announced, agent.binding), catch: () => fail("binding_mismatch", "Plugin endpoint and MCP readback owners differ") });
  }
  return agent;
});

export const connectAgentTransport = <ProcessID extends number | null>(
  launch: AgentClientConnection,
  transport: Transport,
  ownership: Readonly<{ pid: ProcessID; observedProcessClosed: Promise<void>; close: () => Promise<void> }>,
  elicitation?: AgentElicitation,
): Effect.Effect<AgentClient<ProcessID>, AgentError, Scope.Scope> => Effect.gen(function*() {
  const processClosedPromise = ownership.observedProcessClosed;
  // Observe rejection immediately, even if discovery fails before returning a client.
  void processClosedPromise.catch(() => undefined);
  let connectedClient: Client | undefined;
  let teardownPromise: Promise<Exit.Exit<void, AgentError>> | undefined;
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
  if (!validTimeout(launch.connectTimeoutMs)) return yield* Effect.fail(fail("invalid_input", "Invalid agent MCP connection timeout"));
  let resolveConnectionClosed!: () => void;
  const connectionClosedPromise = new Promise<void>((resolve) => { resolveConnectionClosed = resolve; });
  let connectionClosed = false;
  let rejectConnectionError!: (error: AgentError) => void;
  const connectionErrorPromise = new Promise<never>((_, reject) => { rejectConnectionError = reject; });
  void connectionErrorPromise.catch(() => undefined);
  const sdkEffect = <A>(run: (signal: AbortSignal) => Promise<A>, deadline = false) =>
    transportEffect(signal => connectionClosed
      ? Promise.reject(transportFailure())
      : Promise.race([run(signal), connectionClosedPromise.then(() => { throw transportFailure(); })]), deadline);
  const client = yield* Effect.try({ try: () => new Client(
    { name: "coordinator-agent", version: "0.1.0" },
    {
      capabilities: elicitation ? { elicitation: elicitation.capabilities } : {},
      supportedProtocolVersions: [AgentMcpProtocolVersion],
      versionNegotiation: { mode: "legacy" },
      enforceStrictCapabilities: true,
      listMaxPages: 16,
    },
  ), catch: () => agentFailure() });
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

  yield* sdkEffect((signal) => client.connect(transport, { signal, timeout: launch.connectTimeoutMs }), true).pipe(Effect.withSpan("AgentClient.connect"));
  if (client.getNegotiatedProtocolVersion() !== AgentMcpProtocolVersion) return yield* Effect.fail(fail("unsupported_operation", "Agent server negotiated an unsupported protocol version"));
  const pid = yield* Effect.try({ try: () => ownership.pid, catch: () => transportFailure() });

  const serverCapabilities = client.getServerCapabilities();
  if (!serverCapabilities?.tools || !serverCapabilities.resources) return yield* Effect.fail(fail("unsupported_operation", "Agent server lacks required tools or resources"));
  const supportsSubscriptions = serverCapabilities.resources.subscribe === true;
  const allowedNames = agentToolDescriptors.filter((descriptor) => descriptor.hosts.some((host) => host === launch.host)).map((descriptor) => descriptor.name);
  const discovery = yield* sdkEffect((signal) => client.listTools(undefined, { signal, timeout: launch.connectTimeoutMs })).pipe(Effect.withSpan("AgentClient.discoverTools"));
  const expectedNames = discovery.tools.map(({ name }) => name);
  if (new Set(expectedNames).size !== expectedNames.length || !expectedNames.includes(AgentTools.getState.name) ||
    expectedNames.some((name) => !allowedNames.some((allowed) => allowed === name))) {
    return yield* Effect.fail(fail("unsupported_operation", "Agent server advertises duplicate, foreign or missing bootstrap capabilities"));
  }
  const capabilities = allowedNames.filter((name) => expectedNames.includes(name));
  for (const tool of discovery.tools) {
    const contract = agentToolDiscovery.get(tool.name);
    if (contract === undefined) return yield* Effect.fail(fail("unsupported_operation", "Agent server advertises an unknown capability"));
    if (!isDeepStrictEqual(tool.inputSchema, contract.inputSchema) || !isDeepStrictEqual(tool.outputSchema, contract.outputSchema)) {
      return yield* Effect.fail(fail("unsupported_operation", `Agent server schema discovery mismatch for ${contract.name}`));
    }
  }
  const [resources, templates] = yield* Effect.all([
    sdkEffect((signal) => client.listResources(undefined, { signal, timeout: launch.connectTimeoutMs })),
    sdkEffect((signal) => client.listResourceTemplates(undefined, { signal, timeout: launch.connectTimeoutMs })),
  ]).pipe(Effect.withSpan("AgentClient.discoverResources"));
  if (resources.resources.length < 1 || !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate.endsWith("/events{?after,limit}"))) {
    return yield* Effect.fail(fail("unsupported_operation", "Agent lifecycle resources are not discoverable"));
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

  const call = <I extends Schema.ConstraintCodec<{ readonly binding?: AgentBindingType | undefined }, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(descriptor: AgentToolDescriptor<I, O>, input: I["Type"], budget: AgentCallBudget): Effect.Effect<O["Type"], AgentError> => Effect.gen(function*() {
    if (connectionClosed) return yield* Effect.fail(transportFailure());
    if (!validTimeout(budget.timeoutMs)) return yield* Effect.fail(fail("invalid_input", "Invalid agent request budget"));
    const deadlineAt = performance.now() + budget.timeoutMs;
    if (!expectedNames.includes(descriptor.name)) return yield* Effect.fail(fail("unsupported_operation", "Agent tool is unavailable on the connected host"));
    const arguments_ = yield* Effect.try({
      try: () => Schema.encodeSync<I>(descriptor.input, { onExcessProperty: "error" })(input),
      catch: () => fail("invalid_input", "Invalid agent tool input"),
    });
    const response = yield* sdkEffect((signal) => client.callTool({ name: descriptor.name, arguments: arguments_ }, { signal, timeout: budget.timeoutMs, maxTotalTimeout: budget.timeoutMs }), true);
    if (response.isError) {
      if (response.structuredContent === undefined) return yield* Effect.fail(agentFailure());
      const failure = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(failureEnvelope), { onExcessProperty: "error" })(response.structuredContent).pipe(
        Effect.mapError(() => agentFailure()),
      );
      return yield* Effect.fail(failure.error);
    }
    if (response.structuredContent === undefined) return yield* Effect.fail(agentFailure());
    const envelope = yield* Schema.decodeUnknownEffect<Schema.toCodecJson<typeof descriptor.structuredOutput>>(Schema.toCodecJson(descriptor.structuredOutput), { onExcessProperty: "error" })(response.structuredContent).pipe(
      Effect.mapError(() => agentFailure()),
    );
    if ("result" in envelope) return envelope.result;
    const reference = envelope.resultResource;
    const bytes = yield* sdkEffect(async (signal) => {
      if (reference.binding.host !== launch.host) throw fail("binding_mismatch", "Agent result resource belongs to another host");
      if ("binding" in input && input.binding !== undefined) assertBinding(input.binding, reference.binding);
      const resultId = Schema.decodeUnknownSync(AgentPrivateResultId, { onExcessProperty: "error" })(reference.uri.split("/").at(-1));
      if (reference.uri !== agentPrivateResultUri(reference.binding, resultId)) throw fail("binding_mismatch", "Agent result resource URI does not match its owner binding");
      const bytes = new Uint8Array(reference.byteLength);
      let offset = 0;
      while (offset < bytes.length) {
        const remainingMs = Math.floor(deadlineAt - performance.now());
        if (remainingMs < 1) throw deadlineFailure();
        const limit = Math.min(32768, bytes.length - offset);
        const uri = `${reference.uri}?offset=${offset}&limit=${limit}`;
        const result = await client.readResource({ uri }, { signal, timeout: remainingMs, maxTotalTimeout: remainingMs, cacheMode: "bypass" });
        if (result.contents.length !== 1) throw fail("resource_gap", "Agent private result chunk is unavailable");
        const content = result.contents[0];
        if (!content || content.uri !== uri || content.mimeType !== reference.mimeType || !("blob" in content)) throw fail("binding_mismatch", "Agent private result chunk identity does not match");
        if (content.blob.length > 44000 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(content.blob)) throw fail("resource_gap", "Agent private result chunk is invalid");
        const chunk = Buffer.from(content.blob, "base64");
        if (chunk.byteLength !== limit || chunk.toString("base64") !== content.blob) throw fail("resource_gap", "Agent private result cursor or EOF is incomplete");
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return bytes;
    }, true);
    const decoded = yield* Effect.try({
      try: () => descriptor.decodeOutput(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
      catch: () => agentFailure(),
    });
    const outputBinding = decoded.binding;
    if (outputBinding !== undefined) {
      yield* Effect.try({ try: () => assertBinding(reference.binding, outputBinding), catch: () => fail("binding_mismatch", "Agent state snapshot binding does not match its result resource") });
    }
    return decoded.output;
  });

  // The server, not launch arguments, owns the binding readback.
  const initialState = yield* call(AgentTools.getState, {}, { timeoutMs: launch.connectTimeoutMs }).pipe(Effect.withSpan("AgentClient.discoverBinding"));
  if (initialState.binding.host !== launch.host) return yield* Effect.fail(fail("binding_mismatch", "Agent readback host does not match the requested host"));
  if (!resources.resources.some(({ uri }) => uri === agentEventsUri(initialState.binding)) ||
    !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${agentEventsUri(initialState.binding)}{?after,limit}`) ||
    !templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === "agent://{host}/{session}/{incarnation}/{generation}/results/{resultId}{?offset,limit}")) {
    return yield* Effect.fail(fail("unsupported_operation", "Agent lifecycle and private-result resources are not discoverable"));
  }
  const hasPresentation = resources.resources.some(({ uri }) => uri === agentPresentationUri(initialState.binding)) &&
    templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${agentPresentationUri(initialState.binding)}{?after,limit}`) &&
    templates.resourceTemplates.some(({ uriTemplate }) => uriTemplate === `${agentPresentationUri(initialState.binding)}/snapshots/{snapshot}{?offset,limit}`);

  const readPage = async (binding: AgentBindingType, after: AgentEventSequenceType, signal: AbortSignal): Promise<AgentEventPageType> => {
    const uri = queryEventsUri(binding, after);
    const response = await client.readResource({ uri }, { signal, timeout: launch.connectTimeoutMs, cacheMode: "bypass" });
    if (response.contents.length !== 1 || response.contents[0]?.uri !== uri || response.contents[0]?.mimeType !== "application/json") throw fail("resource_gap", "Agent lifecycle page identity is invalid");
    const text = response.contents.find((content) => "text" in content)?.text;
    if (text === undefined) throw agentFailure();
    const page = Schema.decodeSync(Schema.fromJsonString(AgentEventPage), { onExcessProperty: "error" })(text);
    assertBinding(binding, page.binding);
    if (page.gap !== null) throw fail("resource_gap", "Agent lifecycle retention gap prevents proving run state");
    if (page.after !== after || page.highWater < after || page.nextAfter > page.highWater) throw fail("resource_gap", "Agent lifecycle page cursor is invalid");
    return page;
  };

  const closeObserver = (drain: () => Promise<void>, cancellation: AbortController, unsubscribe: () => Effect.Effect<void, AgentError>, finish: () => void, deadlineAt: number) => Effect.gen(function*() {
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

  const watchLifecycle = (cursor: AgentLifecycleCursor, onEvent: (event: AgentLifecycleEventType) => void | Promise<void>, deadlineAt?: number) => Effect.gen(function*() {
    const baseUri = agentEventsUri(cursor.binding);
    const cancellation = new AbortController();
    let failure: AgentError | undefined;
    let after = cursor.after;
    let stopped = false;
    let closing = false;
    let closeHighWater: AgentEventSequenceType | undefined;
    let reading: Promise<void> | undefined;
    let hinted = false;
    let rejectWatch!: (error: AgentError) => void;
    const watchFailure = new Promise<never>((_, reject) => { rejectWatch = reject; });
    void watchFailure.catch(() => undefined);
    const drain = () => {
      if (failure) return Promise.reject(failure);
      if (stopped) return Promise.resolve();
      if (reading) { hinted = true; return reading; }
      reading = (async () => {
        do {
          hinted = false;
          let page: AgentEventPageType;
          do {
            page = await readPage(cursor.binding, after, cancellation.signal);
            if (closing && closeHighWater === undefined) closeHighWater = page.highWater;
            if (stopped) return;
            for (const event of page.entries) {
              if (closeHighWater !== undefined && event.seq > closeHighWater) break;
              assertBinding(cursor.binding, event.binding);
              if (event.seq <= after || event.seq > page.nextAfter) throw fail("resource_gap", "Agent lifecycle events are duplicated or reordered");
              await waitFor(Promise.resolve(onEvent(event)), cancellation.signal);
              after = event.seq;
              if (stopped) return;
            }
            if (page.nextAfter < after || (page.more && (page.entries.length === 0 || page.nextAfter !== page.entries.at(-1)?.seq || page.nextAfter >= page.highWater)) || (!page.more && page.nextAfter !== page.highWater)) throw fail("resource_gap", "Agent lifecycle page made no valid progress");
            after = closeHighWater === undefined ? page.nextAfter : AgentEventSequence.make(Math.min(page.nextAfter, closeHighWater));
          } while (closeHighWater === undefined ? page.more : after < closeHighWater);
        } while (hinted && !closing && !stopped);
      })().catch((cause) => {
        failure = cause instanceof AgentError ? cause : SdkError.isInstance(cause) ? cause.code === SdkErrorCode.RequestTimeout ? deadlineFailure() : transportFailure() : agentFailure();
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
    ]), catch: (cause) => cause instanceof AgentError ? cause : transportFailure() });
    return { drain: sdkEffect((signal) => waitFor(drain(), signal)), connectionClosed, close: release };
  });

  const watchPresentation = (cursor: AgentPresentationCursor, onValue: (value: AgentPresentationValue) => void | Promise<void>) => Effect.gen(function*() {
    if (!hasPresentation) return yield* Effect.fail(fail("unsupported_operation", "Public plugin does not expose agent presentation resources"));
    const baseUri = agentPresentationUri(cursor.binding);
    const cancellation = new AbortController();
    let after = cursor.after;
    let stopped = false;
    let closing = false;
    let closeHighWater: number | undefined;
    let hinted = false;
    let reading: Promise<void> | undefined;
    let failure: AgentError | undefined;
    let rejectWatch!: (error: AgentError) => void;
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
            if (response.contents.length !== 1) throw fail("resource_gap", "Agent presentation page is unavailable");
            const content = response.contents[0];
            if (!content || content.uri !== uri || content.mimeType !== "application/json" || !("text" in content)) throw fail("binding_mismatch", "Agent presentation page identity does not match");
            if (new TextEncoder().encode(encodeJson({ result: response })).byteLength > AgentMcpResultBytes) throw fail("resource_gap", "Agent presentation page exceeds its wire bound");
            const page = Schema.decodeSync(Schema.fromJsonString(AgentPresentationPage), { onExcessProperty: "error" })(content.text);
            assertBinding(cursor.binding, page.binding);
            if (page.gap !== null) throw fail("resource_gap", "Agent presentation retention gap prevents complete delivery");
            if (page.after !== after || page.highWater < after || page.nextAfter > page.highWater || page.entries.length > 256) throw fail("resource_gap", "Agent presentation page cursor is invalid");
            if (closing && closeHighWater === undefined) closeHighWater = page.highWater;
            for (const entry of page.entries) {
              if (closeHighWater !== undefined && entry.seq > closeHighWater) break;
              assertBinding(cursor.binding, entry.binding);
              if (entry.seq !== after + 1) throw fail("resource_gap", "Agent presentation sequence is incomplete");
              const snapshotId = Schema.decodeUnknownSync(AgentPrivateResultId)(entry.snapshot.uri.split("/").at(-1));
              if (entry.snapshot.uri !== `${baseUri}/snapshots/${snapshotId}`) throw fail("binding_mismatch", "Agent presentation snapshot belongs to another owner");
              const bytes = new Uint8Array(entry.snapshot.byteLength);
              let offset = 0;
              while (offset < bytes.byteLength) {
                const limit = Math.min(32768, bytes.byteLength - offset);
                const chunkUri = `${entry.snapshot.uri}?offset=${offset}&limit=${limit}`;
                const result = await client.readResource({ uri: chunkUri }, { signal: cancellation.signal, timeout: launch.connectTimeoutMs, cacheMode: "bypass" });
                if (stopped) return;
                if (result.contents.length !== 1) throw fail("resource_gap", "Agent presentation snapshot was evicted");
                const chunkContent = result.contents[0];
                if (!chunkContent || chunkContent.uri !== chunkUri || chunkContent.mimeType !== entry.snapshot.mimeType || !("blob" in chunkContent)) throw fail("binding_mismatch", "Agent presentation chunk identity does not match");
                if (chunkContent.blob.length > 44000 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(chunkContent.blob)) throw fail("resource_gap", "Agent presentation chunk encoding is invalid");
                const chunk = Buffer.from(chunkContent.blob, "base64");
                if (chunk.byteLength !== limit || chunk.toString("base64") !== chunkContent.blob) throw fail("resource_gap", "Agent presentation snapshot cursor or EOF is incomplete");
                bytes.set(chunk, offset);
                offset += chunk.byteLength;
              }
              const value = Schema.decodeSync(Schema.fromJsonString(Schema.Json), { onExcessProperty: "error" })(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
              if (stopped) return;
              await waitFor(Promise.resolve(onValue({ binding: entry.binding, seq: entry.seq, key: entry.key, value })), cancellation.signal);
              after = entry.seq;
            }
            if (page.nextAfter !== (page.entries.at(-1)?.seq ?? page.after) || page.more !== (page.nextAfter < page.highWater) || (page.more && page.entries.length === 0)) throw fail("resource_gap", "Agent presentation page made no valid progress");
            more = closeHighWater === undefined ? page.more : after < closeHighWater;
          } while (more && !stopped);
        } while (hinted && !closing && !stopped);
      })().catch((cause) => {
        failure = cause instanceof AgentError ? cause : SdkError.isInstance(cause) ? cause.code === SdkErrorCode.RequestTimeout ? deadlineFailure() : transportFailure() : agentFailure();
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
    const watch: AgentPresentationWatch = {
      drain: sdkEffect((signal) => waitFor(drain(), signal)),
      close,
      connectionClosed: sdkEffect((signal) => waitFor(Promise.race([
        connectionClosedPromise.then(() => { throw transportFailure(); }), connectionErrorPromise, watchFailure,
      ]), signal)),
    };
    return watch;
  });

  const prompt = (input: AgentPromptInputType, options: AgentPromptCallOptions) => Effect.scoped(Effect.gen(function*() {
    if (!validTimeout(options.timeoutMs)) return yield* Effect.fail(fail("invalid_input", "Invalid agent request budget"));
    const deadlineAt = performance.now() + options.timeoutMs;
    let watch: AgentLifecycleWatch | undefined;
    return yield* Effect.gen(function*() {
      let admission: AgentPromptAdmissionType | undefined;
      let settlement: Extract<AgentLifecycleEventType, { kind: "run_settled" }> | undefined;
      watch = yield* watchLifecycle({ binding: input.binding, after: options.after }, async (event) => {
        if (performance.now() >= deadlineAt) throw deadlineFailure();
        await options.onLifecycle?.(event);
        if (event.kind === "session_retired") throw fail("binding_mismatch", "Agent owner generation retired while prompt state was unresolved");
        if (event.kind === "submission_admitted" && event.submissionId === input.submissionId) {
          if (admission !== undefined) throw fail("agent_failure", "Agent prompt submission was admitted more than once");
          admission = { binding: event.binding, submissionId: event.submissionId, disposition: event.disposition, runId: event.runId, eventsUri: agentEventsUri(event.binding), seq: event.seq };
          if ((admission.disposition === "handled") !== (admission.runId === null)) throw fail("agent_failure", "Agent prompt admission has a contradictory run identity");
          if (performance.now() >= deadlineAt) throw deadlineFailure();
          await options.onAdmission(admission);
        }
        if (event.kind === "run_settled" && admission?.runId !== null && event.runId === admission?.runId) {
          if (settlement !== undefined) throw fail("agent_failure", "Agent prompt run settled more than once");
          settlement = event;
        }
      }, deadlineAt);
      const remainingMs = Math.floor(deadlineAt - performance.now());
      if (remainingMs < 1) return yield* Effect.fail(deadlineFailure());
      const result = yield* Effect.raceFirst(call(AgentTools.prompt, input, { timeoutMs: remainingMs }), watch.connectionClosed);
      yield* Effect.raceFirst(watch.drain, watch.connectionClosed);
      yield* watch.close;
      const retainedAdmission = admission;
      const retainedSettlement = settlement;
      yield* Effect.try({ try: () => assertBinding(input.binding, result.admission.binding), catch: () => fail("binding_mismatch", "Agent prompt admission belongs to another owner generation") });
      if (result.admission.submissionId !== input.submissionId) return yield* Effect.fail(fail("binding_mismatch", "Agent prompt result belongs to another submission"));
      if (retainedAdmission === undefined) return yield* Effect.fail(fail("resource_gap", "Agent prompt has no retained admission proof"));
      if (!isDeepStrictEqual(retainedAdmission, result.admission)) return yield* Effect.fail(fail("agent_failure", "Agent prompt admission event disagrees with the final result"));
      if (result.disposition === "handled") {
        if (retainedAdmission.disposition !== "handled" || retainedAdmission.runId !== null) return yield* Effect.fail(fail("agent_failure", "Agent handled result contradicts its admission"));
      } else {
        yield* Effect.try({ try: () => assertBinding(retainedAdmission.binding, result.final.binding), catch: () => fail("binding_mismatch", "Agent prompt final result belongs to another owner generation") });
        if (retainedAdmission.disposition === "handled" || retainedAdmission.runId === null || result.final.runId !== retainedAdmission.runId) return yield* Effect.fail(fail("agent_failure", "Agent final result contradicts its admitted run"));
        if (retainedSettlement === undefined) return yield* Effect.fail(fail("resource_gap", "Agent prompt has no retained whole-run settlement proof"));
        yield* Effect.try({ try: () => assertBinding(result.final.binding, retainedSettlement.binding), catch: () => fail("binding_mismatch", "Agent prompt settlement belongs to another owner generation") });
        if (retainedSettlement.seq <= retainedAdmission.seq || retainedSettlement.outcome !== result.final.outcome) return yield* Effect.fail(fail("agent_failure", "Agent final result contradicts its lifecycle settlement"));
      }
      return result;
    }).pipe(
      Effect.timeoutOrElse({ duration: options.timeoutMs, orElse: () => Effect.fail(deadlineFailure()) }),
      Effect.onExit(() => watch?.close ?? Effect.void),
    );
  }));

  return {
    host: launch.host,
    binding: initialState.binding,
    pid,
    capabilities,
    connectionClosed: Effect.tryPromise({ try: (signal) => waitFor(connectionClosedPromise, signal).then(() => { throw transportFailure(); }), catch: (cause) => cause instanceof AgentError ? cause : transportFailure() }),
    connectionError: Effect.tryPromise({ try: (signal) => waitFor(connectionErrorPromise, signal), catch: (cause) => cause instanceof AgentError ? cause : transportFailure() }),
    observedProcessClosed: Effect.tryPromise({ try: (signal) => waitFor(processClosedPromise, signal), catch: (cause) => cause instanceof AgentError ? cause : transportFailure() }),
    call,
    prompt,
    watchLifecycle,
    watchPresentation,
  };
});
