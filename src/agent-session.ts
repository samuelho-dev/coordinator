import { ByteSize, Config, Effect, Exit, Layer, ManagedRuntime, Option, Schema, Scope } from "effect";
import { HttpIncomingMessage, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpServer, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import * as PiSdk from "@earendil-works/pi-coding-agent";
import type { AgentSession as PiSession, AgentSessionEvent as PiEvent, SessionManager as PiManager, SessionEntry as PiSessionEntry } from "@earendil-works/pi-coding-agent";
import type { AgentSession as OmpSession, AgentSessionEvent as OmpEvent, SessionManager as OmpManager, SessionEntry as OmpSessionEntry } from "@oh-my-pi/pi-coding-agent";
import { AgentError, AgentConfig, AgentEndpoint, AgentTools, AgentPrivateResultBytes, boundedJsonSchema, AgentGeneration, AgentIncarnationId, AgentRunId, AgentSessionId, AgentHostEnum, AgentRunOutcomeEnum, AgentAdmissionDispositionEnum, AgentRunPhaseEnum, AgentPhaseTransitionEnum, agentEventsUri } from "./agent/protocol.ts";
import type { AgentAbortResult, AgentAdmissionDisposition, AgentBinding, AgentHost, AgentPromptAdmission, AgentPromptInput, AgentPromptResult, AgentRunOutcome } from "./agent/protocol.ts";
import { agentHttpLayer, agentTool } from "./agent/server.ts";
import { Platform } from "./platform.ts";
import type { Context, Host } from "./plugin.ts";
import { AgentLifecycleJournal } from "./agent/protocol.ts";

type Selection = { provider: string; id: string };
const failure = () => new AgentError({ code: "agent_failure", message: "Owned SDK operation failed; no result was inferred" });
const operation = <A>(run: () => Promise<A>) => Effect.tryPromise({
  try: run,
  catch: error => Option.getOrElse(Schema.decodeUnknownOption(AgentError)(error), failure),
});

export interface AgentSession {
  readonly session: PiSession | OmpSession;
  readonly manager: PiManager | OmpManager;
  readonly backend: ({ host: typeof AgentHostEnum.pi; session: PiSession; manager: PiManager } | { host: typeof AgentHostEnum.omp; session: OmpSession; manager: OmpManager }) & { drain(): Promise<void>; dispose(): Promise<void> };
  readonly binding: AgentBinding;
  readonly journal: AgentLifecycleJournal;
  readonly currentRunId: AgentRunId | null;
  readonly pendingSubmissions: number;
  readonly pendingMessageCount: number;
  readonly pendingAsyncWorkCount: number | null;
  readonly isBusy: boolean;
  retireIfIdle(): boolean;
  bound(binding?: AgentBinding): Effect.Effect<void, AgentError>;
  prompt(input: AgentPromptInput, onAdmission?: (admission: AgentPromptAdmission) => void | Promise<void>, canAdmit?: () => boolean): Effect.Effect<AgentPromptResult, AgentError>;
  queue(input: Omit<AgentPromptInput, "submissionId" | "streamingBehavior">, behavior: "steer" | "followUp"): Effect.Effect<void | "queued" | "handled", AgentError>;
  abort(): Effect.Effect<AgentAbortResult, AgentError>;
  operation<A>(run: () => Promise<A>): Effect.Effect<A, AgentError>;
}

// These are independent SDK sessions, never handles to the surrounding TUI/RPC session.
export const makeAgentSession = Effect.fn("Plugin.ownedSession")(function*(host: AgentHost, cwd: string, directory: string, model?: Selection): Effect.fn.Return<AgentSession, AgentError, Scope.Scope> {
  const backend = yield* Effect.acquireRelease(operation(async () => {
    if (host === AgentHostEnum.pi) {
      const agentDir = PiSdk.getAgentDir();
      const settings = PiSdk.SettingsManager.inMemory(PiSdk.SettingsManager.create(cwd, agentDir).getSettings());
      const resourceLoader = new PiSdk.DefaultResourceLoader({ cwd, agentDir, settingsManager: settings, noExtensions: true });
      await resourceLoader.reload();
      const manager = PiSdk.SessionManager.create(cwd, directory);
      const modelRuntime = await PiSdk.ModelRuntime.create();
      const selected = model === undefined ? undefined : modelRuntime.getModel(model.provider, model.id);
      if (model !== undefined && selected === undefined) throw failure();
      const { session } = await PiSdk.createAgentSession({ cwd, agentDir, sessionManager: manager, settingsManager: settings, resourceLoader, modelRuntime, ...(selected === undefined ? {} : { model: selected }) });
      return { host, session, manager, drain: () => session.waitForIdle(), dispose: async () => { await session.abort(); session.dispose(); } };
    }
    // OMP's SDK requires Bun; Pi runs on Node and must not load it.
    const sdk = await import("@oh-my-pi/pi-coding-agent");
    const { AsyncJobManager } = await import("@oh-my-pi/pi-coding-agent/async");
    const { AgentRegistry } = await import("@oh-my-pi/pi-coding-agent/registry/agent-registry");
    const agentId = `coordinator-${randomUUID()}`;
    const settings = (await sdk.Settings.loadReadOnly({ cwd, agentDir: sdk.getAgentDir() })).overlay();
    const manager = sdk.SessionManager.create(cwd, directory);
    const { session } = await sdk.createAgentSession({ cwd, sessionManager: manager, settings,
      disableExtensionDiscovery: true, bindProcessState: false, enableMCP: false, agentId, agentRegistry: new AgentRegistry(),
      // The public SDK routes scoped jobs to this session; never replace or dispose the host's manager.
      ...(AsyncJobManager.instance() === undefined ? {} : { parentTaskPrefix: agentId }),
      ...(model === undefined ? {} : { modelPattern: `${model.provider}/${model.id}` }),
    });
    return { host, session, manager, drain: async () => {
      do {
        await session.waitForAdmittedSubmissions();
        await session.waitForIdle();
        if (session.hasPendingAsyncWork()) await session.settleAsyncWork();
        await session.waitForAdmittedSubmissions();
        await session.waitForIdle();
      } while (session.hasPendingAsyncWork() || session.hasAdmittedSubmission || session.isStreaming || session.isCompacting);
    }, dispose: async () => { session.beginDispose(); await session.dispose(); } };
  }), (backend) => operation(() => backend.dispose()).pipe(Effect.orDie));
  const { session, manager } = backend;
  const binding: AgentBinding = { host, sessionId: Schema.decodeSync(AgentSessionId)(manager.getSessionId()),
    incarnation: AgentIncarnationId.make(randomUUID()), generation: AgentGeneration.make(1) };
  const journal = new AgentLifecycleJournal();
  let retired = false;
  type Submission = { admission: AgentPromptAdmission | undefined; publication: Promise<void>; publicationFailed: boolean;
    onAdmission: ((admission: AgentPromptAdmission) => void | Promise<void>) | undefined };
  type Run = Submission & { id: AgentRunId; input: AgentPromptInput; outcome: AgentRunOutcome | undefined; cancelled: boolean; started: boolean; terminal: boolean;
    accepting: boolean; enqueues: Promise<void>[]; result: Promise<AgentPromptResult> };
  let active: Run | undefined;
  let tail = Promise.resolve();
  let pending = 0;
  let runningOperations = 0;
  let cancellationEpoch = 0;
  const submissions = new Set<string>();
  const admit = (run: Run, input: AgentPromptInput, disposition: AgentAdmissionDisposition, submission: Submission = run) => {
    if (submission.admission !== undefined) return;
    const runId = disposition === AgentAdmissionDispositionEnum.handled ? null : run.id;
    const event = journal.append({ type: "agent_lifecycle", kind: "submission_admitted", binding, runId, at: Date.now(), submissionId: input.submissionId, disposition });
    submission.admission = { binding, submissionId: input.submissionId, disposition, runId, eventsUri: agentEventsUri(binding), seq: event.seq };
    const admission = submission.admission;
    submission.publication = Promise.resolve().then(() => submission.onAdmission?.(admission)).then(() => undefined, () => { submission.publicationFailed = true; });
  };
  const observe = (event: PiEvent | OmpEvent) => {
    const run = active;
    if (run === undefined) return;
    const phase = (phase: AgentRunPhaseEnum, transition: AgentPhaseTransitionEnum) => {
      journal.append({ type: "agent_lifecycle", kind: "run_phase", binding, runId: run.id, at: Date.now(), phase, transition });
    };
    switch (event.type) {
      case "agent_start":
        if (backend.host === AgentHostEnum.omp) admit(run, run.input, AgentAdmissionDispositionEnum.started);
        run.terminal = false;
        run.outcome = undefined;
        if (!run.started) {
          run.started = true;
          journal.append({ type: "agent_lifecycle", kind: "run_started", binding, runId: run.id, at: Date.now() });
        }
        break;
      case "message_end":
        if (event.message.role === "assistant") {
          switch (event.message.stopReason) {
            case "aborted": run.outcome = AgentRunOutcomeEnum.aborted; break;
            case "error": run.outcome = AgentRunOutcomeEnum.error; break;
            case "pending":
            case "stop":
            case "length":
            case "toolUse":
            case "deferred": run.outcome = AgentRunOutcomeEnum.completed; break;
          }
        }
        break;
      case "auto_retry_start": phase(AgentRunPhaseEnum.retry, AgentPhaseTransitionEnum.started); break;
      case "auto_retry_end": phase(AgentRunPhaseEnum.retry, AgentPhaseTransitionEnum.ended); break;
      case "auto_compaction_start":
      case "compaction_start": phase(AgentRunPhaseEnum.compaction, AgentPhaseTransitionEnum.started); break;
      case "auto_compaction_end":
      case "compaction_end": phase(AgentRunPhaseEnum.compaction, AgentPhaseTransitionEnum.ended); break;
    }
  };
  const unsubscribe = backend.host === AgentHostEnum.pi
    ? backend.session.subscribe((event) => { observe(event); if (event.type === "agent_settled" && active !== undefined) active.terminal = true; })
    : backend.session.subscribe((event) => { observe(event); if (event.type === "agent_end" && active !== undefined) active.terminal = event.isTerminal === true; });
  journal.append({ type: "agent_lifecycle", kind: "session_started", binding, runId: null, at: Date.now() });
  const bound = (requested?: AgentBinding) => Effect.try({ try: () => {
    if (retired || manager.getSessionId() !== binding.sessionId || (requested !== undefined && agentEventsUri(requested) !== agentEventsUri(binding))) {
      throw new AgentError({ code: "binding_mismatch", message: "Owned SDK session generation changed" });
    }
  }, catch: () => new AgentError({ code: "binding_mismatch", message: "Owned SDK session generation changed" }) });
  const execute = async (input: AgentPromptInput, epoch: number, result: Promise<AgentPromptResult>, onAdmission?: (admission: AgentPromptAdmission) => void | Promise<void>, canAdmit?: () => boolean): Promise<AgentPromptResult> => {
    if (retired || epoch !== cancellationEpoch) throw failure();
    if (canAdmit && !canAdmit()) throw new AgentError({ code: "invalid_input", message: "Agent admission is fenced" });
    const run: Run = { id: AgentRunId.make(randomUUID()), input, admission: undefined, outcome: undefined, cancelled: false, started: false, terminal: false,
      publication: Promise.resolve(), publicationFailed: false, onAdmission, accepting: true, enqueues: [], result };
    active = run;
    try {
      try {
        const images = input.images === undefined ? {} : { images: Array.from(input.images) };
        if (backend.host === AgentHostEnum.pi) {
          await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, source: "rpc",
            preflightResult: (disposition) => admit(run, input, disposition) });
        } else {
          const invoked = await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, throwOnDrop: true });
          if (!invoked) admit(run, input, AgentAdmissionDispositionEnum.handled);
        }
      } catch {
        run.outcome = AgentRunOutcomeEnum.error;
        // Setup rejection has no admission. Admitted failures still drain the actual SDK run.
      }
      await backend.drain();
      run.accepting = false;
      await Promise.all(run.enqueues);
      await backend.drain();
      const admission = run.admission;
      if (admission === undefined) throw failure();
      if (admission.disposition === AgentAdmissionDispositionEnum.handled) {
        await run.publication;
        if (run.publicationFailed) throw failure();
        return { admission, disposition: "handled" };
      }
      const queuedMessages = sdkPendingMessageCount();
      if (((!run.terminal || run.outcome === undefined) && !run.cancelled) || queuedMessages > 0) {
        retired = true;
        journal.append({ type: "agent_lifecycle", kind: "session_retired", binding, runId: run.id, at: Date.now(), reason: "SDK terminal proof unavailable" });
        throw failure();
      }
      const outcome = run.cancelled ? AgentRunOutcomeEnum.aborted : run.outcome;
      if (outcome === undefined) throw failure();
      const final = { binding, runId: run.id, outcome };
      journal.append({ type: "agent_lifecycle", kind: "run_settled", binding, runId: run.id, at: Date.now(), outcome });
      active = undefined;
      await run.publication;
      if (run.publicationFailed) throw failure();
      return { admission, disposition: "settled", final };
    } finally { run.accepting = false; active = undefined; }
  };
  const prompt = (input: AgentPromptInput, onAdmission?: (admission: AgentPromptAdmission) => void | Promise<void>, canAdmit?: () => boolean) => Effect.gen(function*(): Effect.fn.Return<AgentPromptResult, AgentError> {
    yield* bound(input.binding);
    if (canAdmit && !canAdmit()) return yield* new AgentError({ code: "invalid_input", message: "Agent admission is fenced" });
    if (submissions.has(input.submissionId)) return yield* new AgentError({ code: "invalid_input", message: "Submission identity was already used; delivery is never replayed" });
    submissions.add(input.submissionId);
    const run = active;
    if (input.streamingBehavior !== undefined && run?.accepting && session.isStreaming) {
      const submission: Submission = { admission: undefined, publication: Promise.resolve(), publicationFailed: false, onAdmission };
      const enqueue = (async () => {
        const images = input.images === undefined ? {} : { images: Array.from(input.images) };
        if (backend.host === AgentHostEnum.pi) await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, source: "rpc", streamingBehavior: input.streamingBehavior,
          preflightResult: (disposition) => admit(run, input, disposition, submission) });
        else {
          const invoked = await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, throwOnDrop: true, streamingBehavior: input.streamingBehavior,
            onPromptAdmitted: () => admit(run, input, AgentAdmissionDispositionEnum.queued, submission) });
          if (!invoked) admit(run, input, AgentAdmissionDispositionEnum.handled, submission);
        }
      })();
      // Queue insertion belongs to this logical run, including admission that races its last turn.
      run.enqueues.push(enqueue.then(() => undefined, () => undefined));
      yield* operation(() => enqueue);
      const admission = submission.admission;
      if (admission === undefined) return yield* failure();
      yield* operation(() => submission.publication);
      if (submission.publicationFailed) return yield* failure();
      if (admission.disposition === AgentAdmissionDispositionEnum.handled) return { admission, disposition: "handled" };
      const result = yield* operation(() => run.result);
      if (result.disposition !== "settled") return yield* failure();
      return { admission, disposition: "settled", final: result.final };
    }
    // ponytail: one FIFO per owned session. Concurrent independent submissions never share an outcome.
    const epoch = cancellationEpoch;
    pending++;
    const result: Promise<AgentPromptResult> = tail.then(() => execute(input, epoch, result, onAdmission, canAdmit)).finally(() => { pending--; });
    // A failed ticket must not poison later independent requests. The caller receives its failure.
    tail = result.then(() => undefined, () => undefined);
    return yield* operation(() => result);
  });
  const queue = (input: Omit<AgentPromptInput, "submissionId" | "streamingBehavior">, behavior: "steer" | "followUp") => Effect.gen(function*() {
    yield* bound(input.binding);
    const run = active;
    if (!run?.accepting || !session.isStreaming) return yield* new AgentError({ code: "invalid_input", message: "Queue controls require an active owned SDK turn; use agent.prompt for independent input" });
    const images = input.images === undefined ? undefined : Array.from(input.images);
    const enqueue = behavior === "steer" ? session.steer(input.message, images) : session.followUp(input.message, images);
    run.enqueues.push(enqueue.then(() => undefined, () => undefined));
    return yield* operation<void | "queued" | "handled">(() => enqueue);
  });
  const abort = () => operation(async () => {
    const run = active;
    cancellationEpoch++;
    if (run !== undefined) run.cancelled = true;
    if (backend.host === AgentHostEnum.omp) {
      for (const job of backend.session.getAsyncJobSnapshot()?.running ?? []) backend.session.cancelAsyncJob(job.id);
      backend.session.clearQueue({ forInterrupt: true });
    }
    else backend.session.clearQueue();
    session.abortBash();
    await session.abort();
    const disposition: "requested" = "requested";
    return { binding, runId: run?.admission?.runId ?? null, disposition };
  });
  yield* Effect.addFinalizer(() => operation(async () => {
    retired = true;
    cancellationEpoch++;
    if (backend.host === AgentHostEnum.omp) backend.session.beginDispose();
    if (active !== undefined) active.cancelled = true;
    if (backend.host === AgentHostEnum.omp) {
      for (const job of backend.session.getAsyncJobSnapshot()?.running ?? []) backend.session.cancelAsyncJob(job.id);
      backend.session.clearQueue({ forInterrupt: true });
    }
    else backend.session.clearQueue();
    session.abortBash();
    await session.abort();
    await tail;
    unsubscribe();
    journal.append({ type: "agent_lifecycle", kind: "session_retired", binding, runId: null, at: Date.now(), reason: "owned SDK session disposed" });
  }).pipe(Effect.orDie));
  const sdkPendingMessageCount = () => backend.host === AgentHostEnum.pi ? backend.session.pendingMessageCount : backend.session.queuedMessageCount;
  const pendingAsyncWorkCount = () => backend.host === AgentHostEnum.pi ? 0 : backend.session.hasPendingAsyncWork() ? null : 0;
  const isBusy = () => pending > 0 || session.isStreaming || session.isCompacting || sdkPendingMessageCount() > 0 || pendingAsyncWorkCount() !== 0;
  const retireIfIdle = () => {
    if (runningOperations > 0 || active !== undefined || isBusy()) return false;
    retired = true;
    return true;
  };
  return { session, manager, backend, binding, journal, bound, prompt, queue, abort, retireIfIdle,
    get currentRunId() { return active?.admission?.runId ?? null; }, get pendingSubmissions() { return pending; },
    get pendingMessageCount() { return sdkPendingMessageCount() + Math.max(0, pending - 1); },
    get pendingAsyncWorkCount() { return pendingAsyncWorkCount(); },
    get isBusy() { return isBusy(); },
    operation: <A>(run: () => Promise<A>) => operation(async () => {
      if (retired) throw new AgentError({ code: "binding_mismatch", message: "Owned SDK session retired" });
      runningOperations++;
      try { return await run(); }
      finally { runningOperations--; }
    }) };
});

const encodeTranscriptJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeTranscriptJson = Schema.decodeEffect(Schema.fromJsonString(boundedJsonSchema));
const transcriptJson = Effect.fn("Plugin.transcriptJson")(function*(value: PiSessionEntry | OmpSessionEntry | PiSession["messages"][number] | OmpSession["messages"][number], kind: "entry" | "message") {
  const json = yield* encodeTranscriptJson(value).pipe(Effect.mapError(() => new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} is not JSON serializable` })));
  if (Buffer.byteLength(json, "utf8") > AgentPrivateResultBytes) return yield* new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} exceeds the agent contract` });
  return yield* decodeTranscriptJson(json).pipe(Effect.mapError(() => new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} exceeds the agent contract` })));
});

export const makeAgentSessionServer = Effect.fn("Plugin.agentControl")(function*(config: AgentConfig, initial: Pick<Context, "cwd" | "model">) {
  const platform = yield* Platform;
  if (!platform.path.isAbsolute(config.endpointFile) || !platform.path.isAbsolute(config.tokenFile) ||
    (yield* platform.fs.realPath(config.tokenFile)) !== config.tokenFile ||
    ((yield* platform.fs.stat(platform.path.dirname(config.endpointFile))).mode & 0o077) !== 0) {
    return yield* new AgentError({ code: "invalid_input", message: "Canonical private plugin control paths required" });
  }
  if (yield* platform.fs.exists(config.endpointFile)) return yield* new AgentError({ code: "binding_mismatch", message: "Plugin endpoint is already owned; refusing to replace it" });
  const token = yield* platform.privateFile(config.tokenFile);
  if (token.length < 32 || token.length > 480) return yield* new AgentError({ code: "invalid_input", message: "Invalid private plugin credential" });
  const tokenHash = yield* platform.digest(token);
  const directory = platform.path.join(platform.path.dirname(config.endpointFile), "sessions");
  yield* platform.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const owner = yield* makeAgentSession(config.host, initial.cwd, directory, initial.model);
  const { binding, journal, bound, session, manager, operation } = owner;
  const tools = [
    agentTool(AgentTools.prompt, (input) => owner.prompt(input)),
    agentTool(AgentTools.abortAndPrompt, (input) => Effect.gen(function*() { yield* bound(input.binding); yield* owner.abort(); return yield* owner.prompt(input); })),
    agentTool(AgentTools.getState, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const model = session.model;
      const sessionFile = manager.getSessionFile();
      return { binding, ...(sessionFile === undefined ? {} : { sessionFile }), cwd: initial.cwd,
        model: model === undefined ? null : { id: model.id, name: model.name, api: model.api, provider: model.provider, reasoning: model.reasoning,
          input: model.input, cost: model.cost, contextWindow: model.contextWindow ?? null, maxTokens: model.maxTokens ?? null },
        currentRunId: owner.currentRunId, isStreaming: session.isStreaming || owner.pendingSubmissions > 0, isCompacting: session.isCompacting,
        pendingMessageCount: owner.pendingMessageCount,
        pendingAsyncWorkCount: owner.pendingAsyncWorkCount,
        messageCount: session.messages.length, systemPrompt: session.systemPrompt };
    })),
    agentTool(AgentTools.getMessages, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = manager.getBranch().filter((entry) => entry.type === "message");
      const previous = input.cursor === undefined ? -1 : entries.findIndex((entry) => entry.id === input.cursor);
      if (input.cursor !== undefined && previous < 0) return yield* new AgentError({ code: "resource_gap", message: "Transcript cursor is no longer on this branch" });
      const page = entries.slice(previous + 1, previous + 1 + (input.limit ?? 128));
      const more = previous + 1 + page.length < entries.length;
      return { binding, messages: yield* Effect.forEach(page, (entry) => transcriptJson(entry.message, "message")), nextCursor: more ? page.at(-1)?.id ?? null : null, more };
    })),
    agentTool(AgentTools.abort, (input) => Effect.gen(function*() { yield* bound(input.binding); return yield* owner.abort(); })),
    agentTool(AgentTools.steer, (input) => Effect.gen(function*() {
      const result = yield* owner.queue(input, "steer");
      return result === undefined ? null : { disposition: result };
    })),
    agentTool(AgentTools.followUp, (input) => Effect.gen(function*() {
      const result = yield* owner.queue(input, "followUp");
      return result === undefined ? null : { disposition: result };
    })),
    agentTool(AgentTools.getEntries, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = manager.getEntries();
      const previous = input.since === undefined ? -1 : entries.findIndex((entry) => entry.id === input.since);
      if (input.since !== undefined && previous < 0) return yield* new AgentError({ code: "resource_gap", message: "Append-history cursor is unavailable" });
      return { entries: yield* Effect.forEach(entries.slice(previous + 1), (entry) => transcriptJson(entry, "entry")), leafId: manager.getLeafId() };
    })),
    agentTool(AgentTools.bash, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const result = yield* operation(() => session.executeBash(input.command, undefined, { excludeFromContext: input.excludeFromContext ?? true }));
      return { binding, exitCode: result.exitCode ?? null, output: result.output, truncated: result.truncated, killed: result.cancelled };
    })),
  ];
  const app = yield* HttpRouter.toHttpEffect(agentHttpLayer({ host: config.host, binding, journal, tools, name: "coordinator-owned-sdk", version: "0.1.0" }));
  const server = yield* NodeHttpServer.make(() => createServer({ requestTimeout: 15000 }), { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" });
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const authorization = request.headers.authorization;
    if (!authorization?.startsWith("Bearer ") || authorization.length > 512 || !(yield* platform.checkToken(authorization.slice(7), tokenHash))) return HttpServerResponse.empty({ status: 401, headers: { "cache-control": "no-store" } });
    return yield* app.pipe(Effect.interruptible, Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(64 * 1024)));
  }));
  const address = server.address;
  if (address._tag !== "InetAddressV4") return yield* new AgentError({ code: "agent_failure", message: "Plugin control requires an IPv4 loopback address" });
  const pendingEndpoint = `${config.endpointFile}.${binding.incarnation}.pending`;
  yield* Effect.acquireRelease(platform.fs.writeFileString(pendingEndpoint, Schema.encodeSync(Schema.fromJsonString(AgentEndpoint))({ url: `http://127.0.0.1:${address.port}/mcp`, binding }), { flag: "wx", mode: 0o600 }), () => platform.fs.remove(pendingEndpoint, { force: true }).pipe(Effect.orDie));
  yield* platform.fs.link(pendingEndpoint, config.endpointFile);
  yield* Effect.addFinalizer(() => platform.fs.remove(config.endpointFile).pipe(Effect.orDie));
  yield* platform.fs.remove(pendingEndpoint);
  return owner;
});

export default async function agentControl(host: Pick<Host, "on">): Promise<void> {
  const layer = Layer.merge(Platform.layer, NodeServices.layer);
  let runtime = ManagedRuntime.make(layer);
  let stopped = false;
  try {
    const path = await runtime.runPromise(Config.option(Config.String("COORDINATOR_AGENT_CONFIG")));
    if (Option.isNone(path)) { await runtime.dispose(); return; }
    const config = await runtime.runPromise(Effect.gen(function*() {
      const platform = yield* Platform;
      if (!platform.path.isAbsolute(path.value)) return yield* new AgentError({ code: "invalid_input", message: "Absolute plugin configuration path required" });
      return yield* Schema.decodeEffect(Schema.fromJsonString(AgentConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path.value)).pipe(Effect.mapError(() => new AgentError({ code: "invalid_input", message: "Invalid private agent plugin configuration" })));
    }));
    let scope: Scope.Closeable | undefined;
    const start = async (ctx: Context): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (stopped) { runtime = ManagedRuntime.make(layer); stopped = false; }
      if (scope !== undefined) await runtime.runPromise(Scope.close(scope, Exit.void));
      const currentScope = await runtime.runPromise(Scope.make());
      scope = currentScope;
      await runtime.runPromise(makeAgentSessionServer(config, ctx).pipe(Effect.provideService(Scope.Scope, currentScope), Effect.onExit((exit) => exit._tag === "Failure" ? Scope.close(currentScope, exit) : Effect.void)));
    };
    host.on("session_start", (_event, ctx) => start(ctx));
    if (config.host === AgentHostEnum.omp) host.on("session_switch", (_event, ctx) => start(ctx));
    host.on("session_shutdown", async () => {
      if (scope !== undefined) { await runtime.runPromise(Scope.close(scope, Exit.void)); scope = undefined; }
      stopped = true;
      await runtime.dispose();
    });
  } catch { await runtime.dispose(); throw new AgentError({ code: "agent_failure", message: "Owned agent plugin initialization failed; check private configuration" }); }
}
