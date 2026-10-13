import { Effect, Schema } from "effect";
import type { Scope } from "effect";
import { randomUUID } from "node:crypto";
import type { AgentSession as PiSession, AgentSessionEvent as PiEvent, SessionManager as PiManager } from "@earendil-works/pi-coding-agent";
import type { AgentSession as OmpSession, AgentSessionEvent as OmpEvent, SessionManager as OmpManager } from "@oh-my-pi/pi-coding-agent";
import { NativeControlError, NativeGeneration, NativeIncarnationId, NativeRunId, NativeSessionId, nativeEventsUri } from "@samuelho/coordinator-native-control/contracts";
import type { NativeAbortResult, NativeBinding, NativeHost, NativePromptAdmission, NativePromptInput, NativePromptResult } from "@samuelho/coordinator-native-control/contracts";
import { NativeLifecycleJournal } from "@samuelho/coordinator-native-control/lifecycle";

type Outcome = "completed" | "aborted" | "error";
type Selection = { provider: string; id: string };
const failure = () => new NativeControlError({ code: "native_failure", message: "Owned SDK operation failed; no result was inferred" });
const operation = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: error => error instanceof NativeControlError ? error : failure() });

export interface NativeSession {
  readonly session: PiSession | OmpSession;
  readonly manager: PiManager | OmpManager;
  readonly backend: ({ host: "pi"; session: PiSession; manager: PiManager } | { host: "omp"; session: OmpSession; manager: OmpManager }) & { drain(): Promise<void>; dispose(): Promise<void> };
  readonly binding: NativeBinding;
  readonly journal: NativeLifecycleJournal;
  readonly currentRunId: NativeRunId | null;
  readonly pendingSubmissions: number;
  retireIfIdle(): boolean;
  bound(binding?: NativeBinding): Effect.Effect<void, NativeControlError>;
  prompt(input: NativePromptInput, onAdmission?: (admission: NativePromptAdmission) => void | Promise<void>, canAdmit?: () => boolean): Effect.Effect<NativePromptResult, NativeControlError>;
  queue(input: Omit<NativePromptInput, "submissionId" | "streamingBehavior">, behavior: "steer" | "followUp"): Effect.Effect<void | "queued" | "handled", NativeControlError>;
  abort(): Effect.Effect<NativeAbortResult, NativeControlError>;
  operation<A>(run: () => Promise<A>): Effect.Effect<A, NativeControlError>;
}

// These are independent SDK sessions, never handles to the surrounding TUI/RPC session.
export const makeNativeSession = Effect.fn("Plugin.ownedSession")(function*(host: NativeHost, cwd: string, directory: string, model?: Selection): Effect.fn.Return<NativeSession, NativeControlError, Scope.Scope> {
  const backend = yield* Effect.acquireRelease(operation(async () => {
    // Platform boundary: OMP's source SDK requires Bun and cannot be loaded by Pi's Node host.
    if (host === "pi") {
      const sdk = await import("@earendil-works/pi-coding-agent");
      const agentDir = sdk.getAgentDir();
      const settings = sdk.SettingsManager.inMemory(sdk.SettingsManager.create(cwd, agentDir).getSettings());
      const resourceLoader = new sdk.DefaultResourceLoader({ cwd, agentDir, settingsManager: settings, noExtensions: true });
      await resourceLoader.reload();
      const manager = sdk.SessionManager.create(cwd, directory);
      const modelRuntime = await sdk.ModelRuntime.create();
      const selected = model === undefined ? undefined : modelRuntime.getModel(model.provider, model.id);
      if (model !== undefined && selected === undefined) throw failure();
      const { session } = await sdk.createAgentSession({ cwd, agentDir, sessionManager: manager, settingsManager: settings, resourceLoader, modelRuntime, ...(selected === undefined ? {} : { model: selected }) });
      return { host, session, manager, drain: () => session.waitForIdle(), dispose: async () => { await session.abort(); session.dispose(); } };
    }
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
  const binding: NativeBinding = { host, sessionId: Schema.decodeUnknownSync(NativeSessionId)(manager.getSessionId()),
    incarnation: NativeIncarnationId.make(randomUUID()), generation: NativeGeneration.make(1) };
  const journal = new NativeLifecycleJournal();
  let retired = false;
  type Submission = { admission: NativePromptAdmission | undefined; publication: Promise<void>; publicationFailed: boolean;
    onAdmission: ((admission: NativePromptAdmission) => void | Promise<void>) | undefined };
  type Run = Submission & { id: NativeRunId; input: NativePromptInput; outcome: Outcome | undefined; cancelled: boolean; started: boolean; terminal: boolean;
    accepting: boolean; enqueues: Promise<void>[]; result: Promise<NativePromptResult> };
  let active: Run | undefined;
  let tail = Promise.resolve();
  let pending = 0;
  let runningOperations = 0;
  let cancellationEpoch = 0;
  const submissions = new Set<string>();
  const admit = (run: Run, input: NativePromptInput, disposition: "started" | "queued" | "handled", submission: Submission = run) => {
    if (submission.admission !== undefined) return;
    const runId = disposition === "handled" ? null : run.id;
    const event = journal.append({ type: "native_lifecycle", kind: "submission_admitted", binding, runId, at: Date.now(), submissionId: input.submissionId, disposition });
    submission.admission = { binding, submissionId: input.submissionId, disposition, runId, eventsUri: nativeEventsUri(binding), seq: event.seq };
    const admission = submission.admission;
    submission.publication = Promise.resolve().then(() => submission.onAdmission?.(admission)).then(() => undefined, () => { submission.publicationFailed = true; });
  };
  const observe = (event: PiEvent | OmpEvent) => {
    const run = active;
    if (run === undefined) return;
    if (event.type === "agent_start" && backend.host === "omp") admit(run, run.input, "started");
    if (event.type === "agent_start") {
      run.terminal = false;
      run.outcome = undefined;
      if (!run.started) {
        run.started = true;
        journal.append({ type: "native_lifecycle", kind: "run_started", binding, runId: run.id, at: Date.now() });
      }
    }
    if (event.type === "message_end" && event.message.role === "assistant") {
      run.outcome = event.message.stopReason === "aborted" ? "aborted" : event.message.stopReason === "error" ? "error" : "completed";
    }
    if (event.type === "auto_retry_start" || event.type === "auto_retry_end" || event.type === "auto_compaction_start" || event.type === "auto_compaction_end" || event.type === "compaction_start" || event.type === "compaction_end") {
      journal.append({ type: "native_lifecycle", kind: "run_phase", binding, runId: run.id, at: Date.now(),
        phase: event.type === "auto_retry_start" || event.type === "auto_retry_end" ? "retry" : "compaction",
        transition: event.type.endsWith("start") ? "started" : "ended" });
    }
  };
  const unsubscribe = backend.host === "pi"
    ? backend.session.subscribe((event) => { observe(event); if (event.type === "agent_settled" && active !== undefined) active.terminal = true; })
    : backend.session.subscribe((event) => { observe(event); if (event.type === "agent_end" && active !== undefined) active.terminal = event.isTerminal === true; });
  journal.append({ type: "native_lifecycle", kind: "session_started", binding, runId: null, at: Date.now() });
  const bound = (requested?: NativeBinding) => Effect.try({ try: () => {
    if (retired || manager.getSessionId() !== binding.sessionId || (requested !== undefined && nativeEventsUri(requested) !== nativeEventsUri(binding))) {
      throw new NativeControlError({ code: "binding_mismatch", message: "Owned SDK session generation changed" });
    }
  }, catch: () => new NativeControlError({ code: "binding_mismatch", message: "Owned SDK session generation changed" }) });
  const execute = async (input: NativePromptInput, epoch: number, result: Promise<NativePromptResult>, onAdmission?: (admission: NativePromptAdmission) => void | Promise<void>, canAdmit?: () => boolean): Promise<NativePromptResult> => {
    if (retired || epoch !== cancellationEpoch) throw failure();
    if (canAdmit && !canAdmit()) throw new NativeControlError({ code: "invalid_input", message: "Native admission is fenced" });
    const run: Run = { id: NativeRunId.make(randomUUID()), input, admission: undefined, outcome: undefined, cancelled: false, started: false, terminal: false,
      publication: Promise.resolve(), publicationFailed: false, onAdmission, accepting: true, enqueues: [], result };
    active = run;
    try {
      try {
        const images = input.images === undefined ? {} : { images: Array.from(input.images) };
        if (backend.host === "pi") {
          await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, source: "rpc",
            preflightResult: (disposition) => admit(run, input, disposition) });
        } else {
          const invoked = await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, throwOnDrop: true });
          if (!invoked) admit(run, input, "handled");
        }
      } catch {
        run.outcome = "error";
        // Setup rejection has no admission. Admitted failures still drain the actual SDK run.
      }
      await backend.drain();
      run.accepting = false;
      await Promise.all(run.enqueues);
      await backend.drain();
      const admission = run.admission;
      if (admission === undefined) throw failure();
      if (admission.disposition === "handled") {
        await run.publication;
        if (run.publicationFailed) throw failure();
        return { admission, disposition: "handled" };
      }
      const queuedMessages = backend.host === "pi" ? backend.session.pendingMessageCount : backend.session.queuedMessageCount;
      if (((!run.terminal || run.outcome === undefined) && !run.cancelled) || queuedMessages > 0) {
        retired = true;
        journal.append({ type: "native_lifecycle", kind: "session_retired", binding, runId: run.id, at: Date.now(), reason: "SDK terminal proof unavailable" });
        throw failure();
      }
      const outcome = run.cancelled ? "aborted" : run.outcome;
      if (outcome === undefined) throw failure();
      const final = { binding, runId: run.id, outcome };
      journal.append({ type: "native_lifecycle", kind: "run_settled", binding, runId: run.id, at: Date.now(), outcome });
      active = undefined;
      await run.publication;
      if (run.publicationFailed) throw failure();
      return { admission, disposition: "settled", final };
    } finally { run.accepting = false; active = undefined; }
  };
  const prompt = (input: NativePromptInput, onAdmission?: (admission: NativePromptAdmission) => void | Promise<void>, canAdmit?: () => boolean) => Effect.gen(function*(): Effect.fn.Return<NativePromptResult, NativeControlError> {
    yield* bound(input.binding);
    if (canAdmit && !canAdmit()) return yield* new NativeControlError({ code: "invalid_input", message: "Native admission is fenced" });
    if (submissions.has(input.submissionId)) return yield* new NativeControlError({ code: "invalid_input", message: "Submission identity was already used; delivery is never replayed" });
    submissions.add(input.submissionId);
    const run = active;
    if (input.streamingBehavior !== undefined && run?.accepting && session.isStreaming) {
      const submission: Submission = { admission: undefined, publication: Promise.resolve(), publicationFailed: false, onAdmission };
      const enqueue = (async () => {
        const images = input.images === undefined ? {} : { images: Array.from(input.images) };
        if (backend.host === "pi") await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, source: "rpc", streamingBehavior: input.streamingBehavior,
          preflightResult: (disposition) => admit(run, input, disposition, submission) });
        else {
          const invoked = await backend.session.prompt(input.message, { ...images, expandPromptTemplates: false, throwOnDrop: true, streamingBehavior: input.streamingBehavior,
            onPromptAdmitted: () => admit(run, input, "queued", submission) });
          if (!invoked) admit(run, input, "handled", submission);
        }
      })();
      // Queue insertion belongs to this logical run, including admission that races its last turn.
      run.enqueues.push(enqueue.then(() => undefined, () => undefined));
      yield* operation(() => enqueue);
      const admission = submission.admission;
      if (admission === undefined) return yield* failure();
      yield* operation(() => submission.publication);
      if (submission.publicationFailed) return yield* failure();
      if (admission.disposition === "handled") return { admission, disposition: "handled" };
      const result = yield* operation(() => run.result);
      if (result.disposition !== "settled") return yield* failure();
      return { admission, disposition: "settled", final: result.final };
    }
    // ponytail: one FIFO per owned session. Concurrent independent submissions never share an outcome.
    const epoch = cancellationEpoch;
    pending++;
    const result: Promise<NativePromptResult> = tail.then(() => execute(input, epoch, result, onAdmission, canAdmit)).finally(() => { pending--; });
    // A failed ticket must not poison later independent requests. The caller receives its failure.
    tail = result.then(() => undefined, () => undefined);
    return yield* operation(() => result);
  });
  const queue = (input: Omit<NativePromptInput, "submissionId" | "streamingBehavior">, behavior: "steer" | "followUp") => Effect.gen(function*() {
    yield* bound(input.binding);
    const run = active;
    if (!run?.accepting || !session.isStreaming) return yield* new NativeControlError({ code: "invalid_input", message: "Queue controls require an active owned SDK turn; use native.prompt for independent input" });
    const images = input.images === undefined ? undefined : Array.from(input.images);
    const enqueue = behavior === "steer" ? session.steer(input.message, images) : session.followUp(input.message, images);
    run.enqueues.push(enqueue.then(() => undefined, () => undefined));
    return yield* operation<void | "queued" | "handled">(() => enqueue);
  });
  const abort = () => operation(async () => {
    const run = active;
    cancellationEpoch++;
    if (run !== undefined) run.cancelled = true;
    if (backend.host === "omp") {
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
    if (backend.host === "omp") backend.session.beginDispose();
    if (active !== undefined) active.cancelled = true;
    if (backend.host === "omp") {
      for (const job of backend.session.getAsyncJobSnapshot()?.running ?? []) backend.session.cancelAsyncJob(job.id);
      backend.session.clearQueue({ forInterrupt: true });
    }
    else backend.session.clearQueue();
    session.abortBash();
    await session.abort();
    await tail;
    unsubscribe();
    journal.append({ type: "native_lifecycle", kind: "session_retired", binding, runId: null, at: Date.now(), reason: "owned SDK session disposed" });
  }).pipe(Effect.orDie));
  const retireIfIdle = () => {
    if (runningOperations > 0 || pending > 0 || active !== undefined || session.isStreaming || session.isCompacting ||
      (backend.host === "pi" ? backend.session.pendingMessageCount > 0 :
        backend.session.queuedMessageCount > 0 || backend.session.hasPendingAsyncWork())) return false;
    retired = true;
    return true;
  };
  return { session, manager, backend, binding, journal, bound, prompt, queue, abort, retireIfIdle,
    get currentRunId() { return active?.admission?.runId ?? null; }, get pendingSubmissions() { return pending; },
    operation: <A>(run: () => Promise<A>) => operation(async () => {
      if (retired) throw new NativeControlError({ code: "binding_mismatch", message: "Owned SDK session retired" });
      runningOperations++;
      try { return await run(); }
      finally { runningOperations--; }
    }) };
});
