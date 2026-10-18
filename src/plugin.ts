import { ByteSize, Cause, Clock, Config, ConfigProvider, Context as EffectContext, Crypto, Deferred, Duration, Effect, Exit, Fiber, Formatter, Graph, Layer, ManagedRuntime, Option, PlatformError, Record as EffectRecord, Schedule, Schema, SchemaGetter, Scope, Semaphore, Stdio as EffectStdio, Stream } from "effect";
import { Base64, Hex } from "effect/encoding";
import { NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { Commands, CommandError, ConnectHosts, DispositionEnum, OfferDecisionEnum, RunnerCommand, RunnerControlEnum, HttpError, PluginConfig, SessionKey, Json, WorkerId, ArchiveEntry, ArchivePayload, ArchiveParticipants, Inbox, type JsonValue } from "./contracts.ts";
import type { CommandDescriptor, CommandCapabilities, CommandInput, CommandOutput, Disposition, Repo } from "./contracts.ts";
import { SessionId, Evidence, RunnerTask, Archive, Peer, type PeerId } from "./generated/database/types.ts";
import { TaskStateEnum } from "./generated/database/enums.ts";
import { AgentError, AgentEventSequence, AgentConfig, AgentSubmissionId, AgentHostEnum, AgentInputKindEnum, AgentRunOutcomeEnum, AgentAdmissionDispositionEnum, AgentLifecycleEvent, agentEventsUri } from "./agent/protocol.ts";
import type { AgentInputKind } from "./agent/protocol.ts";
import { makeAgentSessionServer } from "./agent-session.ts";
import type { AgentSession } from "./agent-session.ts";
import { LIMIT, Mcp } from "./mcp.ts";
import { Platform, redactedCause } from "./platform.ts";

export class PluginDeclined extends Schema.TaggedError<PluginDeclined>()("PluginDeclined", { message: Schema.String }) {}
// Pi and omp expose this minimal structural subset; no runtime host dependency.
export type Context = {
  readonly cwd: string;
  readonly hasUI: boolean;
  readonly model?: { readonly provider: string; readonly id: string } | undefined;
  readonly mode: "tui" | "rpc" | "json" | "print";
  readonly agent?: { readonly kind: "main" | "sub" } | undefined;
  readonly sessionManager: { readonly getSessionId: () => string };
  readonly ui: {
    readonly notify: (text: string, level?: "info" | "warning" | "error") => void;
    readonly confirm: (title: string, text: string) => Promise<boolean>;
    readonly setStatus?: ((key: string, text: string | undefined) => void) | undefined;
    readonly onTerminalInput?: ((handler: (data: string) => { consume?: boolean; data?: string } | undefined) => () => void) | undefined;
  };
};
export type AgentEventName = "session_start" | "session_shutdown" | "session_switch";
export type Host = {
  readonly on: (event: AgentEventName, handler: (event: unknown, ctx: Context) => Promise<void>) => unknown;
  readonly registerCommand: (name: string, options: { description: string; handler: (args: string, ctx: Context) => Promise<void> }) => void;
};
export const JournalHandledKey = Schema.String.pipe(Schema.brand("JournalHandledKey"));
export type JournalHandledKey = typeof JournalHandledKey.Type;
export const JournalReleaseKey = Schema.String.pipe(Schema.brand("JournalReleaseKey"));
export type JournalReleaseKey = typeof JournalReleaseKey.Type;
export enum JournalReleaseStateEnum {
  intent = "intent",
  sent = "sent",
}
export const JournalReleaseState = Schema.Enum(JournalReleaseStateEnum);
export type JournalReleaseState = typeof JournalReleaseState.Type;
export const JournalState = Schema.Struct({
  handled: Schema.Record(JournalHandledKey, Schema.mutableKey(Schema.Literal(true))),
  releases: Schema.Record(JournalReleaseKey, Schema.mutableKey(JournalReleaseState)),
  consent: Schema.mutableKey(Schema.Boolean), participants: Schema.mutableKey(ArchiveParticipants), seen: Schema.mutableKey(Schema.Record(ArchiveEntry.fields.digest, Schema.mutableKey(ArchiveEntry.fields.digest))),
  fileDigest: Schema.mutableKey(Schema.optional(ArchiveEntry.fields.digest)), spool: Schema.mutableKey(Schema.mutable(Schema.Array(ArchiveEntry))), gaps: Schema.mutableKey(ArchiveEntry.fields.gaps),
  inbox: Schema.mutableKey(Inbox.pipe(Schema.withDecodingDefaultKey(Effect.sync(() => ({}))))),
});
export type JournalState = typeof JournalState.Type;
// One-way host journal boundary: unknown fields and message content never survive.
export const HostJournalEntry = Schema.Struct({
  id: Archive.fields.entry,
  parentId: Schema.optional(ArchivePayload.members[0].fields.parentId).pipe(Schema.catchDecoding(() => Effect.succeed(Option.some(null)))),
  type: Schema.optional(ArchivePayload.members[0].fields.type).pipe(Schema.catchDecoding(() => Effect.succeed(Option.some(undefined)))),
  message: Schema.optional(Schema.Struct({
    role: ArchivePayload.members[0].fields.role.pipe(Schema.catchDecoding(() => Effect.succeed(Option.some(undefined)))),
  })).pipe(Schema.catchDecoding(() => Effect.succeed(Option.some(undefined)))),
}).pipe(Schema.decodeTo(ArchivePayload.members[0], {
  decode: SchemaGetter.transform(entry => ({
    id: entry.id, parentId: entry.parentId ?? null, type: entry.type ?? "omitted",
    ...(entry.message?.role === undefined ? {} : { role: entry.message.role }), contentOmitted: true,
  })),
  encode: SchemaGetter.forbiddenEncoding,
}));
export type HostJournalEntry = typeof HostJournalEntry.Type;
export const ObservedFile = Schema.Struct({ path: Schema.String, size: Schema.Finite, mtimeMs: Schema.Finite });
export type ObservedFile = typeof ObservedFile.Type;

export interface Journal {
  readonly path: string;
  readonly state: JournalState;
  save: () => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  handled: (id: JournalHandledKey) => Effect.Effect<boolean>;
  handle: (id: JournalHandledKey) => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  clearInbox: () => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  receive: (messageId: PeerId, payload: JsonValue) => Effect.Effect<boolean, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  releaseIntent: (key: JournalReleaseKey) => Effect.Effect<boolean, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  releaseSent: (key: JournalReleaseKey) => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  consent: (participants: Array<typeof Peer.fields.sender.Type>) => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  erase: () => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  scan: (file: string | undefined) => Effect.Effect<void, CommandError | PlatformError.PlatformError | Schema.SchemaError>;
  upload: <A, E, R>(send: (entry: ArchiveEntry) => Effect.Effect<A, E, R>, maximum?: number) => Effect.Effect<void, E | CommandError | PlatformError.PlatformError | Schema.SchemaError, R>;
}

const parseHostEntry = Schema.decodeEffect(Schema.fromJsonString(HostJournalEntry));

export const makeJournal = Effect.fn("Journal.make")(function*(directory: string, sessionId: SessionId) {
  const platform = yield* Platform;
  const fs = platform.fs;
  const path = platform.path.join(directory, `${yield* platform.digest(sessionId)}.json`);
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  if (((yield* fs.stat(directory)).mode & 0o077) !== 0) return yield* Effect.fail(new CommandError({ message: "Private journal directory required" }));
  const exists = yield* fs.exists(path);
  if (exists && ((yield* fs.stat(path)).mode & 0o077) !== 0) return yield* Effect.fail(new CommandError({ message: "Private journal file required" }));
  const state: JournalState = exists ? yield* Schema.decodeEffect(Schema.fromJsonString(JournalState), { onExcessProperty: "error" })(yield* fs.readFileString(path)) : {
    handled: {}, releases: {}, consent: false, participants: [], seen: {}, spool: [], gaps: [], inbox: {},
  };
  const semaphore = yield* Semaphore.make(1);
  let observedFile: ObservedFile | undefined;
  let writeFailed = false;
  const syncDirectory = Effect.scoped(Effect.gen(function*() {
    const file = yield* fs.open(directory, { flag: "r" });
    yield* file.sync;
  }));
  const persist = Effect.fn("Journal.persist")(function*() {
    const text = yield* Schema.encodeEffect(Schema.fromJsonString(JournalState), { onExcessProperty: "error" })(state);
    const temporary = `${path}.tmp`;
    yield* Effect.scoped(Effect.gen(function*() {
      const file = yield* fs.open(temporary, { flag: "w", mode: 0o600 });
      yield* fs.chmod(temporary, 0o600);
      yield* file.writeAll(new TextEncoder().encode(text));
      yield* file.sync;
    }));
    yield* fs.rename(temporary, path);
    yield* syncDirectory;
  }, Effect.onError(() => Effect.sync(() => { writeFailed = true; })));
  const serial = <A, E, R>(operation: Effect.Effect<A, E, R>) => semaphore.withPermits(1)(Effect.suspend<A, E | CommandError, R>(() =>
    writeFailed ? Effect.fail(new CommandError({ message: "Journal durability unavailable; reopen required" })) : operation
  ).pipe(Effect.uninterruptible));
  const handled = Effect.fn("Journal.handled")(function*(id: JournalHandledKey) { return state.handled[id] === true; });
  const save = Effect.fn("Journal.save")(() => serial(persist()));
  const handle = Effect.fn("Journal.handle")((id: JournalHandledKey) => serial(Effect.gen(function*() { state.handled[id] = true; yield* persist(); })));
  const clearInbox = Effect.fn("Journal.clearInbox")(() => serial(Effect.gen(function*() { state.inbox = {}; yield* persist(); })));
  const receive = Effect.fn("Journal.receive")((messageId: PeerId, payload: JsonValue) => serial(Effect.gen(function*() {
    const hash = yield* platform.fingerprint(payload);
    const prior = state.inbox[messageId];
    if (prior) {
      if (prior.digest !== hash) return yield* Effect.fail(new CommandError({ message: "Received message ID payload conflict" }));
      return false;
    }
    const handledKey = JournalHandledKey.make(`peer:${messageId}`);
    if (state.handled[handledKey]) return false;
    const entry = { payload, digest: hash, receivedAt: yield* Clock.currentTimeMillis };
    if (new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(JournalState.fields.inbox), { onExcessProperty: "error" })({ ...state.inbox, [messageId]: entry })).length > 128 * 1024) return yield* Effect.fail(new CommandError({ message: "Local inbox full; handling acknowledgement withheld" }));
    state.inbox[messageId] = entry;
    state.handled[handledKey] = true;
    yield* persist();
    return true;
  })));
  const releaseIntent = Effect.fn("Journal.releaseIntent")((key: JournalReleaseKey) => serial(Effect.gen(function*() {
    if (state.releases[key]) return false;
    const intent = `${path}.${yield* platform.digest(key)}.intent`;
    const created = yield* Effect.scoped(Effect.gen(function*() {
      const file = yield* fs.open(intent, { flag: "wx", mode: 0o600 });
      yield* file.writeAll(new TextEncoder().encode(JournalReleaseStateEnum.intent));
      yield* file.sync;
      yield* syncDirectory;
      return true;
    })).pipe(Effect.catch((error) => error.reason._tag === "AlreadyExists" ? Effect.succeed(false) : Effect.fail(error)), Effect.onError(() => Effect.sync(() => { writeFailed = true; })));
    state.releases[key] = JournalReleaseStateEnum.intent;
    yield* persist();
    return created;
  })));
  const releaseSent = Effect.fn("Journal.releaseSent")((key: JournalReleaseKey) => serial(Effect.gen(function*() { state.releases[key] = JournalReleaseStateEnum.sent; yield* persist(); })));
  const consent = Effect.fn("Journal.consent")((participants: Array<typeof Peer.fields.sender.Type>) => serial(Effect.gen(function*() { state.consent = true; state.participants = participants; yield* persist(); })));
  const erase = Effect.fn("Journal.erase")(() => serial(Effect.gen(function*() {
    state.consent = false; state.participants = []; state.spool = []; state.seen = {}; state.gaps = []; delete state.fileDigest; observedFile = undefined;
    // Delivery dedup/intent records survive opt-out: never admit a duplicate prompt.
    yield* persist();
  })));
  const coverage = Effect.fn("Journal.coverage")(function*() {
    if (!state.gaps.length) return;
    const payload: ArchivePayload = { coverageOnly: true, gaps: [...state.gaps] };
    const hash = yield* platform.fingerprint(payload);
    if (state.fileDigest !== hash) { state.spool.push({ entryId: Archive.fields.entry.make(`coverage:${hash}`), digest: hash, payload, gaps: [...state.gaps] }); state.fileDigest = hash; }
  });
  const scan = Effect.fn("Journal.scan")((file: string | undefined) => serial(Effect.gen(function*() {
    if (!state.consent) return;
    const gap = (name: string) => { if (!state.gaps.includes(name)) state.gaps.push(name); };
    gap("content-omitted");
    if (!file) { gap("ephemeral"); yield* coverage(); yield* persist(); return; }
    if (!(yield* fs.exists(file))) { gap("missing"); yield* coverage(); yield* persist(); return; }
    // ponytail: scans cap at 8 MiB; oversized native journals report coverage loss.
    const metadata = yield* fs.stat(file);
    const size = Number(ByteSize.toBigInt(metadata.size));
    const mtimeMs = Option.getOrUndefined(metadata.mtime)?.getTime();
    if (size > 8 * 1024 * 1024) { gap("overflow"); yield* coverage(); yield* persist(); return; }
    if (mtimeMs !== undefined && observedFile?.path === file && observedFile.size === size && observedFile.mtimeMs === mtimeMs) return;
    const text = yield* fs.readFileString(file);
    const lines = text.slice(0, text.lastIndexOf("\n") + 1).split("\n").filter(Boolean);
    const present = new Set<typeof RunnerTask.fields.fingerprint.Type>();
    const graph = Graph.beginMutation(Graph.directed<{ entry: HostJournalEntry; digest: typeof RunnerTask.fields.fingerprint.Type }, undefined>());
    const nodes = new Map<typeof Archive.fields.entry.Type, Graph.NodeIndex>();
    for (const line of lines) {
      if (new TextEncoder().encode(line).length > LIMIT) { gap("overflow"); continue; }
      const decoded = yield* parseHostEntry(line).pipe(Effect.option);
      if (Option.isNone(decoded)) { gap("invalid-line"); continue; }
      nodes.set(decoded.value.id, Graph.addNode(graph, { entry: decoded.value, digest: yield* platform.digest(line) }));
    }
    for (const [index, { entry }] of graph) {
      const parent = entry.parentId === null ? undefined : nodes.get(entry.parentId);
      if (parent !== undefined) Graph.addEdge(graph, parent, index, undefined);
    }
    for (const [index, { entry: safe, digest: originalDigest }] of graph) {
      const entryKey = yield* platform.digest(safe.id);
      present.add(entryKey);
      if (state.seen[entryKey] === originalDigest) continue;
      if (state.seen[entryKey]) gap("rewrite");
      if (Graph.inDegree(graph, index) === 0) gap("unlinked");
      const item = { entryId: safe.id, digest: yield* platform.fingerprint(safe), payload: safe, gaps: [...state.gaps] };
      if (new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(JournalState.fields.spool), { onExcessProperty: "error" })([...state.spool, item])).length > 510 * 1024) { gap("overflow"); break; }
      state.seen[entryKey] = originalDigest; state.spool.push(item);
    }
    if (EffectRecord.keys(state.seen).some((id) => !present.has(id))) gap("rewrite");
    yield* coverage(); yield* persist();
    observedFile = mtimeMs === undefined ? undefined : { path: file, size, mtimeMs };
  })));
  const upload = Effect.fn("Journal.upload")(<A, E, R>(send: (entry: ArchiveEntry) => Effect.Effect<A, E, R>, maximum = 8) => serial(Effect.gen(function*() {
    for (let count = 0; count < maximum && state.consent && state.spool.length; count++) { yield* send(state.spool[0]!); state.spool.shift(); yield* persist(); }
  })));
  return { path, state, save, handled, handle, clearInbox, receive, releaseIntent, releaseSent, consent, erase, scan, upload };
});

const prefix = "\x1b]7501;";
const reply = /\x1b\]7501;\?[^\x00-\x1f\x7f]*(?:\x07|\x1b\\)/g;
export const ProgramState = Object.freeze({ idle: "idle", working: "working", done: "done", error: "error" });
export type ProgramState = typeof ProgramState[keyof typeof ProgramState];
export const ProgramNetworkState = Object.freeze({ retrying: "retrying", paused: "paused" });
export type ProgramNetworkState = typeof ProgramNetworkState[keyof typeof ProgramNetworkState];
const inputStatus: Record<AgentInputKind, { kind: string; message: string }> = {
  [AgentInputKindEnum.approval]: { kind: "permission", message: "Waiting for permission" },
  [AgentInputKindEnum.question]: { kind: "question", message: "Waiting for an answer" },
};
const networkMessages: Record<ProgramNetworkState, string> = {
  [ProgramNetworkState.retrying]: "Coordinator reconnecting",
  [ProgramNetworkState.paused]: "Coordinator network paused",
};
type State = ProgramState | undefined;
type NetworkState = ProgramNetworkState | undefined;

export interface ProgramStatus {
  report: (state: State, kind?: AgentInputKind) => Effect.Effect<void, CommandError>;
  clear: () => Effect.Effect<void, CommandError>;
  network: (state: NetworkState) => Effect.Effect<void, CommandError>;
}

export const makeProgramStatus = (ctx: Context): Effect.Effect<ProgramStatus, never, EffectStdio.Stdio | Scope.Scope> => Effect.gen(function*() {
  const stdio = yield* EffectStdio.Stdio;
  let supported = false;
  let retired = false;
  let last: string | undefined;
  let taskState: State;
  let taskKind: AgentInputKind | undefined;
  let networkState: NetworkState;
  let flushing = false;
  const write = (fields: string) => Stream.make(`${prefix}id=coordinator:app=coordinator:${fields}\x1b\\`).pipe(
    Stream.run(stdio.stdout({ endOnDone: false })),
    Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator terminal status unavailable" }))),
  );
  const fields = () => {
    if (taskState === undefined && networkState === undefined) return undefined;
    const input = taskKind === undefined ? undefined : inputStatus[taskKind];
    const state = input ? `state=blocked:kind=${input.kind}` : `state=${taskState ?? (networkState === ProgramNetworkState.paused ? ProgramState.error : ProgramState.working)}`;
    const message = [input?.message, networkState ? networkMessages[networkState] : undefined].filter((value) => value !== undefined).join("; ");
    return message ? `${state}:msg=${Base64.encode(message)}` : state;
  };
  // One owner composes task and connectivity updates, including changes during a write.
  const flush = () => Effect.gen(function*() {
    if (retired || !supported || flushing) return;
    flushing = true;
    yield* Effect.gen(function*() {
      while (!retired) {
        const next = fields();
        if (next === last) return;
        yield* write(next ?? "state=clear");
        last = next;
      }
    }).pipe(Effect.ensuring(Effect.sync(() => { flushing = false; })));
  });
  const network = (state: NetworkState) => Effect.gen(function*() {
    if (retired || networkState === state) return;
    networkState = state;
    if (ctx.hasUI) {
      ctx.ui.setStatus?.("coordinator.network", state === ProgramNetworkState.retrying ? networkMessages.retrying : undefined);
      ctx.ui.setStatus?.("coordinator.paused", state === ProgramNetworkState.paused ? networkMessages.paused : undefined);
    }
    yield* flush();
  });
  const clear = () => Effect.gen(function*() {
    if (retired) return;
    taskState = undefined;
    taskKind = undefined;
    yield* network(undefined);
    yield* flush();
  });
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    const transientError = taskState === ProgramState.error && networkState !== undefined;
    // Connectivity is transient; completed task results still survive process exit.
    yield* network(undefined).pipe(Effect.catchCause(() => Effect.void));
    if (transientError || taskKind !== undefined || (taskState !== ProgramState.done && taskState !== ProgramState.error)) {
      yield* clear().pipe(Effect.catchCause(() => Effect.void));
    }
    retired = true;
  }));
  const eligible = ctx.hasUI && ctx.mode === "tui" && ctx.agent?.kind !== "sub" && ctx.ui.onTerminalInput !== undefined && (yield* stdio.stdoutIsTerminal);
  if (eligible) {
    const received = yield* Deferred.make<void>();
    let observed = "";
    let listening = true;
    const unsubscribe = yield* Effect.acquireRelease(Effect.sync(() => {
      const stop = ctx.ui.onTerminalInput!((data) => {
      if (!listening || retired) return;
      // Native hosts normally frame complete OSCs. Pi can flush a torn OSC and
      // offers no reinjection API: observe fragments, never hold/drop their bytes.
      const combined = observed + data;
      reply.lastIndex = 0;
      for (const match of combined.matchAll(reply)) {
        if (Buffer.byteLength(match[0], "utf8") <= 4096) Deferred.doneUnsafe(received, Effect.void);
      }
      const start = combined.lastIndexOf("\x1b");
      const candidate = start < 0 ? "" : combined.slice(start);
      // Retain only a possible query reply, including a split introducer/ST.
      if (candidate === "\x1b" && observed.startsWith(`${prefix}?`) && combined.endsWith("\x1b")) {
        observed = combined.length <= 4096 ? combined : "";
      } else observed = (prefix.startsWith(candidate) || candidate.startsWith(`${prefix}?`)) && candidate.length <= 4096 ? candidate : "";
      const filtered = data.replace(reply, (sequence) => Buffer.byteLength(sequence, "utf8") <= 4096 ? "" : sequence);
      return filtered === data ? undefined : filtered.length === 0 ? { consume: true } : { data: filtered };
      });
      return () => {
        if (!listening) return;
        listening = false;
        observed = "";
        stop();
      };
    }), (unsubscribe) => Effect.sync(unsubscribe));
    yield* Effect.gen(function*() {
      const queried = yield* Stream.make(`${prefix}?\x1b\\`).pipe(
        Stream.run(stdio.stdout({ endOnDone: false })),
        Effect.map(() => true), Effect.catchCause(() => Effect.succeed(false)),
      );
      const answer = queried ? yield* Deferred.await(received).pipe(Effect.timeoutOption("100 millis")) : Option.none();
      supported = Option.isSome(answer);
      yield* flush().pipe(Effect.catchCause(() => Effect.void));
    }).pipe(
      Effect.ensuring(Effect.sync(unsubscribe)),
      Effect.forkScoped,
    );
  }
  return {
    clear,
    network,
    report: (state: State, kind?: AgentInputKind) => Effect.gen(function*() {
      if (retired) return;
      taskState = state;
      taskKind = state === undefined ? undefined : kind;
      yield* flush();
    }),
  };
});

const pluginCommands = Object.values(Commands).filter((entry) => entry.target === "plugin");
const optoutJson = Schema.fromJsonString(Schema.NullOr(SessionKey));
const makePlugin = Effect.fn("Plugin.make")(function*() {
  const platform = yield* Platform;
  const configuredPath = yield* Config.option(Config.String("COORDINATOR_CONFIG"));
  let path: string;
  if (Option.isSome(configuredPath)) path = configuredPath.value;
  else {
    const configuredDirectory = yield* Config.option(Config.String("XDG_CONFIG_HOME"));
    const directory = Option.isSome(configuredDirectory) ? configuredDirectory.value : platform.path.join(yield* Config.String("HOME"), ".config");
    path = platform.path.join(directory, "coordinator", "plugin.json");
  }
  if (!platform.path.isAbsolute(path)) return yield* Effect.fail(new CommandError({ message: "Coordinator configuration path must be absolute" }));
  if (Option.isNone(configuredPath) && !(yield* platform.fs.exists(path))) return undefined;
  const config = yield* Schema.decodeEffect(Schema.fromJsonString(PluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path));
  if (!platform.path.isAbsolute(config.stateDirectory) || !platform.path.isAbsolute(config.tokenFile)) return yield* Effect.fail(new CommandError({ message: "Invalid coordinator configuration" }));
  // ponytail: one preference per state directory; other live hosts must opt out separately.
  const offFile = platform.path.join(config.stateDirectory, "off.json");
  let disabled = yield* platform.fs.exists(offFile);
  let pendingOptout = disabled ? yield* Schema.decodeEffect(optoutJson)(yield* platform.privateFile(offFile)) : null;
  const token = yield* platform.privateFile(config.tokenFile);
  const connectHosts = config.connectHosts === undefined
    ? yield* Config.String("COORDINATOR_CONNECT_HOSTS_JSON").pipe(Config.withDefault("{}"))
    : yield* Schema.encodeEffect(Schema.fromJsonString(ConnectHosts))(config.connectHosts);
  const mcp = EffectContext.get(yield* Layer.build(Mcp.layer.pipe(Layer.provide(ConfigProvider.layer(
    ConfigProvider.fromUnknown({ COORDINATOR_CONNECT_HOSTS_JSON: connectHosts }),
  )))), Mcp);
  const stdio = yield* EffectStdio.Stdio;
  let base: ProgramState | undefined;
  let session: string | undefined;
  const inputs = new Map<string, AgentInputKind>();
  const report = Effect.fn("Plugin.reportStatus")(function*(ctx: Context, clear = false) {
    const status = active?.status;
    if (!status || active?.ctx !== ctx) return;
    let permission = false;
    for (const input of inputs.values()) if (input === AgentInputKindEnum.approval) { permission = true; break; }
    const kind = permission ? AgentInputKindEnum.approval : inputs.size > 0 ? AgentInputKindEnum.question : undefined;
    yield* (clear ? status.clear() : status.report(base, kind)).pipe(
      Effect.tapError(() => Effect.sync(() => ctx.ui.notify("Coordinator terminal status unavailable; displayed status may be stale.", "warning"))),
    );
  });
  const clearStatus = Effect.fn("Plugin.clearStatus")(function*(status: ProgramStatus) {
    inputs.clear();
    yield* status.clear();
  });
  const agent = Effect.fn("Plugin.ownedStatus")(function*(event: AgentLifecycleEvent, ctx: Context) {
    const current = active;
    if (disabled) return;
    if (!current || agentEventsUri(event.binding) !== agentEventsUri(current.owner.binding)) return;
    if (event.kind !== "session_started" && session !== event.binding.sessionId) { base = undefined; inputs.clear(); }
    else yield* AgentLifecycleEvent.match(event, {
      session_started: (event) => Effect.sync(() => { session = event.binding.sessionId; base = ProgramState.idle; inputs.clear(); }),
      session_retired: () => Effect.gen(function*() {
        base = undefined; inputs.clear();
        yield* controlFailure(current, Cause.fail(new AgentError({ code: "binding_mismatch", message: "Owned agent binding retired" })), false);
      }),
      run_started: () => Effect.sync(() => { base = ProgramState.working; }),
      input_requested: (event) => Effect.sync(() => { inputs.set(event.inputId, event.inputKind); }),
      input_resolved: (event) => Effect.sync(() => { if (!inputs.delete(event.inputId)) base = undefined; }),
      run_settled: (event) => Effect.sync(() => {
        if (inputs.size > 0) base = undefined;
        else switch (event.outcome) {
          case AgentRunOutcomeEnum.completed: base = ProgramState.done; break;
          case AgentRunOutcomeEnum.error: base = ProgramState.error; break;
          case AgentRunOutcomeEnum.aborted: base = ProgramState.idle; break;
        }
      }),
      submission_admitted: () => Effect.void,
      run_phase: () => Effect.void,
    });
    yield* report(ctx);
  });
  const semaphore = yield* Semaphore.make(1);
  const pluginScope = yield* Scope.Scope;
  const recoverySchedule = Schedule.exponential("1 second").pipe(
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.millis(Math.min(30000, Duration.toMillis(duration))))),
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.millis(Math.max(1000, Math.min(30000, Duration.toMillis(duration)))))),
  );
  let active: { ctx: Context; parentSessionId: string; sessionId: SessionId; incarnation: typeof RunnerTask.fields.incarnation.Type; workerId: WorkerId;
    owner: AgentSession; scope: Scope.Closeable; journal: Journal; repo: Repo; heartbeat: number; dispatching: boolean;
    status: ProgramStatus; outage: boolean; paused: boolean; restart: boolean; nextAttempt: number; epoch: number;
    step: (now: number, input: void) => Effect.Effect<[Duration.Duration, Duration.Duration], Cause.Done<Duration.Duration>> } | undefined;
  let sessionScope: Scope.Closeable | undefined;
  let optoutGeneration = 0;
  const serial = <A, E, R>(effect: Effect.Effect<A, E, R>) => semaphore.withPermits(1)(effect);
  type ControlError = AgentError | CommandError | HttpError | Cause.TimeoutError | PlatformError.PlatformError | Schema.SchemaError;
  const controlFailure = (current: NonNullable<typeof active>, cause: Cause.Cause<ControlError>, broker: boolean, presence = false) => Effect.gen(function*() {
    if (active !== current || Cause.hasInterrupts(cause) || current.paused) return;
    const found = Cause.findErrorOption(cause);
    const error = Option.isSome(found) ? found.value : undefined;
    const conflict = presence && error?._tag === "HttpError" && (error.status === 409 || error.status === 410);
    const transient = broker && (conflict || error?._tag === "TimeoutError" ||
      error?._tag === "CommandError" && error.reason === "transport" ||
      error?._tag === "HttpError" && (error.status === 408 || error.status === 429 || error.status >= 500 && error.status !== 501) &&
        !(error.protocolCode !== undefined && error.protocolCode >= -32603 && error.protocolCode <= -32600));
    const epoch = ++current.epoch;
    current.paused = !transient;
    current.restart ||= conflict;
    if (!current.outage) {
      current.outage = true;
      yield* notify(current.ctx, transient ? "Coordinator connection unavailable; reconnecting automatically. Agent work continues." :
        "Coordinator control paused; check authorization, configuration or local journal. Agent work continues.", "warning");
    }
    if (active !== current || current.epoch !== epoch) return;
    yield* current.status.network(transient ? ProgramNetworkState.retrying : ProgramNetworkState.paused).pipe(Effect.catchCause(() => Effect.void));
    if (!transient) return;
    const now = yield* Clock.currentTimeMillis;
    const delay = yield* current.step(now, undefined).pipe(Effect.orDie);
    let lowerBound = 0;
    if (error?._tag === "HttpError" && error.retryAfter !== undefined) {
      const value = error.retryAfter.trim();
      const seconds = /^\d+$/.test(value) ? Number(value) : undefined;
      const date = seconds === undefined ? Date.parse(value) : NaN;
      lowerBound = seconds !== undefined && Number.isFinite(seconds) ? seconds * 1000 : Number.isFinite(date) ? Math.max(0, date - now) : 0;
    }
    if (active === current && current.epoch === epoch) current.nextAttempt = Math.max(current.nextAttempt, now + Math.max(Duration.toMillis(delay[1]), lowerBound));
  });
  const held = <A, E extends ControlError, R>(_operation: string, effect: Effect.Effect<A, E, R>, owner = active) => effect.pipe(
    Effect.tapCause((cause) => owner ? controlFailure(owner, cause, false) : Effect.void),
  );
  const call = <I extends Schema.ConstraintCodec<unknown, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, S extends Schema.Top & Schema.ConstraintCodec<{ readonly result: O["Type"] }, unknown>>(descriptor: CommandDescriptor<I, O> & { readonly structuredOutput: S; readonly target: "broker" }, params: I["Type"], owner = active) => Effect.suspend(() => {
    const current = owner;
    if (disabled || current && (current !== active || current.paused)) return Effect.fail(new CommandError({ message: "Coordinator control paused; use /coordinator on after opt-out cleanup" }));
    return mcp.call(config.brokerUrl, token, descriptor, params).pipe(
      Effect.tapCause((cause) => current ? controlFailure(current, cause, true, descriptor.name === Commands.heartbeatSession.name || descriptor.name === Commands.pollSession.name) : Effect.void),
    );
  });
  const notify = (ctx: Context, text: string, level: "info" | "warning" | "error" = "info") => Effect.sync(() => ctx.ui.notify(text, level));
  const confirm = (ctx: Context, title: string, text: string) => Effect.gen(function*() {
    inputs.set("coordinator-approval", AgentInputKindEnum.approval);
    return yield* Effect.gen(function*() {
      yield* report(ctx);
      return yield* Effect.tryPromise({ try: () => ctx.ui.confirm(title, text), catch: () => new CommandError({ message: "Coordinator approval UI unavailable" }) });
    }).pipe(Effect.ensuring(Effect.gen(function*() {
      inputs.delete("coordinator-approval");
      yield* report(ctx);
    }).pipe(Effect.orDie)));
  });
  const local = Effect.fn("Plugin.local")(function*(ctx: Context) {
    const env = { ...process.env };
    delete env.COORDINATOR_DATABASE_URL;
    delete env.COORDINATOR_RUNNER_DATABASE_URL;
    delete env.COORDINATOR_TEST_POSTGRES_URL;
    return { repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote),
      ref: (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "--symbolic-full-name", "HEAD"], { cwd: ctx.cwd, env, extendEnv: false }))).trim(),
      sha: Evidence.fields.sha.make((yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: ctx.cwd, env, extendEnv: false }))).trim()) };
  });
  const tick = Effect.fn("Plugin.tick")(function*() {
    const current = active;
    if (disabled || !current || current.paused || current.restart) return;
    if ((yield* Clock.currentTimeMillis) < current.nextAttempt) return;
    const { ctx, sessionId, incarnation, workerId, journal } = current;
    if (ctx.sessionManager.getSessionId() !== current.parentSessionId) return;
    const epoch = current.epoch;
    let heartbeated = false;
    current.repo = yield* held("repository inspection", platform.inspectedRepo(ctx.cwd, config.selectedRemote));
    if (current.outage || (yield* Clock.currentTimeMillis) - current.heartbeat >= 10000) {
      yield* call(Commands.heartbeatSession, { sessionId, incarnation, machineId: config.machineId, host: config.host, repo: current.repo, workerId });
      current.heartbeat = (yield* Clock.currentTimeMillis);
      heartbeated = true;
    }
    const poll = yield* call(Commands.pollSession, { sessionId, incarnation });
    if (active !== current || current.paused || current.restart || current.epoch !== epoch) return;
    if (current.outage && heartbeated) {
      current.outage = false;
      current.nextAttempt = 0;
      current.step = yield* Schedule.toStep(recoverySchedule);
      yield* current.status.network(undefined).pipe(Effect.catchCause(() => Effect.void));
      if (active !== current || current.paused || current.restart || current.epoch !== epoch) return;
      yield* notify(ctx, "Coordinator connection recovered.");
    }
    if (poll.archiveEnabled === false && journal.state.consent) yield* held("journal write (erase)", journal.erase());
    for (const message of poll.messages) {
      if (message.incarnation !== incarnation || message.expiresAt <= (yield* Clock.currentTimeMillis)) continue;
      const newlyHandled = yield* held("journal write (receive)", journal.receive(message.messageId, message.payload));
      if (newlyHandled) yield* notify(ctx, `Coordinator message ${message.messageId}; /coordinator inbox shows the durably received content.`);
      yield* call(Commands.acknowledgePeer, { sessionId, incarnation, messageId: message.messageId });
    }
    for (const offer of poll.offers) {
      const handledKey = JournalHandledKey.make(`offer:${offer.taskId}`);
      if (yield* held("journal read", journal.handled(handledKey))) continue;
      yield* notify(ctx, `Coordinator offer ${offer.taskId}: ${offer.request.repo} ${offer.request.ref} ${offer.request.sha}. /coordinator status {"taskId":"${offer.taskId}"}; then accept or decline explicitly.`);
      yield* held("journal write (offer)", journal.handle(handledKey));
    }
    for (const task of poll.releases) {
      const address = task.address;
      if (!address || task.confirmedRevision !== address.revision || task.disposition !== DispositionEnum.ready || !task.readback || (yield* platform.fingerprint(task.readback)) !== (yield* platform.fingerprint(address)) || address.sessionId !== sessionId || address.incarnation !== incarnation) continue;
      const approvedAddress = task.instruction?.address;
      if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== task.request.repo || (approvedAddress ? (yield* platform.fingerprint(address)) !== (yield* platform.fingerprint(approvedAddress)) : address.ref !== task.request.ref || address.sha !== task.request.sha) || address.workerId !== workerId) continue;
      if (current.dispatching || current.owner.isBusy) continue;
      const actual = yield* held("repository inspection", local(ctx));
      if (actual.repo !== address.repo || actual.ref !== address.ref || actual.sha !== address.sha) {
        yield* call(Commands.readbackTask, { taskId: task.taskId, address, disposition: DispositionEnum.hold, reason: "Local repository/ref/SHA changed", nextCheck: (yield* Clock.currentTimeMillis) + 60000 });
        continue;
      }
      const key = JournalReleaseKey.make(`${task.taskId}:${address.revision}`);
      if (journal.state.releases[key]) {
        if (journal.state.releases[key] === JournalReleaseStateEnum.intent) {
          yield* call(Commands.reportTask, { taskId: task.taskId, sessionId, incarnation, state: TaskStateEnum.HELD, result: { reason: "Release delivery uncertain; explicit new addressed revision required" } });
        }
        continue;
      }
      if (!(yield* held("journal write (release intent)", journal.releaseIntent(key)))) continue;
      if (active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch) return;
      const submissionId = AgentSubmissionId.make(yield* platform.crypto.randomUUIDv4());
      if (active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch) return;
      const publishingGeneration = optoutGeneration;
      current.dispatching = true;
      let admissionFenced = false;
      // Durable intent precedes SDK admission. No response/connection failure can replay this revision.
      yield* Effect.suspend(() => active !== current || current.outage || current.paused || current.restart || current.epoch !== epoch
        ? Effect.void
        : current.owner.prompt({ binding: current.owner.binding, submissionId, message: task.instruction?.prompt ?? task.request.prompt },
        (admission) => Effect.runPromise(Effect.gen(function*() {
          yield* held("journal write (release sent)", journal.releaseSent(key), current);
          if (publishingGeneration !== optoutGeneration) return;
          if (admission.disposition !== AgentAdmissionDispositionEnum.handled) yield* call(Commands.reportTask, { taskId: task.taskId, sessionId, incarnation, state: TaskStateEnum.RUNNING,
            result: { revision: address.revision, admission, evidenceVerified: false } }, current);
        })), () => {
          const allowed = active === current && !current.outage && !current.paused && !current.restart && current.epoch === epoch;
          admissionFenced ||= !allowed;
          return allowed;
        }).pipe(
        Effect.flatMap((result) => publishingGeneration !== optoutGeneration ? Effect.void : call(Commands.reportTask, { taskId: task.taskId, sessionId, incarnation,
          state: result.disposition === "handled" || result.final.outcome === AgentRunOutcomeEnum.aborted ? TaskStateEnum.HELD : result.final.outcome === AgentRunOutcomeEnum.error ? TaskStateEnum.FAILED : TaskStateEnum.SUCCEEDED,
          result: { revision: address.revision, admission: result.admission, ...(result.disposition === "settled" ? { final: result.final } : {}),
            evidenceVerified: false, reason: "SDK execution outcome; deliverable evidence remains separate" } }, current)))).pipe(
        Effect.catchCause((cause) => Effect.gen(function*() {
          if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause);
          if (admissionFenced || publishingGeneration !== optoutGeneration) return;
          const failure = Cause.findErrorOption(cause);
          if (Option.isSome(failure) && failure.value._tag === "AgentError" && (failure.value.code === "binding_mismatch" || failure.value.code === "invalid_input")) {
            yield* controlFailure(current, cause, false);
            return;
          }
          if (current.outage || active !== current) return;
          const reason = `SDK delivery or outcome: ${redactedCause(cause)}; explicit new addressed revision required`;
          yield* notify(ctx, `Coordinator dispatch held at ${reason}.`, "warning");
          return yield* call(Commands.reportTask, { taskId: task.taskId, sessionId, incarnation, state: TaskStateEnum.HELD,
            result: { revision: address.revision, reason } }, current);
        })),
        Effect.ensuring(Effect.sync(() => { current.dispatching = false; })),
        Effect.tapCause((cause) => current.outage || Cause.hasInterrupts(cause) || active !== current ? Effect.void :
          notify(ctx, `Coordinator SDK outcome report unavailable: ${redactedCause(cause)}; reconcile this revision explicitly.`, "warning")),
        Effect.forkIn(current.scope),
      );
    }
    yield* held("journal scan", journal.scan(current.owner.manager.getSessionFile()));
    yield* journal.upload((entry) => call(Commands.appendArchive, { sessionId, ...entry }), 1);
  });
  const stop = Effect.fn("Plugin.stop")(function*() {
    const previous = active, scope = sessionScope;
    active = undefined;
    sessionScope = undefined;
    yield* (previous ? previous.status.network(undefined).pipe(Effect.andThen(clearStatus(previous.status))) : Effect.void).pipe(Effect.ensuring(Effect.gen(function*() {
      if (scope) yield* Scope.close(scope, Exit.void);
      if (previous && !disabled) yield* call(Commands.stopSession, { sessionId: previous.sessionId, incarnation: previous.incarnation }).pipe(
        Effect.catchCause(() => notify(previous.ctx, "Coordinator stop unavailable; prior enrollment expires by heartbeat lease.", "warning")));
    }).pipe(Effect.orDie)));
  });
  yield* Effect.addFinalizer(() => serial(stop()).pipe(Effect.orDie));
  const runOwner = (ctx: Context, current: NonNullable<typeof active>, initial: Deferred.Deferred<void>): Effect.Effect<void> => Effect.gen(function*() {
    while (active === current && ctx.sessionManager.getSessionId() === current.parentSessionId) {
      if (current.restart && !current.paused && !current.dispatching && !current.owner.isBusy) {
        yield* Effect.sleep(Math.max(0, current.nextAttempt - (yield* Clock.currentTimeMillis)));
        yield* start(ctx, true, current).pipe(Effect.forkIn(pluginScope));
      }
      yield* tick().pipe(
        Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.interrupt :
          current.outage ? Effect.void : controlFailure(current, cause, false)),
        Effect.ensuring(Deferred.succeed(initial, undefined)),
      );
      yield* Effect.sleep(Math.max(1000, current.nextAttempt - (yield* Clock.currentTimeMillis)));
    }
  });
  const start = Effect.fn("Plugin.start")((ctx: Context, recovering = false, expected = active) => serial(Effect.gen(function*() {
    const initial = yield* Deferred.make<void>();
    if (recovering && active !== expected) {
      yield* Deferred.succeed(initial, undefined);
      return initial;
    }
    if (recovering && (!expected || expected.paused || expected.dispatching || !expected.owner.retireIfIdle())) {
      yield* Deferred.succeed(initial, undefined);
      return initial;
    }
    const outage = recovering && active?.outage === true;
    const recoveryStep = recovering ? active?.step : undefined;
    yield* stop();
    if (disabled) {
      yield* Deferred.succeed(initial, undefined);
      return initial;
    }
    const parentSessionId = ctx.sessionManager.getSessionId();
    const scope = yield* Scope.make();
    sessionScope = scope;
    yield* platform.fs.makeDirectory(config.stateDirectory, { recursive: true, mode: 0o700 });
    const configuredAgent = yield* Config.option(Config.String("COORDINATOR_AGENT_CONFIG"));
    const agentConfig = yield* Effect.gen(function*() {
      if (Option.isSome(configuredAgent)) {
        if (!platform.path.isAbsolute(configuredAgent.value)) return yield* new CommandError({ message: "Absolute agent configuration path required" });
        const value = yield* Schema.decodeEffect(Schema.fromJsonString(AgentConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configuredAgent.value));
        if (value.host !== config.host) return yield* new CommandError({ message: "Agent owner host mismatch" });
        return value;
      }
      const directory = platform.path.join(config.stateDirectory, `agent-${yield* platform.digest(parentSessionId)}`);
      yield* platform.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
      const tokenFile = platform.path.join(directory, "control.token");
      if (!(yield* platform.fs.exists(tokenFile))) yield* platform.fs.writeFileString(tokenFile, Hex.encode(yield* platform.crypto.randomBytes(32)), { flag: "wx", mode: 0o600 });
      return { host: config.host, endpointFile: platform.path.join(directory, "endpoint.json"), tokenFile };
    });
    const owner = yield* makeAgentSessionServer(agentConfig, ctx).pipe(Effect.provideService(Scope.Scope, scope),
      Effect.provideService(Platform, platform), Effect.provideService(Crypto.Crypto, platform.crypto),
      Effect.onExit((exit) => exit._tag === "Failure" ? Scope.close(scope, exit) : Effect.void));
    const sessionId = SessionId.make(owner.binding.sessionId), workerId = WorkerId.make(sessionId);
    const journal = yield* makeJournal(config.stateDirectory, sessionId).pipe(Effect.provideService(Platform, platform));
    const status = yield* makeProgramStatus(ctx).pipe(Effect.provideService(Scope.Scope, scope), Effect.provideService(EffectStdio.Stdio, stdio));
    active = { ctx, parentSessionId, sessionId, workerId, incarnation: RunnerTask.fields.incarnation.make(owner.binding.incarnation), owner, scope,
      repo: yield* platform.inspectedRepo(ctx.cwd, config.selectedRemote), journal, heartbeat: 0, dispatching: false,
      status, outage, paused: false, restart: false, nextAttempt: 0, epoch: 0, step: recoveryStep ?? (yield* Schedule.toStep(recoverySchedule)) };
    let updates = Promise.resolve();
    const unsubscribe = owner.journal.subscribe((event) => {
      updates = updates.then(() => Effect.runPromise(agent(event, ctx).pipe(Effect.catchCause((cause) => Effect.sync(() => {
        base = undefined;
        ctx.ui.notify(`Coordinator owned SDK status unavailable: ${redactedCause(cause)}; displayed status may be stale.`, "warning");
      })))));
    });
    yield* Effect.addFinalizer(() => Effect.promise(async () => { unsubscribe(); await updates; })).pipe(Effect.provideService(Scope.Scope, scope));
    for (const event of owner.journal.read(owner.binding, AgentEventSequence.make(0)).entries) yield* agent(event, ctx);
    const current = active;
    yield* Effect.addFinalizer(() => Deferred.succeed(initial, undefined)).pipe(Effect.provideService(Scope.Scope, scope));
    yield* runOwner(ctx, current, initial).pipe(Effect.forkIn(scope));
    return initial;
  })).pipe(Effect.flatMap(Deferred.await)));
  const syncStateDirectory = Effect.scoped(Effect.gen(function*() {
    const directory = yield* platform.fs.open(config.stateDirectory, { flag: "r" });
    yield* directory.sync;
  }));
  const persistOptout = Effect.fn("Plugin.persistOptout")(function*(key: SessionKey | null) {
    yield* platform.fs.makeDirectory(config.stateDirectory, { recursive: true, mode: 0o700 });
    const temporary = `${offFile}.${yield* platform.crypto.randomUUIDv4()}.pending`;
    yield* Effect.gen(function*() {
      yield* Effect.scoped(Effect.gen(function*() {
        const file = yield* platform.fs.open(temporary, { flag: "wx", mode: 0o600 });
        yield* file.writeAll(new TextEncoder().encode(yield* Schema.encodeEffect(optoutJson)(key)));
        yield* file.sync;
      }));
      yield* platform.fs.rename(temporary, offFile);
      yield* syncStateDirectory;
    }).pipe(Effect.ensuring(platform.fs.remove(temporary, { force: true }).pipe(Effect.orDie)));
  });
  const off = Effect.fn("Plugin.off")(function*(ctx: Context) {
    disabled = true;
    optoutGeneration++;
    let current = active;
    if (current) {
      current.paused = true;
      current.epoch++;
    }
    return yield* serial(Effect.gen(function*() {
      const wasDisabled = yield* platform.fs.exists(offFile);
      disabled = true;
      current = active;
      if (current) {
        current.paused = true;
        current.epoch++;
      }
      if (!wasDisabled) pendingOptout = current ? { sessionId: current.sessionId, incarnation: current.incarnation } : null;
      yield* persistOptout(pendingOptout);
      const key = pendingOptout;
      if (key) {
        const journal = current?.sessionId === key.sessionId ? current.journal :
          yield* makeJournal(config.stateDirectory, key.sessionId).pipe(Effect.provideService(Platform, platform));
        yield* journal.erase();
        // Archive deletion revokes enrollment atomically; stopped control must not prevent explicit cleanup.
        yield* mcp.call(config.brokerUrl, token, Commands.deleteArchive, { sessionId: key.sessionId });
        yield* persistOptout(null);
        pendingOptout = null;
        yield* mcp.call(config.brokerUrl, token, Commands.stopSession, key);
      }
      yield* notify(ctx, "Coordinator off. Accepted agent work continues locally; remote outcomes require explicit reconciliation. /coordinator on resumes without granting archive consent.");
      const result: CommandOutput<typeof Commands.pluginOff> = { enabled: false, sharing: false };
      return result;
    })).pipe(
      Effect.ensuring(current ? current.status.network(undefined).pipe(Effect.andThen(clearStatus(current.status)),
        Effect.catchCause(cause => notify(ctx, `Coordinator terminal status unavailable: ${redactedCause(cause)}; displayed status may be stale.`, "warning"))) : Effect.void),
      Effect.tapCause(() => notify(ctx, "Coordinator is locally off; remote cleanup is unconfirmed. Restore connectivity and retry /coordinator off explicitly.", "warning")),
    );
  });
  const on = Effect.fn("Plugin.on")(function*(ctx: Context) {
    yield* serial(Effect.gen(function*() {
      if (pendingOptout) return yield* new CommandError({ message: "Archive cleanup pending; retry /coordinator off before enabling" });
      if (disabled) {
        yield* platform.fs.remove(offFile);
        yield* syncStateDirectory;
        disabled = false;
        if (active) {
          active.paused = false;
          active.epoch++;
          active.heartbeat = 0;
          active.nextAttempt = 0;
          base = undefined;
          inputs.clear();
        }
      }
    }));
    if (!active || active.parentSessionId !== ctx.sessionManager.getSessionId()) yield* start(ctx);
    const result: CommandOutput<typeof Commands.pluginOn> = { enabled: true };
    return result;
  });
  const command = Effect.fn("Plugin.command")((args: string, ctx: Context) => Effect.suspend(() => {
    const text = args.trim(), split = text.indexOf(" ");
    const action = split < 0 ? text : text.slice(0, split);
    const scope = action === "off" || action === "on" ? undefined : active?.scope;
    const operation = Effect.gen(function*() {
    if (action === "" || action === "help") {
      return yield* notify(ctx, pluginCommands.map((entry) => `${entry.name.slice("plugin.".length)}: ${entry.description}`).join("\n"));
    }
    const entry = pluginCommands.find((entry) => entry.name === `plugin.${action}`);
    if (!entry) return yield* notify(ctx, "Unknown Coordinator action; use /coordinator help. Send free-form messages in normal chat.", "error");
    const data = split < 0 ? {} : yield* Schema.decodeEffect(Schema.fromJsonString(Json), { onExcessProperty: "error" })(text.slice(split + 1));
    if (entry.capability === Commands.pluginOff.capability) {
      const result = yield* entry.execute({ pluginOff: () => off(ctx) }, data);
      return yield* notify(ctx, Formatter.formatJson(result, { space: 2 }));
    }
    if (entry.capability === Commands.pluginOn.capability) {
      const result = yield* entry.execute({ pluginOn: () => on(ctx) }, data);
      return yield* notify(ctx, Formatter.formatJson(result, { space: 2 }));
    }
    if (disabled) return yield* new CommandError({ message: "Coordinator is off; use /coordinator on" });
    const current = active;
    if (!current || current.parentSessionId !== ctx.sessionManager.getSessionId()) return yield* new CommandError({ message: "Session not enrolled" });
    const { sessionId, incarnation, workerId, journal } = current;
    const approve = Effect.fn("Plugin.approve")(function*(title: string, text: string) {
      if (!ctx.hasUI || !(yield* confirm(ctx, title, text))) return yield* Effect.fail(new PluginDeclined({ message: "Human approval declined or unavailable" }));
    });
    const readback = Effect.fn("Plugin.readback")(function*(data: CommandInput<typeof Commands.pluginReadback>, disposition: Disposition) {
      const { address } = data;
      if (address.sessionId !== sessionId || address.incarnation !== incarnation || address.workerId !== workerId) return yield* Effect.fail(new CommandError({ message: "Readback must address this agent session incarnation and worker" }));
      if (disposition === DispositionEnum.ready) {
        const actual = yield* local(ctx);
        if (address.deadline <= (yield* Clock.currentTimeMillis) || address.repo !== actual.repo || address.sha !== actual.sha || address.ref !== actual.ref) return yield* Effect.fail(new CommandError({ message: "Readback does not match current local repository/ref/SHA/worker/deadline" }));
      }
      return yield* call(Commands.readbackTask, { ...data, disposition });
    });
    const capabilities: Omit<CommandCapabilities<"plugin", CommandError | PluginDeclined | HttpError | Cause.TimeoutError | PlatformError.PlatformError | Schema.SchemaError>, "pluginOff" | "pluginOn"> = {
      pluginParticipantConsent: (data) => Effect.gen(function*() {
        yield* approve("Your participant archive consent", `${data.enabled ? "Grant" : "Revoke"} YOUR consent for owner ${data.owner}, session ${data.sessionId}? Content omitted; graph metadata only.`);
        return yield* call(Commands.consentParticipant, data);
      }),
      pluginConsent: ({ participants }) => serial(Effect.gen(function*() {
        yield* approve("Your session archive consent", `Grant YOUR consent to archive this session with participants ${participants.join(", ")}? Each other participant must separately enroll consent with their own credential; broker checks it. Only graph metadata is archived across branches; content is omitted.`);
        yield* call(Commands.consentArchive, { sessionId, participants, enabled: true });
        yield* held("journal write (consent)", journal.consent(participants), current);
        const result: CommandOutput<typeof Commands.pluginConsent> = { enabled: true };
        return result;
      })),
      pluginOptout: () => serial(Effect.gen(function*() {
        // Revoke server enrollment first; failure leaves local spool intact for retry.
        yield* call(Commands.consentArchive, { sessionId, participants: journal.state.participants, enabled: false });
        yield* held("journal write (erase)", journal.erase(), current);
        yield* call(Commands.deleteArchive, { sessionId });
        const result: CommandOutput<typeof Commands.pluginOptout> = { enabled: false };
        return result;
      })),
      pluginAccept: (data) => call(Commands.replyPeer, { ...data, sessionId, incarnation, decision: OfferDecisionEnum.accepted }),
      pluginDecline: (data) => call(Commands.replyPeer, { ...data, sessionId, incarnation, decision: OfferDecisionEnum.declined }),
      pluginSubmit: (work) => Effect.gen(function*() {
        yield* approve("Approve coordinator task", `${work.taskId}: ${work.repo} ${work.ref} ${work.sha}; deadline ${work.deadline}; runner ${work.launcher}. Submit this exact approved work?`);
        return yield* call(Commands.submitTask, work);
      }),
      pluginReadback: (data) => readback(data, DispositionEnum.ready),
      pluginHold: (data) => readback(data, DispositionEnum.hold),
      pluginClarify: (data) => readback(data, DispositionEnum.clarify),
      pluginUnable: (data) => readback(data, DispositionEnum.unable),
      pluginAddress: (data) => call(Commands.addressTask, data),
      pluginConfirm: (data) => Effect.gen(function*() {
        yield* approve("Confirm coordinator release", `Release this exact ready addressed revision?\n${Formatter.formatJson(data, { space: 2 })}`);
        return yield* call(Commands.confirmTask, data);
      }),
      pluginHandoff: (data) => Effect.gen(function*() {
        yield* approve("Approve coordinator handoff", `Transfer ownership to this exact agent recipient?\n${Formatter.formatJson(data, { space: 2 })}`);
        return yield* call(Commands.handoffTask, data);
      }),
      pluginInstruction: (instruction) => Effect.gen(function*() {
        yield* approve("Approve later addressed instruction", `Approve this exact new addressed revision? Existing agent work is not retroactively gated.\n${Formatter.formatJson(instruction, { space: 2 })}`);
        return yield* call(Commands.instructTask, instruction);
      }),
      pluginReport: (data) => call(Commands.reportTask, { ...data, sessionId, incarnation }),
      pluginEvidenceTarget: (data) => call(Commands.targetEvidence, data),
      pluginEvidenceCi: (data) => call(Commands.collectCiEvidence, data),
      pluginEvidenceLocal: (data) => call(Commands.recordLocalEvidence, data),
      pluginEvidenceReview: (data) => call(Commands.recordReviewEvidence, data),
      pluginEvidenceDeployment: (data) => call(Commands.recordDeploymentEvidence, data),
      pluginStatus: (data) => call(Commands.getTask, data),
      pluginSessions: () => Effect.gen(function*() {
        const result: CommandOutput<typeof Commands.pluginSessions> = [];
        let cursor: SessionId | undefined;
        do {
          const page = yield* call(Commands.listSessions, cursor === undefined ? {} : { cursor });
          result.push(...page.sessions);
          cursor = page.nextCursor;
        } while (cursor !== undefined);
        return result;
      }),
      pluginAttach: (data) => call(Commands.attachTask, data),
      pluginControl: (data) => Effect.gen(function*() {
        yield* RunnerCommand.match(data, {
          [RunnerControlEnum.getState]: () => Effect.void,
          [RunnerControlEnum.abort]: () => approve("Approve sensitive agent control", `Abort this agent child; the task remains held for reconciliation?\n${Formatter.formatJson(data, { space: 2 })}`),
          [RunnerControlEnum.getMessages]: () => approve("Approve sensitive agent control", `Read potentially sensitive agent child messages?\n${Formatter.formatJson(data, { space: 2 })}`),
        });
        return yield* call(Commands.controlTask, data);
      }),
      pluginArchive: (data) => call(Commands.listArchive, { ...data, sessionId }),
      pluginInbox: (data) => Effect.gen(function*() {
        if (data.clear === true) {
          yield* held("journal write (clear inbox)", journal.clearInbox(), current);
          const result: CommandOutput<typeof Commands.pluginInbox> = { cleared: true };
          return result;
        }
        return journal.state.inbox;
      }),
      pluginIdentity: () => Effect.gen(function*() {
        const result: CommandOutput<typeof Commands.pluginIdentity> = { sessionId, incarnation, machineId: config.machineId, ...(yield* local(ctx)), workerId, workerIdentitySource: "agent-session" };
        return result;
      }),
    };
    const result = yield* entry.execute(capabilities, data);
    // Status/readback is user-visible, not model context. Never echo credentials or arbitrary server errors.
    yield* notify(ctx, Formatter.formatJson(result, { space: 2 }));
  }).pipe(
    Effect.catchTag("PluginDeclined", () => Effect.void),
    Effect.tapCause((cause) => Cause.hasInterrupts(cause) ? Effect.void :
      notify(ctx, "Coordinator command failed; no automatic retry. Check exact JSON fields, authorization and connection.", "error")),
    );
    return scope ? operation.pipe(Effect.forkIn(scope), Effect.flatMap(Fiber.join)) : operation;
  }));
  return { config, start, stop: () => serial(stop()), command, agent };
});

export class Plugin extends EffectContext.Service<Plugin>()("coordinator/Plugin", { make: makePlugin }) {
  static readonly layer = Layer.effect(this, this.make());
}

// Public extension callbacks are the only Promise/runtime boundary.
export default async function coordinator(host: Host): Promise<void> {
  const layer = Plugin.layer.pipe(Layer.provide(Platform.layer), Layer.provide(NodeServices.layer));
  let runtime = ManagedRuntime.make(layer);
  try {
    const initial = await runtime.runPromise(Plugin);
    if (!initial) { await runtime.dispose(); return; }
    let disposed = false;
    const start = async (_event: unknown, ctx: Context): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (disposed) { runtime = ManagedRuntime.make(layer); disposed = false; }
      await runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.start(ctx) : Effect.void));
    };
    host.on("session_start", start);
    if (initial.config.host === AgentHostEnum.omp) host.on("session_switch", start);
    host.on("session_shutdown", async () => { await runtime.dispose(); disposed = true; });
    host.registerCommand("coordinator", { description: "Coordinator: on/off participation, addressed dispatch, status, consent and runner control (JSON arguments)", handler: (args, ctx) => runtime.runPromise(Plugin.use((plugin) => plugin ? plugin.command(args, ctx) : Effect.void).pipe(
      Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator command failed; no automatic retry" }))),
    )) });
  } catch { await runtime.dispose(); throw new CommandError({ message: "Coordinator plugin initialization failed; check private configuration and connection" }); }
}
