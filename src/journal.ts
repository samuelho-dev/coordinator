import { ByteSize, Clock, Context, Effect, Graph, Layer, Option, Record, Schema, Semaphore } from "effect";
import { ArchiveEntry, CommandError, HostJournalEntry, JournalHandledKey, JournalReleaseKey, JournalReleaseStateEnum, JournalState, type ArchivePayload, type JsonValue, type ObservedFile } from "./contracts.ts";
import { Archive, Peer, RunnerTask, type PeerId, type SessionId } from "./generated/database/types.ts";
import { LIMIT } from "./mcp.ts";
import { Platform } from "./platform.ts";

const parseHostEntry = Schema.decodeEffect(Schema.fromJsonString(HostJournalEntry));

const makeJournal = Effect.fn("Journal.make")(function*(directory: string, sessionId: SessionId) {
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
    if (Record.keys(state.seen).some((id) => !present.has(id))) gap("rewrite");
    yield* coverage(); yield* persist();
    observedFile = mtimeMs === undefined ? undefined : { path: file, size, mtimeMs };
  })));
  const upload = Effect.fn("Journal.upload")(<A, E, R>(send: (entry: ArchiveEntry) => Effect.Effect<A, E, R>, maximum = 8) => serial(Effect.gen(function*() {
    for (let count = 0; count < maximum && state.consent && state.spool.length; count++) { yield* send(state.spool[0]!); state.spool.shift(); yield* persist(); }
  })));
  return { path, state, save, handled, handle, clearInbox, receive, releaseIntent, releaseSent, consent, erase, scan, upload };
});

export class Journal extends Context.Service<Journal>()("coordinator/Journal", { make: makeJournal }) {
  static layer(directory: string, sessionId: SessionId) { return Layer.effect(this, this.make(directory, sessionId)); }
}
