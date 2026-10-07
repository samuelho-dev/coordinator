import { ByteSize, Clock, Context, Effect, Layer, Option, Schema, Semaphore } from "effect";
import { ArchiveEntry, ArchiveEntryId, CommandError, JournalHandledKey, JournalReleaseKey, JournalState, JsonValue, type Hash, type MessageId, type PrincipalId, type SessionId, type ObservedFile, type SafeEntry } from "./contracts.ts";
import { canonicalJson, decode, decodeJson, encodeJson, LIMIT } from "./http.ts";
import { Platform } from "./platform.ts";

// Archive graph metadata only; no native message content crosses this boundary.
export function safeEntry(value: unknown): SafeEntry | undefined {
  if (!value || typeof value !== "object" || !("id" in value)) return;
  const id = Schema.decodeUnknownOption(ArchiveEntryId)(value.id);
  if (Option.isNone(id)) return;
  const type = "type" in value && typeof value.type === "string" && /^[a-z_]{1,40}$/.test(value.type) ? value.type : "omitted";
  const parentId = Option.getOrNull(Schema.decodeUnknownOption(ArchiveEntryId)("parentId" in value ? value.parentId : null));
  const message = "message" in value && value.message && typeof value.message === "object" ? value.message : undefined;
  const role = message && "role" in message && (message.role === "user" || message.role === "assistant" || message.role === "toolResult") ? message.role : undefined;
  return { id: id.value, linked: parentId !== null, payload: { id: id.value, parentId, type, ...(role ? { role } : {}), contentOmitted: true } };
}

const makeJournal = Effect.fn("Journal.make")(function*(directory: string, sessionId: SessionId) {
  const platform = yield* Platform;
  const fs = platform.fs;
  const path = platform.path.join(directory, `${yield* platform.digest(sessionId)}.json`);
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  if (((yield* fs.stat(directory)).mode & 0o077) !== 0) return yield* Effect.fail(new CommandError({ message: "Private journal directory required" }));
  const exists = yield* fs.exists(path);
  if (exists && ((yield* fs.stat(path)).mode & 0o077) !== 0) return yield* Effect.fail(new CommandError({ message: "Private journal file required" }));
  const state: JournalState = exists ? decodeJson(JournalState, yield* fs.readFileString(path)) : {
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
    const text = encodeJson(JournalState, state);
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
  const serial = <A, E>(operation: Effect.Effect<A, E>) => semaphore.withPermits(1)(Effect.suspend<A, E | CommandError, never>(() =>
    writeFailed ? Effect.fail(new CommandError({ message: "Journal durability unavailable; reopen required" })) : operation
  ).pipe(Effect.uninterruptible));
  const handled = Effect.fn("Journal.handled")(function*(id: JournalHandledKey) { return state.handled[id] === true; });
  const save = Effect.fn("Journal.save")(() => serial(persist()));
  const handle = Effect.fn("Journal.handle")((id: JournalHandledKey) => serial(Effect.gen(function*() { state.handled[id] = true; yield* persist(); })));
  const clearInbox = Effect.fn("Journal.clearInbox")(() => serial(Effect.gen(function*() { state.inbox = {}; yield* persist(); })));
  const receive = Effect.fn("Journal.receive")((messageId: MessageId, payload: JsonValue) => serial(Effect.gen(function*() {
    const hash = yield* platform.fingerprint(payload);
    const prior = state.inbox[messageId];
    if (prior) {
      if (prior.digest !== hash) return yield* Effect.fail(new CommandError({ message: "Received message ID payload conflict" }));
      return false;
    }
    const handledKey = JournalHandledKey.make(`peer:${messageId}`);
    if (state.handled[handledKey]) return false;
    const entry = { payload, digest: hash, receivedAt: yield* Clock.currentTimeMillis };
    if (new TextEncoder().encode(canonicalJson({ ...state.inbox, [messageId]: entry })).length > 128 * 1024) return yield* Effect.fail(new CommandError({ message: "Local inbox full; handling acknowledgement withheld" }));
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
      yield* file.writeAll(new TextEncoder().encode("intent"));
      yield* file.sync;
      yield* syncDirectory;
      return true;
    })).pipe(Effect.catch((error) => error.reason._tag === "AlreadyExists" ? Effect.succeed(false) : Effect.fail(error)), Effect.onError(() => Effect.sync(() => { writeFailed = true; })));
    state.releases[key] = "intent";
    yield* persist();
    return created;
  })));
  const releaseSent = Effect.fn("Journal.releaseSent")((key: JournalReleaseKey) => serial(Effect.gen(function*() { state.releases[key] = "sent"; yield* persist(); })));
  const consent = Effect.fn("Journal.consent")((participants: PrincipalId[]) => serial(Effect.gen(function*() { state.consent = true; state.participants = participants; yield* persist(); })));
  const erase = Effect.fn("Journal.erase")(() => serial(Effect.gen(function*() {
    state.consent = false; state.participants = []; state.spool = []; state.seen = {}; state.gaps = []; delete state.fileDigest; observedFile = undefined;
    // Delivery dedup/intent records survive opt-out: never admit a duplicate prompt.
    yield* persist();
  })));
  const coverage = Effect.fn("Journal.coverage")(function*() {
    if (!state.gaps.length) return;
    const payload = { coverageOnly: true as const, gaps: [...state.gaps] };
    const hash = yield* platform.fingerprint(payload);
    if (state.fileDigest !== hash) { state.spool.push({ entryId: ArchiveEntryId.make(`coverage:${hash}`), digest: hash, payload, gaps: [...state.gaps] }); state.fileDigest = hash; }
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
    const present = new Set<Hash>();
    for (const line of lines) {
      if (new TextEncoder().encode(line).length > LIMIT) { gap("overflow"); continue; }
      let raw: JsonValue;
      try { raw = decodeJson(JsonValue, line); } catch { gap("invalid-line"); continue; }
      const safe = safeEntry(raw);
      if (!safe) continue;
      const entryKey = yield* platform.digest(safe.id);
      present.add(entryKey);
      const originalDigest = yield* platform.digest(line);
      if (state.seen[entryKey] === originalDigest) continue;
      if (state.seen[entryKey]) gap("rewrite");
      if (!safe.linked) gap("unlinked");
      const item = { entryId: safe.id, digest: yield* platform.fingerprint(safe.payload), payload: safe.payload, gaps: [...state.gaps] };
      if (new TextEncoder().encode(canonicalJson([...state.spool, item])).length > 510 * 1024) { gap("overflow"); break; }
      state.seen[entryKey] = originalDigest; state.spool.push(item);
    }
    if (Object.keys(state.seen).some((id) => !present.has(decode(JournalState.fields.seen.schema.key, id)))) gap("rewrite");
    yield* coverage(); yield* persist();
    observedFile = mtimeMs === undefined ? undefined : { path: file, size, mtimeMs };
  })));
  const upload = Effect.fn("Journal.upload")((send: (entry: ArchiveEntry) => Effect.Effect<unknown, unknown>, maximum = 8) => serial(Effect.gen(function*() {
    for (let count = 0; count < maximum && state.consent && state.spool.length; count++) { yield* send(state.spool[0]!); state.spool.shift(); yield* persist(); }
  })));
  return { path, state, save, handled, handle, clearInbox, receive, releaseIntent, releaseSent, consent, erase, scan, upload };
});

export class Journal extends Context.Service<Journal>()("coordinator/Journal", { make: makeJournal }) {
  static layer(directory: string, sessionId: SessionId) { return Layer.effect(this, this.make(directory, sessionId)); }
}
