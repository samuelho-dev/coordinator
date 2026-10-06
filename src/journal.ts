import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { digest, fingerprint, LIMIT } from "./protocol.js";

export function durable(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.tmp`;
  const fd = openSync(temporary, "w", 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(temporary, path);
  const directory = openSync(dirname(path), "r");
  try { fsyncSync(directory); } finally { closeSync(directory); }
}

interface ArchiveEntry { entryId: string; digest: string; payload: unknown; gaps: string[] }
interface State {
  handled: Record<string, true>; releases: Record<string, "intent" | "sent">;
  consent: boolean; participants: string[]; seen: Record<string, string>;
  fileDigest?: string; spool: ArchiveEntry[]; gaps: string[];
  inbox: Record<string, { payload: unknown; digest: string; receivedAt: number }>;
}
// Archive metadata only: never copy message text, tool arguments/results, thinking, paths,
// custom data, credentials, or model payloads across this trust boundary.
export function safeEntry(value: unknown): { id: string; payload: unknown; linked: boolean } | undefined {
  if (!value || typeof value !== "object") return;
  const entry = value as Record<string, unknown>;
  if (typeof entry.id !== "string" || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(entry.id)) return;
  const type = typeof entry.type === "string" && /^[a-z_]{1,40}$/.test(entry.type) ? entry.type : "omitted";
  const parentId = typeof entry.parentId === "string" && /^[a-zA-Z0-9_.:-]{1,160}$/.test(entry.parentId) ? entry.parentId : null;
  const message = entry.message && typeof entry.message === "object" ? entry.message as Record<string, unknown> : undefined;
  const role = message && ["user", "assistant", "toolResult"].includes(String(message.role)) ? message.role : undefined;
  return { id: entry.id, linked: parentId !== null, payload: { id: entry.id, parentId, type, ...(role ? { role } : {}), contentOmitted: true } };
}

export class Journal {
  readonly path: string;
  state: State;
  private observedFile?: { path: string; size: number; mtimeMs: number };
  constructor(directory: string, sessionId: string) {
    this.path = join(directory, `${digest(sessionId)}.json`);
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    if ((statSync(directory).mode & 0o077) !== 0) throw new Error("Private journal directory required");
    if (existsSync(this.path) && (statSync(this.path).mode & 0o077) !== 0) throw new Error("Private journal file required");
    this.state = existsSync(this.path) ? JSON.parse(readFileSync(this.path, "utf8")) as State : {
      handled: {}, releases: {}, consent: false, participants: [], seen: {}, spool: [], gaps: [], inbox: {},
    };
    this.state.inbox ??= {};
  }
  save(): void { durable(this.path, this.state); }
  handled(id: string): boolean { return this.state.handled[id] === true; }
  handle(id: string): void { this.state.handled[id] = true; this.save(); }
  receive(messageId: string, payload: unknown): boolean {
    const hash = fingerprint(payload); const prior = this.state.inbox[messageId];
    if (prior) {
      if (prior.digest !== hash) throw new Error("Received message ID payload conflict");
      return false;
    }
    if (this.handled(`peer:${messageId}`)) return false;
    if (Buffer.byteLength(JSON.stringify({ ...this.state.inbox, [messageId]: payload })) > 128 * 1024) throw new Error("Local inbox full; handling acknowledgement withheld");
    this.state.inbox[messageId] = { payload, digest: hash, receivedAt: Date.now() };
    this.state.handled[`peer:${messageId}`] = true;
    this.save(); return true;
  }
  releaseIntent(key: string): boolean {
    if (this.state.releases[key]) return false;
    const intent = `${this.path}.${digest(key)}.intent`;
    try {
      const fd = openSync(intent, "wx", 0o600);
      try { writeFileSync(fd, "intent"); fsyncSync(fd); } finally { closeSync(fd); }
    } catch (error) {
      if (!error || typeof error !== "object" || !("code" in error) || error.code !== "EEXIST") throw error;
      this.state.releases[key] = "intent"; this.save(); return false;
    }
    this.state.releases[key] = "intent"; this.save(); return true;
  }
  releaseSent(key: string): void { this.state.releases[key] = "sent"; this.save(); }
  consent(participants: string[]): void { this.state.consent = true; this.state.participants = participants; this.save(); }
  erase(): void {
    this.state.consent = false; this.state.participants = []; this.state.spool = [];
    this.state.seen = {}; this.state.gaps = []; delete this.state.fileDigest;
    this.observedFile = undefined;
    // Keep delivery dedup/intent records: opting out must not enable duplicate prompts.
    this.save();
  }
  coverage(): void {
    if (!this.state.gaps.length) return;
    const payload = { coverageOnly: true, gaps: [...this.state.gaps] };
    const hash = fingerprint(payload);
    if (this.state.fileDigest !== hash) {
      this.state.spool.push({ entryId: `coverage:${hash}`, digest: hash, payload, gaps: [...this.state.gaps] });
      this.state.fileDigest = hash;
    }
  }
  scan(file: string | undefined): void {
    if (!this.state.consent) return;
    const gap = (name: string) => { if (!this.state.gaps.includes(name)) this.state.gaps.push(name); };
    gap("content-omitted");
    if (!file) { gap("ephemeral"); this.coverage(); this.save(); return; }
    if (!existsSync(file)) { gap("missing"); this.coverage(); this.save(); return; }
    // ponytail: cap file scans at 8 MiB; larger host journals report coverage loss, not unsafe allocation.
    const metadata = statSync(file);
    if (metadata.size > 8 * 1024 * 1024) { gap("overflow"); this.coverage(); this.save(); return; }
    if (this.observedFile?.path === file && this.observedFile.size === metadata.size && this.observedFile.mtimeMs === metadata.mtimeMs) return;
    this.observedFile = { path: file, size: metadata.size, mtimeMs: metadata.mtimeMs };
    const text = readFileSync(file, "utf8");
    const complete = text.slice(0, text.lastIndexOf("\n") + 1);
    const lines = complete.split("\n").filter(Boolean);
    const present = new Set<string>();
    for (const line of lines) {
      if (Buffer.byteLength(line) > LIMIT) { gap("overflow"); continue; }
      let raw: unknown;
      try { raw = JSON.parse(line); } catch { gap("invalid-line"); continue; }
      const safe = safeEntry(raw);
      if (!safe) continue;
      const entryKey = digest(safe.id);
      present.add(entryKey);
      const originalDigest = digest(line);
      if (this.state.seen[entryKey] === originalDigest) continue;
      if (this.state.seen[entryKey]) gap("rewrite");
      if (!safe.linked) gap("unlinked");
      const payloadDigest = fingerprint(safe.payload);
      const item = { entryId: safe.id, digest: payloadDigest, payload: safe.payload, gaps: [...this.state.gaps] };
      if (Buffer.byteLength(JSON.stringify([...this.state.spool, item])) > 510 * 1024) { gap("overflow"); break; }
      this.state.seen[entryKey] = originalDigest;
      this.state.spool.push(item);
    }
    if (Object.keys(this.state.seen).some((id) => !present.has(id))) gap("rewrite");
    this.coverage();
    this.save();
  }
  async upload(send: (entry: ArchiveEntry) => Promise<unknown>, maximum = 8): Promise<void> {
    for (let count = 0; count < maximum && this.state.consent && this.state.spool.length; count++) {
      await send(this.state.spool[0]!);
      this.state.spool.shift(); this.save();
    }
  }
}
