import { Schema } from "effect";
import { DatabaseSync } from "node:sqlite";
import { spawn, execFileSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { chmodSync, mkdirSync, realpathSync, statSync, accessSync, constants, existsSync, cpSync, readdirSync, lchownSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import type { Server } from "node:http";
import { Address, Work, Id, LIMIT, checkToken, decode, digest, fingerprint, inspectedRepo, privateFile, rpc, type Acceptance, type RunnerConfig, type Task } from "./protocol.ts";
import { HttpError, serveRpc } from "./http.ts";

const FRAME_LIMIT = 4 * 1024 * 1024;
const RETAIN_EVENTS = 128;
const Terminal: Record<string, true> = { SUCCEEDED: true, FAILED: true };
const TaskInput = Schema.Struct({ taskId: Id });
const InspectInput = Schema.Struct({ taskId: Id, cursor: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))) });
const CommandInput = Schema.Struct({ taskId: Id, type: Schema.Literals(["get_state", "get_messages", "abort"]) });
const PrepareInput = Schema.Struct({ taskId: Id, address: Address });
const ReleaseInput = Schema.Struct({ taskId: Id, address: Address, prompt: Work.fields.prompt });
type Json = Record<string, unknown>;
function record(value: unknown): value is Json { return typeof value === "object" && value !== null && !Array.isArray(value); }
function inside(root: string, path: string): boolean { const r = relative(root, path); return r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); }
function git(cwd: string, args: string[]): string { return execFileSync("git", args, { cwd, encoding: "utf8", timeout: 30000, maxBuffer: LIMIT, stdio: ["ignore", "pipe", "ignore"] }).trim(); }

export const CHILD_UID_BASE = 1000000;
/** setpriv clears groups and every capability before executing an untrusted child. */
export function isolationArgs(uid: number): string[] {
  if (process.platform !== "linux" || !Number.isSafeInteger(uid) || uid < CHILD_UID_BASE || uid >= 0xffffffff) throw new Error("Isolated Linux child UID required; macOS is plugin-only");
  return [`--reuid=${uid}`, `--regid=${uid}`, "--clear-groups", "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs"];
}
export function childEnvironment(home?: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", ...(home ? { HOME: home } : {}) };
  for (const key of ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "OMP_AUTH_BROKER_URL", "OMP_AUTH_BROKER_TOKEN"]) {
    if (process.env[key]) env[key] = process.env[key];
  }
  return env;
}
export function ownChildDirectory(path: string, uid: number): void {
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    const child = join(path, entry.name);
    if (entry.isDirectory()) ownChildDirectory(child, uid);
    else lchownSync(child, uid, uid);
  }
  lchownSync(path, uid, uid);
}
export function requireIsolation(config: RunnerConfig): void {
  const privatePaths = [config.stateDirectory, dirname(config.tokenFile), config.modelDirectory, ...Object.values(config.repositories)];
  if (!isAbsolute(config.childDirectory) || resolve(config.childDirectory) !== config.childDirectory ||
    privatePaths.some((path) => inside(config.childDirectory, path) || inside(path, config.childDirectory))) throw new Error("Child root must be separate from private runner data");
  if (!isAbsolute(config.modelDirectory) || realpathSync(config.modelDirectory) !== config.modelDirectory || !statSync(config.modelDirectory).isDirectory()) throw new Error("Canonical model seed required");
  mkdirSync(config.childDirectory, { recursive: true, mode: 0o711 });
  if (realpathSync(config.childDirectory) !== config.childDirectory) throw new Error("Canonical child root required");
  chmodSync(config.childDirectory, 0o711);
  for (let parent = dirname(config.childDirectory); parent !== "/"; parent = dirname(parent)) {
    if (!(statSync(parent).mode & 0o001)) throw new Error("Child root ancestors must permit traversal without exposing private runner directories");
  }
  const modelFiles = [".omp/agent/config.yml", ".omp/agent/auth.json", ".omp/agent/models.json",
    ".pi/agent/settings.json", ".pi/agent/auth.json", ".pi/agent/models.json"];
  const checkSeed = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name), name = relative(config.modelDirectory, path);
      if (entry.isDirectory() && modelFiles.some((file) => file.startsWith(`${name}/`))) checkSeed(path);
      else if (!entry.isFile() || !modelFiles.includes(name)) throw new Error("Model seed must contain only scoped model config/auth, not sessions, history or symlinks");
    }
  };
  checkSeed(config.modelDirectory);
  try {
    execFileSync("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), process.execPath, "-e",
      "const f=require('node:fs'),s=Object.fromEntries(f.readFileSync('/proc/self/status','utf8').split(String.fromCharCode(10)).map(l=>l.split(':').map(v=>v.trim())));if(process.getuid()!==1000000||process.getgid()!==1000000||s.Groups!==''||['CapInh','CapPrm','CapEff','CapBnd','CapAmb'].some(k=>s[k]!=='0000000000000000')||s.NoNewPrivs!=='1')process.exit(1)"],
      { env: childEnvironment(), stdio: "pipe", timeout: 10000 });
    for (const launcher of Object.values(config.launchers)) execFileSync("/usr/bin/setpriv", [...isolationArgs(CHILD_UID_BASE), launcher, "--version"], { env: childEnvironment("/tmp"), stdio: "pipe", timeout: 10000 });
  }
  catch { throw new Error("OS child UID/capability isolation unavailable; runner enrollment/launch blocked"); }
}
/** LF wire framing accepts optional CRLF; invalid UTF-8 and incomplete frames are errors. */
export class LfFrames {
  private pending = Buffer.alloc(0);
  push(chunk: Buffer): unknown[] {
    const bytes = Buffer.concat([this.pending, chunk]);
    const frames: unknown[] = []; let start = 0;
    for (let end = bytes.indexOf(10); end !== -1; end = bytes.indexOf(10, start)) {
      if (end - start > FRAME_LIMIT) throw new Error("RPC frame overflow");
      const line = bytes.subarray(start, bytes[end - 1] === 13 ? end - 1 : end);
      if (line.length === 0) throw new Error("Invalid LF frame");
      frames.push(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(line)));
      start = end + 1;
    }
    this.pending = Buffer.from(bytes.subarray(start));
    if (this.pending.length > FRAME_LIMIT) throw new Error("RPC frame overflow");
    return frames;
  }
  end(): void { if (this.pending.length) throw new Error("Incomplete RPC frame"); }
}
interface Row { taskId: string; childUid: number; fingerprint: string; work: string; worktree: string; state: string; occupied: number; pid: number | null; incarnation: string; address: string | null; promptId: string | null; reason: string | null; result: string | null; detached: number; }
interface Child {
  process: ChildProcessWithoutNullStreams;
  pending: Map<string, { type: string; resolve: (value: Json) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>;
  lost: boolean; completing: boolean; started: boolean; failed: boolean;
}
const Config = Schema.Struct({
  id: Id, brokerUrl: Schema.String, tokenFile: Schema.String, host: Schema.String,
  controlTokenHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  port: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65535 })),
  stateDirectory: Schema.String, slots: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  repositories: Schema.Record(Schema.String, Schema.String), launchers: Schema.Record(Schema.String, Schema.String),
  modelDirectory: Schema.String, childDirectory: Schema.String,
  childUidBase: Schema.Int.check(Schema.isBetween({ minimum: CHILD_UID_BASE, maximum: 0xffffffff - CHILD_UID_BASE })),
});

export class Runner {
  readonly config: RunnerConfig;
  readonly database: DatabaseSync;
  private readonly token: string;
  private readonly children = new Map<string, Child>();
  private readonly admissions = new Map<string, { fingerprint: string; promise: Promise<Acceptance> }>();
  private server?: Server;
  private interval?: NodeJS.Timeout;
  private heartbeatBusy = false;
  private reportsBusy = false;
  private closed = false;
  constructor(input: RunnerConfig) {
    this.config = decode(Config, input);
    if (!isAbsolute(input.stateDirectory) || !isAbsolute(input.tokenFile)) throw new Error("Absolute private paths required");
    const endpoint = new URL(input.brokerUrl);
    if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) throw new Error("TLS required");
    requireIsolation(this.config);
    this.token = privateFile(input.tokenFile);
    if (this.token.length < 32 || this.token.length > 480) throw new Error("Invalid credential length");
    if (input.controlTokenHash === digest(this.token)) throw new Error("Distinct enrollment and control credentials required");
    process.umask(0o077); // Inherited by hosts: session files and host artifacts must remain private.
    mkdirSync(input.stateDirectory, { recursive: true, mode: 0o700 });
    if ((statSync(input.stateDirectory).mode & 0o077) !== 0) throw new Error("Private state directory required");
    if (realpathSync(input.stateDirectory) !== input.stateDirectory) throw new Error("Canonical state directory required");
    for (const [repo, path] of Object.entries(input.repositories)) {
      if (!isAbsolute(path) || inspectedRepo(realpathSync(path)) !== repo) throw new Error("Invalid allowlisted repository");
    }
    for (const [name, path] of Object.entries(input.launchers)) {
      if (!["omp", "pi"].includes(name) || !isAbsolute(path) || !statSync(path).isFile()) throw new Error("Invalid launcher");
      accessSync(path, constants.X_OK);
    }
    const dbPath = join(input.stateDirectory, "runner.sqlite");
    this.database = new DatabaseSync(dbPath);
    chmodSync(dbPath, 0o600);
    this.database.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS tasks (taskId TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, work TEXT NOT NULL,
        worktree TEXT NOT NULL, state TEXT NOT NULL, occupied INTEGER NOT NULL, pid INTEGER,
        incarnation TEXT NOT NULL, address TEXT, promptId TEXT, reason TEXT, result TEXT, detached INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, taskId TEXT NOT NULL, payload TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS events_task ON events(taskId,seq);
      CREATE TABLE IF NOT EXISTS owner (id INTEGER PRIMARY KEY CHECK(id=1), pid INTEGER);
      INSERT OR IGNORE INTO owner(id,pid) VALUES (1,NULL);`);
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const owner = this.database.prepare("SELECT pid FROM owner WHERE id=1").get()!.pid;
      if (typeof owner === "number") {
        try { process.kill(owner, 0); throw new Error("Runner journal already owned"); }
        catch (error) { if (!record(error) || error.code !== "ESRCH") throw error; }
      }
      this.database.prepare("UPDATE owner SET pid=? WHERE id=1").run(process.pid);
      this.database.exec("COMMIT");
    } catch (error) { this.database.exec("ROLLBACK"); this.database.close(); throw error; }
    if (!this.database.prepare("PRAGMA table_info(tasks)").all().some((column) => column.name === "detached")) this.database.exec("ALTER TABLE tasks ADD COLUMN detached INTEGER NOT NULL DEFAULT 0");
    this.database.exec("PRAGMA secure_delete=ON");
    if (this.database.prepare("PRAGMA user_version").get()?.user_version === 0) {
      // One-time scrub of legacy raw journals, including deleted SQLite/WAL pages.
      this.database.prepare("UPDATE tasks SET reason='historical diagnostic omitted' WHERE reason IS NOT NULL").run();
      for (const event of this.database.prepare("SELECT seq,payload FROM events").all()) {
        if (typeof event.seq !== "number") throw new Error("Invalid journal event identity");
        this.database.prepare("UPDATE events SET payload=? WHERE seq=?").run(JSON.stringify(this.eventMetadata(JSON.parse(String(event.payload)))), event.seq);
      }
      for (const row of this.database.prepare("SELECT taskId,result FROM tasks WHERE result IS NOT NULL").all()) {
        if (typeof row.taskId !== "string") throw new Error("Invalid journal task identity");
        this.database.prepare("UPDATE tasks SET result=? WHERE taskId=?").run(JSON.stringify(this.resultMetadata(JSON.parse(String(row.result)))), row.taskId);
      }
      this.database.exec("VACUUM; PRAGMA wal_checkpoint(TRUNCATE); PRAGMA user_version=1");
    }
    // ponytail: no stdio reattachment exists; uncertain owned children reserve their slot until operator reconciliation.
    this.database.prepare("UPDATE tasks SET state='HELD',reason='restart: launch or prompt outcome unknown' WHERE occupied=1 AND state!='ACCEPTED'").run();
    for (const row of this.database.prepare("SELECT ?+rowid AS childUid,* FROM tasks WHERE occupied=1 AND pid IS NOT NULL").all(this.config.childUidBase) as unknown as Row[]) {
      try { process.kill(row.pid!, 0); }
      catch (error) {
        if (record(error) && error.code === "ESRCH") this.database.prepare("UPDATE tasks SET occupied=0,reason='restart: child gone; outcome remains unknown' WHERE taskId=?").run(row.taskId);
      }
    }
  }
  // ponytail: one million non-overlapping reserved UIDs per runner; accepted rows are never pruned/reused.
  private row(taskId: string): Row | undefined { return this.database.prepare("SELECT ?+rowid AS childUid,* FROM tasks WHERE taskId=?").get(this.config.childUidBase, taskId) as Row | undefined; }
  private requireRow(taskId: string): Row { const row = this.row(taskId); if (!row) throw new HttpError(404, "Task not accepted"); return row; }
  private work(row: Row): Work { return decode(Work, JSON.parse(row.work)); }
  private acceptance(row: Row): Acceptance {
    return { taskId: row.taskId, accepted: true, state: row.state, worktree: row.worktree,
      ...(row.address ? { address: decode(Address, JSON.parse(row.address)) } : {}), ...(row.reason ? { reason: row.reason } : {}) };
  }
  private load(): number { return Number(this.database.prepare("SELECT count(*) AS count FROM tasks WHERE occupied=1").get()!.count); }
  private resultMetadata(value: unknown): Json | null {
    if (!record(value)) return null;
    const result: Json = {};
    for (const key of ["launched", "hostSettled", "evidenceVerified"]) if (typeof value[key] === "boolean") result[key] = value[key];
    if (typeof value.exitCode === "number" && Number.isSafeInteger(value.exitCode)) result.exitCode = value.exitCode;
    if (typeof value.signal === "string" && /^SIG[A-Z0-9]{1,16}$/.test(value.signal)) result.signal = value.signal;
    return result;
  }
  private eventMetadata(value: unknown): Json {
    if (!record(value)) return { type: "host_event", contentOmitted: true };
    const type = typeof value.type === "string" && /^[a-z_]{1,40}$/.test(value.type) ? value.type : "host_event";
    const result: Json = { type, contentOmitted: true };
    if (type === "readback") result.address = decode(Address, value.address);
    if (typeof value.state === "string" && ["ACCEPTED", "LAUNCHING", "READY", "PROMPT_INTENT", "RUNNING", "SUCCEEDED", "FAILED", "HELD"].includes(value.state)) result.state = value.state;
    if (typeof value.success === "boolean") result.success = value.success;
    if (typeof value.status === "string" && ["completed", "failed", "aborted", "error"].includes(value.status)) result.status = value.status;
    if (typeof value.command === "string" && ["get_state", "get_messages", "bash", "prompt", "abort"].includes(value.command)) result.command = value.command;
    for (const key of ["fingerprint", "digest"]) if (typeof value[key] === "string" && /^[a-f0-9]{64}$/.test(value[key])) result[key] = value[key];
    if (typeof value.id === "string" && /^[a-f0-9-]{36}$/.test(value.id)) result.id = value.id;
    return result;
  }
  private event(taskId: string, value: unknown): void {
    const payload = JSON.stringify(this.eventMetadata(value));
    this.database.prepare("INSERT INTO events(taskId,payload) VALUES (?,?)").run(taskId, payload);
    this.database.prepare("DELETE FROM events WHERE taskId=? AND seq NOT IN (SELECT seq FROM events WHERE taskId=? ORDER BY seq DESC LIMIT ?)").run(taskId, taskId, RETAIN_EVENTS);
  }
  private state(taskId: string, state: string, reason: string | null = null, result: unknown = null): void {
    this.database.prepare("UPDATE tasks SET state=?,reason=?,result=? WHERE taskId=?").run(state, reason, JSON.stringify(this.resultMetadata(result)), taskId);
    this.event(taskId, { type: "runner_state", state, reason });
  }
  private hold(taskId: string, reason: string): void {
    this.state(taskId, "HELD", reason);
    const child = this.children.get(taskId);
    if (child) { child.lost = true; for (const pending of child.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error("Child RPC unavailable")); } child.pending.clear(); }
  }
  async start(): Promise<Server> {
    this.server = await serveRpc({ host: this.config.host, port: this.config.port,
      authorize: (token) => { if (!checkToken(token, this.config.controlTokenHash) && !checkToken(token, digest(this.token))) throw new HttpError(401, "Unauthorized"); return token; },
      handle: (token, method, params) => this.handle(method, params, String(token)) });
    for (const row of this.database.prepare("SELECT ?+rowid AS childUid,* FROM tasks WHERE state='ACCEPTED' AND occupied=1 AND detached=0").all(this.config.childUidBase) as unknown as Row[]) this.schedule(row.taskId);
    this.interval = setInterval(() => { void this.heartbeat(); }, 5000);
    void this.heartbeat();
    return this.server;
  }
  async close(): Promise<void> {
    this.closed = true; clearInterval(this.interval);
    if (this.server) { const done = Promise.withResolvers<void>(); this.server.close(() => done.resolve()); await done.promise; }
    // Do not terminate work on transport/service shutdown. The journal stays open while child callbacks exist.
    if (!this.children.size) { this.database.exec("UPDATE owner SET pid=NULL WHERE id=1"); this.database.close(); }
  }
  private schedule(taskId: string): void { setImmediate(() => { if (this.server && !this.closed) void this.launch(taskId); }); }
  async handle(method: string, params: unknown, controlToken: string): Promise<unknown> {
    if (!checkToken(controlToken, this.config.controlTokenHash)) throw new HttpError(401, "Control credential required");
    try {
      switch (method) {
        case "acceptTask": return this.admit(decode(Work, params));
        case "journal": {
          const { taskId } = decode(TaskInput, params);
          if (this.admissions.has(taskId)) throw new HttpError(503, "Acceptance pending; ownership unknown");
          const row = this.row(taskId); return row ? this.acceptance(row) : { taskId, accepted: false, state: "not-accepted" };
        }
        case "inspect": {
          const { taskId, cursor = 0 } = decode(InspectInput, params); const row = this.requireRow(taskId);
          const rows = this.database.prepare("SELECT seq,payload FROM events WHERE taskId=? AND seq>? ORDER BY seq LIMIT 2").all(taskId, cursor);
          const oldest = this.database.prepare("SELECT min(seq) AS seq FROM events WHERE taskId=?").get(taskId)?.seq;
          const result = { ...this.acceptance(row), events: [] as unknown[], nextCursor: cursor };
          if (typeof oldest === "number" && cursor < oldest - 1) result.events.push({ type: "gap", reason: "retention or cursor gap", before: oldest });
          for (const event of rows) {
            const value: unknown = JSON.parse(String(event.payload));
            result.events.push(value); result.nextCursor = Number(event.seq);
            if (Buffer.byteLength(JSON.stringify(result)) > LIMIT - 512) {
              result.events.pop(); result.events.push({ type: "gap", reason: "event exceeds remaining response bound", seq: event.seq }); break;
            }
          }
          return result;
        }
        case "command": {
          const { taskId, type } = decode(CommandInput, params); this.requireRow(taskId);
          const response = await this.request(taskId, type);
          if (type === "abort") this.state(taskId, "HELD", "operator requested abort; await host outcome");
          return this.boundedResponse(response);
        }
        case "prepare": {
          const input = decode(PrepareInput, params); const row = this.requireRow(input.taskId);
          if (row.state !== "READY" || row.promptId || !row.address) throw new HttpError(409, "Corrected readback requires unprompted live owned child");
          const prior = decode(Address, JSON.parse(row.address));
          if (input.address.revision <= prior.revision || input.address.sessionId !== prior.sessionId || input.address.incarnation !== prior.incarnation) throw new HttpError(409, "Correction revision or child binding mismatch");
          const actual = await this.readback(row, await this.request(row.taskId, "get_state"), input.address);
          if (fingerprint(actual) !== fingerprint(input.address)) throw new HttpError(409, "Corrected consequential readback mismatch");
          this.database.prepare("UPDATE tasks SET address=? WHERE taskId=? AND state='READY' AND promptId IS NULL").run(JSON.stringify(actual), row.taskId);
          this.event(row.taskId, { type: "readback", address: actual }); return actual;
        }
        case "release": return await this.release(decode(ReleaseInput, params));
        default: throw new HttpError(404, "Unknown method");
      }
    } catch (error) { if (error instanceof HttpError) throw error; throw new HttpError(400, "Invalid request or unavailable owned child"); }
  }
  private async brokerTask(work: Work, address?: Address): Promise<Task> {
    const task = await rpc<Task>(this.config.brokerUrl, this.token, "task.get", { taskId: work.taskId });
    if (task.taskId !== work.taskId || task.runnerId !== this.config.id || task.fingerprint !== fingerprint(work) || fingerprint(task.request) !== fingerprint(work)) throw new HttpError(403, "Broker task binding mismatch");
    if (address && (!task.address || !task.readback || task.disposition !== "ready" || task.confirmedRevision !== address.revision ||
      fingerprint(task.address) !== fingerprint(address) || fingerprint(task.readback) !== fingerprint(address))) throw new HttpError(409, "Broker confirmation or current readback mismatch");
    return task;
  }
  private admit(work: Work): Promise<Acceptance> {
    const hash = fingerprint(work), pending = this.admissions.get(work.taskId);
    if (pending) {
      if (pending.fingerprint !== hash) throw new HttpError(409, "Task fingerprint conflict");
      return pending.promise;
    }
    const promise = this.accept(work).finally(() => { this.admissions.delete(work.taskId); });
    this.admissions.set(work.taskId, { fingerprint: hash, promise });
    return promise;
  }
  private async accept(work: Work): Promise<Acceptance> {
    if (Buffer.byteLength(JSON.stringify(work)) > LIMIT - 1024) throw new HttpError(413, "Work too large");
    const existing = this.row(work.taskId); const hash = fingerprint(work);
    if (existing) { if (existing.fingerprint !== hash) throw new HttpError(409, "Task fingerprint conflict"); return this.acceptance(existing); }
    if (this.load() >= this.config.slots) return { taskId: work.taskId, accepted: false, state: "full", reason: "full" };
    await this.brokerTask(work);
    const concurrent = this.row(work.taskId);
    if (concurrent) { if (concurrent.fingerprint !== hash) throw new HttpError(409, "Task fingerprint conflict"); return this.acceptance(concurrent); }
    const repo = this.config.repositories[work.repo]; const launcher = this.config.launchers[work.launcher];
    if (!repo || !launcher) throw new HttpError(403, "Repository or launcher not allowlisted");
    if (!work.ref || work.ref.startsWith("-") || /[\r\n\0]/.test(work.ref)) throw new HttpError(400, "Invalid ref");
    if (git(repo, ["rev-parse", "--verify", `${work.ref}^{commit}`]) !== work.sha || git(repo, ["rev-parse", "--verify", `${work.sha}^{commit}`]) !== work.sha) throw new HttpError(409, "Exact ref/SHA unavailable");
    const assigned = await this.brokerTask(work);
    if (work.deadline <= Date.now() || assigned.state !== "ASSIGNING" && assigned.state !== "ACCEPTED") throw new HttpError(409, "Assignment expired or no longer dispatchable");
    const accepted = this.row(work.taskId);
    if (accepted) { if (accepted.fingerprint !== hash) throw new HttpError(409, "Task fingerprint conflict"); return this.acceptance(accepted); }
    const worktree = join(this.config.childDirectory, digest(work.taskId), "repo");
    this.database.exec("BEGIN IMMEDIATE");
    try {
      if (this.load() >= this.config.slots) { this.database.exec("COMMIT"); return { taskId: work.taskId, accepted: false, state: "full", reason: "full" }; }
      this.database.prepare("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (?,?,?,?,'ACCEPTED',1,?)").run(work.taskId, hash, JSON.stringify(work), worktree, randomUUID());
      this.database.exec("COMMIT");
    } catch (error) { this.database.exec("ROLLBACK"); throw error; }
    this.event(work.taskId, { type: "accepted", fingerprint: hash });
    this.schedule(work.taskId);
    return this.acceptance(this.requireRow(work.taskId));
  }
  private verifyCheckout(row: Row): void {
    const work = this.work(row);
    if (realpathSync(row.worktree) !== row.worktree || !inside(realpathSync(this.config.childDirectory), row.worktree)) throw new Error("Worktree escaped child root");
    if (inspectedRepo(row.worktree) !== work.repo || git(row.worktree, ["rev-parse", "HEAD"]) !== work.sha || git(row.worktree, ["rev-parse", "--verify", `${work.ref}^{commit}`]) !== work.sha) throw new Error("Checkout identity mismatch");
  }
  private async launch(taskId: string): Promise<void> {
    let spawned = false;
    try {
      const row = this.requireRow(taskId); if (row.state !== "ACCEPTED" || row.detached || this.children.has(taskId)) return;
      const work = this.work(row); const repo = this.config.repositories[work.repo]!;
      if (row.childUid >= this.config.childUidBase + CHILD_UID_BASE) throw new Error("Reserved child UID range exhausted");
      if (work.deadline <= Date.now()) {
        this.state(taskId, "FAILED", "Approved deadline elapsed before launch", { launched: false, evidenceVerified: false });
        this.database.prepare("UPDATE tasks SET occupied=0 WHERE taskId=?").run(taskId);
        void this.heartbeat(); return;
      }
      const childRoot = join(this.config.childDirectory, digest(taskId));
      mkdirSync(childRoot, { recursive: true, mode: 0o700 });
      if (!existsSync(join(row.worktree, ".git"))) {
        git(repo, ["clone", "--no-hardlinks", "--no-checkout", repo, row.worktree]);
        git(row.worktree, ["remote", "set-url", "origin", git(repo, ["remote", "get-url", "origin"])]);
        git(row.worktree, ["checkout", "--detach", work.sha]);
        git(row.worktree, ["fetch", "--no-tags", repo, "+refs/heads/*:refs/heads/*"]);
      }
      chmodSync(row.worktree, 0o700); this.verifyCheckout(row);
      const sessions = join(childRoot, "sessions"); mkdirSync(sessions, { recursive: true, mode: 0o700 });
      const home = join(childRoot, "home");
      mkdirSync(home, { recursive: true, mode: 0o700 });
      cpSync(this.config.modelDirectory, home, { recursive: true, dereference: true });
      ownChildDirectory(childRoot, row.childUid); chmodSync(childRoot, 0o700);
      this.state(taskId, "LAUNCHING"); // Durable intent must precede spawn: a crash here is intentionally held.
      const process = spawn("/usr/bin/setpriv", [...isolationArgs(row.childUid), this.config.launchers[work.launcher]!,
        "--mode", "rpc", "--session-dir", sessions, "--no-extensions"], {
        cwd: row.worktree, env: childEnvironment(home), stdio: ["pipe", "pipe", "pipe"], detached: true,
      });
      spawned = true;
      const child: Child = { process, pending: new Map(), lost: false, completing: false, started: false, failed: false };
      this.children.set(taskId, child);
      if (process.pid) this.database.prepare("UPDATE tasks SET pid=? WHERE taskId=?").run(process.pid, taskId);
      const frames = new LfFrames();
      process.stdout.on("data", (chunk: Buffer) => {
        if (child.lost) return;
        try { for (const frame of frames.push(chunk)) this.frame(taskId, child, frame); }
        catch { this.hold(taskId, "invalid, truncated, or oversized host RPC output"); }
      });
      process.stdout.on("end", () => { try { frames.end(); } catch { this.hold(taskId, "truncated host RPC output"); } if (!child.completing) this.hold(taskId, "host stdio disconnected"); });
      process.stdout.on("error", () => this.hold(taskId, "host stdout error"));
      process.stdin.on("error", () => this.hold(taskId, "host stdin error"));
      // stderr is drained but never persisted: provider diagnostics may contain credentials.
      process.stderr.on("data", () => {});
      process.on("error", () => this.hold(taskId, "child launch failure or uncertainty"));
      process.on("close", (code, signal) => {
        const current = this.requireRow(taskId);
        for (const pending of child.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error("Child exited")); } child.pending.clear();
        if (!Terminal[current.state]) this.state(taskId, "FAILED", "owned child exited before confirmed completion", { exitCode: code, signal });
        this.database.prepare("UPDATE tasks SET occupied=0 WHERE taskId=?").run(taskId);
        this.children.delete(taskId); void this.heartbeat();
      });
      const response = await this.request(taskId, "get_state", undefined, 60000);
      const address = await this.readback(this.requireRow(taskId), response);
      this.database.prepare("UPDATE tasks SET address=?,state='READY',reason=NULL WHERE taskId=? AND state='LAUNCHING'").run(JSON.stringify(address), taskId);
      this.event(taskId, { type: "readback", address }); void this.heartbeat();
    } catch {
      if (spawned) this.hold(taskId, "launch/readback uncertain; no automatic relaunch");
      else { this.state(taskId, "FAILED", "allowlisted checkout preparation failed"); this.database.prepare("UPDATE tasks SET occupied=0 WHERE taskId=?").run(taskId); }
    }
  }
  private async readback(row: Row, response: Json, template?: Address): Promise<Address> {
    this.verifyCheckout(row);
    const work = this.work(row); const data = response.data;
    if (response.success !== true || !record(data) || typeof data.sessionId !== "string" || typeof data.sessionFile !== "string" || data.isStreaming !== false) throw new Error("Unverified durable host identity");
    const sessions = realpathSync(join(this.config.childDirectory, digest(row.taskId), "sessions"));
    if (!isAbsolute(data.sessionFile) || !inside(sessions, resolve(data.sessionFile))) throw new Error("Unexpected session path");
    const cwdResponse = await this.request(row.taskId, "bash", "pwd -P");
    const cwdData = cwdResponse.data;
    if (!record(cwdData) || cwdData.exitCode !== 0 || typeof cwdData.output !== "string" || cwdData.output.trim() !== row.worktree) throw new Error("Host cwd mismatch");
    // Native session identity is the actual addressable root worker, not a fabricated requested ID.
    return decode(Address, { messageId: template?.messageId ?? row.taskId, revision: template?.revision ?? 1, workerId: data.sessionId, sessionId: data.sessionId,
      incarnation: row.incarnation, repo: inspectedRepo(row.worktree), ref: work.ref, sha: git(row.worktree, ["rev-parse", "HEAD"]),
      scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline });
  }
  private request(taskId: string, type: string, message?: string, timeout = 30000, forcedId?: string): Promise<Json> {
    const child = this.children.get(taskId); if (!child || child.lost) return Promise.reject(new Error("Owned child stdio unavailable"));
    const id = forcedId ?? randomUUID(); const done = Promise.withResolvers<Json>();
    const timer = setTimeout(() => { child.pending.delete(id); done.reject(new Error("Host RPC timeout")); }, timeout);
    child.pending.set(id, { type, resolve: done.resolve, reject: done.reject, timer });
    child.process.stdin.write(`${JSON.stringify({ id, type, ...(message === undefined ? {} : type === "bash" ? { command: message } : { message }) })}\n`, (error) => {
      if (error) { clearTimeout(timer); child.pending.delete(id); done.reject(new Error("Host write uncertain")); }
    });
    return done.promise;
  }
  private boundedResponse(response: Json): unknown {
    if (response.command === "get_state" && record(response.data)) {
      const data = response.data;
      return { type: "response", id: response.id, command: "get_state", success: response.success,
        data: { sessionId: data.sessionId, sessionFile: data.sessionFile, isStreaming: data.isStreaming, isCompacting: data.isCompacting,
          isSettled: data.isSettled, messageCount: data.messageCount, pendingMessageCount: data.pendingMessageCount, queuedMessageCount: data.queuedMessageCount } };
    }
    return Buffer.byteLength(JSON.stringify(response)) < LIMIT - 1024 ? response : { type: "gap", reason: "host response exceeds endpoint bound", command: response.command };
  }
  private frame(taskId: string, child: Child, value: unknown): void {
    if (!record(value) || typeof value.type !== "string") throw new Error("Malformed RPC object");
    if (value.type === "rpc_chunk") throw new Error("Unnegotiated chunk protocol");
    if (value.type === "response") {
      if (typeof value.id !== "string" || typeof value.command !== "string" || typeof value.success !== "boolean") throw new Error("Malformed response");
      const pending = child.pending.get(value.id);
      if (pending) {
        if (pending.type !== value.command) throw new Error("Response command mismatch");
        clearTimeout(pending.timer); child.pending.delete(value.id);
        if (value.success) pending.resolve(value); else pending.reject(new Error("Host command rejected"));
      }
      this.event(taskId, { type: "host_response", id: value.id, command: value.command, success: value.success }); return;
    }
    this.event(taskId, { type: value.type === "readback" ? "host_event" : value.type, status: value.status });
    const row = this.requireRow(taskId); if (!row.promptId || child.lost) return;
    if (value.type === "agent_start") child.started = true;
    if (value.type === "message_end" && record(value.message) && ["error", "aborted"].includes(String(value.message.stopReason))) child.failed = true;
    if (this.work(row).launcher === "omp") {
      if (value.type === "prompt_result" && value.id === row.promptId) {
        if (value.status !== "completed") child.failed = true;
        if (value.sessionSettled === true) void this.finish(taskId, child);
      } else if (value.type === "session_settled" && child.started) void this.finish(taskId, child);
    } else if ((value.type === "agent_end" || value.type === "agent_settled") && child.started) void this.finish(taskId, child);
  }
  private async finish(taskId: string, child: Child): Promise<void> {
    if (child.completing || child.lost) return; child.completing = true;
    try {
      const response = await this.request(taskId, "get_state"); const data = response.data;
      if (!record(data) || data.isStreaming !== false || data.isCompacting !== false || data.hasPendingAsyncWork === true || data.isSettled === false || Number(data.pendingMessageCount ?? data.queuedMessageCount ?? 0) !== 0) { child.completing = false; return; }
      const row = this.requireRow(taskId);
      if (!row.address || data.sessionId !== decode(Address, JSON.parse(row.address)).sessionId) throw new Error("Host identity changed");
      this.state(taskId, child.failed ? "FAILED" : "SUCCEEDED", null, { hostSettled: true, evidenceVerified: false });
      child.process.stdin.end(); void this.heartbeat();
    } catch { this.hold(taskId, "completion identity/outcome uncertain"); }
  }
  private async release(input: typeof ReleaseInput.Type): Promise<{ released: true }> {
    const row = this.requireRow(input.taskId); const work = this.work(row);
    if (!row.address || fingerprint(input.address) !== fingerprint(JSON.parse(row.address)) || input.prompt !== work.prompt) throw new HttpError(409, "Release address or approved prompt mismatch");
    if (row.promptId) {
      if (row.state === "RUNNING" || Terminal[row.state]) return { released: true };
      throw new HttpError(409, "Prompt delivery uncertain; never replay");
    }
    if (row.state !== "READY") throw new HttpError(409, "Child not ready");
    if (row.detached) throw new HttpError(403, "Task no longer assigned");
    await this.brokerTask(work, input.address);
    const response = await this.request(row.taskId, "get_state"); const actual = await this.readback(this.requireRow(row.taskId), response, decode(Address, JSON.parse(row.address)));
    if (fingerprint(actual) !== fingerprint(input.address)) throw new HttpError(409, "Actual child address changed");
    // Native readback awaited RPCs; recheck the ledger immediately before reserving prompt intent.
    await this.brokerTask(work, input.address);
    if (input.address.deadline <= Date.now()) throw new HttpError(409, "Approved prompt deadline elapsed");
    const promptId = randomUUID();
    const changed = this.database.prepare("UPDATE tasks SET state='PROMPT_INTENT',promptId=? WHERE taskId=? AND state='READY' AND promptId IS NULL").run(promptId, row.taskId);
    if (changed.changes !== 1) throw new HttpError(409, "Prompt already reserved");
    this.event(row.taskId, { type: "prompt_intent", id: promptId, digest: digest(work.prompt) });
    try {
      await this.request(row.taskId, "prompt", work.prompt, 60000, promptId);
      this.database.prepare("UPDATE tasks SET state='RUNNING' WHERE taskId=? AND state='PROMPT_INTENT'").run(row.taskId);
      void this.heartbeat(); return { released: true };
    } catch { this.hold(row.taskId, "prompt send/admission uncertain; never replay"); throw new HttpError(409, "Prompt send uncertain; held"); }
  }
  private async heartbeat(): Promise<void> {
    if (this.closed || this.heartbeatBusy) return; this.heartbeatBusy = true;
    try {
      const rows = this.database.prepare("SELECT ?+rowid AS childUid,* FROM tasks WHERE detached=0 ORDER BY taskId").all(this.config.childUidBase) as unknown as Row[];
      // Advertisements are bounded; absence is never represented as not-accepted.
      const journal: Acceptance[] = []; let size = 1024;
      for (const row of rows) { const item = this.acceptance(row); const bytes = Buffer.byteLength(JSON.stringify(item)); if (size + bytes > LIMIT - 1024) break; journal.push(item); size += bytes; }
      const load = this.load(); await rpc(this.config.brokerUrl, this.token, "runner.heartbeat", { runnerId: this.config.id, slots: this.config.slots, load, journal }, 4000);
      if (!this.reportsBusy) { this.reportsBusy = true; void this.reports(rows); }
    } catch { /* Broker unavailability never aborts child work or retries uncertain prompt sends. */ }
    finally { this.heartbeatBusy = false; }
  }
  private async reports(rows: Row[]): Promise<void> {
    try {
      for (const row of rows) {
        if (row.detached) continue;
        try {
          if (row.address && row.state === "READY") await rpc(this.config.brokerUrl, this.token, "runner.readback", { runnerId: this.config.id, taskId: row.taskId, address: JSON.parse(row.address) });
          if (["RUNNING", "SUCCEEDED", "FAILED", "HELD"].includes(row.state)) await rpc(this.config.brokerUrl, this.token, "runner.report", { runnerId: this.config.id, taskId: row.taskId, state: row.state, result: row.result ? JSON.parse(row.result) : {} });
        } catch (error) {
          if (!(error instanceof Error) || !error.message.startsWith("RPC 403:")) continue;
          // Only an authenticated ledger denial confirms obsolete ownership; outages remain retryable.
          try { await rpc(this.config.brokerUrl, this.token, "task.get", { taskId: row.taskId }); }
          catch (bindingError) {
            if (!(bindingError instanceof Error) || !bindingError.message.startsWith("RPC 403:")) continue;
            if (!Terminal[row.state]) this.hold(row.taskId, "broker assignment withdrawn; no replay");
            this.database.prepare("UPDATE tasks SET detached=1,occupied=CASE WHEN state='HELD' AND pid IS NULL THEN 0 ELSE occupied END WHERE taskId=?").run(row.taskId);
          }
        }
      }
    } finally { this.reportsBusy = false; }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const path = process.env.COORDINATOR_CONFIG;
    if (!path || !isAbsolute(path)) throw new Error("COORDINATOR_CONFIG must be absolute");
    const runner = new Runner(decode(Config, JSON.parse(privateFile(path))));
    await runner.start();
    console.log(JSON.stringify({ service: "coordinator-runner", status: "ready", runnerId: runner.config.id }));
  } catch { console.error(JSON.stringify({ service: "coordinator-runner", status: "startup-failed" })); process.exitCode = 1; }
}
