import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import * as PersistedQueue from "effect/persistence/PersistedQueue";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { DatabaseSync, backup } from "node:sqlite";
import { chmodSync, copyFileSync, mkdirSync, readdirSync, renameSync, rmSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import type { Server } from "node:http";
import type { SqlError } from "effect/sql/SqlError";
import { Address, Work, Id, Repo, Text, Sha, checkToken, decode, fingerprint, privateFile, rpc, type BrokerConfig, type Principal, type Task } from "./protocol.ts";
import { HttpError, serveRpc } from "./http.ts";
import { DeploymentEvidence, LocalEvidence, ReviewEvidence, pollCi } from "./evidence.ts";

const Session = Schema.Struct({ sessionId: Id, incarnation: Id, machineId: Id, host: Schema.Literals(["pi", "omp"]), repo: Repo, workerId: Schema.optional(Id) });
const SessionKey = Schema.Struct({ sessionId: Id, incarnation: Id });
const TaskKey = Schema.Struct({ taskId: Id });
const AcceptanceResponse = Schema.Struct({ taskId: Id, accepted: Schema.Boolean, state: Schema.optional(Text), worktree: Schema.optional(Text), address: Schema.optional(Address), reason: Schema.optional(Text) });
const ArchivePayload = Schema.Union([
  Schema.Struct({ id: Id, parentId: Schema.NullOr(Id), type: Schema.String.check(Schema.isPattern(/^[a-z_]{1,40}$/)), role: Schema.optional(Schema.Literals(["user", "assistant", "toolResult"])), contentOmitted: Schema.Literal(true) }),
  Schema.Struct({ coverageOnly: Schema.Literal(true), gaps: Schema.Array(Text).check(Schema.isMaxLength(32)) }),
]);
const Role = Schema.Literals(["coordinator", "session", "runner", "archive-writer", "reviewer", "admin"]);
const Config = Schema.Struct({
  database: Text, host: Text, port: Schema.Int, backupDirectory: Text,
  archiveRetentionMs: Schema.Number, backupRetentionMs: Schema.Number, githubTokenEnv: Schema.optional(Text),
  repositoryIds: Schema.Record(Repo, Schema.Int.check(Schema.isGreaterThan(0))),
  principals: Schema.Array(Schema.Struct({ id: Id, owner: Id, tokenHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)), roles: Schema.Array(Role), repos: Schema.Array(Repo), runnerUrl: Schema.optional(Text), controlTokenHash: Schema.optional(Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))) })),
});
interface LiveSession extends typeofSession { owner: string; principal: string; verified: boolean; liveUntil: number; polledAt: number }
type typeofSession = typeof Session.Type;
interface LiveRunner { runnerId: string; owner: string; slots: number; load: number; liveUntil: number }
interface Delivery { messageId: string; incarnation: string; payload: unknown; expiresAt: number }
function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new HttpError(400, "Object expected");
  return value as Record<string, unknown>;
}
const exact = (a: unknown, b: unknown): boolean => fingerprint(a) === fingerprint(b);

export class Broker {
  readonly db: DatabaseSync;
  readonly sessions = new Map<string, LiveSession>();
  readonly runners = new Map<string, LiveRunner>();
  readonly config: BrokerConfig;
  private queue?: PersistedQueue.PersistedQueue<string>;
  private runtime?: ManagedRuntime.ManagedRuntime<PersistedQueue.PersistedQueueFactory, SqlError>;
  private server?: Server;
  private stopping = false;
  private consumers: Promise<unknown>[] = [];
  private timer?: NodeJS.Timeout;
  private backingUp = false;
  constructor(config: BrokerConfig) {
    this.config = decode(Config, config);
    if (config.port < 0 || config.port > 65535 || config.archiveRetentionMs <= 0 || config.backupRetentionMs <= 0) throw new Error("Invalid broker limits");
    if (new Set(config.principals.map((p) => p.id)).size !== config.principals.length || new Set(config.principals.map((p) => p.tokenHash)).size !== config.principals.length) throw new Error("Distinct enrolled principal credentials required");
    const hashes = config.principals.flatMap((principal) => principal.controlTokenHash ? [principal.tokenHash, principal.controlTokenHash] : [principal.tokenHash]);
    if (new Set(hashes).size !== hashes.length || config.principals.some((principal) => principal.roles.includes("runner") && !principal.controlTokenHash)) throw new Error("Distinct runner enrollment and broker-only control credentials required");
    for (const path of [dirname(config.database), config.backupDirectory]) { mkdirSync(path, { recursive: true, mode: 0o700 }); chmodSync(path, 0o700); }
    this.db = new DatabaseSync(config.database);
    chmodSync(config.database, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS peers (id TEXT PRIMARY KEY, owner TEXT NOT NULL, sender TEXT NOT NULL, session TEXT NOT NULL, incarnation TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL, expires REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS consents (owner TEXT NOT NULL, session TEXT NOT NULL, enabled INTEGER NOT NULL, participants TEXT NOT NULL, PRIMARY KEY(owner,session));
      CREATE TABLE IF NOT EXISTS participant_consents (owner TEXT NOT NULL, session TEXT NOT NULL, participant TEXT NOT NULL, enabled INTEGER NOT NULL, PRIMARY KEY(owner,session,participant));
      CREATE TABLE IF NOT EXISTS archive (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, session TEXT NOT NULL, entry TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL, gaps TEXT NOT NULL, created REAL NOT NULL, UNIQUE(owner,session,entry,digest));
      CREATE TABLE IF NOT EXISTS evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL, kind TEXT NOT NULL, sha TEXT NOT NULL, principal TEXT NOT NULL, data TEXT NOT NULL, observed REAL NOT NULL);`);
    for (const row of this.db.prepare("SELECT data FROM sessions").all()) {
      const session = JSON.parse(String(row.data)) as LiveSession;
      session.verified = false; session.liveUntil = 0; session.polledAt = 0;
      this.sessions.set(session.sessionId, session);
    }
    for (const file of readdirSync(config.backupDirectory)) if (file.endsWith(".partial")) rmSync(join(config.backupDirectory, file));
  }
  private createRuntime() {
    const sqlite = SqliteClient.layer({ filename: this.config.database });
    const storage = PersistedQueue.layerStoreSql({ pollInterval: "200 millis", lockRefreshInterval: "1 second", lockExpiration: "5 seconds" }).pipe(Layer.provide(sqlite));
    return ManagedRuntime.make(PersistedQueue.layer.pipe(Layer.provide(storage)));
  }
  authorize(token: string): Principal {
    const principal = this.config.principals.find((p) => checkToken(token, p.tokenHash));
    if (!principal) throw new HttpError(401, "Invalid credential");
    return principal;
  }
  private role(principal: Principal, ...roles: string[]): void {
    if (!roles.some((role) => principal.roles.includes(role))) throw new HttpError(403, "Operation not authorized");
  }
  private repo(principal: Principal, repo: string): void {
    if (!principal.repos.includes(repo)) throw new HttpError(403, "Repository not authorized");
  }
  private task(principal: Principal, id: string): Task {
    const row = this.db.prepare("SELECT data FROM tasks WHERE id=?").get(id);
    if (!row) throw new HttpError(404, "Task not found");
    const task = JSON.parse(String(row.data)) as Task;
    if (task.owner !== principal.owner) throw new HttpError(403, "Owner mismatch");
    this.repo(principal, task.request.repo);
    return task;
  }
  private tasks(): Task[] { return this.db.prepare("SELECT data FROM tasks").all().map((r) => JSON.parse(String(r.data)) as Task); }
  private save(task: Task): void {
    task.updatedAt = Date.now();
    this.db.prepare("INSERT INTO tasks VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data").run(task.taskId, JSON.stringify(task));
  }
  private session(principal: Principal, key: typeof SessionKey.Type, live = true): LiveSession {
    const session = this.sessions.get(key.sessionId);
    if (!session || session.principal !== principal.id || session.owner !== principal.owner || session.incarnation !== key.incarnation) throw new HttpError(403, "Session binding mismatch");
    if (live && (!session.verified || session.liveUntil <= Date.now())) throw new HttpError(409, "Recipient is not verified live");
    return session;
  }
  private recipient(principal: Principal, key: typeof SessionKey.Type): LiveSession {
    const session = this.sessions.get(key.sessionId);
    if (!session || session.owner !== principal.owner || session.incarnation !== key.incarnation || !session.verified || session.liveUntil <= Date.now() || session.polledAt < Date.now() - 10000) throw new HttpError(409, "Recipient is not verified live");
    this.repo(principal, session.repo);
    return session;
  }
  private async offer(task: Task): Promise<void> {
    if (task.state !== "QUEUED" || task.offerDecision === "accepted") return;
    if (task.offerDecision === "pending" && (task.offerExpires ?? 0) > Date.now()) return;
    if (task.offerDecision === "pending") { task.offerDecision = "expired"; this.save(task); }
    await this.runtime!.runPromise(this.queue!.offer(task.taskId, { id: task.taskId }));
  }
  async start(): Promise<Server> {
    this.runtime = this.createRuntime();
    this.queue = await this.runtime.runPromise(PersistedQueue.make({ name: "coordinator.dispatch", schema: Id, maxAttempts: Number.MAX_SAFE_INTEGER }));
    for (const task of this.tasks()) await this.offer(task);
    this.server = await serveRpc({ host: this.config.host, port: this.config.port, authorize: (token) => this.authorize(token), handle: (principal, method, params) => this.handle(principal as Principal, method, params) });
    // ponytail: four bounded claim handlers; increase only when independent runner count requires it.
    for (let i = 0; i < 4; i++) this.consumers.push(this.consume());
    this.timer = setInterval(() => { void this.maintain().catch(() => console.error("broker maintenance failed")); }, 1000);
    await this.snapshot();
    return this.server;
  }
  private async consume(): Promise<void> {
    while (!this.stopping) {
      try { await this.runtime!.runPromise(this.queue!.take((id) => Effect.tryPromise({ try: () => this.assign(id), catch: () => new Error("Dispatch retry") }))); }
      catch { if (!this.stopping) await delay(500); }
    }
  }
  private runnerPrincipal(task: Task): Principal | undefined { return this.config.principals.find((p) => p.id === task.runnerId && p.owner === task.owner && p.roles.includes("runner")); }
  private async assign(id: string): Promise<void> {
    while (!this.stopping) {
      let task = this.tasks().find((t) => t.taskId === id);
      if (!task || ["SUCCEEDED", "FAILED", "ACCEPTED", "RUNNING"].includes(task.state) || task.offerDecision === "accepted") return;
      if (task.runnerId && ["ASSIGNING", "HELD"].includes(task.state)) {
        const principal = this.runnerPrincipal(task);
        if (!principal?.runnerUrl) { task.state = "HELD"; task.reason = "Assigned runner no longer enrolled"; this.save(task); await delay(1000); continue; }
        try {
          const result = decode(AcceptanceResponse, await rpc(principal.runnerUrl, this.runnerToken(principal), "journal", { taskId: id }));
          task = this.tasks().find((t) => t.taskId === id)!;
          if (task.runnerId !== principal.id || ["RUNNING", "SUCCEEDED", "FAILED"].includes(task.state)) return;
          if (result.taskId !== id) throw new HttpError(409, "Runner journal task mismatch");
          if (result.accepted) { task.state = ["HELD", "PROMPT_INTENT"].includes(result.state ?? "") ? "HELD" : "ACCEPTED"; task.reason = result.reason; this.save(task); return; }
          if (result.state === "not-accepted") { task.state = "QUEUED"; delete task.runnerId; this.save(task); }
          else { task.state = "HELD"; task.reason = "Runner acceptance unknown; reassignment blocked"; this.save(task); }
        } catch { task.state = "HELD"; task.reason = "Runner acceptance unknown; reassignment blocked"; this.save(task); }
        await delay(1000); continue;
      }
      if (task.state === "HELD") { await delay(1000); continue; }
      const candidate = [...this.runners.values()].find((r) => r.owner === task.owner && r.liveUntil > Date.now() && r.load < r.slots && this.config.principals.some((p) => p.id === r.runnerId && p.repos.includes(task.request.repo) && p.runnerUrl));
      if (!candidate) { await delay(500); continue; }
      const principal = this.config.principals.find((p) => p.id === candidate.runnerId)!;
      delete task.peerSession; delete task.peerIncarnation; delete task.offerExpires;
      delete task.address; delete task.readback; delete task.confirmedRevision;
      task.state = "ASSIGNING"; task.runnerId = principal.id; this.save(task);
      candidate.load++;
      try {
        const result = decode(AcceptanceResponse, await rpc(principal.runnerUrl!, this.runnerToken(principal), "acceptTask", task.request, 15000));
        if (result.taskId !== id) throw new HttpError(409, "Runner acceptance task mismatch");
        const current = this.tasks().find((t) => t.taskId === id)!;
        if (current.runnerId !== principal.id || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
        if (result.accepted) { if (current.state === "ASSIGNING") current.state = "ACCEPTED"; this.save(current); return; }
        if (result.reason === "full" || result.state === "not-accepted") { current.state = "QUEUED"; delete current.runnerId; this.save(current); }
        else { current.state = "HELD"; current.reason = result.reason ?? "Runner declined unsafe material"; this.save(current); }
      } catch {
        const current = this.tasks().find((t) => t.taskId === id)!;
        if (current.runnerId !== principal.id || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
        current.state = "HELD"; current.reason = "Runner acceptance unknown; reassignment blocked"; this.save(current);
      }
      await delay(500);
    }
    throw new Error("Broker stopping before queue acknowledgement");
  }
  private runnerToken(principal: Principal): string {
    // Outbound secrets are separate files, never in the ledger/config/response or process arguments.
    const directory = process.env.COORDINATOR_RUNNER_TOKEN_DIRECTORY;
    if (!directory) throw new Error("Runner credential directory missing");
    const token = privateFile(join(directory, principal.id));
    if (!principal.controlTokenHash || !checkToken(token, principal.controlTokenHash)) throw new Error("Runner control credential rotation mismatch");
    return token;
  }
  private appendEvidence(task: Task, principal: Principal, kind: string, sha: string, data: unknown): void {
    this.db.prepare("INSERT INTO evidence(task,kind,sha,principal,data,observed) VALUES (?,?,?,?,?,?)").run(task.taskId, kind, sha, principal.id, JSON.stringify(data), Date.now());
  }
  async handle(principal: Principal, method: string, params: unknown): Promise<unknown> {
    if (method === "session.heartbeat") {
      this.role(principal, "session"); const input = decode(Session, params); this.repo(principal, input.repo);
      const existing = this.sessions.get(input.sessionId);
      if (existing && (existing.principal !== principal.id || existing.owner !== principal.owner)) throw new HttpError(403, "Session already enrolled by another principal");
      if (existing && existing.incarnation !== input.incarnation && existing.verified && existing.liveUntil > Date.now()) throw new HttpError(409, "Prior session incarnation still live");
      const session: LiveSession = { ...input, owner: principal.owner, principal: principal.id, verified: true, liveUntil: Date.now() + 30000, polledAt: existing?.incarnation === input.incarnation ? existing.polledAt : 0 };
      this.sessions.set(input.sessionId, session);
      this.db.prepare("INSERT INTO sessions VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data").run(input.sessionId, JSON.stringify(session));
      return { liveUntil: session.liveUntil };
    }
    if (method === "session.poll") {
      this.role(principal, "session"); const input = decode(SessionKey, params); const session = this.session(principal, input); session.polledAt = Date.now();
      const messages: Delivery[] = this.db.prepare("SELECT id,incarnation,payload,expires FROM peers WHERE owner=? AND session=? AND incarnation=? AND expires>? ORDER BY expires LIMIT 8").all(principal.owner, input.sessionId, input.incarnation, Date.now()).map((r) => ({ messageId: String(r.id), incarnation: String(r.incarnation), payload: JSON.parse(String(r.payload)), expiresAt: Number(r.expires) }));
      const bound = this.tasks().filter((task) => task.owner === principal.owner && task.peerSession === input.sessionId && task.peerIncarnation === input.incarnation);
      const result: { messages: Delivery[]; offers: Task[]; releases: Task[]; archiveEnabled: boolean } = { messages: [], offers: [], releases: [], archiveEnabled: Boolean(this.db.prepare("SELECT enabled FROM consents WHERE owner=? AND session=?").get(principal.owner, input.sessionId)?.enabled) };
      for (const message of messages) { const next = { ...result, messages: [...result.messages, message] }; if (Buffer.byteLength(JSON.stringify(next)) < 60000) result.messages.push(message); }
      for (const task of bound) {
        const key = !task.runnerId && task.offerDecision === "pending" && (task.offerExpires ?? 0) > Date.now() ? "offers"
          : !task.runnerId && task.offerDecision === "accepted" && task.address && session.workerId === task.address.workerId && session.repo === task.address.repo && task.address.deadline > Date.now() && task.confirmedRevision === task.address.revision && task.disposition === "ready" && task.state === "ACCEPTED" ? "releases" : undefined;
        if (key && Buffer.byteLength(JSON.stringify({ ...result, [key]: [...result[key], task] })) < 60000) result[key].push(task);
      }
      return result;
    }
    if (method === "session.stop") {
      this.role(principal, "session"); const input = decode(SessionKey, params); const session = this.session(principal, input, false); session.verified = false; session.liveUntil = 0;
      this.db.prepare("DELETE FROM peers WHERE owner=? AND session=? AND incarnation=?").run(principal.owner, input.sessionId, input.incarnation);
      this.db.prepare("DELETE FROM sessions WHERE id=?").run(input.sessionId); return { stopped: true };
    }
    if (method === "peer.send") {
      this.role(principal, "session", "coordinator"); const input = decode(Schema.Struct({ messageId: Id, sessionId: Id, incarnation: Id, payload: Schema.Unknown, ttlMs: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 60000 })) }), params);
      this.recipient(principal, input);
      if (Buffer.byteLength(JSON.stringify(input.payload)) > 8000) throw new HttpError(413, "Peer payload too large");
      const hash = fingerprint({ ...input, owner: principal.owner, sender: principal.id }); const prior = this.db.prepare("SELECT fingerprint FROM peers WHERE id=?").get(input.messageId);
      if (prior && prior.fingerprint !== hash) throw new HttpError(409, "Message ID conflict");
      if (!prior) this.db.prepare("INSERT INTO peers VALUES (?,?,?,?,?,?,?,?)").run(input.messageId, principal.owner, principal.id, input.sessionId, input.incarnation, JSON.stringify(input.payload), hash, Date.now() + input.ttlMs);
      return { delivered: false, committed: true, messageId: input.messageId };
    }
    if (method === "peer.ack") {
      this.role(principal, "session"); const input = decode(Schema.Struct({ ...SessionKey.fields, messageId: Id }), params); this.session(principal, input);
      this.db.prepare("DELETE FROM peers WHERE id=? AND owner=? AND session=? AND incarnation=?").run(input.messageId, principal.owner, input.sessionId, input.incarnation); return { handled: true };
    }
    if (method === "peer.reply") {
      this.role(principal, "session"); const input = decode(Schema.Struct({ ...SessionKey.fields, taskId: Id, decision: Schema.Literals(["accepted", "declined"]) }), params); const session = this.session(principal, input);
      const task = this.task(principal, input.taskId);
      if (task.peerSession !== input.sessionId || task.peerIncarnation !== input.incarnation || task.offerDecision !== "pending" || (task.offerExpires ?? 0) <= Date.now()) throw new HttpError(409, "Offer expired or binding mismatch");
      if (session.workerId !== task.request.workerId || session.repo !== task.request.repo || task.runnerId) throw new HttpError(409, "Offer worker/repository binding changed");
      task.offerDecision = input.decision; if (input.decision === "accepted") task.state = "ACCEPTED"; this.save(task); if (input.decision === "declined") await this.offer(task); return task;
    }
    if (method === "task.submit") {
      this.role(principal, "coordinator"); const work = decode(Work, params); this.repo(principal, work.repo);
      if (Buffer.byteLength(JSON.stringify(work)) > 28000) throw new HttpError(413, "Approved work exceeds delivery budget");
      const prior = this.db.prepare("SELECT data FROM tasks WHERE id=?").get(work.taskId);
      if (prior) { const task = this.task(principal, work.taskId); if (task.fingerprint !== fingerprint(work)) throw new HttpError(409, "Task ID fingerprint conflict"); return task; }
      if (work.deadline <= Date.now() || !work.prompt || !work.ref || !work.scope || !work.requiredLanes.length) throw new HttpError(400, "Approved bounded scope, deadline, ref and explicit CI lanes required");
      if (this.config.repositoryIds[work.repo] !== work.githubRepoId) throw new HttpError(409, "Inspected enrollment repository ID mismatch");
      const peer = work.bypassPeer ? undefined : [...this.sessions.values()].find((session) => session.owner === principal.owner && session.repo === work.repo && session.workerId === work.workerId && session.verified && session.liveUntil > Date.now() && session.polledAt >= Date.now() - 10000);
      const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: fingerprint(work), request: work, state: "QUEUED", createdAt: Date.now(), updatedAt: Date.now(), offerDecision: work.bypassPeer ? "bypass" : peer ? "pending" : "expired", ...(peer ? { peerSession: peer.sessionId, peerIncarnation: peer.incarnation, offerExpires: Date.now() + 15000 } : {}) };
      this.save(task); await this.offer(task); return task;
    }
    if (["task.get", "task.attach", "task.control"].includes(method)) {
      this.role(principal, "coordinator", "session", "reviewer", "runner"); const raw = object(params); const task = this.task(principal, decode(Id, raw.taskId));
      if (principal.roles.includes("runner") && task.runnerId !== principal.id) throw new HttpError(403, "Assigned runner mismatch");
      if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && !principal.roles.includes("reviewer")) { const recipient = this.sessions.get(task.peerSession ?? ""); if (recipient?.principal !== principal.id) throw new HttpError(403, "Task is not addressed to this session"); }
      if (method === "task.get") {
        decode(TaskKey, params);
        const target = task.evidenceTarget ?? task.request;
        const policy = fingerprint({ ref: target.ref, requiredLanes: [...target.requiredLanes].sort() });
        const observations = this.db.prepare("SELECT kind,sha,principal,data,observed FROM evidence WHERE id IN (SELECT MAX(id) FROM evidence WHERE task=? GROUP BY kind) ORDER BY id DESC").all(task.taskId).map((row) => {
          const data: unknown = JSON.parse(String(row.data));
          return { kind: row.kind, sha: row.sha, principal: row.principal, data, observedAt: row.observed, stale: row.sha !== target.sha || (row.kind === "ci" && object(data).policy !== policy) };
        });
        const evidence: unknown[] = [];
        for (const observation of observations) if (Buffer.byteLength(JSON.stringify({ ...task, evidence: [...evidence, observation] })) < 60000) evidence.push(observation);
        return { ...task, evidence };
      }
      if (!task.runnerId) throw new HttpError(409, "Only runner-owned children are attachable");
      const runner = this.runnerPrincipal(task); if (!runner?.runnerUrl) throw new HttpError(409, "Runner unavailable");
      if (method === "task.attach") { this.role(principal, "coordinator"); const input = decode(Schema.Struct({ taskId: Id, cursor: Schema.optional(Schema.Int) }), params); return rpc(runner.runnerUrl, this.runnerToken(runner), "inspect", input); }
      this.role(principal, "coordinator"); const input = decode(Schema.Struct({ taskId: Id, type: Schema.Literals(["get_state", "get_messages", "abort"]) }), params); return rpc(runner.runnerUrl, this.runnerToken(runner), "command", input);
    }
    if (method === "task.instruction") {
      this.role(principal, "coordinator");
      const input = decode(Schema.Struct({ taskId: Id, address: Address, prompt: Text, approved: Schema.Literal(true) }), params);
      const task = this.task(principal, input.taskId);
      if (task.runnerId) throw new HttpError(409, "Durably accepted launch material is immutable; later native instructions require a reconciled handoff");
      if (task.offerDecision !== "accepted") throw new HttpError(409, "Native peer must explicitly accept before later instructions");
      if (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)) throw new HttpError(409, "Prior native release is live or uncertain; reconcile before another instruction");
      if (!task.address || input.address.revision <= task.address.revision || input.address.messageId === task.address.messageId || input.address.repo !== task.request.repo || input.address.workerId !== task.address.workerId || input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation || input.address.deadline <= Date.now()) throw new HttpError(409, "New approved message/revision and exact current recipient required");
      this.recipient(principal, input.address);
      task.instruction = { prompt: input.prompt, address: input.address }; task.address = input.address;
      delete task.readback; delete task.confirmedRevision; task.disposition = "hold"; task.reason = "Later instruction awaits new readback"; task.state = "ACCEPTED";
      if (Buffer.byteLength(JSON.stringify(task)) > 50000) throw new HttpError(413, "Addressed task exceeds delivery budget");
      this.save(task); return task;
    }
    if (method === "task.address") {
      this.role(principal, "coordinator"); const input = decode(Schema.Struct({ taskId: Id, address: Address }), params); const task = this.task(principal, input.taskId); const approved = task.instruction?.address ?? task.request;
      if (!task.runnerId) {
        if (task.offerDecision !== "accepted" || !task.peerSession || !task.peerIncarnation) throw new HttpError(409, "Native address requires accepted peer ownership");
        const recipient = this.recipient(principal, input.address);
        if (recipient.workerId !== input.address.workerId || recipient.repo !== input.address.repo) throw new HttpError(409, "Native recipient worker/repository binding changed");
      }
      if (input.address.repo !== approved.repo || input.address.sha !== approved.sha || input.address.ref !== approved.ref || input.address.workerId !== (task.address?.workerId ?? approved.workerId)) throw new HttpError(409, "Address does not match approved repo/ref/SHA/current worker");
      if (input.address.scope !== approved.scope || input.address.restrictions !== approved.restrictions || !exact(input.address.expectedEvidence, approved.expectedEvidence) || input.address.deadline !== approved.deadline) throw new HttpError(409, "Address must preserve approved consequential details");
      if (input.address.revision <= (task.address?.revision ?? 0)) { if (exact(input.address, task.address)) return task; throw new HttpError(409, "Address revision must increase"); }
      if (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)) throw new HttpError(409, "Confirmed release cannot be reset; reconcile its result first");
      if (task.runnerId && task.address && (input.address.sessionId !== task.address.sessionId || input.address.incarnation !== task.address.incarnation)) throw new HttpError(409, "Use reconciled handoff for a new session");
      if (!task.runnerId && task.peerSession && (input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation)) throw new HttpError(409, "Peer address mismatch");
      task.address = input.address; delete task.readback; delete task.confirmedRevision; task.disposition = "hold"; task.reason = "Awaiting matching readback";
      if (task.instruction) task.instruction.address = input.address;
      this.save(task);
      if (task.runnerId) {
        const runner = this.runnerPrincipal(task);
        if (!runner?.runnerUrl) throw new HttpError(409, "Runner unavailable for corrected readback");
        const readback = decode(Address, await rpc(runner.runnerUrl, this.runnerToken(runner), "prepare", { taskId: task.taskId, address: input.address }));
        if (!exact(readback, input.address)) throw new HttpError(409, "Corrected runner readback mismatch");
        task.readback = readback; task.disposition = "ready"; delete task.reason; this.save(task);
      }
      return task;
    }
    if (method === "task.readback") {
      this.role(principal, "session"); const input = decode(Schema.Struct({ taskId: Id, address: Address, disposition: Schema.Literals(["ready", "unable", "clarify", "hold"]), reason: Schema.optional(Text), nextCheck: Schema.optional(Schema.Number) }), params); const task = this.task(principal, input.taskId);
      const session = this.session(principal, input.address);
      if (task.runnerId || task.offerDecision !== "accepted" || session.workerId !== input.address.workerId || session.repo !== input.address.repo) throw new HttpError(409, "Native readback requires accepted bound peer ownership, never runner-owned work");
      if (task.peerSession !== input.address.sessionId || task.peerIncarnation !== input.address.incarnation) throw new HttpError(403, "Task/session binding mismatch");
      if (!task.address || !exact(task.address, input.address)) throw new HttpError(409, "Readback mismatch; correct address and read back again");
      if (task.confirmedRevision !== undefined) { if (input.disposition === "ready" && exact(task.readback, input.address)) return task; throw new HttpError(409, "Confirmed release cannot be reset by readback"); }
      if (input.disposition !== "ready" && !input.reason) throw new HttpError(400, "Disposition reason required");
      if (input.disposition === "hold" && !input.nextCheck) throw new HttpError(400, "Hold next-check required");
      task.readback = input.address; task.disposition = input.disposition; task.reason = input.reason; task.nextCheck = input.nextCheck; delete task.confirmedRevision; this.save(task); return task;
    }
    if (method === "task.confirm") {
      this.role(principal, "coordinator"); const input = decode(Schema.Struct({ taskId: Id, revision: Schema.Int }), params); const task = this.task(principal, input.taskId);
      if (!task.runnerId) {
        if (task.offerDecision !== "accepted" || !task.address || task.peerSession !== task.address.sessionId || task.peerIncarnation !== task.address.incarnation) throw new HttpError(409, "Native confirmation requires accepted peer ownership");
        const recipient = this.recipient(principal, task.address);
        if (recipient.workerId !== task.address.workerId || recipient.repo !== task.address.repo) throw new HttpError(409, "Native confirmation worker/repository binding changed");
      }
      if (!task.address || task.address.revision !== input.revision || !task.readback || !exact(task.address, task.readback) || task.disposition !== "ready" || task.address.deadline <= Date.now()) throw new HttpError(409, "Confirmation requires current ready matching readback before deadline");
      if (task.confirmedRevision === input.revision) return task;
      task.confirmedRevision = input.revision; this.save(task);
      if (task.runnerId) {
        const runner = this.runnerPrincipal(task);
        try {
          if (!runner?.runnerUrl) throw new HttpError(409, "Runner unavailable");
          await rpc(runner.runnerUrl, this.runnerToken(runner), "release", { taskId: task.taskId, address: task.address, prompt: task.request.prompt }, 15000);
        } catch {
          task.state = "HELD"; task.reason = "Prompt delivery uncertain; inspect assigned runner journal, never replay automatically"; this.save(task);
          throw new HttpError(409, task.reason);
        }
      }
      return task;
    }
    if (method === "task.handoff") {
      this.role(principal, "coordinator"); const input = decode(Schema.Struct({ taskId: Id, oldSessionId: Id, oldIncarnation: Id, newSessionId: Id, newIncarnation: Id, revision: Schema.Int }), params); const task = this.task(principal, input.taskId);
      if (!task.address || task.address.sessionId !== input.oldSessionId || task.address.incarnation !== input.oldIncarnation || input.revision <= task.address.revision) throw new HttpError(409, "Handoff old session or revision mismatch");
      if (!task.runnerId && (task.state === "RUNNING" || task.state === "HELD" || (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)))) {
        task.state = "HELD"; task.reason = "Old native release is live or uncertain; reconcile its result before handoff"; this.save(task); throw new HttpError(409, task.reason);
      }
      if (task.runnerId) {
        const runner = this.runnerPrincipal(task); if (!runner?.runnerUrl) throw new HttpError(409, "Cannot reconcile old runner");
        const journal = decode(AcceptanceResponse, await rpc(runner.runnerUrl, this.runnerToken(runner), "journal", { taskId: task.taskId }));
        if (journal.taskId !== task.taskId) throw new HttpError(409, "Old runner journal task mismatch");
        if (journal.accepted ? !["SUCCEEDED", "FAILED"].includes(journal.state ?? "") : journal.state !== "not-accepted") throw new HttpError(409, "Old runner acceptance remains live or uncertain; handoff blocked");
        task.reconciledRunner = { runnerId: task.runnerId, state: journal.state!, observedAt: Date.now() };
      }
      const recipient = this.recipient(principal, { sessionId: input.newSessionId, incarnation: input.newIncarnation }); if (recipient.repo !== task.request.repo || !recipient.workerId) throw new HttpError(409, "Handoff requires verified repo and actual native worker binding");
      task.peerSession = input.newSessionId; task.peerIncarnation = input.newIncarnation; task.address = { ...task.address, workerId: recipient.workerId, sessionId: input.newSessionId, incarnation: input.newIncarnation, revision: input.revision };
      delete task.runnerId; delete task.instruction; delete task.readback; delete task.confirmedRevision;
      task.offerDecision = "accepted";
      task.disposition = "hold"; task.reason = "Handoff requires new readback"; this.save(task); return task;
    }
    if (method === "task.report") {
      this.role(principal, "session"); const input = decode(Schema.Struct({ ...SessionKey.fields, taskId: Id, state: Schema.Literals(["RUNNING", "SUCCEEDED", "FAILED", "HELD"]), result: Schema.Unknown }), params); const task = this.task(principal, input.taskId); const session = this.session(principal, input);
      if (session.sessionId !== task.peerSession || session.incarnation !== task.peerIncarnation) throw new HttpError(403, "Report session mismatch");
      if (task.runnerId || task.offerDecision !== "accepted" || !task.address || session.workerId !== task.address.workerId || task.confirmedRevision !== task.address.revision || !task.readback || !exact(task.readback, task.address)) throw new HttpError(409, "Native status requires accepted bound peer and confirmed current readback, never runner-owned work");
      if (["SUCCEEDED", "FAILED"].includes(task.state) && input.state !== task.state) throw new HttpError(409, "Terminal native result cannot regress");
      task.state = input.state; task.result = input.result; this.save(task); return task;
    }
    if (method === "runner.heartbeat") {
      this.role(principal, "runner"); const input = decode(Schema.Struct({ runnerId: Id, slots: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })), load: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100000 })), journal: Schema.Array(Schema.Unknown).check(Schema.isMaxLength(256)) }), params); if (input.runnerId !== principal.id || !principal.runnerUrl) throw new HttpError(403, "Runner enrollment mismatch");
      this.runners.set(input.runnerId, { runnerId: input.runnerId, owner: principal.owner, slots: input.slots, load: input.load, liveUntil: Date.now() + 15000 }); return { ok: true };
    }
    if (method === "runner.readback") {
      this.role(principal, "runner"); const input = decode(Schema.Struct({ runnerId: Id, taskId: Id, address: Address }), params); const task = this.task(principal, input.taskId);
      if (input.runnerId !== principal.id || task.runnerId !== principal.id) throw new HttpError(403, "Assigned runner mismatch");
      const address = input.address;
      if (address.repo !== task.request.repo || address.ref !== task.request.ref || address.sha !== task.request.sha || address.workerId !== address.sessionId || address.scope !== task.request.scope || address.restrictions !== task.request.restrictions || !exact(address.expectedEvidence, task.request.expectedEvidence) || address.deadline !== task.request.deadline) throw new HttpError(409, "Runner readback must bind its actual native session and approved work");
      if (task.address && !exact(task.address, address)) throw new HttpError(409, "Child binding/revision mismatch");
      task.address = address; task.readback = address; task.disposition = "ready";
      if (task.state === "ASSIGNING" || task.state === "QUEUED") task.state = "ACCEPTED";
      this.save(task); return { ok: true };
    }
    if (method === "runner.report") {
      this.role(principal, "runner"); const input = decode(Schema.Struct({ runnerId: Id, taskId: Id, state: Schema.Literals(["RUNNING", "SUCCEEDED", "FAILED", "HELD"]), result: Schema.Unknown }), params); const task = this.task(principal, input.taskId);
      if (input.runnerId !== principal.id || task.runnerId !== principal.id) throw new HttpError(403, "Assigned runner mismatch");
      if ((task.state === "SUCCEEDED" || task.state === "FAILED") && input.state !== task.state) throw new HttpError(409, "Terminal runner result cannot regress");
      if (input.state === "RUNNING" && (!task.address || task.confirmedRevision !== task.address.revision)) throw new HttpError(409, "Substantive prompt not confirmed");
      task.state = input.state; task.result = input.result; this.save(task); return { ok: true };
    }
    if (method.startsWith("evidence.")) {
      if (method === "evidence.target") {
        this.role(principal, "coordinator");
        const input = decode(Schema.Struct({ taskId: Id, sha: Sha, ref: Text, requiredLanes: Schema.Array(Text).check(Schema.isBetweenLength(1,32)) }), params);
        const task = this.task(principal, input.taskId);
        task.evidenceTarget = { sha: input.sha, ref: input.ref, repositoryId: task.request.githubRepoId, requiredLanes: [...input.requiredLanes] };
        this.save(task); return { target: task.evidenceTarget, executionAuthorized: false };
      }
      if (method === "evidence.ci") {
        this.role(principal, "coordinator", "reviewer");
        const task = this.task(principal, decode(TaskKey, params).taskId);
        const work = task.evidenceTarget ? { ...task.request, sha: task.evidenceTarget.sha, ref: task.evidenceTarget.ref, requiredLanes: task.evidenceTarget.requiredLanes } : task.request;
        const evidence = await pollCi(work, this.config.githubTokenEnv ? process.env[this.config.githubTokenEnv] : undefined);
        this.appendEvidence(task, principal, "ci", work.sha, { ...evidence, policy: fingerprint({ ref: work.ref, requiredLanes: [...work.requiredLanes].sort() }) }); return evidence;
      }
      if (method === "evidence.local") { this.role(principal, "coordinator", "runner"); const input = decode(LocalEvidence, params); const task = this.task(principal, input.taskId); if (principal.roles.includes("runner") && task.runnerId !== principal.id) throw new HttpError(403, "Assigned runner mismatch"); this.appendEvidence(task, principal, "local", input.sha, input); return { recorded: true }; }
      if (method === "evidence.deployment") { this.role(principal, "coordinator"); const input = decode(DeploymentEvidence, params); const task = this.task(principal, input.taskId); this.appendEvidence(task, principal, "deployment", input.sha, input); return { recorded: true }; }
      if (method === "evidence.review") { this.role(principal, "reviewer"); const input = decode(ReviewEvidence, params); const task = this.task(principal, input.taskId); this.appendEvidence(task, principal, "human-review", input.sha, { ...input, reviewer: principal.id, provenance: "enrolled human reviewer credential" }); return { recorded: true }; }
    }
    if (method.startsWith("archive.")) return this.archive(principal, method, params);
    throw new HttpError(404, "Unknown operation");
  }
  private archive(principal: Principal, method: string, params: unknown): unknown {
    if (method === "archive.participantConsent") {
      this.role(principal, "session");
      const input = decode(Schema.Struct({ owner: Id, sessionId: Id, enabled: Schema.Boolean }), params);
      const session = this.sessions.get(input.sessionId);
      if (!session || session.owner !== input.owner) throw new HttpError(404, "Consent session not found");
      this.repo(principal, session.repo);
      if (!input.enabled && principal.owner !== input.owner) {
        const consent = this.db.prepare("SELECT participants FROM consents WHERE owner=? AND session=?").get(input.owner, input.sessionId);
        if (!consent || !(JSON.parse(String(consent.participants)) as string[]).includes(principal.owner)) throw new HttpError(403, "Not an enrolled archive participant");
      }
      this.db.exec("BEGIN IMMEDIATE");
      try {
        this.db.prepare("INSERT INTO participant_consents VALUES (?,?,?,?) ON CONFLICT(owner,session,participant) DO UPDATE SET enabled=excluded.enabled").run(input.owner, input.sessionId, principal.owner, input.enabled ? 1 : 0);
        if (!input.enabled) {
          this.db.prepare("UPDATE consents SET enabled=0 WHERE owner=? AND session=?").run(input.owner, input.sessionId);
          this.db.prepare("DELETE FROM archive WHERE owner=? AND session=?").run(input.owner, input.sessionId);
        }
        this.db.exec("COMMIT");
      } catch (error) { this.db.exec("ROLLBACK"); throw error; }
      return { participant: principal.owner, enabled: input.enabled };
    }
    if (method === "archive.consent") {
      this.role(principal, "session"); const input = decode(Schema.Struct({ sessionId: Id, participants: Schema.Array(Id).check(Schema.isMaxLength(32)), enabled: Schema.Boolean }), params);
      const session = this.sessions.get(input.sessionId); if (!session || session.principal !== principal.id || session.owner !== principal.owner) throw new HttpError(403, "Consent session mismatch");
      if (input.enabled && (!input.participants.length || !input.participants.includes(principal.owner))) throw new HttpError(400, "Explicit owner/participant consent required");
      for (const participant of input.participants) {
        if (participant !== principal.owner && input.enabled && !this.db.prepare("SELECT enabled FROM participant_consents WHERE owner=? AND session=? AND participant=?").get(principal.owner, input.sessionId, participant)?.enabled) throw new HttpError(403, "Each participant must explicitly consent using their own enrolled credential");
      }
      this.db.prepare("INSERT INTO consents VALUES (?,?,?,?) ON CONFLICT(owner,session) DO UPDATE SET enabled=excluded.enabled,participants=excluded.participants").run(principal.owner, input.sessionId, input.enabled ? 1 : 0, JSON.stringify(input.participants)); return { enabled: input.enabled };
    }
    if (method === "archive.append") {
      this.role(principal, "session", "archive-writer"); const input = decode(Schema.Struct({ sessionId: Id, entryId: Id, digest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)), payload: ArchivePayload, gaps: Schema.Array(Text).check(Schema.isMaxLength(32)) }), params);
      const consent = this.db.prepare("SELECT enabled FROM consents WHERE owner=? AND session=?").get(principal.owner, input.sessionId); if (!consent?.enabled) throw new HttpError(403, "Archive enrollment revoked or absent");
      if (principal.roles.includes("session")) { const session = this.sessions.get(input.sessionId); if (session?.principal !== principal.id || session.owner !== principal.owner) throw new HttpError(403, "Archive session mismatch"); }
      const payload = JSON.stringify(input.payload); if (Buffer.byteLength(payload) > 16000) throw new HttpError(413, "Archive entry too large");
      if (fingerprint(input.payload) !== input.digest) throw new HttpError(409, "Archive digest mismatch");
      const prior = this.db.prepare("SELECT digest FROM archive WHERE owner=? AND session=? AND entry=?").all(principal.owner, input.sessionId, input.entryId);
      const gaps = prior.some((row) => row.digest !== input.digest) ? [...input.gaps, "entry-rewrite"] : input.gaps;
      this.db.prepare("INSERT OR IGNORE INTO archive(owner,session,entry,digest,payload,gaps,created) VALUES (?,?,?,?,?,?,?)").run(principal.owner, input.sessionId, input.entryId, input.digest, payload, JSON.stringify(gaps), Date.now()); return { appended: true, digest: input.digest };
    }
    if (method === "archive.list") {
      this.role(principal, "session", "coordinator"); const input = decode(Schema.Struct({ sessionId: Id, cursor: Schema.optional(Schema.Int) }), params); if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && this.sessions.get(input.sessionId)?.principal !== principal.id) throw new HttpError(403, "Archive session mismatch");
      return this.db.prepare("SELECT sequence,entry,digest,payload,gaps,created FROM archive WHERE owner=? AND session=? AND sequence>? ORDER BY sequence LIMIT 2").all(principal.owner, input.sessionId, input.cursor ?? 0).map((row) => ({ sequence: row.sequence, entryId: row.entry, digest: row.digest, payload: JSON.parse(String(row.payload)), gaps: JSON.parse(String(row.gaps)), createdAt: row.created }));
    }
    if (method === "archive.delete") {
      this.role(principal, "session", "coordinator"); const input = decode(Schema.Struct({ sessionId: Id }), params);
      if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && this.sessions.get(input.sessionId)?.principal !== principal.id) throw new HttpError(403, "Archive session mismatch");
      this.db.exec("BEGIN IMMEDIATE");
      try { this.db.prepare("UPDATE consents SET enabled=0 WHERE owner=? AND session=?").run(principal.owner, input.sessionId); this.db.prepare("DELETE FROM archive WHERE owner=? AND session=?").run(principal.owner, input.sessionId); this.db.exec("COMMIT"); }
      catch (error) { this.db.exec("ROLLBACK"); throw error; }
      return { revoked: true, deleted: true, backupPolicy: "journal rows and enrollment are omitted from every broker snapshot" };
    }
    throw new HttpError(404, "Unknown archive operation");
  }
  private async maintain(): Promise<void> {
    const now = Date.now(); this.db.prepare("DELETE FROM peers WHERE expires<=?").run(now);
    this.db.prepare("DELETE FROM archive WHERE created<=?").run(now - this.config.archiveRetentionMs);
    for (const task of this.tasks()) {
      if (task.offerDecision === "pending" && (task.offerExpires ?? 0) <= now) { task.offerDecision = "expired"; this.save(task); await this.offer(task); }
      if (task.state === "QUEUED") await this.offer(task);
    }
    for (const file of readdirSync(this.config.backupDirectory)) if (file.endsWith(".sqlite") && statSync(join(this.config.backupDirectory, file)).mtimeMs < now - this.config.backupRetentionMs) rmSync(join(this.config.backupDirectory, file));
    if (now % 60000 < 1000) await this.snapshot();
  }
  async snapshot(): Promise<string | undefined> {
    if (this.backingUp) return undefined; this.backingUp = true;
    const target = join(this.config.backupDirectory, `${Date.now()}.sqlite`);
    const partial = join(dirname(this.config.database), "snapshot.partial");
    const sanitized = `${target}.partial`;
    try {
      await backup(this.db, partial); chmodSync(partial, 0o600);
      const snapshot = new DatabaseSync(partial);
      // Never put host transcripts or old opt-in enrollment in recoverable snapshots.
      snapshot.exec("PRAGMA journal_mode=DELETE; PRAGMA secure_delete=ON; DELETE FROM archive; DELETE FROM consents; DELETE FROM participant_consents; VACUUM;"); snapshot.close();
      copyFileSync(partial, sanitized); chmodSync(sanitized, 0o600); renameSync(sanitized, target); return target;
    } finally { this.backingUp = false; rmSync(partial, { force: true }); rmSync(sanitized, { force: true }); }
  }
  async close(): Promise<void> {
    this.stopping = true; clearInterval(this.timer);
    this.server?.closeAllConnections();
    const { promise, resolve: closed } = Promise.withResolvers<void>();
    if (this.server) this.server.close(() => closed()); else closed();
    await promise;
    await this.runtime?.dispose(); await Promise.allSettled(this.consumers); this.db.close();
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const path = process.env.COORDINATOR_CONFIG; if (!path) throw new Error("COORDINATOR_CONFIG required");
  const broker = new Broker(decode(Config, JSON.parse(privateFile(path))));
  await broker.start(); console.log("coordinator broker ready");
  const stop = () => { void broker.close().then(() => process.exit(0)); };
  process.once("SIGTERM", stop); process.once("SIGINT", stop);
}
