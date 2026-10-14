import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Option, Schedule, Schema, Scope, Semaphore } from "effect";
import * as PersistedQueue from "effect/persistence/PersistedQueue";
import { Postgres } from "./postgres.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { HttpClient, HttpServer } from "effect/http";
import { Commands, Address, Work, TaskId, SessionId, IncarnationId, PrincipalId, ArchiveEntryId, RunnerId, WorkerId, Repo, Sha, Hash, BrokerConfig, Task, LiveSession, ArchivePayload, ArchiveEntry, ArchiveRow, JournalState, Json, JsonValue, Poll, Delivery, EvidenceObservation, HttpError, type CommandCapabilities, type CommandInput, type CommandOutput, type BrokerRequest, type Principal, type Role, type SessionKey, type LiveRunner } from "./contracts.ts";
import { Platform, redactedCause } from "./platform.ts";
import { Mcp, serverLayer, MCP_RESULT_LIMIT } from "./http.ts";
import { pollCi } from "./evidence.ts";
import { SnapshotStore, dumpBrokerDatabase } from "./backup.ts";
import { assertBrokerSchema } from "./database.ts";
import { ArchiveId, type Evidence as StoredEvidence } from "./generated/database/types.ts";
import { Database } from "./kysely.ts";

const BrokerConfiguration = Context.Service<BrokerConfig>("coordinator/BrokerConfiguration");
const pollResultText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands["session.poll"].structuredOutput)), { onExcessProperty: "error" });
const sessionResultText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands["session.list"].structuredOutput)), { onExcessProperty: "error" });
const taskResultText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands["task.get"].structuredOutput)), { onExcessProperty: "error" });
const archiveResultText = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Commands["archive.list"].structuredOutput)), { onExcessProperty: "error" });
export class Broker extends Context.Service<Broker>()("coordinator/Broker", {
  make: Effect.gen(function*() {
    const config = yield* BrokerConfiguration;
    const platform = yield* Platform;
    const mcp = yield* Mcp;
    const client = yield* HttpClient.HttpClient;
    if (config.archiveRetentionMs <= 0 || config.backupRetentionMs <= 0) return yield* new HttpError({ status: 400, message: "Invalid broker limits" });
    if (new Set(config.principals.map((p) => p.id)).size !== config.principals.length) return yield* new HttpError({ status: 400, message: "Distinct enrolled principals required" });
    const hashes = config.principals.flatMap((p) => p.controlTokenHash ? [p.tokenHash, p.controlTokenHash] : [p.tokenHash]);
    if (new Set(hashes).size !== hashes.length || config.principals.some((p) => p.roles.includes("runner") && !p.controlTokenHash)) return yield* new HttpError({ status: 400, message: "Distinct runner enrollment and broker-only control credentials required" });
    yield* platform.fs.makeDirectory(config.backupDirectory, { recursive: true, mode: 0o700 });
    yield* platform.fs.chmod(config.backupDirectory, 0o700);
    const fatal = yield* Deferred.make<never, HttpError>();
    let ownershipLost = false;
    const owned = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.suspend<A, E | HttpError, R>(() =>
      ownershipLost ? Deferred.await(fatal) : effect.pipe(Effect.raceFirst(Deferred.await(fatal))));
    const ledgerScope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void));
    const revoke = Effect.gen(function*() {
      ownershipLost = true;
      yield* Deferred.fail(fatal, new HttpError({ status: 503, message: "Broker database ownership unavailable" }));
      yield* Scope.close(ledgerScope, Exit.void);
    });
    const databaseContext = yield* owned(Layer.build(Postgres.layer(config.databaseUrlFile)).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const database = Context.get(databaseContext, Postgres);
    const sql = database.sql;
    const { client: db, execute } = Context.get(databaseContext, Database);
    yield* database.failure.pipe(
      Effect.catch((error) => Effect.gen(function*() {
        yield* Effect.logError(`broker PostgreSQL ownership failed: ${redactedCause(Cause.fail(error))}`);
        yield* revoke;
      })),
      Effect.forkScoped({ startImmediately: true })
    );
    yield* owned(assertBrokerSchema(sql));
    const storage = PersistedQueue.layerStoreSql({ pollInterval: "200 millis", lockRefreshInterval: "1 second", lockExpiration: "5 seconds" });
    const services = yield* owned(Layer.build(PersistedQueue.layer.pipe(Layer.provideMerge(storage), Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)))).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const sessions = new Map<SessionId, LiveSession>();
    const runners = new Map<RunnerId, LiveRunner>();
    const versions = new WeakMap<Task, string>();
    for (const row of yield* execute(db.selectFrom("sessions").selectAll())) {
      const value = Schema.decodeUnknownSync(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(row.data);
      value.verified = false; value.liveUntil = 0; value.polledAt = 0;
      sessions.set(value.sessionId, value);
    }
    for (const file of yield* platform.fs.readDirectory(config.backupDirectory)) if (file.endsWith(".partial")) yield* platform.fs.remove(platform.path.join(config.backupDirectory, file));
    const queue = yield* PersistedQueue.make({ name: "coordinator.dispatch", schema: TaskId, maxAttempts: Number.MAX_SAFE_INTEGER }).pipe(Effect.provideContext(services));
    const exact = Effect.fnUntraced(function*(a: unknown, b: unknown) {
      return (yield* platform.fingerprint(a)) === (yield* platform.fingerprint(b));
    });
    const authorize = Effect.fn("Broker.authorize")(function*(token: string) {
      for (const principal of config.principals) if (yield* platform.checkToken(token, principal.tokenHash)) return principal;
      return yield* new HttpError({ status: 401, message: "Invalid credential" });
    }, owned);
    const live = owned(Effect.void);
    // ponytail: one outstanding probe; a stalled database must not accumulate SELECTs.
    let pendingReadiness: Deferred.Deferred<void, HttpError> | undefined;
    const ready = owned(Effect.gen(function*() {
      let probe = pendingReadiness;
      if (!probe) {
        probe = yield* Deferred.make<void, HttpError>();
        pendingReadiness = probe;
        const completion = probe;
        yield* execute(db.selectNoFrom((eb) => eb.val(1).as("ready"))).pipe(
          Effect.asVoid,
          Effect.catchCause(() => Effect.fail(new HttpError({ status: 503, message: "Broker database not ready" }))),
          Effect.exit,
          Effect.flatMap((exit) => Deferred.done(completion, exit)),
          Effect.ensuring(Effect.sync(() => { pendingReadiness = undefined; })),
          Effect.forkScoped({ startImmediately: true }),
          Effect.provideService(Scope.Scope, ledgerScope)
        );
      }
      return yield* Deferred.await(probe).pipe(
        Effect.interruptible,
        Effect.timeout("1 second"),
        Effect.mapError(() => new HttpError({ status: 503, message: "Broker database not ready" }))
      );
    }));

    const objectBackup = config.objectBackup ? Context.get(yield* Layer.build(SnapshotStore.layer(config.objectBackup)), SnapshotStore) : undefined;
    if (!objectBackup) yield* Effect.logWarning("Durable object backup is unconfigured: dedicated bucket and narrow credentials required");
    const snapshotLock = yield* Semaphore.make(1);
    const snapshot = Effect.fn("Broker.snapshot")(function*() {
      return yield* snapshotLock.withPermits(1)(Effect.gen(function*() {
        const target = platform.path.join(config.backupDirectory, `${yield* Clock.currentTimeMillis}.pgdump`);
        const partial = `${target}.partial`;
        let stage = "local pruning";
        return yield* Effect.gen(function*() {
          const cutoff = (yield* Clock.currentTimeMillis) - config.backupRetentionMs;
          for (const file of yield* platform.fs.readDirectory(config.backupDirectory)) {
            if (!file.endsWith(".pgdump")) continue;
            const path = platform.path.join(config.backupDirectory, file);
            if (Option.getOrThrow((yield* platform.fs.stat(path)).mtime).getTime() < cutoff) yield* platform.fs.remove(path);
          }
          stage = "local creation";
          yield* dumpBrokerDatabase(config, partial, true).pipe(Effect.provideService(Platform, platform));
          yield* platform.fs.rename(partial, target);
          if (objectBackup) {
            stage = "remote put";
            yield* objectBackup.put(target);
            stage = "remote expiry";
            yield* objectBackup.expire((yield* Clock.currentTimeMillis) - config.backupRetentionMs);
          }
          return target;
        }).pipe(
          Effect.tapCause((cause) => Effect.logError(`broker snapshot ${stage} failed: ${redactedCause(cause)}`)),
          Effect.ensuring(platform.fs.remove(partial, { force: true }).pipe(Effect.orDie)),
        );
      }));
    }, owned);
    const role = Effect.fn("Broker.role")(function*(principal: Principal, ...roles: Role[]) {
    if (!roles.some((role) => principal.roles.includes(role))) return yield* new HttpError({ status: 403, message: "Operation not authorized" });

    });
    const repo = Effect.fn("Broker.repo")(function*(principal: Principal, repo: typeof Repo.Type) {
    if (!principal.repos.includes(repo)) return yield* new HttpError({ status: 403, message: "Repository not authorized" });

    });
    const loadTask = Effect.fn("Broker.task")(function*(principal: Principal, id: TaskId) {
    const row = (yield* execute(db.selectFrom("tasks").selectAll().where("id", "=", id)))[0];
    if (!row) return yield* new HttpError({ status: 404, message: "Task not found" });
    const task = Schema.decodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data);
    versions.set(task, row.data);
    if (task.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Owner mismatch" });
    (yield* repo(principal, task.request.repo));
    return task;

    });
    const tasks = Effect.fn("Broker.tasks")(function*() {
      return (yield* execute(db.selectFrom("tasks").selectAll())).map((r) => {
        const value = Schema.decodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(r.data);
        versions.set(value, r.data);
        return value;
      });
    });
    const save = Effect.fn("Broker.save")(function*(task: Task) {
      task.updatedAt = yield* Clock.currentTimeMillis;
      const data = Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task);
      const previous = versions.get(task);
      const changed = previous === undefined
        ? yield* execute(db.insertInto("tasks").values({ id: task.taskId, data: data }).onConflict((oc) => oc.column("id").doNothing()).returning("id"))
        : yield* execute(db.updateTable("tasks").set({ data: data }).where("id", "=", task.taskId).where("data", "=", previous).returning("id"));
      if (!changed.length) return yield* new HttpError({ status: 409, message: "Task changed while reconciling; re-read current ownership and revision" });
      versions.set(task, data);
    });
    const saveReturnedTask = Effect.fn("Broker.saveReturnedTask")(function*(task: Task) {
      if (new TextEncoder().encode(taskResultText({ result: task })).byteLength > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Task exceeds delivery budget" });
      yield* save(task);
      return task;
    });
    const boundSession = Effect.fn("Broker.session")(function*(principal: Principal, key: SessionKey, live = true) {
    const now = yield* Clock.currentTimeMillis;
    const session = sessions.get(key.sessionId);
    if (!session || session.principal !== principal.id || session.owner !== principal.owner || session.incarnation !== key.incarnation) return yield* new HttpError({ status: 403, message: "Session binding mismatch" });
    if (live && (!session.verified || session.liveUntil <= now)) return yield* new HttpError({ status: 409, message: "Recipient is not verified live" });
    return session;

    });
    const recipient = Effect.fn("Broker.recipient")(function*(principal: Principal, key: SessionKey) {
    const now = yield* Clock.currentTimeMillis;
    const session = sessions.get(key.sessionId);
    if (!session || session.owner !== principal.owner || session.incarnation !== key.incarnation || !session.verified || session.liveUntil <= now || session.polledAt < now - 10000) return yield* new HttpError({ status: 409, message: "Recipient is not verified live" });
    (yield* repo(principal, session.repo));
    return session;

    });
    const offer = Effect.fn("Broker.offer")(function*(task: Task) {
    const now = yield* Clock.currentTimeMillis;
    if (task.state !== "QUEUED" || task.offerDecision === "accepted") return;
    if (task.offerDecision === "pending" && (task.offerExpires ?? 0) > now) return;
    if (task.offerDecision === "pending") { task.offerDecision = "expired"; (yield* save(task)); }
    yield* queue.offer(task.taskId, { id: task.taskId });

    });
    const runnerPrincipal = (task: Task) => { return config.principals.find((p) => p.roles.includes("runner") && RunnerId.make(p.id, { disableChecks: true }) === task.runnerId && p.owner === task.owner); };
    const assign = Effect.fn("Broker.assign")(function*(id: TaskId) {
    while (true) {
      const now = yield* Clock.currentTimeMillis;
      const task = (yield* tasks()).find((t) => t.taskId === id);
      if (!task || ["SUCCEEDED", "FAILED", "ACCEPTED", "RUNNING"].includes(task.state) || task.offerDecision === "accepted") return;
      if (task.runnerId && ["ASSIGNING", "HELD"].includes(task.state)) {
        const principal = runnerPrincipal(task);
        if (!principal?.runnerUrl) { task.state = "HELD"; task.reason = "Assigned runner no longer enrolled"; (yield* save(task)); yield* Effect.sleep(1000); continue; }
        const runnerUrl = principal.runnerUrl;
        const runnerId = RunnerId.make(principal.id, { disableChecks: true });
        yield* Effect.gen(function*() {
          const result = yield* mcp.call(runnerUrl, yield* runnerToken(principal), "journal", { taskId: id });
          const current = (yield* tasks()).find((t) => t.taskId === id);
          if (!current || current.runnerId !== runnerId || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
          if (result.taskId !== id) return yield* new HttpError({ status: 409, message: "Runner journal task mismatch" });
          if (result.accepted) { current.state = ["HELD", "PROMPT_INTENT"].includes(result.state ?? "") ? "HELD" : "ACCEPTED"; current.reason = result.reason; yield* save(current); return; }
          if (result.state === "not-accepted") { current.state = "QUEUED"; delete current.runnerId; yield* save(current); }
          else { current.state = "HELD"; current.reason = "Runner acceptance unknown; reassignment blocked"; yield* save(current); }
        }).pipe(Effect.catch((error) => Effect.gen(function*() {
          yield* Effect.logWarning(`broker runner journal reconciliation held: ${redactedCause(Cause.fail(error))}`);
          const current = (yield* tasks()).find((t) => t.taskId === id);
          if (!current || current.runnerId !== runnerId || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
          current.state = "HELD"; current.reason = "Runner acceptance unknown; reassignment blocked"; yield* save(current);
        })));
        yield* Effect.sleep(1000); continue;
      }
      if (task.state === "HELD") { yield* Effect.sleep(1000); continue; }
      const candidate = [...runners.values()].find((r) => r.owner === task.owner && r.liveUntil > now && r.load < r.slots && config.principals.some((p) => p.roles.includes("runner") && RunnerId.make(p.id, { disableChecks: true }) === r.runnerId && p.repos.includes(task.request.repo) && p.runnerUrl));
      if (!candidate) { yield* Effect.sleep(500); continue; }
      const principal = config.principals.find((p) => p.roles.includes("runner") && RunnerId.make(p.id, { disableChecks: true }) === candidate.runnerId)!;
      delete task.peerSession; delete task.peerIncarnation; delete task.offerExpires;
      delete task.address; delete task.readback; delete task.confirmedRevision;
      task.state = "ASSIGNING"; task.runnerId = candidate.runnerId; (yield* save(task));
      candidate.load++;
      yield* Effect.gen(function*() {
        const result = (yield* mcp.call(principal.runnerUrl!, (yield* runnerToken(principal)), "acceptTask", task.request, 15000));
        if (result.taskId !== id) return yield* new HttpError({ status: 409, message: "Runner acceptance task mismatch" });
        const current = (yield* tasks()).find((t) => t.taskId === id);
        if (!current || current.runnerId !== candidate.runnerId || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
        if (result.accepted) { if (current.state === "ASSIGNING") current.state = "ACCEPTED"; (yield* save(current)); return; }
        if (result.reason === "full" || result.state === "not-accepted") { current.state = "QUEUED"; delete current.runnerId; (yield* save(current)); }
        else { current.state = "HELD"; current.reason = result.reason ?? "Runner declined unsafe material"; (yield* save(current)); }
      }).pipe(Effect.catch((error) => Effect.gen(function*() {
        yield* Effect.logWarning(`broker runner acceptTask held: ${redactedCause(Cause.fail(error))}`);
        const current = (yield* tasks()).find((t) => t.taskId === id);
        if (!current || current.runnerId !== candidate.runnerId || ["RUNNING", "SUCCEEDED", "FAILED"].includes(current.state)) return;
        current.state = "HELD"; current.reason = "Runner acceptance unknown; reassignment blocked"; (yield* save(current));
      })));
      yield* Effect.sleep(500);
    }


    });
    const runnerToken = Effect.fn("Broker.runnerToken")(function*(principal: Principal) {
    // Outbound secrets are separate files, never in the ledger/config/response or process arguments.
    const directory = Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_RUNNER_TOKEN_DIRECTORY")));
    if (!directory) return yield* new HttpError({ status: 500, message: "Runner credential directory missing" });
    const token = (yield* platform.privateFile(platform.path.join(directory, principal.id)));
    if (!principal.controlTokenHash || !(yield* platform.checkToken(token, principal.controlTokenHash))) return yield* new HttpError({ status: 500, message: "Runner control credential rotation mismatch" });
    return token;

    });
    const appendEvidence = Effect.fn("Broker.appendEvidence")(function*(task: Task, principal: Principal, kind: string, sha: typeof Sha.Type, data: Json) {
    const now = yield* Clock.currentTimeMillis;
    (yield* execute(db.insertInto("evidence").values({ task: task.taskId, kind: kind, sha: sha, principal: principal.id, data: Schema.encodeUnknownSync(Schema.fromJsonString(Json), { onExcessProperty: "error" })(data), observed: now })));

    });
    const bindCommandCapabilities = (principal: Principal, now: number): CommandCapabilities<"broker"> => {
      const readableTask = Effect.fn("Broker.readableTask")(function*(taskId: TaskId) {
        (yield* role(principal, "coordinator", "session", "reviewer", "runner")); const task = (yield* loadTask(principal, taskId));
        if (principal.roles.includes("runner") && task.runnerId !== RunnerId.make(principal.id, { disableChecks: true })) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
        if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && !principal.roles.includes("reviewer")) { const recipient = task.peerSession ? sessions.get(task.peerSession) : undefined; if (recipient?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Task is not addressed to this session" }); }
        return task;
      });
      return {
        listSessions: Effect.fn("Broker.listSessions")(function*({ cursor }: CommandInput<"session.list">): Effect.fn.Return<CommandOutput<"session.list">, unknown> {
          yield* role(principal, "coordinator", "session");
          const visible = [...sessions.values()].filter((session) => session.owner === principal.owner && (cursor === undefined || session.sessionId > cursor))
            .sort((a, b) => a.sessionId < b.sessionId ? -1 : a.sessionId > b.sessionId ? 1 : 0);
          const page: CommandOutput<"session.list"> = { sessions: [] };
          for (const session of visible) {
            const { sessionId, incarnation, machineId, host, repo, workerId, liveUntil } = session;
            page.sessions.push({ sessionId, incarnation, machineId, host, repo, workerId, liveUntil,
              status: session.verified && liveUntil > now && session.polledAt >= now - 10000 ? "live" : "stale" });
            if (new TextEncoder().encode(sessionResultText({ result: { ...page, nextCursor: sessionId } })).byteLength > MCP_RESULT_LIMIT) {
              page.sessions.pop();
              const last = page.sessions.at(-1);
              if (!last) return yield* new HttpError({ status: 413, message: "Session metadata exceeds delivery budget" });
              return { sessions: page.sessions, nextCursor: last.sessionId };
            }
          }
          return page;
        }),
        heartbeatSession: Effect.fn("Broker.heartbeatSession")(function*(params: CommandInput<"session.heartbeat">): Effect.fn.Return<CommandOutput<"session.heartbeat">, unknown> {
          // Repository is detected presence metadata, not session admission authority.
          (yield* role(principal, "session")); const input = params;
          const existing = sessions.get(input.sessionId);
          if (existing && (existing.principal !== principal.id || existing.owner !== principal.owner)) return yield* new HttpError({ status: 403, message: "Session already enrolled by another principal" });
          if (existing && existing.incarnation !== input.incarnation && existing.verified && existing.liveUntil > now) return yield* new HttpError({ status: 409, message: "Prior session incarnation still live" });
          const session: LiveSession = { ...input, owner: principal.owner, principal: principal.id, verified: true, liveUntil: now + 30000, polledAt: existing?.incarnation === input.incarnation ? existing.polledAt : 0 };
          sessions.set(input.sessionId, session);
          (yield* execute(db.insertInto("sessions").values({ id: input.sessionId, data: Schema.encodeUnknownSync(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(session) }).onConflict((oc) => oc.column("id").doUpdateSet((eb) => ({ data: eb.ref("excluded.data") })))));
          return { liveUntil: session.liveUntil };
        }),
        pollSession: Effect.fn("Broker.pollSession")(function*(params: CommandInput<"session.poll">): Effect.fn.Return<CommandOutput<"session.poll">, unknown> {
          (yield* role(principal, "session")); const input = params; const session = (yield* boundSession(principal, input)); session.polledAt = now;
          const messages: Delivery[] = (yield* execute(db.selectFrom("peers").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation).where("expires", ">", now).orderBy("expires").limit(8))).map((r) => ({ messageId: r.id, incarnation: IncarnationId.make(r.incarnation, { disableChecks: true }), payload: Schema.decodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(r.payload), expiresAt: r.expires }));
          const bound = (yield* tasks()).filter((task) => task.owner === principal.owner && task.peerSession === input.sessionId && task.peerIncarnation === input.incarnation);
          const result: Poll = { messages: [], offers: [], releases: [], archiveEnabled: Boolean((yield* execute(db.selectFrom("consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId)))[0]?.enabled) };
          for (const message of messages) { const next = { ...result, messages: [...result.messages, message] }; if (new TextEncoder().encode(pollResultText({ result: next })).byteLength < 60000) result.messages.push(message); }
          for (const task of bound) {
            const key = !task.runnerId && task.offerDecision === "pending" && (task.offerExpires ?? 0) > now ? "offers"
              : !task.runnerId && task.offerDecision === "accepted" && task.address && session.workerId === task.address.workerId && session.repo === task.address.repo && task.address.deadline > now && task.confirmedRevision === task.address.revision && task.disposition === "ready" && task.state === "ACCEPTED" ? "releases" : undefined;
            if (key && new TextEncoder().encode(pollResultText({ result: { ...result, [key]: [...result[key], task] } })).byteLength < 60000) result[key].push(task);
          }
          return result;
        }),
        stopSession: Effect.fn("Broker.stopSession")(function*(params: CommandInput<"session.stop">): Effect.fn.Return<CommandOutput<"session.stop">, unknown> {
          (yield* role(principal, "session")); const input = params; const session = (yield* boundSession(principal, input, false)); session.verified = false; session.liveUntil = 0;
          (yield* execute(db.deleteFrom("peers").where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation)));
          (yield* execute(db.deleteFrom("sessions").where("id", "=", input.sessionId))); return { stopped: true };
        }),
        sendPeer: Effect.fn("Broker.sendPeer")(function*(params: CommandInput<"peer.send">): Effect.fn.Return<CommandOutput<"peer.send">, unknown> {
          (yield* role(principal, "session", "coordinator")); const input = params;
          (yield* recipient(principal, input));
          if (new TextEncoder().encode(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(input.payload)).byteLength > 8000) return yield* new HttpError({ status: 413, message: "Peer payload too large" });
          const hash = (yield* platform.fingerprint({ ...input, owner: principal.owner, sender: principal.id })); const prior = (yield* execute(db.selectFrom("peers").selectAll().where("id", "=", input.messageId)))[0];
          if (prior && prior.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Message ID conflict" });
          if (!prior) (yield* execute(db.insertInto("peers").values({ id: input.messageId, owner: principal.owner, sender: principal.id, session: input.sessionId, incarnation: input.incarnation, payload: Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(input.payload), fingerprint: hash, expires: now + input.ttlMs })));
          return { delivered: false, committed: true, messageId: input.messageId };
        }),
        acknowledgePeer: Effect.fn("Broker.acknowledgePeer")(function*(params: CommandInput<"peer.ack">): Effect.fn.Return<CommandOutput<"peer.ack">, unknown> {
          (yield* role(principal, "session")); const input = params; (yield* boundSession(principal, input));
          (yield* execute(db.deleteFrom("peers").where("id", "=", input.messageId).where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation))); return { handled: true };
        }),
        replyPeer: Effect.fn("Broker.replyPeer")(function*(params: CommandInput<"peer.reply">): Effect.fn.Return<CommandOutput<"peer.reply">, unknown> {
          (yield* role(principal, "session")); const input = params; const session = (yield* boundSession(principal, input));
          const task = (yield* loadTask(principal, input.taskId));
          if (task.peerSession !== input.sessionId || task.peerIncarnation !== input.incarnation || task.offerDecision !== "pending" || (task.offerExpires ?? 0) <= now) return yield* new HttpError({ status: 409, message: "Offer expired or binding mismatch" });
          if (session.workerId !== task.request.workerId || session.repo !== task.request.repo || task.runnerId) return yield* new HttpError({ status: 409, message: "Offer worker/repository binding changed" });
          task.offerDecision = input.decision; if (input.decision === "accepted") task.state = "ACCEPTED"; return yield* saveReturnedTask(task);
        }),
        submitTask: Effect.fn("Broker.submitTask")(function*(params: CommandInput<"task.submit">): Effect.fn.Return<CommandOutput<"task.submit">, unknown> {
          (yield* role(principal, "coordinator")); const work = params; (yield* repo(principal, work.repo));
          if (new TextEncoder().encode(Schema.encodeUnknownSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work)).byteLength > 28000) return yield* new HttpError({ status: 413, message: "Approved work exceeds delivery budget" });
          const prior = (yield* execute(db.selectFrom("tasks").selectAll().where("id", "=", work.taskId)))[0];
          if (prior) { const task = (yield* loadTask(principal, work.taskId)); if (task.fingerprint !== (yield* platform.fingerprint(work))) return yield* new HttpError({ status: 409, message: "Task ID fingerprint conflict" }); return task; }
          if (work.deadline <= now || !work.prompt || !work.ref || !work.scope || !work.requiredLanes.length) return yield* new HttpError({ status: 400, message: "Approved bounded scope, deadline, ref and explicit CI lanes required" });
          if (config.repositoryIds[work.repo] !== work.githubRepoId) return yield* new HttpError({ status: 409, message: "Inspected enrollment repository ID mismatch" });
          const peer = work.bypassPeer ? undefined : [...sessions.values()].find((session) => session.owner === principal.owner && session.repo === work.repo && session.workerId === work.workerId && session.verified && session.liveUntil > now && session.polledAt >= now - 10000);
          const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: (yield* platform.fingerprint(work)), request: work, state: "QUEUED", createdAt: now, updatedAt: now, offerDecision: work.bypassPeer ? "bypass" : peer ? "pending" : "expired", ...(peer ? { peerSession: peer.sessionId, peerIncarnation: peer.incarnation, offerExpires: now + 15000 } : {}) };
          return yield* saveReturnedTask(task);
        }),
        getTask: Effect.fn("Broker.getTask")(function*(params: CommandInput<"task.get">): Effect.fn.Return<CommandOutput<"task.get">, unknown> {
          const task = yield* readableTask(params.taskId);
          const target = task.evidenceTarget ?? task.request;
          const policy = (yield* platform.fingerprint({ ref: target.ref, requiredLanes: [...target.requiredLanes].sort() }));
          const observations: EvidenceObservation[] = (yield* execute(db.selectFrom("evidence").selectAll().where("id", "in", db.selectFrom("evidence").select((eb) => eb.fn.max<StoredEvidence["id"]>("id").as("id")).where("task", "=", task.taskId).groupBy("kind")).orderBy("id", "desc"))).map((row) => {
            const data = Schema.decodeUnknownSync(Schema.fromJsonString(Json), { onExcessProperty: "error" })(row.data);
            return { kind: row.kind, sha: Sha.make(row.sha, { disableChecks: true }), principal: PrincipalId.make(row.principal, { disableChecks: true }), data, observedAt: row.observed, stale: row.sha !== target.sha || (row.kind === "ci" && data.policy !== policy) };
          });
          const evidence: EvidenceObservation[] = [];
          for (const observation of observations) if (new TextEncoder().encode(taskResultText({ result: { ...task, evidence: [...evidence, observation] } })).byteLength < 60000) evidence.push(observation);
          return { ...task, evidence };
        }),
        attachTask: Effect.fn("Broker.attachTask")(function*(params: CommandInput<"task.attach">): Effect.fn.Return<CommandOutput<"task.attach">, unknown> {
          const task = yield* readableTask(params.taskId);
          if (!task.runnerId) return yield* new HttpError({ status: 409, message: "Only runner-owned children are attachable" });
          const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
          yield* role(principal, "coordinator");
          return yield* mcp.call(runner.runnerUrl, yield* runnerToken(runner), "inspect", params);
        }),
        controlTask: Effect.fn("Broker.controlTask")(function*(params: CommandInput<"task.control">): Effect.fn.Return<CommandOutput<"task.control">, unknown> {
          const task = yield* readableTask(params.taskId);
          if (!task.runnerId) return yield* new HttpError({ status: 409, message: "Only runner-owned children are attachable" });
          const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
          yield* role(principal, "coordinator");
          return yield* mcp.call(runner.runnerUrl, yield* runnerToken(runner), "command", params);
        }),
        instructTask: Effect.fn("Broker.instructTask")(function*(params: CommandInput<"task.instruction">): Effect.fn.Return<CommandOutput<"task.instruction">, unknown> {
          (yield* role(principal, "coordinator"));
          const input = params;
          const task = (yield* loadTask(principal, input.taskId));
          if (task.runnerId) return yield* new HttpError({ status: 409, message: "Durably accepted launch material is immutable; later native instructions require a reconciled handoff" });
          if (task.offerDecision !== "accepted") return yield* new HttpError({ status: 409, message: "Native peer must explicitly accept before later instructions" });
          if (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)) return yield* new HttpError({ status: 409, message: "Prior native release is live or uncertain; reconcile before another instruction" });
          if (!task.address || input.address.revision <= task.address.revision || input.address.messageId === task.address.messageId || input.address.repo !== task.request.repo || input.address.workerId !== task.address.workerId || input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation || input.address.deadline <= now) return yield* new HttpError({ status: 409, message: "New approved message/revision and exact current recipient required" });
          (yield* recipient(principal, input.address));
          task.instruction = { prompt: input.prompt, address: input.address }; task.address = input.address;
          delete task.readback; delete task.confirmedRevision; task.disposition = "hold"; task.reason = "Later instruction awaits new readback"; task.state = "ACCEPTED";
          if (new TextEncoder().encode(taskResultText({ result: task })).byteLength > 50000) return yield* new HttpError({ status: 413, message: "Addressed task exceeds delivery budget" });
          return yield* saveReturnedTask(task);
        }),
        addressTask: Effect.fn("Broker.addressTask")(function*(params: CommandInput<"task.address">): Effect.fn.Return<CommandOutput<"task.address">, unknown> {
          (yield* role(principal, "coordinator")); const input = params; const task = (yield* loadTask(principal, input.taskId)); const approved = task.instruction?.address ?? task.request;
          if (!task.runnerId) {
            if (task.offerDecision !== "accepted" || !task.peerSession || !task.peerIncarnation) return yield* new HttpError({ status: 409, message: "Native address requires accepted peer ownership" });
            const boundRecipient = yield* recipient(principal, input.address);
            if (boundRecipient.workerId !== input.address.workerId || boundRecipient.repo !== input.address.repo) return yield* new HttpError({ status: 409, message: "Native recipient worker/repository binding changed" });
          }
          if (input.address.repo !== approved.repo || input.address.sha !== approved.sha || input.address.ref !== approved.ref || input.address.workerId !== (task.address?.workerId ?? approved.workerId)) return yield* new HttpError({ status: 409, message: "Address does not match approved repo/ref/SHA/current worker" });
          if (input.address.scope !== approved.scope || input.address.restrictions !== approved.restrictions || !(yield* exact(input.address.expectedEvidence, approved.expectedEvidence)) || input.address.deadline !== approved.deadline) return yield* new HttpError({ status: 409, message: "Address must preserve approved consequential details" });
          if (input.address.revision <= (task.address?.revision ?? 0)) { if ((yield* exact(input.address, task.address))) return task; return yield* new HttpError({ status: 409, message: "Address revision must increase" }); }
          if (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)) return yield* new HttpError({ status: 409, message: "Confirmed release cannot be reset; reconcile its result first" });
          if (task.runnerId && task.address && (input.address.sessionId !== task.address.sessionId || input.address.incarnation !== task.address.incarnation)) return yield* new HttpError({ status: 409, message: "Use reconciled handoff for a new session" });
          if (!task.runnerId && task.peerSession && (input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation)) return yield* new HttpError({ status: 409, message: "Peer address mismatch" });
          task.address = input.address; delete task.readback; delete task.confirmedRevision; task.disposition = "hold"; task.reason = "Awaiting matching readback";
          if (task.instruction) task.instruction.address = input.address;
          if (new TextEncoder().encode(taskResultText({ result: task.runnerId ? { ...task, readback: input.address, disposition: "ready", reason: undefined } : task })).byteLength > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Task exceeds delivery budget" });
          if (task.runnerId) {
            yield* save(task);
            const runner = runnerPrincipal(task);
            if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable for corrected readback" });
            const readback = (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), "prepare", { taskId: task.taskId, address: input.address }));
            if (!(yield* exact(readback, input.address))) return yield* new HttpError({ status: 409, message: "Corrected runner readback mismatch" });
            task.readback = readback; task.disposition = "ready"; delete task.reason;
          }
          return yield* saveReturnedTask(task);
        }),
        readbackTask: Effect.fn("Broker.readbackTask")(function*(params: CommandInput<"task.readback">): Effect.fn.Return<CommandOutput<"task.readback">, unknown> {
          (yield* role(principal, "session")); const input = params; const task = (yield* loadTask(principal, input.taskId));
          const session = (yield* boundSession(principal, input.address));
          if (task.runnerId || task.offerDecision !== "accepted" || session.workerId !== input.address.workerId || session.repo !== input.address.repo) return yield* new HttpError({ status: 409, message: "Native readback requires accepted bound peer ownership, never runner-owned work" });
          if (task.peerSession !== input.address.sessionId || task.peerIncarnation !== input.address.incarnation) return yield* new HttpError({ status: 403, message: "Task/session binding mismatch" });
          if (!task.address || !(yield* exact(task.address, input.address))) return yield* new HttpError({ status: 409, message: "Readback mismatch; correct address and read back again" });
          if (task.confirmedRevision !== undefined) { if (input.disposition === "ready" && (yield* exact(task.readback, input.address))) return task; return yield* new HttpError({ status: 409, message: "Confirmed release cannot be reset by readback" }); }
          if (input.disposition !== "ready" && !input.reason) return yield* new HttpError({ status: 400, message: "Disposition reason required" });
          if (input.disposition === "hold" && !input.nextCheck) return yield* new HttpError({ status: 400, message: "Hold next-check required" });
          task.readback = input.address; task.disposition = input.disposition; task.reason = input.reason; task.nextCheck = input.nextCheck; delete task.confirmedRevision; return yield* saveReturnedTask(task);
        }),
        confirmTask: Effect.fn("Broker.confirmTask")(function*(params: CommandInput<"task.confirm">): Effect.fn.Return<CommandOutput<"task.confirm">, unknown> {
          (yield* role(principal, "coordinator")); const input = params; const task = (yield* loadTask(principal, input.taskId));
          if (!task.runnerId) {
            if (task.offerDecision !== "accepted" || !task.address || task.peerSession !== task.address.sessionId || task.peerIncarnation !== task.address.incarnation) return yield* new HttpError({ status: 409, message: "Native confirmation requires accepted peer ownership" });
            const boundRecipient = yield* recipient(principal, task.address);
            if (boundRecipient.workerId !== task.address.workerId || boundRecipient.repo !== task.address.repo) return yield* new HttpError({ status: 409, message: "Native confirmation worker/repository binding changed" });
          }
          if (!task.address || task.address.revision !== input.revision || !task.readback || !(yield* exact(task.address, task.readback)) || task.disposition !== "ready" || task.address.deadline <= now) return yield* new HttpError({ status: 409, message: "Confirmation requires current ready matching readback before deadline" });
          if (task.confirmedRevision === input.revision) return task;
          const address = task.address;
          task.confirmedRevision = input.revision; yield* saveReturnedTask(task);
          if (task.runnerId) {
            const runner = runnerPrincipal(task);
            yield* Effect.gen(function*() {
              if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
              (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), "release", { taskId: task.taskId, address, prompt: task.request.prompt }, 15000));
            }).pipe(Effect.catch((error) => Effect.gen(function*() {
              yield* Effect.logWarning(`broker runner release held: ${redactedCause(Cause.fail(error))}`);
              task.state = "HELD"; task.reason = "Prompt delivery uncertain; inspect assigned runner journal, never replay automatically"; (yield* save(task));
              return yield* new HttpError({ status: 409, message: task.reason });
            })));
          }
          return task;
        }),
        handoffTask: Effect.fn("Broker.handoffTask")(function*(params: CommandInput<"task.handoff">): Effect.fn.Return<CommandOutput<"task.handoff">, unknown> {
          (yield* role(principal, "coordinator")); const input = params; const task = (yield* loadTask(principal, input.taskId));
          if (!task.address || task.address.sessionId !== input.oldSessionId || task.address.incarnation !== input.oldIncarnation || input.revision <= task.address.revision) return yield* new HttpError({ status: 409, message: "Handoff old session or revision mismatch" });
          if (!task.runnerId && (task.state === "RUNNING" || task.state === "HELD" || (task.confirmedRevision !== undefined && !["SUCCEEDED", "FAILED"].includes(task.state)))) {
            task.state = "HELD"; task.reason = "Old native release is live or uncertain; reconcile its result before handoff"; (yield* save(task)); return yield* new HttpError({ status: 409, message: task.reason });
          }
          if (task.runnerId) {
            const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Cannot reconcile old runner" });
            const journal = (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), "journal", { taskId: task.taskId }));
            if (journal.taskId !== task.taskId) return yield* new HttpError({ status: 409, message: "Old runner journal task mismatch" });
            if (journal.accepted ? !["SUCCEEDED", "FAILED"].includes(journal.state ?? "") : journal.state !== "not-accepted") return yield* new HttpError({ status: 409, message: "Old runner acceptance remains live or uncertain; handoff blocked" });
            task.reconciledRunner = { runnerId: task.runnerId, state: journal.state!, observedAt: now };
          }
          const boundRecipient = yield* recipient(principal, { sessionId: input.newSessionId, incarnation: input.newIncarnation }); if (boundRecipient.repo !== task.request.repo || !boundRecipient.workerId) return yield* new HttpError({ status: 409, message: "Handoff requires verified repo and actual native worker binding" });
          task.peerSession = input.newSessionId; task.peerIncarnation = input.newIncarnation; task.address = { ...task.address, workerId: boundRecipient.workerId, sessionId: input.newSessionId, incarnation: input.newIncarnation, revision: input.revision };
          delete task.runnerId; delete task.instruction; delete task.readback; delete task.confirmedRevision;
          task.offerDecision = "accepted";
          task.disposition = "hold"; task.reason = "Handoff requires new readback"; return yield* saveReturnedTask(task);
        }),
        reportTask: Effect.fn("Broker.reportTask")(function*(params: CommandInput<"task.report">): Effect.fn.Return<CommandOutput<"task.report">, unknown> {
          (yield* role(principal, "session")); const input = params; const task = (yield* loadTask(principal, input.taskId)); const session = (yield* boundSession(principal, input));
          if (session.sessionId !== task.peerSession || session.incarnation !== task.peerIncarnation) return yield* new HttpError({ status: 403, message: "Report session mismatch" });
          if (task.runnerId || task.offerDecision !== "accepted" || !task.address || session.workerId !== task.address.workerId || task.confirmedRevision !== task.address.revision || !task.readback || !(yield* exact(task.readback, task.address))) return yield* new HttpError({ status: 409, message: "Native status requires accepted bound peer and confirmed current readback, never runner-owned work" });
          if (["SUCCEEDED", "FAILED"].includes(task.state) && input.state !== task.state) return yield* new HttpError({ status: 409, message: "Terminal native result cannot regress" });
          task.state = input.state; task.result = input.result; return yield* saveReturnedTask(task);
        }),
        heartbeatRunner: Effect.fn("Broker.heartbeatRunner")(function*(params: CommandInput<"runner.heartbeat">): Effect.fn.Return<CommandOutput<"runner.heartbeat">, unknown> {
          (yield* role(principal, "runner")); const runnerId = RunnerId.make(principal.id, { disableChecks: true }); const input = params; if (input.runnerId !== runnerId || !principal.runnerUrl) return yield* new HttpError({ status: 403, message: "Runner enrollment mismatch" });
          runners.set(input.runnerId, { runnerId: input.runnerId, owner: principal.owner, slots: input.slots, load: input.load, liveUntil: now + 15000 }); return { ok: true };
        }),
        readbackRunner: Effect.fn("Broker.readbackRunner")(function*(params: CommandInput<"runner.readback">): Effect.fn.Return<CommandOutput<"runner.readback">, unknown> {
          (yield* role(principal, "runner")); const input = params; const task = (yield* loadTask(principal, input.taskId));
          const runnerId = RunnerId.make(principal.id, { disableChecks: true });
          if (input.runnerId !== runnerId || task.runnerId !== runnerId) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
          const address = input.address;
          if (address.repo !== task.request.repo || address.ref !== task.request.ref || address.sha !== task.request.sha || address.workerId !== WorkerId.make(address.sessionId, { disableChecks: true }) || address.scope !== task.request.scope || address.restrictions !== task.request.restrictions || !(yield* exact(address.expectedEvidence, task.request.expectedEvidence)) || address.deadline !== task.request.deadline) return yield* new HttpError({ status: 409, message: "Runner readback must bind its actual native session and approved work" });
          if (task.address && !(yield* exact(task.address, address))) return yield* new HttpError({ status: 409, message: "Child binding/revision mismatch" });
          task.address = address; task.readback = address; task.disposition = "ready";
          if (task.state === "ASSIGNING" || task.state === "QUEUED") task.state = "ACCEPTED";
          (yield* save(task)); return { ok: true };
        }),
        reportRunner: Effect.fn("Broker.reportRunner")(function*(params: CommandInput<"runner.report">): Effect.fn.Return<CommandOutput<"runner.report">, unknown> {
          (yield* role(principal, "runner")); const input = params; const task = (yield* loadTask(principal, input.taskId));
          const runnerId = RunnerId.make(principal.id, { disableChecks: true });
          if (input.runnerId !== runnerId || task.runnerId !== runnerId) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
          if ((task.state === "SUCCEEDED" || task.state === "FAILED") && input.state !== task.state) return yield* new HttpError({ status: 409, message: "Terminal runner result cannot regress" });
          if (input.state === "RUNNING" && (!task.address || task.confirmedRevision !== task.address.revision)) return yield* new HttpError({ status: 409, message: "Substantive prompt not confirmed" });
          task.state = input.state; task.result = input.result; (yield* save(task)); return { ok: true };
        }),
        targetEvidence: Effect.fn("Broker.targetEvidence")(function*(params: CommandInput<"evidence.target">): Effect.fn.Return<CommandOutput<"evidence.target">, unknown> {
          (yield* role(principal, "coordinator"));
          const input = params;
          const task = (yield* loadTask(principal, input.taskId));
          task.evidenceTarget = { sha: input.sha, ref: input.ref, repositoryId: task.request.githubRepoId, requiredLanes: [...input.requiredLanes] };
          (yield* save(task)); return { target: task.evidenceTarget, executionAuthorized: false };
        }),
        collectCiEvidence: Effect.fn("Broker.collectCiEvidence")(function*(params: CommandInput<"evidence.ci">): Effect.fn.Return<CommandOutput<"evidence.ci">, unknown> {
          (yield* role(principal, "coordinator", "reviewer"));
          const task = (yield* loadTask(principal, params.taskId));
          const work = task.evidenceTarget ? { ...task.request, sha: task.evidenceTarget.sha, ref: task.evidenceTarget.ref, requiredLanes: task.evidenceTarget.requiredLanes } : task.request;
          const evidence = (yield* pollCi(work, config.githubTokenEnv ? Option.getOrUndefined(yield* Config.option(Config.String(config.githubTokenEnv))) : undefined).pipe(Effect.provideService(HttpClient.HttpClient, client)));
          (yield* appendEvidence(task, principal, "ci", work.sha, { ...evidence, policy: (yield* platform.fingerprint({ ref: work.ref, requiredLanes: [...work.requiredLanes].sort() })) })); return evidence;
        }),
        recordLocalEvidence: Effect.fn("Broker.recordLocalEvidence")(function*(params: CommandInput<"evidence.local">): Effect.fn.Return<CommandOutput<"evidence.local">, unknown> {
          (yield* role(principal, "coordinator", "runner")); const input = params; const task = (yield* loadTask(principal, input.taskId)); if (principal.roles.includes("runner") && task.runnerId !== RunnerId.make(principal.id, { disableChecks: true })) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" }); (yield* appendEvidence(task, principal, "local", input.sha, input)); return { recorded: true };
        }),
        recordDeploymentEvidence: Effect.fn("Broker.recordDeploymentEvidence")(function*(params: CommandInput<"evidence.deployment">): Effect.fn.Return<CommandOutput<"evidence.deployment">, unknown> {
          (yield* role(principal, "coordinator")); const input = params; const task = (yield* loadTask(principal, input.taskId)); (yield* appendEvidence(task, principal, "deployment", input.sha, input)); return { recorded: true };
        }),
        recordReviewEvidence: Effect.fn("Broker.recordReviewEvidence")(function*(params: CommandInput<"evidence.review">): Effect.fn.Return<CommandOutput<"evidence.review">, unknown> {
          (yield* role(principal, "reviewer")); const input = params; const task = (yield* loadTask(principal, input.taskId)); (yield* appendEvidence(task, principal, "human-review", input.sha, { ...input, reviewer: principal.id, provenance: "enrolled human reviewer credential" })); return { recorded: true };
        }),
        consentParticipant: Effect.fn("Broker.consentParticipant")(function*(params: CommandInput<"archive.participantConsent">): Effect.fn.Return<CommandOutput<"archive.participantConsent">, unknown> {
          (yield* role(principal, "session"));
          const input = params;
          const session = sessions.get(input.sessionId);
          if (!session || session.owner !== input.owner) return yield* new HttpError({ status: 404, message: "Consent session not found" });
          (yield* repo(principal, session.repo));
          if (!input.enabled && principal.owner !== input.owner) {
            const consent = (yield* execute(db.selectFrom("consents").selectAll().where("owner", "=", input.owner).where("session", "=", input.sessionId)))[0];
            if (!consent || !Schema.decodeUnknownSync(Schema.fromJsonString(JournalState.fields.participants), { onExcessProperty: "error" })(consent.participants).includes(principal.id)) return yield* new HttpError({ status: 403, message: "Not an enrolled archive participant" });
          }

            (yield* execute(db.insertInto("participant_consents").values({ owner: input.owner, session: input.sessionId, participant: principal.id, enabled: input.enabled ? 1 : 0 }).onConflict((oc) => oc.columns(["owner", "session", "participant"]).doUpdateSet((eb) => ({ enabled: eb.ref("excluded.enabled") })))));
            if (!input.enabled) {
              (yield* execute(db.updateTable("consents").set({ enabled: 0 }).where("owner", "=", input.owner).where("session", "=", input.sessionId)));
              (yield* execute(db.deleteFrom("archive").where("owner", "=", input.owner).where("session", "=", input.sessionId)));
            }

          return { participant: principal.id, enabled: input.enabled };
        }),
        consentArchive: Effect.fn("Broker.consentArchive")(function*(params: CommandInput<"archive.consent">): Effect.fn.Return<CommandOutput<"archive.consent">, unknown> {
          (yield* role(principal, "session")); const input = params;
          const session = sessions.get(input.sessionId); if (!session || session.principal !== principal.id || session.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Consent session mismatch" });
          if (input.enabled && (!input.participants.length || !input.participants.includes(principal.id))) return yield* new HttpError({ status: 400, message: "Explicit owner/participant consent required" });
          for (const participant of input.participants) {
            if (participant !== principal.id && input.enabled && !(yield* execute(db.selectFrom("participant_consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("participant", "=", participant)))[0]?.enabled) return yield* new HttpError({ status: 403, message: "Each participant must explicitly consent using their own enrolled credential" });
          }
          (yield* execute(db.insertInto("consents").values({ owner: principal.owner, session: input.sessionId, enabled: input.enabled ? 1 : 0, participants: Schema.encodeUnknownSync(Schema.fromJsonString(JournalState.fields.participants), { onExcessProperty: "error" })(input.participants) }).onConflict((oc) => oc.columns(["owner", "session"]).doUpdateSet((eb) => ({ enabled: eb.ref("excluded.enabled"), participants: eb.ref("excluded.participants") }))))); return { enabled: input.enabled };
        }),
        appendArchive: Effect.fn("Broker.appendArchive")(function*(params: CommandInput<"archive.append">): Effect.fn.Return<CommandOutput<"archive.append">, unknown> {
          (yield* role(principal, "session", "archive-writer")); const input = params;
          const consent = (yield* execute(db.selectFrom("consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId)))[0]; if (!consent?.enabled) return yield* new HttpError({ status: 403, message: "Archive enrollment revoked or absent" });
          if (principal.roles.includes("session")) { const session = sessions.get(input.sessionId); if (session?.principal !== principal.id || session.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Archive session mismatch" }); }
          const payload = Schema.encodeUnknownSync(Schema.fromJsonString(ArchivePayload), { onExcessProperty: "error" })(input.payload); if (new TextEncoder().encode(payload).byteLength > 16000) return yield* new HttpError({ status: 413, message: "Archive entry too large" });
          if ((yield* platform.fingerprint(input.payload)) !== input.digest) return yield* new HttpError({ status: 409, message: "Archive digest mismatch" });
          const prior = yield* execute(db.selectFrom("archive").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("entry", "=", input.entryId));
          const gaps = prior.some((row) => row.digest !== input.digest) ? [...input.gaps, "entry-rewrite"] : input.gaps;
          (yield* execute(db.insertInto("archive").values({ owner: principal.owner, session: input.sessionId, entry: input.entryId, digest: input.digest, payload: payload, gaps: Schema.encodeUnknownSync(Schema.fromJsonString(ArchiveEntry.fields.gaps), { onExcessProperty: "error" })(gaps), created: now }).onConflict((oc) => oc.columns(["owner", "session", "entry", "digest"]).doNothing()))); return { appended: true, digest: input.digest };
        }),
        listArchive: Effect.fn("Broker.listArchive")(function*(params: CommandInput<"archive.list">): Effect.fn.Return<CommandOutput<"archive.list">, unknown> {
          (yield* role(principal, "session", "coordinator")); const input = params; if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && sessions.get(input.sessionId)?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Archive session mismatch" });
          const rows: ArchiveRow[] = [];
          for (const row of yield* execute(db.selectFrom("archive").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("sequence", ">", ArchiveId.make(BigInt(input.cursor ?? 0), { disableChecks: true })).orderBy("sequence").limit(2))) {
            if (row.sequence > BigInt(Number.MAX_SAFE_INTEGER) || row.sequence < 0n) return yield* new HttpError({ status: 503, message: "Archive sequence exceeds the protocol's safe integer range" });
            rows.push({ sequence: Number(row.sequence), entryId: ArchiveEntryId.make(row.entry, { disableChecks: true }), digest: Hash.make(row.digest, { disableChecks: true }), payload: Schema.decodeUnknownSync(Schema.fromJsonString(ArchivePayload), { onExcessProperty: "error" })(row.payload), gaps: Schema.decodeUnknownSync(Schema.fromJsonString(ArchiveEntry.fields.gaps), { onExcessProperty: "error" })(row.gaps), createdAt: row.created });
          }
          const page: ArchiveRow[] = [];
          for (const row of rows) {
            if (new TextEncoder().encode(archiveResultText({ result: [...page, row] })).byteLength > MCP_RESULT_LIMIT) {
              if (!page.length) return yield* new HttpError({ status: 413, message: "Archive row exceeds delivery budget" });
              break;
            }
            page.push(row);
          }
          return page;
        }),
        deleteArchive: Effect.fn("Broker.deleteArchive")(function*(params: CommandInput<"archive.delete">): Effect.fn.Return<CommandOutput<"archive.delete">, unknown> {
          (yield* role(principal, "session", "coordinator")); const input = params;
          if (principal.roles.includes("session") && !principal.roles.includes("coordinator") && sessions.get(input.sessionId)?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Archive session mismatch" });
          (yield* execute(db.updateTable("consents").set({ enabled: 0 }).where("owner", "=", principal.owner).where("session", "=", input.sessionId))); (yield* execute(db.deleteFrom("archive").where("owner", "=", principal.owner).where("session", "=", input.sessionId)));
          return { revoked: true, deleted: true, backupPolicy: "journal rows and enrollment are omitted from every broker snapshot" };
        }),
      };
    };
    const maintain = Effect.fn("Broker.maintain")(function*() {
    const now = yield* Clock.currentTimeMillis;
    (yield* execute(db.deleteFrom("peers").where("expires", "<=", now)));
    (yield* execute(db.deleteFrom("archive").where("created", "<=", now - config.archiveRetentionMs)));
    for (const task of (yield* tasks())) {
      if (task.offerDecision === "pending" && (task.offerExpires ?? 0) <= now) { task.offerDecision = "expired"; (yield* save(task)); (yield* offer(task)); }
      if (task.state === "QUEUED") (yield* offer(task));
    }

    }, owned);
    const handle = Effect.fn("Broker.handle")(function*(principal: Principal, request: BrokerRequest) {
      // Network operations commit their fences before awaiting a runner, which may call task.get.
      const command = Commands[request.method];
      let offeredTask: Task | undefined;
      const execution = Effect.gen(function*() {
        const capabilities = bindCommandCapabilities(principal, yield* Clock.currentTimeMillis);
        switch (request.method) {
          case "session.list": return yield* Commands["session.list"].execute(capabilities, request.params);
          case "session.heartbeat": return yield* Commands["session.heartbeat"].execute(capabilities, request.params);
          case "session.poll": return yield* Commands["session.poll"].execute(capabilities, request.params);
          case "session.stop": return yield* Commands["session.stop"].execute(capabilities, request.params);
          case "peer.send": return yield* Commands["peer.send"].execute(capabilities, request.params);
          case "peer.ack": return yield* Commands["peer.ack"].execute(capabilities, request.params);
          case "peer.reply": {
            offeredTask = yield* Commands["peer.reply"].execute(capabilities, request.params);
            return offeredTask;
          }
          case "task.submit": {
            offeredTask = yield* Commands["task.submit"].execute(capabilities, request.params);
            return offeredTask;
          }
          case "task.get": return yield* Commands["task.get"].execute(capabilities, request.params);
          case "task.attach": return yield* Commands["task.attach"].execute(capabilities, request.params);
          case "task.control": return yield* Commands["task.control"].execute(capabilities, request.params);
          case "task.instruction": return yield* Commands["task.instruction"].execute(capabilities, request.params);
          case "task.address": return yield* Commands["task.address"].execute(capabilities, request.params);
          case "task.readback": return yield* Commands["task.readback"].execute(capabilities, request.params);
          case "task.confirm": return yield* Commands["task.confirm"].execute(capabilities, request.params);
          case "task.handoff": return yield* Commands["task.handoff"].execute(capabilities, request.params);
          case "task.report": return yield* Commands["task.report"].execute(capabilities, request.params);
          case "runner.heartbeat": return yield* Commands["runner.heartbeat"].execute(capabilities, request.params);
          case "runner.readback": return yield* Commands["runner.readback"].execute(capabilities, request.params);
          case "runner.report": return yield* Commands["runner.report"].execute(capabilities, request.params);
          case "evidence.target": return yield* Commands["evidence.target"].execute(capabilities, request.params);
          case "evidence.ci": return yield* Commands["evidence.ci"].execute(capabilities, request.params);
          case "evidence.local": return yield* Commands["evidence.local"].execute(capabilities, request.params);
          case "evidence.deployment": return yield* Commands["evidence.deployment"].execute(capabilities, request.params);
          case "evidence.review": return yield* Commands["evidence.review"].execute(capabilities, request.params);
          case "archive.participantConsent": return yield* Commands["archive.participantConsent"].execute(capabilities, request.params);
          case "archive.consent": return yield* Commands["archive.consent"].execute(capabilities, request.params);
          case "archive.append": return yield* Commands["archive.append"].execute(capabilities, request.params);
          case "archive.list": return yield* Commands["archive.list"].execute(capabilities, request.params);
          case "archive.delete": return yield* Commands["archive.delete"].execute(capabilities, request.params);
          default: {
            const exhaustive: never = request;
            throw new Error(`Unsupported broker request: ${exhaustive}`);
          }
        }
      });
      const value = yield* (command.transaction === "remote" ? execution : sql.withTransaction(execution));
      if (command.offer && offeredTask) {
        yield* offer(yield* loadTask(principal, offeredTask.taskId));
      }
      return value;
    }, owned);
    const serverContext = yield* owned(Layer.build(serverLayer(config.host, config.port)).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const server = Context.get(serverContext, HttpServer.HttpServer);
    yield* owned(mcp.serve(server, { target: "broker", authorize, handle }, { live, ready }).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    for (const value of yield* tasks()) yield* offer(value);
    // ponytail: four bounded consumers; increase only for independent runner throughput.
    for (let i = 0; i < 4; i++) yield* queue.take(assign).pipe(Effect.catch((error) => Effect.logError(`broker dispatch failed: ${redactedCause(Cause.fail(error))}`).pipe(Effect.andThen(Effect.sleep(500)))), Effect.forever, Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    yield* maintain().pipe(Effect.catch((error) => Effect.logError(`broker maintenance failed: ${redactedCause(Cause.fail(error))}`)), Effect.repeat(Schedule.spaced("1 second")), Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    yield* snapshot().pipe(Effect.catch(() => Effect.void), Effect.repeat(Schedule.spaced("1 minute")), Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    return { config, sql, sessions, runners, server, authorize, handle, snapshot, maintain };
  }),
}) {
  static layer(config: BrokerConfig) {
    return Layer.effect(this, this.make).pipe(Layer.provide(Layer.succeed(BrokerConfiguration, config)));
  }
}
