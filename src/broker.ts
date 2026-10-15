import { Cause, Clock, Config, Context, Deferred, Effect, Exit, Layer, Option, Schedule, Schema, Scope, Semaphore } from "effect";
import * as PersistedQueue from "effect/persistence/PersistedQueue";
import { layerDispatchStore } from "./dispatch-store.ts";
import { HttpClient, HttpServer } from "effect/http";
import { Commands, Address, Work, RunnerId, WorkerId, Repo, BrokerConfig, Task, LiveSession, ArchivePayload, ArchiveEntry, ArchiveRow, JournalState, Json, JsonValue, Poll, Delivery, EvidenceObservation, HttpError, RunnerRejectionStateEnum, RoleEnum, DispositionEnum, OfferDecisionEnum, SessionLivenessEnum, BrokerRequest, type Command, type CommandInput, type CommandOutput, type Principal, type Role, type SessionKey, type LiveRunner } from "./contracts.ts";
import { Platform, redactedCause } from "./platform.ts";
import { Mcp, serverLayer, MCP_RESULT_LIMIT } from "./mcp.ts";
import { pollCi } from "./evidence.ts";
import { SnapshotStore, dumpBrokerDatabase } from "./backup.ts";
import { assertBrokerSchema } from "./database.ts";
import { ArchiveId, TaskId, type SessionId, type Evidence as StoredEvidence } from "./generated/database/types.ts";
import { TaskStateEnum, RunnerStateEnum } from "./generated/database/enums.ts";
import { awaitDatabaseLoss, DatabaseService, type DatabaseError } from "./database-service.ts";

const BrokerConfiguration = Context.Service<BrokerConfig>("coordinator/BrokerConfiguration");
const pollResultText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.pollSession.structuredOutput)), { onExcessProperty: "error" });
const sessionResultText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.listSessions.structuredOutput)), { onExcessProperty: "error" });
const taskResultText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.getTask.structuredOutput)), { onExcessProperty: "error" });
const archiveResultText = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Commands.listArchive.structuredOutput)), { onExcessProperty: "error" });
export class Broker extends Context.Service<Broker>()("coordinator/Broker", {
  make: Effect.gen(function*() {
    const config = yield* BrokerConfiguration;
    const platform = yield* Platform;
    const mcp = yield* Mcp;
    const client = yield* HttpClient.HttpClient;
    if (config.archiveRetentionMs <= 0 || config.backupRetentionMs <= 0) return yield* new HttpError({ status: 400, message: "Invalid broker limits" });
    if (new Set(config.principals.map((p) => p.id)).size !== config.principals.length) return yield* new HttpError({ status: 400, message: "Distinct enrolled principals required" });
    const hashes = config.principals.flatMap((p) => p.controlTokenHash ? [p.tokenHash, p.controlTokenHash] : [p.tokenHash]);
    if (new Set(hashes).size !== hashes.length || config.principals.some((p) => p.roles.includes(RoleEnum.runner) && !p.controlTokenHash)) return yield* new HttpError({ status: 400, message: "Distinct runner enrollment and broker-only control credentials required" });
    yield* platform.fs.makeDirectory(config.backupDirectory, { recursive: true, mode: 0o700 });
    yield* platform.fs.chmod(config.backupDirectory, 0o700);
    const fatal = yield* Deferred.make<never, HttpError>();
    let ownershipLost = false;
    const owned = <A, E, R>(effect: Effect.Effect<A, E | DatabaseError, R>) => Effect.suspend<A, E | HttpError, R>(() =>
      ownershipLost ? Deferred.await(fatal) : effect.pipe(
        Effect.catchTag("DatabaseError", (error) => Effect.logError(`broker database failed: ${error._tag}`).pipe(
          Effect.andThen(Effect.fail(new HttpError({ status: 503, message: "Broker database operation unavailable" }))),
        )),
        Effect.raceFirst(Deferred.await(fatal)),
      ));
    const ledgerScope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void));
    const revoke = Effect.gen(function*() {
      ownershipLost = true;
      yield* Deferred.fail(fatal, new HttpError({ status: 503, message: "Broker database ownership unavailable" }));
      yield* Scope.close(ledgerScope, Exit.void);
    });
    const databaseContext = yield* owned(Layer.build(DatabaseService.layer()).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const database = Context.get(databaseContext, DatabaseService);
    yield* awaitDatabaseLoss(database).pipe(
      Effect.catch((error) => Effect.gen(function*() {
        yield* Effect.logError(`broker PostgreSQL ownership failed: ${redactedCause(Cause.fail(error))}`);
        yield* revoke;
      })),
      Effect.forkScoped({ startImmediately: true })
    );
    yield* owned(assertBrokerSchema(database));
    const services = yield* owned(Layer.build(PersistedQueue.layer.pipe(Layer.provideMerge(layerDispatchStore(TaskId)), Layer.provideMerge(Layer.succeed(DatabaseService, database)))).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const sessions = new Map<SessionId, LiveSession>();
    const runners = new Map<RunnerId, LiveRunner>();
    const versions = new WeakMap<Task, string>();
    for (const row of yield* database.query((db) => db.selectFrom("sessions").selectAll())) {
      const value = yield* Schema.decodeEffect(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(row.data);
      value.verified = false; value.liveUntil = 0; value.polledAt = 0;
      sessions.set(value.sessionId, value);
    }
    for (const file of yield* platform.fs.readDirectory(config.backupDirectory)) if (file.endsWith(".partial")) yield* platform.fs.remove(platform.path.join(config.backupDirectory, file));
    const queue = yield* PersistedQueue.make({ name: "coordinator.dispatch", schema: TaskId, maxAttempts: Number.MAX_SAFE_INTEGER }).pipe(Effect.provideContext(services));
    const exact = <A, B>(a: A, b: B) => Effect.gen(function*() {
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
        yield* database.query((db) => db.selectNoFrom((eb) => eb.val(1).as("ready"))).pipe(
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
          yield* dumpBrokerDatabase(partial, true).pipe(Effect.provideService(DatabaseService, database), Effect.provideService(Platform, platform));
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
    const row = (yield* database.query((db) => db.selectFrom("tasks").selectAll().where("id", "=", id)))[0];
    if (!row) return yield* new HttpError({ status: 404, message: "Task not found" });
    const task = yield* Schema.decodeEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data);
    versions.set(task, row.data);
    if (task.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Owner mismatch" });
    (yield* repo(principal, task.request.repo));
    return task;

    });
    const tasks = Effect.fn("Broker.tasks")(function*() {
      return yield* Effect.forEach(yield* database.query((db) => db.selectFrom("tasks").selectAll()), (r) => Effect.gen(function*() {
        const value = yield* Schema.decodeEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(r.data);
        versions.set(value, r.data);
        return value;
      }));
    });
    const save = Effect.fn("Broker.save")(function*(task: Task) {
      task.updatedAt = yield* Clock.currentTimeMillis;
      const data = Schema.encodeSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task);
      const previous = versions.get(task);
      const changed = previous === undefined
        ? yield* database.query((db) => db.insertInto("tasks").values({ id: task.taskId, data: data }).onConflict((oc) => oc.column("id").doNothing()).returning("id"))
        : yield* database.query((db) => db.updateTable("tasks").set({ data: data }).where("id", "=", task.taskId).where("data", "=", previous).returning("id"));
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
    if (task.state !== TaskStateEnum.QUEUED || task.offerDecision === OfferDecisionEnum.accepted) return;
    if (task.offerDecision === OfferDecisionEnum.pending && (task.offerExpires ?? 0) > now) return;
    if (task.offerDecision === OfferDecisionEnum.pending) { task.offerDecision = OfferDecisionEnum.expired; (yield* save(task)); }
    yield* queue.offer(task.taskId, { id: task.fingerprint });

    });
    const runnerPrincipal = (task: Task) => { return config.principals.find((p) => p.roles.includes(RoleEnum.runner) && RunnerId.make(p.id, { disableChecks: true }) === task.runnerId && p.owner === task.owner); };
    const assign = Effect.fn("Broker.assign")(function*(id: TaskId) {
    while (true) {
      const now = yield* Clock.currentTimeMillis;
      const task = (yield* tasks()).find((t) => t.taskId === id);
      if (!task || [TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED, TaskStateEnum.ACCEPTED, TaskStateEnum.RUNNING].includes(task.state) || task.offerDecision === OfferDecisionEnum.accepted) return;
      if (task.runnerId !== undefined && [TaskStateEnum.ASSIGNING, TaskStateEnum.HELD].includes(task.state)) {
        const principal = runnerPrincipal(task);
        if (!principal?.runnerUrl) { task.state = TaskStateEnum.HELD; task.reason = "Assigned runner no longer enrolled"; (yield* save(task)); yield* Effect.sleep(1000); continue; }
        const runnerUrl = principal.runnerUrl;
        const runnerId = RunnerId.make(principal.id, { disableChecks: true });
        yield* Effect.gen(function*() {
          const result = yield* mcp.call(runnerUrl, yield* runnerToken(principal), Commands.journalTask, { taskId: id });
          const current = (yield* tasks()).find((t) => t.taskId === id);
          if (!current || current.runnerId !== runnerId || [TaskStateEnum.RUNNING, TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(current.state)) return;
          if (result.taskId !== id) return yield* new HttpError({ status: 409, message: "Runner journal task mismatch" });
          if (result.accepted) { current.state = result.state === RunnerStateEnum.HELD || result.state === RunnerStateEnum.PROMPT_INTENT ? TaskStateEnum.HELD : TaskStateEnum.ACCEPTED; current.reason = result.reason; yield* save(current); return; }
          if (result.state === RunnerRejectionStateEnum.notAccepted) { current.state = TaskStateEnum.QUEUED; delete current.runnerId; yield* save(current); }
          else { current.state = TaskStateEnum.HELD; current.reason = "Runner acceptance unknown; reassignment blocked"; yield* save(current); }
        }).pipe(Effect.catch((error) => Effect.gen(function*() {
          yield* Effect.logWarning(`broker runner journal reconciliation held: ${redactedCause(Cause.fail(error))}`);
          const current = (yield* tasks()).find((t) => t.taskId === id);
          if (!current || current.runnerId !== runnerId || [TaskStateEnum.RUNNING, TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(current.state)) return;
          current.state = TaskStateEnum.HELD; current.reason = "Runner acceptance unknown; reassignment blocked"; yield* save(current);
        })));
        yield* Effect.sleep(1000); continue;
      }
      if (task.state === TaskStateEnum.HELD) { yield* Effect.sleep(1000); continue; }
      const candidate = [...runners.values()].find((r) => r.owner === task.owner && r.liveUntil > now && r.load < r.slots && config.principals.some((p) => p.roles.includes(RoleEnum.runner) && RunnerId.make(p.id, { disableChecks: true }) === r.runnerId && p.repos.includes(task.request.repo) && p.runnerUrl));
      if (!candidate) { yield* Effect.sleep(500); continue; }
      const principal = config.principals.find((p) => p.roles.includes(RoleEnum.runner) && RunnerId.make(p.id, { disableChecks: true }) === candidate.runnerId)!;
      delete task.peerSession; delete task.peerIncarnation; delete task.offerExpires;
      delete task.address; delete task.readback; delete task.confirmedRevision;
      task.state = TaskStateEnum.ASSIGNING; task.runnerId = candidate.runnerId; (yield* save(task));
      candidate.load++;
      yield* Effect.gen(function*() {
        const result = (yield* mcp.call(principal.runnerUrl!, (yield* runnerToken(principal)), Commands.acceptTask, task.request, 15000));
        if (result.taskId !== id) return yield* new HttpError({ status: 409, message: "Runner acceptance task mismatch" });
        const current = (yield* tasks()).find((t) => t.taskId === id);
        if (!current || current.runnerId !== candidate.runnerId || [TaskStateEnum.RUNNING, TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(current.state)) return;
        if (result.accepted) { if (current.state === TaskStateEnum.ASSIGNING) current.state = TaskStateEnum.ACCEPTED; (yield* save(current)); return; }
        if (result.state === RunnerRejectionStateEnum.full || result.state === RunnerRejectionStateEnum.notAccepted) { current.state = TaskStateEnum.QUEUED; delete current.runnerId; (yield* save(current)); }
        else { current.state = TaskStateEnum.HELD; current.reason = result.reason ?? "Runner declined unsafe material"; (yield* save(current)); }
      }).pipe(Effect.catch((error) => Effect.gen(function*() {
        yield* Effect.logWarning(`broker runner acceptTask held: ${redactedCause(Cause.fail(error))}`);
        const current = (yield* tasks()).find((t) => t.taskId === id);
        if (!current || current.runnerId !== candidate.runnerId || [TaskStateEnum.RUNNING, TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(current.state)) return;
        current.state = TaskStateEnum.HELD; current.reason = "Runner acceptance unknown; reassignment blocked"; (yield* save(current));
      })));
      yield* Effect.sleep(500);
    }


    });
    const runnerToken = Effect.fn("Broker.runnerToken")(function*(principal: Principal) {
    // Outbound secrets are separate files, never in the ledger/config/response or process arguments.
    const directory = Option.getOrUndefined(yield* Config.option(Config.String("COORDINATOR_RUNNER_TOKEN_DIRECTORY")));
    if (!directory) return yield* new HttpError({ status: 500, message: "Runner credential directory missing" });
    const token = (yield* platform.privateFile(platform.path.join(directory, yield* platform.digest(principal.id))));
    if (!principal.controlTokenHash || !(yield* platform.checkToken(token, principal.controlTokenHash))) return yield* new HttpError({ status: 500, message: "Runner control credential rotation mismatch" });
    return token;

    });
    const appendEvidence = Effect.fn("Broker.appendEvidence")(function*(task: Task, principal: Principal, kind: string, sha: typeof StoredEvidence.fields.sha.Type, data: Json) {
    const now = yield* Clock.currentTimeMillis;
    (yield* database.query((db) => db.insertInto("evidence").values({ task: task.taskId, kind: kind, sha: sha, principal: principal.id, data: Schema.encodeSync(Schema.fromJsonString(Json), { onExcessProperty: "error" })(data), observed: now })));

    });
    const bindCommandCapabilities = (principal: Principal, now: number) => {
      const readableTask = Effect.fn("Broker.readableTask")(function*(taskId: TaskId) {
        (yield* role(principal, RoleEnum.coordinator, RoleEnum.session, RoleEnum.reviewer, RoleEnum.runner)); const task = (yield* loadTask(principal, taskId));
        if (principal.roles.includes(RoleEnum.runner) && task.runnerId !== RunnerId.make(principal.id, { disableChecks: true })) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
        if (principal.roles.includes(RoleEnum.session) && !principal.roles.includes(RoleEnum.coordinator) && !principal.roles.includes(RoleEnum.reviewer)) { const recipient = task.peerSession === undefined ? undefined : sessions.get(task.peerSession); if (recipient?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Task is not addressed to this session" }); }
        return task;
      });
      return {
        listSessions: Effect.fn("Broker.listSessions")(function*({ cursor }: CommandInput<typeof Commands.listSessions>) { yield* role(principal, RoleEnum.coordinator, RoleEnum.session);
        const visible = [...sessions.values()].filter((session) => session.owner === principal.owner && (cursor === undefined || session.sessionId > cursor))
          .sort((a, b) => a.sessionId < b.sessionId ? -1 : a.sessionId > b.sessionId ? 1 : 0);
        const page: CommandOutput<typeof Commands.listSessions> = { sessions: [] };
        for (const session of visible) {
          const { sessionId, incarnation, machineId, host, repo, workerId, liveUntil } = session;
          page.sessions.push({ sessionId, incarnation, machineId, host, repo, workerId, liveUntil,
            status: session.verified && liveUntil > now && session.polledAt >= now - 10000 ? SessionLivenessEnum.live : SessionLivenessEnum.stale });
          if (new TextEncoder().encode(sessionResultText({ result: { ...page, nextCursor: sessionId } })).byteLength > MCP_RESULT_LIMIT) {
            page.sessions.pop();
            const last = page.sessions.at(-1);
            if (!last) return yield* new HttpError({ status: 413, message: "Session metadata exceeds delivery budget" });
            return { sessions: page.sessions, nextCursor: last.sessionId };
          }
        }
        return page; }),
        heartbeatSession: Effect.fn("Broker.heartbeatSession")(function*(params: CommandInput<typeof Commands.heartbeatSession>) { // Repository is detected presence metadata, not session admission authority.
        (yield* role(principal, RoleEnum.session)); const input = params;
        const existing = sessions.get(input.sessionId);
        if (existing && (existing.principal !== principal.id || existing.owner !== principal.owner)) return yield* new HttpError({ status: 403, message: "Session already enrolled by another principal" });
        if (existing && existing.incarnation !== input.incarnation && existing.verified && existing.liveUntil > now) return yield* new HttpError({ status: 409, message: "Prior session incarnation still live" });
        const session: LiveSession = { ...input, owner: principal.owner, principal: principal.id, verified: true, liveUntil: now + 30000, polledAt: existing?.incarnation === input.incarnation ? existing.polledAt : 0 };
        sessions.set(input.sessionId, session);
        (yield* database.query((db) => db.insertInto("sessions").values({ id: input.sessionId, data: Schema.encodeSync(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(session) }).onConflict((oc) => oc.column("id").doUpdateSet((eb) => ({ data: eb.ref("excluded.data") })))));
        return { liveUntil: session.liveUntil }; }),
        pollSession: Effect.fn("Broker.pollSession")(function*(params: CommandInput<typeof Commands.pollSession>) { (yield* role(principal, RoleEnum.session)); const input = params; const session = (yield* boundSession(principal, input)); session.polledAt = now;
        const messages: Delivery[] = yield* Effect.forEach(yield* database.query((db) => db.selectFrom("peers").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation).where("expires", ">", now).orderBy("expires").limit(8)), (r) => Effect.gen(function*() {
          const payload = yield* Schema.decodeEffect(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(r.payload);
          return { messageId: r.id, incarnation: r.incarnation, payload, expiresAt: r.expires };
        }));
        const bound = (yield* tasks()).filter((task) => task.owner === principal.owner && task.peerSession === input.sessionId && task.peerIncarnation === input.incarnation);
        const result: Poll = { messages: [], offers: [], releases: [], archiveEnabled: Boolean((yield* database.query((db) => db.selectFrom("consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId)))[0]?.enabled) };
        for (const message of messages) { const next = { ...result, messages: [...result.messages, message] }; if (new TextEncoder().encode(pollResultText({ result: next })).byteLength < 60000) result.messages.push(message); }
        for (const task of bound) {
          const key = task.runnerId === undefined && task.offerDecision === OfferDecisionEnum.pending && (task.offerExpires ?? 0) > now ? "offers"
            : task.runnerId === undefined && task.offerDecision === OfferDecisionEnum.accepted && task.address && session.workerId === task.address.workerId && session.repo === task.address.repo && task.address.deadline > now && task.confirmedRevision === task.address.revision && task.disposition === DispositionEnum.ready && task.state === TaskStateEnum.ACCEPTED ? "releases" : undefined;
          if (key && new TextEncoder().encode(pollResultText({ result: { ...result, [key]: [...result[key], task] } })).byteLength < 60000) result[key].push(task);
        }
        return result; }),
        stopSession: Effect.fn("Broker.stopSession")(function*(params: CommandInput<typeof Commands.stopSession>) { (yield* role(principal, RoleEnum.session)); const input = params; const session = (yield* boundSession(principal, input, false)); session.verified = false; session.liveUntil = 0;
        (yield* database.query((db) => db.deleteFrom("peers").where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation)));
        (yield* database.query((db) => db.deleteFrom("sessions").where("id", "=", input.sessionId))); const result: CommandOutput<typeof Commands.stopSession> = { stopped: true }; return result; }),
        sendPeer: Effect.fn("Broker.sendPeer")(function*(params: CommandInput<typeof Commands.sendPeer>) { (yield* role(principal, RoleEnum.session, RoleEnum.coordinator)); const input = params;
        (yield* recipient(principal, input));
        if (new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(input.payload)).byteLength > 8000) return yield* new HttpError({ status: 413, message: "Peer payload too large" });
        const hash = (yield* platform.fingerprint({ ...input, owner: principal.owner, sender: principal.id })); const prior = (yield* database.query((db) => db.selectFrom("peers").selectAll().where("id", "=", input.messageId)))[0];
        if (prior && prior.fingerprint !== hash) return yield* new HttpError({ status: 409, message: "Message ID conflict" });
        if (!prior) (yield* database.query((db) => db.insertInto("peers").values({ id: input.messageId, owner: principal.owner, sender: principal.id, session: input.sessionId, incarnation: input.incarnation, payload: Schema.encodeSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })(input.payload), fingerprint: hash, expires: now + input.ttlMs })));
        const result: CommandOutput<typeof Commands.sendPeer> = { delivered: false, committed: true, messageId: input.messageId }; return result; }),
        acknowledgePeer: Effect.fn("Broker.acknowledgePeer")(function*(params: CommandInput<typeof Commands.acknowledgePeer>) { (yield* role(principal, RoleEnum.session)); const input = params; (yield* boundSession(principal, input));
        (yield* database.query((db) => db.deleteFrom("peers").where("id", "=", input.messageId).where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("incarnation", "=", input.incarnation))); const result: CommandOutput<typeof Commands.acknowledgePeer> = { handled: true }; return result; }),
        replyPeer: Effect.fn("Broker.replyPeer")(function*(params: CommandInput<typeof Commands.replyPeer>) { (yield* role(principal, RoleEnum.session)); const input = params; const session = (yield* boundSession(principal, input));
        const task = (yield* loadTask(principal, input.taskId));
        if (task.peerSession !== input.sessionId || task.peerIncarnation !== input.incarnation || task.offerDecision !== OfferDecisionEnum.pending || (task.offerExpires ?? 0) <= now) return yield* new HttpError({ status: 409, message: "Offer expired or binding mismatch" });
        if (session.workerId !== task.request.workerId || session.repo !== task.request.repo || task.runnerId !== undefined) return yield* new HttpError({ status: 409, message: "Offer worker/repository binding changed" });
        task.offerDecision = input.decision; if (input.decision === OfferDecisionEnum.accepted) task.state = TaskStateEnum.ACCEPTED; return yield* saveReturnedTask(task); }),
        submitTask: Effect.fn("Broker.submitTask")(function*(params: CommandInput<typeof Commands.submitTask>) { (yield* role(principal, RoleEnum.coordinator)); const work = params; (yield* repo(principal, work.repo));
        if (new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(Work), { onExcessProperty: "error" })(work)).byteLength > 28000) return yield* new HttpError({ status: 413, message: "Approved work exceeds delivery budget" });
        const prior = (yield* database.query((db) => db.selectFrom("tasks").selectAll().where("id", "=", work.taskId)))[0];
        if (prior) { const task = (yield* loadTask(principal, work.taskId)); if (task.fingerprint !== (yield* platform.fingerprint(work))) return yield* new HttpError({ status: 409, message: "Task ID fingerprint conflict" }); return task; }
        if (work.deadline <= now || !work.prompt || !work.ref || !work.scope || !work.requiredLanes.length) return yield* new HttpError({ status: 400, message: "Approved bounded scope, deadline, ref and explicit CI lanes required" });
        if (config.repositoryIds[work.repo] !== work.githubRepoId) return yield* new HttpError({ status: 409, message: "Inspected enrollment repository ID mismatch" });
        const peer = work.bypassPeer ? undefined : [...sessions.values()].find((session) => session.owner === principal.owner && session.repo === work.repo && session.workerId === work.workerId && session.verified && session.liveUntil > now && session.polledAt >= now - 10000);
        const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: (yield* platform.fingerprint(work)), request: work, state: TaskStateEnum.QUEUED, createdAt: now, updatedAt: now, offerDecision: work.bypassPeer ? OfferDecisionEnum.bypass : peer ? OfferDecisionEnum.pending : OfferDecisionEnum.expired, ...(peer ? { peerSession: peer.sessionId, peerIncarnation: peer.incarnation, offerExpires: now + 15000 } : {}) };
        return yield* saveReturnedTask(task); }),
        getTask: Effect.fn("Broker.getTask")(function*(params: CommandInput<typeof Commands.getTask>) { const task = yield* readableTask(params.taskId);
        const target = task.evidenceTarget ?? task.request;
        const policy = (yield* platform.fingerprint({ ref: target.ref, requiredLanes: [...target.requiredLanes].sort() }));
        const observations: EvidenceObservation[] = yield* Effect.forEach(yield* database.query((db) => db.selectFrom("evidence").selectAll().where("id", "in", db.selectFrom("evidence").select((eb) => eb.fn.max<StoredEvidence["id"]>("id").as("id")).where("task", "=", task.taskId).groupBy("kind")).orderBy("id", "desc")), (row) => Effect.gen(function*() {
          const data = yield* Schema.decodeEffect(Schema.fromJsonString(Json), { onExcessProperty: "error" })(row.data);
          return { kind: row.kind, sha: row.sha, principal: row.principal, data, observedAt: row.observed, stale: row.sha !== target.sha || (row.kind === "ci" && data.policy !== policy) };
        }));
        const evidence: EvidenceObservation[] = [];
        for (const observation of observations) if (new TextEncoder().encode(taskResultText({ result: { ...task, evidence: [...evidence, observation] } })).byteLength < 60000) evidence.push(observation);
        return { ...task, evidence }; }),
        attachTask: Effect.fn("Broker.attachTask")(function*(params: CommandInput<typeof Commands.attachTask>) { const task = yield* readableTask(params.taskId);
        if (task.runnerId === undefined) return yield* new HttpError({ status: 409, message: "Only runner-owned children are attachable" });
        const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
        yield* role(principal, RoleEnum.coordinator);
        return yield* mcp.call(runner.runnerUrl, yield* runnerToken(runner), Commands.inspectTask, params); }),
        controlTask: Effect.fn("Broker.controlTask")(function*(params: CommandInput<typeof Commands.controlTask>) { const task = yield* readableTask(params.taskId);
        if (task.runnerId === undefined) return yield* new HttpError({ status: 409, message: "Only runner-owned children are attachable" });
        const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
        yield* role(principal, RoleEnum.coordinator);
        return yield* mcp.call(runner.runnerUrl, yield* runnerToken(runner), Commands.commandTask, params); }),
        instructTask: Effect.fn("Broker.instructTask")(function*(params: CommandInput<typeof Commands.instructTask>) { (yield* role(principal, RoleEnum.coordinator));
        const input = params;
        const task = (yield* loadTask(principal, input.taskId));
        if (task.runnerId !== undefined) return yield* new HttpError({ status: 409, message: "Durably accepted launch material is immutable; later agent instructions require a reconciled handoff" });
        if (task.offerDecision !== OfferDecisionEnum.accepted) return yield* new HttpError({ status: 409, message: "Agent peer must explicitly accept before later instructions" });
        if (task.confirmedRevision !== undefined && ![TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(task.state)) return yield* new HttpError({ status: 409, message: "Prior agent release is live or uncertain; reconcile before another instruction" });
        if (!task.address || input.address.revision <= task.address.revision || input.address.messageId === task.address.messageId || input.address.repo !== task.request.repo || input.address.workerId !== task.address.workerId || input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation || input.address.deadline <= now) return yield* new HttpError({ status: 409, message: "New approved message/revision and exact current recipient required" });
        (yield* recipient(principal, input.address));
        task.instruction = { prompt: input.prompt, address: input.address }; task.address = input.address;
        delete task.readback; delete task.confirmedRevision; task.disposition = DispositionEnum.hold; task.reason = "Later instruction awaits new readback"; task.state = TaskStateEnum.ACCEPTED;
        if (new TextEncoder().encode(taskResultText({ result: task })).byteLength > 50000) return yield* new HttpError({ status: 413, message: "Addressed task exceeds delivery budget" });
        return yield* saveReturnedTask(task); }),
        addressTask: Effect.fn("Broker.addressTask")(function*(params: CommandInput<typeof Commands.addressTask>) { (yield* role(principal, RoleEnum.coordinator)); const input = params; const task = (yield* loadTask(principal, input.taskId)); const approved = task.instruction?.address ?? task.request;
        if (task.runnerId === undefined) {
          if (task.offerDecision !== OfferDecisionEnum.accepted || task.peerSession === undefined || task.peerIncarnation === undefined) return yield* new HttpError({ status: 409, message: "Agent address requires accepted peer ownership" });
          const boundRecipient = yield* recipient(principal, input.address);
          if (boundRecipient.workerId !== input.address.workerId || boundRecipient.repo !== input.address.repo) return yield* new HttpError({ status: 409, message: "Agent recipient worker/repository binding changed" });
        }
        if (input.address.repo !== approved.repo || input.address.sha !== approved.sha || input.address.ref !== approved.ref || input.address.workerId !== (task.address?.workerId ?? approved.workerId)) return yield* new HttpError({ status: 409, message: "Address does not match approved repo/ref/SHA/current worker" });
        if (input.address.scope !== approved.scope || input.address.restrictions !== approved.restrictions || !(yield* exact(input.address.expectedEvidence, approved.expectedEvidence)) || input.address.deadline !== approved.deadline) return yield* new HttpError({ status: 409, message: "Address must preserve approved consequential details" });
        if (input.address.revision <= (task.address?.revision ?? 0)) { if ((yield* exact(input.address, task.address))) return task; return yield* new HttpError({ status: 409, message: "Address revision must increase" }); }
        if (task.confirmedRevision !== undefined && ![TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(task.state)) return yield* new HttpError({ status: 409, message: "Confirmed release cannot be reset; reconcile its result first" });
        if (task.runnerId !== undefined && task.address && (input.address.sessionId !== task.address.sessionId || input.address.incarnation !== task.address.incarnation)) return yield* new HttpError({ status: 409, message: "Use reconciled handoff for a new session" });
        if (task.runnerId === undefined && task.peerSession !== undefined && (input.address.sessionId !== task.peerSession || input.address.incarnation !== task.peerIncarnation)) return yield* new HttpError({ status: 409, message: "Peer address mismatch" });
        task.address = input.address; delete task.readback; delete task.confirmedRevision; task.disposition = DispositionEnum.hold; task.reason = "Awaiting matching readback";
        if (task.instruction) task.instruction.address = input.address;
        if (new TextEncoder().encode(taskResultText({ result: task.runnerId !== undefined ? { ...task, readback: input.address, disposition: DispositionEnum.ready, reason: undefined } : task })).byteLength > MCP_RESULT_LIMIT) return yield* new HttpError({ status: 413, message: "Task exceeds delivery budget" });
        if (task.runnerId !== undefined) {
          yield* save(task);
          const runner = runnerPrincipal(task);
          if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable for corrected readback" });
          const readback = (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), Commands.prepareTask, { taskId: task.taskId, address: input.address }));
          if (!(yield* exact(readback, input.address))) return yield* new HttpError({ status: 409, message: "Corrected runner readback mismatch" });
          task.readback = readback; task.disposition = DispositionEnum.ready; delete task.reason;
        }
        return yield* saveReturnedTask(task); }),
        readbackTask: Effect.fn("Broker.readbackTask")(function*(params: CommandInput<typeof Commands.readbackTask>) { (yield* role(principal, RoleEnum.session)); const input = params; const task = (yield* loadTask(principal, input.taskId));
        const session = (yield* boundSession(principal, input.address));
        if (task.runnerId !== undefined || task.offerDecision !== OfferDecisionEnum.accepted || session.workerId !== input.address.workerId || session.repo !== input.address.repo) return yield* new HttpError({ status: 409, message: "Agent readback requires accepted bound peer ownership, never runner-owned work" });
        if (task.peerSession !== input.address.sessionId || task.peerIncarnation !== input.address.incarnation) return yield* new HttpError({ status: 403, message: "Task/session binding mismatch" });
        if (!task.address || !(yield* exact(task.address, input.address))) return yield* new HttpError({ status: 409, message: "Readback mismatch; correct address and read back again" });
        if (task.confirmedRevision !== undefined) { if (input.disposition === DispositionEnum.ready && (yield* exact(task.readback, input.address))) return task; return yield* new HttpError({ status: 409, message: "Confirmed release cannot be reset by readback" }); }
        if (input.disposition !== DispositionEnum.ready && !input.reason) return yield* new HttpError({ status: 400, message: "Disposition reason required" });
        if (input.disposition === DispositionEnum.hold && !input.nextCheck) return yield* new HttpError({ status: 400, message: "Hold next-check required" });
        task.readback = input.address; task.disposition = input.disposition; task.reason = input.reason; task.nextCheck = input.nextCheck; delete task.confirmedRevision; return yield* saveReturnedTask(task); }),
        confirmTask: Effect.fn("Broker.confirmTask")(function*(params: CommandInput<typeof Commands.confirmTask>) { (yield* role(principal, RoleEnum.coordinator)); const input = params; const task = (yield* loadTask(principal, input.taskId));
        if (task.runnerId === undefined) {
          if (task.offerDecision !== OfferDecisionEnum.accepted || !task.address || task.peerSession !== task.address.sessionId || task.peerIncarnation !== task.address.incarnation) return yield* new HttpError({ status: 409, message: "Agent confirmation requires accepted peer ownership" });
          const boundRecipient = yield* recipient(principal, task.address);
          if (boundRecipient.workerId !== task.address.workerId || boundRecipient.repo !== task.address.repo) return yield* new HttpError({ status: 409, message: "Agent confirmation worker/repository binding changed" });
        }
        if (!task.address || task.address.revision !== input.revision || !task.readback || !(yield* exact(task.address, task.readback)) || task.disposition !== DispositionEnum.ready || task.address.deadline <= now) return yield* new HttpError({ status: 409, message: "Confirmation requires current ready matching readback before deadline" });
        if (task.confirmedRevision === input.revision) return task;
        const address = task.address;
        task.confirmedRevision = input.revision; yield* saveReturnedTask(task);
        if (task.runnerId !== undefined) {
          const runner = runnerPrincipal(task);
          yield* Effect.gen(function*() {
            if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Runner unavailable" });
            (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), Commands.releaseTask, { taskId: task.taskId, address, prompt: task.request.prompt }, 15000));
          }).pipe(Effect.catch((error) => Effect.gen(function*() {
            yield* Effect.logWarning(`broker runner release held: ${redactedCause(Cause.fail(error))}`);
            task.state = TaskStateEnum.HELD; task.reason = "Prompt delivery uncertain; inspect assigned runner journal, never replay automatically"; (yield* save(task));
            return yield* new HttpError({ status: 409, message: task.reason });
          })));
        }
        return task; }),
        handoffTask: Effect.fn("Broker.handoffTask")(function*(params: CommandInput<typeof Commands.handoffTask>) { (yield* role(principal, RoleEnum.coordinator)); const input = params; const task = (yield* loadTask(principal, input.taskId));
        if (!task.address || task.address.sessionId !== input.oldSessionId || task.address.incarnation !== input.oldIncarnation || input.revision <= task.address.revision) return yield* new HttpError({ status: 409, message: "Handoff old session or revision mismatch" });
        if (task.runnerId === undefined && (task.state === TaskStateEnum.RUNNING || task.state === TaskStateEnum.HELD || (task.confirmedRevision !== undefined && ![TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(task.state)))) {
          task.state = TaskStateEnum.HELD; task.reason = "Old agent release is live or uncertain; reconcile its result before handoff"; (yield* save(task)); return yield* new HttpError({ status: 409, message: task.reason });
        }
        if (task.runnerId !== undefined) {
          const runner = runnerPrincipal(task); if (!runner?.runnerUrl) return yield* new HttpError({ status: 409, message: "Cannot reconcile old runner" });
          const journal = (yield* mcp.call(runner.runnerUrl, (yield* runnerToken(runner)), Commands.journalTask, { taskId: task.taskId }));
          if (journal.taskId !== task.taskId) return yield* new HttpError({ status: 409, message: "Old runner journal task mismatch" });
          if (journal.accepted ? journal.state !== RunnerStateEnum.SUCCEEDED && journal.state !== RunnerStateEnum.FAILED : journal.state !== RunnerRejectionStateEnum.notAccepted) return yield* new HttpError({ status: 409, message: "Old runner acceptance remains live or uncertain; handoff blocked" });
          task.reconciledRunner = { runnerId: task.runnerId, state: journal.state, observedAt: now };
        }
        const boundRecipient = yield* recipient(principal, { sessionId: input.newSessionId, incarnation: input.newIncarnation }); if (boundRecipient.repo !== task.request.repo || boundRecipient.workerId === undefined) return yield* new HttpError({ status: 409, message: "Handoff requires verified repo and actual agent worker binding" });
        task.peerSession = input.newSessionId; task.peerIncarnation = input.newIncarnation; task.address = { ...task.address, workerId: boundRecipient.workerId, sessionId: input.newSessionId, incarnation: input.newIncarnation, revision: input.revision };
        delete task.runnerId; delete task.instruction; delete task.readback; delete task.confirmedRevision;
        task.offerDecision = OfferDecisionEnum.accepted;
        task.disposition = DispositionEnum.hold; task.reason = "Handoff requires new readback"; return yield* saveReturnedTask(task); }),
        reportTask: Effect.fn("Broker.reportTask")(function*(params: CommandInput<typeof Commands.reportTask>) { (yield* role(principal, RoleEnum.session)); const input = params; const task = (yield* loadTask(principal, input.taskId)); const session = (yield* boundSession(principal, input));
        if (session.sessionId !== task.peerSession || session.incarnation !== task.peerIncarnation) return yield* new HttpError({ status: 403, message: "Report session mismatch" });
        if (task.runnerId !== undefined || task.offerDecision !== OfferDecisionEnum.accepted || !task.address || session.workerId !== task.address.workerId || task.confirmedRevision !== task.address.revision || !task.readback || !(yield* exact(task.readback, task.address))) return yield* new HttpError({ status: 409, message: "Agent status requires accepted bound peer and confirmed current readback, never runner-owned work" });
        if ([TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED].includes(task.state) && input.state !== task.state) return yield* new HttpError({ status: 409, message: "Terminal agent result cannot regress" });
        task.state = input.state; task.result = input.result; return yield* saveReturnedTask(task); }),
        heartbeatRunner: Effect.fn("Broker.heartbeatRunner")(function*(params: CommandInput<typeof Commands.heartbeatRunner>) { (yield* role(principal, RoleEnum.runner)); const runnerId = RunnerId.make(principal.id, { disableChecks: true }); const input = params; if (input.runnerId !== runnerId || !principal.runnerUrl) return yield* new HttpError({ status: 403, message: "Runner enrollment mismatch" });
        runners.set(input.runnerId, { runnerId: input.runnerId, owner: principal.owner, slots: input.slots, load: input.load, liveUntil: now + 15000 }); const result: CommandOutput<typeof Commands.heartbeatRunner> = { ok: true }; return result; }),
        readbackRunner: Effect.fn("Broker.readbackRunner")(function*(params: CommandInput<typeof Commands.readbackRunner>) { (yield* role(principal, RoleEnum.runner)); const input = params; const task = (yield* loadTask(principal, input.taskId));
        const runnerId = RunnerId.make(principal.id, { disableChecks: true });
        if (input.runnerId !== runnerId || task.runnerId !== runnerId) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
        const address = input.address;
        if (address.repo !== task.request.repo || address.ref !== task.request.ref || address.sha !== task.request.sha || address.workerId !== WorkerId.make(address.sessionId, { disableChecks: true }) || address.scope !== task.request.scope || address.restrictions !== task.request.restrictions || !(yield* exact(address.expectedEvidence, task.request.expectedEvidence)) || address.deadline !== task.request.deadline) return yield* new HttpError({ status: 409, message: "Runner readback must bind its actual agent session and approved work" });
        if (task.address && !(yield* exact(task.address, address))) return yield* new HttpError({ status: 409, message: "Child binding/revision mismatch" });
        task.address = address; task.readback = address; task.disposition = DispositionEnum.ready;
        if (task.state === TaskStateEnum.ASSIGNING || task.state === TaskStateEnum.QUEUED) task.state = TaskStateEnum.ACCEPTED;
        (yield* save(task)); const result: CommandOutput<typeof Commands.heartbeatRunner> = { ok: true }; return result; }),
        reportRunner: Effect.fn("Broker.reportRunner")(function*(params: CommandInput<typeof Commands.reportRunner>) { (yield* role(principal, RoleEnum.runner)); const input = params; const task = (yield* loadTask(principal, input.taskId));
        const runnerId = RunnerId.make(principal.id, { disableChecks: true });
        if (input.runnerId !== runnerId || task.runnerId !== runnerId) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" });
        if ((task.state === TaskStateEnum.SUCCEEDED || task.state === TaskStateEnum.FAILED) && input.state !== task.state) return yield* new HttpError({ status: 409, message: "Terminal runner result cannot regress" });
        if (input.state === TaskStateEnum.RUNNING && (!task.address || task.confirmedRevision !== task.address.revision)) return yield* new HttpError({ status: 409, message: "Substantive prompt not confirmed" });
        task.state = input.state; task.result = input.result; (yield* save(task)); const result: CommandOutput<typeof Commands.heartbeatRunner> = { ok: true }; return result; }),
        targetEvidence: Effect.fn("Broker.targetEvidence")(function*(params: CommandInput<typeof Commands.targetEvidence>) { (yield* role(principal, RoleEnum.coordinator));
        const input = params;
        const task = (yield* loadTask(principal, input.taskId));
        task.evidenceTarget = { sha: input.sha, ref: input.ref, repositoryId: task.request.githubRepoId, requiredLanes: [...input.requiredLanes] };
        (yield* save(task)); const result: CommandOutput<typeof Commands.targetEvidence> = { target: task.evidenceTarget, executionAuthorized: false }; return result; }),
        collectCiEvidence: Effect.fn("Broker.collectCiEvidence")(function*(params: CommandInput<typeof Commands.collectCiEvidence>) { (yield* role(principal, RoleEnum.coordinator, RoleEnum.reviewer));
        const task = (yield* loadTask(principal, params.taskId));
        const work = task.evidenceTarget ? { ...task.request, sha: task.evidenceTarget.sha, ref: task.evidenceTarget.ref, requiredLanes: task.evidenceTarget.requiredLanes } : task.request;
        const evidence = (yield* pollCi(work, config.githubTokenEnv ? Option.getOrUndefined(yield* Config.option(Config.String(config.githubTokenEnv))) : undefined).pipe(Effect.provideService(HttpClient.HttpClient, client)));
        (yield* appendEvidence(task, principal, "ci", work.sha, { ...evidence, policy: (yield* platform.fingerprint({ ref: work.ref, requiredLanes: [...work.requiredLanes].sort() })) })); return evidence; }),
        recordLocalEvidence: Effect.fn("Broker.recordLocalEvidence")(function*(params: CommandInput<typeof Commands.recordLocalEvidence>) { (yield* role(principal, RoleEnum.coordinator, RoleEnum.runner)); const input = params; const task = (yield* loadTask(principal, input.taskId)); if (principal.roles.includes(RoleEnum.runner) && task.runnerId !== RunnerId.make(principal.id, { disableChecks: true })) return yield* new HttpError({ status: 403, message: "Assigned runner mismatch" }); (yield* appendEvidence(task, principal, "local", input.sha, input)); const result: CommandOutput<typeof Commands.recordLocalEvidence> = { recorded: true }; return result; }),
        recordDeploymentEvidence: Effect.fn("Broker.recordDeploymentEvidence")(function*(params: CommandInput<typeof Commands.recordDeploymentEvidence>) { (yield* role(principal, RoleEnum.coordinator)); const input = params; const task = (yield* loadTask(principal, input.taskId)); (yield* appendEvidence(task, principal, "deployment", input.sha, input)); const result: CommandOutput<typeof Commands.recordLocalEvidence> = { recorded: true }; return result; }),
        recordReviewEvidence: Effect.fn("Broker.recordReviewEvidence")(function*(params: CommandInput<typeof Commands.recordReviewEvidence>) { (yield* role(principal, RoleEnum.reviewer)); const input = params; const task = (yield* loadTask(principal, input.taskId)); (yield* appendEvidence(task, principal, "human-review", input.sha, { ...input, reviewer: principal.id, provenance: "enrolled human reviewer credential" })); const result: CommandOutput<typeof Commands.recordLocalEvidence> = { recorded: true }; return result; }),
        consentParticipant: Effect.fn("Broker.consentParticipant")(function*(params: CommandInput<typeof Commands.consentParticipant>) { (yield* role(principal, RoleEnum.session));
        const input = params;
        const session = sessions.get(input.sessionId);
        if (!session || session.owner !== input.owner) return yield* new HttpError({ status: 404, message: "Consent session not found" });
        (yield* repo(principal, session.repo));
        if (!input.enabled && principal.owner !== input.owner) {
          const consent = (yield* database.query((db) => db.selectFrom("consents").selectAll().where("owner", "=", input.owner).where("session", "=", input.sessionId)))[0];
          if (!consent || !(yield* Schema.decodeEffect(Schema.fromJsonString(JournalState.fields.participants), { onExcessProperty: "error" })(consent.participants)).includes(principal.id)) return yield* new HttpError({ status: 403, message: "Not an enrolled archive participant" });
        }
        
          (yield* database.query((db) => db.insertInto("participant_consents").values({ owner: input.owner, session: input.sessionId, participant: principal.id, enabled: input.enabled ? 1 : 0 }).onConflict((oc) => oc.columns(["owner", "session", "participant"]).doUpdateSet((eb) => ({ enabled: eb.ref("excluded.enabled") })))));
          if (!input.enabled) {
            (yield* database.query((db) => db.updateTable("consents").set({ enabled: 0 }).where("owner", "=", input.owner).where("session", "=", input.sessionId)));
            (yield* database.query((db) => db.deleteFrom("archive").where("owner", "=", input.owner).where("session", "=", input.sessionId)));
          }
        
        return { participant: principal.id, enabled: input.enabled }; }),
        consentArchive: Effect.fn("Broker.consentArchive")(function*(params: CommandInput<typeof Commands.consentArchive>) { (yield* role(principal, RoleEnum.session)); const input = params;
        const session = sessions.get(input.sessionId); if (!session || session.principal !== principal.id || session.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Consent session mismatch" });
        if (input.enabled && (!input.participants.length || !input.participants.includes(principal.id))) return yield* new HttpError({ status: 400, message: "Explicit owner/participant consent required" });
        for (const participant of input.participants) {
          if (participant !== principal.id && input.enabled && !(yield* database.query((db) => db.selectFrom("participant_consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("participant", "=", participant)))[0]?.enabled) return yield* new HttpError({ status: 403, message: "Each participant must explicitly consent using their own enrolled credential" });
        }
        (yield* database.query((db) => db.insertInto("consents").values({ owner: principal.owner, session: input.sessionId, enabled: input.enabled ? 1 : 0, participants: Schema.encodeSync(Schema.fromJsonString(JournalState.fields.participants), { onExcessProperty: "error" })(input.participants) }).onConflict((oc) => oc.columns(["owner", "session"]).doUpdateSet((eb) => ({ enabled: eb.ref("excluded.enabled"), participants: eb.ref("excluded.participants") }))))); return { enabled: input.enabled }; }),
        appendArchive: Effect.fn("Broker.appendArchive")(function*(params: CommandInput<typeof Commands.appendArchive>) { (yield* role(principal, RoleEnum.session, RoleEnum.archiveWriter)); const input = params;
        const consent = (yield* database.query((db) => db.selectFrom("consents").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId)))[0]; if (!consent?.enabled) return yield* new HttpError({ status: 403, message: "Archive enrollment revoked or absent" });
        if (principal.roles.includes(RoleEnum.session)) { const session = sessions.get(input.sessionId); if (session?.principal !== principal.id || session.owner !== principal.owner) return yield* new HttpError({ status: 403, message: "Archive session mismatch" }); }
        const payload = Schema.encodeSync(Schema.fromJsonString(ArchivePayload), { onExcessProperty: "error" })(input.payload); if (new TextEncoder().encode(payload).byteLength > 16000) return yield* new HttpError({ status: 413, message: "Archive entry too large" });
        if ((yield* platform.fingerprint(input.payload)) !== input.digest) return yield* new HttpError({ status: 409, message: "Archive digest mismatch" });
        const prior = yield* database.query((db) => db.selectFrom("archive").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("entry", "=", input.entryId));
        const gaps = prior.some((row) => row.digest !== input.digest) ? [...input.gaps, "entry-rewrite"] : input.gaps;
        (yield* database.query((db) => db.insertInto("archive").values({ owner: principal.owner, session: input.sessionId, entry: input.entryId, digest: input.digest, payload: payload, gaps: Schema.encodeSync(Schema.fromJsonString(ArchiveEntry.fields.gaps), { onExcessProperty: "error" })(gaps), created: now }).onConflict((oc) => oc.columns(["owner", "session", "entry", "digest"]).doNothing()))); const result: CommandOutput<typeof Commands.appendArchive> = { appended: true, digest: input.digest }; return result; }),
        listArchive: Effect.fn("Broker.listArchive")(function*(params: CommandInput<typeof Commands.listArchive>) { (yield* role(principal, RoleEnum.session, RoleEnum.coordinator)); const input = params; if (principal.roles.includes(RoleEnum.session) && !principal.roles.includes(RoleEnum.coordinator) && sessions.get(input.sessionId)?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Archive session mismatch" });
        const rows: ArchiveRow[] = [];
        for (const row of yield* database.query((db) => db.selectFrom("archive").selectAll().where("owner", "=", principal.owner).where("session", "=", input.sessionId).where("sequence", ">", ArchiveId.make(BigInt(input.cursor ?? 0), { disableChecks: true })).orderBy("sequence").limit(2))) {
          if (row.sequence > BigInt(Number.MAX_SAFE_INTEGER) || row.sequence < 0n) return yield* new HttpError({ status: 503, message: "Archive sequence exceeds the protocol's safe integer range" });
          const payload = yield* Schema.decodeEffect(Schema.fromJsonString(ArchivePayload), { onExcessProperty: "error" })(row.payload);
          const gaps = yield* Schema.decodeEffect(Schema.fromJsonString(ArchiveEntry.fields.gaps), { onExcessProperty: "error" })(row.gaps);
          rows.push({ sequence: Number(row.sequence), entryId: row.entry, digest: row.digest, payload, gaps, createdAt: row.created });
        }
        const page: ArchiveRow[] = [];
        for (const row of rows) {
          if (new TextEncoder().encode(archiveResultText({ result: [...page, row] })).byteLength > MCP_RESULT_LIMIT) {
            if (!page.length) return yield* new HttpError({ status: 413, message: "Archive row exceeds delivery budget" });
            break;
          }
          page.push(row);
        }
        return page; }),
        deleteArchive: Effect.fn("Broker.deleteArchive")(function*(params: CommandInput<typeof Commands.deleteArchive>) { (yield* role(principal, RoleEnum.session, RoleEnum.coordinator)); const input = params;
        if (principal.roles.includes(RoleEnum.session) && !principal.roles.includes(RoleEnum.coordinator) && sessions.get(input.sessionId)?.principal !== principal.id) return yield* new HttpError({ status: 403, message: "Archive session mismatch" });
        (yield* database.query((db) => db.updateTable("consents").set({ enabled: 0 }).where("owner", "=", principal.owner).where("session", "=", input.sessionId))); (yield* database.query((db) => db.deleteFrom("archive").where("owner", "=", principal.owner).where("session", "=", input.sessionId)));
        const result: CommandOutput<typeof Commands.deleteArchive> = { revoked: true, deleted: true, backupPolicy: "journal rows and enrollment are omitted from every broker snapshot" }; return result; }),
      };
    };
    const maintain = Effect.fn("Broker.maintain")(function*() {
    const now = yield* Clock.currentTimeMillis;
    (yield* database.query((db) => db.deleteFrom("peers").where("expires", "<=", now)));
    (yield* database.query((db) => db.deleteFrom("archive").where("created", "<=", now - config.archiveRetentionMs)));
    for (const task of (yield* tasks())) {
      if (task.offerDecision === OfferDecisionEnum.pending && (task.offerExpires ?? 0) <= now) { task.offerDecision = OfferDecisionEnum.expired; (yield* save(task)); (yield* offer(task)); }
      if (task.state === TaskStateEnum.QUEUED) (yield* offer(task));
    }

    }, owned);
    const handle = Effect.fn("Broker.handle")(function*(principal: Principal, request: BrokerRequest) {
      // Network operations commit their fences before awaiting a runner, which may call task.get.
      let offeredTask: Task | undefined;
      const capabilities = Effect.map(Clock.currentTimeMillis, (now) => bindCommandCapabilities(principal, now));
      const execute = <A, E, R>(command: Command, invoke: (bound: Effect.Success<typeof capabilities>) => Effect.Effect<A, E, R>) => Effect.gen(function*() {
        const execution = Effect.flatMap(capabilities, invoke);
        const value = yield* (command.transaction === "remote" ? execution : database.transaction(execution));
        if (command.offer && offeredTask) yield* offer(yield* loadTask(principal, offeredTask.taskId));
        return value;
      });
      return yield* BrokerRequest.match(request, {
        [Commands.listSessions.name]: ({ params }: typeof Commands.listSessions.request.Type) => execute(Commands.listSessions, (capabilities) => Commands.listSessions.executeDecoded(capabilities, params)),
        [Commands.heartbeatSession.name]: ({ params }: typeof Commands.heartbeatSession.request.Type) => execute(Commands.heartbeatSession, (capabilities) => Commands.heartbeatSession.executeDecoded(capabilities, params)),
        [Commands.pollSession.name]: ({ params }: typeof Commands.pollSession.request.Type) => execute(Commands.pollSession, (capabilities) => Commands.pollSession.executeDecoded(capabilities, params)),
        [Commands.stopSession.name]: ({ params }: typeof Commands.stopSession.request.Type) => execute(Commands.stopSession, (capabilities) => Commands.stopSession.executeDecoded(capabilities, params)),
        [Commands.sendPeer.name]: ({ params }: typeof Commands.sendPeer.request.Type) => execute(Commands.sendPeer, (capabilities) => Commands.sendPeer.executeDecoded(capabilities, params)),
        [Commands.acknowledgePeer.name]: ({ params }: typeof Commands.acknowledgePeer.request.Type) => execute(Commands.acknowledgePeer, (capabilities) => Commands.acknowledgePeer.executeDecoded(capabilities, params)),
        [Commands.replyPeer.name]: ({ params }: typeof Commands.replyPeer.request.Type) => execute(Commands.replyPeer, (capabilities) => Commands.replyPeer.executeDecoded(capabilities, params).pipe(Effect.tap((task) => Effect.sync(() => { offeredTask = task; })))),
        [Commands.submitTask.name]: ({ params }: typeof Commands.submitTask.request.Type) => execute(Commands.submitTask, (capabilities) => Commands.submitTask.executeDecoded(capabilities, params).pipe(Effect.tap((task) => Effect.sync(() => { offeredTask = task; })))),
        [Commands.getTask.name]: ({ params }: typeof Commands.getTask.request.Type) => execute(Commands.getTask, (capabilities) => Commands.getTask.executeDecoded(capabilities, params)),
        [Commands.attachTask.name]: ({ params }: typeof Commands.attachTask.request.Type) => execute(Commands.attachTask, (capabilities) => Commands.attachTask.executeDecoded(capabilities, params)),
        [Commands.controlTask.name]: ({ params }: typeof Commands.controlTask.request.Type) => execute(Commands.controlTask, (capabilities) => Commands.controlTask.executeDecoded(capabilities, params)),
        [Commands.instructTask.name]: ({ params }: typeof Commands.instructTask.request.Type) => execute(Commands.instructTask, (capabilities) => Commands.instructTask.executeDecoded(capabilities, params)),
        [Commands.addressTask.name]: ({ params }: typeof Commands.addressTask.request.Type) => execute(Commands.addressTask, (capabilities) => Commands.addressTask.executeDecoded(capabilities, params)),
        [Commands.readbackTask.name]: ({ params }: typeof Commands.readbackTask.request.Type) => execute(Commands.readbackTask, (capabilities) => Commands.readbackTask.executeDecoded(capabilities, params)),
        [Commands.confirmTask.name]: ({ params }: typeof Commands.confirmTask.request.Type) => execute(Commands.confirmTask, (capabilities) => Commands.confirmTask.executeDecoded(capabilities, params)),
        [Commands.handoffTask.name]: ({ params }: typeof Commands.handoffTask.request.Type) => execute(Commands.handoffTask, (capabilities) => Commands.handoffTask.executeDecoded(capabilities, params)),
        [Commands.reportTask.name]: ({ params }: typeof Commands.reportTask.request.Type) => execute(Commands.reportTask, (capabilities) => Commands.reportTask.executeDecoded(capabilities, params)),
        [Commands.heartbeatRunner.name]: ({ params }: typeof Commands.heartbeatRunner.request.Type) => execute(Commands.heartbeatRunner, (capabilities) => Commands.heartbeatRunner.executeDecoded(capabilities, params)),
        [Commands.readbackRunner.name]: ({ params }: typeof Commands.readbackRunner.request.Type) => execute(Commands.readbackRunner, (capabilities) => Commands.readbackRunner.executeDecoded(capabilities, params)),
        [Commands.reportRunner.name]: ({ params }: typeof Commands.reportRunner.request.Type) => execute(Commands.reportRunner, (capabilities) => Commands.reportRunner.executeDecoded(capabilities, params)),
        [Commands.targetEvidence.name]: ({ params }: typeof Commands.targetEvidence.request.Type) => execute(Commands.targetEvidence, (capabilities) => Commands.targetEvidence.executeDecoded(capabilities, params)),
        [Commands.collectCiEvidence.name]: ({ params }: typeof Commands.collectCiEvidence.request.Type) => execute(Commands.collectCiEvidence, (capabilities) => Commands.collectCiEvidence.executeDecoded(capabilities, params)),
        [Commands.recordLocalEvidence.name]: ({ params }: typeof Commands.recordLocalEvidence.request.Type) => execute(Commands.recordLocalEvidence, (capabilities) => Commands.recordLocalEvidence.executeDecoded(capabilities, params)),
        [Commands.recordDeploymentEvidence.name]: ({ params }: typeof Commands.recordDeploymentEvidence.request.Type) => execute(Commands.recordDeploymentEvidence, (capabilities) => Commands.recordDeploymentEvidence.executeDecoded(capabilities, params)),
        [Commands.recordReviewEvidence.name]: ({ params }: typeof Commands.recordReviewEvidence.request.Type) => execute(Commands.recordReviewEvidence, (capabilities) => Commands.recordReviewEvidence.executeDecoded(capabilities, params)),
        [Commands.consentParticipant.name]: ({ params }: typeof Commands.consentParticipant.request.Type) => execute(Commands.consentParticipant, (capabilities) => Commands.consentParticipant.executeDecoded(capabilities, params)),
        [Commands.consentArchive.name]: ({ params }: typeof Commands.consentArchive.request.Type) => execute(Commands.consentArchive, (capabilities) => Commands.consentArchive.executeDecoded(capabilities, params)),
        [Commands.appendArchive.name]: ({ params }: typeof Commands.appendArchive.request.Type) => execute(Commands.appendArchive, (capabilities) => Commands.appendArchive.executeDecoded(capabilities, params)),
        [Commands.listArchive.name]: ({ params }: typeof Commands.listArchive.request.Type) => execute(Commands.listArchive, (capabilities) => Commands.listArchive.executeDecoded(capabilities, params)),
        [Commands.deleteArchive.name]: ({ params }: typeof Commands.deleteArchive.request.Type) => execute(Commands.deleteArchive, (capabilities) => Commands.deleteArchive.executeDecoded(capabilities, params)),
      });
    }, owned);
    const serverContext = yield* owned(Layer.build(serverLayer(config.host, config.port)).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    const server = Context.get(serverContext, HttpServer.HttpServer);
    yield* owned(mcp.serve(server, { target: "broker", authorize, handle }, { live, ready }).pipe(Effect.provideService(Scope.Scope, ledgerScope)));
    for (const value of yield* tasks()) yield* offer(value);
    // ponytail: four bounded consumers; increase only for independent runner throughput.
    for (let i = 0; i < 4; i++) yield* queue.take(assign).pipe(Effect.catch((error) => Effect.logError(`broker dispatch failed: ${redactedCause(Cause.fail(error))}`).pipe(Effect.andThen(Effect.sleep(500)))), Effect.forever, Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    yield* maintain().pipe(Effect.catch((error) => Effect.logError(`broker maintenance failed: ${redactedCause(Cause.fail(error))}`)), Effect.repeat(Schedule.spaced("1 second")), Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    yield* snapshot().pipe(Effect.catch(() => Effect.void), Effect.repeat(Schedule.spaced("1 minute")), Effect.forkScoped, Effect.provideService(Scope.Scope, ledgerScope));
    return { config, database, sessions, runners, server, authorize, handle, snapshot, maintain };
  }),
}) {
  static layer(config: BrokerConfig) {
    return Layer.effect(this, this.make).pipe(Layer.provide(Layer.succeed(BrokerConfiguration, config)));
  }
}
