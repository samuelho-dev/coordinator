import assert from "node:assert/strict";
import { Effect, Option, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { Platform } from "../src/platform.ts";
import { LegacySqlite, importLegacySqlite, legacyBrokerSql } from "./import-sqlite.ts";
import { privateRecoveryPath } from "../src/backup.ts";
import { BrokerConfig, GitHubRepositoryId, IncarnationId, LiveSession, MachineId, OwnerId, PrincipalId, Repo, RunnerConfig, RunnerEvent, RunnerResult, RunnerId, SessionId, Sha, Task, TaskId, UserId, Work, WorkerId } from "../src/contracts.ts";
import { provisionPostgres, postgresAdmin, dropPostgres } from "../test/postgres-fixture.ts";
import { makeDatabase } from "../src/kysely.ts";

process.umask(0o077);
const huge = 9007199254740993n, floor = huge + 20n;
const secret = "legacy-secret-prompt-and-provider-token";
const nativeBinding = { host: "omp", sessionId: "legacy-session", incarnation: "legacy-native-incarnation", generation: 3 };
const terminalEvent = { type: "native_lifecycle", binding: nativeBinding, runId: "legacy-run", nativeCursor: 4, kind: "run_settled", status: "completed" };
const seedPath = process.argv[2] === "--seed" ? process.argv[3] : undefined;
const seedVersion = process.argv[4] === "1" ? 1 : 2;
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  if (seedPath) {
    yield* privateRecoveryPath(seedPath, "file", true);
    return yield* Effect.gen(function*() {
      const legacy = yield* LegacySqlite;
      yield* legacy.execute("PRAGMA journal_mode=WAL");
      yield* legacy.execute("PRAGMA synchronous=FULL");
      yield* legacy.execute("PRAGMA fullfsync=ON");
      yield* legacy.execute("PRAGMA wal_autocheckpoint=0");
      for (const statement of legacyBrokerSql(seedVersion)) yield* legacy.execute(statement);
      yield* legacy.execute("INSERT INTO coordinator_broker_migrations(migration_id,name) VALUES (1,'relational_baseline')");
      yield* legacy.execute("INSERT INTO effect_queue_migrations(migration_id,name) VALUES (1,'create_table')");
      if (seedVersion === 2) yield* legacy.execute("INSERT INTO effect_queue_migrations(migration_id,name) VALUES (2,'upgrade_schema')");
      const repo = Repo.make("github.com/proof/legacy");
      const work: Work = { taskId: TaskId.make("legacy-accepted"), repo, ref: "refs/heads/proof", sha: Sha.make("a".repeat(40)), launcher: "omp", prompt: "never dispatch imported accepted work", scope: "offline import", restrictions: "fixture only", expectedEvidence: ["held"], deadline: 1900000000000, workerId: WorkerId.make("proof-worker"), githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["proof"], bypassPeer: true, approved: true };
      const task: Task = { taskId: work.taskId, owner: OwnerId.make("proof-owner"), fingerprint: yield* platform.fingerprint(work), request: work, runnerId: RunnerId.make("import-runner"), state: "ACCEPTED", offerDecision: "accepted", createdAt: 1780000000000, updatedAt: 1780000000001 };
      yield* legacy.execute("INSERT INTO tasks(id,data) VALUES (?,?)", [task.taskId, Schema.encodeSync(Schema.fromJsonString(Task))(task)]);
      const session: LiveSession = { sessionId: SessionId.make("private-session"), incarnation: IncarnationId.make("incarnation"), machineId: MachineId.make("private-machine"), host: "omp", repo, owner: OwnerId.make("proof-owner"), principal: PrincipalId.make("private-principal"), verified: true, liveUntil: 1900000000000, polledAt: 1780000000000 };
      yield* legacy.execute("INSERT INTO sessions(id,data) VALUES (?,?)", [session.sessionId, Schema.encodeSync(Schema.fromJsonString(LiveSession))(session)]);
      yield* legacy.execute("INSERT INTO peers(id,owner,sender,session,incarnation,payload,fingerprint,expires) VALUES ('private-peer','owner','sender','session','incarnation','{\"private\":\"opaque payload\"}',?,1780000000000.25)", ["b".repeat(64)]);
      yield* legacy.execute("INSERT INTO consents(owner,session,enabled,participants) VALUES ('owner','session',1,'[\"participant\"]')");
      yield* legacy.execute("INSERT INTO participant_consents(owner,session,participant,enabled) VALUES ('owner','session','participant',0)");
      yield* legacy.execute("INSERT INTO archive(sequence,owner,session,entry,digest,payload,gaps,created) VALUES (?,'owner','session','entry',?,'private transcript','[]',1780000000000.25)", [huge, "b".repeat(64)]);
      yield* legacy.execute("INSERT INTO evidence(id,task,kind,sha,principal,data,observed) VALUES (?,'legacy-accepted','readback',?,'principal','opaque evidence',1780000000000.5)", [huge, "a".repeat(40)]);
      const queueColumns = seedVersion === 1 ? "completed" : "state,visible_at";
      const queueValues = seedVersion === 1 ? "1" : "'completed','2026-10-13 01:02:03.123456'";
      // The native queue owner is a valid RFC v4 UUID; import must preserve it exactly, not normalize it.
      yield* legacy.execute(`INSERT INTO effect_queue(sequence,id,queue_name,element,${queueColumns},attempts,last_failure,acquired_at,acquired_by,created_at,updated_at) VALUES (?,'legacy-accepted','coordinator.dispatch','\"legacy-accepted\"',${queueValues},3,'uncertain prior result','2026-10-13 01:02:03.123456','aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee','2026-10-13 01:02:03.123456','2026-10-13 01:02:03.123456')`, [huge]);
      yield* legacy.execute("UPDATE sqlite_sequence SET seq=? WHERE name IN ('archive','evidence','effect_queue')", [floor]);
      const directory = platform.path.dirname(seedPath);
      return yield* Effect.gen(function*() {
        const journal = yield* LegacySqlite;
        yield* journal.execute("PRAGMA journal_mode=WAL");
        yield* journal.execute("PRAGMA synchronous=FULL");
        yield* journal.execute("PRAGMA wal_autocheckpoint=0");
        for (const statement of [
          "CREATE TABLE coordinator_runner_migrations (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)",
          "CREATE TABLE tasks (taskId TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, work TEXT NOT NULL,worktree TEXT NOT NULL,state TEXT NOT NULL,occupied INTEGER NOT NULL,pid INTEGER,incarnation TEXT NOT NULL,address TEXT,promptId TEXT,reason TEXT,result TEXT,detached INTEGER NOT NULL DEFAULT 0,nativeBinding TEXT,nativeRunId TEXT,nativeCursor INTEGER NOT NULL DEFAULT 0)",
          "CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT,taskId TEXT NOT NULL,payload TEXT NOT NULL)",
          "CREATE INDEX events_task ON events(taskId,seq)",
          "INSERT INTO coordinator_runner_migrations(migration_id,name) VALUES (1,'relational_baseline'),(2,'native_mcp_binding')",
        ]) yield* journal.execute(statement);
        yield* journal.execute("INSERT INTO tasks(rowid,taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (7,?,?,?,?,'ACCEPTED',1,'legacy-incarnation')", [work.taskId, task.fingerprint, Schema.encodeSync(Schema.fromJsonString(Work))(work), platform.path.join(directory, "children", yield* platform.digest(work.taskId), "repo")]);
        yield* journal.execute("INSERT INTO events(seq,taskId,payload) VALUES (12,?,?)", [work.taskId, '{"type":"runner_state","state":"ACCEPTED"}']);
        const terminalWork: Work = { ...work, taskId: TaskId.make("legacy-terminal"), prompt: secret };
        yield* journal.execute("PRAGMA user_version=" + (seedVersion === 1 ? 1 : 2));
        yield* journal.execute("INSERT INTO tasks(rowid,taskId,fingerprint,work,worktree,state,occupied,incarnation,promptId,reason,result,nativeBinding,nativeRunId,nativeCursor) VALUES (8,?,?,?,?,'SUCCEEDED',0,'legacy-incarnation','legacy-submission',?,?,?,'legacy-run',4)", [
          terminalWork.taskId, yield* platform.fingerprint(terminalWork), Schema.encodeSync(Schema.fromJsonString(Work))(terminalWork),
          platform.path.join(directory, "children", yield* platform.digest(terminalWork.taskId), "repo"),
          seedVersion === 1 ? secret : "historical diagnostic omitted",
          JSON.stringify({ hostSettled: true, evidenceVerified: false, ...(seedVersion === 1 ? { exitCode: null, signal: null, output: secret, messages: [{ content: secret }] } : {}) }),
          JSON.stringify(nativeBinding),
        ]);
        yield* journal.execute("INSERT INTO events(seq,taskId,payload) VALUES (13,?,?)", [terminalWork.taskId,
          JSON.stringify({ ...terminalEvent, ...(seedVersion === 1 ? { reason: secret, prompt: secret, output: secret } : { contentOmitted: true }) })]);
        const scalarWork: Work = { ...work, taskId: TaskId.make("legacy-scalar-result") };
        yield* journal.execute("INSERT INTO tasks(rowid,taskId,fingerprint,work,worktree,state,occupied,incarnation,result) VALUES (9,?,?,?,?,'SUCCEEDED',0,'legacy-incarnation',?)", [
          scalarWork.taskId, yield* platform.fingerprint(scalarWork), Schema.encodeSync(Schema.fromJsonString(Work))(scalarWork),
          platform.path.join(directory, "children", yield* platform.digest(scalarWork.taskId), "repo"), seedVersion === 1 ? JSON.stringify(secret) : null,
        ]);
        yield* journal.execute("UPDATE sqlite_sequence SET seq=30 WHERE name='events'");
        console.log("committed-wal-ready");
        return yield* Effect.never;
      }).pipe(Effect.provide(LegacySqlite.layer(platform.path.join(directory, "legacy-runner", "runner.sqlite"))));
    }).pipe(Effect.provide(LegacySqlite.layer(seedPath)));
  }
  const root = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-legacy-import-proof-" });
  for (const version of [1, 2]) {
    const directory = platform.path.join(root, `v${version}`);
    yield* platform.fs.makeDirectory(directory, { mode: 0o700 });
    const source = platform.path.join(directory, "ledger.sqlite");
    for (const name of ["legacy-runner", "runner-pg", "runner-state", "children", "model"]) yield* platform.fs.makeDirectory(platform.path.join(directory, name), { mode: 0o700 });
    const runnerSource = platform.path.join(directory, "legacy-runner", "runner.sqlite");
    yield* Effect.scoped(Effect.gen(function*() {
      const seed = yield* platform.spawner.spawn(ChildProcess.make(process.execPath, ["--import", "tsx", new URL(import.meta.url).pathname, "--seed", source, String(version)], {
        stdin: "ignore", forceKillAfter: "1 second",
      }));
      yield* Effect.forkScoped(Stream.runDrain(seed.stderr));
      const marker = yield* seed.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runHead);
      assert.equal(Option.isSome(marker) && marker.value === "committed-wal-ready", true, "seed must acknowledge committed writes while SQLite remains open");
      assert.equal(yield* seed.isRunning, true, "seed must stay alive until its owner crashes it");
      yield* seed.kill({ killSignal: "SIGKILL" });
      const killed = yield* Effect.flip(seed.exitCode);
      assert.equal(killed.reason.cause instanceof Error, true);
      if (killed.reason.cause instanceof Error) assert.match(killed.reason.cause.message, /SIGKILL/, "seed must exit by an uncatchable signal, not normal teardown");
      assert.equal(yield* seed.isRunning, false);
    }).pipe(Effect.timeout("60 seconds")));
    assert.equal(yield* platform.fs.exists(`${source}-wal`), true, "crashed SQLite worker must leave committed source WAL");
    const committedWal = yield* platform.fs.readFile(`${source}-wal`);
    assert.ok(committedWal.byteLength > 32, "committed WAL must contain frames, not an empty header");
    const urlFile = yield* provisionPostgres(directory);
    yield* Effect.addFinalizer(() => dropPostgres(urlFile).pipe(Effect.orDie));
    const runnerUrlFile = yield* provisionPostgres(platform.path.join(directory, "runner-pg"));
    yield* Effect.addFinalizer(() => dropPostgres(runnerUrlFile).pipe(Effect.orDie));
    const tokenFile = platform.path.join(directory, "runner.token");
    yield* platform.fs.writeFileString(tokenFile, "fixture-import-token", { mode: 0o600 });
    const runner: RunnerConfig = { databaseUrlFile: runnerUrlFile, id: RunnerId.make("import-runner"), brokerUrl: "http://127.0.0.1:1", tokenFile, controlTokenHash: yield* platform.digest("import-control-fixture"), host: "127.0.0.1", port: 0,
      stateDirectory: platform.path.join(directory, "runner-state"), childDirectory: platform.path.join(directory, "children"), childUidBase: UserId.make(1000000), slots: 1, repositories: {}, launchers: { omp: "/usr/bin/true" }, modelDirectory: platform.path.join(directory, "model") };
    assert.ok((yield* platform.fs.readFile(`${runnerSource}-wal`)).byteLength > 32, "legacy runner must also retain committed WAL after crash");
    const config: BrokerConfig = { databaseUrlFile: urlFile, backupDirectory: platform.path.join(directory, "backups"), host: "127.0.0.1", port: 0, principals: [], repositoryIds: {}, archiveRetentionMs: 1, backupRetentionMs: 1 };
    const artifact = platform.path.join(directory, "paired-artifact");
    const sha = Sha.make("a".repeat(40));
    yield* Effect.scoped(Effect.gen(function*() {
      yield* platform.acquireJournalOwnership(`${source}.lock`);
      assert.equal((yield* Effect.exit(importLegacySqlite(source, config, artifact, sha, runner, true, runnerSource)))._tag, "Failure", "source flock must fence import");
    }));
    assert.deepEqual(yield* platform.fs.readFile(`${source}-wal`), committedWal, "source flock rejection must not checkpoint the crashed WAL");
    assert.equal((yield* Effect.exit(importLegacySqlite(source, config, artifact, sha, runner, false, runnerSource)))._tag, "Failure", "accepted import must require acknowledgment");
    // The acknowledgment rejection follows the real exclusive checkpoint; only now is the source standalone.
    for (const suffix of ["-wal", "-shm", "-journal"]) assert.equal(yield* platform.fs.exists(`${source}${suffix}`), false, "offline checkpoint must materialize the standalone baseline before import");
    const preservedSource = yield* platform.fs.readFile(source);
    // Reject corrupted native identity and unknown protocols before opening either target gate.
    for (const invalid of ["native", "event", "version"]) {
      const rejectedDirectory = platform.path.join(directory, `reject-${invalid}`);
      yield* platform.fs.makeDirectory(rejectedDirectory, { mode: 0o700 });
      const rejectedUrl = yield* provisionPostgres(rejectedDirectory);
      yield* Effect.addFinalizer(() => dropPostgres(rejectedUrl).pipe(Effect.orDie));
      yield* Effect.gen(function*() {
        const legacy = yield* LegacySqlite;
        if (invalid === "native") yield* legacy.execute("UPDATE tasks SET nativeBinding=? WHERE taskId='legacy-terminal'", [JSON.stringify({ ...nativeBinding, generation: -1 })]);
        else if (invalid === "event") yield* legacy.execute("UPDATE events SET payload=? WHERE seq=13", [JSON.stringify({ ...terminalEvent, binding: { ...nativeBinding, generation: -1 } })]);
        else yield* legacy.execute("PRAGMA user_version=3");
      }).pipe(Effect.provide(LegacySqlite.layer(runnerSource)));
      assert.equal((yield* Effect.exit(importLegacySqlite(source, config, artifact, sha, { ...runner, databaseUrlFile: rejectedUrl }, true, runnerSource)))._tag, "Failure");
      for (const target of [urlFile, rejectedUrl]) yield* Effect.gen(function*() {
        const db = makeDatabase(yield* SqlClient.SqlClient);
        assert.notEqual((yield* db.execute(db.client.selectFrom("recovery_gate").selectAll()))[0]?.ready, true);
        assert.equal((yield* db.execute(db.client.selectFrom("runner_tasks").selectAll())).length, 0, "rejected import must roll back every runner row");
      }).pipe(Effect.provide(postgresAdmin(target)));
      yield* Effect.gen(function*() {
        const legacy = yield* LegacySqlite;
        yield* legacy.execute("UPDATE tasks SET nativeBinding=? WHERE taskId='legacy-terminal'", [JSON.stringify(nativeBinding)]);
        yield* legacy.execute("UPDATE events SET payload=? WHERE seq=13", [JSON.stringify({ ...terminalEvent, ...(version === 1 ? { reason: secret, prompt: secret, output: secret } : { contentOmitted: true }) })]);
        yield* legacy.execute("PRAGMA user_version=" + (version === 1 ? 1 : 2));
      }).pipe(Effect.provide(LegacySqlite.layer(runnerSource)));
    }
    const runnerBaseline = yield* platform.fs.readFile(runnerSource);
    const metadata = yield* importLegacySqlite(source, config, artifact, sha, runner, true, runnerSource);
    assert.equal(metadata.version, 3);
    assert.equal(metadata.broker.pendingAccepted, 1);
    assert.equal(yield* platform.fs.exists(`${source}-wal`), false, "offline checkpoint must finish before standalone import");
    assert.deepEqual(yield* platform.fs.readFile(source), preservedSource, "standalone source bytes must remain untouched by successful import");
    assert.equal(metadata.runner?.pendingAccepted, 1);
    assert.equal(metadata.runner?.eventRows, 2);
    for (const suffix of ["-wal", "-shm", "-journal"]) assert.equal(yield* platform.fs.exists(`${runnerSource}${suffix}`), false, "runner WAL must be checkpointed under offline ownership");
    const preservedRunner = yield* platform.fs.readFile(runnerSource);
    assert.deepEqual(preservedRunner, runnerBaseline, "metadata projection must not scrub the original private journal");
    yield* Effect.scoped(Effect.gen(function*() {
      const db = makeDatabase(yield* SqlClient.SqlClient);
      const journal = (yield* db.execute(db.client.selectFrom("runner_tasks").selectAll().where("taskId", "=", TaskId.make("legacy-accepted"))))[0]!;
      assert.equal(journal.taskId, "legacy-accepted");
      assert.equal(journal.sequence, 7, "legacy rowid preserves child UID allocation");
      assert.equal(journal.state, "HELD");
      assert.equal(journal.occupied, 1);
      assert.equal((yield* db.execute(db.client.selectFrom("runner_tasks").selectAll().where("state", "=", "ACCEPTED"))).length, 0, "no accepted row may remain replayable");
      const terminal = (yield* db.execute(db.client.selectFrom("runner_tasks").selectAll().where("taskId", "=", TaskId.make("legacy-terminal"))))[0]!;
      assert.equal(terminal.state, "SUCCEEDED");
      assert.equal(terminal.occupied, 0);
      assert.equal(terminal.nativeBinding, JSON.stringify(nativeBinding));
      assert.equal(terminal.nativeRunId, "legacy-run");
      assert.equal(terminal.nativeCursor, 4);
      assert.equal(terminal.promptId, "legacy-submission");
      assert.equal(terminal.reason, "historical diagnostic omitted");
      assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(RunnerResult))(terminal.result!), { hostSettled: true, evidenceVerified: false });
      assert.equal(JSON.stringify({ reason: terminal.reason, result: terminal.result }).includes(secret), false);
      assert.equal((yield* db.execute(db.client.selectFrom("runner_tasks").select("result").where("taskId", "=", TaskId.make("legacy-scalar-result"))))[0]?.result, null);
      const events = yield* db.execute(db.client.selectFrom("runner_events").selectAll().orderBy("seq"));
      assert.equal(events[0]?.seq, 12);
      assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent))(events[0]!.payload), { type: "runner_state", state: "ACCEPTED", ...(version === 1 ? { contentOmitted: true } : {}) });
      assert.equal(events[1]?.seq, 13);
      assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(events[1]!.payload), { ...terminalEvent, contentOmitted: true });
      assert.equal(JSON.stringify(events).includes(secret), false);
      if (version === 2) assert.equal(events[1]?.payload, JSON.stringify({ ...terminalEvent, contentOmitted: true }), "modern safe metadata remains byte-exact");
      assert.equal((yield* db.execute(db.client.selectNoFrom((eb) => eb.fn<bigint>("nextval", [eb.val("runner_events_seq_seq")]).as("value"))))[0]?.value, 31n, "deleted event sequence floor survives");
      assert.equal((yield* db.execute(db.client.selectNoFrom((eb) => eb.fn<bigint>("nextval", [eb.val("runner_tasks_sequence_seq")]).as("value"))))[0]?.value, 10n);
      assert.equal((yield* db.execute(db.client.selectFrom("recovery_gate").selectAll()))[0]?.ready, true);
    }).pipe(Effect.provide(postgresAdmin(runnerUrlFile))));
    assert.deepEqual(yield* platform.fs.readFile(runnerSource), preservedRunner);
    yield* Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient;
      const db = makeDatabase(sql);
      const id = TaskId.make("legacy-accepted");
      const task = Schema.decodeUnknownSync(Schema.fromJsonString(Task))((yield* db.execute(db.client.selectFrom("tasks").selectAll().where("id", "=", id)))[0]!.data);
      assert.equal(task.state, "HELD");
      assert.equal(task.offerDecision, "accepted");
      assert.equal((yield* db.execute(db.client.selectFrom("recovery_gate").selectAll()))[0]?.ready, true);
      assert.equal((yield* db.execute(db.client.selectFrom("consents").selectAll()))[0]?.enabled, 1);
      assert.equal((yield* db.execute(db.client.selectFrom("participant_consents").selectAll()))[0]?.enabled, 0);
      assert.equal((yield* db.execute(db.client.selectFrom("archive").selectAll()))[0]?.payload, "private transcript");
      assert.equal((yield* db.execute(db.client.selectFrom("archive").selectAll()))[0]?.sequence, huge);
      assert.equal((yield* db.execute(db.client.selectFrom("archive").selectAll()))[0]?.created, 1780000000000.25);
      assert.equal((yield* db.execute(db.client.selectFrom("evidence").selectAll()))[0]?.id, huge);
      assert.equal((yield* db.execute(db.client.selectFrom("evidence").selectAll()))[0]?.observed, 1780000000000.5);
      const queue = (yield* db.execute(db.client.selectFrom("effect_queue").select(["sequence", "id", "queue_name", "element", "state", "attempts", "last_failure", "acquired_by"]).select((eb) => [
        eb.fn<string>("to_char", [eb.ref("acquired_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("acquired"),
        eb.fn<string>("to_char", [eb.ref("created_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("created"),
        eb.fn<string>("to_char", [eb.ref("visible_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("visible"),
        eb.fn<string>("to_char", [eb.ref("updated_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("updated"),
      ])))[0]!;
      assert.equal(queue.state, "completed");
      assert.equal(queue.sequence, huge);
      assert.equal(queue.id, id);
      assert.equal(queue.queue_name, "coordinator.dispatch");
      assert.equal(queue.element, '"legacy-accepted"');
      assert.equal(queue.last_failure, "uncertain prior result");
      assert.equal(queue.attempts, 3);
      assert.equal(queue.acquired_by, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
      for (const timestamp of [queue.acquired, queue.created, queue.visible, queue.updated]) assert.equal(timestamp, "2026-10-13 01:02:03.123456");
      for (const [table, column] of [["archive", "sequence"], ["evidence", "id"], ["effect_queue", "sequence"]]) {
        assert.equal((yield* db.execute(db.client.selectNoFrom((eb) => eb.fn<bigint>("nextval", [
          eb.fn<string>("pg_get_serial_sequence", [eb.val(`public.${table}`), eb.val(column)]),
        ]).as("value"))))[0]?.value, floor + 1n);
      }
      assert.equal(Number((yield* db.execute(db.client.selectFrom("sessions").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 1);
      assert.equal((yield* db.execute(db.client.selectFrom("peers").selectAll()))[0]?.expires, 1780000000000.25);
    }).pipe(Effect.provide(postgresAdmin(urlFile))));
    assert.equal((yield* Effect.exit(importLegacySqlite(source, config, platform.path.join(directory, "overwrite"), sha, runner, true, runnerSource)))._tag, "Failure", "import must refuse an occupied target");
    assert.deepEqual(yield* platform.fs.readFile(source), preservedSource);
  }
  console.log(JSON.stringify({ scenario: "offline-sqlite-postgres-import", fixtureOnly: true, supportedQueueVersions: [1, 2], committedWalPreserved: true, exactBigintIdsAndSequenceFloors: true, microsecondTimestampsAndBooleansRetained: true, sourceFlockAndEmptyTargetRequired: true, acceptedHeldWithoutReplay: true, productionImportClaim: false }));
})).pipe(Effect.provide(Platform.layer)));
