import assert from "node:assert/strict";
import { Effect, Option, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { Platform } from "../src/platform.ts";
import { Sqlite } from "../src/sqlite.ts";
import { importLegacySqlite, legacyBrokerSql, privateRecoveryPath } from "../src/backup.ts";
import { BrokerConfig, GitHubRepositoryId, IncarnationId, LiveSession, MachineId, OwnerId, PrincipalId, Repo, SessionId, Sha, Task, TaskId, Work, WorkerId } from "../src/contracts.ts";
import { provisionPostgres, postgresAdmin, dropPostgres } from "../test/postgres-fixture.ts";
import { makeDatabase } from "../src/kysely.ts";

process.umask(0o077);
const huge = 9007199254740993n, floor = huge + 20n;
const seedPath = process.argv[2] === "--seed" ? process.argv[3] : undefined;
const seedVersion = process.argv[4] === "1" ? 1 : 2;
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  if (seedPath) {
    yield* privateRecoveryPath(seedPath, "file", true);
    return yield* Effect.gen(function*() {
      const { sql } = yield* Sqlite;
      yield* sql.unsafe("PRAGMA synchronous=FULL");
      yield* sql.unsafe("PRAGMA fullfsync=ON");
      yield* sql.unsafe("PRAGMA wal_autocheckpoint=0");
      for (const statement of legacyBrokerSql(seedVersion)) yield* sql.unsafe(statement);
      yield* sql.unsafe("INSERT INTO coordinator_broker_migrations(migration_id,name) VALUES (1,'relational_baseline')");
      yield* sql.unsafe("INSERT INTO effect_queue_migrations(migration_id,name) VALUES (1,'create_table')");
      if (seedVersion === 2) yield* sql.unsafe("INSERT INTO effect_queue_migrations(migration_id,name) VALUES (2,'upgrade_schema')");
      const repo = Repo.make("github.com/proof/legacy");
      const work: Work = { taskId: TaskId.make("legacy-accepted"), repo, ref: "refs/heads/proof", sha: Sha.make("a".repeat(40)), launcher: "omp", prompt: "never dispatch imported accepted work", scope: "offline import", restrictions: "fixture only", expectedEvidence: ["held"], deadline: 1900000000000, workerId: WorkerId.make("proof-worker"), githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["proof"], bypassPeer: true, approved: true };
      const task: Task = { taskId: work.taskId, owner: OwnerId.make("proof-owner"), fingerprint: yield* platform.fingerprint(work), request: work, state: "ACCEPTED", offerDecision: "accepted", createdAt: 1780000000000, updatedAt: 1780000000001 };
      yield* sql.unsafe("INSERT INTO tasks(id,data) VALUES (?,?)", [task.taskId, Schema.encodeSync(Schema.fromJsonString(Task))(task)]);
      const session: LiveSession = { sessionId: SessionId.make("private-session"), incarnation: IncarnationId.make("incarnation"), machineId: MachineId.make("private-machine"), host: "omp", repo, owner: OwnerId.make("proof-owner"), principal: PrincipalId.make("private-principal"), verified: true, liveUntil: 1900000000000, polledAt: 1780000000000 };
      yield* sql.unsafe("INSERT INTO sessions(id,data) VALUES (?,?)", [session.sessionId, Schema.encodeSync(Schema.fromJsonString(LiveSession))(session)]);
      yield* sql.unsafe("INSERT INTO peers(id,owner,sender,session,incarnation,payload,fingerprint,expires) VALUES ('private-peer','owner','sender','session','incarnation','{\"private\":\"opaque payload\"}',?,1780000000000.25)", ["b".repeat(64)]);
      yield* sql.unsafe("INSERT INTO consents(owner,session,enabled,participants) VALUES ('owner','session',1,'[\"participant\"]')");
      yield* sql.unsafe("INSERT INTO participant_consents(owner,session,participant,enabled) VALUES ('owner','session','participant',0)");
      yield* sql.unsafe("INSERT INTO archive(sequence,owner,session,entry,digest,payload,gaps,created) VALUES (?,'owner','session','entry',?,'private transcript','[]',1780000000000.25)", [huge, "b".repeat(64)]);
      yield* sql.unsafe("INSERT INTO evidence(id,task,kind,sha,principal,data,observed) VALUES (?,'legacy-accepted','readback',?,'principal','opaque evidence',1780000000000.5)", [huge, "a".repeat(40)]);
      const queueColumns = seedVersion === 1 ? "completed" : "state,visible_at";
      const queueValues = seedVersion === 1 ? "1" : "'completed','2026-10-13 01:02:03.123456'";
      // The native queue owner is a valid RFC v4 UUID; import must preserve it exactly, not normalize it.
      yield* sql.unsafe(`INSERT INTO effect_queue(sequence,id,queue_name,element,${queueColumns},attempts,last_failure,acquired_at,acquired_by,created_at,updated_at) VALUES (?,'legacy-accepted','coordinator.dispatch','\"legacy-accepted\"',${queueValues},3,'uncertain prior result','2026-10-13 01:02:03.123456','aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee','2026-10-13 01:02:03.123456','2026-10-13 01:02:03.123456')`, [huge]);
      yield* sql.unsafe("UPDATE sqlite_sequence SET seq=? WHERE name IN ('archive','evidence','effect_queue')", [floor]);
      // Keep the real SQLite worker open until its scoped parent is killed with SIGKILL.
      console.log("committed-wal-ready");
      return yield* Effect.never;
    }).pipe(Effect.provide(Sqlite.layer({ filename: seedPath })));
  }
  const root = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-legacy-import-proof-" });
  for (const version of [1, 2]) {
    const directory = platform.path.join(root, `v${version}`);
    yield* platform.fs.makeDirectory(directory, { mode: 0o700 });
    const source = platform.path.join(directory, "ledger.sqlite");
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
    const config: BrokerConfig = { databaseUrlFile: urlFile, backupDirectory: platform.path.join(directory, "backups"), host: "127.0.0.1", port: 0, principals: [], repositoryIds: {}, archiveRetentionMs: 1, backupRetentionMs: 1 };
    const artifact = platform.path.join(directory, "paired-artifact");
    const sha = Sha.make("a".repeat(40));
    yield* Effect.scoped(Effect.gen(function*() {
      yield* platform.acquireJournalOwnership(`${source}.lock`);
      assert.equal((yield* Effect.exit(importLegacySqlite(source, config, artifact, sha, undefined, true)))._tag, "Failure", "source flock must fence import");
    }));
    assert.deepEqual(yield* platform.fs.readFile(`${source}-wal`), committedWal, "source flock rejection must not checkpoint the crashed WAL");
    assert.equal((yield* Effect.exit(importLegacySqlite(source, config, artifact, sha)))._tag, "Failure", "accepted import must require acknowledgment");
    // The acknowledgment rejection follows the real exclusive checkpoint; only now is the source standalone.
    for (const suffix of ["-wal", "-shm", "-journal"]) assert.equal(yield* platform.fs.exists(`${source}${suffix}`), false, "offline checkpoint must materialize the standalone baseline before import");
    const preservedSource = yield* platform.fs.readFile(source);
    const metadata = yield* importLegacySqlite(source, config, artifact, sha, undefined, true);
    assert.equal(metadata.version, 2);
    assert.equal(metadata.broker.pendingAccepted, 1);
    assert.equal(yield* platform.fs.exists(`${source}-wal`), false, "offline checkpoint must finish before standalone import");
    assert.deepEqual(yield* platform.fs.readFile(source), preservedSource, "standalone source bytes must remain untouched by successful import");
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
    assert.equal((yield* Effect.exit(importLegacySqlite(source, config, platform.path.join(directory, "overwrite"), sha, undefined, true)))._tag, "Failure", "import must refuse an occupied target");
    assert.deepEqual(yield* platform.fs.readFile(source), preservedSource);
  }
  console.log(JSON.stringify({ scenario: "offline-sqlite-postgres-import", fixtureOnly: true, supportedQueueVersions: [1, 2], committedWalPreserved: true, exactBigintIdsAndSequenceFloors: true, microsecondTimestampsAndBooleansRetained: true, sourceFlockAndEmptyTargetRequired: true, acceptedHeldWithoutReplay: true, productionImportClaim: false }));
})).pipe(Effect.provide(Platform.layer)));
