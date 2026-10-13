// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Clock, Config, Context, Effect, Layer, Schema, Stdio, Stream } from "effect";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { SqlClient } from "effect/sql";
import { SnapshotStore, restoreBrokerDatabase } from "../src/backup.ts";
import { Broker } from "../src/broker.ts";
import { ArchiveEntryId, BackupError, BrokerConfig, BrokerRequest, Commands, GitHubRepositoryId, JsonValue, JournalState, LiveSession, ObjectBackupConfig, Principal, Repo, SnapshotKey, Task } from "../src/contracts.ts";
import { Mcp } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { provisionPostgres, postgresAdmin, dropPostgres } from "../test/postgres-fixture.ts";
import { makeDatabase } from "../src/kysely.ts";
import * as Model from "../src/generated/database/types.ts";

const runBackupSmoke = () => Effect.gen(function*() {
  const platform = yield* Platform;
  const stdio = yield* Stdio.Stdio;
  const configPath = yield* Config.String("COORDINATOR_BACKUP_SMOKE_CONFIG");
  const objectBackup = Schema.decodeUnknownSync(Schema.fromJsonString(ObjectBackupConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configPath));
  if (!["127.0.0.1", "[::1]", "localhost"].includes(new URL(objectBackup.endpoint).hostname) || objectBackup.bucket !== "coordinator-backup-proof") return yield* Effect.fail(new BackupError({ message: "Backup smoke requires its isolated loopback proof bucket" }));
  const state = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-backup-smoke-" });
  const repo = Repo.make("github.com/proof/backup");
  const enrollment = `enrollment-plaintext-${yield* platform.crypto.randomUUIDv4()}`;
  const transcript = `raw-transcript-${yield* platform.crypto.randomUUIDv4()}`;
  yield* platform.fs.writeFileString(platform.path.join(state, "enrollment.token"), enrollment, { flag: "wx", mode: 0o600 });
  yield* platform.fs.writeFileString(platform.path.join(state, "native-session-private.jsonl"), transcript, { flag: "wx", mode: 0o600 });
  const principal = Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({
    id: "proof-session", owner: "proof-owner", roles: ["session", "coordinator"], repos: [repo],
    tokenHash: yield* platform.digest(enrollment),
  });
  const sourceDirectory = platform.path.join(state, "broker");
  yield* platform.fs.makeDirectory(sourceDirectory, { mode: 0o700 });
  const sourceUrlFile = yield* provisionPostgres(sourceDirectory);
  yield* Effect.addFinalizer(() => dropPostgres(sourceUrlFile).pipe(Effect.orDie));
  const config: BrokerConfig = {
    databaseUrlFile: sourceUrlFile, backupDirectory: platform.path.join(state, "snapshots"),
    host: "127.0.0.1", port: 0, archiveRetentionMs: 86_400_000, backupRetentionMs: 3_600_000,
    repositoryIds: { [repo]: GitHubRepositoryId.make(1) }, principals: [principal], objectBackup,
  };
  const brokerContext = yield* Layer.build(Broker.layer(config));
  const broker = Context.get(brokerContext, Broker);
  const storeContext = yield* Layer.build(SnapshotStore.layer(objectBackup));
  const store = Context.get(storeContext, SnapshotStore);
  const initialDeadline = (yield* Clock.currentTimeMillis) + 30_000;
  while (!(yield* platform.fs.readDirectory(config.backupDirectory)).some((file) => file.endsWith(".pgdump"))) {
    if ((yield* Clock.currentTimeMillis) >= initialDeadline) return yield* Effect.fail(new BackupError({ message: "Initial broker snapshot deadline expired" }));
    yield* Effect.sleep("20 millis");
  }
  const now = yield* Clock.currentTimeMillis;
  const task = Schema.decodeUnknownSync(Task, { onExcessProperty: "error" })({
    taskId: "backup-proof-task", owner: principal.owner, fingerprint: yield* platform.digest("backup-proof-task"),
    request: { taskId: "backup-proof-task", repo, ref: "refs/heads/proof", sha: "a".repeat(40), launcher: "omp",
      prompt: "sanitized metadata fixture", scope: "backup proof", restrictions: "isolated fixture", expectedEvidence: ["restored"],
      deadline: now + 60_000, workerId: "backup-proof-worker", githubRepoId: 1, requiredLanes: ["proof"], bypassPeer: true, approved: true },
    state: "HELD", offerDecision: "bypass", createdAt: now, updatedAt: now,
  });
  const session = Schema.decodeUnknownSync(LiveSession, { onExcessProperty: "error" })({ sessionId: "backup-proof-session", incarnation: "one", machineId: "backup-proof-machine",
    host: "omp", repo, owner: principal.owner, principal: principal.id, verified: true, liveUntil: now + 60_000, polledAt: now });

  // Wait for the immediate background snapshot before fixture writes; subsequent snapshots are one minute apart.
  const db = makeDatabase(broker.sql);
  yield* db.execute(db.client.insertInto("tasks").values(yield* Schema.decodeUnknownEffect(Model.TasksInsert)({ id: task.taskId, data: Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task) })));
  yield* db.execute(db.client.insertInto("sessions").values(yield* Schema.decodeUnknownEffect(Model.SessionsInsert)({ id: session.sessionId, data: Schema.encodeUnknownSync(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(session) })));
  broker.sessions.set(session.sessionId, session);
  yield* db.execute(db.client.insertInto("consents").values(yield* Schema.decodeUnknownEffect(Model.ConsentsInsert)({ owner: principal.owner, session: session.sessionId, enabled: 1, participants: Schema.encodeUnknownSync(Schema.fromJsonString(JournalState.fields.participants), { onExcessProperty: "error" })([principal.id]) })));
  yield* db.execute(db.client.insertInto("participant_consents").values(yield* Schema.decodeUnknownEffect(Model.ParticipantConsentsInsert)({ owner: principal.owner, session: session.sessionId, participant: principal.id, enabled: 1 })));
  yield* db.execute(db.client.insertInto("archive").values(yield* Schema.decodeUnknownEffect(Model.ArchiveInsert)({ owner: principal.owner, session: session.sessionId, entry: ArchiveEntryId.make("raw-fixture"), digest: yield* platform.digest(transcript), payload: transcript, gaps: "[]", created: now })));

  const firstFile = yield* broker.snapshot();
  yield* broker.handle(principal, Schema.decodeUnknownSync(BrokerRequest, { onExcessProperty: "error" })({ method: "archive.delete", params: { sessionId: session.sessionId } }));
  assert.equal(Number((yield* db.execute(db.client.selectFrom("archive").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0);
  yield* Effect.sleep("2 millis");
  const secondFile = yield* broker.snapshot();
  assert.notEqual(firstFile, secondFile);
  const firstKey = Schema.decodeUnknownSync(SnapshotKey, { onExcessProperty: "error" })(`sanitized/${platform.path.basename(firstFile)}`);
  const secondKey = Schema.decodeUnknownSync(SnapshotKey, { onExcessProperty: "error" })(`sanitized/${platform.path.basename(secondFile)}`);

  const inspect = Effect.fnUntraced(function*(key: SnapshotKey, source: string, label: string) {
    const restored = platform.path.join(state, `${label}.pgdump`);
    yield* store.restore(key, restored);
    assert.deepEqual(yield* platform.fs.readFile(restored), yield* platform.fs.readFile(source), "uploaded and restored bytes differ");
    const bytes = yield* platform.fs.readFile(restored);
    const text = new TextDecoder().decode(bytes);
    assert.equal(text.includes(transcript), false, "raw transcript bytes survived sanitization");
    assert.equal(text.includes(enrollment), false, "enrollment plaintext survived sanitization");
    const targetDirectory = platform.path.join(state, label);
    yield* platform.fs.makeDirectory(targetDirectory, { mode: 0o700 });
    const targetUrlFile = yield* provisionPostgres(targetDirectory);
    yield* Effect.addFinalizer(() => dropPostgres(targetUrlFile).pipe(Effect.orDie));
    yield* restoreBrokerDatabase({ ...config, databaseUrlFile: targetUrlFile }, restored);
    yield* Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient;
      const db = makeDatabase(sql);
      const excluded: ReadonlyArray<"archive" | "consents" | "participant_consents"> = ["archive", "consents", "participant_consents"];
      for (const table of excluded) {
        assert.equal(Number((yield* db.execute(db.client.selectFrom(table).select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0);
      }
      const row = (yield* db.execute(db.client.selectFrom("tasks").selectAll().where("id", "=", yield* Schema.decodeUnknownEffect(Model.TasksId)(task.taskId))))[0];
      assert.equal(Schema.decodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(String(row?.data)).request.scope, task.request.scope);
    }).pipe(Effect.provide(postgresAdmin(targetUrlFile))));
    return restored;
  });
  yield* inspect(firstKey, firstFile, "before-opt-out");
  const secondRestored = yield* inspect(secondKey, secondFile, "after-opt-out");
  const existingBytes = yield* platform.fs.readFile(secondRestored);
  const overwrite = yield* Effect.exit(store.restore(secondKey, secondRestored));
  assert.equal(overwrite._tag, "Failure", "restore overwrote an existing target");
  assert.deepEqual(yield* platform.fs.readFile(secondRestored), existingBytes, "failed restore deleted or changed an existing target");

  const oldFile = platform.path.join(state, "0000000000000.pgdump");
  yield* platform.fs.copyFile(secondFile, oldFile);
  const oldKey = yield* store.put(oldFile);
  assert.equal(oldKey, "sanitized/0000000000000.pgdump");
  assert.equal(yield* store.expire(1), 1);
  const expired = yield* Effect.exit(store.restore(oldKey, platform.path.join(state, "expired.pgdump")));
  assert.equal(expired._tag, "Failure", "old remote snapshot was not expired");
  const currentAgain = platform.path.join(state, "current-again.pgdump");
  yield* store.restore(secondKey, currentAgain);
  assert.deepEqual(yield* platform.fs.readFile(currentAgain), yield* platform.fs.readFile(secondRestored));

  yield* Stream.make(`${Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })({ scenario: "application-object-backup", fixtureOnly: true,
    uploadedBytesRestored: true, restoredPostgresOpened: true, taskMetadataSurvived: true,
    privacy: { archive: 0, consents: 0, participantConsents: 0, rawTranscriptAbsent: true, enrollmentPlaintextAbsent: true },
    optOutSnapshotSafe: true, existingRestoreTargetPreserved: true, expiredOldObject: true, currentObjectRetained: true,
    productionBackupClaim: false })}\n`).pipe(Stream.run(stdio.stdout()));
});

NodeRuntime.runMain(Commands["program.smoke:backup"].execute({ runBackupSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer)),
));
