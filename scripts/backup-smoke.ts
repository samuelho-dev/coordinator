// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Clock, Config, Context, Effect, Layer, Stdio, Stream } from "effect";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { SnapshotStore } from "../src/backup.ts";
import { Broker } from "../src/broker.ts";
import { ArchiveEntryId, BackupError, BrokerConfig, BrokerRequest, Commands, GitHubRepositoryId, Json, JsonValue, JournalState, LiveSession, ObjectBackupConfig, Principal, Repo, SnapshotKey, Task } from "../src/contracts.ts";
import { decode, decodeJson, encodeJson, Mcp } from "../src/http.ts";
import { Platform } from "../src/platform.ts";

const runBackupSmoke = () => Effect.gen(function*() {
  const platform = yield* Platform;
  const stdio = yield* Stdio.Stdio;
  const configPath = yield* Config.String("COORDINATOR_BACKUP_SMOKE_CONFIG");
  const objectBackup = decodeJson(ObjectBackupConfig, yield* platform.privateFile(configPath));
  if (!["127.0.0.1", "[::1]", "localhost"].includes(new URL(objectBackup.endpoint).hostname) || objectBackup.bucket !== "coordinator-backup-proof") return yield* Effect.fail(new BackupError({ message: "Backup smoke requires its isolated loopback proof bucket" }));
  const state = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-backup-smoke-" });
  const repo = Repo.make("github.com/proof/backup");
  const enrollment = `enrollment-plaintext-${yield* platform.crypto.randomUUIDv4}`;
  const transcript = `raw-transcript-${yield* platform.crypto.randomUUIDv4}`;
  yield* platform.fs.writeFileString(platform.path.join(state, "enrollment.token"), enrollment, { flag: "wx", mode: 0o600 });
  yield* platform.fs.writeFileString(platform.path.join(state, "native-session-private.jsonl"), transcript, { flag: "wx", mode: 0o600 });
  const principal = decode(Principal, {
    id: "proof-session", owner: "proof-owner", roles: ["session", "coordinator"], repos: [repo],
    tokenHash: yield* platform.digest(enrollment),
  });
  const config: BrokerConfig = {
    database: platform.path.join(state, "broker", "ledger.sqlite"), backupDirectory: platform.path.join(state, "snapshots"),
    host: "127.0.0.1", port: 0, archiveRetentionMs: 86_400_000, backupRetentionMs: 3_600_000,
    repositoryIds: { [repo]: GitHubRepositoryId.make(1) }, principals: [principal], objectBackup,
  };
  const brokerContext = yield* Layer.build(Broker.layer(config));
  const broker = Context.get(brokerContext, Broker);
  const storeContext = yield* Layer.build(SnapshotStore.layer(objectBackup));
  const store = Context.get(storeContext, SnapshotStore);
  const initialDeadline = (yield* Clock.currentTimeMillis) + 30_000;
  while (!(yield* platform.fs.readDirectory(config.backupDirectory)).some((file) => file.endsWith(".sqlite"))) {
    if ((yield* Clock.currentTimeMillis) >= initialDeadline) return yield* Effect.fail(new BackupError({ message: "Initial broker snapshot deadline expired" }));
    yield* Effect.sleep("20 millis");
  }
  const now = yield* Clock.currentTimeMillis;
  const task = decode(Task, {
    taskId: "backup-proof-task", owner: principal.owner, fingerprint: yield* platform.digest("backup-proof-task"),
    request: { taskId: "backup-proof-task", repo, ref: "refs/heads/proof", sha: "a".repeat(40), launcher: "omp",
      prompt: "sanitized metadata fixture", scope: "backup proof", restrictions: "isolated fixture", expectedEvidence: ["restored"],
      deadline: now + 60_000, workerId: "backup-proof-worker", githubRepoId: 1, requiredLanes: ["proof"], bypassPeer: true, approved: true },
    state: "HELD", offerDecision: "bypass", createdAt: now, updatedAt: now,
  });
  const session = decode(LiveSession, { sessionId: "backup-proof-session", incarnation: "one", machineId: "backup-proof-machine",
    host: "omp", repo, owner: principal.owner, principal: principal.id, verified: true, liveUntil: now + 60_000, polledAt: now });

  // Wait for the immediate background snapshot before fixture writes; subsequent snapshots are one minute apart.
  yield* broker.sql.unsafe("INSERT INTO tasks(id,data) VALUES (?,?)", [task.taskId, encodeJson(Task, task)]);
  yield* broker.sql.unsafe("INSERT INTO sessions(id,data) VALUES (?,?)", [session.sessionId, encodeJson(LiveSession, session)]);
  broker.sessions.set(session.sessionId, session);
  yield* broker.sql.unsafe("INSERT INTO consents(owner,session,enabled,participants) VALUES (?,?,?,?)", [principal.owner, session.sessionId, 1, encodeJson(JournalState.fields.participants, [principal.id])]);
  yield* broker.sql.unsafe("INSERT INTO participant_consents(owner,session,participant,enabled) VALUES (?,?,?,?)", [principal.owner, session.sessionId, principal.id, 1]);
  yield* broker.sql.unsafe("INSERT INTO archive(owner,session,entry,digest,payload,gaps,created) VALUES (?,?,?,?,?,?,?)",
    [principal.owner, session.sessionId, ArchiveEntryId.make("raw-fixture"), yield* platform.digest(transcript), transcript, "[]", now]);

  const firstFile = yield* broker.snapshot();
  yield* broker.handle(principal, decode(BrokerRequest, { method: "archive.delete", params: { sessionId: session.sessionId } }));
  assert.equal((yield* broker.sql.unsafe<Json>("SELECT count(*) AS count FROM archive"))[0]?.count, 0);
  yield* Effect.sleep("2 millis");
  const secondFile = yield* broker.snapshot();
  assert.notEqual(firstFile, secondFile);
  const firstKey = decodeSnapshotKey(firstFile, platform.path.basename);
  const secondKey = decodeSnapshotKey(secondFile, platform.path.basename);

  const inspect = Effect.fnUntraced(function*(key: SnapshotKey, source: string, label: string) {
    const restored = platform.path.join(state, `${label}.sqlite`);
    yield* store.restore(key, restored);
    assert.deepEqual(yield* platform.fs.readFile(restored), yield* platform.fs.readFile(source), "uploaded and restored bytes differ");
    const bytes = yield* platform.fs.readFile(restored);
    const text = new TextDecoder().decode(bytes);
    assert.equal(text.includes(transcript), false, "raw transcript bytes survived sanitization");
    assert.equal(text.includes(enrollment), false, "enrollment plaintext survived sanitization");
    yield* Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqliteClient.SqliteClient;
      for (const table of ["archive", "consents", "participant_consents"]) {
        assert.equal((yield* sql.unsafe<Json>(`SELECT count(*) AS count FROM ${table}`))[0]?.count, 0);
      }
      const row = (yield* sql.unsafe<Json>("SELECT data FROM tasks WHERE id=?", [task.taskId]))[0];
      assert.equal(decodeJson(Task, String(row?.data)).request.scope, task.request.scope);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: restored, readonly: true }))));
    return restored;
  });
  yield* inspect(firstKey, firstFile, "before-opt-out");
  const secondRestored = yield* inspect(secondKey, secondFile, "after-opt-out");
  const existingBytes = yield* platform.fs.readFile(secondRestored);
  const overwrite = yield* Effect.exit(store.restore(secondKey, secondRestored));
  assert.equal(overwrite._tag, "Failure", "restore overwrote an existing target");
  assert.deepEqual(yield* platform.fs.readFile(secondRestored), existingBytes, "failed restore deleted or changed an existing target");

  const oldFile = platform.path.join(state, "0000000000000.sqlite");
  yield* platform.fs.copyFile(secondFile, oldFile);
  const oldKey = yield* store.put(oldFile);
  assert.equal(oldKey, "sanitized/0000000000000.sqlite");
  assert.equal(yield* store.expire(1), 1);
  const expired = yield* Effect.exit(store.restore(oldKey, platform.path.join(state, "expired.sqlite")));
  assert.equal(expired._tag, "Failure", "old remote snapshot was not expired");
  const currentAgain = platform.path.join(state, "current-again.sqlite");
  yield* store.restore(secondKey, currentAgain);
  assert.deepEqual(yield* platform.fs.readFile(currentAgain), yield* platform.fs.readFile(secondRestored));

  yield* Stream.make(`${encodeJson(JsonValue, { scenario: "application-object-backup", fixtureOnly: true,
    uploadedBytesRestored: true, restoredSqliteOpened: true, taskMetadataSurvived: true,
    privacy: { archive: 0, consents: 0, participantConsents: 0, rawTranscriptAbsent: true, enrollmentPlaintextAbsent: true },
    optOutSnapshotSafe: true, existingRestoreTargetPreserved: true, expiredOldObject: true, currentObjectRetained: true,
    productionBackupClaim: false })}\n`).pipe(Stream.run(stdio.stdout()));
});

function decodeSnapshotKey(file: string, basename: (path: string) => string): SnapshotKey {
  return decode(SnapshotKey, `sanitized/${basename(file)}`);
}

NodeRuntime.runMain(Commands["program.smoke:backup"].execute({ runBackupSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer)),
));
