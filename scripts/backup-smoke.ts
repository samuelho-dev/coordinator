import { RoleEnum, OfferDecisionEnum } from "../src/contracts.ts";
import { TaskStateEnum } from "../src/generated/database/enums.ts";
import { AgentHostEnum } from "../src/agent/protocol.ts";
// Node assert has no Effect equivalent; assertions intentionally remain synchronous.
import assert from "node:assert/strict";
import { Clock, Config, Context, Effect, Formatter, Layer, Schema, Stdio, Stream } from "effect";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";

import { BackupError, makeSnapshotStore, restoreBrokerDatabase } from "../src/backup.ts";
import { Broker, LiveSession } from "../src/broker.ts";
import { BrokerConfig, Commands, command, GitHubRepositoryId, ArchiveParticipants, ObjectBackupConfig, Principal, Repo, SnapshotKey, Task } from "../src/contracts.ts";
import { Mcp } from "../src/mcp.ts";
import { Platform } from "../src/platform.ts";
import { provisionPostgres, postgresAdmin, postgresConfig, dropPostgres } from "../test/postgres-fixture.ts";
import { DatabaseService } from "../src/database.ts";
import { Archive, type ArchiveInsert, type ConsentInsert, type ParticipantConsentInsert, type SessionInsert, type TaskInsert } from "../src/generated/database/types.ts";
import { Tool } from "effect/ai";

const backupSmokeCommand = command("program.smoke:backup", "program", "runBackupSmoke", "Exercise real object backup using private proof config restricted to loopback and the proof bucket.", Tool.EmptyParams, Schema.Void, { exposure: "private" });

const runBackupSmoke = () => Effect.gen(function*() {
  const platform = yield* Platform;
  const stdio = yield* Stdio.Stdio;
  const configPath = yield* Config.String("COORDINATOR_BACKUP_SMOKE_CONFIG");
  const objectBackup = yield* Schema.decodeEffect(Schema.fromJsonString(ObjectBackupConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configPath));
  if (!["127.0.0.1", "[::1]", "localhost"].includes(new URL(objectBackup.endpoint).hostname) || objectBackup.bucket !== "coordinator-backup-proof") return yield* Effect.fail(new BackupError({ message: "Backup smoke requires its isolated loopback proof bucket" }));
  const state = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-backup-smoke-" });
  const repo = Repo.make("github.com/proof/backup");
  const enrollment = `enrollment-plaintext-${yield* platform.crypto.randomUUIDv4()}`;
  const transcript = `raw-transcript-${yield* platform.crypto.randomUUIDv4()}`;
  yield* platform.fs.writeFileString(platform.path.join(state, "enrollment.token"), enrollment, { flag: "wx", mode: 0o600 });
  yield* platform.fs.writeFileString(platform.path.join(state, "agent-session-private.jsonl"), transcript, { flag: "wx", mode: 0o600 });
  const principal = Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({
    id: "proof-session", owner: "proof-owner", roles: [RoleEnum.session, RoleEnum.coordinator], repos: [repo],
    tokenHash: yield* platform.digest(enrollment),
  });
  const sourceUrl = yield* provisionPostgres();
  yield* Effect.addFinalizer(() => dropPostgres(sourceUrl).pipe(Effect.orDie));
  const config: BrokerConfig = {
    backupDirectory: platform.path.join(state, "snapshots"),
    host: "127.0.0.1", port: 0, archiveRetentionMs: 86_400_000, backupRetentionMs: 3_600_000,
    repositoryIds: { [repo]: GitHubRepositoryId.make(1) }, principals: [principal], objectBackup,
  };
  const brokerContext = yield* Layer.build(Broker.layer(config).pipe(Layer.provide(postgresConfig(sourceUrl))));
  const broker = Context.get(brokerContext, Broker);
  const store = yield* makeSnapshotStore(objectBackup);
  const initialDeadline = (yield* Clock.currentTimeMillis) + 30_000;
  while (!(yield* platform.fs.readDirectory(config.backupDirectory)).some((file) => file.endsWith(".pgdump"))) {
    if ((yield* Clock.currentTimeMillis) >= initialDeadline) return yield* Effect.fail(new BackupError({ message: "Initial broker snapshot deadline expired" }));
    yield* Effect.sleep("20 millis");
  }
  const now = yield* Clock.currentTimeMillis;
  const task = Schema.decodeUnknownSync(Task, { onExcessProperty: "error" })({
    taskId: "backup-proof-task", owner: principal.owner, fingerprint: yield* platform.digest("backup-proof-task"),
    request: { taskId: "backup-proof-task", repo, ref: "refs/heads/proof", sha: "a".repeat(40), launcher: AgentHostEnum.omp,
      prompt: "sanitized metadata fixture", scope: "backup proof", restrictions: "isolated fixture", expectedEvidence: ["restored"],
      deadline: now + 60_000, workerId: "backup-proof-worker", githubRepoId: 1, requiredLanes: ["proof"], bypassPeer: true, approved: true },
    state: TaskStateEnum.HELD, offerDecision: OfferDecisionEnum.bypass, createdAt: now, updatedAt: now,
  });
  const session = Schema.decodeUnknownSync(LiveSession, { onExcessProperty: "error" })({ sessionId: "backup-proof-session", incarnation: "one", machineId: "backup-proof-machine",
    host: AgentHostEnum.omp, repo, owner: principal.owner, principal: principal.id, verified: true, liveUntil: now + 60_000, polledAt: now });

  // Wait for the immediate background snapshot before fixture writes; subsequent snapshots are one minute apart.
  const db = broker.database;
  const taskInsert: TaskInsert = { id: task.taskId, data: Schema.encodeSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task) };
  yield* db.query((client) => client.insertInto("tasks").values(taskInsert));
  const sessionInsert: SessionInsert = { id: session.sessionId, data: Schema.encodeSync(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(session) };
  yield* db.query((client) => client.insertInto("sessions").values(sessionInsert));
  broker.sessions.set(session.sessionId, session);
  const consentInsert: ConsentInsert = { owner: principal.owner, session: session.sessionId, enabled: 1, participants: Schema.encodeSync(Schema.fromJsonString(ArchiveParticipants), { onExcessProperty: "error" })([principal.id]) };
  yield* db.query((client) => client.insertInto("consents").values(consentInsert));
  const participantInsert: ParticipantConsentInsert = { owner: principal.owner, session: session.sessionId, participant: principal.id, enabled: 1 };
  yield* db.query((client) => client.insertInto("participant_consents").values(participantInsert));
  const digest = yield* platform.digest(transcript);
  const archiveInsert: ArchiveInsert = { owner: principal.owner, session: session.sessionId, entry: Archive.fields.entry.make("raw-fixture"), digest, payload: transcript, gaps: "[]", created: now };
  yield* db.query((client) => client.insertInto("archive").values(archiveInsert));

  const firstFile = yield* broker.snapshot();
  yield* broker.handle(principal, { method: Commands.deleteArchive.name, params: { sessionId: session.sessionId } });
  assert.equal(Number((yield* db.query((client) => client.selectFrom("archive").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0);
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
    const targetUrl = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(targetUrl).pipe(Effect.orDie));
    yield* restoreBrokerDatabase(restored).pipe(Effect.provide(postgresConfig(targetUrl)));
    yield* Effect.scoped(Effect.gen(function*() {
      const db = yield* DatabaseService;
      const excluded: ReadonlyArray<"archive" | "consents" | "participant_consents"> = ["archive", "consents", "participant_consents"];
      for (const table of excluded) {
        assert.equal(Number((yield* db.query((client) => client.selectFrom(table).select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0);
      }
      const row = (yield* db.query((client) => client.selectFrom("tasks").selectAll().where("id", "=", task.taskId)))[0];
      assert.ok(row, "Restored snapshot retains the canonical task row");
      assert.equal(Schema.decodeSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data).request.scope, task.request.scope);
    }).pipe(Effect.provide(postgresAdmin(targetUrl))));
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

  yield* Stream.make(`${Formatter.formatJson({ scenario: "application-object-backup", fixtureOnly: true,
    uploadedBytesRestored: true, restoredPostgresOpened: true, taskMetadataSurvived: true,
    privacy: { archive: 0, consents: 0, participantConsents: 0, rawTranscriptAbsent: true, enrollmentPlaintextAbsent: true },
    optOutSnapshotSafe: true, existingRestoreTargetPreserved: true, expiredOldObject: true, currentObjectRetained: true,
    productionBackupClaim: false }, { space: 2 })}\n`).pipe(Stream.run(stdio.stdout()));
});

NodeRuntime.runMain(backupSmokeCommand.executeDecoded({ runBackupSmoke }, {}).pipe(
  Effect.scoped, Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer, NodeStdio.layer)),
));
