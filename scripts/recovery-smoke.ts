import assert from "node:assert/strict";
import { Effect, Context, Layer, Option, Schema } from "effect";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Broker } from "../src/broker.ts";
import { Runner } from "../src/runner.ts";
import { Platform } from "../src/platform.ts";
import { Mcp } from "../src/http.ts";
import { migrateRunner } from "../src/database.ts";
import { recover } from "../src/backup.ts";
import { BrokerConfig, GitHubRepositoryId, OwnerId, RunnerConfig, RunnerId, Sha, Task, Work, Repo, TaskId, UserId, WorkerId } from "../src/contracts.ts";
import { SqlClient } from "effect/sql";
import { provisionPostgres, postgresAdmin, dropPostgres } from "../test/postgres-fixture.ts";
import { makeDatabase } from "../src/kysely.ts";
import * as Model from "../src/generated/database/types.ts";

// Requires real Linux flock/setpriv + runner supervisor capabilities. No provider, enrollment, or task dispatch.
process.umask(0o077);
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs, path: { join } } = platform;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-recovery-" });
  yield* fs.chmod(root, 0o711);
  const privateRoot = join(root, "private");
  yield* fs.makeDirectory(privateRoot, { mode: 0o700 });
  for (const name of ["broker", "repository", "model"]) yield* fs.makeDirectory(join(privateRoot, name), { mode: 0o700 });
  const workRoot = join(root, "work");
  yield* fs.makeDirectory(workRoot, { mode: 0o711 });
  yield* fs.chmod(workRoot, 0o711);
  const children = join(workRoot, "children");
  yield* fs.makeDirectory(children, { mode: 0o711 });
  yield* fs.chmod(children, 0o711);
  yield* fs.makeDirectory(join(workRoot, "runner"), { mode: 0o700 });
  const repo = Repo.make("github.com/proof/recovery"), repository = join(privateRoot, "repository");
  yield* platform.exec(ChildProcess.make("git", ["init", repository]));
  yield* platform.exec(ChildProcess.make("git", ["-C", repository, "remote", "add", "origin", "https://github.com/proof/recovery.git"]));
  const launcher = join(root, "fixture-launcher");
  const marker = join(root, "launcher-invoked");
  yield* fs.writeFileString(marker, "", { mode: 0o666 });
  yield* fs.chmod(marker, 0o666);
  yield* fs.writeFileString(launcher, `#!/bin/sh\nif [ "$1" = --version ]; then printf 'offline-recovery-fixture\\n'; exit 0; fi\nprintf 'unexpected-launch\\n' >> '${marker}'\nexit 99\n`, { mode: 0o755 });
  yield* fs.chmod(launcher, 0o755);
  const token = "recovery-fixture-enrollment-token-0000000000000000";
  const tokenFile = join(privateRoot, "token");
  yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
  const sourceUrlFile = yield* provisionPostgres(join(privateRoot, "broker"));
  yield* Effect.addFinalizer(() => dropPostgres(sourceUrlFile).pipe(Effect.orDie));
  const broker: BrokerConfig = { databaseUrlFile: sourceUrlFile, backupDirectory: join(privateRoot, "snapshots"), host: "127.0.0.1", port: 0, principals: [], repositoryIds: {}, archiveRetentionMs: 86400000, backupRetentionMs: 3600000 };
  const runner: RunnerConfig = { id: RunnerId.make("recovery-runner"), brokerUrl: "http://127.0.0.1:1", tokenFile, host: "127.0.0.1", port: 0,
    controlTokenHash: yield* platform.digest("distinct-control-fixture"), stateDirectory: join(workRoot, "runner"), slots: 2, repositories: { [repo]: repository }, launchers: { omp: launcher }, modelDirectory: join(privateRoot, "model"), childDirectory: children, childUidBase: UserId.make(1000000) };
  const now = Date.now();
  const work: Work = { taskId: TaskId.make("recovery-task"), repo, ref: "refs/heads/proof", sha: Sha.make("a".repeat(40)), launcher: "omp", prompt: "offline fixture never dispatched", scope: "recovery", restrictions: "no dispatch", expectedEvidence: ["held"], deadline: now + 3600000, workerId: WorkerId.make("proof-worker"), githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["proof"], bypassPeer: true, approved: true };
  const task: Task = { taskId: work.taskId, owner: OwnerId.make("proof-owner"), fingerprint: yield* platform.fingerprint(work), request: work, runnerId: runner.id, state: "HELD", offerDecision: "bypass", createdAt: now, updatedAt: now };
  yield* Effect.scoped(Effect.gen(function*() {
    const context = yield* Layer.build(Broker.layer(broker));
    const live = Context.get(context, Broker);
    const db = makeDatabase(live.sql);
    yield* db.execute(db.client.insertInto("tasks").values({ id: task.taskId, data: Schema.encodeSync(Schema.fromJsonString(Task))(task) } satisfies Model.TasksInsert));
    const peerWork = { ...work, taskId: TaskId.make("accepted-native-peer") };
    const peer: Task = { ...task, taskId: peerWork.taskId, request: peerWork, fingerprint: yield* platform.fingerprint(peerWork), runnerId: undefined, state: "ACCEPTED", offerDecision: "accepted" };
    yield* db.execute(db.client.insertInto("tasks").values({ id: peer.taskId, data: Schema.encodeSync(Schema.fromJsonString(Task))(peer) } satisfies Model.TasksInsert));
    yield* db.execute(db.client.insertInto("archive").values({ owner: "proof-owner", session: "private-session", entry: "private-entry", digest: "b".repeat(64), payload: "private transcript retained", gaps: "[]", created: now } satisfies Model.ArchiveInsert));
    yield* db.execute(db.client.insertInto("consents").values({ owner: "proof-owner", session: "private-session", enabled: 1, participants: '["proof-owner"]' } satisfies Model.ConsentsInsert));
    yield* db.execute(db.client.insertInto("participant_consents").values({ owner: "proof-owner", session: "private-session", participant: "proof-owner", enabled: 1 } satisfies Model.ParticipantConsentsInsert));
    const fenced = yield* Effect.exit(recover("backup", broker, join(privateRoot, "blocked"), Sha.make("a".repeat(40))));
    assert.equal(fenced._tag, "Failure", "live broker writer must fence recovery");
  }));
  const childRoot = join(children, yield* platform.digest(work.taskId));
  for (const name of [childRoot, join(childRoot, "repo"), join(childRoot, "sessions"), join(childRoot, "home"), join(childRoot, "home", ".omp"), join(childRoot, "home", ".omp", "agent")]) yield* fs.makeDirectory(name, { mode: 0o700 });
  yield* fs.writeFileString(join(childRoot, "repo", "retained.txt"), "accepted workspace state", { mode: 0o600 });
  yield* fs.writeFileString(join(childRoot, "sessions", "fixture.jsonl"), "private native session fixture", { mode: 0o600 });
  yield* fs.writeFileString(join(childRoot, "home", ".omp", "agent", "auth.json"), "excluded-model-secret", { mode: 0o600 });
  yield* fs.symlink("retained.txt", join(childRoot, "repo", "retained-link"));
  yield* fs.chown(join(childRoot, "sessions", "fixture.jsonl"), 1000001, 1000001);
  for (const file of [join(childRoot, "repo", "retained.txt"), join(childRoot, "repo", "retained-link"), join(childRoot, "repo"), join(childRoot, "sessions")]) {
    yield* platform.exec(ChildProcess.make("/usr/bin/chown", ["--no-dereference", "1000001:1000001", "--", file], { env: {}, extendEnv: false }));
  }
  yield* Effect.scoped(Effect.gen(function*() {
    const sql = yield* SqliteClient.SqliteClient;
    yield* migrateRunner(sql);
    yield* sql.unsafe("PRAGMA user_version=2");
    yield* sql.unsafe("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation,promptId) VALUES (?,?,?,?,'PROMPT_INTENT',1,?,?)", [work.taskId, yield* platform.fingerprint(work), Schema.encodeUnknownSync(Schema.fromJsonString(Work))(work), join(childRoot, "repo"), "fixture-incarnation", "fixture-prompt"]);
    const accepted = { ...work, taskId: TaskId.make("accepted-recovery-task") };
    yield* sql.unsafe("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (?,?,?,?,'ACCEPTED',1,?)", [accepted.taskId, yield* platform.fingerprint(accepted), Schema.encodeUnknownSync(Schema.fromJsonString(Work))(accepted), join(children, yield* platform.digest(accepted.taskId), "repo"), "accepted-incarnation"]);
    yield* sql.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", [work.taskId, JSON.stringify({ type: "runner_state", state: "PROMPT_INTENT" })]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: join(runner.stateDirectory, "runner.sqlite") }))));
  const artifact = join(privateRoot, "artifact"), sourceSha = Sha.make("a".repeat(40));
  yield* Effect.scoped(Effect.gen(function*() {
    yield* platform.acquireJournalOwnership(join(runner.stateDirectory, "runner.lock"));
    assert.equal((yield* Effect.exit(recover("backup", broker, join(privateRoot, "runner-blocked"), sourceSha, runner)))._tag, "Failure", "owned runner journal must fence recovery");
  }));
  yield* fs.chmod(workRoot, 0o751);
  const unsafeParentArtifact = join(privateRoot, "unsafe-parent");
  assert.equal((yield* Effect.exit(recover("backup", broker, unsafeParentArtifact, sourceSha, runner)))._tag, "Failure", "runner parent permits only private or traversal-only modes");
  assert.equal(yield* fs.exists(unsafeParentArtifact), false, "unsafe runner parent must fence backup before artifact creation");
  yield* fs.chmod(workRoot, 0o711);
  yield* Effect.scoped(Effect.gen(function*() {
    const child = yield* platform.spawner.spawn(ChildProcess.make("/usr/bin/sleep", ["30"], { env: {}, extendEnv: false, stdin: "ignore" }));
    yield* Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqliteClient.SqliteClient;
      yield* sql.unsafe("UPDATE tasks SET pid=? WHERE taskId=?", [child.pid, work.taskId]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: join(runner.stateDirectory, "runner.sqlite") }))));
    assert.equal((yield* Effect.exit(recover("backup", broker, join(privateRoot, "live-child-blocked"), sourceSha, runner)))._tag, "Failure", "recorded live native child must fence backup");
    yield* Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqliteClient.SqliteClient;
      yield* sql.unsafe("UPDATE tasks SET pid=NULL WHERE taskId=?", [work.taskId]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: join(runner.stateDirectory, "runner.sqlite") }))));
  }));
  const metadata = yield* recover("backup", broker, artifact, sourceSha, runner);
  assert.equal(metadata.runner?.pendingAccepted, 1);
  assert.equal(metadata.broker.pendingAccepted, 1);
  // Disposable fixture only: simulate fresh volumes mounted at identical absolute paths without touching original bytes.
  for (const directory of [runner.stateDirectory, repository, children]) {
    yield* fs.rename(directory, `${directory}.source`);
    yield* fs.makeDirectory(directory, { mode: directory === children ? 0o711 : 0o700 });
    if (directory === children) yield* fs.chmod(directory, 0o711);
  }
  const targetDirectory = join(privateRoot, "restore-broker");
  yield* fs.makeDirectory(targetDirectory, { mode: 0o700 });
  const targetUrlFile = yield* provisionPostgres(targetDirectory);
  yield* Effect.addFinalizer(() => dropPostgres(targetUrlFile).pipe(Effect.orDie));
  const restoredBroker: BrokerConfig = { ...broker, databaseUrlFile: targetUrlFile };
  const acknowledgement = yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, runner));
  assert.equal(acknowledgement._tag, "Failure", "pending accepted recovery requires explicit HELD fence acknowledgement");
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, Sha.make("b".repeat(40)), runner, true)))._tag, "Failure", "source revision mismatch must fence restore");
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, { ...runner, childUidBase: UserId.make(runner.childUidBase + 1000) }, true)))._tag, "Failure", "runner UID layout mismatch must fence restore");
  const dumpFile = join(artifact, "broker.pgdump"), dumpBytes = yield* fs.readFile(dumpFile);
  yield* fs.writeFileString(dumpFile, "tampered native dump", { mode: 0o600 });
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, runner, true)))._tag, "Failure", "broker digest mismatch must fence restore before target mutation");
  yield* fs.writeFile(dumpFile, dumpBytes, { mode: 0o600 });
  yield* Effect.scoped(Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    const db = makeDatabase(sql);
    assert.equal(Number((yield* db.execute(db.client.selectFrom("tasks").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0, "rejected restore must not write ledger");
  }).pipe(Effect.provide(postgresAdmin(targetUrlFile))));
  const staleWal = join(runner.stateDirectory, "runner.sqlite-wal");
  yield* fs.writeFileString(staleWal, "stale disposable WAL sentinel", { mode: 0o600 });
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, runner, true)))._tag, "Failure", "stale runner WAL must fence fresh paired restore");
  assert.equal(yield* fs.exists(join(runner.stateDirectory, "runner.sqlite")), false);
  yield* fs.remove(staleWal);
  yield* recover("restore", restoredBroker, artifact, sourceSha, runner, true);
  assert.equal(yield* fs.readFileString(join(childRoot, "repo", "retained-link")), "accepted workspace state");
  assert.equal(yield* fs.readFileString(join(childRoot, "sessions", "fixture.jsonl")), "private native session fixture");
  assert.equal(yield* fs.exists(join(childRoot, "home", ".omp", "agent", "auth.json")), false);
  for (const [file, mode] of [
    [join(childRoot, "repo"), 0o700], [join(childRoot, "repo", "retained.txt"), 0o600],
    [join(childRoot, "sessions"), 0o700], [join(childRoot, "sessions", "fixture.jsonl"), 0o600],
  ] as const) {
    const info = yield* fs.stat(file);
    assert(Option.isSome(info.uid) && Option.isSome(info.gid), "native ownership metadata must be available");
    assert.equal(info.uid.value, 1000001, "child directory/file ownership must survive");
    assert.equal(info.gid.value, 1000001, "child directory/file group must survive");
    assert.equal(info.mode & 0o777, mode, "private child modes must survive");
  }
  const restoredLink = join(childRoot, "repo", "retained-link");
  assert.equal(yield* fs.readLink(restoredLink), "retained.txt", "retained workspace link must remain a symlink");
  assert.equal(yield* platform.exec(ChildProcess.make("/usr/bin/stat", ["--printf=%u %g", "--", restoredLink], { env: {}, extendEnv: false })), "1000001 1000001", "workspace symlink ownership and group must survive");
  yield* Effect.scoped(Effect.gen(function*() {
    const brokerContext = yield* Layer.build(Broker.layer(restoredBroker));
    const openedBroker = Context.get(brokerContext, Broker);
    const db = makeDatabase(openedBroker.sql);
    const rows = yield* db.execute(db.client.selectFrom("tasks").selectAll().where("id", "=", task.taskId));
    assert.equal(Schema.decodeUnknownSync(Schema.fromJsonString(Task))(rows[0]!.data).state, "HELD");
    const peerRows = yield* db.execute(db.client.selectFrom("tasks").selectAll().where("id", "=", TaskId.make("accepted-native-peer")));
    const peer = Schema.decodeUnknownSync(Schema.fromJsonString(Task))(peerRows[0]!.data);
    assert.equal(peer.state, "HELD");
    assert.equal(peer.offerDecision, "accepted", "recovery must not erase accepted peer ownership");
    assert.equal((yield* db.execute(db.client.selectFrom("archive").selectAll()))[0]?.payload, "private transcript retained");
    assert.equal((yield* db.execute(db.client.selectFrom("consents").selectAll()))[0]?.enabled, 1);
    assert.equal((yield* db.execute(db.client.selectFrom("participant_consents").selectAll()))[0]?.enabled, 1);
    assert.equal((yield* db.execute(db.client.selectFrom("recovery_gate").selectAll()))[0]?.ready, true);
    const sessionInfo = yield* fs.stat(join(childRoot, "sessions", "fixture.jsonl"));
    assert(Option.isSome(sessionInfo.uid));
    assert.equal(sessionInfo.uid.value, 1000001, "native session ownership must survive");
    const runnerContext = yield* Layer.build(Runner.layer(runner));
    const openedRunner = Context.get(runnerContext, Runner);
    const uncertain = yield* openedRunner.row(work.taskId);
    assert.equal(uncertain?.state, "HELD");
    assert.equal(uncertain?.promptId, "fixture-prompt");
    assert.equal(uncertain?.occupied, 1);
    assert.equal((yield* openedRunner.row(TaskId.make("accepted-recovery-task")))?.state, "HELD");
    assert.equal((yield* openedRunner.sql.unsafe<{ count: number }>("SELECT count(*) AS count FROM events"))[0]?.count, 1);
    yield* Effect.sleep("100 millis");
    assert.equal(yield* fs.readFileString(marker), "", "restored accepted/uncertain work must never invoke launcher");
  }));
  const overwrite = yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, runner, true));
  assert.equal(overwrite._tag, "Failure", "restore cannot overwrite existing ledger");
  console.log(JSON.stringify({ scenario: "private-offline-recovery", fixtureOnly: true, actualBrokerReopened: true, actualRunnerReopened: true, workspaceAndNativeSessionRetained: true,
    modelCredentialExcluded: true, brokerLockFenced: true, acceptedRecoveryExplicitlyHeld: true, uncertainPromptAndOccupiedSlotRetained: true, launcherInvocations: 0, productionBackupClaim: false }));
})).pipe(Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer))));
