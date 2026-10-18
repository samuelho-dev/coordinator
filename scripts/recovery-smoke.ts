import { OfferDecisionEnum } from "../src/contracts.ts";
import { TaskStateEnum, RunnerStateEnum } from "../src/generated/database/enums.ts";
import assert from "node:assert/strict";
import { Console, Effect, Context, Formatter, Layer, Option, Schema } from "effect";
import { ChildProcess } from "effect/process";
import { NodeRuntime } from "@effect/platform-node";

import { Broker } from "../src/broker.ts";
import { Runner } from "../src/runner.ts";
import { Platform } from "../src/platform.ts";
import { Mcp } from "../src/mcp.ts";
import { recover } from "../src/backup.ts";
import { BrokerConfig, GitHubRepositoryId, RunnerConfig, RunnerId, Task, Work, Repo, UserId, WorkerId } from "../src/contracts.ts";
import { TaskId, Archive, Evidence, ParticipantConsent, Peer, RunnerTask, SessionId } from "../src/generated/database/types.ts";
import { provisionPostgres, postgresAdmin, postgresConfig, dropPostgres } from "../test/postgres-fixture.ts";
import { DatabaseService } from "../src/database.ts";
import { AgentHostEnum, AgentSubmissionId } from "../src/agent/protocol.ts";

// Requires real Linux flock/setpriv + runner supervisor capabilities. No provider, enrollment, or task dispatch.
process.umask(0o077);
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs, path: { join } } = platform;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-recovery-" });
  yield* fs.chmod(root, 0o711);
  const privateRoot = join(root, "private");
  yield* fs.makeDirectory(privateRoot, { mode: 0o700 });
  for (const name of ["repository", "model"]) yield* fs.makeDirectory(join(privateRoot, name), { mode: 0o700 });
  const workRoot = join(root, "work");
  yield* fs.makeDirectory(workRoot, { mode: 0o711 });
  yield* fs.chmod(workRoot, 0o711);
  const children = join(workRoot, "children");
  yield* fs.makeDirectory(children, { mode: 0o711 });
  yield* fs.chmod(children, 0o711);
  yield* fs.makeDirectory(join(workRoot, "runner"), { mode: 0o700 });
  const repo = Repo.make("github.com/proof/recovery"), repository = join(privateRoot, "repository");
  yield* platform.exec(ChildProcess.make("git", ["init", repository], { env: {}, extendEnv: false }));
  yield* platform.exec(ChildProcess.make("git", ["-C", repository, "remote", "add", "origin", "https://github.com/proof/recovery.git"], { env: {}, extendEnv: false }));
  const launcher = join(root, "fixture-launcher");
  const marker = join(root, "launcher-invoked");
  yield* fs.writeFileString(marker, "", { mode: 0o666 });
  yield* fs.chmod(marker, 0o666);
  yield* fs.writeFileString(launcher, `#!/bin/sh\nif [ "$1" = --version ]; then printf 'offline-recovery-fixture\\n'; exit 0; fi\nprintf 'unexpected-launch\\n' >> '${marker}'\nexit 99\n`, { mode: 0o755 });
  yield* fs.chmod(launcher, 0o755);
  const token = "recovery-fixture-enrollment-token-0000000000000000";
  const tokenFile = join(privateRoot, "token");
  yield* fs.writeFileString(tokenFile, token, { mode: 0o600 });
  const sourceUrl = yield* provisionPostgres();
  yield* Effect.addFinalizer(() => dropPostgres(sourceUrl).pipe(Effect.orDie));
  const broker: BrokerConfig = { backupDirectory: join(privateRoot, "snapshots"), host: "127.0.0.1", port: 0, principals: [], repositoryIds: {}, archiveRetentionMs: 86400000, backupRetentionMs: 3600000 };
  const runnerUrl = yield* provisionPostgres();
  yield* Effect.addFinalizer(() => dropPostgres(runnerUrl).pipe(Effect.orDie));
  const runner: RunnerConfig = { id: RunnerId.make("recovery-runner"), brokerUrl: "http://127.0.0.1:1", tokenFile, host: "127.0.0.1", port: 0,
    controlTokenHash: yield* platform.digest("distinct-control-fixture"), stateDirectory: join(workRoot, "runner"), slots: 2, repositories: { [repo]: repository }, launchers: { omp: launcher }, modelDirectory: join(privateRoot, "model"), childDirectory: children, childUidBase: UserId.make(1000000) };
  yield* Effect.gen(function*() {
  const now = Date.now();
  const work: Work = { taskId: TaskId.make("recovery-task"), repo, ref: "refs/heads/proof", sha: Evidence.fields.sha.make("a".repeat(40)), launcher: AgentHostEnum.omp, prompt: "offline fixture never dispatched", scope: "recovery", restrictions: "no dispatch", expectedEvidence: ["held"], deadline: now + 3600000, workerId: WorkerId.make("proof-worker"), githubRepoId: GitHubRepositoryId.make(1), requiredLanes: ["proof"], bypassPeer: true, approved: true };
  const task: Task = { taskId: work.taskId, owner: Peer.fields.owner.make("proof-owner"), fingerprint: yield* platform.fingerprint(work), request: work, runnerId: runner.id, state: TaskStateEnum.HELD, offerDecision: OfferDecisionEnum.bypass, createdAt: now, updatedAt: now };
  yield* Effect.scoped(Effect.gen(function*() {
    const context = yield* Layer.build(Broker.layer(broker));
    const live = Context.get(context, Broker);
    const db = live.database;
    yield* db.query((client) => client.insertInto("tasks").values({ id: task.taskId, data: Schema.encodeSync(Schema.fromJsonString(Task))(task) }));
    const peerWork = { ...work, taskId: TaskId.make("accepted-agent-peer") };
    const peer: Task = { ...task, taskId: peerWork.taskId, request: peerWork, fingerprint: yield* platform.fingerprint(peerWork), runnerId: undefined, state: TaskStateEnum.ACCEPTED, offerDecision: OfferDecisionEnum.accepted };
    yield* db.query((client) => client.insertInto("tasks").values({ id: peer.taskId, data: Schema.encodeSync(Schema.fromJsonString(Task))(peer) }));
    yield* db.query((client) => client.insertInto("archive").values({ owner: task.owner, session: SessionId.make("private-session"), entry: Archive.fields.entry.make("private-entry"), digest: Archive.fields.digest.make("b".repeat(64)), payload: "private transcript retained", gaps: "[]", created: now }));
    yield* db.query((client) => client.insertInto("consents").values({ owner: task.owner, session: SessionId.make("private-session"), enabled: 1, participants: '["proof-owner"]' }));
    yield* db.query((client) => client.insertInto("participant_consents").values({ owner: task.owner, session: SessionId.make("private-session"), participant: ParticipantConsent.fields.participant.make("proof-owner"), enabled: 1 }));
    const fenced = yield* Effect.exit(recover("backup", broker, join(privateRoot, "blocked"), Evidence.fields.sha.make("a".repeat(40))));
    assert.equal(fenced._tag, "Failure", "live broker writer must fence recovery");
  }));
  const childRoot = join(children, yield* platform.digest(work.taskId));
  for (const name of [childRoot, join(childRoot, "repo"), join(childRoot, "sessions"), join(childRoot, "home"), join(childRoot, "home", ".omp"), join(childRoot, "home", ".omp", "agent")]) yield* fs.makeDirectory(name, { mode: 0o700 });
  yield* fs.writeFileString(join(childRoot, "repo", "retained.txt"), "accepted workspace state", { mode: 0o600 });
  yield* fs.writeFileString(join(childRoot, "sessions", "fixture.jsonl"), "private agent session fixture", { mode: 0o600 });
  yield* fs.writeFileString(join(childRoot, "home", ".omp", "agent", "auth.json"), "excluded-model-secret", { mode: 0o600 });
  yield* fs.symlink("retained.txt", join(childRoot, "repo", "retained-link"));
  yield* fs.chown(join(childRoot, "sessions", "fixture.jsonl"), 1000001, 1000001);
  for (const file of [join(childRoot, "repo", "retained.txt"), join(childRoot, "repo", "retained-link"), join(childRoot, "repo"), join(childRoot, "sessions")]) {
    yield* platform.exec(ChildProcess.make("/usr/bin/chown", ["--no-dereference", "1000001:1000001", "--", file], { env: {}, extendEnv: false }));
  }
  yield* Effect.scoped(Effect.gen(function*() {
    const db = yield* DatabaseService;
    const fingerprint = yield* platform.fingerprint(work);
    yield* db.query((client) => client.insertInto("runner_tasks").values({ taskId: work.taskId, fingerprint, work: Schema.encodeSync(Schema.fromJsonString(Work))(work), worktree: join(childRoot, "repo"), state: RunnerStateEnum.PROMPT_INTENT, occupied: 1, incarnation: RunnerTask.fields.incarnation.make("fixture-incarnation"), promptId: AgentSubmissionId.make("fixture-prompt") }));
    const accepted = { ...work, taskId: TaskId.make("accepted-recovery-task") };
    const acceptedFingerprint = yield* platform.fingerprint(accepted);
    const acceptedDirectory = yield* platform.digest(accepted.taskId);
    yield* db.query((client) => client.insertInto("runner_tasks").values({ taskId: accepted.taskId, fingerprint: acceptedFingerprint, work: Schema.encodeSync(Schema.fromJsonString(Work))(accepted), worktree: join(children, acceptedDirectory, "repo"), state: RunnerStateEnum.ACCEPTED, occupied: 1, incarnation: RunnerTask.fields.incarnation.make("accepted-incarnation") }));
    yield* db.query((client) => client.insertInto("runner_events").values({ taskId: work.taskId, payload: Schema.encodeSync(Schema.fromJsonString(Schema.Json))({ type: "runner_state", state: RunnerStateEnum.PROMPT_INTENT }) }));
  }).pipe(Effect.provide(DatabaseService.layer(runnerUrl))));
  const artifact = join(privateRoot, "artifact"), sourceSha = Evidence.fields.sha.make("a".repeat(40));
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
      const db = yield* DatabaseService;
      yield* db.query((client) => client.updateTable("runner_tasks").set({ pid: RunnerTask.fields.pid.members[0].make(child.pid) }).where("taskId", "=", work.taskId));
    }).pipe(Effect.provide(DatabaseService.layer(runnerUrl))));
    assert.equal((yield* Effect.exit(recover("backup", broker, join(privateRoot, "live-child-blocked"), sourceSha, runner)))._tag, "Failure", "recorded live agent child must fence backup");
    yield* Effect.scoped(Effect.gen(function*() {
      const db = yield* DatabaseService;
      yield* db.query((client) => client.updateTable("runner_tasks").set({ pid: null }).where("taskId", "=", work.taskId));
    }).pipe(Effect.provide(DatabaseService.layer(runnerUrl))));
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
  const targetUrl = yield* provisionPostgres();
  yield* Effect.addFinalizer(() => dropPostgres(targetUrl).pipe(Effect.orDie));
  const targetRunnerUrl = yield* provisionPostgres();
  yield* Effect.addFinalizer(() => dropPostgres(targetRunnerUrl).pipe(Effect.orDie));
  const restoredBroker = broker, restoredRunner = runner;
  yield* Effect.gen(function*() {
  const acknowledgement = yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, restoredRunner));
  assert.equal(acknowledgement._tag, "Failure", "pending accepted recovery requires explicit HELD fence acknowledgement");
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, Evidence.fields.sha.make("b".repeat(40)), restoredRunner, true)))._tag, "Failure", "source revision mismatch must fence restore");
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, { ...restoredRunner, childUidBase: UserId.make(runner.childUidBase + 1000) }, true)))._tag, "Failure", "runner UID layout mismatch must fence restore");
  const dumpFile = join(artifact, "broker.pgdump"), dumpBytes = yield* fs.readFile(dumpFile);
  yield* fs.writeFileString(dumpFile, "tampered native dump", { mode: 0o600 });
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, restoredRunner, true)))._tag, "Failure", "broker digest mismatch must fence restore before target mutation");
  yield* fs.writeFile(dumpFile, dumpBytes, { mode: 0o600 });
  yield* Effect.scoped(Effect.gen(function*() {
    const db = yield* DatabaseService;
    assert.equal(Number((yield* db.query((client) => client.selectFrom("tasks").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0, "rejected restore must not write ledger");
  }).pipe(Effect.provide(postgresAdmin(targetUrl))));
  const unexpectedState = join(runner.stateDirectory, "unexpected-state");
  yield* fs.writeFileString(unexpectedState, "disposable state sentinel", { mode: 0o600 });
  assert.equal((yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, restoredRunner, true)))._tag, "Failure", "unexpected runner local state must fence paired restore");
  yield* fs.remove(unexpectedState);
  yield* recover("restore", restoredBroker, artifact, sourceSha, restoredRunner, true);
  assert.equal(yield* fs.readFileString(join(childRoot, "repo", "retained-link")), "accepted workspace state");
  assert.equal(yield* fs.readFileString(join(childRoot, "sessions", "fixture.jsonl")), "private agent session fixture");
  assert.equal(yield* fs.exists(join(childRoot, "home", ".omp", "agent", "auth.json")), false);
  for (const [file, mode] of Array.of<readonly [string, number]>(
    [join(childRoot, "repo"), 0o700], [join(childRoot, "repo", "retained.txt"), 0o600],
    [join(childRoot, "sessions"), 0o700], [join(childRoot, "sessions", "fixture.jsonl"), 0o600],
  )) {
    const info = yield* fs.stat(file);
    assert(Option.isSome(info.uid) && Option.isSome(info.gid), "agent ownership metadata must be available");
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
    const db = openedBroker.database;
    const rows = yield* db.query((client) => client.selectFrom("tasks").selectAll().where("id", "=", task.taskId));
    assert.equal(Schema.decodeSync(Schema.fromJsonString(Task))(rows[0]!.data).state, TaskStateEnum.HELD);
    const peerRows = yield* db.query((client) => client.selectFrom("tasks").selectAll().where("id", "=", TaskId.make("accepted-agent-peer")));
    const peer = Schema.decodeSync(Schema.fromJsonString(Task))(peerRows[0]!.data);
    assert.equal(peer.state, TaskStateEnum.HELD);
    assert.equal(peer.offerDecision, OfferDecisionEnum.accepted, "recovery must not erase accepted peer ownership");
    assert.equal((yield* db.query((client) => client.selectFrom("archive").selectAll()))[0]?.payload, "private transcript retained");
    assert.equal((yield* db.query((client) => client.selectFrom("consents").selectAll()))[0]?.enabled, 1);
    assert.equal((yield* db.query((client) => client.selectFrom("participant_consents").selectAll()))[0]?.enabled, 1);
    assert.equal((yield* db.query((client) => client.selectFrom("recovery_gate").selectAll()))[0]?.ready, true);
    const sessionInfo = yield* fs.stat(join(childRoot, "sessions", "fixture.jsonl"));
    assert(Option.isSome(sessionInfo.uid));
    assert.equal(sessionInfo.uid.value, 1000001, "agent session ownership must survive");
    const runnerContext = yield* Layer.build(Runner.layer(restoredRunner).pipe(Layer.provide(postgresConfig(targetRunnerUrl))));
    const openedRunner = Context.get(runnerContext, Runner);
    const uncertain = yield* openedRunner.row(work.taskId);
    assert.equal(uncertain?.state, RunnerStateEnum.HELD);
    assert.equal(uncertain?.promptId, "fixture-prompt");
    assert.equal(uncertain?.occupied, 1);
    assert.equal((yield* openedRunner.row(TaskId.make("accepted-recovery-task")))?.state, RunnerStateEnum.HELD);
    const journal = openedRunner.database;
    assert.equal(Number((yield* journal.query((client) => client.selectFrom("runner_events").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 1);
    yield* Effect.sleep("100 millis");
    assert.equal(yield* fs.readFileString(marker), "", "restored accepted/uncertain work must never invoke launcher");
  }));
  const overwrite = yield* Effect.exit(recover("restore", restoredBroker, artifact, sourceSha, restoredRunner, true));
  assert.equal(overwrite._tag, "Failure", "restore cannot overwrite existing ledger");
  yield* Console.log(Formatter.formatJson({ scenario: "private-offline-recovery", fixtureOnly: true, actualBrokerReopened: true, actualRunnerReopened: true, workspaceAndAgentSessionRetained: true,
    modelCredentialExcluded: true, brokerLockFenced: true, acceptedRecoveryExplicitlyHeld: true, uncertainPromptAndOccupiedSlotRetained: true, launcherInvocations: 0, productionBackupClaim: false }, { space: 2 }));
  }).pipe(Effect.provide(postgresConfig(targetUrl, targetRunnerUrl)));
  }).pipe(Effect.provide(postgresConfig(sourceUrl, runnerUrl)));
})).pipe(Effect.provide(Layer.mergeAll(Platform.layer, Mcp.layer))));
