import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { randomBytes, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { DatabaseSync } from "node:sqlite";
import { digest, fingerprint, rpc, type BrokerConfig, type RunnerConfig, type Task, type Work, type Acceptance, type Address } from "../src/protocol.ts";
import { inspectGithubRepository } from "../src/evidence.ts";

const root = resolve(import.meta.dirname, "..");
const state = process.env.COORDINATOR_SMOKE_DIRECTORY ?? mkdtempSync(join(tmpdir(), "coordinator-sockets-"));
mkdirSync(state, { recursive: true, mode: 0o700 }); chmodSync(state, 0o700);
const repo = "github.com/samuelho-dev/coordinator";
const sha = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
const identity = await inspectGithubRepository(repo, process.env.GH_TOKEN);
const secret = () => randomBytes(32).toString("hex");
const coordinator = secret(), recipient = secret(), runnerToken = secret(), reviewer = secret(), writer = secret(), outsider = secret();
const tokenDirectory = join(state, "tokens"); mkdirSync(tokenDirectory, { mode: 0o700 });
writeFileSync(join(tokenDirectory, "homelab-runner"), runnerToken, { mode: 0o600 });
async function port(): Promise<number> {
  const server = createServer(); const ready = Promise.withResolvers<number>();
  server.listen(0, "127.0.0.1", () => { const address = server.address(); if (!address || typeof address === "string") ready.reject(new Error("No port")); else server.close(() => ready.resolve(address.port)); });
  return ready.promise;
}
const brokerPort = await port(), runnerPort = await port();
const brokerUrl = `http://127.0.0.1:${brokerPort}`, runnerUrl = `http://127.0.0.1:${runnerPort}`;
const brokerConfig: BrokerConfig = { database: join(state, "broker", "ledger.sqlite"), host: "127.0.0.1", port: brokerPort,
  backupDirectory: join(state, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000, githubTokenEnv: "GH_TOKEN",
  principals: [
    { id: "coordinator", owner: "owner", roles: ["coordinator"], repos: [repo], tokenHash: digest(coordinator) },
    { id: "recipient", owner: "owner", roles: ["session"], repos: [repo], tokenHash: digest(recipient) },
    { id: "homelab-runner", owner: "owner", roles: ["runner"], repos: [repo], tokenHash: digest(runnerToken), runnerUrl },
    { id: "reviewer", owner: "owner", roles: ["reviewer"], repos: [repo], tokenHash: digest(reviewer) },
    { id: "writer", owner: "owner", roles: ["archive-writer"], repos: [repo], tokenHash: digest(writer) },
    { id: "outsider", owner: "outsider", roles: ["coordinator", "session"], repos: [repo], tokenHash: digest(outsider) },
  ] };
const launcher = process.env.COORDINATOR_TEST_OMP ?? "/home/admin/.bun/bin/omp";
const runnerConfig: RunnerConfig = { id: "homelab-runner", brokerUrl, tokenFile: join(tokenDirectory, "homelab-runner"), host: "127.0.0.1", port: runnerPort,
  stateDirectory: join(state, "runner"), slots: 1, repositories: { [repo]: root }, launchers: { omp: launcher } };
const brokerPath = join(state, "broker.json"), runnerPath = join(state, "runner.json");
writeFileSync(brokerPath, JSON.stringify(brokerConfig), { mode: 0o600 }); writeFileSync(runnerPath, JSON.stringify(runnerConfig), { mode: 0o600 });
const transcript: { scenario: string; observed: unknown; time: string }[] = [];
function evidence(scenario: string, observed: unknown): void { transcript.push({ scenario, observed, time: new Date().toISOString() }); console.log(JSON.stringify({ scenario, observed })); }
function processFor(file: string, config: string): ChildProcessWithoutNullStreams {
  const child = spawn(process.execPath, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "src", file)], {
    cwd: root, env: { ...process.env, COORDINATOR_CONFIG: config, COORDINATOR_RUNNER_TOKEN_DIRECTORY: tokenDirectory }, stdio: ["pipe", "pipe", "pipe"],
  });
  child.stdout.on("data", () => {}); child.stderr.on("data", () => {});
  return child;
}
async function until<T>(read: () => Promise<T>, check: (value: T) => boolean, milliseconds = 90000): Promise<T> {
  const deadline = Date.now() + milliseconds;
  let error: unknown;
  while (Date.now() < deadline) { try { const value = await read(); if (check(value)) return value; } catch (caught) { error = caught; } await delay(200); }
  throw new Error(`Scenario deadline expired${error instanceof Error ? `: ${error.message.slice(0,200)}` : ""}`);
}
async function ready(url: string): Promise<void> { await until(async () => (await fetch(new URL("/healthz", url), { headers: { authorization: `Bearer ${url === runnerUrl ? runnerToken : coordinator}` } })).ok, (value) => value, 30000); }
async function exit(child: ChildProcessWithoutNullStreams, signal: NodeJS.Signals): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const done = Promise.withResolvers<void>(); child.once("exit", () => done.resolve()); child.kill(signal); await done.promise;
}
let broker = processFor("broker.ts", brokerPath); let runner: ChildProcessWithoutNullStreams | undefined;
let workerPid: number | undefined;
try {
  await ready(brokerUrl);
  await assert.rejects(rpc(brokerUrl, secret(), "task.get", { taskId: "missing" }), /401/);
  await assert.rejects(rpc(brokerUrl, writer, "task.submit", {}), /403/);
  evidence("authentication-and-operation-authorization", { invalidCredential: 401, archiveWriterCannotDispatch: 403 });
  const session = { sessionId: "native-owner-session", incarnation: randomUUID(), machineId: "homelab", host: "omp", repo };
  await rpc(brokerUrl, recipient, "session.heartbeat", session); await rpc(brokerUrl, recipient, "session.poll", sessionKey(session));
  const work: Work = { taskId: `smoke-${randomUUID()}`, repo, ref: "refs/heads/main", sha, launcher: "omp", prompt: "Respond exactly COORDINATOR_SMOKE_OK. Do not call tools, change files, merge or deploy.", scope: "Socket lifecycle smoke only", restrictions: "No file modifications, merging or deployment", expectedEvidence: ["one owned real omp child", "exact-SHA result"], deadline: Date.now() + 600000,
    workerId: "existing-omp-smoke-worker", githubRepoId: identity.id, requiredLanes: ["CI/verify"], bypassPeer: false, approved: true };
  const peerTask = await rpc<Task>(brokerUrl, coordinator, "task.submit", work);
  assert.equal(peerTask.offerDecision, "pending"); assert.equal(peerTask.runnerId, undefined);
  evidence("peer-first-owner-offer", { taskId: work.taskId, state: peerTask.state, decision: peerTask.offerDecision, runnerAssigned: false });
  const message = { messageId: randomUUID(), ...sessionKey(session), payload: { note: "addressed owner notification" }, ttlMs: 60000 };
  await rpc(brokerUrl, coordinator, "peer.send", message);
  const before = await rpc<{messages: unknown[]}>(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.equal(before.messages.length, 1);
  await exit(broker, "SIGKILL"); broker = processFor("broker.ts", brokerPath); await ready(brokerUrl);
  await assert.rejects(rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: randomUUID() }), /409/);
  await rpc(brokerUrl, recipient, "session.heartbeat", session);
  const replay = await rpc<{messages: unknown[]}>(brokerUrl, recipient, "session.poll", sessionKey(session)); assert.deepEqual(replay.messages, before.messages);
  await rpc(brokerUrl, recipient, "peer.ack", { ...sessionKey(session), messageId: message.messageId });
  assert.equal((await rpc<{messages: unknown[]}>(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  evidence("broker-crash-same-incarnation-replay", { replayed: 1, newSendBeforeHeartbeatRejected: true, handledAckDeletes: true });
  await rpc(brokerUrl, coordinator, "peer.send", { ...message, messageId: "expires", ttlMs: 100 }); await delay(150);
  assert.equal((await rpc<{messages: unknown[]}>(brokerUrl, recipient, "session.poll", sessionKey(session))).messages.length, 0);
  const address: Address = { messageId: work.taskId, revision: 1, workerId: work.workerId, ...sessionKey(session), repo, ref: work.ref, sha, scope: work.scope, restrictions: work.restrictions, expectedEvidence: work.expectedEvidence, deadline: work.deadline };
  await rpc(brokerUrl, recipient, "peer.reply", { taskId: work.taskId, ...sessionKey(session), decision: "accepted" });
  await rpc(brokerUrl, coordinator, "task.address", { taskId: work.taskId, address });
  await assert.rejects(rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: { ...address, ref: "wrong-ref" }, disposition: "ready" }), /409/);
  await assert.rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), /409/);
  await rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "hold", reason: "await correction", nextCheck: Date.now() + 60000 });
  await assert.rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 }), /409/);
  await rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address, disposition: "ready" }); await rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 1 });
  const second = { ...session, sessionId: "handoff-session", incarnation: randomUUID() }; await rpc(brokerUrl, recipient, "session.heartbeat", second); await rpc(brokerUrl, recipient, "session.poll", sessionKey(second));
  await rpc(brokerUrl, coordinator, "task.handoff", { taskId: work.taskId, oldSessionId: session.sessionId, oldIncarnation: session.incarnation, newSessionId: second.sessionId, newIncarnation: second.incarnation, revision: 2 });
  await assert.rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), /409/);
  const handoffAddress = { ...address, ...sessionKey(second), revision: 2 };
  await rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: handoffAddress, disposition: "ready" }); await rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 });
  evidence("expiry-correction-hold-and-handoff-gate", { expiredPeerNotDelivered: true, wrongRefRejected: true, heldConfirmationRejected: true, handoffRequiresNewReadback: true });
  const instructionAddress = { ...handoffAddress, messageId: "later-owned-message", revision: 3, scope: "A separately approved later instruction" };
  await rpc(brokerUrl, coordinator, "task.instruction", { taskId: work.taskId, address: instructionAddress, prompt: "Later approved instruction; do not repeat the launch prompt", approved: true });
  assert.equal((await rpc<{releases:Task[]}>(brokerUrl, recipient, "session.poll", sessionKey(second))).releases.length, 0);
  await assert.rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 2 }), /409/);
  await rpc(brokerUrl, recipient, "task.readback", { taskId: work.taskId, address: instructionAddress, disposition: "ready" });
  await rpc(brokerUrl, coordinator, "task.confirm", { taskId: work.taskId, revision: 3 });
  const later = (await rpc<{releases:Task[]}>(brokerUrl, recipient, "session.poll", sessionKey(second))).releases[0]!;
  assert.equal(later.instruction?.prompt, "Later approved instruction; do not repeat the launch prompt");
  await rpc(brokerUrl, recipient, "task.report", { taskId: work.taskId, ...sessionKey(second), state: "SUCCEEDED", result: { syntheticPeerGate: true } });
  evidence("later-native-instruction-gate", { sameTaskId: later.taskId, previousRevisionCannotRelease: true, newReadbackRequired: true, approvedPromptDistinct: true });
  await rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner"], enabled: true });
  const payload = { id: "entry-one", parentId: null, type: "message", contentOmitted: true };
  const archive = { sessionId: session.sessionId, entryId: "entry-one", digest: fingerprint(payload), payload, gaps: ["content-omitted", "unlinked"] };
  await rpc(brokerUrl, writer, "archive.append", archive); await rpc(brokerUrl, writer, "archive.append", archive);
  const unsafePayload = { ...payload, text: "unsafe-raw-content-must-not-persist" };
  await assert.rejects(rpc(brokerUrl, writer, "archive.append", { ...archive, entryId: "unsafe", payload: unsafePayload, digest: fingerprint(unsafePayload) }), /RPC (400|409)/);
  assert.equal((await rpc<unknown[]>(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId })).length, 1);
  await assert.rejects(rpc(brokerUrl, writer, "archive.delete", { sessionId: session.sessionId }), /403/);
  assert.deepEqual(await rpc(brokerUrl, outsider, "archive.list", { sessionId: session.sessionId }), []);
  const snapshotNames = new Set(readdirSync(brokerConfig.backupDirectory));
  const snapshotName = await until(async () => readdirSync(brokerConfig.backupDirectory).find((name) => name.endsWith(".sqlite") && !snapshotNames.has(name)), (name) => Boolean(name), 75000);
  const snapshot = new DatabaseSync(join(brokerConfig.backupDirectory, snapshotName!), { readOnly: true });
  for (const table of ["archive", "consents", "participant_consents"]) assert.equal(snapshot.prepare(`SELECT count(*) AS count FROM ${table}`).get()?.count, 0);
  assert.ok(snapshot.prepare("SELECT id FROM tasks WHERE id=?").get(work.taskId)); snapshot.close();
  assert.equal((await rpc<unknown[]>(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId })).length, 1);
  await assert.rejects(rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: false }), /403/);
  await rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: true });
  await rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner", "outsider"], enabled: true });
  await rpc(brokerUrl, outsider, "archive.participantConsent", { owner: "owner", sessionId: session.sessionId, enabled: false });
  assert.deepEqual(await rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  await rpc(brokerUrl, recipient, "session.heartbeat", session);
  assert.equal((await rpc<{archiveEnabled:boolean}>(brokerUrl, recipient, "session.poll", sessionKey(session))).archiveEnabled, false);
  await rpc(brokerUrl, recipient, "archive.consent", { sessionId: session.sessionId, participants: ["owner"], enabled: true });
  await rpc(brokerUrl, writer, "archive.append", archive);
  await rpc(brokerUrl, coordinator, "archive.delete", { sessionId: session.sessionId });
  await assert.rejects(rpc(brokerUrl, writer, "archive.append", archive), /403/);
  await exit(broker, "SIGTERM"); broker = processFor("broker.ts", brokerPath); await ready(brokerUrl);
  assert.deepEqual(await rpc(brokerUrl, coordinator, "archive.list", { sessionId: session.sessionId }), []);
  evidence("archive-privacy-idempotency-revocation-and-restart", { uniqueEntries: 1, appendOnlyCannotDelete: true, otherOwnerSeesNoRows: true, optoutRejectsAppendAfterRestart: true, snapshotsExcludeArchiveAndConsent: true });
  runner = processFor("runner.ts", runnerPath); await ready(runnerUrl); await delay(300);
  const worker = { ...work, taskId: `worker-${randomUUID()}`, bypassPeer: true };
  const submitted = await rpc<Task>(brokerUrl, coordinator, "task.submit", worker); const duplicate = await rpc<Task>(brokerUrl, coordinator, "task.submit", worker); assert.equal(submitted.taskId, duplicate.taskId);
  await assert.rejects(rpc(brokerUrl, coordinator, "task.submit", { ...worker, prompt: "different" }), /409/);
  const accepted = await until(() => rpc<Task>(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => Boolean(task.readback), 120000);
  assert.equal(accepted.address?.sha, sha); assert.equal(accepted.runnerId, "homelab-runner");
  const runnerDb = new DatabaseSync(join(runnerConfig.stateDirectory, "runner.sqlite"), { readOnly: true });
  const row = runnerDb.prepare("SELECT pid FROM tasks WHERE taskId=?").get(worker.taskId); workerPid = Number(row?.pid); runnerDb.close();
  assert.ok(workerPid > 0);
  assert.equal((await rpc<Acceptance>(runnerUrl, runnerToken, "acceptTask", worker)).taskId, worker.taskId);
  const corrected = { ...accepted.address!, revision: 2 };
  const correctedTask = await rpc<Task>(brokerUrl, coordinator, "task.address", { taskId: worker.taskId, address: corrected });
  assert.deepEqual(correctedTask.readback, corrected);
  await assert.rejects(rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 1 }), /409/);
  const full = await rpc<Acceptance>(runnerUrl, runnerToken, "acceptTask", { ...worker, taskId: "full-capacity" }); assert.equal(full.accepted, false); assert.equal(full.reason, "full");
  await assert.rejects(rpc(brokerUrl, outsider, "task.get", { taskId: worker.taskId }), /403/);
  await assert.rejects(rpc(brokerUrl, writer, "task.attach", { taskId: worker.taskId }), /403/);
  await exit(broker, "SIGKILL"); broker = processFor("broker.ts", brokerPath); await ready(brokerUrl);
  const attached = await rpc<{taskId:string}>(brokerUrl, coordinator, "task.attach", { taskId: worker.taskId }); assert.equal(attached.taskId, worker.taskId); process.kill(workerPid, 0);
  await rpc(brokerUrl, coordinator, "task.confirm", { taskId: worker.taskId, revision: 2 });
  const finished = await until(() => rpc<Task>(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }), (task) => ["SUCCEEDED", "FAILED", "HELD"].includes(task.state), 180000);
  assert.equal(finished.state, "SUCCEEDED", `Real host final state ${finished.state}: ${JSON.stringify(finished.result)}`);
  const finalDb = new DatabaseSync(join(runnerConfig.stateDirectory, "runner.sqlite"), { readOnly: true });
  const one = finalDb.prepare("SELECT count(*) AS count FROM tasks WHERE taskId=?").get(worker.taskId); const launches = finalDb.prepare("SELECT count(*) AS count FROM events WHERE taskId=? AND json_extract(payload,'$.state')='LAUNCHING'").get(worker.taskId); assert.equal(one?.count, 1); assert.equal(launches?.count, 1); finalDb.close();
  evidence("real-owned-omp-child-duplicate-full-and-coordinator-exit", { sha, workerPid, nativeSession: accepted.address!.sessionId, duplicateConflict: true, fullSlotsRejectedWithoutAcceptance: true, brokerCrashAttachPreserved: true, finalState: finished.state, acceptanceRows: 1, launchEvents: 1, evidenceVerified: false });
  await rpc(brokerUrl, coordinator, "evidence.local", { taskId: worker.taskId, sha, commands: ["real socket smoke"], passed: true });
  await rpc(brokerUrl, coordinator, "evidence.deployment", { taskId: worker.taskId, sha: "a".repeat(40), environment: "independent-demo-environment", url: "https://example.invalid", observedState: "not deployed", provenance: "smoke distinct negative observation" });
  await assert.rejects(rpc(brokerUrl, coordinator, "evidence.review", { taskId: worker.taskId, sha, decision: "approved", note: "unauthorized" }), /403/);
  await rpc(brokerUrl, reviewer, "evidence.review", { taskId: worker.taskId, sha, decision: "changes-requested", note: "Smoke credential provenance; not user approval" });
  await rpc(brokerUrl, coordinator, "evidence.target", { taskId: worker.taskId, sha: "b".repeat(40), ref: "main", requiredLanes: ["CI/verify"] });
  const observed = await rpc<Task & { evidence: { stale: boolean; kind: string }[] }>(brokerUrl, coordinator, "task.get", { taskId: worker.taskId }); assert.equal(observed.state, "SUCCEEDED"); assert.ok(observed.evidence.every((item) => item.stale));
  evidence("independent-evidence-and-new-SHA-staleness", { runnerStateUnchanged: observed.state, oldLocalCiDeploymentReviewStale: true, reviewerAuthorizationSeparate: true });
  writeFileSync(join(state, "transcript.json"), JSON.stringify({ brokerRuntime: process.version, host: execFileSync(launcher, ["--version"], { encoding: "utf8" }).trim(), privateRepository: identity.private, repositoryId: identity.id, sha, scenarios: transcript }, null, 2), { mode: 0o600 });
  console.log(`Evidence: ${join(state, "transcript.json")}`);
} finally {
  if (runner) await exit(runner, "SIGTERM"); await exit(broker, "SIGTERM");
  if (workerPid) { try { process.kill(workerPid, "SIGTERM"); } catch {} }
}
function sessionKey(session: {sessionId:string;incarnation:string}) { return { sessionId: session.sessionId, incarnation: session.incarnation }; }
