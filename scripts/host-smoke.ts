import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes, randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { Schema } from "effect";
import { digest, fingerprint, rpc, type Address, type BrokerConfig, type PluginConfig, type Task, type Work } from "../src/protocol.ts";

// Only explicitly synthetic sessions and an empty, local Git repository are read.
// No model prompt, real host profile, user history, GitHub credential or service manager is used.
const root = resolve(import.meta.dirname, "..");
const directory = mkdtempSync(join(tmpdir(), "coordinator-native-hosts-"));
chmodSync(directory, 0o700);
const transcriptPath = join(directory, "transcript.json");
interface Observation { operation: string; observed: unknown }
const observations: Observation[] = [];
function record(operation: string, observed: unknown): void {
  observations.push({ operation, observed });
  writeFileSync(transcriptPath, JSON.stringify({ platform: process.platform, macRuntimeProved: false, observations }, null, 2), { mode: 0o600 });
}
function privateJson(path: string, value: unknown): void { writeFileSync(path, JSON.stringify(value), { mode: 0o600 }); }
function privateDirectory(path: string): string { mkdirSync(path, { recursive: true, mode: 0o700 }); return path; }
const children = new Set<ChildProcessWithoutNullStreams>();
// Whitelist, not inherited provider credentials/profile environment.
function isolatedEnvironment(home: string, agent: string): NodeJS.ProcessEnv {
  return { PATH: process.env.PATH, HOME: home, USER: "coordinator-synthetic", LANG: "C.UTF-8", TERM: "dumb",
    XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state"),
    XDG_CACHE_HOME: join(home, "cache"), PI_CODING_AGENT_DIR: agent, PI_CODING_AGENT_SESSION_DIR: join(home, "sessions"),
    // Synthetic invalid key permits native model discovery; slash-command proof must start no model turn.
    OPENAI_API_KEY: "coordinator-synthetic-invalid-key" };
}
function start(binary: string, args: string[], cwd: string, env: NodeJS.ProcessEnv): ChildProcessWithoutNullStreams {
  const child = spawn(binary, args, { cwd, env, detached: true, stdio: ["pipe", "pipe", "pipe"] });
  children.add(child);
  child.stderr.on("data", () => {}); // Never echo arbitrary host/provider diagnostics.
  return child;
}
async function stop(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (!child.pid) return;
  const signal = (value: NodeJS.Signals) => {
    try { process.kill(-child.pid!, value); }
    catch (error) { if (!error || typeof error !== "object" || !("code" in error) || error.code !== "ESRCH") throw error; }
  };
  signal("SIGTERM");
  if (child.exitCode === null && child.signalCode === null) {
    const exited = Promise.withResolvers<void>();
    child.once("exit", () => exited.resolve());
    await Promise.race([exited.promise, delay(3000)]);
  }
  signal("SIGKILL"); // Include descendants even if their group leader already exited.
  if (child.exitCode === null && child.signalCode === null) {
    const exited = Promise.withResolvers<void>(); child.once("exit", () => exited.resolve());
    await Promise.race([exited.promise, delay(3000)]);
    assert.ok(child.exitCode !== null || child.signalCode !== null, "Owned host failed to terminate");
  }
  children.delete(child);
}
interface Frame { type: string; id?: string; command?: string; success?: boolean; data?: unknown; method?: string; message?: string; title?: string; notifyType?: string }
const decodeFrame = Schema.decodeUnknownSync(Schema.Struct({
  type: Schema.String, id: Schema.optional(Schema.String), command: Schema.optional(Schema.String),
  success: Schema.optional(Schema.Boolean), data: Schema.optional(Schema.Unknown), method: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String), title: Schema.optional(Schema.String), notifyType: Schema.optional(Schema.String),
}));
interface Pending { resolve(value: Frame): void; reject(error: Error): void }
interface NativeState { sessionId: string; sessionFile?: string; isStreaming: boolean; messageCount: number }
interface Identity { sessionId: string; incarnation: string; machineId: string; repo: string; ref: string; sha: string; workerId: string; workerIdentitySource: "native-session" }
interface InboxEntry { payload: unknown; digest: string; receivedAt: number }
interface ArchiveRow { sequence: number; entryId: string; digest: string; payload: unknown; gaps: string[] }
class NativeRpc {
  readonly frames: Frame[] = [];
  readonly pending = new Map<string, Pending>();
  failure: Error | undefined;
  private diagnostics = "";
  constructor(readonly child: ChildProcessWithoutNullStreams, readonly host: string) {
    createInterface({ input: child.stdout }).on("line", line => {
      let frame: Frame;
      try { frame = decodeFrame(JSON.parse(line)); } catch { this.fail(new Error(`${host}: invalid native RPC stdout (unsupported API)`)); return; }
      this.frames.push(frame);
      if (frame.type === "response" && frame.id) {
        const pending = this.pending.get(frame.id);
        if (pending) { this.pending.delete(frame.id); if (frame.success === true) pending.resolve(frame); else pending.reject(new Error(`${host}: ${frame.command} native RPC rejected (unsupported API or extension failure)`)); }
      }
    });
    child.on("error", () => this.fail(new Error(`${host}: cannot start native executable`)));
    child.on("exit", (code, signal) => this.fail(new Error(`${host}: native process exited (${code ?? signal}); ${this.diagnostics}`)));
    child.stderr.on("data", bytes => { if (this.diagnostics.length < 4096) this.diagnostics += String(bytes).slice(0, 4096 - this.diagnostics.length); });
  }
  fail(error: Error): void { this.failure = error; for (const pending of this.pending.values()) pending.reject(error); this.pending.clear(); }
  send(value: unknown): void { if (this.failure) throw this.failure; this.child.stdin.write(`${JSON.stringify(value)}\n`); }
  async request(type: string, fields: Record<string, unknown> = {}): Promise<Frame> {
    const id = randomUUID(), result = Promise.withResolvers<Frame>();
    this.pending.set(id, result); this.send({ type, id, ...fields });
    const timer = setTimeout(() => { this.pending.delete(id); result.reject(new Error(`${this.host}: ${type} response deadline expired; frames=${this.frames.map(frame => frame.type).join(",")}; ${this.diagnostics}`)); }, 60000);
    try { const frame = await result.promise; assert.equal(frame.command, type); return frame; } finally { clearTimeout(timer); }
  }
  async frame(from: number, check: (frame: Frame) => boolean): Promise<Frame> {
    return until(async () => { if (this.failure) throw this.failure; return this.frames.slice(from).find(check); }, value => value !== undefined, `${this.host}: missing expected extension_ui_request`) as Promise<Frame>;
  }
  async command(action: string, check: (value: unknown) => boolean): Promise<unknown> {
    const from = this.frames.length;
    await this.request("prompt", { message: `/coordinator ${action}` });
    const frame = await this.frame(from, item => {
      if (item.type !== "extension_ui_request" || item.method !== "notify" || !item.message) return false;
      if (item.notifyType === "error") throw new Error(`${this.host}: coordinator command failed`);
      try { return check(JSON.parse(item.message)); } catch (error) { if (error instanceof SyntaxError) return false; throw error; }
    });
    return JSON.parse(frame.message!);
  }
  async rejectedCommand(action: string): Promise<void> {
    const from = this.frames.length;
    await this.request("prompt", { message: `/coordinator ${action}` });
    await this.frame(from, item => item.type === "extension_ui_request" && item.method === "notify" && item.notifyType === "error");
  }
}
async function until<T>(read: () => Promise<T>, check: (value: T) => boolean, label: string): Promise<T> {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) { const value = await read(); if (check(value)) return value; await delay(100); }
  throw new Error(`${label}: deadline expired`);
}
async function freePort(): Promise<number> {
  const server = createServer(), ready = Promise.withResolvers<number>();
  server.on("error", ready.reject);
  server.listen(0, "127.0.0.1", () => { const address = server.address(); if (!address || typeof address === "string") ready.reject(new Error("No loopback port")); else server.close(() => ready.resolve(address.port)); });
  return ready.promise;
}
async function run(binary: string, args: string[], cwd: string, env: NodeJS.ProcessEnv): Promise<string> {
  const child = start(binary, args, cwd, env), result = Promise.withResolvers<string>(); let output = "";
  child.stdout.on("data", bytes => { output += String(bytes); if (output.length > 1024 * 1024) result.reject(new Error("Native command output limit exceeded")); });
  child.on("error", () => result.reject(new Error("Native command executable unavailable")));
  child.on("exit", code => code === 0 ? result.resolve(output) : result.reject(new Error(`Native command failed: ${args[0]} (output suppressed)`)));
  const timer = setTimeout(() => result.reject(new Error(`Native command timed out: ${args[0]}`)), 30000);
  try { return await result.promise; } finally { clearTimeout(timer); await stop(child); }
}
const repo = "github.com/samuelho-dev/coordinator", owner = "synthetic-host-smoke-owner";
const coordinatorToken = randomBytes(32).toString("hex"), sessionToken = randomBytes(32).toString("hex");
const brokerPort = await freePort(), brokerUrl = `http://127.0.0.1:${brokerPort}`;
const brokerConfig: BrokerConfig = { database: join(directory, "broker", "ledger.sqlite"), host: "127.0.0.1", port: brokerPort,
  backupDirectory: join(directory, "snapshots"), archiveRetentionMs: 86400000, backupRetentionMs: 3600000,
  repositoryIds: { [repo]: 1 },
  principals: [{ id: "synthetic-control", owner, roles: ["coordinator"], repos: [repo], tokenHash: digest(coordinatorToken) },
    { id: "synthetic-native", owner, roles: ["session"], repos: [repo], tokenHash: digest(sessionToken) }] };
const brokerPath = join(directory, "broker.json"); privateJson(brokerPath, brokerConfig);
const home = privateDirectory(join(directory, "broker-home")), brokerEnv = isolatedEnvironment(home, join(home, "agent"));
const syntheticRepo = privateDirectory(join(directory, "synthetic-repository"));
const gitEnv = { ...brokerEnv, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" };
function git(args: string[]): string { return execFileSync("git", args, { cwd: syntheticRepo, env: gitEnv, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim(); }
try {
  git(["init", "--initial-branch=main"]);
  git(["-c", "user.name=Synthetic Smoke", "-c", "user.email=synthetic@example.invalid", "commit", "--allow-empty", "-m", "Synthetic native host smoke only"]);
  git(["remote", "add", "origin", "https://github.com/samuelho-dev/coordinator.git"]);
  const sha = git(["rev-parse", "HEAD"]);
  const broker = start(process.execPath, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "src/broker.ts")], root, { ...brokerEnv, COORDINATOR_CONFIG: brokerPath });
  broker.stdout.on("data", () => {}); broker.on("error", () => {});
  await until(async () => { if (broker.exitCode !== null) throw new Error("Private broker exited"); try { return (await fetch(`${brokerUrl}/healthz`, { headers: { authorization: `Bearer ${coordinatorToken}` }, signal: AbortSignal.timeout(1000) })).ok; } catch { return false; } }, value => value, "Private broker readiness");
  for (const host of ["omp", "pi"] as const) {
    const binary = host === "omp" ? (process.env.COORDINATOR_TEST_OMP ?? "/home/admin/.bun/bin/omp") : join(root, "node_modules/.bin/pi");
    const hostHome = privateDirectory(join(directory, host)), agent = privateDirectory(join(hostHome, "agent"));
    const env = isolatedEnvironment(hostHome, agent);
    const version = (await run(binary, ["--version"], syntheticRepo, env)).trim();
    assert.match(version, host === "omp" ? /\b18\.6\.1\b/ : /\b1\.0\.4\b/, `${host}: unexpected installed native version`);
    record(`${host}.version`, { binary, version });
    const tokenFile = join(hostHome, "session.token"); writeFileSync(tokenFile, sessionToken, { mode: 0o600 });
    const stateDirectory = privateDirectory(join(hostHome, "journal")), configPath = join(hostHome, "plugin.json");
    const config: PluginConfig = { host, brokerUrl, tokenFile, stateDirectory, machineId: `synthetic-${host}`, selectedRemote: "origin" }; privateJson(configPath, config);
    interface InstallPlan { dryRun: boolean; service?: string; launcher: string; operations: { binary: string; args: string[] }[] }
    const plan = JSON.parse(await run(process.execPath, ["--import", join(root, "node_modules/tsx/dist/loader.mjs"), join(root, "scripts/install.ts"), "--host", host, "--runtime", process.execPath, "--host-binary", binary, "--plugin-config", configPath, "--plugin-only", "--dry-run"], root, env)) as InstallPlan;
    assert.equal(plan.dryRun, true); assert.equal(plan.service, undefined); assert.ok(plan.launcher.startsWith(`${hostHome}/`));
    record(`${host}.installer-dry-run`, plan);
    const syntheticId = randomUUID(), entryId = "synthetic-diagnostic-only";
    const sessionFile = join(hostHome, "synthetic-owned-session.jsonl");
    writeFileSync(sessionFile, [
      { type: "session", version: 3, id: syntheticId, timestamp: new Date().toISOString(), cwd: syntheticRepo },
      { type: "custom", id: entryId, parentId: null, timestamp: new Date().toISOString(), customType: "coordinator-smoke-synthetic-only", data: { diagnostic: "SYNTHETIC_CONTENT_MUST_NOT_UPLOAD" } },
    ].map(item => JSON.stringify(item)).join("\n") + "\n", { mode: 0o600 });
    const launch = (explicit: boolean) => new NativeRpc(start(binary, ["--mode", "rpc", "--session", sessionFile, "--session-dir", privateDirectory(join(hostHome, "sessions")), ...(explicit ? ["-e", join(root, "src/plugin.ts")] : [])], syntheticRepo, { ...env, COORDINATOR_CONFIG: configPath }), host);
    let native = launch(true);
    const state = (await native.request("get_state")).data as NativeState;
    assert.equal(state.sessionId, syntheticId); assert.equal(resolve(state.sessionFile!), sessionFile); assert.equal(state.isStreaming, false);
    let identity = await native.command("identity", value => (value as Identity)?.sessionId === syntheticId) as Identity;
    assert.equal(identity.machineId, `synthetic-${host}`); assert.equal(identity.repo, repo); assert.equal(identity.ref, "refs/heads/main"); assert.equal(identity.sha, sha); assert.match(identity.incarnation, /^[a-f0-9-]{36}$/);
    assert.equal(identity.workerId, state.sessionId); assert.equal(identity.workerIdentitySource, "native-session");
    record(`${host}.explicit-extension-session-start`, { state: { sessionId: state.sessionId, sessionFile: state.sessionFile, isStreaming: state.isStreaming, messageCount: state.messageCount }, identity });
    const messageId = randomUUID(), payload = { note: "synthetic durable native inbox" };
    await rpc(brokerUrl, coordinatorToken, "peer.send", { messageId, sessionId: identity.sessionId, incarnation: identity.incarnation, payload, ttlMs: 60000 });
    const inbox = await until(async () => await native.command("inbox", value => typeof value === "object" && value !== null) as Record<string, InboxEntry>, value => value[messageId]?.digest === fingerprint(payload), `${host}: durable inbox receipt`);
    assert.deepEqual(inbox[messageId]!.payload, payload);
    const persisted = JSON.parse(readFileSync(join(stateDirectory, `${digest(syntheticId)}.json`), "utf8")) as { inbox: Record<string, InboxEntry> };
    assert.deepEqual(persisted.inbox[messageId], inbox[messageId]);
    await until(() => rpc<{ messages: unknown[] }>(brokerUrl, sessionToken, "session.poll", { sessionId: identity.sessionId, incarnation: identity.incarnation }), value => value.messages.length === 0, `${host}: broker peer acknowledgement`);
    record(`${host}.native-durable-inbox-and-ack`, { messageId, entry: inbox[messageId] });
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0, "Native commands must not start model work");
    await stop(native.child);
    // Execute only native registration, never systemctl/launchctl or user's real HOME.
    await run(binary, plan.operations[0]!.args, root, env);
    record(`${host}.native-registration`, { isolatedAgent: agent, package: root, serviceStarted: false });
    native = launch(false); // Registration must discover the real plugin, without -e.
    assert.equal(((await native.request("get_state")).data as NativeState).sessionId, syntheticId);
    const previous = identity;
    identity = await native.command("identity", value => (value as Identity)?.sessionId === syntheticId) as Identity;
    assert.notEqual(identity.incarnation, previous.incarnation);
    assert.deepEqual((await native.command("inbox", value => (value as Record<string, InboxEntry>)?.[messageId]?.digest === fingerprint(payload)) as Record<string, InboxEntry>)[messageId], inbox[messageId]);
    record(`${host}.installed-extension-restart`, { identity, persistedInboxMessageId: messageId });
    assert.equal(identity.workerId, syntheticId); assert.equal(identity.workerIdentitySource, "native-session");
    // Native SIGTERM need not emit session_shutdown: preserve the broker's prior-incarnation lease fence.
    // This send is accepted only after the installed extension itself has heartbeated AND polled.
    const enrollmentProbe = { messageId: randomUUID(), sessionId: identity.sessionId, incarnation: identity.incarnation, payload: { note: "fresh installed native enrollment" }, ttlMs: 60000 };
    await until(async () => {
      try { await rpc(brokerUrl, coordinatorToken, "peer.send", enrollmentProbe); return true; }
      catch (error) { if (error instanceof Error && /RPC 409/.test(error.message)) return false; throw error; }
    }, value => value, `${host}: actual restarted native heartbeat/poll after prior lease`);
    await until(async () => await native.command("inbox", value => typeof value === "object" && value !== null) as Record<string, InboxEntry>, inbox => inbox[enrollmentProbe.messageId]?.digest === fingerprint(enrollmentProbe.payload), `${host}: fresh installed inbox receipt`);
    record(`${host}.installed-native-fresh-enrollment`, { sessionId: identity.sessionId, incarnation: identity.incarnation, workerId: identity.workerId, priorIncarnationNoLongerAuthoritative: true });
    // Actual session_start performs heartbeat then poll; task.submit must find that fresh native enrollment.
    // No runner is started: a correctly targeted peer must own the offer before any runner dispatch.
    const work = (workerId = identity.workerId): Work => ({
      taskId: randomUUID(), repo, ref: identity.ref, sha, launcher: host,
      prompt: "Synthetic bounded work; must not enter model context without coordinator confirmation.",
      scope: "Synthetic native ownership/readback only", restrictions: "No model turn or external operation",
      expectedEvidence: ["synthetic addressed ready readback"], deadline: Date.now() + 60000,
      workerId, githubRepoId: 1, requiredLanes: ["synthetic-native-readback"], bypassPeer: false, approved: true,
    });
    const poll = () => rpc<{ offers: Task[]; releases: Task[] }>(brokerUrl, sessionToken, "session.poll", { sessionId: identity.sessionId, incarnation: identity.incarnation });
    const offered = async (request: Work): Promise<Task> => {
      const from = native.frames.length;
      const task = await rpc<Task>(brokerUrl, coordinatorToken, "task.submit", request);
      assert.equal(task.offerDecision, "pending"); assert.equal(task.state, "QUEUED");
      assert.equal(task.peerSession, identity.sessionId); assert.equal(task.peerIncarnation, identity.incarnation); assert.equal(task.runnerId, undefined);
      await native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "notify" && frame.message?.startsWith(`Coordinator offer ${task.taskId}:`) === true);
      return task;
    };
    const wrong = await rpc<Task>(brokerUrl, coordinatorToken, "task.submit", work(`wrong-${randomUUID()}`));
    assert.equal(wrong.offerDecision, "expired"); assert.equal(wrong.peerSession, undefined);
    record(`${host}.wrong-target-not-offered`, { taskId: wrong.taskId, targetedWorker: wrong.request.workerId, enrolledWorker: identity.workerId, offerDecision: wrong.offerDecision });
    const declinedOffer = await offered(work());
    assert.ok(!(await poll()).offers.some(task => task.taskId === wrong.taskId));
    const declined = await native.command(`decline ${JSON.stringify({ taskId: declinedOffer.taskId })}`, value => (value as Task)?.offerDecision === "declined") as Task;
    assert.equal(declined.runnerId, undefined); assert.equal(declined.state, "QUEUED");
    assert.ok(!(await poll()).offers.some(task => task.taskId === declined.taskId));
    record(`${host}.native-fresh-offer-decline`, { taskId: declined.taskId, workerId: identity.workerId, peerSession: declined.peerSession, offerDecision: declined.offerDecision, runnerDispatched: false });
    const acceptedOffer = await offered(work());
    const accepted = await native.command(`accept ${JSON.stringify({ taskId: acceptedOffer.taskId })}`, value => (value as Task)?.offerDecision === "accepted") as Task;
    assert.equal(accepted.state, "ACCEPTED"); assert.equal(accepted.runnerId, undefined);
    assert.deepEqual((await poll()).releases, []);
    const address: Address = {
      messageId: randomUUID(), revision: 1, workerId: identity.workerId, sessionId: identity.sessionId,
      incarnation: identity.incarnation, repo, ref: identity.ref, sha, scope: accepted.request.scope,
      restrictions: accepted.request.restrictions, expectedEvidence: accepted.request.expectedEvidence, deadline: accepted.request.deadline,
    };
    await rpc(brokerUrl, coordinatorToken, "task.address", { taskId: accepted.taskId, address });
    for (const change of [{ workerId: `wrong-${randomUUID()}` }, { repo: "github.com/synthetic/wrong" }, { ref: "refs/heads/wrong" }, { sha: "0".repeat(40) }, { deadline: Date.now() - 1 }]) {
      await native.rejectedCommand(`readback ${JSON.stringify({ taskId: accepted.taskId, address: { ...address, ...change } })}`);
    }
    const ready = await native.command(`readback ${JSON.stringify({ taskId: accepted.taskId, address })}`, value => (value as Task)?.disposition === "ready") as Task;
    assert.deepEqual(ready.readback, address); assert.equal(ready.state, "ACCEPTED");
    assert.equal(ready.confirmedRevision, undefined); assert.equal(ready.runnerId, undefined);
    await delay(1200); // Let the actual extension tick observe readiness without confirmation.
    assert.deepEqual((await poll()).releases, []);
    const nativeAfterReady = (await native.request("get_state")).data as NativeState;
    assert.equal(nativeAfterReady.isStreaming, false); assert.equal(nativeAfterReady.messageCount, state.messageCount);
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0);
    record(`${host}.native-accepted-addressed-ready`, { taskId: ready.taskId, identity, address, readback: ready.readback, disposition: ready.disposition, coordinatorConfirmed: false, substantivePromptAdmitted: false, runnerDispatched: false, rejectedMismatches: ["worker", "repo", "ref", "sha", "deadline"] });
    async function archive(): Promise<ArchiveRow[]> {
      const rows: ArchiveRow[] = []; let cursor = 0;
      for (;;) { const page = await rpc<ArchiveRow[]>(brokerUrl, coordinatorToken, "archive.list", { sessionId: syntheticId, cursor }); rows.push(...page); if (page.length < 2) return rows; cursor = page.at(-1)!.sequence; }
    }
    assert.deepEqual(await archive(), []);
    // Grant solely OUR synthetic owner's metadata consent via actual host confirmation.
    const from = native.frames.length;
    const consent = native.request("prompt", { message: `/coordinator consent ${JSON.stringify({ participants: [owner] })}` });
    void consent.catch(() => {}); // A missing confirmation must still clean up, not create an unhandled rejection.
    const confirmation = await native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "confirm" && frame.title === "Your session archive consent");
    assert.ok(confirmation.message?.includes(owner)); assert.ok(confirmation.message?.includes("content is omitted"));
    native.send({ type: "extension_ui_response", id: confirmation.id, confirmed: true }); await consent;
    await native.frame(from, frame => frame.type === "extension_ui_request" && frame.method === "notify" && frame.message === '{"enabled":true}');
    const archived = await until(archive, rows => rows.some(row => row.entryId === entryId), `${host}: synthetic metadata upload`);
    const diagnostic = archived.find(row => row.entryId === entryId)!;
    assert.deepEqual(diagnostic.payload, { id: entryId, parentId: null, type: "custom", contentOmitted: true });
    assert.equal(diagnostic.digest, fingerprint(diagnostic.payload)); assert.ok(diagnostic.gaps.includes("content-omitted"));
    assert.ok(!JSON.stringify(archived).includes("SYNTHETIC_CONTENT_MUST_NOT_UPLOAD"));
    record(`${host}.native-confirmed-synthetic-consent`, { confirmation: { method: confirmation.method, title: confirmation.title, message: confirmation.message }, diagnostic });
    await native.command("optout", value => value !== null && typeof value === "object" && "enabled" in value && value.enabled === false);
    assert.deepEqual(await archive(), []);
    await assert.rejects(rpc(brokerUrl, sessionToken, "archive.append", { sessionId: syntheticId, entryId, digest: diagnostic.digest, payload: diagnostic.payload, gaps: diagnostic.gaps }), /RPC 403/);
    const erased = JSON.parse(readFileSync(join(stateDirectory, `${digest(syntheticId)}.json`), "utf8")) as { consent: boolean; spool: unknown[]; seen: Record<string, string> };
    assert.equal(erased.consent, false); assert.deepEqual(erased.spool, []); assert.deepEqual(erased.seen, {});
    record(`${host}.native-optout`, { serverArchiveRows: 0, appendAfterRevoke: 403, localSpoolErased: true });
    const changed = await native.request("new_session");
    assert.ok(changed.data !== null && typeof changed.data === "object" && "cancelled" in changed.data && changed.data.cancelled === false);
    const replacement = (await native.request("get_state")).data as NativeState; assert.notEqual(replacement.sessionId, syntheticId);
    const next = await native.command("identity", value => (value as Identity)?.sessionId === replacement.sessionId) as Identity;
    assert.notEqual(next.incarnation, identity.incarnation);
    await assert.rejects(rpc(brokerUrl, coordinatorToken, "peer.send", { messageId: randomUUID(), sessionId: syntheticId, incarnation: identity.incarnation, payload, ttlMs: 60000 }), /RPC 409/);
    assert.equal(replacement.isStreaming, false);
    record(`${host}.native-session-replacement-lifecycle`, { oldSessionId: syntheticId, replacement: next, oldAddressRejected: 409 });
    assert.equal(native.frames.filter(frame => frame.type === "agent_start").length, 0, "Installed native commands must not start model work");
    await stop(native.child);
  }
  record("completed", { hosts: ["omp18.6.1", "pi1.0.4"], modelCallsRequested: 0, macRuntimeProved: false });
} catch (error) {
  record("failed", { reason: error instanceof Error ? error.message.replaceAll(coordinatorToken, "[redacted]").replaceAll(sessionToken, "[redacted]") : "Unknown native smoke failure" });
  process.exitCode = 1;
} finally {
  const cleanup = await Promise.allSettled([...children].map(stop));
  if (cleanup.some(result => result.status === "rejected")) { record("cleanup-failed", { ownedProcessTerminationFailed: true }); process.exitCode = 1; }
  console.log(JSON.stringify({ success: process.exitCode !== 1, transcript: transcriptPath, privateSyntheticDirectory: directory, macRuntimeProved: false }));
}
