import test from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, mkdirSync, writeFileSync, rmSync, realpathSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { setImmediate as immediate } from "node:timers/promises";
import { LfFrames, Runner } from "../src/runner.ts";
import { Broker } from "../src/broker.ts";
import { HttpError, serveRpc } from "../src/http.ts";
import { digest, fingerprint, rpc, type Address, type Principal, type RunnerConfig, type Task, type Work } from "../src/protocol.ts";

const enrollment = "regression-only-enrollment-000000000000";
const control = "regression-only-control-000000000000000";
async function fixture(slots = 1) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "coordinator-runner-")));
  chmodSync(root, 0o711);
  const repo = join(root, "repo"); mkdirSync(repo, { mode: 0o700 });
  const git = (...args: string[]) => execFileSync("git", args, { cwd: repo, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  git("init"); git("config", "user.name", "Runner regression"); git("config", "user.email", "runner@example.invalid");
  git("remote", "add", "origin", "https://github.com/test/runner"); writeFileSync(join(repo, "tracked"), "acceptance invariant\n"); git("add", "tracked"); git("commit", "-m", "initial");
  const credentials = join(root, "credentials"); mkdirSync(credentials, { mode: 0o700 });
  const tokenFile = join(credentials, "token"); writeFileSync(tokenFile, enrollment, { mode: 0o600 });
  const principal: Principal = { id: "runner-test", owner: "owner", roles: ["runner"], repos: ["github.com/test/runner"], tokenHash: digest(enrollment), controlTokenHash: digest(control) };
  const broker = new Broker({ database: join(root, "broker", "broker.sqlite"), backupDirectory: join(root, "backups"), host: "127.0.0.1", port: 0,
    archiveRetentionMs: 60000, backupRetentionMs: 60000, repositoryIds: { "github.com/test/runner": 1 }, principals: [principal] });
  let unavailable = false;
  const server = await serveRpc({ host: "127.0.0.1", port: 0, authorize: (token) => broker.authorize(token), handle: (p, method, params) => {
    if (unavailable) throw new HttpError(503, "Transient broker outage");
    return broker.handle(p as Principal, method, params);
  } });
  const bound = server.address(); assert.ok(bound && typeof bound !== "string");
  const modelDirectory = join(root, "model"); mkdirSync(modelDirectory, { mode: 0o700 });
  const config: RunnerConfig = { id: principal.id, brokerUrl: `http://127.0.0.1:${bound.port}`, tokenFile, controlTokenHash: digest(control), host: "127.0.0.1", port: 0,
    stateDirectory: join(root, "state"), slots, repositories: { "github.com/test/runner": repo }, launchers: { omp: "/usr/bin/true" }, modelDirectory, childDirectory: join(root, "children"), childUidBase: 1000000 };
  const work: Work = { taskId: "one", repo: "github.com/test/runner", ref: "HEAD", sha: git("rev-parse", "HEAD"), launcher: "omp", prompt: "approved prompt", scope: "scope", restrictions: "no deployment", expectedEvidence: ["checks"], deadline: Date.now() + 60000,
    workerId: "native-worker", githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
  function save(work: Work, overrides: Partial<Task> = {}) {
    const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: fingerprint(work), request: work, state: "ACCEPTED", runnerId: principal.id, createdAt: 1, updatedAt: 1, ...overrides };
    broker.db.prepare("INSERT OR REPLACE INTO tasks(id,data) VALUES (?,?)").run(task.taskId, JSON.stringify(task)); return task;
  }
  save(work);
  return { root, broker, config, work, save, outage(value: boolean) { unavailable = value; }, async close() { server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve())); broker.db.close(); rmSync(root, { recursive: true, force: true }); } };
}

test("host JSONL accepts LF and CRLF, rejects invalid UTF-8 and bounded unfinished output", () => {
  const frames = new LfFrames();
  assert.deepEqual(frames.push(Buffer.from('{"type":"response","data":"')), []);
  assert.deepEqual(frames.push(Buffer.from('✓"}\r\n{"type":"agent_end"}\n')), [{ type: "response", data: "✓" }, { type: "agent_end" }]);
  frames.end();
  assert.throws(() => new LfFrames().push(Buffer.from([123, 34, 120, 34, 58, 34, 255, 34, 125, 10])));
  assert.throws(() => new LfFrames().push(Buffer.alloc(4 * 1024 * 1024 + 1, 120)));
  const unfinished = new LfFrames(); unfinished.push(Buffer.from('{}')); assert.throws(() => unfinished.end());
});

test("real broker-bound SQLite acceptance is unique, conflict-safe and reserves uncertain capacity", async () => {
  const f = await fixture(); let runner: Runner | undefined;
  try {
    mkdirSync(f.config.stateDirectory, { mode: 0o700 });
    const legacy = new DatabaseSync(join(f.config.stateDirectory, "runner.sqlite"));
    legacy.exec("CREATE TABLE owner (id INTEGER PRIMARY KEY, pid INTEGER)");
    legacy.prepare("INSERT INTO owner(id,pid) VALUES (1,?)").run(process.pid);
    legacy.close();
    runner = new Runner(f.config);
    const call = (method: string, params: unknown) => runner!.handle(method, params, control);
    const first = await call("acceptTask", f.work);
    assert.deepEqual(await call("acceptTask", f.work), first);
    await assert.rejects(call("acceptTask", { ...f.work, prompt: "changed" }), /fingerprint conflict/);
    const two = { ...f.work, taskId: "two" }; f.save(two);
    assert.deepEqual(await call("acceptTask", two), { taskId: "two", accepted: false, state: "full", reason: "full" });
    await runner.close(); runner = new Runner(f.config);
    assert.deepEqual(await call("journal", { taskId: "one" }), first);
    runner.database.prepare("UPDATE tasks SET state='PROMPT_INTENT',promptId='uncertain' WHERE taskId='one'").run();
    await runner.close(); runner = new Runner(f.config);
    const recovered = await call("journal", { taskId: "one" });
    assert.ok(typeof recovered === "object" && recovered !== null && "state" in recovered); assert.equal(recovered.state, "HELD");
    assert.equal(runner.database.prepare("SELECT occupied FROM tasks WHERE taskId='one'").get()!.occupied, 1);
    assert.deepEqual(await call("acceptTask", two), { taskId: "two", accepted: false, state: "full", reason: "full" });
    assert.throws(() => new Runner(f.config), /already owned/);
    assert.deepEqual(await call("journal", { taskId: "missing" }), { taskId: "missing", accepted: false, state: "not-accepted" });
    await runner.close(); runner = new Runner({ ...f.config, stateDirectory: join(f.root, "expired-state") });
    await call("acceptTask", f.work);
    runner.database.prepare("UPDATE tasks SET work=? WHERE taskId='one'").run(JSON.stringify({ ...f.work, deadline: 12345 }));
    await runner.start(); await immediate();
    const expired = await call("journal", { taskId: "one" });
    assert.ok(typeof expired === "object" && expired !== null && "state" in expired); assert.equal(expired.state, "FAILED");
    assert.equal(runner.database.prepare("SELECT pid FROM tasks WHERE taskId='one'").get()!.pid, null);
  } finally { if (runner) await runner.close(); await f.close(); }
});

test("enrollment bearer is health-only and control bearer cannot invent broker work or confirmation", async () => {
  const f = await fixture(); let runner = new Runner(f.config);
  try {
    assert.throws(() => new Runner({ ...f.config, controlTokenHash: digest(enrollment) }), /Distinct/);
    const server = await runner.start(); const bound = server.address(); assert.ok(bound && typeof bound !== "string");
    const url = `http://127.0.0.1:${bound.port}`;
    assert.equal((await fetch(`${url}/healthz`, { headers: { authorization: `Bearer ${enrollment}` } })).status, 200);
    assert.equal((await fetch(`${url}/rpc`, { method: "POST", body: JSON.stringify({ method: "journal", params: { taskId: "one" } }) })).status, 401);
    await assert.rejects(rpc(url, "not-an-enrolled-bearer", "journal", { taskId: "one" }), /RPC 401/);
    for (const method of ["acceptTask", "prepare", "release", "inspect", "command", "journal"]) {
      await assert.rejects(rpc(url, enrollment, method, {}), /RPC 401/);
      await assert.rejects(runner.handle(method, {}, enrollment), /Control credential/);
    }
    await runner.close(); runner = new Runner(f.config);
    await assert.rejects(runner.handle("acceptTask", { ...f.work, taskId: "invented" }, control), /RPC 404/);
    f.save(f.work, { runnerId: "other-runner" });
    await assert.rejects(runner.handle("acceptTask", f.work, control), /RPC 403/);
    f.save(f.work, { fingerprint: "0".repeat(64) });
    await assert.rejects(runner.handle("acceptTask", f.work, control), /binding mismatch/);
    f.save(f.work, { request: { ...f.work, prompt: "unapproved ledger prompt" } });
    await assert.rejects(runner.handle("acceptTask", f.work, control), /binding mismatch/);
    f.save(f.work); await runner.handle("acceptTask", f.work, control);
    // No native child is needed to prove the pre-prompt ledger gate: denial must precede child RPC.
    const address: Address = { messageId: "one", revision: 1, workerId: f.work.workerId, sessionId: "session", incarnation: "incarnation", repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: Date.now() + 60000 };
    runner.database.prepare("UPDATE tasks SET state='READY',address=? WHERE taskId='one'").run(JSON.stringify(address));
    const release = () => runner.handle("release", { taskId: "one", address, prompt: f.work.prompt }, control);
    await assert.rejects(release(), /Broker confirmation/);
    f.save(f.work, { address, readback: { ...address, revision: 2 }, confirmedRevision: 1, disposition: "ready" });
    await assert.rejects(release(), /Broker confirmation/);
    f.save(f.work, { address, readback: address, confirmedRevision: 2, disposition: "ready" });
    await assert.rejects(release(), /Broker confirmation/);
    f.save(f.work, { runnerId: "other-runner", address, readback: address, confirmedRevision: 1, disposition: "ready" });
    await assert.rejects(release(), /Invalid request or unavailable/);
    assert.equal(runner.database.prepare("SELECT promptId FROM tasks WHERE taskId='one'").get()!.promptId, null);
  } finally { await runner.close(); await f.close(); }
});

test("native event and result projection omits secret content from durable journal and inspect", async () => {
  const f = await fixture(); let runner = new Runner(f.config);
  try {
    await runner.handle("acceptTask", f.work, control);
    const secret = "native-secret-provider-token-DO-NOT-STORE";
    const frames = new LfFrames().push(Buffer.from(`${JSON.stringify({ type: "message_end", message: { content: [{ type: "thinking", thinking: secret }, { type: "toolCall", arguments: { token: secret } }], stopReason: "stop" } })}\n${JSON.stringify({ type: "tool_execution_end", result: { content: secret }, credentials: secret })}\n`));
    // Exercise production ingestion/projection, not a response echo or source-text assertion.
    for (const frame of frames) Reflect.apply(Reflect.get(runner, "frame"), runner, [f.work.taskId, undefined, frame]);
    Reflect.apply(Reflect.get(runner, "state"), runner, [f.work.taskId, "SUCCEEDED", null, { hostSettled: true, evidenceVerified: false, message: secret, toolResult: secret, credentials: secret }]);
    const events = runner.database.prepare("SELECT payload FROM events").all();
    assert.ok(events.some((event) => JSON.parse(String(event.payload)).type === "message_end"));
    assert.equal(JSON.stringify(events).includes(secret), false);
    assert.deepEqual(JSON.parse(String(runner.database.prepare("SELECT result FROM tasks WHERE taskId='one'").get()!.result)), { hostSettled: true, evidenceVerified: false });
    assert.equal(JSON.stringify(await runner.handle("inspect", { taskId: "one" }, control)).includes(secret), false);
    // A legacy raw journal must also be erased, not merely hidden from inspect.
    runner.database.prepare("INSERT INTO events(taskId,payload) VALUES (?,?)").run(f.work.taskId, JSON.stringify({ type: "message_end", message: secret }));
    runner.database.prepare("UPDATE tasks SET result=?,reason=? WHERE taskId='one'").run(JSON.stringify({ hostSettled: true, credentials: secret }), secret);
    runner.database.exec("PRAGMA user_version=0");
    await runner.close(); runner = new Runner(f.config);
    assert.equal(JSON.stringify(runner.database.prepare("SELECT payload FROM events").all()).includes(secret), false);
    assert.equal(JSON.stringify(runner.database.prepare("SELECT result,reason FROM tasks").all()).includes(secret), false);
    assert.equal(readFileSync(join(f.config.stateDirectory, "runner.sqlite")).includes(Buffer.from(secret)), false);
  } finally { await runner.close(); await f.close(); }
});

test("obsolete handed-off report reconciles durably without starving later live reports", async () => {
  const f = await fixture(2); let runner = new Runner(f.config);
  try {
    const live = { ...f.work, taskId: "two" }; f.save(live);
    await runner.handle("acceptTask", f.work, control); await runner.handle("acceptTask", live, control);
    runner.database.prepare("UPDATE tasks SET state='RUNNING',promptId='reserved',pid=? WHERE taskId='one'").run(process.pid);
    runner.database.prepare("UPDATE tasks SET state='SUCCEEDED',occupied=0,result='{}' WHERE taskId='two'").run();
    f.save(f.work, { runnerId: undefined });
    const rows = runner.database.prepare("SELECT * FROM tasks ORDER BY taskId").all();
    await Reflect.apply(Reflect.get(runner, "reports"), runner, [rows]);
    const obsolete = runner.database.prepare("SELECT state,detached,occupied,promptId FROM tasks WHERE taskId='one'").get()!;
    assert.deepEqual({ ...obsolete }, { state: "HELD", detached: 1, occupied: 1, promptId: "reserved" });
    const delivered = JSON.parse(String(f.broker.db.prepare("SELECT data FROM tasks WHERE id='two'").get()!.data)); assert.equal(delivered.state, "SUCCEEDED");
    await runner.close(); runner = new Runner(f.config);
    assert.equal(runner.database.prepare("SELECT detached,occupied FROM tasks WHERE taskId='one'").get()!.detached, 1);
    assert.equal(runner.database.prepare("SELECT occupied FROM tasks WHERE taskId='one'").get()!.occupied, 1);
    // An authenticated report conflict is retryable, never evidence of obsolete ownership.
    f.save(live, { state: "FAILED" });
    await Reflect.apply(Reflect.get(runner, "reports"), runner, [runner.database.prepare("SELECT * FROM tasks WHERE taskId='two'").all()]);
    assert.equal(runner.database.prepare("SELECT detached FROM tasks WHERE taskId='two'").get()!.detached, 0);
    f.outage(true);
    await Reflect.apply(Reflect.get(runner, "reports"), runner, [runner.database.prepare("SELECT * FROM tasks WHERE taskId='two'").all()]);
    assert.equal(runner.database.prepare("SELECT detached FROM tasks WHERE taskId='two'").get()!.detached, 0);
  } finally { await runner.close(); await f.close(); }
});

test("ownership lost before durable admission refuses acceptance and pending duplicates never expose not-accepted", async () => {
  const f = await fixture(); const runner = new Runner(f.config);
  const reached = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  try {
    const original = Reflect.get(runner, "brokerTask");
    let reads = 0;
    Reflect.set(runner, "brokerTask", async (work: Work) => {
      if (++reads === 2) { reached.resolve(); await proceed.promise; }
      return Reflect.apply(original, runner, [work]);
    });
    const acceptance = runner.handle("acceptTask", f.work, control);
    const duplicate = runner.handle("acceptTask", f.work, control);
    await Promise.race([reached.promise, acceptance.then(() => { throw new Error("Acceptance committed without final ownership fence"); })]);
    await assert.rejects(runner.handle("journal", { taskId: "one" }, control), /Acceptance pending/);
    await assert.rejects(runner.handle("acceptTask", { ...f.work, prompt: "conflict" }, control), /fingerprint conflict/);
    // Broker timeout/reassignment after checkout, before final authoritative read.
    f.save(f.work, { runnerId: "reassigned-runner", state: "ASSIGNING" });
    const refused = Promise.all([assert.rejects(acceptance, /RPC 403/), assert.rejects(duplicate, /RPC 403/)]);
    proceed.resolve();
    await refused;
    assert.deepEqual(await runner.handle("journal", { taskId: "one" }, control), { taskId: "one", accepted: false, state: "not-accepted" });
    assert.equal(runner.database.prepare("SELECT count(*) AS count FROM tasks").get()!.count, 0);
  } finally { proceed.resolve(); await runner.close(); await f.close(); }
});

test("runner refuses overlapping child roots and model seeds containing session history", async () => {
  const f = await fixture();
  try {
    assert.throws(() => new Runner({ ...f.config, childDirectory: f.root }), /Child root must be separate/);
    writeFileSync(join(f.config.modelDirectory, "history.db"), "sensitive history", { mode: 0o600 });
    assert.throws(() => new Runner(f.config), /Model seed must contain only scoped/);
  } finally { await f.close(); }
});
