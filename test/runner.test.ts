import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { setImmediate as immediate } from "node:timers/promises";
import { LfFrames, Runner } from "../src/runner.ts";
import type { RunnerConfig, Work } from "../src/protocol.ts";

test("host JSONL uses complete strict LF frames and bounded unfinished output", () => {
  const frames = new LfFrames();
  assert.deepEqual(frames.push(Buffer.from('{"type":"response","data":"')), []);
  assert.deepEqual(frames.push(Buffer.from('✓"}\n{"type":"agent_end"}\n')), [{ type: "response", data: "✓" }, { type: "agent_end" }]);
  frames.end();
  assert.throws(() => new LfFrames().push(Buffer.from([123, 34, 120, 34, 58, 34, 255, 34, 125, 10])));
  assert.throws(() => new LfFrames().push(Buffer.alloc(4 * 1024 * 1024 + 1, 120)));
  const unfinished = new LfFrames(); unfinished.push(Buffer.from('{}')); assert.throws(() => unfinished.end());
});

test("SQLite acceptance is unique, complete, conflict-safe and reserves recovered uncertain capacity", async () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "coordinator-runner-")));
  const repo = join(root, "repo"); mkdirSync(repo, { mode: 0o700 });
  const git = (...args: string[]) => execFileSync("git", args, { cwd: repo, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  git("init"); git("config", "user.name", "Runner regression"); git("config", "user.email", "runner@example.invalid");
  git("remote", "add", "origin", "https://github.com/test/runner"); writeFileSync(join(repo, "tracked"), "acceptance invariant\n"); git("add", "tracked"); git("commit", "-m", "initial");
  const tokenFile = join(root, "token"); writeFileSync(tokenFile, "regression-only-credential-000000000000", { mode: 0o600 });
  const config: RunnerConfig = { id: "runner-test", brokerUrl: "http://127.0.0.1:1", tokenFile, host: "127.0.0.1", port: 0,
    stateDirectory: join(root, "state"), slots: 1, repositories: { "github.com/test/runner": repo }, launchers: { omp: process.env.COORDINATOR_TEST_OMP ?? "/home/admin/.bun/bin/omp" } };
  const work: Work = { taskId: "one", repo: "github.com/test/runner", ref: "HEAD", sha: git("rev-parse", "HEAD"), launcher: "omp", prompt: "approved prompt", scope: "scope", restrictions: "no deployment", expectedEvidence: ["checks"], deadline: 12345,
    workerId: "native-worker", githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
  let runner: Runner | undefined;
  try {
    runner = new Runner(config);
    const first = await runner.handle("acceptTask", work);
    assert.deepEqual(await runner.handle("acceptTask", work), first);
    await assert.rejects(runner.handle("acceptTask", { ...work, prompt: "changed" }), /fingerprint conflict/);
    assert.deepEqual(await runner.handle("acceptTask", { ...work, taskId: "two" }), { taskId: "two", accepted: false, state: "full", reason: "full" });
    const stored = runner.database.prepare("SELECT work,worktree,state FROM tasks WHERE taskId='one'").get()!;
    assert.deepEqual(JSON.parse(String(stored.work)), work); assert.equal(stored.state, "ACCEPTED"); assert.ok(String(stored.worktree).startsWith(config.stateDirectory));
    assert.equal(runner.database.prepare("SELECT count(*) AS count FROM tasks").get()!.count, 1);
    await runner.close(); runner = new Runner(config);
    assert.deepEqual(await runner.handle("journal", { taskId: "one" }), first);
    runner.database.prepare("UPDATE tasks SET state='PROMPT_INTENT',promptId='uncertain' WHERE taskId='one'").run();
    await runner.close(); runner = new Runner(config);
    const recovered = await runner.handle("journal", { taskId: "one" });
    assert.ok(typeof recovered === "object" && recovered !== null && "state" in recovered);
    assert.equal(recovered.state, "HELD");
    assert.equal(runner.database.prepare("SELECT occupied FROM tasks WHERE taskId='one'").get()!.occupied, 1);
    assert.deepEqual(await runner.handle("acceptTask", { ...work, taskId: "two" }), { taskId: "two", accepted: false, state: "full", reason: "full" });
    assert.throws(() => new Runner(config), /already owned/);
    assert.deepEqual(await runner.handle("journal", { taskId: "missing" }), { taskId: "missing", accepted: false, state: "not-accepted" });
    await runner.close(); runner = new Runner({ ...config, stateDirectory: join(root, "expired-state") });
    await runner.handle("acceptTask", work); await runner.start(); await immediate();
    const expired = await runner.handle("journal", { taskId: "one" });
    assert.ok(typeof expired === "object" && expired !== null && "state" in expired); assert.equal(expired.state, "FAILED");
    assert.equal(runner.database.prepare("SELECT pid FROM tasks WHERE taskId='one'").get()!.pid, null);
    const next = await runner.handle("acceptTask", { ...work, taskId: "after-expiry", deadline: Date.now() + 60000 });
    assert.ok(typeof next === "object" && next !== null && "accepted" in next); assert.equal(next.accepted, true);
  } finally { if (runner) await runner.close(); rmSync(root, { recursive: true, force: true }); }
});
