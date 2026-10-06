import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import coordinator from "../src/plugin.js";

test("actual extension records intent before native send and holds uncertain delivery across incarnation restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "coordinator-plugin-"));
  const previousConfig = process.env.COORDINATOR_CONFIG;
  const previousFetch = globalThis.fetch;
  const hooks = new Map<string, (event: unknown, context: typeof ctx) => Promise<void>>();
  const ctx = { cwd: directory, hasUI: true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "native-session", getSessionFile: () => undefined },
    ui: { notify: () => {}, confirm: async () => true } };
  let sends = 0;
  let incarnation = "";
  const reports: unknown[] = [];
  try {
    execFileSync("git", ["init", "-b", "main"], { cwd: directory, stdio: "pipe" });
    execFileSync("git", ["-c", "user.name=Coordinator Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "test"], { cwd: directory, stdio: "pipe" });
    execFileSync("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory });
    const sha = execFileSync("git", ["rev-parse", "HEAD"], { cwd: directory, encoding: "utf8" }).trim();
    const stateDirectory = join(directory, "state"); mkdirSync(stateDirectory, { mode: 0o700 });
    const tokenFile = join(directory, "token"); writeFileSync(tokenFile, "test-token", { mode: 0o600 });
    const config = join(directory, "config.json");
    writeFileSync(config, JSON.stringify({ brokerUrl: "http://127.0.0.1:1", tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
    process.env.COORDINATOR_CONFIG = config;
    globalThis.fetch = async (_input, init) => {
      const envelope = JSON.parse(String(init?.body)) as { method: string; params: { incarnation?: string } };
      if (envelope.method === "session.heartbeat") incarnation = String(envelope.params.incarnation);
      if (envelope.method === "task.report") reports.push(envelope.params);
      const address = { messageId: "message", revision: 1, sessionId: "native-session", incarnation, workerId: "native-worker", repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: Date.now() + 60000 };
      return new Response(JSON.stringify(envelope.method === "session.poll" ? { messages: [], offers: [], releases: [{ taskId: "task", address, readback: address, confirmedRevision: 1, disposition: "ready", request: { prompt: "Substantive task", repo: address.repo, ref: address.ref, sha } }] } : { ok: true }));
    };
    coordinator({ on: (event, handler) => { hooks.set(event, handler); }, registerCommand: () => {},
      sendUserMessage: () => { sends++; throw new Error("native delivery uncertain"); } });
    await assert.rejects(hooks.get("session_start")!({}, ctx));
    assert.equal(sends, 1);
    await hooks.get("session_shutdown")!({}, ctx);
    await hooks.get("session_start")!({}, ctx);
    assert.equal(sends, 1);
    assert.equal(reports.length, 1);
    assert.match(JSON.stringify(reports[0]), /HELD/);
  } finally {
    await hooks.get("session_shutdown")?.({}, ctx);
    globalThis.fetch = previousFetch;
    if (previousConfig === undefined) delete process.env.COORDINATOR_CONFIG;
    else process.env.COORDINATOR_CONFIG = previousConfig;
    rmSync(directory, { recursive: true, force: true });
  }
});
