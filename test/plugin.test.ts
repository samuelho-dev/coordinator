import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Schema } from "effect";
import type { Server } from "node:http";
import { serveRpc } from "../src/http.ts";
import coordinator from "../src/plugin.js";

test("actual extension records intent before native send and holds uncertain delivery across incarnation restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "coordinator-plugin-"));
  const previousConfig = process.env.COORDINATOR_CONFIG;
  let server: Server | undefined;
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
    server = await serveRpc({ host: "127.0.0.1", port: 0, authorize: token => { assert.equal(token, "test-token"); return {}; }, handle: async (_principal, method, params) => {
      if (method === "session.heartbeat") incarnation = Schema.decodeUnknownSync(Schema.Struct({ incarnation: Schema.String }))(params).incarnation;
      if (method === "task.report") reports.push(params);
      const address = { messageId: "message", revision: 1, sessionId: "native-session", incarnation, workerId: "native-session", repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: Date.now() + 60000 };
      return method === "session.poll" ? { messages: [], offers: [], releases: [{ taskId: "task", address, readback: address, confirmedRevision: 1, disposition: "ready", request: { prompt: "Substantive task", repo: address.repo, ref: address.ref, sha } }] } : { ok: true };
    } });
    const listening = server.address(); assert.ok(listening && typeof listening !== "string");
    writeFileSync(config, JSON.stringify({ brokerUrl: `http://127.0.0.1:${listening.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
    process.env.COORDINATOR_CONFIG = config;
    coordinator({ on: (event, handler) => { hooks.set(event, handler); }, registerCommand: () => {},
      sendUserMessage: () => { sends++; throw new Error("native delivery uncertain"); } });
    await assert.rejects(hooks.get("session_start")!({}, ctx));
    assert.equal(sends, 1);
    await hooks.get("session_shutdown")!({}, ctx);
    await hooks.get("session_start")!({}, ctx);
    assert.equal(sends, 1);
    assert.equal(reports.length, 1);
    assert.equal(Schema.decodeUnknownSync(Schema.Struct({ state: Schema.String }))(reports[0]).state, "HELD");
  } finally {
    await hooks.get("session_shutdown")?.({}, ctx);
    server?.closeAllConnections();
    if (server) { const done = Promise.withResolvers<void>(); server.close(() => done.resolve()); await done.promise; }
    if (previousConfig === undefined) delete process.env.COORDINATOR_CONFIG;
    else process.env.COORDINATOR_CONFIG = previousConfig;
    rmSync(directory, { recursive: true, force: true });
  }
});
