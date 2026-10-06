import test from "node:test";
import assert from "node:assert/strict";
import { Clock, Context, Deferred, Effect, Layer, ManagedRuntime } from "effect";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { Reactivity } from "effect/reactivity";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { LfFrames, Runner } from "../src/runner.ts";
import { Broker } from "../src/broker.ts";
import { Mcp, canonicalJson, decode, decodeJson, encodeJson, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Address, BrokerRequest, CommandError, HttpError, Json, JsonValue, NativeFrame, RunnerEvent, RunnerResult, RunnerRequest, RunnerRow, Task, type Principal, type RunnerConfig, type Work } from "../src/contracts.ts";

const enrollment = "regression-only-enrollment-000000000000";
const control = "regression-only-control-000000000000000";
const common = Layer.merge(Platform.layer, Mcp.layer);
const request = (method: string, params: unknown) => decode(RunnerRequest, { method, params });
const bytes = (text: string) => new TextEncoder().encode(text);
const boundaryStatus = <A, E>(effect: Effect.Effect<A, E>) => effect.pipe(Effect.match({ onSuccess: () => 200, onFailure: (error) => error instanceof HttpError ? error.status : 500 }));

async function fixture(slots = 1) {
  const services = ManagedRuntime.make(common);
  const platform = await services.runPromise(Platform);
  const rpc = await services.runPromise(Mcp);
  const client = await services.runPromise(HttpClient.HttpClient);
  const fs = platform.fs, join = platform.path.join;
  const run = <A, E>(effect: Effect.Effect<A, E>) => services.runPromise(effect);
  const root = await run(fs.realPath(await run(fs.makeTempDirectory({ prefix: "coordinator-runner-" }))));
  await run(fs.chmod(root, 0o711));
  const repo = join(root, "repo"); await run(fs.makeDirectory(repo, { mode: 0o700 }));
  const git = (...args: string[]) => run(platform.exec(ChildProcess.make("git", args, { cwd: repo, env: { PATH: "/usr/local/bin:/usr/bin:/bin" }, extendEnv: false }))).then((output) => output.trim());
  await git("init"); await git("config", "user.name", "Runner regression"); await git("config", "user.email", "runner@example.invalid");
  await git("remote", "add", "origin", "https://github.com/test/runner");
  await run(fs.writeFileString(join(repo, "tracked"), "acceptance invariant\n")); await git("add", "tracked"); await git("commit", "-m", "initial");
  const credentials = join(root, "credentials"); await run(fs.makeDirectory(credentials, { mode: 0o700 }));
  const tokenFile = join(credentials, "token"); await run(fs.writeFileString(tokenFile, enrollment, { mode: 0o600 }));
  const principal: Principal = { id: "runner-test", owner: "owner", roles: ["runner"], repos: ["github.com/test/runner"], tokenHash: await run(platform.digest(enrollment)), controlTokenHash: await run(platform.digest(control)) };
  const brokerRuntime = ManagedRuntime.make(Broker.layer({ database: join(root, "broker", "broker.sqlite"), backupDirectory: join(root, "backups"), host: "127.0.0.1", port: 0,
    archiveRetentionMs: 60000, backupRetentionMs: 60000, repositoryIds: { "github.com/test/runner": 1 }, principals: [principal] }).pipe(Layer.provide(common)));
  const broker = await brokerRuntime.runPromise(Broker);
  let unavailable = false, taskReads = 0;
  let gate: { reached: Deferred.Deferred<void>; proceed: Deferred.Deferred<void> } | undefined;
  class Proxy extends Context.Service<Proxy>()("runner-test/Proxy", {
    make: Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer;
      yield* rpc.serve(server, { target: "broker", authorize: broker.authorize, handle: (p, input) => Effect.gen(function*() {
        if (unavailable) return yield* new HttpError({ status: 503, message: "Transient broker outage" });
        if (input.method === "task.get" && gate && ++taskReads === 2) {
          yield* Deferred.succeed(gate.reached, undefined);
          yield* Deferred.await(gate.proceed);
        }
        return yield* broker.handle(p, decode(BrokerRequest, input));
      }) });
      return { server };
    })
  }) {}
  const proxyRuntime = ManagedRuntime.make(Layer.effect(Proxy, Proxy.make).pipe(Layer.provide(serverLayer("127.0.0.1", 0))));
  const proxy = await proxyRuntime.runPromise(Proxy);
  const brokerUrl = HttpServer.formatAddress(proxy.server.address);
  const modelDirectory = join(root, "model"); await run(fs.makeDirectory(modelDirectory, { mode: 0o700 }));
  const launcher = join(root, "host.mjs");
  const workspace = platform.path.dirname(platform.path.dirname(await run(platform.path.fromFileUrl(new URL(import.meta.url)))));
  const moduleUrl = async (relative: string) => encodeJson(JsonValue, (await run(platform.path.toFileUrl(join(workspace, relative)))).href);
  // Node exposes no Effect executable-path API; use the observed binary, never assume /usr/bin/node.
  await run(fs.writeFileString(launcher, `#!${process.execPath}
import * as Effect from ${await moduleUrl("node_modules/effect/dist/Effect.js")};
import * as Layer from ${await moduleUrl("node_modules/effect/dist/Layer.js")};
import * as Stream from ${await moduleUrl("node_modules/effect/dist/Stream.js")};
import * as Stdio from ${await moduleUrl("node_modules/effect/dist/Stdio.js")};
import * as ChildProcess from ${await moduleUrl("node_modules/effect/dist/process/ChildProcess.js")};
import * as NodeStdio from ${await moduleUrl("node_modules/@effect/platform-node/dist/NodeStdio.js")};
import * as NodeRuntime from ${await moduleUrl("node_modules/@effect/platform-node/dist/NodeRuntime.js")};
import { LfFrames } from ${await moduleUrl("src/runner.ts")};
import { Platform } from ${await moduleUrl("src/platform.ts")};
import { CommandError, NativeFrame } from ${await moduleUrl("src/contracts.ts")};
import { decode, encodeJson } from ${await moduleUrl("src/http.ts")};
NodeRuntime.runMain(Effect.gen(function*() {
  const stdio = yield* Stdio.Stdio;
  const platform = yield* Platform;
  const args = yield* stdio.args;
  if (args.includes("--version")) {
    yield* Stream.succeed("fixture-host\\n").pipe(Stream.run(stdio.stdout({ endOnDone: false })));
    return;
  }
  const sessions = args[args.indexOf("--session-dir") + 1];
  if (!sessions) return yield* Effect.fail(new CommandError({ message: "Session directory required" }));
  const frames = new LfFrames();
  yield* stdio.stdin.pipe(Stream.runForEach((chunk) => Effect.gen(function*() {
    const values = yield* Effect.try(() => frames.push(chunk));
    for (const value of values) {
      const input = decode(NativeFrame, value);
      const data = input.type === "get_state"
        ? { sessionId: "real-session", sessionFile: sessions + "/real.jsonl", isStreaming: false, isCompacting: false, isSettled: true, messageCount: 0, pendingMessageCount: 0 }
        : input.type === "bash"
          ? { exitCode: 0, output: yield* platform.exec(ChildProcess.make("/usr/bin/pwd", ["-P"], { env: { PATH: "/usr/local/bin:/usr/bin:/bin" }, extendEnv: false })) }
          : {};
      yield* Stream.succeed(encodeJson(NativeFrame, { type: "response", id: input.id, command: input.type, success: true, data }) + "\\n").pipe(Stream.run(stdio.stdout({ endOnDone: false })));
    }
  })), Effect.andThen(Effect.try(() => frames.end())));
}).pipe(Effect.provide(Layer.merge(Platform.layer, NodeStdio.layer))));
`, { mode: 0o755 }));
  await run(fs.chmod(launcher, 0o755));
  const config: RunnerConfig = { id: principal.id, brokerUrl, tokenFile, controlTokenHash: principal.controlTokenHash!, host: "127.0.0.1", port: 0,
    stateDirectory: join(root, "state"), slots, repositories: { "github.com/test/runner": repo }, launchers: { omp: launcher }, modelDirectory, childDirectory: join(root, "children"), childUidBase: 1000000 };
  const work: Work = { taskId: "one", repo: "github.com/test/runner", ref: "HEAD", sha: await git("rev-parse", "HEAD"), launcher: "omp", prompt: "approved prompt", scope: "scope", restrictions: "no deployment", expectedEvidence: ["checks"], deadline: await run(Clock.currentTimeMillis) + 60000,
    workerId: "native-worker", githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
  async function save(work: Work, overrides: Partial<Task> = {}) {
    const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: await run(platform.fingerprint(work)), request: work, state: "ACCEPTED", runnerId: principal.id, createdAt: 1, updatedAt: 1, ...overrides };
    await run(broker.sql.unsafe("INSERT OR REPLACE INTO tasks(id,data) VALUES (?,?)", [task.taskId, encodeJson(Task, task)])); return task;
  }
  await save(work);
  const runtimes = new Set<ManagedRuntime.ManagedRuntime<Runner, unknown>>();
  async function open(input = config, servicesLayer = common) {
    const runtime = ManagedRuntime.make(Runner.layer(input).pipe(Layer.provide(servicesLayer))); runtimes.add(runtime);
    const service = await runtime.runPromise(Runner);
    return { service, async close() { await runtime.dispose(); runtimes.delete(runtime); }, call(method: string, params: unknown, bearer = control) { return runtime.runPromise(service.handle(request(method, params), bearer)); }, status(method: string, params: unknown, bearer = control) { return runtime.runPromise(boundaryStatus(service.handle(request(method, params), bearer))); } };
  }
  const ready = (runner: Runner["Service"], taskId = work.taskId) => run(Effect.gen(function*() {
    for (;;) { const row = yield* runner.row(taskId); if (row?.state === "READY") return row; if (row?.state === "FAILED" || row?.state === "HELD") return yield* Effect.fail(new CommandError({ message: `Native fixture did not become READY: ${row.reason}` })); yield* Effect.sleep("10 millis"); }
  }).pipe(Effect.timeout("15 seconds")));
  const seed = (directory: string, action: (sql: SqliteClient.SqliteClient) => Effect.Effect<void, unknown>) => run(Effect.gen(function*() {
    yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
    const sql = yield* SqliteClient.make({ filename: join(directory, "runner.sqlite") });
    yield* action(sql);
  }).pipe(Effect.provide(Reactivity.layer), Effect.scoped));
  return { root, broker, config, work, save, open, ready, seed, run, fs, platform, rpc, client, join,
    outage(value: boolean) { unavailable = value; },
    async fence() { gate = { reached: await run(Deferred.make<void>()), proceed: await run(Deferred.make<void>()) }; taskReads = 0; return gate; },
    async close() { if (gate) await run(Deferred.succeed(gate.proceed, undefined)); for (const runtime of runtimes) await runtime.dispose(); await proxyRuntime.dispose(); await brokerRuntime.dispose(); await run(fs.remove(root, { recursive: true, force: true })); await services.dispose(); }
  };
}

test("host JSONL accepts LF and CRLF, rejects invalid UTF-8 and bounded unfinished output", () => {
  const frames = new LfFrames(); assert.deepEqual(frames.push(bytes('{"type":"response","data":"')), []);
  assert.deepEqual(frames.push(bytes('✓"}\r\n{"type":"agent_end"}\n')), [{ type: "response", data: "✓" }, { type: "agent_end" }]); frames.end();
  assert.throws(() => new LfFrames().push(new Uint8Array([123, 34, 120, 34, 58, 34, 255, 34, 125, 10])));
  assert.throws(() => new LfFrames().push(new Uint8Array(4 * 1024 * 1024 + 1).fill(120)));
  const unfinished = new LfFrames(); unfinished.push(bytes("{}")); assert.throws(() => unfinished.end());
});

test("kernel ownership loss interrupts blocked startup before recovery can overwrite a replacement", async () => {
  const f = await fixture();
  const reached = await f.run(Deferred.make<void>());
  const proceed = await f.run(Deferred.make<void>());
  const lost = await f.run(Deferred.make<void>());
  let ownership: ChildProcessSpawner.ChildProcessHandle | undefined;
  const guardedPlatform = {
    ...f.platform,
    acquireJournalOwnership: (file: string) => f.platform.acquireJournalOwnership(file).pipe(Effect.tap((guard) => Effect.sync(() => { ownership = guard; }))),
    fs: {
      ...f.fs,
      chmod: (path: string, mode: number) => path === f.join(f.config.stateDirectory, "runner.sqlite")
        ? Effect.gen(function*() {
          yield* Deferred.succeed(reached, undefined);
          yield* Deferred.await(proceed);
          return yield* f.fs.chmod(path, mode);
        }).pipe(Effect.onInterrupt(() => Deferred.succeed(lost, undefined).pipe(Effect.asVoid)))
        : f.fs.chmod(path, mode),
    },
  };
  const starting = f.open(f.config, Layer.merge(Layer.succeed(Platform, guardedPlatform), Mcp.layer));
  // Attach rejection handling before killing the actual flock helper.
  const outcome = starting.then(() => undefined, (error: unknown) => error);
  try {
    await f.run(Deferred.await(reached));
    assert.ok(ownership);
    await f.run(ownership.kill());
    await f.run(ownership.exitCode.pipe(Effect.ignore));
    await f.run(Deferred.await(lost).pipe(Effect.timeout("5 seconds")));
    const replacement = await f.open();
    await replacement.call("acceptTask", f.work);
    const row = await f.ready(replacement.service);
    await f.run(Deferred.succeed(proceed, undefined));
    const error = await outcome;
    assert.ok(error instanceof CommandError);
    assert.deepEqual(await f.run(replacement.service.row("one")), row);
    assert.equal((await f.run(replacement.service.row("one")))!.state, "READY");
  } finally {
    await f.run(Deferred.succeed(proceed, undefined));
    await f.close();
    await outcome;
  }
});

test("real broker-bound SQLite acceptance is unique, conflict-safe and reserves uncertain capacity", async () => {
  const f = await fixture();
  try {
    await f.seed(f.config.stateDirectory, (sql) => Effect.gen(function*() {
      yield* sql.unsafe("CREATE TABLE owner (id INTEGER PRIMARY KEY, pid INTEGER)");
      yield* sql.unsafe("INSERT INTO owner(id,pid) VALUES (1,?)", [process.pid]);
    }));
    let runner = await f.open();
    assert.equal((await f.run(runner.service.sql.unsafe<Json>("SELECT count(*) AS count FROM sqlite_master WHERE type='table' AND name='owner'")))[0]!.count, 0);
    await runner.call("acceptTask", f.work); await f.ready(runner.service);
    const first = await runner.call("journal", { taskId: "one" }); assert.deepEqual(await runner.call("acceptTask", f.work), first);
    await assert.rejects(runner.call("acceptTask", { ...f.work, prompt: "changed" }));
    const two = { ...f.work, taskId: "two" }; await f.save(two);
    assert.partialDeepStrictEqual(await runner.call("acceptTask", two), { accepted: false, state: "full" });
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='PROMPT_INTENT',promptId='uncertain',pid=? WHERE taskId='one'", [process.pid]));
    await runner.close(); runner = await f.open();
    assert.equal((await f.run(runner.service.row("one")))!.state, "HELD"); assert.equal((await f.run(runner.service.row("one")))!.occupied, 1);
    assert.partialDeepStrictEqual(await runner.call("acceptTask", two), { accepted: false, state: "full" });
    await assert.rejects(f.open());
    assert.partialDeepStrictEqual(await runner.call("journal", { taskId: "missing" }), { accepted: false, state: "not-accepted" });
    await runner.close();
    const expired = { ...f.work, taskId: "expired", deadline: 12345 }; await f.save(expired);
    runner = await f.open({ ...f.config, stateDirectory: f.join(f.root, "expired-state") });
    await assert.rejects(runner.call("acceptTask", expired));
    assert.equal((await f.run(runner.service.sql.unsafe<Json>("SELECT count(*) AS count FROM tasks")))[0]!.count, 0);
    await f.run(runner.service.sql.unsafe("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (?,?,?,?,'ACCEPTED',1,?)", [expired.taskId, await f.run(f.platform.fingerprint(expired)), encodeJson(Task.fields.request, expired), f.join(f.config.childDirectory, "expired", "repo"), "expired-incarnation"]));
    await runner.close(); runner = await f.open({ ...f.config, stateDirectory: f.join(f.root, "expired-state") });
    await f.run(Effect.gen(function*() {
      for (;;) { const row = yield* runner.service.row(expired.taskId); if (row?.state === "FAILED") return; yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("10 seconds")));
    assert.equal((await f.run(runner.service.row(expired.taskId)))!.pid, null);
  } finally { await f.close(); }
});

test("enrollment bearer is health-only and control bearer cannot invent broker work or confirmation", async () => {
  const f = await fixture();
  try {
    const runner = await f.open();
    await assert.rejects(f.open({ ...f.config, controlTokenHash: await f.run(f.platform.digest(enrollment)) }));
    const url = HttpServer.formatAddress(runner.service.server.address);
    assert.equal(await f.run(f.client.execute(HttpClientRequest.post(`${url}/mcp`).pipe(HttpClientRequest.bodyText(encodeJson(JsonValue, { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "journal", arguments: { taskId: "one" } } }), "application/json"), HttpClientRequest.setHeader("accept", "application/json, text/event-stream"))).pipe(Effect.map((response) => response.status), Effect.scoped)), 401);
    assert.equal(await f.run(boundaryStatus(f.rpc.call(url, "not-an-enrolled-bearer", "journal", { taskId: "one" }))), 401);
    const address: Address = { messageId: "one", revision: 1, workerId: f.work.workerId, sessionId: "session", incarnation: "incarnation", repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: f.work.deadline };
    for (const input of [request("acceptTask", f.work), request("prepare", { taskId: "one", address }), request("release", { taskId: "one", address, prompt: f.work.prompt }), request("inspect", { taskId: "one" }), request("command", { taskId: "one", type: "get_state" }), request("journal", { taskId: "one" })]) {
      assert.equal(await f.run(boundaryStatus(f.rpc.call(url, enrollment, input.method, input.params))), 401);
      assert.equal(await runner.status(input.method, input.params, enrollment), 401);
    }
    assert.equal(await runner.status("acceptTask", { ...f.work, taskId: "invented" }), 404);
    await f.save(f.work, { runnerId: "other-runner" }); assert.equal(await runner.status("acceptTask", f.work), 403);
    await f.save(f.work, { fingerprint: "0".repeat(64) }); await assert.rejects(runner.call("acceptTask", f.work));
    await f.save(f.work, { request: { ...f.work, prompt: "unapproved ledger prompt" } }); await assert.rejects(runner.call("acceptTask", f.work));
    await f.save(f.work); await runner.call("acceptTask", f.work); const owned = await f.ready(runner.service); const actual = decodeJson(Address, owned.address!);
    f.outage(true);
    await f.run(runner.service.heartbeat());
    assert.equal((await f.run(runner.service.row("one")))!.state, "READY");
    f.outage(false);
    const release = () => runner.call("release", { taskId: "one", address: actual, prompt: f.work.prompt });
    await f.save(f.work); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: { ...actual, revision: 2 }, confirmedRevision: 1, disposition: "ready" }); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: actual, confirmedRevision: 2, disposition: "ready" }); await assert.rejects(release());
    await f.save(f.work, { runnerId: "other-runner", address: actual, readback: actual, confirmedRevision: 1, disposition: "ready" }); assert.equal(await runner.status("release", { taskId: "one", address: actual, prompt: f.work.prompt }), 403);
    assert.equal((await f.run(runner.service.row("one")))!.promptId, null);
  } finally { await f.close(); }
});

test("native event and result projection omits secret content from durable journal and inspect", async () => {
  const f = await fixture();
  try {
    let runner = await f.open(); await runner.call("acceptTask", f.work); await f.ready(runner.service);
    const secret = "native-secret-provider-token-DO-NOT-STORE";
    const frames = new LfFrames().push(bytes(`${encodeJson(NativeFrame, { type: "message_end", message: { content: [{ type: "thinking", thinking: secret }, { type: "toolCall", arguments: { token: secret } }], stopReason: "stop" } })}\n${encodeJson(NativeFrame, { type: "tool_execution_end", result: { content: secret }, credentials: secret })}\n`));
    for (const frame of frames) await f.run(runner.service.frame(f.work.taskId, undefined, frame));
    await f.run(runner.service.state(f.work.taskId, "SUCCEEDED", null, { hostSettled: true, evidenceVerified: false, message: secret, toolResult: secret, credentials: secret }));
    const events = await f.run(runner.service.sql.unsafe<Json>("SELECT payload FROM events"));
    assert.ok(events.some((event) => decodeJson(RunnerEvent, String(event.payload)).type === "message_end")); assert.equal(canonicalJson(events).includes(secret), false);
    assert.deepEqual(decodeJson(RunnerResult, (await f.run(runner.service.row("one")))!.result!), { hostSettled: true, evidenceVerified: false });
    assert.equal(canonicalJson(await runner.call("inspect", { taskId: "one" })).includes(secret), false);
    await f.run(runner.service.sql.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", [f.work.taskId, encodeJson(Json, { type: "message_end", message: secret })]));
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET result=?,reason=? WHERE taskId='one'", [encodeJson(Json, { hostSettled: true, credentials: secret }), secret]));
    await f.run(runner.service.sql.unsafe("PRAGMA user_version=0"));
    await runner.close(); runner = await f.open();
    assert.equal(canonicalJson(await f.run(runner.service.sql.unsafe("SELECT payload FROM events"))).includes(secret), false);
    assert.equal(canonicalJson(await f.run(runner.service.sql.unsafe("SELECT result,reason FROM tasks"))).includes(secret), false);
    const database = await f.run(f.fs.readFile(f.join(f.config.stateDirectory, "runner.sqlite")));
    assert.equal(new TextDecoder().decode(database).includes(secret), false);
  } finally { await f.close(); }
});

test("obsolete handed-off report reconciles durably without starving later live reports", async () => {
  const f = await fixture(2);
  try {
    let runner = await f.open(); const live = { ...f.work, taskId: "two" }; await f.save(live);
    await runner.call("acceptTask", f.work); await runner.call("acceptTask", live); await f.ready(runner.service); await f.ready(runner.service, "two");
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='RUNNING',promptId='reserved',pid=? WHERE taskId='one'", [process.pid]));
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='SUCCEEDED',occupied=0,result='{}' WHERE taskId='two'"));
    await f.save(f.work, { runnerId: undefined });
    const rows = (await f.run(runner.service.sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks ORDER BY taskId", [f.config.childUidBase]))).map((row) => decode(RunnerRow, row));
    await f.run(runner.service.reports(rows));
    const obsolete = (await f.run(runner.service.sql.unsafe<Json>("SELECT state,detached,occupied,promptId FROM tasks WHERE taskId='one'")))[0]!;
    assert.deepEqual({ ...obsolete }, { state: "HELD", detached: 1, occupied: 1, promptId: "reserved" });
    assert.equal(decodeJson(Task, String((await f.run(f.broker.sql.unsafe<Json>("SELECT data FROM tasks WHERE id='two'")))[0]!.data)).state, "SUCCEEDED");
    await runner.close(); runner = await f.open(); assert.equal((await f.run(runner.service.row("one")))!.detached, 1); assert.equal((await f.run(runner.service.row("one")))!.occupied, 1);
    await f.save(live, { state: "FAILED" }); await f.run(runner.service.reports([(await f.run(runner.service.row("two")))!])); assert.equal((await f.run(runner.service.row("two")))!.detached, 0);
    f.outage(true); await f.run(runner.service.reports([(await f.run(runner.service.row("two")))!])); assert.equal((await f.run(runner.service.row("two")))!.detached, 0);
  } finally { await f.close(); }
});

test("ownership lost before durable admission refuses acceptance and pending duplicates never expose not-accepted", async () => {
  const f = await fixture();
  try {
    const runner = await f.open(), gate = await f.fence();
    const acceptance = runner.call("acceptTask", f.work), duplicate = runner.call("acceptTask", f.work);
    // Observe the actual second broker read, not monkey-patched runner internals.
    await f.run(Deferred.await(gate.reached).pipe(Effect.timeout("10 seconds")));
    await assert.rejects(runner.call("journal", { taskId: "one" }));
    await assert.rejects(runner.call("acceptTask", { ...f.work, prompt: "conflict" }));
    await f.save(f.work, { runnerId: "reassigned-runner", state: "ASSIGNING" });
    const refused = Promise.all([
      assert.rejects(acceptance, (error: unknown) => error instanceof HttpError && error.status === 403),
      assert.rejects(duplicate, (error: unknown) => error instanceof HttpError && error.status === 403),
    ]);
    await f.run(Deferred.succeed(gate.proceed, undefined)); await refused;
    const refusedJournal = await runner.call("journal", { taskId: "one" });
    if (!refusedJournal || typeof refusedJournal !== "object" || !("accepted" in refusedJournal) || !("state" in refusedJournal)) throw new Error("Expected journal result");
    assert.equal(refusedJournal.accepted, false); assert.equal(refusedJournal.state, "not-accepted");
    assert.equal((await f.run(runner.service.sql.unsafe<Json>("SELECT count(*) AS count FROM tasks")))[0]!.count, 0);
  } finally { await f.close(); }
});

test("runner refuses overlapping child roots and model seeds containing session history", async () => {
  const f = await fixture();
  try {
    await assert.rejects(f.open({ ...f.config, childDirectory: f.root }));
    await f.run(f.fs.writeFileString(f.join(f.config.modelDirectory, "history.db"), "sensitive history", { mode: 0o600 }));
    await assert.rejects(f.open());
  } finally { await f.close(); }
});
