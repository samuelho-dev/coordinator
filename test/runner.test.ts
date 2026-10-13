import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { Clock, Context, Deferred, Effect, Exit, Fiber, Layer, ManagedRuntime, Schema } from "effect";
import { ChildProcess, type ChildProcessSpawner } from "effect/process";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { Reactivity } from "effect/reactivity";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { Runner } from "../src/runner.ts";
import { Broker } from "../src/broker.ts";
import { makeDatabase } from "../src/kysely.ts";
import { Mcp, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Address, BrokerRequest, CommandError, HttpError, Json, JsonValue, RunnerEvent, RunnerResult, RunnerNativeState, RunnerRequest, RunnerRow, Task, TaskId, RunnerId, Repo, Hash, GitHubRepositoryId, Principal, RunnerConfig, Work } from "../src/contracts.ts";
import { NativeBinding, NativeEventSequence, NativeGeneration, NativeIncarnationId, NativeSessionId, NativeMessagesPage } from "@samuelho/coordinator-native-control/contracts";
import { NativeLifecycleJournal } from "@samuelho/coordinator-native-control/lifecycle";
import { provisionPostgres, dropPostgres } from "./postgres-fixture.ts";

const enrollment = "regression-only-enrollment-000000000000";
const control = "regression-only-control-000000000000000";
const common = Layer.merge(Platform.layer, Mcp.layer);
const request = (method: string, params: unknown) => Schema.decodeUnknownSync(RunnerRequest, { onExcessProperty: "error" })({ method, params });
const boundaryStatus = <A, E>(effect: Effect.Effect<A, E>) => effect.pipe(Effect.match({ onSuccess: () => 200, onFailure: (error) => error instanceof HttpError ? error.status : 500 }));

async function fixture(slots = 1, hostMode = "complete", phase: (name: string) => void = () => {}) {
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
  const principal = Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({ id: "runner-test", owner: "owner", roles: ["runner"], repos: ["github.com/test/runner"], tokenHash: await run(platform.digest(enrollment)), controlTokenHash: await run(platform.digest(control)) });
  const databaseUrlFile = await services.runPromise(provisionPostgres(credentials));
  const brokerRuntime = ManagedRuntime.make(Broker.layer({ databaseUrlFile, backupDirectory: join(root, "backups"), host: "127.0.0.1", port: 0,
    archiveRetentionMs: 60000, backupRetentionMs: 60000, repositoryIds: { [Repo.make("github.com/test/runner")]: GitHubRepositoryId.make(1) }, principals: [principal] }).pipe(Layer.provide(common)));
  const broker = await brokerRuntime.runPromise(Broker);
  const { client: db, execute } = makeDatabase(broker.sql);
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
        return yield* broker.handle(p, Schema.decodeUnknownSync(BrokerRequest, { onExcessProperty: "error" })(input));
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
  const moduleUrl = async (relative: string) => Schema.encodeUnknownSync(Schema.fromJsonString(Schema.String))((await run(platform.path.toFileUrl(join(workspace, relative)))).href);
  // Node exposes no Effect executable-path API; use the observed binary, never assume /usr/bin/node.
  await run(fs.writeFileString(launcher, `#!${process.execPath}
import * as Effect from ${await moduleUrl("node_modules/effect/dist/Effect.js")};
import * as Layer from ${await moduleUrl("node_modules/effect/dist/Layer.js")};
import * as Schema from ${await moduleUrl("node_modules/effect/dist/Schema.js")};
import * as NodeServices from ${await moduleUrl("node_modules/@effect/platform-node/dist/NodeServices.js")};
import * as NodeHttpServer from ${await moduleUrl("node_modules/@effect/platform-node/dist/NodeHttpServer.js")};
import * as HttpRouter from ${await moduleUrl("node_modules/effect/dist/http/HttpRouter.js")};
import * as HttpServerRequest from ${await moduleUrl("node_modules/effect/dist/http/HttpServerRequest.js")};
import * as HttpServerResponse from ${await moduleUrl("node_modules/effect/dist/http/HttpServerResponse.js")};
import * as NodeRuntime from ${await moduleUrl("node_modules/@effect/platform-node/dist/NodeRuntime.js")};
import { NativeBinding, NativeControlError, NativePluginConfig, NativeTools, NativePromptResult, nativeEventsUri } from ${JSON.stringify(import.meta.resolve("@samuelho/coordinator-native-control/contracts"))};
import { NativeLifecycleJournal } from ${JSON.stringify(import.meta.resolve("@samuelho/coordinator-native-control/lifecycle"))};
import { nativeControlHttpLayer, nativeTool } from ${JSON.stringify(import.meta.resolve("@samuelho/coordinator-native-control/server"))};
import { createNativePresentation } from ${JSON.stringify(import.meta.resolve("@samuelho/coordinator-native-control/interaction"))};
import { realpathSync, writeFileSync, readFileSync, renameSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
if (process.argv.includes("--version")) {
  process.stdout.write("fixture-host\\n");
} else {
  const sessions = realpathSync(process.argv[process.argv.indexOf("--session-dir") + 1]);
  const sessionFile = sessions + "/real.jsonl";
  writeFileSync(sessionFile, "", { mode: 0o600 });
  const status = readFileSync("/proc/self/status", "utf8");
  // Node getgroups includes the effective GID; kernel Groups records supplementary groups only.
  if (process.getuid() < 1000000 || process.getuid() !== process.geteuid() ||
      ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"].some(key => !new RegExp("^" + key + ":\\\\s+0+$", "m").test(status)) || !/^NoNewPrivs:\\s+1$/m.test(status) || !/^Groups:[\\t ]*$/m.test(status) ||
      (statSync(process.env.HOME).mode & 0o777) !== 0o700 || statSync(process.env.HOME).uid !== process.getuid() ||
      Object.keys(process.env).some(key => !["HOME", "PATH", "LANG", "COORDINATOR_NATIVE_CONFIG", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "OMP_AUTH_BROKER_URL", "OMP_AUTH_BROKER_TOKEN"].includes(key))) throw new Error("Child privilege boundary failed");
  if (${JSON.stringify(hostMode)} === "acquisition-discovery") writeFileSync(process.env.HOME + "/acquisition.pid", JSON.stringify(process.pid), { mode: 0o600 });
  const binding = Schema.decodeUnknownSync(NativeBinding)({ host: "omp", sessionId: randomUUID(), incarnation: randomUUID(), generation: 0 });
  const journal = new NativeLifecycleJournal({ retention: ${hostMode === "gap" ? 1 : 1024} });
  if (${JSON.stringify(hostMode)} === "invalid-event") {
    const read = journal.read.bind(journal);
    journal.read = (...args) => {
      const page = read(...args);
      return { ...page, entries: page.entries.map(event => event.kind === "run_started" ? { ...event, seq: -1 } : event) };
    };
  }
  const secret = "native-secret-provider-token-DO-NOT-STORE";
  const messages = [{ role: "assistant", content: [{ type: "thinking", thinking: secret }, { type: "toolCall", arguments: { token: secret } }] }, { role: "user", content: "λ ✓", fixtureIdentity: { uid: process.getuid(), effectiveUid: process.geteuid(), cwd: realpathSync(process.cwd()), capabilityEffective: status.match(/^CapEff:\\s+(.*)$/m)[1], noNewPrivileges: status.match(/^NoNewPrivs:\\s+(.*)$/m)[1], environmentKeys: Object.keys(process.env) } }];
  const privateState = { systemPrompt: [secret], dumpTools: [{ name: "fixture-tool", description: secret, parameters: { type: "object", properties: { token: { type: "string", description: secret } } }, examples: [{ call: { token: secret } }] }], todoPhases: [{ name: "fixture-phase", tasks: [{ content: secret, status: "pending" }] }], queuedMessages: { steering: [], followUp: [] }, autoCompactionEnabled: false, interruptMode: "immediate", fastModeEnabled: false, fastModeActive: false, slowModeSupported: false, slowModeEnabled: false, tokensPerSecond: null, goal: null };
  let currentRunId = null;
  const emit = (kind, fields = {}) => journal.append({ type: "native_lifecycle", binding, runId: currentRunId, at: Date.now(), kind, ...fields });
  const presentation = createNativePresentation(() => ({
    nativeBinding: binding,
    nativeRunId: currentRunId,
    emitNativeInput: (kind, inputKind, inputId, runId, owner = binding) => journal.append({ type: "native_lifecycle", binding: owner, runId, at: Date.now(), kind, inputKind, inputId }),
  }));
  const validate = input => {
    if (input.binding && JSON.stringify(input.binding) !== JSON.stringify(binding)) throw new NativeControlError({ code: "binding_mismatch", message: "Stale fixture binding" });
  };
  const handlers = {
    "native.get_state": input => Effect.sync(() => {
      validate(input);
      if (${JSON.stringify(hostMode)} === "stale" && input.binding) return { binding: { ...binding, generation: binding.generation + 1 }, ...privateState, cwd: realpathSync(process.cwd()), sessionFile: realpathSync(sessionFile), model: null, currentRunId, isStreaming: false, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: messages.length, steeringMode: "all", followUpMode: "all" };
      return { binding, ...privateState, cwd: realpathSync(process.cwd()), sessionFile: realpathSync(sessionFile), model: null, currentRunId, isStreaming: currentRunId !== null, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: messages.length, steeringMode: "all", followUpMode: "all" };
    }),
    "native.bash": input => Effect.try({
      try: () => { validate(input); return { binding, exitCode: 0, output: execFileSync("/bin/sh", ["-c", input.command], { cwd: process.cwd(), env: { PATH: "/usr/local/bin:/usr/bin:/bin", HOME: process.env.HOME }, encoding: "utf8" }), truncated: false, killed: false }; },
      catch: error => error instanceof NativeControlError ? error : new NativeControlError({ code: "native_failure", message: "Actual fixture shell failed" }),
    }),
    "native.get_messages": input => Effect.sync(() => {
      validate(input);
      const start = input.cursor === undefined ? 0 : Number(input.cursor);
      if (!Number.isSafeInteger(start) || start < 0 || start > messages.length) throw new NativeControlError({ code: "invalid_input", message: "Invalid message cursor" });
      const end = Math.min(messages.length, start + (input.limit ?? 256));
      return { binding, messages: messages.slice(start, end), nextCursor: end < messages.length ? String(end) : null, more: end < messages.length };
    }),
    "native.abort": input => Effect.sync(() => { validate(input); return { binding, runId: currentRunId, disposition: currentRunId === null ? "already_settled" : "requested" }; }),
    "native.prompt": input => Effect.sync(() => {
      validate(input);
      if (${JSON.stringify(hostMode)} === "transport") process.exit(23);
      currentRunId = randomUUID();
      const seq = ${JSON.stringify(hostMode)} === "missing-admission" ? journal.highWater : emit("submission_admitted", { submissionId: input.submissionId, disposition: "started" }).seq;
      const admission = { binding, submissionId: input.submissionId, runId: currentRunId, disposition: "started", eventsUri: nativeEventsUri(binding), seq };
      emit("run_started");
      if (${JSON.stringify(hostMode)} !== "missing-terminal") emit("run_settled", { outcome: "completed" });
      const result = Schema.decodeUnknownSync(NativePromptResult)({ admission, disposition: "settled", final: { binding, runId: currentRunId, outcome: "completed" } });
      currentRunId = null;
      writeFileSync(sessionFile, JSON.stringify(messages) + "\\n", { mode: 0o600 });
      return result;
    }),
  };
  const tools = Object.entries(NativeTools).filter(([name, descriptor]) => descriptor.hosts.includes("omp") && (${JSON.stringify(hostMode)} !== "acquisition-discovery" || name !== "native.get_state") && (${JSON.stringify(hostMode)} !== "public-hooks" || name !== "native.prompt")).map(([name]) => nativeTool(name, handlers[name] ?? (() => Effect.fail(new NativeControlError({ code: "unsupported_operation", message: "Fixture operation is not implemented" })))));
  NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
    const config = Schema.decodeUnknownSync(NativePluginConfig)(JSON.parse(readFileSync(process.env.COORDINATOR_NATIVE_CONFIG, "utf8")));
    const token = readFileSync(config.tokenFile, "utf8");
    const app = yield* HttpRouter.toHttpEffect(nativeControlHttpLayer({ host: "omp", binding, journal, tools, presentation, name: "isolated-runner-fixture", version: "1" }));
    const server = yield* NodeHttpServer.make(() => createServer(), { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" });
    yield* server.serve(Effect.gen(function*() {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return request.headers.authorization === "Bearer " + token ? yield* app : HttpServerResponse.empty({ status: 401 });
    }));
    if (server.address._tag !== "InetAddressV4") throw new Error("Expected fixture loopback address");
    writeFileSync(config.endpointFile + ".pending", JSON.stringify({ binding, url: "http://127.0.0.1:" + server.address.port + "/mcp" }), { mode: 0o600 });
    renameSync(config.endpointFile + ".pending", config.endpointFile);
    yield* Effect.never;
  }).pipe(Effect.provide(NodeServices.layer))));
}
`, { mode: 0o755 }));
  await run(fs.chmod(launcher, 0o755));
  const runnerId = RunnerId.make(principal.id);
  const config = Schema.decodeUnknownSync(RunnerConfig, { onExcessProperty: "error" })({ id: runnerId, brokerUrl, tokenFile, controlTokenHash: principal.controlTokenHash!, host: "127.0.0.1", port: 0,
    stateDirectory: join(root, "state"), slots, repositories: { "github.com/test/runner": repo }, launchers: { omp: launcher }, modelDirectory, childDirectory: join(root, "children"), childUidBase: 1000000 });
  const work = Schema.decodeUnknownSync(Work, { onExcessProperty: "error" })({ taskId: "one", repo: "github.com/test/runner", ref: "HEAD", sha: await git("rev-parse", "HEAD"), launcher: "omp", prompt: "approved prompt", scope: "scope", restrictions: "no deployment", expectedEvidence: ["checks"], deadline: await run(Clock.currentTimeMillis) + 60000,
    workerId: "native-worker", githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true });
  async function save(work: Work, overrides: Partial<Task> = {}) {
    const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: await run(platform.fingerprint(work)), request: work, state: "ACCEPTED", runnerId, createdAt: 1, updatedAt: 1, ...overrides };
    const data = Schema.encodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task);
    await run(execute(db.insertInto("tasks").values({ id: task.taskId, data }).onConflict((conflict) => conflict.column("id").doUpdateSet({ data })))); return task;
  }
  await save(work);
  const runtimes = new Set<ManagedRuntime.ManagedRuntime<Runner, unknown>>();
  async function open(input = config, servicesLayer = common) {
    const runtime = ManagedRuntime.make(Runner.layer(input).pipe(Layer.provide(servicesLayer))); runtimes.add(runtime);
    const service = await runtime.runPromise(Runner);
    return { service, async close() { phase("runner.dispose.start"); await runtime.dispose(); runtimes.delete(runtime); phase("runner.dispose.done"); }, call(method: string, params: unknown, bearer = control) { return runtime.runPromise(service.handle(request(method, params), bearer)); }, status(method: string, params: unknown, bearer = control) { return runtime.runPromise(boundaryStatus(service.handle(request(method, params), bearer))); } };
  }
  const ready = (runner: Runner["Service"], taskId = work.taskId) => run(Effect.gen(function*() {
    for (;;) {
      const row = yield* runner.row(taskId);
      if (row?.state === "READY") return row;
      if (row?.state === "FAILED" || row?.state === "HELD") {
        return yield* Effect.fail(new CommandError({ message: `Native fixture did not become READY: ${row.reason}` }));
      }
      yield* Effect.sleep("10 millis");
    }
  }).pipe(Effect.timeout("15 seconds")));
  const seed = (directory: string, action: (sql: SqliteClient.SqliteClient) => Effect.Effect<void, unknown>) => run(Effect.gen(function*() {
    yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
    const sql = yield* SqliteClient.make({ filename: join(directory, "runner.sqlite") });
    yield* action(sql);
  }).pipe(Effect.provide(Reactivity.layer), Effect.scoped));
  return { root, broker, config, work, save, open, ready, seed, run, fs, platform, rpc, client, join,
    outage(value: boolean) { unavailable = value; },
    async fence() { gate = { reached: await run(Deferred.make<void>()), proceed: await run(Deferred.make<void>()) }; taskReads = 0; return gate; },
    async close() {
      phase("fixture.gate.release.start");
      if (gate) await run(Deferred.succeed(gate.proceed, undefined));
      phase("fixture.gate.release.done");
      phase("fixture.runners.dispose.start");
      for (const runtime of runtimes) await runtime.dispose();
      runtimes.clear();
      phase("fixture.runners.dispose.done");
      phase("fixture.proxy.dispose.start");
      await proxyRuntime.dispose();
      phase("fixture.proxy.dispose.done");
      phase("fixture.broker.dispose.start");
      await brokerRuntime.dispose();
      await services.runPromise(dropPostgres(databaseUrlFile));
      phase("fixture.broker.dispose.done");
      phase("fixture.root.remove.start");
      await run(fs.remove(root, { recursive: true, force: true }));
      phase("fixture.root.remove.done");
      phase("fixture.services.dispose.start");
      await services.dispose();
      phase("fixture.services.dispose.done");
    }
  };
}

test("typed lifecycle derivation preserves retained gaps, binding generations and ordered reentrant fanout", async () => {
  const journal = new NativeLifecycleJournal({ retention: 2 });
  const binding: NativeBinding = { host: "omp", sessionId: NativeSessionId.make("session"),
    incarnation: NativeIncarnationId.make("incarnation"), generation: NativeGeneration.make(0) };
  const retired = { ...binding, generation: NativeGeneration.make(1) };
  const delivered: number[] = [];
  const stop = journal.subscribe((event) => {
    delivered.push(event.seq);
    if (event.seq === 1) journal.append({ type: "native_lifecycle", binding, runId: null, at: 2, kind: "session_retired", reason: "retired", replacement: retired });
  });
  const first = journal.append({ type: "native_lifecycle", binding, runId: null, at: 1, kind: "session_started" });
  assert.notEqual(first.binding, binding, "Journal freezing must not mutate the caller's binding");
  assert.equal(Object.isFrozen(binding), false);
  await new Promise<void>((resolve) => queueMicrotask(() => queueMicrotask(resolve)));
  assert.deepEqual(delivered, [1, 2]);
  journal.append({ type: "native_lifecycle", binding: retired, runId: null, at: 3, kind: "session_started" });
  const old = journal.read(binding, NativeEventSequence.make(0));
  assert.deepEqual(old.gap, { from: 1, through: 1 });
  assert.deepEqual(old.entries.map((event) => event.seq), [2]);
  assert.equal(old.nextAfter, 3);
  assert.deepEqual(journal.read(retired, NativeEventSequence.make(1)).entries.map((event) => event.seq), [3]);
  assert.throws(() => journal.read(binding, NativeEventSequence.make(4)), { code: "invalid_input" });
  assert.throws(() => journal.read(binding, NativeEventSequence.make(0), 0), { code: "invalid_input" });
  stop();
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
    await Promise.race([f.run(Deferred.await(reached).pipe(Effect.timeout("15 seconds"))), starting]);
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
    assert.deepEqual(await f.run(replacement.service.row(f.work.taskId)), row);
    assert.equal((await f.run(replacement.service.row(f.work.taskId)))!.state, "READY");
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
    const two = { ...f.work, taskId: TaskId.make("two") }; await f.save(two);
    assert.partialDeepStrictEqual(await runner.call("acceptTask", two), { accepted: false, state: "full" });
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='PROMPT_INTENT',promptId='uncertain',pid=? WHERE taskId='one'", [process.pid]));
    await runner.close(); runner = await f.open();
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.state, "HELD"); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.occupied, 1);
    assert.partialDeepStrictEqual(await runner.call("acceptTask", two), { accepted: false, state: "full" });
    await assert.rejects(f.open());
    assert.partialDeepStrictEqual(await runner.call("journal", { taskId: "missing" }), { accepted: false, state: "not-accepted" });
    await runner.close();
    const expired = { ...f.work, taskId: TaskId.make("expired"), deadline: 12345 }; await f.save(expired);
    runner = await f.open({ ...f.config, stateDirectory: f.join(f.root, "expired-state") });
    await assert.rejects(runner.call("acceptTask", expired));
    assert.equal((await f.run(runner.service.sql.unsafe<Json>("SELECT count(*) AS count FROM tasks")))[0]!.count, 0);
    await f.run(runner.service.sql.unsafe("INSERT INTO tasks(taskId,fingerprint,work,worktree,state,occupied,incarnation) VALUES (?,?,?,?,'ACCEPTED',1,?)", [expired.taskId, await f.run(f.platform.fingerprint(expired)), Schema.encodeUnknownSync(Schema.fromJsonString(Task.fields.request))(expired), f.join(f.config.childDirectory, "expired", "repo"), "expired-incarnation"]));
    await runner.close(); runner = await f.open({ ...f.config, stateDirectory: f.join(f.root, "expired-state") });
    await f.run(Effect.gen(function*() {
      for (;;) { const row = yield* runner.service.row(expired.taskId); if (row?.state === "FAILED") return; yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("10 seconds")));
    assert.equal((await f.run(runner.service.row(expired.taskId)))!.pid, null);
  } finally { await f.close(); }
});

test("failed SDK discovery drains its child before releasing capacity and remains held without replay after restart", async () => {
  const f = await fixture(1, "acquisition-discovery");
  try {
    let runner = await f.open();
    const waitForHeldCapacityRelease = (taskId: TaskId) => f.run(Effect.gen(function*() {
      for (;;) {
        const row = yield* runner.service.row(taskId);
        if (row?.state === "HELD" && row.occupied === 0) return row;
        yield* Effect.sleep("10 millis");
      }
    }).pipe(Effect.timeout("10 seconds")));
    await runner.call("acceptTask", f.work);
    const original = await waitForHeldCapacityRelease(f.work.taskId);
    assert.equal(original.pid, null);
    assert.equal(original.nativeBinding, null);
    assert.equal(original.promptId, null);
    const marker = f.join(f.config.childDirectory, await f.run(f.platform.digest(f.work.taskId)), "home", "acquisition.pid");
    const observedPid = await f.run(f.fs.readFileString(marker));
    const pid = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Int))(observedPid);
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    await runner.close();
    runner = await f.open();
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.state, "HELD");
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.occupied, 0);
    const receipt = await runner.call("journal", { taskId: f.work.taskId });
    assert.deepEqual(await runner.call("acceptTask", f.work), receipt);
    const next = { ...f.work, taskId: TaskId.make("after-failed-discovery") };
    await f.save(next);
    assert.partialDeepStrictEqual(await runner.call("acceptTask", next), { accepted: true });
    await waitForHeldCapacityRelease(next.taskId);
    assert.equal(await f.run(f.fs.readFileString(marker)), observedPid, "Held launch intent must not be replayed");
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally {
    await f.close();
  }
});

test("deadline expiry during owned SQL admission and prompt transactions never dispatches expired work", async (t) => {
  for (const phase of ["admission", "prompt reservation", "committed prompt intent"]) await t.test(phase, async (child) => {
    const f = await fixture();
    child.after(() => f.close());
    const runner = await f.open();
    const originalTransaction = runner.service.sql.withTransaction;
    const attempted = await f.run(Deferred.make<void>()), unlock = await f.run(Deferred.make<void>());
    let outcome: Promise<number> | undefined;
    let commandTime = f.work.deadline - 1;
    try {
      let address: Address | undefined;
      if (phase !== "admission") {
        await runner.call("acceptTask", f.work);
        address = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })((await f.ready(runner.service)).address!);
        await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: "ready" });
      }
      const clock = await f.run(Clock.Clock);
      const controlledClock: Clock.Clock = {
        currentTimeMillis: Effect.sync(() => commandTime), currentTimeMillisUnsafe: () => commandTime,
        currentTimeNanos: clock.currentTimeNanos, currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
        monotonicTimeNanos: clock.monotonicTimeNanos, monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
        sleep: (duration) => clock.sleep(duration),
      };
      const transaction: typeof originalTransaction = (effect) => phase === "committed prompt intent"
        ? originalTransaction(effect).pipe(Effect.tap(() => Effect.sync(() => { commandTime = f.work.deadline + 1; })))
        : Effect.gen(function*() {
          const locked = yield* Deferred.make<void>();
          // Keep the real SQLite permit/BEGIN until the waiting command has crossed its deadline.
          const holder = yield* originalTransaction(Effect.gen(function*() {
            yield* Deferred.succeed(locked, undefined);
            yield* Deferred.await(unlock);
          })).pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(locked);
          yield* Deferred.succeed(attempted, undefined);
          const exit = yield* originalTransaction(effect).pipe(Effect.exit, Effect.ensuring(Deferred.succeed(unlock, undefined)));
          yield* Fiber.join(holder);
          return yield* Exit.isFailure(exit) ? Effect.failCause(exit.cause) : Effect.succeed(exit.value);
        });
      Object.defineProperty(runner.service.sql, "withTransaction", { value: transaction, configurable: true, writable: true });
      const input = phase === "admission" ? request("acceptTask", f.work) : request("release", { taskId: f.work.taskId, address, prompt: f.work.prompt });
      outcome = f.run(boundaryStatus(runner.service.handle(input, control).pipe(Effect.provideService(Clock.Clock, controlledClock))));
      if (phase !== "committed prompt intent") {
        await f.run(Deferred.await(attempted).pipe(Effect.timeout("5 seconds")));
        commandTime = f.work.deadline + 1;
        await f.run(Deferred.succeed(unlock, undefined));
      }
      assert.equal(await outcome, 409);
      const row = await f.run(runner.service.row(f.work.taskId));
      if (phase === "admission") assert.equal(row, undefined);
      else {
        assert.equal(row!.state, phase === "committed prompt intent" ? "HELD" : "READY");
        if (phase === "committed prompt intent") assert.ok(row!.promptId);
        else assert.equal(row!.promptId, null);
        assert.equal(row!.occupied, 1);
      }
    } finally {
      await f.run(Deferred.succeed(unlock, undefined));
      if (outcome) await outcome;
      Object.defineProperty(runner.service.sql, "withTransaction", { value: originalTransaction, configurable: true, writable: true });
    }
  });
});

test("enrollment bearer is health-only and control bearer cannot invent broker work or confirmation", async () => {
  const f = await fixture();
  try {
    const runner = await f.open();
    await assert.rejects(f.open({ ...f.config, controlTokenHash: await f.run(f.platform.digest(enrollment)) }));
    const url = HttpServer.formatAddress(runner.service.server.address);
    assert.equal(await f.run(f.client.execute(HttpClientRequest.post(`${url}/mcp`).pipe(HttpClientRequest.bodyText(Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue))({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} }, name: "journal", arguments: { taskId: "one" } } }), "application/json"), HttpClientRequest.setHeaders({ accept: "application/json, text/event-stream", "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": "journal" }))).pipe(Effect.map((response) => response.status), Effect.scoped)), 401);
    assert.equal(await f.run(boundaryStatus(f.rpc.call(url, "not-an-enrolled-bearer", "journal", { taskId: f.work.taskId }))), 401);
    const address = Schema.decodeUnknownSync(Address, { onExcessProperty: "error" })({ messageId: "one", revision: 1, workerId: f.work.workerId, sessionId: "session", incarnation: "incarnation", repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: f.work.deadline });
    for (const input of [request("acceptTask", f.work), request("prepare", { taskId: "one", address }), request("release", { taskId: "one", address, prompt: f.work.prompt }), request("inspect", { taskId: "one" }), request("command", { taskId: "one", type: "get_state" }), request("journal", { taskId: "one" })]) {
      assert.equal(await f.run(boundaryStatus(f.rpc.call(url, enrollment, input.method, input.params))), 401);
      assert.equal(await runner.status(input.method, input.params, enrollment), 401);
    }
    assert.equal(await runner.status("acceptTask", { ...f.work, taskId: "invented" }), 404);
    await f.save(f.work, { runnerId: RunnerId.make("other-runner") }); assert.equal(await runner.status("acceptTask", f.work), 403);
    await f.save(f.work, { fingerprint: Hash.make("0".repeat(64)) }); await assert.rejects(runner.call("acceptTask", f.work));
    await f.save(f.work, { request: { ...f.work, prompt: "unapproved ledger prompt" } }); await assert.rejects(runner.call("acceptTask", f.work));
    await f.save(f.work); await runner.call("acceptTask", f.work); const owned = await f.ready(runner.service); const actual = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(owned.address!);
    f.outage(true);
    await f.run(runner.service.heartbeat());
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.state, "READY");
    f.outage(false);
    const release = () => runner.call("release", { taskId: "one", address: actual, prompt: f.work.prompt });
    await f.save(f.work); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: { ...actual, revision: 2 }, confirmedRevision: 1, disposition: "ready" }); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: actual, confirmedRevision: 2, disposition: "ready" }); await assert.rejects(release());
    await f.save(f.work, { runnerId: RunnerId.make("other-runner"), address: actual, readback: actual, confirmedRevision: 1, disposition: "ready" }); assert.equal(await runner.status("release", { taskId: "one", address: actual, prompt: f.work.prompt }), 403);
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.promptId, null);
  } finally { await f.close(); }
});

test("native event and result projection omits secret content from durable journal and inspect", async () => {
  const f = await fixture();
  try {
    let runner = await f.open(); await runner.call("acceptTask", f.work); await f.ready(runner.service);
    const secret = "native-secret-provider-token-DO-NOT-STORE";
    const row = (await f.run(runner.service.row(f.work.taskId)))!;
    const address = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address!);
    await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: "ready" });
    const privateMessages = Schema.decodeUnknownSync(NativeMessagesPage, { onExcessProperty: "error" })(await runner.call("command", { taskId: "one", type: "get_messages", limit: 1 }));
    assert.equal(JSON.stringify(privateMessages).includes(secret), true);
    assert.equal(privateMessages.messages.length, 1);
    assert.equal(privateMessages.more, true);
    assert.equal(privateMessages.nextCursor, "1");
    assert.ok(typeof privateMessages.nextCursor === "string");
    const identityPage = Schema.decodeUnknownSync(NativeMessagesPage, { onExcessProperty: "error" })(await runner.call("command", { taskId: "one", type: "get_messages", cursor: privateMessages.nextCursor, limit: 1 }));
    assert.deepEqual(identityPage.binding, privateMessages.binding);
    assert.equal(identityPage.messages.length, 1);
    assert.partialDeepStrictEqual(identityPage.messages[0], { content: "λ ✓", fixtureIdentity: { uid: row.childUid, effectiveUid: row.childUid, cwd: row.worktree, capabilityEffective: "0000000000000000", noNewPrivileges: "1" } });
    assert.equal(identityPage.more, false);
    assert.equal(identityPage.nextCursor, null);
    await assert.rejects(runner.call("command", { taskId: "one", type: "get_messages", cursor: "not-a-cursor" }));
    assert.throws(() => request("command", { taskId: "one", type: "get_messages", limit: 257 }));
    assert.throws(() => request("command", { taskId: "one", type: "get_state", message: "private" }));
    await runner.call("release", { taskId: "one", address, prompt: f.work.prompt });
    await f.run(Effect.gen(function*() {
      for (;;) { const current = yield* runner.service.row(f.work.taskId); if (current?.state === "SUCCEEDED") return; if (current?.state === "HELD" || current?.state === "FAILED") return yield* Effect.fail(new CommandError({ message: "Fixture prompt did not settle" })); yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("15 seconds")));
    const events = await f.run(runner.service.sql.unsafe<Json>("SELECT payload FROM events"));
    const originalIdentities = events.map((event) => Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload)))
      .filter((event) => event.type === "readback" || event.type === "native_lifecycle")
      .map(({ type, address, binding, runId, nativeCursor, kind }) => ({ type, address, binding, runId, nativeCursor, kind }));
    assert.ok(originalIdentities.some((event) => event.type === "readback" && event.address));
    assert.ok(originalIdentities.some((event) => event.type === "native_lifecycle" && event.binding));
    assert.ok(events.some((event) => Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload)).kind === "run_settled"));
    assert.equal(JSON.stringify(events).includes(secret), false);
    assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })((await f.run(runner.service.row(f.work.taskId)))!.result!), { hostSettled: true, evidenceVerified: false });
    assert.equal(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(await runner.call("inspect", { taskId: "one" })).includes(secret), false);
    await f.run(runner.service.sql.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", [f.work.taskId, Schema.encodeUnknownSync(Schema.fromJsonString(Json))({ type: "message_end", message: secret })]));
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET result=?,reason=? WHERE taskId='one'", [Schema.encodeUnknownSync(Schema.fromJsonString(Json))({ hostSettled: true, credentials: secret }), secret]));
    await f.run(runner.service.sql.unsafe("PRAGMA user_version=0"));
    await runner.close(); runner = await f.open();
    const migratedIdentities = (await f.run(runner.service.sql.unsafe<Json>("SELECT payload FROM events")))
      .map((event) => Schema.decodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload)))
      .filter((event) => event.type === "readback" || event.type === "native_lifecycle")
      .map(({ type, address, binding, runId, nativeCursor, kind }) => ({ type, address, binding, runId, nativeCursor, kind }));
    assert.deepEqual(migratedIdentities, originalIdentities);
    assert.equal(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(await f.run(runner.service.sql.unsafe("SELECT payload FROM events"))).includes(secret), false);
    assert.equal(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(await f.run(runner.service.sql.unsafe("SELECT result,reason FROM tasks"))).includes(secret), false);
    const database = await f.run(f.fs.readFile(f.join(f.config.stateDirectory, "runner.sqlite")));
    assert.equal(new TextDecoder().decode(database).includes(secret), false);
  } finally { await f.close(); }
});

test("legacy journal migration rejects missing and invalid readback addresses and native bindings", async (t) => {
  const f = await fixture();
  try {
    const cases: { name: string; payload: Json }[] = [
      { name: "missing readback address", payload: { type: "readback" } },
      { name: "invalid readback address", payload: { type: "readback", address: {} } },
      { name: "missing native binding", payload: { type: "native_lifecycle", runId: null, nativeCursor: 1, kind: "session_started" } },
      { name: "invalid native binding", payload: { type: "native_lifecycle", binding: {}, runId: null, nativeCursor: 1, kind: "session_started" } },
    ];
    for (const [index, { name, payload }] of cases.entries()) await t.test(name, async () => {
      const config = { ...f.config, stateDirectory: f.join(f.root, `legacy-${index}`) };
      const runner = await f.open(config);
      await runner.close();
      await f.seed(config.stateDirectory, (sql) => Effect.gen(function*() {
        yield* sql.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", [f.work.taskId, Schema.encodeUnknownSync(Schema.fromJsonString(Json))(payload)]);
        yield* sql.unsafe("PRAGMA user_version=0");
      }));
      await assert.rejects(f.open(config));
    });
  } finally { await f.close(); }
});

test("obsolete handed-off report reconciles durably without starving later live reports", async () => {
  const f = await fixture(2);
  try {
    let runner = await f.open(); const live = { ...f.work, taskId: TaskId.make("two") }; await f.save(live);
    await runner.call("acceptTask", f.work); await runner.call("acceptTask", live); await f.ready(runner.service); await f.ready(runner.service, live.taskId);
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='RUNNING',promptId='reserved',pid=? WHERE taskId='one'", [process.pid]));
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET state='SUCCEEDED',occupied=0,result='{}' WHERE taskId='two'"));
    await f.save(f.work, { runnerId: undefined });
    const rows = (await f.run(runner.service.sql.unsafe("SELECT ?+rowid AS childUid,* FROM tasks ORDER BY taskId", [f.config.childUidBase]))).map((row) => Schema.decodeUnknownSync(RunnerRow, { onExcessProperty: "error" })(row));
    await f.run(runner.service.reports(rows));
    const obsolete = (await f.run(runner.service.sql.unsafe<Json>("SELECT state,detached,occupied,promptId FROM tasks WHERE taskId='one'")))[0]!;
    assert.deepEqual({ ...obsolete }, { state: "HELD", detached: 1, occupied: 1, promptId: "reserved" });
    assert.equal(Schema.decodeUnknownSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(String((await f.run(f.broker.sql.unsafe<Json>("SELECT data FROM tasks WHERE id='two'")))[0]!.data)).state, "SUCCEEDED");
    await runner.close(); runner = await f.open(); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.detached, 1); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.occupied, 1);
    await f.save(live, { state: "FAILED" }); await f.run(runner.service.reports([(await f.run(runner.service.row(live.taskId)))!])); assert.equal((await f.run(runner.service.row(live.taskId)))!.detached, 0);
    f.outage(true); await f.run(runner.service.reports([(await f.run(runner.service.row(live.taskId)))!])); assert.equal((await f.run(runner.service.row(live.taskId)))!.detached, 0);
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
    await f.save(f.work, { runnerId: RunnerId.make("reassigned-runner"), state: "ASSIGNING" });
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

test("native missing admission, resource gaps and transport loss hold durable intent without replay", async (t) => {
  for (const mode of ["missing-admission", "gap", "transport", "invalid-event"]) await t.test(mode, async () => {
    // Fixed, metadata-only boundaries remain visible even when disposal never resolves.
    const phase = (name: string) => process.stderr.write(`[native-held:${mode}] ${name} ${Date.now()}\n`);
    phase("fixture.open.start");
    const f = await fixture(1, mode, phase);
    phase("fixture.open.done");
    try {
      let runner = await f.open();
      phase("runner.open.done");
      phase("accept.start");
      await runner.call("acceptTask", f.work);
      phase("accept.done");
      phase("ready.start");
      const ready = await f.ready(runner.service);
      phase("ready.done");
      assert.ok(ready.pid, "READY owns a real native child");
      const address = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(ready.address!);
      await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: "ready" });
      phase("release.start");
      await assert.rejects(runner.call("release", { taskId: "one", address, prompt: f.work.prompt }));
      phase("release.rejected");
      const held = (await f.run(runner.service.row(f.work.taskId)))!;
      assert.equal(held.state, "HELD");
      assert.ok(held.promptId);
      assert.equal(held.result, null, "Unproven terminal work has no persisted result");
      phase("duplicate.release.start");
      await assert.rejects(runner.call("release", { taskId: "one", address, prompt: f.work.prompt }));
      phase("duplicate.release.rejected");
      assert.equal((await f.run(runner.service.row(f.work.taskId)))!.promptId, held.promptId);
      if (mode === "transport") await f.run(Effect.gen(function*() {
        for (;;) { if ((yield* runner.service.row(f.work.taskId))?.occupied === 0) return; yield* Effect.sleep("10 millis"); }
      }).pipe(Effect.timeout("15 seconds")));
      else assert.equal(held.occupied, 1, "A protocol failure is not physical process closure");
      await runner.close();
      assert.throws(() => process.kill(ready.pid!, 0), { code: "ESRCH" }, "Disposal must reap the actual native child before restart");
      phase("restart.start");
      runner = await f.open();
      phase("restart.done");
      const recovered = (await f.run(runner.service.row(f.work.taskId)))!;
      assert.equal(recovered.state, "HELD");
      assert.equal(recovered.promptId, held.promptId);
      assert.equal(recovered.nativeBinding, held.nativeBinding);
      assert.equal(recovered.result, null);
      assert.throws(() => process.kill(ready.pid!, 0), { code: "ESRCH" }, "Recovery must not replay the old native child");
      phase("recovered.release.start");
      await assert.rejects(runner.call("release", { taskId: "one", address, prompt: f.work.prompt }));
      phase("recovered.release.rejected");
    } finally {
      phase("fixture.close.start");
      await f.close();
      assert.equal(existsSync(f.root), false, "All fixture scopes close before the private root is removed");
      phase("fixture.close.done");
    }
  });
});

test("stale native generation cannot establish broker readiness", async () => {
  const f = await fixture(1, "stale");
  try {
    const runner = await f.open();
    await runner.call("acceptTask", f.work);
    await assert.rejects(f.ready(runner.service));
    const row = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(row.state, "HELD");
    assert.equal(row.address, null);
    assert.equal(row.promptId, null);
  } finally { await f.close(); }
});

test("abort acknowledgement and idle snapshots cannot replace a missing whole-run terminal proof", async () => {
  const f = await fixture(1, "missing-terminal");
  try {
    let runner = await f.open();
    await runner.call("acceptTask", f.work);
    const ready = await f.ready(runner.service);
    const address = Schema.decodeUnknownSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(ready.address!);
    await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: "ready" });
    await runner.call("release", { taskId: "one", address, prompt: f.work.prompt });
    const admitted = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(admitted.state, "RUNNING");
    assert.ok(admitted.nativeRunId);
    const snapshot = await runner.call("command", { taskId: "one", type: "get_state" });
    assert.partialDeepStrictEqual(snapshot, { currentRunId: null, isStreaming: false });
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.state, "RUNNING");
    const abort = await runner.call("command", { taskId: "one", type: "abort" });
    assert.partialDeepStrictEqual(abort, { disposition: "already_settled", runId: null });
    const held = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(held.state, "HELD");
    assert.equal(held.occupied, 1);
    assert.equal(held.result, null);
    assert.equal((await f.run(runner.service.sql.unsafe<{ absent: number }>("SELECT result IS NULL AS absent FROM tasks WHERE taskId=?", [f.work.taskId])))[0]!.absent, 1);
    if (held.pid === null) assert.fail("Held physical ownership must retain the native child PID");
    process.kill(held.pid, "SIGTERM");
    await f.run(Effect.gen(function*() {
      for (;;) { if ((yield* runner.service.row(f.work.taskId))?.occupied === 0) return; yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("15 seconds")));
    const closed = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(closed.state, "HELD");
    assert.equal(closed.promptId, admitted.promptId);
    assert.equal(closed.nativeRunId, admitted.nativeRunId);
    await assert.rejects(runner.call("release", { taskId: "one", address, prompt: f.work.prompt }));
    // Exercise readback of the old persisted JSON-null encoding without changing owned intent.
    await f.run(runner.service.sql.unsafe("UPDATE tasks SET result='null' WHERE taskId=?", [f.work.taskId]));
    await runner.close();
    runner = await f.open();
    const recovered = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(recovered.result, null);
    assert.equal(recovered.promptId, admitted.promptId);
    assert.equal(recovered.nativeRunId, admitted.nativeRunId);
    assert.equal(recovered.state, "HELD");
    await assert.rejects(runner.call("release", { taskId: "one", address, prompt: f.work.prompt }));
  } finally {
    await f.close();
  }
});

test("public-hook-only capabilities hold an inspectable child without creating prompt intent", async () => {
  const f = await fixture(1, "public-hooks");
  try {
    const runner = await f.open();
    await runner.call("acceptTask", f.work);
    const held = await f.run(Effect.gen(function*() {
      for (;;) {
        const row = yield* runner.service.row(f.work.taskId);
        if (row?.state === "HELD") return row;
        yield* Effect.sleep("10 millis");
      }
    }).pipe(Effect.timeout("15 seconds")));
    assert.equal(held.promptId, null);
    assert.equal(held.nativeRunId, null);
    assert.equal(held.address, null);
    assert.equal(held.occupied, 1);
    assert.equal(held.result, null);
    const state = Schema.decodeUnknownSync(RunnerNativeState)(await runner.call("command", { taskId: f.work.taskId, type: "get_state" }));
    assert.equal(state.binding.host, "omp");
    const address = Schema.decodeUnknownSync(Address)({ messageId: f.work.taskId, revision: 1, workerId: f.work.workerId,
      sessionId: state.binding.sessionId, incarnation: held.incarnation, repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: f.work.deadline });
    assert.equal(await runner.status("release", { taskId: f.work.taskId, address, prompt: f.work.prompt }), 409);
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.promptId, null);
  } finally { await f.close(); }
});
