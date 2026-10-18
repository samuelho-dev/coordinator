import { RoleEnum, DispositionEnum, RunnerRejectionStateEnum, RunnerControlEnum } from "../src/contracts.ts";
import { TaskStateEnum, RunnerStateEnum } from "../src/generated/database/enums.ts";
import { it, onTestFinished } from "@effect/vitest";
import assert from "node:assert/strict";
import { existsSync, watch } from "node:fs";
import { Clock, ConfigProvider, Context, Deferred, Effect, Exit, Fiber, Layer, ManagedRuntime, Schema } from "effect";
import { ChildProcess } from "effect/process";
import { HttpClient, HttpClientRequest, HttpServer } from "effect/http";
import { Runner, childEnvironment } from "../src/runner.ts";
import { Broker } from "../src/broker.ts";
import { DatabaseService } from "../src/database.ts";
import { Mcp, serverLayer } from "../src/mcp.ts";
import { Platform } from "../src/platform.ts";
import { Commands, type Command, type CommandDescriptor, Address, BrokerRequest, CommandError, HttpError, Json, JsonValue, RunnerEvent, RunnerResult, RunnerAgentState, RunnerRequest, Task, RunnerId, Repo, GitHubRepositoryId, Principal, RunnerConfig, UserId, Work } from "../src/contracts.ts";
import { RunnerTask, TaskId } from "../src/generated/database/types.ts";
import { AgentRunOutcomeEnum, AgentAdmissionDispositionEnum, AgentHostEnum, AgentBinding, AgentEventSequence, AgentGeneration, AgentIncarnationId, AgentSessionId, AgentSubmissionId, AgentMessagesPage, AgentLifecycleJournal } from "../src/agent/protocol.ts";
import { provisionPostgres, dropPostgres, postgresAdmin, postgresConfig } from "./postgres-fixture.ts";


const enrollment = "regression-only-enrollment-000000000000";
const control = "regression-only-control-000000000000000";
const common = Layer.merge(Platform.layer, Mcp.layer);
const request = (method: string, params: unknown) => Schema.decodeUnknownSync(RunnerRequest, { onExcessProperty: "error" })({ method, params });
const boundaryStatus = <A, E>(effect: Effect.Effect<A, E>) => effect.pipe(Effect.match({ onSuccess: () => 200, onFailure: (error) => error instanceof HttpError ? error.status : 500 }));

it.effect("agent child environment excludes broker and runner database credentials", () =>
  Effect.gen(function*() {
    const environment = yield* childEnvironment("/private/home").pipe(
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({
        COORDINATOR_DATABASE_URL: "postgresql://broker:broker-secret@localhost/broker",
        COORDINATOR_RUNNER_DATABASE_URL: "postgresql://runner:runner-secret@localhost/runner",
        OPENAI_API_KEY: "allowed-model-credential",
      }))),
    );
    assert.deepEqual(environment, {
      PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", HOME: "/private/home",
      OPENAI_API_KEY: "allowed-model-credential",
    });
  }),
);

async function fixture(slots = 1, hostMode = "complete", phase: (name: string) => void = () => {}) {
  const services = ManagedRuntime.make(common);
  const platform = await services.runPromise(Platform);
  const rpc = await services.runPromise(Mcp);
  const client = await services.runPromise(HttpClient.HttpClient);
  const fs = platform.fs, join = platform.path.join;
  const run = <A, E>(effect: Effect.Effect<A, E, Platform>) => services.runPromise(effect);
  const root = await run(fs.realPath(await run(fs.makeTempDirectory({ prefix: "coordinator-runner-" }))));
  await run(fs.chmod(root, 0o711));
  const repo = join(root, "repo"); await run(fs.makeDirectory(repo, { mode: 0o700 }));
  const git = (...args: string[]) => run(platform.exec(ChildProcess.make("git", args, { cwd: repo, env: { PATH: "/usr/local/bin:/usr/bin:/bin" }, extendEnv: false }))).then((output) => output.trim());
  await git("init"); await git("config", "user.name", "Runner regression"); await git("config", "user.email", "runner@example.invalid");
  await git("remote", "add", "origin", "https://github.com/test/runner");
  await run(fs.writeFileString(join(repo, "tracked"), "acceptance invariant\n")); await git("add", "tracked"); await git("commit", "-m", "initial");
  const credentials = join(root, "credentials"); await run(fs.makeDirectory(credentials, { mode: 0o700 }));
  const tokenFile = join(credentials, "token"); await run(fs.writeFileString(tokenFile, enrollment, { mode: 0o600 }));
  const principal = Schema.decodeUnknownSync(Principal, { onExcessProperty: "error" })({ id: "runner-test", owner: "owner", roles: [RoleEnum.runner], repos: ["github.com/test/runner"], tokenHash: await run(platform.digest(enrollment)), controlTokenHash: await run(platform.digest(control)) });
  const databaseUrl = await services.runPromise(provisionPostgres());
  const runnerDatabaseUrl = await services.runPromise(provisionPostgres());
  const brokerRuntime = ManagedRuntime.make(Broker.layer({ backupDirectory: join(root, "backups"), host: "127.0.0.1", port: 0,
    archiveRetentionMs: 60000, backupRetentionMs: 60000, repositoryIds: { [Repo.make("github.com/test/runner")]: GitHubRepositoryId.make(1) }, principals: [principal] }).pipe(Layer.provide(postgresConfig(databaseUrl)), Layer.provide(common)));
  const broker = await brokerRuntime.runPromise(Broker);
  let unavailable = false, taskReads = 0;
  let gate: { reached: Deferred.Deferred<void>; proceed: Deferred.Deferred<void> } | undefined;
  class Proxy extends Context.Service<Proxy>()("runner-test/Proxy", {
    make: Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer;
      yield* rpc.serve(server, { target: "broker", authorize: broker.authorize, handle: (p, input) => Effect.gen(function*() {
        if (unavailable) return yield* new HttpError({ status: 503, message: "Transient broker outage" });
        if (input.method === Commands.getTask.name && gate && ++taskReads === 2) {
          yield* Deferred.succeed(gate.reached, undefined);
          yield* Deferred.await(gate.proceed);
        }
        return yield* broker.handle(p, input);
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
  const moduleUrl = async (relative: string) => Schema.encodeSync(Schema.fromJsonString(Schema.String))((await run(platform.path.toFileUrl(join(workspace, relative)))).href);
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
import { AgentHostEnum, AgentRunOutcomeEnum, AgentAdmissionDispositionEnum, AgentBinding, AgentError, AgentConfig, AgentTools, AgentPromptResult, agentEventsUri, AgentLifecycleJournal } from ${await moduleUrl("dist/agent/protocol.js")};
import { agentHttpLayer, agentTool, createAgentPresentation } from ${await moduleUrl("dist/agent/server.js")};
import { realpathSync, writeFileSync, readFileSync, renameSync, statSync, existsSync, watch } from "node:fs";
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
      Object.keys(process.env).some(key => !["HOME", "PATH", "LANG", "COORDINATOR_AGENT_CONFIG", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "OMP_AUTH_BROKER_URL", "OMP_AUTH_BROKER_TOKEN"].includes(key))) throw new Error("Child privilege boundary failed");
  if (${JSON.stringify(hostMode)} === "acquisition-discovery") writeFileSync(process.env.HOME + "/acquisition.pid", JSON.stringify(process.pid), { mode: 0o600 });
  const binding = Schema.decodeUnknownSync(AgentBinding)({ host: AgentHostEnum.omp, sessionId: randomUUID(), incarnation: randomUUID(), generation: 0 });
  const journal = new AgentLifecycleJournal({ retention: ${hostMode === "gap" ? 1 : 1024} });
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
  const emit = (kind, fields = {}) => journal.append({ type: "agent_lifecycle", binding, runId: currentRunId, at: Date.now(), kind, ...fields });
  const presentation = createAgentPresentation(() => ({
    agentBinding: binding,
  }));
  const validate = input => {
    if (input.binding && JSON.stringify(input.binding) !== JSON.stringify(binding)) throw new AgentError({ code: "binding_mismatch", message: "Stale fixture binding" });
  };
  const handlers = new Map([
    [AgentTools.getState, input => Effect.sync(() => {
      validate(input);
      if (${JSON.stringify(hostMode)} === "stale" && input.binding) return { binding: { ...binding, generation: binding.generation + 1 }, ...privateState, cwd: realpathSync(process.cwd()), sessionFile: realpathSync(sessionFile), model: null, currentRunId, isStreaming: false, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: messages.length, steeringMode: "all", followUpMode: "all" };
      return { binding, ...privateState, cwd: realpathSync(process.cwd()), sessionFile: realpathSync(sessionFile), model: null, currentRunId, isStreaming: currentRunId !== null, isCompacting: false, pendingMessageCount: 0, pendingAsyncWorkCount: 0, messageCount: messages.length, steeringMode: "all", followUpMode: "all" };
    })],
    [AgentTools.bash, input => Effect.try({
      try: () => { validate(input); return { binding, exitCode: 0, output: execFileSync("/bin/sh", ["-c", input.command], { cwd: process.cwd(), env: { PATH: "/usr/local/bin:/usr/bin:/bin", HOME: process.env.HOME }, encoding: "utf8" }), truncated: false, killed: false }; },
      catch: error => error instanceof AgentError ? error : new AgentError({ code: "agent_failure", message: "Actual fixture shell failed" }),
    })],
    [AgentTools.getMessages, input => Effect.sync(() => {
      validate(input);
      const start = input.cursor === undefined ? 0 : Number(input.cursor);
      if (!Number.isSafeInteger(start) || start < 0 || start > messages.length) throw new AgentError({ code: "invalid_input", message: "Invalid message cursor" });
      const end = Math.min(messages.length, start + (input.limit ?? 256));
      return { binding, messages: messages.slice(start, end), nextCursor: end < messages.length ? String(end) : null, more: end < messages.length };
    })],
    [AgentTools.abort, input => Effect.sync(() => { validate(input); return { binding, runId: currentRunId, disposition: currentRunId === null ? "already_settled" : "requested" }; })],
    [AgentTools.prompt, input => Effect.promise(async () => {
      validate(input);
      if (${JSON.stringify(hostMode)} === "transport") process.exit(23);
      currentRunId = randomUUID();
      const seq = ${JSON.stringify(hostMode)} === "missing-admission" ? journal.highWater : emit("submission_admitted", { submissionId: input.submissionId, disposition: AgentAdmissionDispositionEnum.started }).seq;
      const admission = { binding, submissionId: input.submissionId, runId: currentRunId, disposition: AgentAdmissionDispositionEnum.started, eventsUri: agentEventsUri(binding), seq };
      emit("run_started");
      if (${JSON.stringify(hostMode)} === "pending") {
        const gate = Promise.withResolvers();
        const observer = watch(process.env.HOME, () => { if (existsSync(process.env.HOME + "/settle")) gate.resolve(); });
        try {
          if (existsSync(process.env.HOME + "/settle")) gate.resolve();
          await gate.promise;
        } finally { observer.close(); }
        writeFileSync(process.env.HOME + "/settled-run", currentRunId, { mode: 0o600 });
      }
      if (${JSON.stringify(hostMode)} !== "missing-terminal") emit("run_settled", { outcome: AgentRunOutcomeEnum.completed });
      const result = Schema.decodeUnknownSync(AgentPromptResult)({ admission, disposition: "settled", final: { binding, runId: currentRunId, outcome: AgentRunOutcomeEnum.completed } });
      currentRunId = null;
      writeFileSync(sessionFile, JSON.stringify(messages) + "\\n", { mode: 0o600 });
      return result;
    })],
  ]);
  const tools = Object.values(AgentTools).filter(descriptor => descriptor.hosts.includes(AgentHostEnum.omp) && (${JSON.stringify(hostMode)} !== "acquisition-discovery" || descriptor !== AgentTools.getState) && (${JSON.stringify(hostMode)} !== "public-hooks" || descriptor !== AgentTools.prompt)).map(descriptor => agentTool(descriptor, handlers.get(descriptor) ?? (() => Effect.fail(new AgentError({ code: "unsupported_operation", message: "Fixture operation is not implemented" })))));
  NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
    const config = Schema.decodeUnknownSync(AgentConfig)(JSON.parse(readFileSync(process.env.COORDINATOR_AGENT_CONFIG, "utf8")));
    const token = readFileSync(config.tokenFile, "utf8");
    const app = yield* HttpRouter.toHttpEffect(agentHttpLayer({ host: AgentHostEnum.omp, binding, journal, tools, presentation, name: "isolated-runner-fixture", version: "1" }));
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
  const work = Schema.decodeUnknownSync(Work, { onExcessProperty: "error" })({ taskId: TaskId.make("one"), repo: "github.com/test/runner", ref: "HEAD", sha: await git("rev-parse", "HEAD"), launcher: AgentHostEnum.omp, prompt: "approved prompt", scope: "scope", restrictions: "no deployment", expectedEvidence: ["checks"], deadline: await run(Clock.currentTimeMillis) + 60000,
    workerId: "native-worker", githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true });
  async function save(work: Work, overrides: Partial<Task> = {}) {
    const task: Task = { taskId: work.taskId, owner: principal.owner, fingerprint: await run(platform.fingerprint(work)), request: work, state: TaskStateEnum.ACCEPTED, runnerId, createdAt: 1, updatedAt: 1, ...overrides };
    const data = Schema.encodeSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })(task);
    await run(broker.database.query(db => db.insertInto("tasks").values({ id: task.taskId, data }).onConflict((conflict) => conflict.column("id").doUpdateSet({ data })))); return task;
  }
  await save(work);
  const runnerLayer = Runner.layer(config);
  const runtimes = new Set<ManagedRuntime.ManagedRuntime<Runner, Layer.Error<typeof runnerLayer> | Layer.Error<typeof common>>>();
  async function open(input = config, servicesLayer = common, url = runnerDatabaseUrl) {
    const runtime = ManagedRuntime.make(Runner.layer(input).pipe(Layer.provide(postgresConfig(url)), Layer.provide(servicesLayer))); runtimes.add(runtime);
    const service = await runtime.runPromise(Runner);
    return { service, async close() { phase("runner.dispose.start"); await runtime.dispose(); runtimes.delete(runtime); phase("runner.dispose.done"); }, call<I extends Schema.ConstraintCodec<unknown, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, S extends Schema.Top & Schema.ConstraintCodec<{ readonly result: O["Type"] }, unknown>>(descriptor: CommandDescriptor<I, O> & { readonly structuredOutput: S; readonly target: "runner" }, params: I["Type"], bearer = control) { return run(rpc.call(HttpServer.formatAddress(service.server.address), bearer, descriptor, params)); }, status(descriptor: Extract<Command, { readonly target: "runner" }>, params: unknown, bearer = control) { return runtime.runPromise(boundaryStatus(service.handle(request(descriptor.name, params), bearer))); } };
  }
  const ready = (runner: Runner["Service"], taskId = work.taskId) => run(Effect.gen(function*() {
    for (;;) {
      const row = yield* runner.row(taskId);
      if (row?.state === RunnerStateEnum.READY) return row;
      if (row?.state === RunnerStateEnum.FAILED || row?.state === RunnerStateEnum.HELD) {
        return yield* Effect.fail(new CommandError({ message: `Native fixture did not become READY: ${row.reason}` }));
      }
      yield* Effect.sleep("10 millis");
    }
  }).pipe(Effect.timeout("15 seconds")));
  let closing: Promise<void> | undefined;
  const result = { root, broker, config, runnerDatabaseUrl, work, save, open, ready, run, fs, platform, rpc, client, join,
    outage(value: boolean) { unavailable = value; },
    async fence() { gate = { reached: await run(Deferred.make<void>()), proceed: await run(Deferred.make<void>()) }; taskReads = 0; return gate; },
    close() { return closing ??= (async () => {
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
      await services.runPromise(dropPostgres(databaseUrl));
      await services.runPromise(dropPostgres(runnerDatabaseUrl));
      phase("fixture.broker.dispose.done");
      phase("fixture.root.remove.start");
      await run(fs.remove(root, { recursive: true, force: true }));
      phase("fixture.root.remove.done");
      phase("fixture.services.dispose.start");
      await services.dispose();
      phase("fixture.services.dispose.done");
    })(); }
  };
  onTestFinished(() => result.close());
  return result;
}

it("typed lifecycle derivation preserves retained gaps, binding generations and ordered reentrant fanout", async () => {
  const journal = new AgentLifecycleJournal({ retention: 2 });
  const binding: AgentBinding = { host: AgentHostEnum.omp, sessionId: AgentSessionId.make("session"),
    incarnation: AgentIncarnationId.make("incarnation"), generation: AgentGeneration.make(0) };
  const retired = { ...binding, generation: AgentGeneration.make(1) };
  const delivered: number[] = [];
  const stop = journal.subscribe((event) => {
    delivered.push(event.seq);
    if (event.seq === 1) journal.append({ type: "agent_lifecycle", binding, runId: null, at: 2, kind: "session_retired", reason: "retired", replacement: retired });
  });
  const first = journal.append({ type: "agent_lifecycle", binding, runId: null, at: 1, kind: "session_started" });
  assert.notEqual(first.binding, binding, "Journal freezing must not mutate the caller's binding");
  assert.equal(Object.isFrozen(binding), false);
  await new Promise<void>((resolve) => queueMicrotask(() => queueMicrotask(resolve)));
  assert.deepEqual(delivered, [1, 2]);
  journal.append({ type: "agent_lifecycle", binding: retired, runId: null, at: 3, kind: "session_started" });
  const old = journal.read(binding, AgentEventSequence.make(0));
  assert.deepEqual(old.gap, { from: 1, through: 1 });
  assert.deepEqual(old.entries.map((event) => event.seq), [2]);
  assert.equal(old.nextAfter, 3);
  assert.deepEqual(journal.read(retired, AgentEventSequence.make(1)).entries.map((event) => event.seq), [3]);
  assert.throws(() => journal.read(binding, AgentEventSequence.make(4)), { code: "invalid_input" });
  assert.throws(() => journal.read(binding, AgentEventSequence.make(0), 0), { code: "invalid_input" });
  stop();
});



it("PostgreSQL loss fences serving while an admitted unsigned-UID native run finishes without replay", async () => {
  const f = await fixture(1, "pending");
  try {
    const runner = await f.open({ ...f.config, childUidBase: UserId.make(3000000000) });
    await runner.call(Commands.acceptTask, f.work);
    const row = await f.ready(runner.service);
    assert.equal(row.childUid, 3000000001);
    const address = Schema.decodeSync(Schema.fromJsonString(Address))(row.address!);
    await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: DispositionEnum.ready });
    await runner.call(Commands.releaseTask, { taskId: f.work.taskId, address, prompt: f.work.prompt });
    const admitted = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(admitted.state, RunnerStateEnum.RUNNING);
    if (admitted.pid === null || admitted.agentRunId === null) assert.fail("Native admission must retain the actual process and run");
    await f.run(Effect.gen(function*() {
      const database = yield* DatabaseService;
      yield* database.query(client => client.withTables<{ pg_stat_activity: { pid: number; datname: string; application_name: string } }>().selectFrom("pg_stat_activity")
        .select((eb) => eb.fn<boolean>("pg_terminate_backend", [eb.ref("pid")]).as("terminated"))
        .where((eb) => eb("datname", "=", eb.fn<string>("current_database", [])))
        .where("application_name", "=", "coordinator")
        .where((eb) => eb("pid", "!=", eb.fn<number>("pg_backend_pid", []))));
    }).pipe(Effect.provide(postgresAdmin(f.runnerDatabaseUrl)), Effect.scoped));
    await assert.rejects(f.run(runner.service.failure));
    assert.equal(await runner.status(Commands.acceptTask, f.work), 503);
    assert.equal(await runner.status(Commands.releaseTask, { taskId: f.work.taskId, address, prompt: f.work.prompt }), 503);
    const bearer = await f.run(f.fs.readFileString(f.config.tokenFile));
    const endpoint = HttpServer.formatAddress(runner.service.server.address);
    assert.equal(await f.run(f.client.get(`${endpoint}/readyz`, { headers: { authorization: `Bearer ${bearer}` } })).then(response => response.status), 503);
    assert.equal(await f.run(f.client.get(`${endpoint}/livez`)).then(response => response.status), 200);
    process.kill(admitted.pid, 0);
    const home = f.join(f.config.childDirectory, await f.run(f.platform.digest(f.work.taskId)), "home");
    const settlement = Promise.withResolvers<void>();
    const observer = watch(home, (event, filename) => { if (event === "change" && filename === "settled-run") settlement.resolve(); });
    try {
      await f.run(f.fs.writeFileString(f.join(home, "settle"), "", { mode: 0o644 }));
      await f.run(Effect.promise(() => settlement.promise).pipe(Effect.timeout("10 seconds")));
      assert.equal(await f.run(f.fs.readFileString(f.join(home, "settled-run"))), admitted.agentRunId);
    } finally { observer.close(); }
    await runner.close();
    const replacement = await f.open({ ...f.config, childUidBase: UserId.make(3000000000) });
    const recovered = (await f.run(replacement.service.row(f.work.taskId)))!;
    assert.equal(recovered.state, RunnerStateEnum.HELD);
    assert.equal(recovered.promptId, admitted.promptId);
    assert.equal(recovered.agentRunId, admitted.agentRunId);
    await assert.rejects(replacement.call(Commands.releaseTask, { taskId: f.work.taskId, address, prompt: f.work.prompt }));
  } finally { await f.close(); }
});

it("real broker-bound PostgreSQL acceptance is unique, conflict-safe and reserves uncertain capacity", async () => {
  const f = await fixture();
  try {
    let runner = await f.open();
    const otherDatabaseUrl = await f.run(provisionPostgres());
    try { await assert.rejects(f.open(f.config, common, otherDatabaseUrl)); }
    finally { await f.run(dropPostgres(otherDatabaseUrl)); }
    await runner.call(Commands.acceptTask, f.work); await f.ready(runner.service);
    const first = await runner.call(Commands.journalTask, { taskId: TaskId.make("one") }); assert.deepEqual(await runner.call(Commands.acceptTask, f.work), first);
    await assert.rejects(runner.call(Commands.acceptTask, { ...f.work, prompt: "changed" }));
    const two = { ...f.work, taskId: TaskId.make("two") }; await f.save(two);
    assert.partialDeepStrictEqual(await runner.call(Commands.acceptTask, two), { accepted: false, state: RunnerRejectionStateEnum.full });
    await f.run(runner.service.database.query(client => client.updateTable("runner_tasks").set({ state: RunnerStateEnum.PROMPT_INTENT, promptId: AgentSubmissionId.make("uncertain"), pid: RunnerTask.fields.pid.members[0].make(process.pid) }).where("taskId", "=", f.work.taskId).returning("taskId")));
    await runner.close(); runner = await f.open();
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.state, RunnerStateEnum.HELD); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.occupied, 1);
    assert.partialDeepStrictEqual(await runner.call(Commands.acceptTask, two), { accepted: false, state: RunnerRejectionStateEnum.full });
    await assert.rejects(f.open());
    assert.partialDeepStrictEqual(await runner.call(Commands.journalTask, { taskId: TaskId.make("missing") }), { accepted: false, state: RunnerRejectionStateEnum.notAccepted });
    await runner.close();
    const expired = { ...f.work, taskId: TaskId.make("expired"), deadline: 12345 }; await f.save(expired);
    const expiredDatabaseUrl = await f.run(provisionPostgres());
    const expiredConfig = { ...f.config, stateDirectory: f.join(f.root, "expired-state"), childUidBase: UserId.make(2000000) };
    try {
      runner = await f.open(expiredConfig, common, expiredDatabaseUrl);
      await assert.rejects(runner.call(Commands.acceptTask, expired));
      assert.equal((await f.run(runner.service.row(expired.taskId))), undefined);
      const fingerprint = await f.run(f.platform.fingerprint(expired));
      await f.run(runner.service.database.query(client => client.insertInto("runner_tasks").values({ taskId: expired.taskId, fingerprint, work: Schema.encodeSync(Schema.fromJsonString(Task.fields.request))(expired), worktree: f.join(f.config.childDirectory, "expired", "repo"), state: RunnerStateEnum.ACCEPTED, occupied: 1, incarnation: RunnerTask.fields.incarnation.make("expired-incarnation") }).returning("taskId")));
      await runner.close(); runner = await f.open(expiredConfig, common, expiredDatabaseUrl);
      await f.run(Effect.gen(function*() {
        for (;;) { const row = yield* runner.service.row(expired.taskId); if (row?.state === RunnerStateEnum.FAILED) return; yield* Effect.sleep("10 millis"); }
      }).pipe(Effect.timeout("10 seconds")));
      assert.equal((await f.run(runner.service.row(expired.taskId)))!.pid, null);
    } finally {
      await runner.close();
      await f.run(dropPostgres(expiredDatabaseUrl));
    }
  } finally { await f.close(); }
});
it("failed SDK discovery drains its child before releasing capacity and remains held without replay after restart", async () => {
  const f = await fixture(1, "acquisition-discovery");
  try {
    let runner = await f.open();
    const waitForHeldCapacityRelease = (taskId: TaskId) => f.run(Effect.gen(function*() {
      for (;;) {
        const row = yield* runner.service.row(taskId);
        if (row?.state === RunnerStateEnum.HELD && row.occupied === 0) return row;
        yield* Effect.sleep("10 millis");
      }
    }).pipe(Effect.timeout("10 seconds")));
    await runner.call(Commands.acceptTask, f.work);
    const original = await waitForHeldCapacityRelease(f.work.taskId);
    assert.equal(original.pid, null);
    assert.equal(original.agentBinding, null);
    assert.equal(original.promptId, null);
    const marker = f.join(f.config.childDirectory, await f.run(f.platform.digest(f.work.taskId)), "home", "acquisition.pid");
    const observedPid = await f.run(f.fs.readFileString(marker));
    const pid = Schema.decodeSync(Schema.fromJsonString(Schema.Int))(observedPid);
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    await runner.close();
    runner = await f.open();
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.state, RunnerStateEnum.HELD);
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.occupied, 0);
    const receipt = await runner.call(Commands.journalTask, { taskId: f.work.taskId });
    assert.deepEqual(await runner.call(Commands.acceptTask, f.work), receipt);
    const next = { ...f.work, taskId: TaskId.make("after-failed-discovery") };
    await f.save(next);
    assert.partialDeepStrictEqual(await runner.call(Commands.acceptTask, next), { accepted: true });
    await waitForHeldCapacityRelease(next.taskId);
    assert.equal(await f.run(f.fs.readFileString(marker)), observedPid, "Held launch intent must not be replayed");
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally {
    await f.close();
  }
});

it.each(["admission", "prompt reservation", "committed prompt intent"])("deadline expiry during %s never dispatches expired work", async (phase) => {
    const f = await fixture();
    onTestFinished(() => f.close());
    const runner = await f.open();
    const originalTransaction = runner.service.database.transaction;
    const attempted = await f.run(Deferred.make<void>()), unlock = await f.run(Deferred.make<void>());
    let outcome: Promise<number> | undefined;
    let commandTime = f.work.deadline - 1;
    try {
      let address: Address | undefined;
      if (phase !== "admission") {
        await runner.call(Commands.acceptTask, f.work);
        address = Schema.decodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })((await f.ready(runner.service)).address!);
        await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: DispositionEnum.ready });
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
          // Keep the real PostgreSQL owner permit/BEGIN until the waiting command has crossed its deadline.
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
      Object.defineProperty(runner.service.database, "transaction", { value: transaction, configurable: true, writable: true });
      const input = phase === "admission" ? request(Commands.acceptTask.name, f.work) : request(Commands.releaseTask.name, { taskId: f.work.taskId, address, prompt: f.work.prompt });
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
        assert.equal(row!.state, phase === "committed prompt intent" ? RunnerStateEnum.HELD : RunnerStateEnum.READY);
        if (phase === "committed prompt intent") assert.ok(row!.promptId);
        else assert.equal(row!.promptId, null);
        assert.equal(row!.occupied, 1);
      }
    } finally {
      await f.run(Deferred.succeed(unlock, undefined));
      if (outcome) await outcome;
      Object.defineProperty(runner.service.database, "transaction", { value: originalTransaction, configurable: true, writable: true });
    }
});

it("enrollment bearer is health-only and control bearer cannot invent broker work or confirmation", async () => {
  const f = await fixture();
  try {
    const runner = await f.open();
    await assert.rejects(f.open({ ...f.config, controlTokenHash: await f.run(f.platform.digest(enrollment)) }));
    const url = HttpServer.formatAddress(runner.service.server.address);
    assert.equal(await f.run(f.client.execute(HttpClientRequest.post(`${url}/mcp`).pipe(HttpClientRequest.bodyText(Schema.encodeSync(Schema.fromJsonString(JsonValue))({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} }, name: "journal", arguments: { taskId: TaskId.make("one") } } }), "application/json"), HttpClientRequest.setHeaders({ accept: "application/json, text/event-stream", "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": "journal" }))).pipe(Effect.map((response) => response.status), Effect.scoped)), 401);
    assert.equal(await f.run(boundaryStatus(f.rpc.call(url, "not-an-enrolled-bearer", Commands.journalTask, { taskId: f.work.taskId }))), 401);
    const address = Schema.decodeUnknownSync(Address, { onExcessProperty: "error" })({ messageId: "one", revision: 1, workerId: f.work.workerId, sessionId: "session", incarnation: "incarnation", repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: f.work.deadline });
    for (const input of [request(Commands.acceptTask.name, f.work), request(Commands.prepareTask.name, { taskId: TaskId.make("one"), address }), request(Commands.releaseTask.name, { taskId: TaskId.make("one"), address, prompt: f.work.prompt }), request(Commands.inspectTask.name, { taskId: TaskId.make("one") }), request(Commands.commandTask.name, { taskId: TaskId.make("one"), type: RunnerControlEnum.getState }), request(Commands.journalTask.name, { taskId: TaskId.make("one") })]) {
      const denied = RunnerRequest.match(input, {
        [Commands.acceptTask.name]: ({ params }: typeof Commands.acceptTask.request.Type) => f.rpc.call(url, enrollment, Commands.acceptTask, params),
        [Commands.prepareTask.name]: ({ params }: typeof Commands.prepareTask.request.Type) => f.rpc.call(url, enrollment, Commands.prepareTask, params),
        [Commands.releaseTask.name]: ({ params }: typeof Commands.releaseTask.request.Type) => f.rpc.call(url, enrollment, Commands.releaseTask, params),
        [Commands.inspectTask.name]: ({ params }: typeof Commands.inspectTask.request.Type) => f.rpc.call(url, enrollment, Commands.inspectTask, params),
        [Commands.commandTask.name]: ({ params }: typeof Commands.commandTask.request.Type) => f.rpc.call(url, enrollment, Commands.commandTask, params),
        [Commands.journalTask.name]: ({ params }: typeof Commands.journalTask.request.Type) => f.rpc.call(url, enrollment, Commands.journalTask, params),
      });
      assert.equal(await f.run(boundaryStatus(denied)), 401);
      assert.equal(await f.run(boundaryStatus(runner.service.handle(input, enrollment))), 401);
    }
    assert.equal(await runner.status(Commands.acceptTask, { ...f.work, taskId: "invented" }), 404);
    await f.save(f.work, { runnerId: RunnerId.make("other-runner") }); assert.equal(await runner.status(Commands.acceptTask, f.work), 403);
    await f.save(f.work, { fingerprint: RunnerTask.fields.fingerprint.make("0".repeat(64)) }); await assert.rejects(runner.call(Commands.acceptTask, f.work));
    await f.save(f.work, { request: { ...f.work, prompt: "unapproved ledger prompt" } }); await assert.rejects(runner.call(Commands.acceptTask, f.work));
    await f.save(f.work); await runner.call(Commands.acceptTask, f.work); const owned = await f.ready(runner.service); const actual = Schema.decodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(owned.address!);
    const release = () => runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address: actual, prompt: f.work.prompt });
    await f.save(f.work); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: { ...actual, revision: 2 }, confirmedRevision: 1, disposition: DispositionEnum.ready }); await assert.rejects(release());
    await f.save(f.work, { address: actual, readback: actual, confirmedRevision: 2, disposition: DispositionEnum.ready }); await assert.rejects(release());
    await f.save(f.work, { runnerId: RunnerId.make("other-runner"), address: actual, readback: actual, confirmedRevision: 1, disposition: DispositionEnum.ready }); assert.equal(await runner.status(Commands.releaseTask, { taskId: TaskId.make("one"), address: actual, prompt: f.work.prompt }), 403);
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.promptId, null);
  } finally { await f.close(); }
});

it("agent event and result projection omits secret content from durable journal and inspect", async () => {
  const f = await fixture();
  try {
    let runner = await f.open(); await runner.call(Commands.acceptTask, f.work); await f.ready(runner.service);
    const secret = "native-secret-provider-token-DO-NOT-STORE";
    const row = (await f.run(runner.service.row(f.work.taskId)))!;
    const address = Schema.decodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(row.address!);
    await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: DispositionEnum.ready });
    const privateMessages = Schema.decodeUnknownSync(AgentMessagesPage, { onExcessProperty: "error" })(await runner.call(Commands.commandTask, { taskId: TaskId.make("one"), type: RunnerControlEnum.getMessages, limit: 1 }));
    assert.equal(JSON.stringify(privateMessages).includes(secret), true);
    assert.equal(privateMessages.messages.length, 1);
    assert.equal(privateMessages.more, true);
    assert.equal(privateMessages.nextCursor, "1");
    assert.ok(typeof privateMessages.nextCursor === "string");
    const identityPage = Schema.decodeUnknownSync(AgentMessagesPage, { onExcessProperty: "error" })(await runner.call(Commands.commandTask, { taskId: TaskId.make("one"), type: RunnerControlEnum.getMessages, cursor: privateMessages.nextCursor, limit: 1 }));
    assert.deepEqual(identityPage.binding, privateMessages.binding);
    assert.equal(identityPage.messages.length, 1);
    assert.partialDeepStrictEqual(identityPage.messages[0], { content: "λ ✓", fixtureIdentity: { uid: row.childUid, effectiveUid: row.childUid, cwd: row.worktree, capabilityEffective: "0000000000000000", noNewPrivileges: "1" } });
    assert.equal(identityPage.more, false);
    assert.equal(identityPage.nextCursor, null);
    await assert.rejects(runner.call(Commands.commandTask, { taskId: TaskId.make("one"), type: RunnerControlEnum.getMessages, cursor: "not-a-cursor" }));
    assert.throws(() => request(Commands.commandTask.name, { taskId: TaskId.make("one"), type: RunnerControlEnum.getMessages, limit: 257 }));
    assert.throws(() => request(Commands.commandTask.name, { taskId: TaskId.make("one"), type: RunnerControlEnum.getState, message: "private" }));
    await runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt });
    await f.run(Effect.gen(function*() {
      for (;;) { const current = yield* runner.service.row(f.work.taskId); if (current?.state === RunnerStateEnum.SUCCEEDED) return; if (current?.state === RunnerStateEnum.HELD || current?.state === RunnerStateEnum.FAILED) return yield* Effect.fail(new CommandError({ message: "Fixture prompt did not settle" })); yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("15 seconds")));
    const events = await f.run(runner.service.database.query(client => client.selectFrom("runner_events").select("payload")));
    const originalIdentities = events.map((event) => Schema.decodeSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload)))
      .filter((event) => event.type === "readback" || event.type === "agent_lifecycle")
      .map(({ type, address, binding, runId, agentCursor, kind }) => ({ type, address, binding, runId, agentCursor, kind }));
    assert.ok(originalIdentities.some((event) => event.type === "readback" && event.address));
    assert.ok(originalIdentities.some((event) => event.type === "agent_lifecycle" && event.binding));
    assert.ok(events.some((event) => Schema.decodeSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(String(event.payload)).kind === "run_settled"));
    assert.equal(JSON.stringify(events).includes(secret), false);
    assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })((await f.run(runner.service.row(f.work.taskId)))!.result!), { hostSettled: true, evidenceVerified: false });
    assert.equal(Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(await runner.call(Commands.inspectTask, { taskId: TaskId.make("one") })).includes(secret), false);
    await runner.close(); runner = await f.open();
    const migratedIdentities = (await f.run(runner.service.database.query(client => client.selectFrom("runner_events").select("payload"))))
      .map((event) => Schema.decodeSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(event.payload))
      .filter((event) => event.type === "readback" || event.type === "agent_lifecycle")
      .map(({ type, address, binding, runId, agentCursor, kind }) => ({ type, address, binding, runId, agentCursor, kind }));
    assert.deepEqual(migratedIdentities, originalIdentities);
    assert.equal(JSON.stringify(await f.run(runner.service.database.query(client => client.selectFrom("runner_events").select("payload")))).includes(secret), false);
    assert.equal(JSON.stringify(await f.run(runner.service.database.query(client => client.selectFrom("runner_tasks").select(["result", "reason"])))).includes(secret), false);
  } finally { await f.close(); }
});


it("obsolete handed-off report reconciles durably without starving later live reports", async () => {
  const f = await fixture(2);
  try {
    let runner = await f.open(); const live = { ...f.work, taskId: TaskId.make("two") }; await f.save(live);
    await runner.call(Commands.acceptTask, f.work); await runner.call(Commands.acceptTask, live); await f.ready(runner.service); await f.ready(runner.service, live.taskId);
    await f.run(runner.service.database.query(client => client.updateTable("runner_tasks").set({ state: RunnerStateEnum.RUNNING, promptId: AgentSubmissionId.make("reserved"), pid: RunnerTask.fields.pid.members[0].make(process.pid) }).where("taskId", "=", f.work.taskId).returning("taskId")));
    await f.run(runner.service.database.query(client => client.updateTable("runner_tasks").set({ state: RunnerStateEnum.SUCCEEDED, occupied: 0, result: "{}" }).where("taskId", "=", live.taskId).returning("taskId")));
    await f.save(f.work, { runnerId: undefined });
    const rows = [(await f.run(runner.service.row(f.work.taskId)))!, (await f.run(runner.service.row(live.taskId)))!];
    const reported = await f.run(runner.service.reports(rows));
    assert.equal(reported.length, rows.length);
    assert.ok(reported.every(result => result._tag === "Success"), "Ownership reconciliation and the later live report both complete");
    const obsolete = (await f.run(runner.service.database.query(client => client.selectFrom("runner_tasks").select(["state", "detached", "occupied", "promptId"]).where("taskId", "=", f.work.taskId))))[0]!;
    assert.deepEqual({ ...obsolete }, { state: RunnerStateEnum.HELD, detached: 1, occupied: 1, promptId: "reserved" });
    const brokerDb = f.broker.database;
    assert.equal(Schema.decodeSync(Schema.fromJsonString(Task), { onExcessProperty: "error" })((await f.run(brokerDb.query(client => client.selectFrom("tasks").select("data").where("id", "=", live.taskId))))[0]!.data).state, TaskStateEnum.SUCCEEDED);
    await runner.close(); runner = await f.open(); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.detached, 1); assert.equal((await f.run(runner.service.row(f.work.taskId)))!.occupied, 1);
    await f.save(live, { state: TaskStateEnum.FAILED });
    const regression = await f.run(runner.service.reports([(await f.run(runner.service.row(live.taskId)))!]));
    assert.equal(regression.length, 1);
    assert.equal(regression[0]!._tag, "Failure");
    if (regression[0]!._tag === "Failure") {
      assert.ok(regression[0]!.failure instanceof HttpError);
      assert.equal(regression[0]!.failure.status, 409, "Terminal report regression remains an observable typed failure");
    }
    assert.equal((await f.run(runner.service.row(live.taskId)))!.detached, 0);
    f.outage(true);
    const unavailable = await f.run(runner.service.reports([(await f.run(runner.service.row(live.taskId)))!]));
    assert.equal(unavailable.length, 1);
    assert.equal(unavailable[0]!._tag, "Failure");
    if (unavailable[0]!._tag === "Failure") {
      assert.ok(unavailable[0]!.failure instanceof HttpError);
      assert.equal(unavailable[0]!.failure.status, 503, "Broker outage remains an observable typed failure");
    }
    assert.equal((await f.run(runner.service.row(live.taskId)))!.detached, 0);
  } finally { await f.close(); }
});

it("ownership lost before durable admission refuses acceptance and pending duplicates never expose not-accepted", async () => {
  const f = await fixture();
  try {
    const runner = await f.open(), gate = await f.fence();
    const acceptance = runner.call(Commands.acceptTask, f.work), duplicate = runner.call(Commands.acceptTask, f.work);
    // Observe the actual second broker read, not monkey-patched runner internals.
    await f.run(Deferred.await(gate.reached).pipe(Effect.timeout("10 seconds")));
    await assert.rejects(runner.call(Commands.journalTask, { taskId: TaskId.make("one") }));
    await assert.rejects(runner.call(Commands.acceptTask, { ...f.work, prompt: "conflict" }));
    await f.save(f.work, { runnerId: RunnerId.make("reassigned-runner"), state: TaskStateEnum.ASSIGNING });
    const refused = Promise.all([
      assert.rejects(acceptance, (error: unknown) => error instanceof HttpError && error.status === 403),
      assert.rejects(duplicate, (error: unknown) => error instanceof HttpError && error.status === 403),
    ]);
    await f.run(Deferred.succeed(gate.proceed, undefined)); await refused;
    const refusedJournal = await runner.call(Commands.journalTask, { taskId: TaskId.make("one") });
    assert.ok(refusedJournal);
    assert.equal(refusedJournal.accepted, false); assert.equal(refusedJournal.state, RunnerRejectionStateEnum.notAccepted);
    assert.equal((await f.run(runner.service.database.query(client => client.selectFrom("runner_tasks").select("taskId")))).length, 0);
  } finally { await f.close(); }
});

it("runner refuses overlapping child roots and model seeds containing session history", async () => {
  const f = await fixture();
  try {
    await assert.rejects(f.open({ ...f.config, childDirectory: f.root }));
    await f.run(f.fs.writeFileString(f.join(f.config.modelDirectory, "history.db"), "sensitive history", { mode: 0o600 }));
    await assert.rejects(f.open());
  } finally { await f.close(); }
});

it.each(["missing-admission", "gap", "transport", "invalid-event"])("native %s holds durable intent without replay", async (mode) => {
    // Fixed, metadata-only boundaries remain visible even when disposal never resolves.
    const phase = (name: string) => process.stderr.write(`[native-held:${mode}] ${name} ${Date.now()}\n`);
    phase("fixture.open.start");
    const f = await fixture(1, mode, phase);
    phase("fixture.open.done");
    try {
      let runner = await f.open();
      phase("runner.open.done");
      phase("accept.start");
      await runner.call(Commands.acceptTask, f.work);
      phase("accept.done");
      phase("ready.start");
      const ready = await f.ready(runner.service);
      phase("ready.done");
      assert.ok(ready.pid, "READY owns a real native child");
      const address = Schema.decodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(ready.address!);
      await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: DispositionEnum.ready });
      phase("release.start");
      await assert.rejects(runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt }));
      phase("release.rejected");
      const held = (await f.run(runner.service.row(f.work.taskId)))!;
      assert.equal(held.state, RunnerStateEnum.HELD);
      assert.ok(held.promptId);
      assert.equal(held.result, null, "Unproven terminal work has no persisted result");
      phase("duplicate.release.start");
      await assert.rejects(runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt }));
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
      assert.equal(recovered.state, RunnerStateEnum.HELD);
      assert.equal(recovered.promptId, held.promptId);
      assert.equal(recovered.agentBinding, held.agentBinding);
      assert.equal(recovered.result, null);
      assert.throws(() => process.kill(ready.pid!, 0), { code: "ESRCH" }, "Recovery must not replay the old native child");
      phase("recovered.release.start");
      await assert.rejects(runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt }));
      phase("recovered.release.rejected");
    } finally {
      phase("fixture.close.start");
      await f.close();
      assert.equal(existsSync(f.root), false, "All fixture scopes close before the private root is removed");
      phase("fixture.close.done");
    }
});

it("stale native generation cannot establish broker readiness", async () => {
  const f = await fixture(1, "stale");
  try {
    const runner = await f.open();
    await runner.call(Commands.acceptTask, f.work);
    await assert.rejects(f.ready(runner.service));
    const row = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(row.state, RunnerStateEnum.HELD);
    assert.equal(row.address, null);
    assert.equal(row.promptId, null);
  } finally { await f.close(); }
});

it("abort acknowledgement and idle snapshots cannot replace a missing whole-run terminal proof", async () => {
  const f = await fixture(1, "missing-terminal");
  try {
    let runner = await f.open();
    await runner.call(Commands.acceptTask, f.work);
    const ready = await f.ready(runner.service);
    const address = Schema.decodeSync(Schema.fromJsonString(Address), { onExcessProperty: "error" })(ready.address!);
    await f.save(f.work, { address, readback: address, confirmedRevision: address.revision, disposition: DispositionEnum.ready });
    await runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt });
    const admitted = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(admitted.state, RunnerStateEnum.RUNNING);
    assert.ok(admitted.agentRunId);
    const snapshot = await runner.call(Commands.commandTask, { taskId: TaskId.make("one"), type: RunnerControlEnum.getState });
    assert.partialDeepStrictEqual(snapshot, { currentRunId: null, isStreaming: false });
    assert.equal((await f.run(runner.service.row(f.work.taskId)))!.state, RunnerStateEnum.RUNNING);
    const abort = await runner.call(Commands.commandTask, { taskId: TaskId.make("one"), type: RunnerControlEnum.abort });
    assert.partialDeepStrictEqual(abort, { disposition: "already_settled", runId: null });
    const held = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(held.state, RunnerStateEnum.HELD);
    assert.equal(held.occupied, 1);
    assert.equal(held.result, null);
    assert.equal((await f.run(runner.service.database.query(client => client.selectFrom("runner_tasks").select("result").where("taskId", "=", f.work.taskId))))[0]!.result, null);
    if (held.pid === null) assert.fail("Held physical ownership must retain the native child PID");
    process.kill(held.pid, "SIGTERM");
    await f.run(Effect.gen(function*() {
      for (;;) { if ((yield* runner.service.row(f.work.taskId))?.occupied === 0) return; yield* Effect.sleep("10 millis"); }
    }).pipe(Effect.timeout("15 seconds")));
    const closed = (await f.run(runner.service.row(f.work.taskId)))!;
    assert.equal(closed.state, RunnerStateEnum.HELD);
    assert.equal(closed.promptId, admitted.promptId);
    assert.equal(closed.agentRunId, admitted.agentRunId);
    await assert.rejects(runner.call(Commands.releaseTask, { taskId: TaskId.make("one"), address, prompt: f.work.prompt }));
  } finally {
    await f.close();
  }
});

it("public-hook-only capabilities hold an inspectable child without creating prompt intent", async () => {
  const f = await fixture(1, "public-hooks");
  try {
    const runner = await f.open();
    await runner.call(Commands.acceptTask, f.work);
    const held = await f.run(Effect.gen(function*() {
      for (;;) {
        const row = yield* runner.service.row(f.work.taskId);
        if (row?.state === RunnerStateEnum.HELD) return row;
        yield* Effect.sleep("10 millis");
      }
    }).pipe(Effect.timeout("15 seconds")));
    assert.equal(held.promptId, null);
    assert.equal(held.agentRunId, null);
    assert.equal(held.address, null);
    assert.equal(held.occupied, 1);
    assert.equal(held.result, null);
    const state = Schema.decodeUnknownSync(RunnerAgentState)(await runner.call(Commands.commandTask, { taskId: f.work.taskId, type: RunnerControlEnum.getState }));
    assert.equal(state.binding.host, AgentHostEnum.omp);
    const address = Schema.decodeUnknownSync(Address)({ messageId: f.work.taskId, revision: 1, workerId: f.work.workerId,
      sessionId: state.binding.sessionId, incarnation: held.incarnation, repo: f.work.repo, ref: f.work.ref, sha: f.work.sha,
      scope: f.work.scope, restrictions: f.work.restrictions, expectedEvidence: f.work.expectedEvidence, deadline: f.work.deadline });
    assert.equal(await runner.status(Commands.releaseTask, { taskId: f.work.taskId, address, prompt: f.work.prompt }), 409);
    assert.equal((await f.run(runner.service.row(f.work.taskId)))?.promptId, null);
  } finally { await f.close(); }
});
