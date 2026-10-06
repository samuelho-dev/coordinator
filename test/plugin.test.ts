import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Clock, ConfigProvider, Context as EffectContext, Deferred, Effect, Layer } from "effect";
import { ChildProcess } from "effect/process";
import { HttpServer } from "effect/http";
import { canonicalJson, decode, Mcp, serverLayer } from "../src/http.ts";
import { Platform } from "../src/platform.ts";
import { Journal } from "../src/journal.ts";
import { CommandError, HttpError, Task, type Context, type BrokerRequest } from "../src/contracts.ts";
import { Plugin } from "../src/plugin.ts";

test("native plugin persists intent before send and holds uncertain delivery after scoped restart", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-plugin-" });
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["-c", "user.name=Coordinator Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "test"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const sha = (yield* platform.exec(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: directory }))).trim();
  const stateDirectory = platform.path.join(directory, "state");
  yield* platform.fs.makeDirectory(stateDirectory, { mode: 0o700 });
  const tokenFile = platform.path.join(directory, "token");
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const config = platform.path.join(directory, "config.json");
  let approval = true;
  const ctx: Context = { cwd: directory, hasUI: true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "native-session", getSessionFile: () => undefined },
    ui: { notify: () => {}, confirm: async () => approval } };
  let sends = 0;
  let incarnation = "";
  const reports: Extract<BrokerRequest, { method: "task.report" }>["params"][] = [];
  let requests = 0;
  const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
  const server = EffectContext.get(serverContext, HttpServer.HttpServer);
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"] as const, repos: ["github.com/example/repo"] }),
    handle: (_principal, request) => Effect.gen(function*() {
      requests++;
      const now = yield* Clock.currentTimeMillis;
      if (request.method === "session.heartbeat") { incarnation = request.params.incarnation; return { liveUntil: now + 30000 }; }
      if (request.method === "session.stop") return { stopped: true };
      const address = { messageId: "message", revision: 1, sessionId: "native-session", incarnation, workerId: "native-session", repo: "github.com/example/repo", ref: "refs/heads/main", sha, scope: "test", restrictions: "none", expectedEvidence: [], deadline: now + 60000 };
      const work = { taskId: "task", prompt: "Substantive task", repo: address.repo, ref: address.ref, sha, launcher: "pi", scope: address.scope, restrictions: address.restrictions, expectedEvidence: [], deadline: address.deadline, workerId: address.workerId, githubRepoId: 1, requiredLanes: [], bypassPeer: false, approved: true };
      const task = decode(Task, { taskId: "task", owner: "owner", fingerprint: yield* platform.fingerprint(work), state: "ACCEPTED", createdAt: now, updatedAt: now, address, readback: address, confirmedRevision: 1, disposition: "ready", request: work });
      if (request.method === "session.poll") return { messages: [], offers: [], releases: [task] };
      if (request.method === "task.report") {
        const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, "native-session").pipe(Layer.provide(Layer.succeed(Platform, platform)))), Journal);
        assert.equal(persisted.state.releases["task:1"], "intent");
        reports.push(request.params);
        return { ...task, state: request.params.state, result: request.params.result };
      }
      return yield* Effect.fail(new CommandError({ message: `Unexpected route ${request.method}` }));
    }).pipe(Effect.scoped),
  });
  if (!("port" in server.address)) return yield* Effect.fail(new CommandError({ message: "Expected TCP server" }));
  yield* platform.fs.writeFileString(config, canonicalJson({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const pluginContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => { sends++; throw new CommandError({ message: "native delivery uncertain" }); } }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
  ));
  const plugin = EffectContext.get(pluginContext, Plugin);
  if (!plugin) return yield* Effect.fail(new CommandError({ message: "Expected configured plugin" }));
  assert.equal((yield* Effect.exit(plugin.start(ctx)))._tag, "Failure");
  assert.equal(sends, 1);
  const persisted = EffectContext.get(yield* Layer.build(Journal.layer(stateDirectory, "native-session")), Journal);
  assert.equal(persisted.state.releases["task:1"], "intent");
  yield* plugin.stop();
  yield* plugin.start(ctx);
  const priorRequests = requests;
  yield* plugin.command('report {"taskId":"task","state":"HELD","result":{},"sessionId":"forged"}', ctx);
  assert.equal(requests, priorRequests, "Catalog rejects model-supplied session identity before remote execution");
  approval = false;
  yield* plugin.command('confirm {"taskId":"task","revision":1}', ctx);
  yield* plugin.command('control {"taskId":"task","type":"get_messages"}', ctx);
  yield* plugin.command('control {"taskId":"task","type":"abort"}', ctx);
  assert.equal(requests, priorRequests, "Declined native approval must not reach the broker");
  assert.equal(sends, 1);
  assert.equal(reports[0]?.state, "HELD");
  yield* plugin.stop();
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));

test("a refused startup heartbeat keeps scoped enrollment alive until the prior lease fence clears", () => Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const mcp = yield* Mcp;
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-enrollment-" });
  yield* platform.exec(ChildProcess.make("git", ["init", "-b", "main"], { cwd: directory }));
  yield* platform.exec(ChildProcess.make("git", ["remote", "add", "origin", "https://github.com/example/repo.git"], { cwd: directory }));
  const stateDirectory = platform.path.join(directory, "state");
  const tokenFile = platform.path.join(directory, "token");
  const config = platform.path.join(directory, "config.json");
  yield* platform.fs.writeFileString(tokenFile, "test-token", { mode: 0o600 });
  const serverContext = yield* Layer.build(serverLayer("127.0.0.1", 0));
  const server = EffectContext.get(serverContext, HttpServer.HttpServer);
  const polled = yield* Deferred.make<void>();
  let heartbeatAttempts = 0;
  let polls = 0;
  yield* mcp.serve(server, { target: "broker",
    authorize: () => Effect.succeed({ id: "session", owner: "owner", tokenHash: "a".repeat(64), roles: ["session"], repos: ["github.com/example/repo"] }),
    handle: (_principal, request) => Effect.gen(function*() {
      if (request.method === "session.heartbeat") {
        heartbeatAttempts++;
        if (heartbeatAttempts === 1) return yield* new HttpError({ status: 409, message: "Prior session incarnation still live" });
        return { liveUntil: (yield* Clock.currentTimeMillis) + 30000 };
      }
      if (request.method === "session.poll") {
        polls++;
        yield* Deferred.succeed(polled, undefined);
        return { messages: [], offers: [], releases: [] };
      }
      if (request.method === "session.stop") return { stopped: true };
      return yield* Effect.fail(new CommandError({ message: `Unexpected route ${request.method}` }));
    }),
  });
  if (!("port" in server.address)) return yield* Effect.fail(new CommandError({ message: "Expected TCP server" }));
  yield* platform.fs.writeFileString(config, canonicalJson({ brokerUrl: `http://127.0.0.1:${server.address.port}`, tokenFile, stateDirectory, machineId: "machine", host: "pi" }), { mode: 0o600 });
  const ctx: Context = { cwd: directory, hasUI: true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "native-session", getSessionFile: () => undefined },
    ui: { notify: () => {}, confirm: async () => true } };
  const pluginContext = yield* Layer.build(Plugin.layer({ on: () => {}, registerCommand: () => {}, sendUserMessage: () => { assert.fail("Enrollment must not admit a prompt"); } }).pipe(
    Layer.provide(Layer.merge(Layer.succeed(Platform, platform), Layer.succeed(Mcp, mcp))),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_CONFIG: config }))),
  ));
  const plugin = EffectContext.get(pluginContext, Plugin);
  if (!plugin) return yield* Effect.fail(new CommandError({ message: "Expected configured plugin" }));
  assert.equal((yield* Effect.exit(plugin.start(ctx)))._tag, "Failure");
  assert.equal(heartbeatAttempts, 1);
  yield* Deferred.await(polled).pipe(Effect.timeout("5 seconds"));
  assert.equal(heartbeatAttempts, 2);
  yield* plugin.stop();
  const stoppedPolls = polls;
  yield* Effect.sleep("1200 millis");
  assert.equal(polls, stoppedPolls, "Closing the session scope must cancel periodic polling");
})).pipe(Effect.provide(Layer.merge(Platform.layer, Mcp.layer)))));
