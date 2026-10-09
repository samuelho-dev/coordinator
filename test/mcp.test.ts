import test from "node:test";
import assert from "node:assert/strict";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { HttpServer } from "effect/http";
import { Acceptance, HttpError, TaskId, type CommandInput } from "../src/contracts.ts";
import { Mcp, serverLayer } from "../src/http.ts";

test("native MCP authorizes each tool with its HTTP bearer, not session or model metadata", async () => {
  let handled = 0;
  class Endpoint extends Context.Service<Endpoint>()("test/McpEndpoint", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: (token) => token === "caller-a" || token === "caller-b"
          ? Effect.succeed(token)
          : Effect.fail(new HttpError({ status: 401, message: "Credential required" })),
        handle: (caller, request) => Effect.gen(function*() {
          handled++;
          if (request.method !== "journal" || request.params.taskId !== caller) return yield* new HttpError({ status: 403, message: "Task belongs to another caller" });
          return Acceptance.make({ taskId: request.params.taskId, accepted: false, state: "absent" });
        }),
      });
      return { server, mcp };
    }),
  }) {}
  const runtime = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(Mcp.layer, serverLayer("127.0.0.1", 0)))));
  try {
    const { server, mcp } = await runtime.runPromise(Endpoint);
    const url = HttpServer.formatAddress(server.address);
    const status = (token: string, params: CommandInput<"journal">) => runtime.runPromise(mcp.call(url, token, "journal", params).pipe(Effect.match({
      onSuccess: () => 200,
      onFailure: (error) => { assert.ok(error instanceof HttpError); return error.status; },
    })));
    assert.equal(await status("caller-a", { taskId: TaskId.make("caller-b") }), 403);
    assert.equal(await status("caller-b", { taskId: TaskId.make("caller-a") }), 403);
    assert.equal(await status("not-enrolled", { taskId: TaskId.make("caller-a") }), 401);
    assert.equal(await status("caller-a", { taskId: TaskId.make("caller-a") }), 200);
    const beforeMalformed = handled;
    const malformed = await fetch(`${url}/mcp`, {
      method: "POST",
      headers: { authorization: "Bearer caller-a", "content-type": "application/json", accept: "application/json, text/event-stream", "mcp-protocol-version": "2025-11-25" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "journal", arguments: { taskId: "caller-a", principal: "caller-b" } } }),
    });
    if (malformed.status === 200) {
      const failure: unknown = await malformed.json();
      assert.ok(failure !== null && typeof failure === "object");
      assert.ok(("error" in failure && failure.error !== undefined) || ("result" in failure && failure.result !== null && typeof failure.result === "object" && "isError" in failure.result && failure.result.isError === true));
    } else {
      assert.equal(malformed.status, 400);
    }
    assert.equal(handled, beforeMalformed);
  } finally {
    await runtime.dispose();
  }
});

test("native MCP proxy disposes after its downstream runner closes during a tool call", async () => {
  let runnerEntered!: () => void;
  const entered = new Promise<void>((resolve) => { runnerEntered = resolve; });
  let runnerFinalized = false;
  class RunnerEndpoint extends Context.Service<RunnerEndpoint>()("test/McpShutdownRunner", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: () => Effect.succeed("caller"),
        handle: () => Effect.sync(runnerEntered).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Effect.sync(() => { runnerFinalized = true; })),
        ),
      });
      return { server };
    }),
  }) {}
  const runner = ManagedRuntime.make(Layer.effect(RunnerEndpoint, RunnerEndpoint.make).pipe(Layer.provide(Layer.merge(Mcp.layer, serverLayer("127.0.0.1", 0)))));
  const { server: runnerServer } = await runner.runPromise(RunnerEndpoint);
  const runnerUrl = HttpServer.formatAddress(runnerServer.address);
  class ProxyEndpoint extends Context.Service<ProxyEndpoint>()("test/McpShutdownProxy", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: () => Effect.succeed("caller"),
        handle: (_caller, request) => request.method === "journal"
          ? mcp.call(runnerUrl, "caller", "journal", request.params)
          : Effect.fail(new HttpError({ status: 400, message: "Unexpected test command" })),
      });
      return { server, mcp };
    }),
  }) {}
  const proxy = ManagedRuntime.make(Layer.effect(ProxyEndpoint, ProxyEndpoint.make).pipe(Layer.provide(Layer.merge(Mcp.layer, serverLayer("127.0.0.1", 0)))));
  try {
    const { server, mcp } = await proxy.runPromise(ProxyEndpoint);
    const request = proxy.runPromise(mcp.call(HttpServer.formatAddress(server.address), "caller", "journal", { taskId: TaskId.make("closing-runner") }).pipe(Effect.exit));
    await entered;
    await runner.dispose();
    assert.equal(runnerFinalized, true);
    assert.equal((await request)._tag, "Failure");
  } finally {
    await runner.dispose();
    await proxy.dispose();
  }
});
