import test from "node:test";
import assert from "node:assert/strict";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { HttpServer } from "effect/http";
import { HttpError } from "../src/contracts.ts";
import { Mcp, serverLayer } from "../src/http.ts";

test("native MCP authorizes each tool with its HTTP bearer, not session or model metadata", async () => {
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
          if (request.method !== "journal" || request.params.taskId !== caller) return yield* new HttpError({ status: 403, message: "Task belongs to another caller" });
          return { taskId: caller, state: "absent" };
        }),
      });
      return { server, mcp };
    }),
  }) {}
  const runtime = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(Mcp.layer, serverLayer("127.0.0.1", 0)))));
  try {
    const { server, mcp } = await runtime.runPromise(Endpoint);
    const url = HttpServer.formatAddress(server.address);
    const status = (token: string, params: unknown) => runtime.runPromise(mcp.call(url, token, "journal", params).pipe(Effect.match({
      onSuccess: () => 200,
      onFailure: (error) => { assert.ok(error instanceof HttpError); return error.status; },
    })));
    assert.equal(await status("caller-a", { taskId: "caller-b" }), 403);
    assert.equal(await status("caller-b", { taskId: "caller-a" }), 403);
    assert.equal(await status("not-enrolled", { taskId: "caller-a" }), 401);
    assert.equal(await status("caller-a", { taskId: "caller-a", principal: "caller-b" }), 400);
  } finally {
    await runtime.dispose();
  }
});
