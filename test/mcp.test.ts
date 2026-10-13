import test from "node:test";
import assert from "node:assert/strict";
import { createServer, request as httpRequest, type IncomingMessage } from "node:http";
import { NodeHttpServer } from "@effect/platform-node";
import { Context, Deferred, Effect, Layer, ManagedRuntime } from "effect";
import { HttpServer } from "effect/http";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { Acceptance, HttpError, TaskId, type CommandInput } from "../src/contracts.ts";
import { Mcp, serverLayer } from "../src/http.ts";

test("Mcp.call closes its real HTTP 401 body before releasing the body or disposing its layer", { timeout: 5000 }, async () => {
  const releaseBody = await Effect.runPromise(Deferred.make<void>());
  const disconnected = Promise.withResolvers<void>();
  let bodyReleased = false;
  const nativeServer = createServer((_request, response) => {
    response.once("close", disconnected.resolve);
    response.writeHead(401, { "content-type": "text/plain" });
    response.write("Credential required");
    void Effect.runPromise(Deferred.await(releaseBody)).then(() => {
      bodyReleased = true;
      response.end();
    });
  });
  const runtime = ManagedRuntime.make(Mcp.layer);
  try {
    await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
    const address = nativeServer.address();
    assert.ok(address !== null && typeof address !== "string");
    const mcp = await runtime.runPromise(Mcp);
    const status = await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("physical-401") }).pipe(Effect.match({
      onSuccess: () => 200,
      onFailure: (error) => { assert.ok(error instanceof HttpError); return error.status; },
    })));
    assert.equal(status, 401);
    await Effect.runPromise(Effect.promise(() => disconnected.promise).pipe(Effect.timeout("1 second")));
    assert.equal(bodyReleased, false, "The peer closes while the server body remains blocked and Mcp.layer remains alive");
  } finally {
    await Effect.runPromise(Deferred.succeed(releaseBody, undefined));
    nativeServer.closeAllConnections();
    await runtime.dispose();
    await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
  }
});

test("Mcp.call timeout closes its real HTTP streaming body before disposing its layer", { timeout: 5000 }, async () => {
  const releaseBody = await Effect.runPromise(Deferred.make<void>());
  const disconnected = Promise.withResolvers<void>();
  let bodyReleased = false;
  const nativeServer = createServer((_request, response) => {
    response.once("close", disconnected.resolve);
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(": waiting\n\n");
    void Effect.runPromise(Deferred.await(releaseBody)).then(() => {
      bodyReleased = true;
      response.end();
    });
  });
  const runtime = ManagedRuntime.make(Mcp.layer);
  try {
    await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
    const address = nativeServer.address();
    assert.ok(address !== null && typeof address !== "string");
    const mcp = await runtime.runPromise(Mcp);
    const result = await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("physical-timeout") }, 250).pipe(Effect.exit));
    assert.equal(result._tag, "Failure");
    await Effect.runPromise(Effect.promise(() => disconnected.promise).pipe(Effect.timeout("1 second")));
    assert.equal(bodyReleased, false, "Timeout closes the peer without server EOF or layer disposal");
  } finally {
    await Effect.runPromise(Deferred.succeed(releaseBody, undefined));
    nativeServer.closeAllConnections();
    await runtime.dispose();
    await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
  }
});

test("stateless MCP calls authorize every HTTP request and close without retaining sessions", { timeout: 15000 }, async () => {
  let handled = 0;
  const slowEntered = Promise.withResolvers<void>();
  const releaseSlow = Promise.withResolvers<void>();
  const nativeServer = createServer({ requestTimeout: 15000 });
  const exchanges: Array<{ protocol: string | undefined; requestSession: string | undefined; responseSession: string | number | string[] | undefined }> = [];
  nativeServer.on("request", (request, response) => {
    const exchange = {
      protocol: request.headersDistinct["mcp-protocol-version"]?.[0],
      requestSession: request.headersDistinct["mcp-session-id"]?.[0],
      responseSession: response.getHeader("mcp-session-id"),
    };
    exchanges.push(exchange);
    response.once("finish", () => { exchange.responseSession = response.getHeader("mcp-session-id"); });
  });
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
          if (caller === "caller-a" && request.params.taskId === "caller-a") {
            slowEntered.resolve();
            yield* Effect.promise(() => releaseSlow.promise);
          }
          return Acceptance.make({ taskId: request.params.taskId, accepted: false, state: "absent" });
        }),
      });
      return { server, mcp };
    }),
  }) {}
  const runtime = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(
    Mcp.layer,
    NodeHttpServer.layerServer(() => nativeServer, { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" }),
  ))));
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
    assert.equal(await status("", { taskId: TaskId.make("caller-a") }), 401);
    // One admitted caller remains pending while another caller completes on its own SDK connection.
    const slow = status("caller-a", { taskId: TaskId.make("caller-a") });
    await slowEntered.promise;
    try {
      assert.equal(await status("caller-b", { taskId: TaskId.make("caller-b") }), 200);
    } finally {
      releaseSlow.resolve();
    }
    assert.equal(await slow, 200);
    for (const exchange of exchanges) {
      assert.equal(exchange.protocol, "2026-07-28");
      assert.equal(exchange.requestSession, undefined);
      assert.equal(exchange.responseSession, undefined);
    }
    const beforeMalformed = handled;
    const malformed = await fetch(`${url}/mcp`, {
      method: "POST",
      headers: { authorization: "Bearer caller-a", "content-type": "application/json", accept: "application/json, text/event-stream", "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/call", "mcp-name": "journal" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} }, name: "journal", arguments: { taskId: "caller-a", principal: "caller-b" } } }),
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
    releaseSlow.resolve();
    await runtime.dispose();
  }
  assert.equal(nativeServer.listening, false);
});

test("native MCP proxy disposes after its downstream runner closes during a tool call", async () => {
  const entered = Promise.withResolvers<void>();
  let runnerFinalized = false;
  class RunnerEndpoint extends Context.Service<RunnerEndpoint>()("test/McpShutdownRunner", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: () => Effect.succeed("caller"),
        handle: () => Effect.sync(entered.resolve).pipe(
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
    await entered.promise;
    await runner.dispose();
    assert.equal(runnerFinalized, true);
    assert.equal((await request)._tag, "Failure");
  } finally {
    await runner.dispose();
    await proxy.dispose();
  }
});

test("cancelled native MCP subscription releases its response and application scope", { timeout: 15000 }, async () => {
  const nativeServer = createServer({ requestTimeout: 15000 });
  class Endpoint extends Context.Service<Endpoint>()("test/McpSubscriptionShutdown", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: () => Effect.succeed("caller"),
        handle: (_caller, request) => request.method === "journal"
          ? Effect.succeed(Acceptance.make({ taskId: request.params.taskId, accepted: false, state: "absent" }))
          : Effect.fail(new HttpError({ status: 400, message: "Unexpected test command" })),
      });
      return { server, mcp };
    }),
  }) {}
  const runtime = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(
    Mcp.layer,
    NodeHttpServer.layerServer(() => nativeServer, { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" }),
  ))));
  const client = new Client({ name: "incremental-sse-regression", version: "1.0.0" }, {
    versionNegotiation: { mode: { pin: "2026-07-28" } },
  });
  try {
    const { server, mcp } = await runtime.runPromise(Endpoint);
    const url = HttpServer.formatAddress(server.address);
    await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`), {
      requestInit: { headers: { authorization: "Bearer caller" }, redirect: "error" },
    }));
    // The official SDK resolves listen only after parsing the acknowledged SSE event.
    // This must happen while the long-lived response is still open, not at EOF.
    const subscription = await client.listen({ toolsListChanged: true }, { timeout: 3000 });
    assert.equal(subscription.honoredFilter.toolsListChanged, true);
    let ended = false;
    const closed = subscription.closed.then((reason) => { ended = true; return reason; });
    const fenceId = TaskId.make("after-listen-admission");
    const fence = await runtime.runPromise(mcp.call(url, "caller", "journal", { taskId: fenceId }));
    assert.equal(fence.taskId, fenceId);
    assert.equal(ended, false, "First SSE event is delivered before stream closure; independent requests keep progressing");
    await subscription.close();
    assert.equal(await closed, "local");
  } finally {
    await client.close();
    await runtime.dispose();
  }
  assert.equal(nativeServer.listening, false);
});

test("aborted unread MCP body cannot hold HTTP server cleanup", { timeout: 5000 }, async () => {
  const nativeServer = createServer({ requestTimeout: 15000 });
  const incoming = Promise.withResolvers<IncomingMessage>();
  const disconnected = Promise.withResolvers<void>();
  const authorizing = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  nativeServer.once("request", (request, response) => {
    incoming.resolve(request);
    response.once("close", disconnected.resolve);
  });
  class Endpoint extends Context.Service<Endpoint>()("test/McpUnreadBodyShutdown", {
    make: Effect.gen(function*() {
      const mcp = yield* Mcp;
      const server = yield* HttpServer.HttpServer;
      yield* mcp.serve(server, {
        target: "runner",
        authorize: () => Effect.promise(async () => { authorizing.resolve(); await proceed.promise; return "caller"; }),
        handle: () => Effect.fail(new HttpError({ status: 400, message: "Unexpected test command" })),
      });
      return { server };
    }),
  }) {}
  const runtime = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(
    Mcp.layer,
    NodeHttpServer.layerServer(() => nativeServer, { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" }),
  ))));
  const { server } = await runtime.runPromise(Endpoint);
  const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: { _meta: { "io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {} } } });
  const request = httpRequest(`${HttpServer.formatAddress(server.address)}/mcp`, {
    method: "POST",
    headers: { authorization: "Bearer caller", "content-type": "application/json", "content-length": Buffer.byteLength(body), accept: "application/json, text/event-stream", "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/list" },
  });
  const clientError = Promise.withResolvers<Error>();
  request.once("error", clientError.resolve);
  try {
    request.end(body);
    await authorizing.promise;
    const received = await incoming.promise;
    while (!received.complete) await new Promise<void>((resolve) => setImmediate(resolve));
    request.destroy(new Error("Client intentionally disconnected before body consumption"));
    await clientError.promise;
    await disconnected.promise;
    assert.equal(received.destroyed, true);
    assert.equal(received.readableEnded, false);
    proceed.resolve();
    await runtime.dispose();
    assert.equal(nativeServer.listening, false);
  } finally {
    request.destroy();
    proceed.resolve();
    await runtime.dispose();
  }
});
