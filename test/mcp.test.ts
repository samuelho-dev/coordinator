import test from "node:test";
import assert from "node:assert/strict";
import { createServer, request as httpRequest, type IncomingMessage } from "node:http";
import { NodeHttpServer } from "@effect/platform-node";
import { Context, Deferred, Effect, Layer, ManagedRuntime } from "effect";
import { HttpServer } from "effect/http";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { Acceptance, CommandError, HttpError, TaskId, type CommandInput } from "../src/contracts.ts";
import { LIMIT, Mcp, serverLayer } from "../src/http.ts";

test("Mcp.call closes its oversized real HTTP 401 body before releasing the body or disposing its layer", { timeout: 5000 }, async () => {
  const releaseBody = await Effect.runPromise(Deferred.make<void>());
  const disconnected = Promise.withResolvers<void>();
  let bodyReleased = false;
  const nativeServer = createServer((_request, response) => {
    response.once("close", disconnected.resolve);
    response.writeHead(401, { "content-type": "text/plain", "retry-after": "Wed, 14 Oct 2026 00:00:00 GMT", "www-authenticate": `Bearer realm="${"x".repeat(600)}"` });
    response.write("x".repeat(LIMIT + 1));
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
      onFailure: (error) => {
        assert.ok(error instanceof HttpError);
        assert.equal(error.retryAfter, "Wed, 14 Oct 2026 00:00:00 GMT");
        assert.equal(error.wwwAuthenticate?.length, 512);
        assert.equal(error.protocolCode, undefined);
        return error.status;
      },
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

for (const variant of ["stalled-401", "stalled-503", "malformed-discovery"] as const) {
  test(`Mcp.call preserves ${variant} failure instead of a retryable timeout`, { timeout: 5000 }, async () => {
    let posts = 0;
    const disconnected = Promise.withResolvers<void>();
    const nativeServer = createServer((request, response) => {
      posts++;
      request.resume();
      response.once("close", disconnected.resolve);
      if (variant === "malformed-discovery") {
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.write("data: {invalid}\n\n");
      } else {
        response.writeHead(variant === "stalled-401" ? 401 : 503, {
          "content-type": "application/json", "retry-after": "60", "www-authenticate": "Bearer",
        });
        response.write("{");
      }
    });
    const runtime = ManagedRuntime.make(Mcp.layer);
    try {
      await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
      const address = nativeServer.address();
      assert.ok(address !== null && typeof address !== "string");
      const mcp = await runtime.runPromise(Mcp);
      await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("incomplete-response") }, 250).pipe(Effect.match({
        onSuccess: () => assert.fail("Invalid response must fail"),
        onFailure: (error) => {
          if (variant === "malformed-discovery") {
            assert.ok(error instanceof CommandError);
            assert.equal(error.reason, "protocol");
          } else {
            assert.ok(error instanceof HttpError);
            assert.equal(error.status, variant === "stalled-401" ? 401 : 503);
            assert.equal(error.retryAfter, "60");
            assert.equal(error.wwwAuthenticate, "Bearer");
          }
        },
      })));
      await Effect.runPromise(Effect.promise(() => disconnected.promise).pipe(Effect.timeout("1 second")));
      assert.equal(posts, 1, "Failure must not trigger another POST");
    } finally {
      nativeServer.closeAllConnections();
      await runtime.dispose();
      await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
    }
  });
}

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
          if (request.method !== "journal" || request.params.taskId !== caller) return yield* new HttpError({ status: 403, message: "Task belongs to another caller", wwwAuthenticate: 'Bearer scope="journal"' });
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
      onFailure: (error) => {
        assert.ok(error instanceof HttpError);
        if (error.status === 403) assert.equal(error.wwwAuthenticate, 'Bearer scope="journal"');
        return error.status;
      },
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

for (const status of [400, 401, 403, 404, 429, 503]) {
  test(`Mcp.call retains HTTP ${status}, bounded headers and protocol meaning without repeating POST`, { timeout: 5000 }, async () => {
    let posts = 0;
    const challenge = 'Bearer error="insufficient_scope", scope="coordinator"';
    const nativeServer = createServer(async (request, response) => {
      posts++;
      assert.equal(request.method, "POST");
      assert.equal(request.url, "/mcp");
      assert.equal(request.headers.authorization, "Bearer caller");
      assert.equal(request.headers["mcp-protocol-version"], "2026-07-28");
      let body = "";
      for await (const chunk of request) body += chunk;
      const rpc: unknown = JSON.parse(body);
      assert.ok(rpc !== null && typeof rpc === "object" && "id" in rpc);
      response.writeHead(status, { "content-type": "application/json", "retry-after": "17", "www-authenticate": challenge });
      response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, error: {
        code: status === 400 ? -32022 : -32601, message: "Private rejected-body text",
        ...(status === 400 ? { data: { supported: ["2026-07-28"], requested: "2026-07-28" } } : {}),
      } }));
    });
    const runtime = ManagedRuntime.make(Mcp.layer);
    try {
      await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
      const address = nativeServer.address();
      assert.ok(address !== null && typeof address !== "string");
      const mcp = await runtime.runPromise(Mcp);
      await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("typed-rejection") }).pipe(Effect.match({
        onSuccess: () => assert.fail("Rejected RPC must fail"),
        onFailure: (error) => {
          assert.ok(error instanceof HttpError);
          assert.equal(error.status, status);
          assert.equal(error.retryAfter, "17");
          assert.equal(error.wwwAuthenticate, challenge);
          assert.equal(error.protocolCode, status === 400 ? -32022 : -32601);
          assert.equal(error.message.includes("Private"), false);
        },
      })));
      assert.equal(posts, 1);
    } finally {
      nativeServer.closeAllConnections();
      await runtime.dispose();
      await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
    }
  });
}

for (const variant of ["json", "sse", "protocol", "invalid-result", "unknown-json", "unknown-sse", "interrupted", "valid-json", "valid-sse"] as const) {
  test(`Mcp.call preserves ${variant} tool response semantics`, { timeout: 5000 }, async () => {
    let tools = 0;
    const nativeServer = createServer(async (request, response) => {
      let body = "";
      for await (const chunk of request) body += chunk;
      const rpc: unknown = JSON.parse(body);
      assert.ok(rpc !== null && typeof rpc === "object" && "id" in rpc && "method" in rpc);
      if (rpc.method === "server/discover") {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { supportedVersions: ["2026-07-28"], capabilities: { tools: {} } } }));
        return;
      }
      if (rpc.method === "tools/list") {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { resultType: "complete", tools: [{ name: "journal", inputSchema: { type: "object" } }] } }));
        return;
      }
      assert.equal(rpc.method, "tools/call");
      tools++;
      response.writeHead(200, { "content-type": variant === "sse" || variant === "unknown-sse" || variant === "valid-sse" || variant === "interrupted" ? "text/event-stream" : "application/json" });
      if (variant === "interrupted") {
        response.write(": connected\n\n");
        setImmediate(() => response.destroy());
      } else if (variant === "sse") response.end("data: not-json\n\n");
      else if (variant === "json") response.end("not-json");
      else if (variant === "invalid-result") response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { resultType: "complete", content: "invalid" } }));
      else if (variant === "unknown-json" || variant === "unknown-sse") {
        const envelope = JSON.stringify({ jsonrpc: "invalid", id: rpc.id, result: {} });
        response.end(variant === "unknown-sse" ? `data: ${envelope}\n\n` : envelope);
      }
      else if (variant === "valid-json" || variant === "valid-sse") {
        const result = JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { resultType: "complete", content: [], structuredContent: { result: { taskId: "response-failure", accepted: false, state: "absent" } } } });
        response.end(variant === "valid-sse" ? `data: ${result}\n\n` : result);
      } else response.end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, error: { code: -32602, message: "Invalid input" } }));
    });
    const runtime = ManagedRuntime.make(Mcp.layer);
    try {
      await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
      const address = nativeServer.address();
      assert.ok(address !== null && typeof address !== "string");
      const mcp = await runtime.runPromise(Mcp);
      await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("response-failure") }, 1500).pipe(Effect.match({
        onSuccess: (result) => {
          assert.ok(variant === "valid-json" || variant === "valid-sse", "Invalid or interrupted RPC must fail");
          assert.equal(result.taskId, "response-failure");
          assert.equal(result.accepted, false);
        },
        onFailure: (error) => {
          assert.ok(variant !== "valid-json" && variant !== "valid-sse", "Valid JSON/SSE RPC must succeed");
          if (variant === "protocol") {
            assert.ok(error instanceof HttpError);
            assert.equal(error.status, 200);
            assert.equal(error.protocolCode, -32602);
          } else {
            assert.ok(error instanceof CommandError);
            assert.equal(error.reason, variant === "interrupted" ? "transport" : variant === "unknown-json" || variant === "unknown-sse" ? undefined : "protocol");
          }
        },
      })));
      assert.equal(tools, 1, "No tool POST is automatically retried");
    } finally {
      nativeServer.closeAllConnections();
      await runtime.dispose();
      await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
    }
  });
}

test("Mcp.call retains an actual native pre-response transport interruption without repeating POST", { timeout: 5000 }, async () => {
  let posts = 0;
  const nativeServer = createServer((request) => {
    posts++;
    request.socket.destroy();
  });
  const runtime = ManagedRuntime.make(Mcp.layer);
  try {
    await new Promise<void>((resolve) => nativeServer.listen(0, "127.0.0.1", resolve));
    const address = nativeServer.address();
    assert.ok(address !== null && typeof address !== "string");
    const mcp = await runtime.runPromise(Mcp);
    await runtime.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, "caller", "journal", { taskId: TaskId.make("transport-failure") }).pipe(Effect.match({
      onSuccess: () => assert.fail("Interrupted transport must fail"),
      onFailure: (error) => {
        assert.ok(error instanceof CommandError);
        assert.equal(error.reason, "transport");
      },
    })));
    assert.equal(posts, 1);
  } finally {
    nativeServer.closeAllConnections();
    await runtime.dispose();
    await new Promise<void>((resolve, reject) => nativeServer.close((error) => error ? reject(error) : resolve()));
  }
});
