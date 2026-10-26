import { assert, expectTypeOf, it } from "@effect/vitest";
import { Cause, Context, Crypto, Effect, Fiber, Layer, ManagedRuntime, Stream } from "effect";
import { HttpClient, HttpClientRequest, HttpServer, HttpServerResponse } from "effect/http";
import { NodeHttpClient, NodeServices } from "@effect/platform-node";
import { Hex } from "effect/encoding";
import { serverLayer } from "../src/platform.ts";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { ConnectorAccepted, ConnectorAnnounce, ConnectorPoll, ConnectorPollResult, ConnectorResult, createInspection, HttpError, InspectionTools, type SessionInspection, type SessionListOutput, type SessionPresence } from "../src/inspection.ts";
import { Inspector } from "../src/inspector.ts";
import { LIMIT, MCP_RESPONSE_LIMIT, Mcp } from "../src/mcp.ts";

const token = "private-inspector-test-token";
const outsiderToken = "private-outsider-test-token";
const presence: SessionPresence = {
  ref: { machineId: "machine-a", harness: "pi", nativeSessionId: "native-😀" }, observedAt: 1, label: "readable-😀-\"\\\n",
  machine: { name: "Machine A", os: { platform: "linux", release: "6", architecture: "x64" } },
  agent: { kind: "primary", name: "reviewer", profile: "careful" },
  model: { provider: "provider", id: "model" },
  workspace: {
    cwd: "/work/linked/subdir",
    project: { root: "/work/linked", source: "configured", id: "project", name: "Project" },
    git: { state: "available", observedAt: 1, worktreeRoot: "/work/linked", commonDir: "/work/main/.git", branch: "feature", head: "a".repeat(40) },
  },
};
class Endpoint extends Context.Service<Endpoint>()("test/InspectorHttp", {
  make: Effect.gen(function*() {
    const mcp = yield* Mcp;
    const crypto = yield* Crypto.Crypto;
    const inspector = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [
      { id: "reader", owner: "owner", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(token))) },
      { id: "outsider", owner: "other-owner", tokenHash: Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(outsiderToken))) },
    ] });
    const server = yield* HttpServer.HttpServer;
    yield* mcp.serve(server, inspector);
    return { mcp, url: HttpServer.formatAddress(server.address) };
  }),
}) {}
const runtime = () => ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provideMerge(Layer.mergeAll(Mcp.layer, NodeServices.layer, NodeHttpClient.layerUndici, serverLayer("127.0.0.1", 0)))));
const connector = { connectorId: "native-connector", incarnation: "process-incarnation" };

it("native MCP exposes only read-only tools and escapes or rejects private parameters", async () => {
  const app = runtime();
  const client = new Client({ name: "inspector-regression", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  try {
    const { mcp, url } = await app.runPromise(Endpoint);
    await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted, { ...connector, machineId: "machine-a", sessions: [presence] }));
    await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`), { requestInit: { headers: { authorization: `Bearer ${token}` }, redirect: "error" } }));
    const catalog = await client.listTools();
    assert.deepStrictEqual(catalog.tools.map((tool) => tool.name).sort(), ["session.inspect", "session.list"]);
    for (const tool of catalog.tools) {
      assert.strictEqual(tool.annotations?.readOnlyHint, true);
      assert.strictEqual(tool.annotations?.destructiveHint, false);
      assert.strictEqual(tool.annotations?.idempotentHint, true);
      assert.strictEqual(tool.annotations?.openWorldHint, false);
      assert.ok(tool.outputSchema);
    }
    const listing = mcp.call(url, token, InspectionTools.list, { limit: 10 });
    expectTypeOf(listing).toEqualTypeOf<Effect.Effect<SessionListOutput, HttpError | Cause.TimeoutError>>();
    const listed = await app.runPromise(listing);
    assert.strictEqual(listed.sessions.length, 1);
    assert.deepStrictEqual(listed.sessions[0]?.ref, presence.ref);
    assert.strictEqual(listed.sessions[0]?.label, presence.label);
    assert.deepStrictEqual(listed.sessions[0]?.machine, presence.machine);
    assert.deepStrictEqual(listed.sessions[0]?.agent, presence.agent);
    assert.deepStrictEqual(listed.sessions[0]?.model, presence.model);
    assert.deepStrictEqual(listed.sessions[0]?.workspace, presence.workspace);
    const inspection = mcp.call(url, token, InspectionTools.inspect, { ref: presence.ref });
    expectTypeOf(inspection).toEqualTypeOf<Effect.Effect<SessionInspection, HttpError | Cause.TimeoutError>>();
    const inspected = await app.runPromise(Effect.gen(function*() {
      const pending = yield* Effect.forkChild(inspection, { startImmediately: true });
      let requests = (yield* mcp.post(url, token, "/connector/poll", ConnectorPoll, ConnectorPollResult, connector)).requests;
      for (let attempt = 0; requests.length === 0 && attempt < 100; attempt++) {
        yield* Effect.sleep("10 millis");
        requests = (yield* mcp.post(url, token, "/connector/poll", ConnectorPoll, ConnectorPollResult, connector)).requests;
      }
      const request = requests.at(0);
      if (!request) return yield* Effect.die("Missing inspection request");
      assert.deepStrictEqual(request.ref, presence.ref);
      yield* mcp.post(url, token, "/connector/result", ConnectorResult, ConnectorAccepted, {
        ...connector, requestId: request.requestId, observation: createInspection(presence, 2),
      });
      return yield* Fiber.join(pending);
    }));
    assert.deepStrictEqual(inspected.presence.workspace, presence.workspace);
    assert.deepStrictEqual(inspected.presence.model, presence.model);
    const readable = await client.callTool({ name: "session.list", arguments: { limit: 10 } });
    const text = readable.content.find((content) => content.type === "text");
    if (text?.type !== "text") return assert.fail("Missing MCP text result");
    assert.deepStrictEqual(JSON.parse(text.text), readable.structuredContent);
    const rejected = await client.callTool({ name: "session.list", arguments: { limit: 10, transcript: "private-model-input" } });
    assert.strictEqual(rejected.isError, true);
    assert.strictEqual(JSON.stringify(rejected).includes("private-model-input"), false);
  } finally { await client.close(); await app.dispose(); }
}, 15000);

for (const protocol of ["2025-11-25", "2025-06-18", "2025-03-26"]) {
  it(`native MCP backward protocol ${protocol} discovers the read-only catalog`, async () => {
    const app = runtime();
    const client = new Client({ name: "native-backward-regression", version: "1.0.0" }, { supportedProtocolVersions: [protocol], versionNegotiation: { mode: "legacy" } });
    try {
      const { url } = await app.runPromise(Endpoint);
      await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`), { requestInit: { headers: { authorization: `Bearer ${token}` }, redirect: "error" } }));
      const catalog = await client.listTools();
      assert.deepStrictEqual(catalog.tools.map((tool) => tool.name).sort(), ["session.inspect", "session.list"]);
      const result = await client.callTool({ name: "session.list", arguments: { limit: 1 } });
      assert.notStrictEqual(result.isError, true);
    } finally { await client.close(); await app.dispose(); }
  }, 10000);
}

it("one owner key discovers multiple installations while connector fences and owner isolation remain enforced", async () => {
  const app = runtime();
  try {
    const { mcp, url } = await app.runPromise(Endpoint);
    const second: SessionPresence = { ...presence, ref: { ...presence.ref, machineId: "machine-b" } };
    await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted, { ...connector, machineId: "machine-a", sessions: [presence] }));
    await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted, { connectorId: "second-installation", incarnation: "second-process", machineId: "machine-b", sessions: [second] }));
    const listed = await app.runPromise(mcp.call(url, token, InspectionTools.list, { limit: 10 }));
    assert.deepStrictEqual(listed.sessions.map((session) => session.ref.machineId).sort(), ["machine-a", "machine-b"]);
    assert.strictEqual((await app.runPromise(mcp.call(url, outsiderToken, InspectionTools.list, { limit: 10 }))).sessions.length, 0);
    for (const ref of [presence.ref, second.ref]) {
      await app.runPromise(mcp.call(url, outsiderToken, InspectionTools.inspect, { ref }).pipe(Effect.match({
        onSuccess: () => assert.fail("Cross-owner detail accepted"),
        onFailure: (error) => { assert.strictEqual(error._tag, "HttpError"); if (error._tag === "HttpError") assert.ok(error.status === 403 || error.status === 404); },
      })));
    }
    for (const announcement of [
      { ...connector, machineId: "machine-b", sessions: [second] },
      { connectorId: "mixed-installation", incarnation: "mixed-process", machineId: "machine-a", sessions: [second] },
    ]) {
      await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted, announcement).pipe(Effect.match({
        onSuccess: () => assert.fail("Connector machine fence accepted"),
        onFailure: (error) => { assert.strictEqual(error._tag, "HttpError"); if (error._tag === "HttpError") assert.ok(error.status === 400 || error.status === 403 || error.status === 409); },
      })));
    }
    await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted,
      { connectorId: "competing-publisher", incarnation: "competing-process", machineId: "machine-a", sessions: [presence] }).pipe(Effect.match({
        onSuccess: () => assert.fail("Active native publisher fence accepted"),
        onFailure: (error) => { assert.strictEqual(error._tag, "HttpError"); if (error._tag === "HttpError") assert.strictEqual(error.status, 409); },
      })));
  } finally { await app.dispose(); }
}, 10000);

it("HTTP probes, credentials, origin, oversize and private connector fields stay bounded and redacted", async () => {
  const app = runtime();
  try {
    const { url, mcp } = await app.runPromise(Endpoint);
    const request = (url: string, options: { method?: "POST"; headers?: Headers | { authorization: string; "content-type"?: string }; body?: string } = {}) => app.runPromise(Effect.gen(function*() {
      let input = HttpClientRequest.make(options.method ?? "GET")(url);
      if (options.headers !== undefined) input = HttpClientRequest.setHeaders(input, options.headers);
      if (options.body !== undefined) input = HttpClientRequest.bodyText(input, options.body, "application/json");
      const response = yield* HttpClient.execute(input);
      const body = yield* response.text;
      return { status: response.status, headers: new Headers(response.headers), body };
    }).pipe(Effect.scoped));
    assert.strictEqual((await request(`${url}/livez`)).status, 200);
    for (const path of ["/healthz", "/readyz", "/mcp", "/connector/poll"]) {
      const response = await request(`${url}${path}`);
      assert.strictEqual(response.status, 401);
    }
    const healthy = await request(`${url}/readyz`, { headers: { authorization: `Bearer ${token}` } });
    assert.strictEqual(healthy.status, 200);
    assert.strictEqual(healthy.headers.get("cache-control"), "no-store");
    const wrongKey = await request(`${url}/readyz`, { headers: { authorization: "Bearer wrong-private-token" } });
    assert.strictEqual(wrongKey.status, 401);
    for (const headers of [{ authorization: `Bearer ${token}`, origin: "https://hostile.example" }, { authorization: `Bearer ${"x".repeat(506)}` }]) {
      const requestHeaders = new Headers({ authorization: headers.authorization, "content-type": "application/json" });
      if (headers.origin !== undefined) requestHeaders.set("origin", headers.origin);
      const response = await request(`${url}/connector/poll`, { method: "POST", headers: requestHeaders, body: JSON.stringify(connector) });
      assert.strictEqual(response.status, headers.origin ? 403 : 401);
    }
    const oversized = await request(`${url}/connector/announce`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ privatePrompt: "x".repeat(LIMIT) }) });
    assert.strictEqual(oversized.status, 413);
    const secret = "private-transcript-secret";
    for (const invalid of [
      { ...presence, transcript: secret },
      { ...presence, machineName: secret },
      { ...presence, cwd: "/obsolete" },
      { ...presence, repository: "/obsolete" },
      { ...presence, agent: secret },
      { ...presence, model: secret },
      { ...presence, machine: { name: "Machine A", transcript: secret } },
      { ...presence, model: { id: "model", prompt: secret } },
      { ...presence, workspace: { cwd: "/work", project: { root: "/work", source: "native", transcript: secret } } },
      { ...presence, workspace: { cwd: "/work", git: { state: "unavailable", observedAt: 1, stderr: secret } } },
    ]) {
      const privateInput = await request(`${url}/connector/announce`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ ...connector, machineId: "machine-a", sessions: [invalid] }) });
      assert.strictEqual(privateInput.status, 400);
      assert.strictEqual(privateInput.body.includes(secret), false);
    }
    assert.strictEqual((await app.runPromise(mcp.call(url, token, InspectionTools.list, { limit: 1 }))).sessions.length, 0);
    await app.runPromise(mcp.call(url, "wrong-private-token", InspectionTools.list, { limit: 1 }).pipe(Effect.match({ onSuccess: () => assert.fail("unauthorized call"), onFailure: (error) => { assert.strictEqual(error._tag, "HttpError"); if (error._tag === "HttpError") assert.strictEqual(error.status, 401); } })));
  } finally { await app.dispose(); }
}, 10000);

for (const variant of ["401", "503", "oversized-json", "oversized-sse", "timeout", "redirect", "interrupted"]) {
  it(`Mcp.call ${variant} closes its socket without retrying or leaking rejected payloads`, async () => {
    let posts = 0;
    const disconnected = Promise.withResolvers<void>();
    class Fixture extends Context.Service<Fixture>()("test/Fixture", {
      make: Effect.gen(function*() {
        const server = yield* HttpServer.HttpServer;
        yield* server.serve(Effect.sync(() => {
          posts++;
          const status = variant === "redirect" ? 307 : variant === "401" || variant === "503" ? Number(variant) : 200;
          const first = variant === "401" || variant === "503" ? "private-rejection".repeat(LIMIT)
            : variant === "oversized-json" ? "x".repeat(512 * 1024 + 1)
            : variant === "oversized-sse" ? `data: ${"x".repeat(MCP_RESPONSE_LIMIT)}` : ": waiting\n\n";
          const body = (variant === "interrupted" ? Stream.fail(new Error("Interrupted fixture response"))
            : variant === "redirect" ? Stream.empty
            : Stream.concat(Stream.make(new TextEncoder().encode(first)), Stream.never)).pipe(
              Stream.ensuring(Effect.sync(disconnected.resolve)),
            );
          return HttpServerResponse.stream(body, { status, headers: variant === "redirect" ? { location: "http://localhost:1/steal" }
            : { "content-type": variant === "oversized-json" ? "application/json" : "text/event-stream" } });
        }));
        return HttpServer.formatAddress(server.address);
      }),
    }) {}
    const app = ManagedRuntime.make(Layer.merge(Mcp.layer, Layer.effect(Fixture, Fixture.make).pipe(Layer.provide(serverLayer("127.0.0.1", 0)))));
    try {
      const url = await app.runPromise(Fixture);
      const mcp = await app.runPromise(Mcp);
      await app.runPromise(mcp.call(url, token, InspectionTools.list, { limit: 1 }, 350).pipe(Effect.match({
        onSuccess: () => assert.fail("invalid peer must fail"),
        onFailure: (error) => {
          assert.strictEqual(error.message.includes("private-rejection"), false);
          if (variant === "401" || variant === "503") { assert.strictEqual(error._tag, "HttpError"); if (error._tag === "HttpError") assert.strictEqual(error.status, Number(variant)); }
        },
      })));
      await Effect.runPromise(Effect.promise(() => disconnected.promise).pipe(Effect.timeout("2 seconds")));
      assert.strictEqual(posts, 1);
    } finally {
      await app.dispose();
    }
  }, 5000);
}

it("network endpoints reject plaintext remote hosts and oversized credentials before a request", async () => {
  const app = ManagedRuntime.make(Mcp.layer);
  try {
    const mcp = await app.runPromise(Mcp);
    for (const [url, credential] of [["http://example.com", token], ["https://example.com", "x".repeat(506)], ["https://user:secret@example.com", token]]) {
      if (url === undefined || credential === undefined) return assert.fail("Missing endpoint case");
      const result = await app.runPromise(mcp.call(url, credential, InspectionTools.list, { limit: 1 }).pipe(Effect.exit));
      assert.strictEqual(result._tag, "Failure");
    }
  } finally { await app.dispose(); }
});
