import { it } from "@effect/vitest";
import { expectTypeOf } from "vitest";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { Cause, Context, Effect, Layer, ManagedRuntime } from "effect";
import { HttpServer } from "effect/http";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { ConnectorAccepted, ConnectorAnnounce, HttpError, InspectionTools, type SessionInspection, type SessionListOutput, type SessionPresence } from "../src/inspection.ts";
import { Inspector } from "../src/inspector.ts";
import { LIMIT, MCP_RESPONSE_LIMIT, Mcp, serverLayer } from "../src/mcp.ts";

const token = "private-inspector-test-token";
const presence: SessionPresence = { ref: { machineId: "machine-a", harness: "pi", nativeSessionId: "native-😀" }, observedAt: 1, label: "readable-😀-\"\\\n" };
class Endpoint extends Context.Service<Endpoint>()("test/InspectorHttp", {
  make: Effect.gen(function*() {
    const mcp = yield* Mcp;
    const inspector = yield* Inspector.make({ host: "127.0.0.1", port: 0, principals: [{ id: "reader", owner: "owner", machineId: "machine-a", tokenHash: createHash("sha256").update(token).digest("hex") }] });
    const server = yield* HttpServer.HttpServer;
    yield* mcp.serve(server, inspector);
    return { mcp, url: HttpServer.formatAddress(server.address) };
  }),
}) {}
const runtime = () => ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.merge(Mcp.layer, serverLayer("127.0.0.1", 0)))));
const connector = { connectorId: "native-connector", incarnation: "process-incarnation" };

it("native MCP exposes only read-only tools and escapes or rejects private parameters", async () => {
  const app = runtime();
  const client = new Client({ name: "inspector-regression", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  try {
    const { mcp, url } = await app.runPromise(Endpoint);
    await app.runPromise(mcp.post(url, token, "/connector/announce", ConnectorAnnounce, ConnectorAccepted, { ...connector, machineId: "machine-a", sessions: [presence] }));
    await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`), { requestInit: { headers: { authorization: `Bearer ${token}` }, redirect: "error" } }));
    const catalog = await client.listTools();
    assert.deepEqual(catalog.tools.map((tool) => tool.name).sort(), ["session.inspect", "session.list"]);
    for (const tool of catalog.tools) {
      assert.equal(tool.annotations?.readOnlyHint, true);
      assert.equal(tool.annotations?.destructiveHint, false);
      assert.equal(tool.annotations?.idempotentHint, true);
      assert.equal(tool.annotations?.openWorldHint, false);
      assert.ok(tool.outputSchema);
    }
    const listing = mcp.call(url, token, InspectionTools.list, { limit: 10 });
    expectTypeOf(listing).toEqualTypeOf<Effect.Effect<SessionListOutput, HttpError | Cause.TimeoutError>>();
    const listed = await app.runPromise(listing);
    assert.equal(listed.sessions.length, 1);
    assert.deepEqual(listed.sessions[0]?.ref, presence.ref);
    assert.equal(listed.sessions[0]?.label, presence.label);
    const inspection = mcp.call(url, token, InspectionTools.inspect, { ref: presence.ref });
    expectTypeOf(inspection).toEqualTypeOf<Effect.Effect<SessionInspection, HttpError | Cause.TimeoutError>>();
    const readable = await client.callTool({ name: "session.list", arguments: { limit: 10 } });
    const text = readable.content.find((content) => content.type === "text");
    assert.ok(text?.type === "text");
    assert.deepEqual(JSON.parse(text.text), readable.structuredContent);
    assert.equal(text.text.includes(presence.label ?? "missing"), false, "JSON escapes control characters");
    const rejected = await client.callTool({ name: "session.list", arguments: { limit: 10, transcript: "private-model-input" } });
    assert.equal(rejected.isError, true);
    assert.equal(JSON.stringify(rejected).includes("private-model-input"), false);
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
      assert.deepEqual(catalog.tools.map((tool) => tool.name).sort(), ["session.inspect", "session.list"]);
      const result = await client.callTool({ name: "session.list", arguments: { limit: 1 } });
      assert.notEqual(result.isError, true);
    } finally { await client.close(); await app.dispose(); }
  }, 10000);
}

it("HTTP probes, credentials, origin, oversize and private connector fields stay bounded and redacted", async () => {
  const app = runtime();
  try {
    const { url, mcp } = await app.runPromise(Endpoint);
    assert.equal((await fetch(`${url}/livez`)).status, 200);
    for (const path of ["/healthz", "/readyz", "/mcp", "/connector/poll"]) {
      const response = await fetch(`${url}${path}`);
      assert.equal(response.status, 401);
      await response.text();
    }
    const healthy = await fetch(`${url}/readyz`, { headers: { authorization: `Bearer ${token}` } });
    assert.equal(healthy.status, 200);
    assert.equal(healthy.headers.get("cache-control"), "no-store");
    await healthy.text();
    for (const headers of [{ authorization: `Bearer ${token}`, origin: "https://hostile.example" }, { authorization: `Bearer ${"x".repeat(506)}` }]) {
      const requestHeaders = new Headers({ authorization: headers.authorization, "content-type": "application/json" });
      if (headers.origin !== undefined) requestHeaders.set("origin", headers.origin);
      const response = await fetch(`${url}/connector/poll`, { method: "POST", headers: requestHeaders, body: JSON.stringify(connector) });
      assert.equal(response.status, headers.origin ? 403 : 401);
      await response.text();
    }
    const oversized = await fetch(`${url}/connector/announce`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ privatePrompt: "x".repeat(LIMIT) }) });
    assert.equal(oversized.status, 413);
    await oversized.text();
    const secret = "private-transcript-secret";
    const privateInput = await fetch(`${url}/connector/announce`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ ...connector, machineId: "machine-a", sessions: [{ ...presence, transcript: secret }] }) });
    assert.equal(privateInput.status, 400);
    assert.equal((await privateInput.text()).includes(secret), false);
    assert.equal((await app.runPromise(mcp.call(url, token, InspectionTools.list, { limit: 1 }))).sessions.length, 0);
    await app.runPromise(mcp.call(url, "wrong-private-token", InspectionTools.list, { limit: 1 }).pipe(Effect.match({ onSuccess: () => assert.fail("unauthorized call"), onFailure: (error) => { assert.equal(error._tag, "HttpError"); if (error._tag === "HttpError") assert.equal(error.status, 401); } })));
  } finally { await app.dispose(); }
}, 10000);

for (const variant of ["401", "503", "oversized-json", "oversized-sse", "timeout", "redirect", "interrupted"]) {
  it(`Mcp.call ${variant} closes its socket without retrying or leaking rejected payloads`, async () => {
    let posts = 0;
    const disconnected = Promise.withResolvers<void>();
    const server = createServer((request, response) => {
      posts++;
      request.resume();
      response.once("close", disconnected.resolve);
      if (variant === "interrupted") { request.socket.destroy(); return; }
      if (variant === "redirect") { response.writeHead(307, { location: "http://localhost:1/steal" }); response.end(); return; }
      if (variant === "401" || variant === "503") { response.writeHead(Number(variant)); response.write("private-rejection".repeat(LIMIT)); return; }
      response.writeHead(200, { "content-type": variant === "oversized-json" ? "application/json" : "text/event-stream" });
      if (variant === "oversized-json") response.write("x".repeat(512 * 1024 + 1));
      else if (variant === "oversized-sse") response.write(`data: ${"x".repeat(MCP_RESPONSE_LIMIT)}`);
      else response.write(": waiting\n\n");
    });
    const app = ManagedRuntime.make(Mcp.layer);
    try {
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      const mcp = await app.runPromise(Mcp);
      await app.runPromise(mcp.call(`http://127.0.0.1:${address.port}`, token, InspectionTools.list, { limit: 1 }, 350).pipe(Effect.match({
        onSuccess: () => assert.fail("invalid peer must fail"),
        onFailure: (error) => {
          assert.equal(error.message.includes("private-rejection"), false);
          if (variant === "401" || variant === "503") { assert.equal(error._tag, "HttpError"); if (error._tag === "HttpError") assert.equal(error.status, Number(variant)); }
        },
      })));
      await Effect.runPromise(Effect.promise(() => disconnected.promise).pipe(Effect.timeout("2 seconds")));
      assert.equal(posts, 1);
    } finally {
      server.closeAllConnections();
      await app.dispose();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
  }, 5000);
}

it("network endpoints reject plaintext remote hosts and oversized credentials before a request", async () => {
  const app = ManagedRuntime.make(Mcp.layer);
  try {
    const mcp = await app.runPromise(Mcp);
    for (const [url, credential] of [["http://example.com", token], ["https://example.com", "x".repeat(506)], ["https://user:secret@example.com", token]]) {
      assert.ok(url && credential);
      const result = await app.runPromise(mcp.call(url, credential, InspectionTools.list, { limit: 1 }).pipe(Effect.exit));
      assert.equal(result._tag, "Failure");
    }
  } finally { await app.dispose(); }
});
