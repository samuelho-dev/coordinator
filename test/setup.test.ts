import { assert, it } from "@effect/vitest";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Context, Effect, Layer, ManagedRuntime, Schema } from "effect";
import { HttpServer } from "effect/http";
import { NodeServices } from "@effect/platform-node";
import { InspectorConfig } from "../src/inspection.ts";
import { Inspector } from "../src/inspector.ts";
import { Mcp } from "../src/mcp.ts";
import { Host, serverLayer } from "../src/platform.ts";
import { loadConnectorConfig, readConnectorToken } from "../src/native.ts";
import { connectClient, initializeServer } from "../src/setup.ts";

it("bootstraps and authenticates real clients without credential disclosure, identity state, overwrites or partial configuration", async () => {
  const root = await mkdtemp(join(tmpdir(), "coordinator-setup-"));
  await chmod(root, 0o700);
  const runtime = ManagedRuntime.make(Layer.mergeAll(NodeServices.layer, Host.layer, Mcp.layer));
  try {
    const directory = join(root, "server");
    const initialized = await runtime.runPromise(initializeServer({ directory, owner: "alice", host: "127.0.0.1", port: 8787 }));
    const token = (await readFile(initialized.tokenFile, "utf8")).trim();
    const config = await runtime.runPromise(Schema.decodeEffect(Schema.fromJsonString(InspectorConfig))(await readFile(initialized.configFile, "utf8")));
    assert.strictEqual(JSON.stringify(config).includes(token), false);
    assert.strictEqual(JSON.stringify(initialized).includes(token), false);
    for (const file of [initialized.tokenFile, initialized.configFile]) assert.strictEqual((await lstat(file)).mode & 0o777, 0o600);
    assert.strictEqual((await lstat(directory)).mode & 0o777, 0o700);
    const repeated = await runtime.runPromise(initializeServer({ directory, owner: "alice", host: "127.0.0.1", port: 8787 }).pipe(Effect.flip));
    assert.strictEqual(repeated._tag, "NativeError");
    assert.strictEqual((await readFile(initialized.tokenFile, "utf8")).trim(), token);

    // Keep the serving scope alive in its own runtime until all client reads finish.
    class Endpoint extends Context.Service<Endpoint>()("test/SetupEndpoint", {
      make: Effect.gen(function*() {
        const inspector = yield* Inspector.make(config);
        const http = yield* HttpServer.HttpServer;
        yield* (yield* Mcp).serve(http, inspector);
        return HttpServer.formatAddress(http.address);
      }),
    }) {}
    const app = ManagedRuntime.make(Layer.effect(Endpoint, Endpoint.make).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, Mcp.layer, serverLayer("127.0.0.1", 0)))));
    try {
      const endpoint = await app.runPromise(Endpoint);
      const configFile = join(root, "client", "inspector.json");
      const bad = await runtime.runPromise(connectClient({ endpoint, token: "invalid-owner-credential", configFile }).pipe(Effect.flip));
      assert.strictEqual(bad._tag, "NativeError");
      assert.strictEqual(JSON.stringify(bad).includes("invalid-owner-credential"), false);
      assert.strictEqual((await readdir(root)).includes("client"), false);
      for (const invalid of ["http://remote.example", "https://user:secret@example.test", "https://example.test?token=secret"]) {
        const rejected = await runtime.runPromise(connectClient({ endpoint: invalid, token, configFile }).pipe(Effect.flip));
        assert.strictEqual(rejected._tag, "NativeError");
        assert.strictEqual(JSON.stringify(rejected).includes(invalid), false);
      }
      const tlsFailure = await runtime.runPromise(connectClient({ endpoint: endpoint.replace("http:", "https:"), token, configFile }).pipe(Effect.flip));
      assert.strictEqual(tlsFailure._tag, "NativeError");
      assert.strictEqual((await readdir(root)).includes("client"), false);
      await runtime.runPromise(connectClient({ endpoint, token, configFile }));
      const client = await runtime.runPromise(loadConnectorConfig(configFile));
      assert.ok(client);
      assert.deepStrictEqual(client, { inspectorUrl: endpoint, tokenFile: join(root, "client", "inspector.token") });
      assert.strictEqual(await runtime.runPromise(readConnectorToken(client)), token);
      assert.strictEqual((await readFile(configFile, "utf8")).includes(token), false);
      assert.deepStrictEqual((await readdir(join(root, "client"))).sort(), ["inspector.json", "inspector.token"]);
      await runtime.runPromise(connectClient({ endpoint, token, configFile }).pipe(Effect.flip));
      assert.strictEqual(await runtime.runPromise(readConnectorToken(client)), token);
      const source = join(root, "source.token");
      await writeFile(source, token, { mode: 0o600 });
      const pointer = join(root, "pointer", "inspector.json");
      await runtime.runPromise(connectClient({ endpoint, token, tokenFile: source, configFile: pointer }));
      assert.deepStrictEqual(await readdir(join(root, "pointer")), ["inspector.json"]);
      const unsafe = join(root, "unsafe");
      await mkdir(unsafe, { mode: 0o755 });
      await chmod(unsafe, 0o755);
      await runtime.runPromise(connectClient({ endpoint, token, configFile: join(unsafe, "inspector.json") }).pipe(Effect.flip));
      assert.deepStrictEqual(await readdir(unsafe), []);
      const linked = join(root, "linked");
      await symlink(join(root, "client"), linked);
      await runtime.runPromise(connectClient({ endpoint, token, configFile: join(linked, "new.json") }).pipe(Effect.flip));
      assert.strictEqual((await readdir(join(root, "client"))).includes("new.json"), false);
      const partial = join(root, "partial");
      await mkdir(partial, { mode: 0o700 });
      await writeFile(join(partial, "inspector.json"), "existing", { mode: 0o600 });
      await runtime.runPromise(connectClient({ endpoint, token, configFile: join(partial, "inspector.json") }).pipe(Effect.flip));
      assert.deepStrictEqual(await readdir(partial), ["inspector.json"]);
      assert.strictEqual(await readFile(join(partial, "inspector.json"), "utf8"), "existing");
    } finally { await app.dispose(); }
  } finally {
    await runtime.dispose();
    await rm(root, { recursive: true, force: true });
  }
}, 20000);
