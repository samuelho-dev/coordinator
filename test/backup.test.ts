import assert from "node:assert/strict";
import { it } from "@effect/vitest"
import { Context, Effect, Layer, ManagedRuntime, Redacted, Schema } from "effect";
import { createServer } from "node:http";
import { NodeHttpClient } from "@effect/platform-node";
import { BrokerConfig, ObjectBackupConfig, type BrokerConfig as BrokerConfigType } from "../src/contracts.ts";
import { decodeSnapshotList, dumpBrokerDatabase, privateRecoveryPath, recover, RecoveryMetadata, restoreBrokerDatabase, SnapshotStore } from "../src/backup.ts";
import { Platform } from "../src/platform.ts";


import { assertBrokerSchema } from "../src/database.ts";
import { provisionPostgres, postgresAdmin, postgresConfig, dropPostgres } from "./postgres-fixture.ts";
import { DatabaseService } from "../src/database-service.ts";
import { Archive, Consent, Evidence, SessionId, TaskId } from "../src/generated/database/types.ts";

it("private recovery contract accepts PostgreSQL pair metadata and rejects unsupported versions", () => {
  const file = { sha256: "a".repeat(64), schema: "exact-contract", rows: 1, pendingAccepted: 1 };
  const metadata = { kind: "coordinator-private-recovery", version: 3, sourceSha: "a".repeat(40), createdAt: new Date(0).toISOString(), broker: file,
    runner: { ...file, eventRows: 2, id: "recovery-runner", layout: "canonical-layout", entries: [] }, excluded: [] };
  assert.equal(Schema.decodeUnknownSync(RecoveryMetadata)(metadata).runner?.eventRows, 2);
  assert.throws(() => Schema.decodeUnknownSync(RecoveryMetadata)({ ...metadata, version: 2 }));
  assert.throws(() => Schema.decodeUnknownSync(RecoveryMetadata)({ ...metadata, runner: { ...metadata.runner, eventRows: undefined } }));
});

it("snapshot expiry rejects foreign keys and malformed pagination pages", () => {
  const key = "sanitized/1788033801000.pgdump";
  const page = `<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>1</KeyCount><MaxKeys>256</MaxKeys><Contents><Key>${key}</Key></Contents></ListBucketResult>`;
  const truncated = page.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>").replace("</ListBucketResult>", "<NextContinuationToken>TOKEN</NextContinuationToken></ListBucketResult>");
  const garageToken = `${"a".repeat(40)}]`;
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", garageToken)).next, garageToken);
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", " ]&amp;&lt;&gt;&quot;&apos;&#93;&#x1F642;+/=%? ")).next, ` ]&<>"']🙂+/=%? `);
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", "x".repeat(4097))).next, "x".repeat(4097));
  assert.deepEqual(decodeSnapshotList(page), { keys: [key] });
  assert.deepEqual(decodeSnapshotList(page.replace(".pgdump", "&#46;pgdump")), { keys: [key] });
  for (const invalid of [
    page.replace(key, "native/private.jsonl"),
    page.replace("</ListBucketResult>", "<Key>sanitized/1788033801001.pgdump</Key></ListBucketResult>"),
    page.replace("</Contents>", "</Contents"),
    page.replace("<KeyCount>1</KeyCount>", "<KeyCount>2</KeyCount>"),
    page.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>"),
    page.replace("</ListBucketResult>", "<NextContinuationToken>unrequested</NextContinuationToken></ListBucketResult>"),
    page.replace("<MaxKeys>256</MaxKeys>", "<MaxKeys>257</MaxKeys>"),
    page.replace("<MaxKeys>256</MaxKeys>", "<MaxKeys>0</MaxKeys>"),
    page.replace(key, "sanitized/1788033801000.pgdump&amp;"),
    truncated.replace("TOKEN", ""),
    truncated.replace("TOKEN", "&unknown;"),
    truncated.replace("TOKEN", "&amp"),
    truncated.replace("TOKEN", "&#0;"),
    truncated.replace("TOKEN", "&#xD800;"),
    truncated.replace("TOKEN", "&#x110000;"),
    truncated.replace("TOKEN", "]]>"),
    truncated.replace("TOKEN", "<unexpected>token</unexpected>"),
    truncated.replace("</ListBucketResult>", "<NextContinuationToken></NextContinuationToken></ListBucketResult>"),
    page.replace("<ListBucketResult>", '<!DOCTYPE ListBucketResult [<!ENTITY token SYSTEM "file:///private">]><ListBucketResult>'),
    page.replace("<ListBucketResult>", '<ListBucketResult xmlns="broken&entity;">'),
    page.replace("<ListBucketResult>", '<ListBucketResult xmlns="one" xmlns="two">'),
  ]) assert.throws(() => decodeSnapshotList(invalid));
});

it("deployed broker config requires HTTPS while explicit local snapshot config permits loopback HTTP", () => {
  const objectBackup = { endpoint: "http://localhost:9000", region: "garage", bucket: "coordinator-backup-proof", accessKeyFile: "/private/access", secretKeyFile: "/private/secret" };
  const config: BrokerConfigType = { host: "0.0.0.0", port: 8787, principals: [], backupDirectory: "/backups", archiveRetentionMs: 1, backupRetentionMs: 1, objectBackup, repositoryIds: {} };
  assert.equal(Schema.decodeUnknownSync(ObjectBackupConfig, { onExcessProperty: "error" })(objectBackup).endpoint, objectBackup.endpoint);
  assert.throws(() => Schema.decodeSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(JSON.stringify(config)));
  assert.throws(() => Schema.encodeSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(config));
  assert.throws(() => Schema.encodeSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })({ ...config, objectBackup: { ...objectBackup, endpoint: "https://backup.example.test\\private" } }));
  assert.equal(Schema.decodeUnknownSync(BrokerConfig, { onExcessProperty: "error" })({ ...config, objectBackup: { ...objectBackup, endpoint: "https://backup.example.test" } }).objectBackup?.endpoint, "https://backup.example.test");
});

it("snapshot expiry carries opaque pagination through real HTTP and never deletes before all pages validate", async () => {
  const key = "sanitized/1788033801000.pgdump";
  const lastPage = `<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>1</KeyCount><MaxKeys>256</MaxKeys><Contents><Key>${key}</Key></Contents></ListBucketResult>`;
  let firstPage = "", secondPage = "";
  const requests: Array<{ method: string | undefined; url: URL }> = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://localhost");
    requests.push({ method: request.method, url });
    if (request.method === "GET") {
      response.writeHead(200, { "content-type": "application/xml" });
      response.end(url.searchParams.has("continuation-token") ? secondPage : firstPage);
    } else if (request.method === "DELETE") {
      response.writeHead(204);
      response.end();
    } else {
      response.writeHead(405);
      response.end();
    }
  });
  await new Promise<void>((resolve, reject) => server.listen(0, "127.0.0.1", resolve).once("error", reject));
  const address = server.address();
  assert(address && typeof address === "object");
  const runtime = ManagedRuntime.make(Layer.merge(Platform.layer, NodeHttpClient.layerNodeHttp));
  const platform = await runtime.runPromise(Platform);
  const root = await runtime.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-snapshots-" }));
  try {
    const accessKeyFile = platform.path.join(root, "access"), secretKeyFile = platform.path.join(root, "secret");
    await runtime.runPromise(platform.fs.writeFileString(accessKeyFile, "fixtureaccess", { mode: 0o600 }));
    await runtime.runPromise(platform.fs.writeFileString(secretKeyFile, "fixturesecret", { mode: 0o600 }));
    await runtime.runPromise(Effect.scoped(Effect.gen(function*() {
      const store = Context.get(yield* Layer.build(SnapshotStore.layer({ endpoint: `http://127.0.0.1:${address.port}`, region: "garage", bucket: "snapshot-regression", accessKeyFile, secretKeyFile })), SnapshotStore);
      const tokens: ReadonlyArray<readonly [string, string]> = [[`${"a".repeat(40)}]`, `${"a".repeat(40)}]`], [" ]&amp;&lt;&gt;&quot;&apos;&#93;&#x1F642;+/=%? ", ` ]&<>"']🙂+/=%? `]];
      for (const [encoded, token] of tokens) {
        firstPage = lastPage.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>").replace("</ListBucketResult>", `<NextContinuationToken>${encoded}</NextContinuationToken></ListBucketResult>`);
        for (const scenario of encoded === token ? ["valid", "bad-count", "foreign-key", "bad-entity", "token-loop", "oversized"] : ["valid"]) {
          requests.length = 0;
          secondPage = scenario === "bad-count" ? lastPage.replace("<KeyCount>1</KeyCount>", "<KeyCount>2</KeyCount>") :
            scenario === "foreign-key" ? lastPage.replace(key, "native/private.jsonl") :
            scenario === "bad-entity" ? firstPage.replace(encoded, "&unknown;") :
            scenario === "token-loop" ? firstPage :
            scenario === "oversized" ? "x".repeat(256 * 1024 + 1) : lastPage;
          const result = yield* Effect.exit(store.expire(1788033801001));
          assert.equal(result._tag, scenario === "valid" ? "Success" : "Failure", scenario);
          assert.equal(requests[1]?.url.searchParams.get("continuation-token"), token);
          assert.deepEqual(requests.map(request => request.method), scenario === "valid" ? ["GET", "GET", "DELETE"] : ["GET", "GET"]);
          if (result._tag === "Success") assert.equal(result.value, 1);
          if (scenario === "valid") assert.equal(requests[2]?.url.pathname, `/snapshot-regression/${key}`);
        }
      }
    })));
  } finally {
    await runtime.runPromise(platform.fs.remove(root, { recursive: true }));
    await runtime.dispose();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});

it("private recovery refuses insecure artifacts, symlinks, hardlinks and overwrite paths", async () => {
  const runtime = ManagedRuntime.make(Platform.layer);
  const platform = await runtime.runPromise(Platform);
  const root = await runtime.runPromise(platform.fs.makeTempDirectory({ prefix: "coordinator-private-recovery-test-" }));
  try {
    await runtime.runPromise(platform.fs.chmod(root, 0o700));
    const file = platform.path.join(root, "artifact.pgdump"), link = platform.path.join(root, "link");
    await runtime.runPromise(platform.fs.writeFileString(file, "private", { mode: 0o600 }));
    await runtime.runPromise(privateRecoveryPath(file, "file"));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(file, "file", true)));
    await runtime.runPromise(platform.fs.chmod(file, 0o644));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(file, "file")));
    await runtime.runPromise(platform.fs.chmod(file, 0o600));
    await runtime.runPromise(platform.fs.symlink(file, link));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(link, "file")));
    const brokenLink = platform.path.join(root, "broken-link");
    await runtime.runPromise(platform.fs.symlink(platform.path.join(root, "missing"), brokenLink));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(brokenLink, "file")));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(brokenLink, "file", true)));
    await runtime.runPromise(platform.fs.link(file, platform.path.join(root, "hardlink")));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(file, "file")));
    await runtime.runPromise(platform.fs.chmod(root, 0o755));
    await assert.rejects(runtime.runPromise(privateRecoveryPath(platform.path.join(root, "fresh"), "file", true)));
    assert.throws(() => Schema.decodeUnknownSync(RecoveryMetadata)({ kind: "sanitized", version: 1 }));
  } finally {
    await runtime.runPromise(platform.fs.remove(root, { recursive: true }));
    await runtime.dispose();
  }
});

it("native PostgreSQL dump restore fences failed partial data and preserves owner/overwrite gates", async () => {
  const runtime = ManagedRuntime.make(Platform.layer);
  try {
    await runtime.runPromise(Effect.scoped(Effect.gen(function*() {
      const platform = yield* Platform;
      const root = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-pg-backup-test-" });
      const urls: Redacted.Redacted<string>[] = [];
      yield* Effect.addFinalizer(() => Effect.forEach(urls, (url) => dropPostgres(url), { discard: true }).pipe(Effect.orDie));
      for (let index = 0; index < 2; index++) urls.push(yield* provisionPostgres());
      const source = urls[0]!, target = urls[1]!;
      const config: BrokerConfigType = { host: "127.0.0.1", port: 0, principals: [], backupDirectory: platform.path.join(root, "snapshots"), archiveRetentionMs: 1, backupRetentionMs: 1, repositoryIds: {} };
      yield* Effect.scoped(Effect.gen(function*() {
        assert.equal((yield* Effect.exit(recover("backup", config, platform.path.join(root, "owned"), Evidence.fields.sha.make("a".repeat(40)))))._tag, "Failure");
        assert.equal(yield* platform.fs.exists(platform.path.join(root, "owned")), false);
      }).pipe(Effect.provide(DatabaseService.layer(source)), Effect.provide(postgresConfig(source))));
      yield* Effect.scoped(Effect.gen(function*() {
        const db = yield* DatabaseService;
        yield* db.query(client => client.insertInto("tasks").values({ id: TaskId.make("invalid-task"), data: "{}" }));
        yield* db.query(client => client.insertInto("archive").values({ owner: Archive.fields.owner.make("owner"), session: SessionId.make("session"), entry: Archive.fields.entry.make("entry"), digest: Archive.fields.digest.make("a".repeat(64)), payload: "private transcript", gaps: "[]", created: 1 }));
        yield* db.query(client => client.insertInto("consents").values({ owner: Consent.fields.owner.make("owner"), session: SessionId.make("session"), enabled: 1, participants: "[]" }));
      }).pipe(Effect.provide(postgresAdmin(source))));
      const dump = platform.path.join(root, "1788033801000.pgdump");
      yield* dumpBrokerDatabase(dump, true).pipe(Effect.provide(DatabaseService.layer(source)), Effect.provide(postgresConfig(target)));
      const original = yield* platform.fs.readFile(dump);
      assert.equal((yield* Effect.exit(dumpBrokerDatabase(dump, true).pipe(Effect.provide(DatabaseService.layer(source)))))._tag, "Failure", "dump overwrote an existing artifact");
      assert.deepEqual(yield* platform.fs.readFile(dump), original);
      assert.equal(new TextDecoder().decode(original).includes("private transcript"), false);
      assert.equal((yield* Effect.exit(restoreBrokerDatabase(dump).pipe(Effect.provide(postgresConfig(target)))))._tag, "Failure", "invalid task must fail after restore");
      yield* Effect.scoped(Effect.gen(function*() {
        const db = yield* DatabaseService;
        assert.equal((yield* db.query(client => client.selectFrom("recovery_gate").selectAll()))[0]?.ready, false, "failed partial target must remain fenced");
        assert.equal(Number((yield* db.query(client => client.selectFrom("tasks").select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 1, "proof did not exercise post-mutation failure");
        const excluded: ReadonlyArray<"archive" | "consents" | "participant_consents"> = ["archive", "consents", "participant_consents"];
        for (const table of excluded) assert.equal(Number((yield* db.query(client => client.selectFrom(table).select((eb) => eb.fn.countAll<string>().as("count"))))[0]?.count), 0);
        assert.equal((yield* Effect.exit(assertBrokerSchema(db)))._tag, "Failure", "fenced target must not serve");
      }).pipe(Effect.provide(postgresAdmin(target))));
      assert.equal((yield* Effect.exit(restoreBrokerDatabase(dump).pipe(Effect.provide(postgresConfig(target)))))._tag, "Failure", "partial/occupied target must refuse reuse");
      assert.deepEqual(yield* platform.fs.readFile(dump), original);
    })));
  } finally {
    await runtime.dispose();
  }
});
