import assert from "node:assert/strict";
import { test } from "node:test";
import { Context, Effect, Layer, ManagedRuntime, Schema } from "effect";
import { createServer } from "node:http";
import { NodeHttpClient } from "@effect/platform-node";
import { BrokerConfig, ObjectBackupConfig, type BrokerConfig as BrokerConfigType } from "../src/contracts.ts";
import { decodeSnapshotList, SnapshotStore } from "../src/backup.ts";
import { Platform } from "../src/platform.ts";

test("snapshot expiry rejects foreign keys and malformed pagination pages", () => {
  const key = "sanitized/1788033801000.sqlite";
  const page = `<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>1</KeyCount><MaxKeys>256</MaxKeys><Contents><Key>${key}</Key></Contents></ListBucketResult>`;
  const truncated = page.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>").replace("</ListBucketResult>", "<NextContinuationToken>TOKEN</NextContinuationToken></ListBucketResult>");
  const garageToken = `${"a".repeat(40)}]`;
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", garageToken)).next, garageToken);
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", " ]&amp;&lt;&gt;&quot;&apos;&#93;&#x1F642;+/=%? ")).next, ` ]&<>"']🙂+/=%? `);
  assert.equal(decodeSnapshotList(truncated.replace("TOKEN", "x".repeat(4097))).next, "x".repeat(4097));
  assert.deepEqual(decodeSnapshotList(page), { keys: [key] });
  assert.deepEqual(decodeSnapshotList(page.replace(".sqlite", "&#46;sqlite")), { keys: [key] });
  for (const invalid of [
    page.replace(key, "native/private.jsonl"),
    page.replace("</ListBucketResult>", "<Key>sanitized/1788033801001.sqlite</Key></ListBucketResult>"),
    page.replace("</Contents>", "</Contents"),
    page.replace("<KeyCount>1</KeyCount>", "<KeyCount>2</KeyCount>"),
    page.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>"),
    page.replace("</ListBucketResult>", "<NextContinuationToken>unrequested</NextContinuationToken></ListBucketResult>"),
    page.replace("<MaxKeys>256</MaxKeys>", "<MaxKeys>257</MaxKeys>"),
    page.replace("<MaxKeys>256</MaxKeys>", "<MaxKeys>0</MaxKeys>"),
    page.replace(key, "sanitized/1788033801000.sqlite&amp;"),
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

test("deployed broker config requires HTTPS while explicit local snapshot config permits loopback HTTP", () => {
  const objectBackup = { endpoint: "http://localhost:9000", region: "garage", bucket: "coordinator-backup-proof", accessKeyFile: "/private/access", secretKeyFile: "/private/secret" };
  const config: BrokerConfigType = { database: "/data/ledger.sqlite", host: "0.0.0.0", port: 8787, principals: [], backupDirectory: "/backups", archiveRetentionMs: 1, backupRetentionMs: 1, objectBackup, repositoryIds: {} };
  assert.equal(Schema.decodeUnknownSync(ObjectBackupConfig, { onExcessProperty: "error" })(objectBackup).endpoint, objectBackup.endpoint);
  assert.throws(() => Schema.decodeUnknownSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(JSON.stringify(config)));
  assert.throws(() => Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(config));
  assert.throws(() => Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })({ ...config, objectBackup: { ...objectBackup, endpoint: "https://backup.example.test\\private" } }));
  assert.equal(Schema.decodeUnknownSync(BrokerConfig, { onExcessProperty: "error" })({ ...config, objectBackup: { ...objectBackup, endpoint: "https://backup.example.test" } }).objectBackup?.endpoint, "https://backup.example.test");
});

test("snapshot expiry carries opaque pagination through real HTTP and never deletes before all pages validate", async () => {
  const key = "sanitized/1788033801000.sqlite";
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
