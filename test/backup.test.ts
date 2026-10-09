import assert from "node:assert/strict";
import { test } from "node:test";
import { Schema } from "effect";
import { BrokerConfig, ObjectBackupConfig, type BrokerConfig as BrokerConfigType } from "../src/contracts.ts";
import { decodeSnapshotList } from "../src/http.ts";

test("snapshot expiry rejects foreign keys and malformed pagination pages", () => {
  const key = "sanitized/1788033801000.sqlite";
  const page = `<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>1</KeyCount><MaxKeys>256</MaxKeys><Contents><Key>${key}</Key></Contents></ListBucketResult>`;
  assert.deepEqual(decodeSnapshotList(page), { keys: [key] });
  for (const invalid of [
    page.replace(key, "native/private.jsonl"),
    page.replace("</ListBucketResult>", "<Key>sanitized/1788033801001.sqlite</Key></ListBucketResult>"),
    page.replace("</Contents>", "</Contents"),
    page.replace("<KeyCount>1</KeyCount>", "<KeyCount>2</KeyCount>"),
    page.replace("<IsTruncated>false</IsTruncated>", "<IsTruncated>true</IsTruncated>"),
    page.replace("</ListBucketResult>", "<NextContinuationToken>unrequested</NextContinuationToken></ListBucketResult>"),
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
