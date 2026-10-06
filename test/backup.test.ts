import assert from "node:assert/strict";
import { test } from "node:test";
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
