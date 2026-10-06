import { strict as assert } from "node:assert";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Journal } from "../src/journal.js";

test("complete host lines across branches, metadata-only consent spool, durable delivery intent and coverage", async () => {
  const directory = mkdtempSync(join(tmpdir(), "coordinator-journal-"));
  try {
    const file = join(directory, "host.jsonl");
    const first = { id: "a", type: "message", parentId: null, message: { role: "user", content: "TOKEN=supersecret" } };
    const branch = { id: "b", type: "message", parentId: "a", message: { role: "assistant", content: [{ type: "thinking", thinking: "private" }] } };
    writeFileSync(file, `${JSON.stringify(first)}\n${JSON.stringify(branch)}`);
    const journal = new Journal(directory, "session");
    journal.scan(file);
    assert.equal(journal.state.spool.length, 0);
    journal.consent(["owner", "participant"]);
    journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "a").length, 1);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 0);
    writeFileSync(file, `${JSON.stringify(first)}\n${JSON.stringify(branch)}\n`);
    journal.scan(file);
    assert.equal(journal.state.spool.filter((item) => item.entryId === "b").length, 1);
    assert.equal(readFileSync(journal.path, "utf8").includes("supersecret"), false);
    assert.equal(readFileSync(journal.path, "utf8").includes("private"), false);
    journal.handle("peer:message");
    assert.equal(journal.releaseIntent("task:1"), true);
    const resumed = new Journal(directory, "session");
    assert.equal(resumed.handled("peer:message"), true);
    assert.equal(resumed.releaseIntent("task:1"), false);
    await assert.rejects(resumed.upload(async () => { throw new Error("offline"); }));
    assert.ok(resumed.state.spool.length > 0);
    writeFileSync(file, `${JSON.stringify(branch)}\n`);
    resumed.scan(file);
    assert.ok(resumed.state.gaps.includes("rewrite"));
    resumed.scan(undefined);
    assert.ok(resumed.state.spool.some((entry) => entry.gaps.includes("ephemeral")));
    const sent: unknown[] = [];
    await resumed.upload(async (entry) => { sent.push(entry); });
    assert.ok(sent.length > 0);
    assert.equal(new Journal(directory, "session").state.spool.length, 0);
    resumed.erase();
    const optedOut = new Journal(directory, "session");
    assert.equal(optedOut.state.consent, false);
    assert.equal(optedOut.releaseIntent("task:1"), false);
    assert.deepEqual(optedOut.state.seen, {});
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
