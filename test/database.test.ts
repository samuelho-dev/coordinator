import test from "node:test";
import assert from "node:assert/strict";
import { Effect, Layer } from "effect";
import { Reactivity } from "effect/reactivity";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { migrateBroker, migrateRunner } from "../src/database.ts";
import { Platform } from "../src/platform.ts";
import type { Json } from "../src/contracts.ts";

test("legacy SQLite rows survive baseline adoption and repeated detached evolution", () => Effect.runPromise(
  Effect.scoped(Effect.gen(function*() {
    const platform = yield* Platform;
    const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-migrations-" });
    const broker = yield* SqliteClient.make({ filename: platform.path.join(directory, "broker.sqlite") });
    yield* broker.unsafe("CREATE TABLE tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL)");
    yield* broker.unsafe("CREATE TABLE archive (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, session TEXT NOT NULL, entry TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL, gaps TEXT NOT NULL, created REAL NOT NULL, UNIQUE(owner,session,entry,digest))");
    yield* broker.unsafe("CREATE TABLE consents (owner TEXT NOT NULL, session TEXT NOT NULL, enabled INTEGER NOT NULL, participants TEXT NOT NULL, PRIMARY KEY(owner,session))");
    yield* broker.unsafe("CREATE TABLE participant_consents (owner TEXT NOT NULL, session TEXT NOT NULL, participant TEXT NOT NULL, enabled INTEGER NOT NULL, PRIMARY KEY(owner,session,participant))");
    yield* broker.unsafe("INSERT INTO tasks VALUES (?,?)", ["legacy", '{"state":"HELD"}']);
    yield* broker.unsafe("INSERT INTO archive(owner,session,entry,digest,payload,gaps,created) VALUES (?,?,?,?,?,?,?)", ["owner", "session", "entry", "digest", "metadata", "[]", 1]);
    yield* broker.unsafe("INSERT INTO consents VALUES (?,?,?,?)", ["owner", "session", 1, '["participant"]']);
    yield* broker.unsafe("INSERT INTO participant_consents VALUES (?,?,?,?)", ["owner", "session", "participant", 1]);
    const brokerTasks = yield* broker.unsafe("SELECT * FROM tasks");
    const archive = yield* broker.unsafe("SELECT * FROM archive");
    const consents = yield* broker.unsafe("SELECT * FROM consents");
    const participantConsents = yield* broker.unsafe("SELECT * FROM participant_consents");
    yield* migrateBroker(broker);
    yield* migrateBroker(broker);
    assert.deepEqual(yield* broker.unsafe("SELECT * FROM tasks"), brokerTasks);
    assert.deepEqual(yield* broker.unsafe("SELECT * FROM archive"), archive);
    assert.deepEqual(yield* broker.unsafe("SELECT * FROM consents"), consents);
    assert.deepEqual(yield* broker.unsafe("SELECT * FROM participant_consents"), participantConsents);
    yield* broker.unsafe("INSERT INTO sessions VALUES (?,?)", ["session", "metadata"]);
    yield* migrateBroker(broker);
    assert.deepEqual(yield* broker.unsafe("SELECT id,data FROM sessions").values, [["session", "metadata"]]);

    const filename = platform.path.join(directory, "runner.sqlite");
    yield* Effect.scoped(Effect.gen(function*() {
      const runner = yield* SqliteClient.make({ filename });
      yield* runner.unsafe(`CREATE TABLE tasks (taskId TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, work TEXT NOT NULL,
        worktree TEXT NOT NULL, state TEXT NOT NULL, occupied INTEGER NOT NULL, pid INTEGER,
        incarnation TEXT NOT NULL, address TEXT, promptId TEXT, reason TEXT, result TEXT)`);
      yield* runner.unsafe("CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, taskId TEXT NOT NULL, payload TEXT NOT NULL)");
      yield* runner.unsafe("CREATE TABLE owner (id INTEGER PRIMARY KEY, pid INTEGER)");
      yield* runner.unsafe("INSERT INTO owner VALUES (1,123)");
      yield* runner.unsafe("INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ["legacy", "fingerprint", "work", "worktree", "HELD", 1, 123, "incarnation", "address", "prompt", "diagnostic", "result"]);
      yield* runner.unsafe("INSERT INTO events(taskId,payload) VALUES (?,?)", ["legacy", "metadata"]);
      const tasks = yield* runner.unsafe("SELECT * FROM tasks");
      const events = yield* runner.unsafe("SELECT * FROM events");
      yield* migrateRunner(runner);
      // Node all() reads column count before sqlite3_step reparses cached SELECT * after ALTER.
      assert.deepEqual(yield* runner.unsafe("SELECT taskId,fingerprint,work,worktree,state,occupied,pid,incarnation,address,promptId,reason,result FROM tasks"), tasks);
      assert.deepEqual(yield* runner.unsafe("SELECT taskId,detached FROM tasks").values, [["legacy", 0]]);
      yield* migrateRunner(runner);
      assert.deepEqual(yield* runner.unsafe("SELECT taskId,detached FROM tasks").values, [["legacy", 0]]);
      assert.deepEqual(yield* runner.unsafe("SELECT * FROM events"), events);
      assert.deepEqual(yield* runner.unsafe("SELECT name FROM sqlite_master WHERE type='table' AND name='owner'"), []);
      assert.equal((yield* runner.unsafe<Json>("PRAGMA user_version"))[0]?.user_version, 0);
      yield* runner.unsafe("UPDATE tasks SET detached=1 WHERE taskId='legacy'");
    }));
    const runner = yield* SqliteClient.make({ filename });
    yield* migrateRunner(runner);
    yield* migrateRunner(runner);
    assert.deepEqual(yield* runner.unsafe("SELECT taskId,fingerprint,work,worktree,state,occupied,pid,incarnation,address,promptId,reason,result,detached FROM tasks").values, [["legacy", "fingerprint", "work", "worktree", "HELD", 1, 123, "incarnation", "address", "prompt", "diagnostic", "result", 1]]);
    assert.deepEqual(yield* runner.unsafe("SELECT seq,taskId,payload FROM events").values, [[1, "legacy", "metadata"]]);
    assert.equal((yield* runner.unsafe<Json>("PRAGMA user_version"))[0]?.user_version, 0);
  })).pipe(Effect.provide(Layer.merge(Reactivity.layer, Platform.layer)))
));
