import test from "node:test";
import assert from "node:assert/strict";
import { Effect, Layer } from "effect";
import { Reactivity } from "effect/reactivity";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import type { ContractMarkerRow } from "@prisma/orm-postgres/family/verify";
import type { LedgerEntryRecord } from "@prisma/orm-postgres/contract/types";
import { assertBrokerSchema, brokerSchemaIdentity, brokerStorageHash, migrateRunner } from "../src/database.ts";
import { Platform } from "../src/platform.ts";
import { SqlClient } from "effect/sql";
import { dropPostgres, postgresAdmin, provisionPostgres } from "./postgres-fixture.ts";
import { migrateBrokerPostgres } from "../scripts/migrate.ts";
import { makeDatabase } from "../src/kysely.ts";
import {
  type ArchiveInsert, EffectQueueMigrationsId, RecoveryGateId, type RecoveryGateInsert, type TasksInsert, TasksId
} from "../src/generated/database/types.ts";

test("runner legacy rows preserve HELD occupancy through repeated native binding migration", () => Effect.runPromise(
  Effect.scoped(Effect.gen(function*() {
    const platform = yield* Platform;
    const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-migrations-" });
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
      assert.deepEqual(yield* runner.unsafe("SELECT nativeBinding,nativeRunId,nativeCursor FROM tasks").values, [[null, null, 0]]);
      const binding = JSON.stringify({ host: "omp", sessionId: "native-session", incarnation: "native-incarnation", generation: 2 });
      yield* runner.unsafe("UPDATE tasks SET detached=1,nativeBinding=?,nativeRunId='run-1',nativeCursor=7 WHERE taskId='legacy'", [binding]);
      const rollback = yield* runner.withTransaction(Effect.gen(function*() {
        yield* runner.unsafe("UPDATE tasks SET nativeRunId='uncertain',nativeCursor=8,occupied=0 WHERE taskId='legacy'");
        return yield* Effect.fail("rollback");
      })).pipe(Effect.result);
      assert.equal(rollback._tag, "Failure");
      assert.deepEqual(yield* runner.unsafe("SELECT nativeBinding,nativeRunId,nativeCursor,occupied FROM tasks").values, [[binding, "run-1", 7, 1]]);
    }));
    const runner = yield* SqliteClient.make({ filename });
    yield* migrateRunner(runner);
    yield* migrateRunner(runner);
    assert.deepEqual(yield* runner.unsafe("SELECT taskId,fingerprint,work,worktree,state,occupied,pid,incarnation,address,promptId,reason,result,detached FROM tasks").values, [["legacy", "fingerprint", "work", "worktree", "HELD", 1, 123, "incarnation", "address", "prompt", "diagnostic", "result", 1]]);
    assert.deepEqual(yield* runner.unsafe("SELECT seq,taskId,payload FROM events").values, [[1, "legacy", "metadata"]]);
    assert.deepEqual(yield* runner.unsafe("SELECT nativeBinding,nativeRunId,nativeCursor FROM tasks").values, [[JSON.stringify({ host: "omp", sessionId: "native-session", incarnation: "native-incarnation", generation: 2 }), "run-1", 7]]);
  })).pipe(Effect.provide(Layer.merge(Reactivity.layer, Platform.layer)))
));

test("committed PostgreSQL migrations preserve opaque rows and reject version or recovery uncertainty", () => Effect.runPromise(
  Effect.scoped(Effect.gen(function*() {
    const platform = yield* Platform;
    const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-schema-" });
    const urlFile = yield* provisionPostgres(directory);
    yield* Effect.addFinalizer(() => dropPostgres(urlFile).pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient;
      const db = makeDatabase(sql);
      const metadata = db.client.withTables<{
        "prisma_contract.marker": Pick<ContractMarkerRow, "core_hash" | "profile_hash"> & Pick<LedgerEntryRecord, "space">;
        "prisma_contract.ledger": {
          space: LedgerEntryRecord["space"];
          migration_hash: LedgerEntryRecord["migrationHash"];
        };
        "information_schema.columns": { table_schema: string; table_name: string; column_name: string; data_type: string };
      }>();
      const held: TasksInsert = { id: TasksId.make("held"), data: '{"state":"HELD","accepted":true}' };
      const gateId = RecoveryGateId.make(1);
      const queueVersion = EffectQueueMigrationsId.make(2);
      yield* assertBrokerSchema(sql);
      assert.match(yield* brokerSchemaIdentity(sql), new RegExp(`^${brokerStorageHash}:queue=`));
      yield* db.execute(db.client.insertInto("tasks").values(held).returning("id"));
      const columns = yield* db.execute(metadata.selectFrom("information_schema.columns")
        .select(["table_name", "column_name", "data_type"]).where("table_schema", "=", "public"));
      for (const [table, column] of [["archive", "sequence"], ["evidence", "id"], ["effect_queue", "sequence"]]) {
        assert.equal(columns.find((row) => row.table_name === table && row.column_name === column)?.data_type, "bigint");
      }
      assert.equal(columns.find((row) => row.table_name === "tasks" && row.column_name === "data")?.data_type, "text");
      assert.equal((yield* db.execute(db.client.insertInto("tasks")
        .values({ ...held, data: "changed" }).returning("id")).pipe(Effect.result))._tag, "Failure");
      const invalidGate: RecoveryGateInsert = { id: RecoveryGateId.make(2), ready: true };
      assert.equal((yield* db.execute(db.client.insertInto("recovery_gate")
        .values(invalidGate).returning("id")).pipe(Effect.result))._tag, "Failure");
      const archive: ArchiveInsert = {
        owner: "owner", session: "session", entry: "entry", digest: "digest", payload: "opaque", gaps: "[]", created: 1
      };
      yield* db.execute(db.client.insertInto("archive").values(archive).returning("sequence"));
      assert.equal((yield* db.execute(db.client.insertInto("archive")
        .values({ ...archive, payload: "changed", created: 2 }).returning("sequence")).pipe(Effect.result))._tag, "Failure");
      yield* db.execute(db.client.insertInto("recovery_gate").values({ id: gateId, ready: false }).returning("id"));
      assert.equal((yield* assertBrokerSchema(sql).pipe(Effect.result))._tag, "Failure");
      assert.match(yield* brokerSchemaIdentity(sql), new RegExp(`^${brokerStorageHash}:queue=`));
      yield* db.execute(db.client.updateTable("recovery_gate").set({ ready: true }).where("id", "=", gateId).returning("id"));
      yield* db.execute(db.client.updateTable("effect_queue_migrations")
        .set({ name: "unexpected" }).where("migration_id", "=", queueVersion).returning("migration_id"));
      assert.equal((yield* assertBrokerSchema(sql).pipe(Effect.result))._tag, "Failure");
      yield* db.execute(db.client.updateTable("effect_queue_migrations")
        .set({ name: "upgrade_schema" }).where("migration_id", "=", queueVersion).returning("migration_id"));
      const marker = (yield* db.execute(metadata.selectFrom("prisma_contract.marker")
        .select("profile_hash").where("space", "=", "app")))[0];
      const ledger = (yield* db.execute(metadata.selectFrom("prisma_contract.ledger")
        .select("migration_hash").where("space", "=", "app")))[0];
      assert.ok(marker);
      assert.ok(ledger);
      yield* db.execute(metadata.updateTable("prisma_contract.marker")
        .set({ profile_hash: "wrong" }).where("space", "=", "app").returning("space"));
      assert.equal((yield* assertBrokerSchema(sql).pipe(Effect.result))._tag, "Failure");
      yield* db.execute(metadata.updateTable("prisma_contract.marker")
        .set({ profile_hash: marker.profile_hash }).where("space", "=", "app").returning("space"));
      yield* db.execute(metadata.updateTable("prisma_contract.ledger")
        .set({ migration_hash: "wrong" }).where("space", "=", "app").returning("space"));
      assert.equal((yield* assertBrokerSchema(sql).pipe(Effect.result))._tag, "Failure");
      yield* db.execute(metadata.updateTable("prisma_contract.ledger")
        .set({ migration_hash: ledger.migration_hash }).where("space", "=", "app").returning("space"));
      yield* db.execute(metadata.updateTable("prisma_contract.marker")
        .set({ core_hash: "wrong" }).where("space", "=", "app").returning("space"));
      assert.equal((yield* assertBrokerSchema(sql).pipe(Effect.result))._tag, "Failure");
      yield* db.execute(metadata.updateTable("prisma_contract.marker")
        .set({ core_hash: brokerStorageHash }).where("space", "=", "app").returning("space"));
    }).pipe(Effect.provide(postgresAdmin(urlFile)), Effect.scoped);
    yield* migrateBrokerPostgres(urlFile);
    yield* Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient;
      const db = makeDatabase(sql);
      const heldId = TasksId.make("held");
      yield* assertBrokerSchema(sql);
      assert.equal((yield* db.execute(db.client.selectFrom("tasks").select("data").where("id", "=", heldId)))[0]?.data,
        '{"state":"HELD","accepted":true}');
    }).pipe(Effect.provide(postgresAdmin(urlFile)), Effect.scoped);
  })).pipe(Effect.provide(Platform.layer))
));
