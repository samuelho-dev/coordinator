import { RunnerStateEnum, DispatchStateEnum } from "../src/generated/database/enums.ts";
import { it } from "@effect/vitest";
import assert from "node:assert/strict";
import { Cause, ConfigProvider, Context, Deferred, Duration, Effect, Exit, Fiber, Layer, Redacted, Schema, Scope } from "effect";
import { PersistedQueueStore } from "effect/persistence/PersistedQueue";
import { layerDispatchStore } from "../src/broker.ts";
import type { ContractMarkerRow } from "@prisma/orm-postgres/family/verify";
import type { LedgerEntryRecord } from "@prisma/orm-postgres/contract/types";
import { assertBrokerSchema, assertRunnerSchema, brokerSchemaIdentity, brokerStorageHash } from "../src/database.ts";
import { Platform } from "../src/platform.ts";

import { dropPostgres, postgresAdmin, provisionPostgres } from "./postgres-fixture.ts";
import { migrateBrokerPostgres } from "../scripts/migrate.ts";
import { DatabaseService } from "../src/database.ts";
import {
  Archive,
  type ArchiveInsert,
  Peer,
  RecoveryGateId,
  type RecoveryGateInsert,
  RunnerTask,
  SessionId,
  type TaskInsert,
  TaskId,
} from "../src/generated/database/types.ts";

it.live("runner committed PostgreSQL schema preserves held occupancy and rolls back uncertain changes", () =>
  Effect.scoped(Effect.gen(function*() {
    const url = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(url).pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      const db = yield* DatabaseService;
      yield* assertRunnerSchema(db);
      assert.deepEqual(yield* db.query(client => client.selectFrom("runner_tasks").selectAll()), []);
      assert.deepEqual(yield* db.query(client => client.selectFrom("runner_events").selectAll()), []);
      yield* db.query(client => client.insertInto("runner_tasks").values({
        taskId: TaskId.make("held"), fingerprint: RunnerTask.fields.fingerprint.make("a".repeat(64)), work: "{}", worktree: "/held",
        state: RunnerStateEnum.HELD, occupied: 1, incarnation: RunnerTask.fields.incarnation.make("incarnation"),
      }).returning("taskId"));
      const before = yield* db.query(client => client.selectFrom("runner_tasks").selectAll());
      const rollback = yield* db.transaction(Effect.gen(function*() {
        yield* db.query(client => client.updateTable("runner_tasks").set({ occupied: 0 }).where("taskId", "=", TaskId.make("held")).returning("taskId"));
        return yield* Effect.fail("uncertain");
      })).pipe(Effect.result);
      assert.equal(rollback._tag, "Failure");
      yield* assertRunnerSchema(db);
      assert.deepEqual(yield* db.query(client => client.selectFrom("runner_tasks").selectAll()), before);
    }).pipe(Effect.provide(postgresAdmin(url)), Effect.scoped);
  })).pipe(Effect.provide(Platform.layer))
);

it.live("pristine PostgreSQL schema preserves opaque rows and rejects metadata or recovery uncertainty", () =>
  Effect.scoped(Effect.gen(function*() {
    const url = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(url).pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      const db = yield* DatabaseService;
      type Metadata = {
        "prisma_contract.marker": Pick<ContractMarkerRow, "core_hash" | "profile_hash"> & Pick<LedgerEntryRecord, "space">;
        "information_schema.columns": { table_schema: string; table_name: string; column_name: string; data_type: string };
      };
      const held: TaskInsert = { id: TaskId.make("held"), data: '{"state":"HELD","accepted":true}' };
      const gateId = RecoveryGateId.make(1);
      yield* assertBrokerSchema(db);
      assert.deepEqual(yield* db.query(client => client.selectFrom("recovery_gate").selectAll()), []);
      assert.deepEqual(yield* db.query(client => client.selectFrom("tasks").selectAll()), []);
      assert.deepEqual(yield* db.query(client => client.selectFrom("dispatch_queue").selectAll()), []);
      assert.match(yield* brokerSchemaIdentity(db), new RegExp(`^${brokerStorageHash}:profile=`));
      yield* db.query(client => client.insertInto("tasks").values(held).returning("id"));
      const columns = yield* db.query(client => client.withTables<Metadata>().selectFrom("information_schema.columns")
        .select(["table_name", "column_name", "data_type"]).where("table_schema", "=", "public"));
      for (const [table, column] of [["archive", "sequence"], ["evidence", "id"], ["dispatch_queue", "sequence"]]) {
        assert.equal(columns.find((row) => row.table_name === table && row.column_name === column)?.data_type, "bigint");
      }
      assert.equal(columns.find((row) => row.table_name === "tasks" && row.column_name === "data")?.data_type, "text");
      assert.deepEqual(columns.filter(row => row.table_name === "dispatch_queue").map(row => row.column_name).sort(),
        ["sequence", "queue", "id", "payload", "state", "attempts", "failure", "available_at", "leased_at", "lease_id", "created_at", "updated_at"].sort());
      assert.equal((yield* db.query(client => client.insertInto("tasks")
        .values({ ...held, data: "changed" }).returning("id")).pipe(Effect.result))._tag, "Failure");
      const invalidGate: RecoveryGateInsert = { id: RecoveryGateId.make(2), ready: true };
      assert.equal((yield* db.query(client => client.insertInto("recovery_gate")
        .values(invalidGate).returning("id")).pipe(Effect.result))._tag, "Failure");
      const archive: ArchiveInsert = {
        owner: Peer.fields.owner.make("owner"), session: SessionId.make("session"), entry: Archive.fields.entry.make("entry"), digest: RunnerTask.fields.fingerprint.make("a".repeat(64)), payload: "opaque", gaps: "[]", created: 1
      };
      yield* db.query(client => client.insertInto("archive").values(archive).returning("sequence"));
      assert.equal((yield* db.query(client => client.insertInto("archive")
        .values({ ...archive, payload: "changed", created: 2 }).returning("sequence")).pipe(Effect.result))._tag, "Failure");
      yield* db.query(client => client.insertInto("recovery_gate").values({ id: gateId, ready: false }).returning("id"));
      assert.equal((yield* assertBrokerSchema(db).pipe(Effect.result))._tag, "Failure");
      assert.equal((yield* assertRunnerSchema(db).pipe(Effect.result))._tag, "Failure");
      assert.match(yield* brokerSchemaIdentity(db), new RegExp(`^${brokerStorageHash}:profile=`));
      yield* db.query(client => client.updateTable("recovery_gate").set({ ready: true }).where("id", "=", gateId).returning("id"));
      const marker = (yield* db.query(client => client.withTables<Metadata>().selectFrom("prisma_contract.marker")
        .select("profile_hash").where("space", "=", "app")))[0];
      assert.ok(marker);
      yield* db.query(client => client.withTables<Metadata>().updateTable("prisma_contract.marker")
        .set({ profile_hash: "wrong" }).where("space", "=", "app").returning("space"));
      assert.equal((yield* assertBrokerSchema(db).pipe(Effect.result))._tag, "Failure");
      yield* db.query(client => client.withTables<Metadata>().updateTable("prisma_contract.marker")
        .set({ profile_hash: marker.profile_hash }).where("space", "=", "app").returning("space"));
      yield* db.query(client => client.withTables<Metadata>().updateTable("prisma_contract.marker")
        .set({ core_hash: "wrong" }).where("space", "=", "app").returning("space"));
      assert.equal((yield* assertBrokerSchema(db).pipe(Effect.result))._tag, "Failure");
      yield* db.query(client => client.withTables<Metadata>().updateTable("prisma_contract.marker")
        .set({ core_hash: brokerStorageHash }).where("space", "=", "app").returning("space"));
    }).pipe(Effect.provide(postgresAdmin(url)), Effect.scoped);
    yield* migrateBrokerPostgres(url);
    yield* Effect.gen(function*() {
      const db = yield* DatabaseService;
      const heldId = TaskId.make("held");
      yield* assertBrokerSchema(db);
      assert.equal((yield* db.query(client => client.selectFrom("tasks").select("data").where("id", "=", heldId)))[0]?.data,
        '{"state":"HELD","accepted":true}');
    }).pipe(Effect.provide(postgresAdmin(url)), Effect.scoped);
  })).pipe(Effect.provide(Platform.layer))
);

it.live("durable queue deduplicates IDs, acknowledges scoped work, requeues interruption, and fences stale claims", () =>
  Effect.scoped(Effect.gen(function*() {
    const url = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(url).pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      const database = yield* DatabaseService;
      const store = yield* PersistedQueueStore;
      const options = { name: "claim-regression", maxAttempts: Number.MAX_SAFE_INTEGER, retryDelay: () => Effect.succeed(Duration.millis(0)) };
      yield* store.offer({ name: options.name, id: "same-id", element: { value: "original" }, isCustomId: true });
      yield* store.offer({ name: options.name, id: "same-id", element: { value: "original" }, isCustomId: true });
      yield* store.offer({ name: options.name, id: "same-id", element: { value: "conflict" }, isCustomId: true });
      const rows = yield* database.query(client => client.selectFrom("dispatch_queue").selectAll().where("queue", "=", options.name));
      assert.equal(rows.length, 1);
      const interrupted = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(interrupted, Exit.interrupt()));
      assert.deepEqual((yield* store.take(options).pipe(Effect.provideService(Scope.Scope, interrupted))).element, { value: "original" });
      yield* Scope.close(interrupted, Exit.interrupt());
      const requeued = yield* database.query(client => client.selectFrom("dispatch_queue").select(["state", "attempts", "lease_id"]).where("queue", "=", options.name));
      assert.deepEqual(requeued, [{ state: DispatchStateEnum.pending, attempts: 0, lease_id: null }]);
      const stale = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(stale, Exit.interrupt()));
      const first = yield* store.take(options).pipe(Effect.provideService(Scope.Scope, stale));
      assert.equal(first.attempts, 1);
      const current = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(current, Exit.interrupt()));
      const second = yield* database.transaction(Effect.gen(function*() {
        yield* database.query(client => client.updateTable("dispatch_queue").set({ leased_at: new Date(0) }).where("queue", "=", options.name));
        return yield* store.take(options).pipe(Effect.provideService(Scope.Scope, current));
      }));
      assert.equal(second.attempts, 2);
      const acquired = yield* database.query(client => client.selectFrom("dispatch_queue").select(["state", "attempts", "lease_id"]).where("queue", "=", options.name));
      assert.ok(acquired[0]?.lease_id);
      yield* Scope.close(stale, Exit.void);
      assert.deepEqual(yield* database.query(client => client.selectFrom("dispatch_queue").select(["state", "attempts", "lease_id"]).where("queue", "=", options.name)), acquired);
      yield* Scope.close(current, Exit.void);
      assert.deepEqual(yield* database.query(client => client.selectFrom("dispatch_queue").select(["state", "attempts", "lease_id"]).where("queue", "=", options.name)), [{ state: DispatchStateEnum.completed, attempts: 2, lease_id: null }]);
      yield* store.offer({ name: options.name, id: "exhausted", element: "crashed", isCustomId: true });
      yield* database.query(client => client.updateTable("dispatch_queue")
        .set({ attempts: 3, leased_at: new Date(0), lease_id: crypto.randomUUID() }).where("id", "=", "exhausted"));
      yield* store.offer({ name: options.name, id: "next", element: "survivor", isCustomId: true });
      const resumed = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(resumed, Exit.interrupt()));
      assert.equal((yield* store.take({ ...options, maxAttempts: 3 }).pipe(Effect.provideService(Scope.Scope, resumed))).element, "survivor");
      yield* Scope.close(resumed, Exit.void);
      assert.deepEqual(yield* database.query(client => client.selectFrom("dispatch_queue").select(["state", "lease_id"])
        .where("id", "=", "exhausted")), [{ state: DispatchStateEnum.failed, lease_id: null }]);
      yield* store.cleanup({ timeToLive: Duration.millis(0), failedTimeToLive: Duration.millis(0) });
      assert.deepEqual(yield* database.query(client => client.selectFrom("dispatch_queue").select("id").where("queue", "=", options.name)), []);
    }).pipe(Effect.provide(Layer.provideMerge(layerDispatchStore(Schema.Unknown), postgresAdmin(url))), Effect.scoped);
  })).pipe(Effect.provide(Platform.layer))
);

it.live("durable queue rejects malformed payloads before delivery and retries handler failures", () =>
  Effect.scoped(Effect.gen(function*() {
    const url = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(url).pipe(Effect.orDie));
    yield* Effect.gen(function*() {
      const database = yield* DatabaseService;
      const store = yield* PersistedQueueStore;
      const options = { name: "payload-regression", maxAttempts: 3, retryDelay: () => Effect.succeed(Duration.millis(0)) };
      yield* store.offer({ name: options.name, id: "broken-json", element: "unused", isCustomId: true });
      yield* database.query(db => db.updateTable("dispatch_queue").set({ payload: "{" }).where("queue", "=", options.name).where("id", "=", "broken-json"));
      yield* store.offer({ name: options.name, id: "wrong-shape", element: 42, isCustomId: true });
      yield* store.offer({ name: options.name, id: "valid", element: "task-1", isCustomId: true });
      const first = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(first, Exit.interrupt()));
      assert.deepEqual(yield* store.take(options).pipe(Effect.provideService(Scope.Scope, first)),
        { id: "valid", attempts: 1, element: "task-1" });
      yield* Scope.close(first, Exit.fail("handler failed"));
      const retry = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(retry, Exit.interrupt()));
      assert.deepEqual(yield* store.take(options).pipe(Effect.provideService(Scope.Scope, retry)),
        { id: "valid", attempts: 2, element: "task-1" });
      yield* Scope.close(retry, Exit.void);
      assert.deepEqual(yield* database.query(db => db.selectFrom("dispatch_queue")
        .select(["id", "state", "attempts", "lease_id"]).where("queue", "=", options.name).orderBy("sequence")), [
        { id: "broken-json", state: DispatchStateEnum.failed, attempts: 1, lease_id: null },
        { id: "wrong-shape", state: DispatchStateEnum.failed, attempts: 1, lease_id: null },
        { id: "valid", state: DispatchStateEnum.completed, attempts: 2, lease_id: null },
      ]);
    }).pipe(Effect.provide(Layer.provideMerge(layerDispatchStore(TaskId), postgresAdmin(url))), Effect.scoped);
  })).pipe(Effect.provide(Platform.layer))
);

it.live("nested Kysely rollback preserves its outer transaction and independent database commits", () =>
  Effect.scoped(Effect.gen(function*() {
    const firstUrl = yield* provisionPostgres(), secondUrl = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(firstUrl).pipe(Effect.orDie));
    yield* Effect.addFinalizer(() => dropPostgres(secondUrl).pipe(Effect.orDie));
    const first = Context.get(yield* Layer.build(DatabaseService.layer(firstUrl)), DatabaseService);
    const second = Context.get(yield* Layer.build(DatabaseService.layer(secondUrl)), DatabaseService);
    yield* first.transaction(Effect.gen(function*() {
      yield* first.query(client => client.insertInto("tasks").values({ id: TaskId.make("outer"), data: "{}" }));
      const nested = yield* first.transaction(Effect.gen(function*() {
        yield* first.query(client => client.insertInto("tasks").values({ id: TaskId.make("nested"), data: "{}" }));
        const descendant = yield* first.transaction(first.query(client => client.insertInto("tasks")
          .values({ id: TaskId.make("descendant"), data: "{}" }))).pipe(Effect.forkChild);
        yield* Fiber.join(descendant);
        return yield* Effect.fail("rollback nested");
      })).pipe(Effect.result);
      assert.equal(nested._tag, "Failure");
      assert.deepEqual(yield* first.query(client => client.selectFrom("tasks").select("id")), [{ id: "outer" }]);
      yield* second.transaction(second.query(client => client.insertInto("tasks").values({ id: TaskId.make("independent"), data: "{}" })));
    }));
    assert.deepEqual(yield* first.query(client => client.selectFrom("tasks").select("id")), [{ id: "outer" }]);
    assert.deepEqual(yield* second.query(client => client.selectFrom("tasks").select("id")), [{ id: "independent" }]);
  })).pipe(Effect.provide(Platform.layer))
);

it.live("transaction interruption releases queued ownership and backend death settles pending queries", () =>
  Effect.gen(function*() {
    const url = yield* provisionPostgres();
    yield* Effect.addFinalizer(() => dropPostgres(url).pipe(Effect.orDie));
    const database = Context.get(yield* Layer.build(DatabaseService.layer(url)), DatabaseService);
    const admin = Context.get(yield* Layer.build(DatabaseService.adminLayer(url)), DatabaseService);
    const entered = yield* Deferred.make<void>(), release = yield* Deferred.make<void>();
    const held = yield* database.transaction(Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
    )).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    const queued = yield* database.transaction(database.query(client => client.insertInto("tasks")
      .values({ id: TaskId.make("cancelled"), data: "{}" }))).pipe(Effect.forkScoped);
    yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
    const interrupted = yield* Fiber.interrupt(queued).pipe(Effect.forkScoped);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(held);
    yield* Fiber.join(interrupted);
    assert.deepEqual(yield* database.query(client => client.selectFrom("tasks").select("id")), []);

    const backend = (yield* database.query(client => client.selectNoFrom((eb) => eb.fn<number>("pg_backend_pid", []).as("pid"))))[0];
    assert.ok(backend);
    const begun = yield* Deferred.make<void>(), end = yield* Deferred.make<void>();
    const dying = yield* database.transaction(Deferred.succeed(begun, undefined).pipe(
      Effect.andThen(Deferred.await(end)), Effect.andThen(Effect.fail("rollback")),
    )).pipe(Effect.exit, Effect.forkScoped);
    yield* Deferred.await(begun);
    const waiting = yield* database.query(client => client.selectFrom("tasks").select("id")).pipe(Effect.result, Effect.forkScoped);
    yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
    assert.deepEqual(yield* admin.query(client => client.selectNoFrom((eb) => eb.fn<boolean>("pg_terminate_backend", [eb.val(backend.pid)]).as("terminated"))), [{ terminated: true }]);
    const result = yield* Fiber.join(waiting);
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure.reason, "connection");
    yield* Deferred.succeed(end, undefined);
    assert.equal((yield* Fiber.join(dying))._tag, "Failure");
  }).pipe(Effect.provide(Platform.layer), Effect.timeout("30 seconds")),
);

it.effect("database configuration rejects unsafe URLs without exposing credentials", () =>
  Effect.gen(function*() {
    const secret = "configuration-boundary-secret";
    for (const value of [
      `postgresql://fixture:${secret}@remote.example/fixture?sslmode=disable`,
      `postgresql://fixture:${secret}@remote.example/fixture`,
      `postgresql://fixture:${secret}@localhost/fixture/extra`,
      `postgresql://fixture:${secret}@localhost/fixture#fragment`,
      `postgresql://fixture:${secret}@localhost/fixture\n`,
      `not-a-url-${secret}`,
    ]) {
      const rejected = yield* DatabaseService.connection().pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ COORDINATOR_DATABASE_URL: value }))),
        Effect.exit,
      );
      assert.equal(rejected._tag, "Failure");
      if (rejected._tag === "Failure") assert.equal(Cause.pretty(rejected.cause).includes(secret), false);
    }
    const accepted = yield* DatabaseService.connection().pipe(
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({
        COORDINATOR_DATABASE_URL: `postgresql://fixture:${secret}@remote.example/fixture?sslmode=verify-full`,
      }))),
    );
    assert.equal(Redacted.value(accepted), `postgresql://fixture:${secret}@remote.example/fixture?sslmode=verify-full`);
  }),
);
