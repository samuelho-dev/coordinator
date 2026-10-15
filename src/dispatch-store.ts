import { Cause, Duration, Effect, Layer, Schedule, Schema } from "effect";
import { PersistedQueueError, PersistedQueueStore } from "effect/persistence/PersistedQueue";
import type { ExpressionBuilder } from "kysely";
import { DatabaseService, type DatabaseError } from "./database-service.ts";
import type { DB, DispatchQueueId } from "./generated/database/types.ts";
import { DispatchStateEnum } from "./generated/database/enums.ts";
import { redactedCause } from "./platform.ts";

const timestamp = (eb: ExpressionBuilder<DB, "dispatch_queue">, seconds = 0) => eb.fn<Date>("to_timestamp", [
  eb(eb.fn<number>("date_part", [eb.val("epoch"), eb.fn<Date>("clock_timestamp", [])]), "+", seconds),
]);
const ackSchedule = Schedule.min([Schedule.exponential(200, 1.5), Schedule.spaced(5000)]).pipe(Schedule.upTo({ duration: "5 seconds" }));

export const layerDispatchStore = <S extends Schema.Constraint>(schema: S) => Layer.effect(PersistedQueueStore, Effect.gen(function*() {
  const database = yield* DatabaseService;
  const decode = Schema.decodeEffect(Schema.fromJsonString(Schema.toEncoded(schema)));
  const active = new Map<DispatchQueueId, string>();
  const wrap = <A, R>(effect: Effect.Effect<A, DatabaseError, R>) => effect.pipe(Effect.mapError((cause) => new PersistedQueueError({ message: "Persisted queue database operation failed", cause })));
  const acknowledge = <A, E, R>(sequence: DispatchQueueId, token: string, operation: Effect.Effect<A, E, R>) => operation.pipe(
    Effect.retry(ackSchedule), Effect.orDie, Effect.asVoid,
    Effect.ensuring(Effect.sync(() => { if (active.get(sequence) === token) active.delete(sequence); })),
  );
  // A per-claim token also fences a stale finalizer after lease reclamation.
  yield* Effect.suspend(() => Effect.forEach(active, ([sequence, token]) => database.query((db) => db.updateTable("dispatch_queue")
    .set((eb) => ({ leased_at: timestamp(eb) })).where("sequence", "=", sequence).where("lease_id", "=", token)), { discard: true })).pipe(
    Effect.tapError((error) => Effect.logWarning(`dispatch lease refresh failed: ${redactedCause(Cause.fail(error))}`)),
    Effect.retry(Schedule.spaced(500)), Effect.repeat(Schedule.fixed("1 second")), Effect.forkScoped,
  );

  return PersistedQueueStore.of({
    offer: ({ name, id, element, delay }) => wrap(Effect.suspend(() => database.query((db) => db.insertInto("dispatch_queue").values((eb) => ({
      id, queue: name, payload: JSON.stringify(element), state: DispatchStateEnum.pending, attempts: 0,
      available_at: timestamp(eb, delay === undefined ? 0 : Math.ceil(Duration.toSeconds(delay))),
      created_at: timestamp(eb), updated_at: timestamp(eb),
    })).onConflict((oc) => oc.columns(["id", "queue"]).doNothing()))).pipe(Effect.asVoid)),
    take: (options) => Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
      while (true) {
        yield* wrap(database.query((db) => db.updateTable("dispatch_queue")
          .set((eb) => ({ state: DispatchStateEnum.failed, leased_at: null, lease_id: null, updated_at: timestamp(eb) }))
          .where("queue", "=", options.name).where("state", "=", DispatchStateEnum.pending)
          .where((eb) => eb("attempts", ">=", eb.cast<number>(eb.val(options.maxAttempts), "bigint")))
          .where((eb) => eb.or([eb("leased_at", "is", null), eb("leased_at", "<", timestamp(eb, -5))]))));
        const token = crypto.randomUUID();
        const rows = yield* wrap(database.query((db) => db.updateTable("dispatch_queue")
          .set((eb) => ({ leased_at: timestamp(eb), lease_id: token, attempts: eb("attempts", "+", 1) }))
          .where("sequence", "in", (eb) => eb.selectFrom("dispatch_queue").select("sequence")
            .where("queue", "=", options.name).where("state", "=", DispatchStateEnum.pending).where((eb) => eb("attempts", "<", eb.cast<number>(eb.val(options.maxAttempts), "bigint")))
            .where((eb) => eb("available_at", "<=", timestamp(eb)))
            .where((eb) => eb.or([eb("leased_at", "is", null), eb("leased_at", "<", timestamp(eb, -5))]))
            .orderBy("available_at").orderBy("sequence").forUpdate().skipLocked().limit(1))
          .returning(["sequence", "id", "payload", "attempts"])));
        const row = rows[0];
        if (!row) { yield* restore(Effect.sleep("200 millis")); continue; }
        active.set(row.sequence, token);
        const parsed = yield* Effect.result(decode(row.payload));
        if (parsed._tag === "Failure") {
          yield* acknowledge(row.sequence, token, database.query((db) => db.updateTable("dispatch_queue").where("sequence", "=", row.sequence).where("lease_id", "=", token).set((eb) => ({ leased_at: null, lease_id: null, state: DispatchStateEnum.failed, updated_at: timestamp(eb), failure: "Invalid persisted queue payload" }))));
          continue;
        }
        yield* Effect.addFinalizer((exit) => Effect.gen(function*() {
          if (exit._tag === "Success") {
            yield* acknowledge(row.sequence, token, database.query((db) => db.updateTable("dispatch_queue").where("sequence", "=", row.sequence).where("lease_id", "=", token).set((eb) => ({ leased_at: null, lease_id: null, state: DispatchStateEnum.completed, updated_at: timestamp(eb) }))));
            return;
          }
          if (Cause.hasInterruptsOnly(exit.cause)) {
            yield* acknowledge(row.sequence, token, database.query((db) => db.updateTable("dispatch_queue").where("sequence", "=", row.sequence).where("lease_id", "=", token).set((eb) => ({ leased_at: null, lease_id: null, attempts: eb("attempts", "-", 1) }))));
            return;
          }
          if (row.attempts >= options.maxAttempts) {
            yield* acknowledge(row.sequence, token, database.query((db) => db.updateTable("dispatch_queue").where("sequence", "=", row.sequence).where("lease_id", "=", token).set((eb) => ({ leased_at: null, lease_id: null, state: DispatchStateEnum.failed, updated_at: timestamp(eb), failure: redactedCause(exit.cause) }))));
            return;
          }
          const delay = yield* options.retryDelay(row.attempts);
          yield* acknowledge(row.sequence, token, database.query((db) => db.updateTable("dispatch_queue").where("sequence", "=", row.sequence).where("lease_id", "=", token).set((eb) => ({ leased_at: null, lease_id: null, updated_at: timestamp(eb), available_at: timestamp(eb, Math.max(Math.ceil(Duration.toSeconds(delay)), 0)), failure: redactedCause(exit.cause) }))));
        }));
        return { id: row.id, attempts: row.attempts, element: parsed.success };
      }
    })),
    cleanup: ({ timeToLive, failedTimeToLive }) => wrap(Effect.gen(function*() {
      for (const [state, ttl] of new Map([[DispatchStateEnum.completed, timeToLive], [DispatchStateEnum.failed, failedTimeToLive]])) {
        if (ttl === undefined) continue;
        while (true) {
          const rows = yield* database.query((db) => db.deleteFrom("dispatch_queue")
            .where("sequence", "in", (eb) => eb.selectFrom("dispatch_queue").select("sequence").where("state", "=", state)
              .where((eb) => eb("updated_at", "<=", timestamp(eb, -Math.max(Math.ceil(Duration.toSeconds(ttl)), 0)))).limit(1000))
            .returning("sequence"));
          if (rows.length < 1000) break;
          yield* Effect.sleep("10 millis");
        }
      }
    })),
  });
}));
