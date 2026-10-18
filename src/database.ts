import { AsyncLocalStorage } from "node:async_hooks";
import { Config, Context, Data, Deferred, Effect, Exit, Layer, Option, Redacted, Schema, Semaphore } from "effect";
import { Kysely, PostgresDialect, type ControlledTransaction, type PostgresCursor, type PostgresQueryResult } from "kysely";
import { Client, TypeOverrides, type QueryResultRow } from "pg";
import type { ContractMarkerRow } from "@prisma/orm-postgres/family/verify";
import type { LedgerEntryRecord } from "@prisma/orm-postgres/contract/types";
import contract from "./generated/prisma/contract.json" with { type: "json" };
import type { DB } from "./generated/database/types.ts";

const fatalPostgresError = Schema.Struct({ severity: Schema.Literal("FATAL") });

type DatabaseOperation = "connect" | "query" | "transaction" | "commit" | "rollback" | "ownership" | "durability" | "close";
type DatabaseFailureReason = "connection" | "execution" | "interrupted" | "owner_unavailable" | "durability_required" | "transaction_aborted";

export class DatabaseError extends Data.TaggedError("DatabaseError")<{
  readonly operation: DatabaseOperation;
  readonly reason: DatabaseFailureReason;
  readonly cause?: unknown;
}> {}

export class DatabaseConnectionError extends Data.TaggedError("DatabaseConnectionError") {
  readonly status = 400;
  override readonly message = "Valid PostgreSQL connection configuration required";
}

export interface DatabaseClient {
  readonly url: Redacted.Redacted<string>;
  readonly query: <A>(build: (db: Kysely<DB>) => { readonly execute: () => Promise<ReadonlyArray<A>> }) => Effect.Effect<ReadonlyArray<A>, DatabaseError>;
  readonly transaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | DatabaseError, R>;
}

const ownerLoss = new WeakMap<DatabaseClient, Effect.Effect<never, DatabaseError>>();
/** @internal Session-loss signal for ownership lifecycle infrastructure, not database operations. */
export const awaitDatabaseLoss = (database: DatabaseClient): Effect.Effect<never, DatabaseError> =>
  Effect.suspend(() => ownerLoss.get(database) ?? Effect.die("Database ownership signal is not registered"));

type TransactionOwner = {
  transaction: ControlledTransaction<DB> | undefined;
  readonly nested: Semaphore.Semaphore;
  nextSavepoint: number;
};
const databaseError = (operation: DatabaseOperation, reason: DatabaseFailureReason, cause?: unknown) =>
  new DatabaseError(cause === undefined ? { operation, reason } : { operation, reason, cause });

const makeLayer = (configuredUrl: Redacted.Redacted<string> | undefined, ownsDatabase: boolean) => Layer.effect(DatabaseService, Effect.gen(function*() {
  const url = configuredUrl ?? (yield* DatabaseService.connection());
  const types = new TypeOverrides();
  types.setTypeParser(20, (value) => BigInt(value));
  const connection = new Client({ connectionString: Redacted.value(url), application_name: "coordinator", options: "-c synchronous_commit=on", types });
  const lost = yield* Deferred.make<never, DatabaseError>();
  let closed = false;
  let unavailable: DatabaseError | undefined;
  const waiters: Array<() => void> = [];
  const signalLoss = (cause: unknown) => {
    if (closed || unavailable) return;
    unavailable = databaseError("query", "connection", cause);
    Effect.runSync(Deferred.fail(lost, unavailable));
    for (const wake of waiters.splice(0)) wake();
  };
  connection.on("error", signalLoss);
  connection.on("end", () => signalLoss(new Error("PostgreSQL session ended")));
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        try {
          await connection.connect();
        } catch (cause) {
          await connection.end();
          throw cause;
        }
      },
      catch: (cause) => databaseError("connect", "connection", cause),
    }),
    () => Effect.gen(function*() {
      closed = true;
      yield* Effect.tryPromise({
        try: () => connection.end(),
        catch: (cause) => databaseError("close", "connection", cause),
      }).pipe(
        Effect.tapError((error) => Effect.logWarning(`database/${error.operation}/${error.reason}`)),
        Effect.ignore,
      );
    }),
  );

  const CurrentTransaction = Context.Reference<TransactionOwner | undefined>(
    `coordinator/Database/CurrentTransaction/${crypto.randomUUID()}`,
    { defaultValue: () => undefined },
  );
  const CurrentNested = Context.Reference<Semaphore.Semaphore | undefined>(
    `coordinator/Database/CurrentNested/${crypto.randomUUID()}`,
    { defaultValue: () => undefined },
  );

  const storage = new AsyncLocalStorage<TransactionOwner | undefined>();
  let holder: TransactionOwner | undefined;
  let unlocked = true;
  const releasePhysical = () => {
    holder = undefined;
    const next = waiters.shift();
    if (next) next();
    else unlocked = true;
  };
  const acquirePhysical = async (owner: TransactionOwner | undefined) => {
    if (unavailable) throw unavailable;
    if (holder !== undefined && owner === holder) return { release: () => {} };
    if (!unlocked) await new Promise<void>((resolve) => waiters.push(resolve));
    else unlocked = false;
    if (unavailable) throw unavailable;
    holder = owner;
    let released = false;
    return {
      release: () => {
        if (!released) {
          released = true;
          releasePhysical();
        }
      },
    };
  };
  let lastCommand: string | undefined;
  const pool = {
    options: {},
    connect: async () => {
      const lease = await acquirePhysical(storage.getStore());
      function query<R>(text: string, parameters: ReadonlyArray<unknown>): Promise<PostgresQueryResult<R>>;
      function query<R>(cursor: PostgresCursor<R>): PostgresCursor<R>;
      function query<R>(input: string | PostgresCursor<R>, parameters: ReadonlyArray<unknown> = []): Promise<PostgresQueryResult<R>> | PostgresCursor<R> {
        if (typeof input !== "string") throw databaseError("query", "execution", "Database cursor streaming is not configured");
        return connection.query<R & QueryResultRow>(input, Array.from(parameters)).then((result): PostgresQueryResult<R> => {
          lastCommand = result.command;
          return {
            command: result.command === "INSERT" || result.command === "UPDATE" || result.command === "DELETE" || result.command === "MERGE" ? result.command : "SELECT",
            rowCount: result.rowCount ?? 0,
            rows: result.rows,
          };
        }).catch((cause: unknown) => {
          if (Option.isSome(Schema.decodeUnknownOption(fatalPostgresError)(cause))) signalLoss(cause);
          throw cause;
        });
      }
      return {
        query,
        release: lease.release,
      };
    },
    end: async () => {},
  };
  const client = new Kysely<DB>({ dialect: new PostgresDialect({ pool }) });
  const query = <A>(build: (db: Kysely<DB>) => { readonly execute: () => Promise<ReadonlyArray<A>> }) => Effect.gen(function*() {
    if (yield* Deferred.isDone(lost)) return yield* Deferred.await(lost);
    const owner = yield* CurrentTransaction;
    const result = yield* Effect.exit(Effect.tryPromise({
      try: () => storage.run(owner, () => build(client).execute()),
      catch: (cause) => databaseError("query", "execution", cause),
    }).pipe(Effect.uninterruptible));
    if (Exit.isFailure(result) && (yield* Deferred.isDone(lost))) return yield* Deferred.await(lost);
    return yield* result;
  });
  const transactionOperation = <A>(operation: "transaction" | "commit" | "rollback", run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => {
        signalLoss(cause);
        return databaseError(operation, operation === "rollback" ? "interrupted" : "execution", cause);
      },
    }).pipe(Effect.uninterruptible);
  const transaction = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | DatabaseError, R> => Effect.gen(function*() {
    const inherited = yield* CurrentTransaction;
    if (inherited) {
      const nested = Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
        const transaction = inherited.transaction;
        if (!transaction) return yield* databaseError("transaction", "connection");
        const savepoint = `coordinator_nested_${inherited.nextSavepoint++}`;
        const saved = yield* transactionOperation("transaction", () => transaction.savepoint(savepoint).execute());
        const result = yield* Effect.exit(restore(effect.pipe(
          Effect.provideService(CurrentNested, yield* Semaphore.make(1)),
        )));
        yield* Exit.isSuccess(result)
          ? transactionOperation("commit", () => saved.releaseSavepoint(savepoint).execute())
          : transactionOperation("rollback", () => saved.rollbackToSavepoint(savepoint).execute());
        return yield* result;
      }));
      const permit = (yield* CurrentNested) ?? inherited.nested;
      return yield* permit.withPermit(nested);
    }
    const owner: TransactionOwner = {
      transaction: undefined,
      nested: yield* Semaphore.make(1),
      nextSavepoint: 0,
    };
    return yield* Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
      const transaction = yield* transactionOperation("transaction", () =>
        storage.run(owner, () => client.startTransaction().execute()));
      owner.transaction = transaction;
      const result = yield* Effect.exit(restore(effect.pipe(Effect.provideService(CurrentTransaction, owner))));
      if (Exit.isSuccess(result)) {
        yield* transactionOperation("commit", () => transaction.commit().execute());
        if (lastCommand === "ROLLBACK") return yield* databaseError("commit", "transaction_aborted");
      } else {
        yield* transactionOperation("rollback", () => transaction.rollback().execute());
      }
      return yield* result;
    }));
  });
  const service: DatabaseClient = { url, query, transaction };
  ownerLoss.set(service, Deferred.await(lost));

  const durability = yield* query((db) => db.selectNoFrom((eb) => [
    eb.fn<string>("current_setting", [eb.val("fsync")]).as("fsync"),
    eb.fn<string>("current_setting", [eb.val("full_page_writes")]).as("full_page_writes"),
    eb.fn<string>("current_setting", [eb.val("synchronous_commit")]).as("synchronous_commit"),
  ]));
  if (durability[0]?.fsync !== "on" || durability[0]?.full_page_writes !== "on" || durability[0]?.synchronous_commit !== "on")
    return yield* databaseError("durability", "durability_required", undefined);
  if (ownsDatabase) {
    const ownership = yield* query((db) => db.selectNoFrom((eb) =>
      eb.fn<boolean>("pg_try_advisory_lock", [eb.cast(eb.val(0x434f4f52), "integer"), eb.cast(eb.val(0x42524b52), "integer")]).as("owned")));
    if (ownership[0]?.owned !== true) return yield* databaseError("ownership", "owner_unavailable", undefined);
  }
  return service;
}));

export class DatabaseService extends Context.Service<DatabaseService, DatabaseClient>()("coordinator/Database") {
  static connection(name = "COORDINATOR_DATABASE_URL") {
    return Effect.gen(function*() {
      const connection = yield* Config.Redacted(name).pipe(Effect.mapError(() => new DatabaseConnectionError()));
      yield* Effect.try({
        try: () => {
          const value = Redacted.value(connection);
          const url = new URL(value);
          const parts = [decodeURIComponent(url.username), decodeURIComponent(url.password), decodeURIComponent(url.pathname.slice(1))];
          const local = ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname);
          if (!/^postgres(?:ql)?:\/\//i.test(value) || /[\u0000-\u0020\u007f]/.test(value) ||
              !url.hostname || url.hash || url.pathname.slice(1).includes("/") ||
              parts.some((part) => !part || /[\u0000-\u001f\u007f]/.test(part)) ||
              parts[2]?.includes("/") || url.searchParams.size !== 1 ||
              !(url.searchParams.get("sslmode") === "verify-full" || (local && url.searchParams.get("sslmode") === "disable")))
            throw new DatabaseConnectionError();
        },
        catch: () => new DatabaseConnectionError(),
      });
      return connection;
    });
  }
  static layer(url?: Redacted.Redacted<string>) { return makeLayer(url, true); }
  static adminLayer(url?: Redacted.Redacted<string>) { return makeLayer(url, false); }
}

export const brokerStorageHash = contract.storage.storageHash;

// Prisma owns this namespace. Project only its public native metadata types;
// rc.17 exports no snake-cased ledger row type, so map the selected columns.
type PrismaMetadata = {
  "prisma_contract.marker": Pick<ContractMarkerRow, "core_hash" | "profile_hash"> & Pick<LedgerEntryRecord, "space">;
};

// Recovery compares the same identity while its durable gate intentionally fences serving.
export const brokerSchemaIdentity = (db: DatabaseClient) => Effect.gen(function*() {
  const markers = yield* db.query((client) => client.withTables<PrismaMetadata>().selectFrom("prisma_contract.marker")
    .select(["space", "core_hash", "profile_hash"]).orderBy("space"));
  if (markers.length !== 1 || markers[0]?.space !== "app" ||
      markers[0].core_hash !== brokerStorageHash || markers[0].profile_hash !== contract.profileHash) {
    // Prisma configuration must load this owner before generated runtime schemas exist.
    const { HttpError } = yield* Effect.promise(() => import("./contracts.ts"));
    return yield* new HttpError({ status: 503, message: "Coordinator Prisma contract mismatch; apply exact committed migrations" });
  }
  yield* db.query((client) => client.selectFrom("dispatch_queue").selectAll().limit(0));
  return `${brokerStorageHash}:profile=${contract.profileHash}`;
});

export const assertBrokerSchema = (db: DatabaseClient) => Effect.gen(function*() {
  yield* brokerSchemaIdentity(db);
  const gate = yield* db.query((client) => client.selectFrom("recovery_gate").selectAll());
  if (gate.length > 1 || gate.some((row) => row.id !== 1 || row.ready !== true)) {
    const { HttpError } = yield* Effect.promise(() => import("./contracts.ts"));
    return yield* new HttpError({ status: 503, message: "Broker recovery incomplete; serving remains fenced" });
  }
});

export const assertRunnerSchema = (db: DatabaseClient) => Effect.gen(function*() {
  yield* assertBrokerSchema(db);
  yield* db.query((client) => client.selectFrom("runner_tasks").selectAll().limit(0));
  yield* db.query((client) => client.selectFrom("runner_events").selectAll().limit(0));
});
