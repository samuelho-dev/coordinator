import { Context, Deferred, Effect, Layer, Option, Queue, Redacted, Semaphore, Stream } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Client from "effect/sql/SqlClient";
import type { Connection } from "effect/sql/SqlConnection";
import { ConnectionError, SqlError, UnknownError } from "effect/sql/SqlError";
import * as PgConnection from "@effect/sql-pg/PgConnection";
import * as PgClient from "@effect/sql-pg/PgClient";
import { HttpError } from "./contracts.ts";
import { Platform } from "./platform.ts";
import { Database, makeDatabase } from "./kysely.ts";

export const readDatabaseUrl = Effect.fn("Postgres.databaseUrl")(function*(file: string) {
  const { fs, path } = yield* Platform;
  const invalid = () => new HttpError({ status: 400, message: "Canonical operator-owned private PostgreSQL URL file required" });
  if (!path.isAbsolute(file) || path.resolve(file) !== file || (yield* fs.realPath(file)) !== file) return yield* invalid();
  const info = yield* fs.stat(file);
  const parent = yield* fs.stat(path.dirname(file));
  const uid = process.getuid?.();
  if (uid === undefined || info.type !== "File" || Option.isNone(info.uid) || info.uid.value !== uid ||
      Option.isNone(info.nlink) || info.nlink.value !== 1 || (info.mode & 0o7777) !== 0o600 || info.size > 8192 ||
      parent.type !== "Directory" || Option.isNone(parent.uid) || parent.uid.value !== uid || (parent.mode & 0o7777) !== 0o700) return yield* invalid();
  const value = (yield* fs.readFileString(file)).trim();
  yield* Effect.try({
    try: () => {
      const url = new URL(value);
      const parts = [decodeURIComponent(url.username), decodeURIComponent(url.password), decodeURIComponent(url.pathname.slice(1))];
      const local = ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname);
      if (!/^postgres(?:ql)?:\/\//i.test(value) || !url.hostname || url.hash || url.pathname.slice(1).includes("/") ||
          parts.some((part) => !part || /[\u0000-\u001f\u007f]/.test(part)) || url.searchParams.size !== 1 ||
          !(url.searchParams.get("sslmode") === "verify-full" || (local && url.searchParams.get("sslmode") === "disable"))) throw invalid();
    },
    catch: invalid,
  });
  return Redacted.make(value);
});

export class Postgres extends Context.Service<Postgres, {
  readonly sql: Client.SqlClient;
  readonly failure: Effect.Effect<never, SqlError>;
}>()("coordinator/Postgres") {
  static layer(databaseUrlFile: string) {
    return Layer.effectContext(Effect.gen(function*() {
      const url = yield* readDatabaseUrl(databaseUrlFile);
      // ponytail: one physical owner, not a reconnecting pool; epoch fencing is needed only for multi-owner operation.
      const base = yield* PgConnection.make({ url, applicationName: "coordinator", startupParameters: { synchronous_commit: "on" } });
      const pg = yield* base.pin;
      const lock = yield* pg.query("SELECT pg_try_advisory_lock($1::integer,$2::integer) AS owned, current_setting('fsync') AS fsync, current_setting('full_page_writes') AS full_page_writes", [0x434f4f52, 0x42524b52]);
      if (lock.rows[0]?.owned !== true) return yield* new HttpError({ status: 409, message: "Coordinator PostgreSQL database already has an owner" });
      if (lock.rows[0]?.fsync !== "on" || lock.rows[0]?.full_page_writes !== "on") return yield* new HttpError({ status: 503, message: "Durable PostgreSQL fsync and full_page_writes required" });
      const lost = yield* Deferred.make<never, SqlError>();
      const notifications = yield* pg.listen("coordinator_owner");
      yield* Queue.take(notifications).pipe(Effect.forever, Effect.catch((error) => Deferred.fail(lost, error)), Effect.forkScoped({ startImmediately: true }));
      // Retained sessions cannot safely reuse a late CancelRequest: ordinary wire cycles finish before interruption.
      const wire = <A>(operation: Effect.Effect<A, SqlError>) => Effect.uninterruptible(Effect.flatMap(Deferred.isDone(lost), (done) =>
        done ? Deferred.await(lost) : Effect.raceFirst(operation, Deferred.await(lost))));
      const connection: Connection = {
        execute: (sql, params, transformRows) => wire(pg.query(sql, params)).pipe(Effect.map((result) => transformRows ? transformRows(result.rows) : result.rows)),
        executeRaw: (sql, params) => wire(pg.query(sql, params)),
        executeValues: (sql, params) => wire(pg.queryValues(sql, params)),
        executeValuesUnprepared: (sql, params) => wire(pg.queryValues(sql, params, false)),
        executeUnprepared: (sql, params, transformRows) => wire(pg.query(sql, params, false)).pipe(Effect.map((result) => transformRows ? transformRows(result.rows) : result.rows)),
        executeStream: (sql, params, transformRows) => Stream.suspend(() => {
          let completed = false;
          const rows = pg.stream(sql, params);
          return Stream.concat(transformRows ? Stream.flattenIterable(Stream.map(Stream.chunks(rows), transformRows)) : rows, Stream.unwrap(Effect.sync(() => {
            completed = true;
            return Stream.empty;
          }))).pipe(
            Stream.interruptWhen(Deferred.await(lost)),
            Stream.onExit(() => completed ? Effect.void : Deferred.fail(lost, new SqlError({
              reason: new ConnectionError({ cause: "Incomplete streaming query", message: "PostgreSQL owner retired after incomplete stream; no session reuse", operation: "stream" }),
            }))),
          );
        }),
      };
      const semaphore = yield* Semaphore.make(1);
      const acquirer = Effect.acquireRelease(Effect.interruptible(semaphore.take(1)), () => semaphore.release(1)).pipe(Effect.as(connection));
      const sql = yield* Client.make({
        acquirer,
        borrower: (use) => semaphore.withPermits(1)(use(connection)),
        transactionAcquirer: acquirer,
        compiler: PgClient.makeCompiler(),
        prepareTransactionControls: true,
        releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
        commit: () => wire(pg.query("COMMIT")).pipe(Effect.flatMap((result) => result.command === "ROLLBACK"
          ? Effect.fail(new SqlError({ reason: new UnknownError({ cause: "COMMIT rolled back an aborted transaction", message: "PostgreSQL COMMIT rolled back an aborted transaction", operation: "commit" }) }))
          : Effect.void)),
        spanAttributes: [["db.system.name", "postgresql"]],
      });
      return Context.make(Postgres, { sql, failure: Deferred.await(lost) }).pipe(
        Context.add(Client.SqlClient, sql),
        Context.add(Database, makeDatabase(sql)),
      );
    })).pipe(Layer.provide(Reactivity.layer));
  }
}
