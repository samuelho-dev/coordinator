import { Worker } from "node:worker_threads";
import * as NodeWorker from "@effect/platform-node/NodeWorker";
import { Context, Deferred, Effect, Exit, Layer, Schema, Semaphore, Stream } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcGroup from "effect/rpc/RpcGroup";
import type { RpcClientError } from "effect/rpc/RpcClientError";
import * as Client from "effect/sql/SqlClient";
import type { Connection } from "effect/sql/SqlConnection";
import { ConnectionError, SqlError } from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";

export const SqliteConfig = Schema.Struct({
  filename: Schema.String,
  disableWAL: Schema.optional(Schema.Boolean),
  readonly: Schema.optional(Schema.Boolean)
});

const query = {
  sql: Schema.String,
  params: Schema.Array(Schema.Unknown),
  safeIntegers: Schema.Boolean
};
const rows = Schema.Array(Schema.Record(Schema.String, Schema.Unknown));
const values = Schema.Array(Schema.Array(Schema.Unknown));
const BackupMetadata = Schema.Struct({ totalPages: Schema.Number, remainingPages: Schema.Number });

export const SqliteRpc = RpcGroup.make(
  Rpc.make("Ready", { error: SqlError }),
  Rpc.make("Execute", { payload: query, success: rows, error: SqlError }),
  Rpc.make("ExecuteRaw", { payload: query, success: Schema.Unknown, error: SqlError }),
  Rpc.make("ExecuteValues", { payload: query, success: values, error: SqlError }),
  Rpc.make("ExecuteValuesUnprepared", { payload: query, success: values, error: SqlError }),
  Rpc.make("ExecuteUnprepared", { payload: query, success: rows, error: SqlError }),
  Rpc.make("Backup", { payload: { destination: Schema.String }, success: BackupMetadata, error: SqlError })
);

export class Sqlite extends Context.Service<Sqlite, {
  readonly sql: Client.SqlClient;
  readonly backup: (destination: string) => Effect.Effect<typeof BackupMetadata.Type, SqlError>;
  readonly failure: Effect.Effect<never, SqlError>;
}>()("coordinator/Sqlite") {
  static layer(options: typeof SqliteConfig.Type): Layer.Layer<Sqlite | Client.SqlClient, SqlError> {
    return Layer.effectContext(Effect.gen(function*() {
      const lost = Deferred.makeUnsafe<never, SqlError>();
      const ownerLost = (cause: unknown) => new SqlError({
        reason: new ConnectionError({ cause, message: "SQLite worker owner lost; connection cannot be reused", operation: "execute" })
      });
      let spawned = false;
      // ponytail: one database owner; never replace a worker with an uncertain transaction.
      const workers = NodeWorker.layer(() => {
        if (spawned) throw ownerLost("SQLite worker replacement is forbidden");
        spawned = true;
        // tsImport installs the existing tsx loader inside the worker, including dependency TypeScript.
        const entry = new URL("./sqlite-worker.ts", import.meta.url).href;
        const worker = new Worker(`import("tsx/esm/api").then(({ tsImport }) => tsImport(${JSON.stringify(entry)}, ${JSON.stringify(import.meta.url)}))`, {
          eval: true,
          workerData: options
        });
        worker.on("error", (cause) => Deferred.doneUnsafe(lost, Exit.fail(ownerLost(cause))));
        worker.on("exit", (code) => Deferred.doneUnsafe(lost, Exit.fail(ownerLost(`Worker exited with code ${code}`))));
        return worker;
      });
      const protocol = yield* Layer.build(
        RpcClient.layerProtocolWorker({ size: 1 }).pipe(Layer.provide(workers))
      ).pipe(Effect.mapError(ownerLost));
      const transport = Context.get(protocol, RpcClient.Protocol);
      // Iso codecs reconstruct RPC exits/errors while structured clone preserves native bigint/blob values.
      const rpc = yield* RpcClient.make(SqliteRpc).pipe(Effect.provideService(RpcClient.Protocol, {
        ...transport, codecFor: Schema.toCodecIso
      }));
      const call = <A>(operation: Effect.Effect<A, SqlError | RpcClientError>) =>
        Effect.uninterruptible(Effect.raceFirst(operation, Deferred.await(lost))).pipe(
          Effect.catchTag("RpcClientError", (cause) => {
            const error = ownerLost(cause);
            Deferred.doneUnsafe(lost, Exit.fail(error));
            return Effect.fail(error);
          })
        );
      yield* call(rpc.Ready());
      const payload = (sql: string, params: ReadonlyArray<unknown>) => Effect.map(
        Effect.service(Client.SafeIntegers),
        (safeIntegers) => ({ sql, params, safeIntegers })
      );
      const connection: Connection = {
        execute: (sql, params, transformRows) => Effect.flatMap(payload(sql, params), (request) =>
          call(rpc.Execute(request)).pipe(Effect.map((result) => transformRows ? transformRows(result) : result))),
        executeRaw: (sql, params) => Effect.flatMap(payload(sql, params), (request) => call(rpc.ExecuteRaw(request))),
        executeValues: (sql, params) => Effect.flatMap(payload(sql, params), (request) => call(rpc.ExecuteValues(request))),
        executeValuesUnprepared: (sql, params) => Effect.flatMap(payload(sql, params), (request) => call(rpc.ExecuteValuesUnprepared(request))),
        executeUnprepared: (sql, params, transformRows) => Effect.flatMap(payload(sql, params), (request) =>
          call(rpc.ExecuteUnprepared(request)).pipe(Effect.map((result) => transformRows ? transformRows(result) : result))),
        executeStream: () => Stream.die("executeStream not implemented")
      };
      const { transactionAcquirer, onCommitFailure } = Client.makeSqliteAcquirers({
        connection: Effect.succeed(connection),
        semaphore: yield* Semaphore.make(1)
      });
      const sql = yield* Client.make({
        // Unlike synchronous execution, the wire must retain the lease until the statement finishes.
        acquirer: transactionAcquirer,
        transactionAcquirer,
        onCommitFailure,
        compiler: Statement.makeCompilerSqlite(),
        releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
        beginTransaction: options.readonly === true ? "BEGIN" : "BEGIN IMMEDIATE",
        spanAttributes: [["db.system.name", "sqlite"]]
      });
      return Context.make(Sqlite, {
        sql,
        failure: Deferred.await(lost),
        backup: (destination) => Effect.scoped(Effect.andThen(transactionAcquirer, call(rpc.Backup({ destination }))))
      }).pipe(Context.add(Client.SqlClient, sql));
    })).pipe(Layer.provide(Reactivity.layer));
  }
}
