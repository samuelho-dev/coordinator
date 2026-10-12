import { workerData } from "node:worker_threads";
import * as NativeSqlite from "@effect/sql-sqlite-node/SqliteClient";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import { Context, Effect, Layer, Schema } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as RpcServer from "effect/rpc/RpcServer";
import * as Client from "effect/sql/SqlClient";
import { SqliteConfig, SqliteRpc } from "./sqlite.ts";

const handlers = SqliteRpc.toLayer(Effect.gen(function*() {
  const options = yield* Schema.decodeUnknownEffect(SqliteConfig)(workerData);
  // Retain open failures for Ready so callers receive the original typed SQL reason.
  const opened = yield* Effect.exit(NativeSqlite.make(options));
  return {
    Ready: () => Effect.asVoid(opened),
    Execute: ({ sql, params, safeIntegers }) => Effect.flatMap(opened, (client) =>
      Effect.scoped(Effect.flatMap(client.reserve, (connection) => connection.execute(sql, params, undefined))).pipe(
        Effect.provideService(Client.SafeIntegers, safeIntegers))),
    ExecuteRaw: ({ sql, params, safeIntegers }) => Effect.flatMap(opened, (client) =>
      Effect.scoped(Effect.flatMap(client.reserve, (connection) => connection.executeRaw(sql, params))).pipe(
        Effect.provideService(Client.SafeIntegers, safeIntegers))),
    ExecuteValues: ({ sql, params, safeIntegers }) => Effect.flatMap(opened, (client) =>
      Effect.scoped(Effect.flatMap(client.reserve, (connection) => connection.executeValues(sql, params))).pipe(
        Effect.provideService(Client.SafeIntegers, safeIntegers))),
    ExecuteValuesUnprepared: ({ sql, params, safeIntegers }) => Effect.flatMap(opened, (client) =>
      Effect.scoped(Effect.flatMap(client.reserve, (connection) => connection.executeValuesUnprepared(sql, params))).pipe(
        Effect.provideService(Client.SafeIntegers, safeIntegers))),
    ExecuteUnprepared: ({ sql, params, safeIntegers }) => Effect.flatMap(opened, (client) =>
      Effect.scoped(Effect.flatMap(client.reserve, (connection) => connection.executeUnprepared(sql, params, undefined))).pipe(
        Effect.provideService(Client.SafeIntegers, safeIntegers))),
    Backup: ({ destination }) => Effect.flatMap(opened, (client) => client.backup(destination))
  };
})).pipe(Layer.provide(Reactivity.layer));

NodeRuntime.runMain(Layer.launch(RpcServer.layer(SqliteRpc, { concurrency: 1 }).pipe(
  Layer.provide(handlers),
  Layer.provide(RpcServer.layerProtocolWorkerRunner.pipe(
    Layer.provide(NodeWorkerRunner.layer),
    Layer.flatMap((context) => Layer.succeed(RpcServer.Protocol, {
      ...Context.get(context, RpcServer.Protocol), codecFor: Schema.toCodecIso
    }))
  ))
)));
