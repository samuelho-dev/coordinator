import { Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { Kysely, PostgresDialect, type Compilable } from "kysely";
import type { DB } from "./generated/database/types.ts";
export interface DatabaseClient {
  readonly client: Kysely<DB>;
  readonly execute: <A extends object>(query: Compilable<A>) => Effect.Effect<ReadonlyArray<A>, SqlError>;
}

export const makeDatabase = (sql: SqlClient.SqlClient): DatabaseClient => {
  // Effect owns the physical session and transactions; Kysely compiles typed queries.
  const client = new Kysely<DB>({
    dialect: new PostgresDialect({
      pool: () => Promise.reject(new Error("Execute PostgreSQL queries through Database.execute")),
    }),
  });
  const execute = <A extends object>(query: Compilable<A>): Effect.Effect<ReadonlyArray<A>, SqlError> => Effect.suspend(() => {
    const compiled = query.compile();
    return sql.unsafe<A>(compiled.sql, compiled.parameters);
  });
  return { client, execute };
};

export class Database extends Context.Service<Database, DatabaseClient>()("coordinator/Database") {
  static readonly layer = Layer.effect(this, Effect.map(SqlClient.SqlClient, makeDatabase));
}
