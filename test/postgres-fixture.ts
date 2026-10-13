import { Config, Effect, Layer, Redacted } from "effect";
import { Reactivity } from "effect/reactivity";
import { SqlClient } from "effect/sql";
import * as PgClient from "@effect/sql-pg/PgClient";
import { Platform } from "../src/platform.ts";
import { readDatabaseUrl } from "../src/postgres.ts";
import { migrateBrokerPostgres } from "../scripts/migrate.ts";

/** Independent administration connection; never takes the broker owner lock. */
export const postgresAdmin = (urlFile: string) => Layer.unwrap(Effect.gen(function*() {
  const url = yield* readDatabaseUrl(urlFile);
  return PgClient.layer({ url, maxConnections: 1 }).pipe(Layer.provide(Reactivity.layer));
}));

const adminFile = Config.String("COORDINATOR_TEST_POSTGRES_URL_FILE").pipe(
  Effect.mapError(() => new Error("COORDINATOR_TEST_POSTGRES_URL_FILE is required: private mode-0600 admin DSN file for disposable PostgreSQL 18")),
);

/** Creates a unique database on the disposable test server and applies production migrations. */
export const provisionPostgres = (directory: string) => Effect.gen(function*() {
  const platform = yield* Platform;
  const baseFile = yield* adminFile;
  const base = yield* readDatabaseUrl(baseFile);
  const name = `coordinator_test_${(yield* platform.crypto.randomUUIDv4()).replaceAll("-", "")}`;
  yield* Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    const version = yield* sql.unsafe<{ server_version_num: string }>("SHOW server_version_num");
    if (!(Number(version[0]?.server_version_num) >= 180000 && Number(version[0]?.server_version_num) < 190000)) {
      return yield* Effect.fail(new Error("Disposable PostgreSQL 18 is required"));
    }
    yield* sql.unsafe(`CREATE DATABASE "${name}"`);
  }).pipe(Effect.provide(postgresAdmin(baseFile)), Effect.scoped);
  const url = new URL(Redacted.value(base));
  url.pathname = `/${name}`;
  const urlFile = platform.path.join(directory, "database.url");
  yield* platform.fs.writeFileString(urlFile, url.href, { mode: 0o600 });
  yield* platform.fs.chmod(urlFile, 0o600);
  yield* migrateBrokerPostgres(urlFile);
  return urlFile;
});

/** Run after broker runtimes close and before removing the URL file. */
export const dropPostgres = (urlFile: string) => Effect.gen(function*() {
  const url = new URL(Redacted.value(yield* readDatabaseUrl(urlFile)));
  const name = url.pathname.slice(1);
  if (!/^coordinator_test_[a-f0-9]{32}$/.test(name)) return yield* Effect.fail(new Error("Refusing to drop a non-fixture PostgreSQL database"));
  const baseFile = yield* adminFile;
  yield* Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(`DROP DATABASE "${name}" WITH (FORCE)`);
  }).pipe(Effect.provide(postgresAdmin(baseFile)), Effect.scoped);
});
