import { ConfigProvider, Effect, Redacted } from "effect";
import { sql } from "kysely";
import { DatabaseService } from "../src/database-service.ts";
import { Platform } from "../src/platform.ts";
import { migrateBrokerPostgres } from "../scripts/migrate.ts";

/** Independent administration connection; never takes the runtime owner lock. */
export const postgresAdmin = DatabaseService.adminLayer;

export const postgresConfig = (url: Redacted.Redacted<string>, runnerUrl?: Redacted.Redacted<string>) =>
  ConfigProvider.layerAdd(ConfigProvider.fromUnknown({
    COORDINATOR_DATABASE_URL: Redacted.value(url),
    ...(runnerUrl ? { COORDINATOR_RUNNER_DATABASE_URL: Redacted.value(runnerUrl) } : {}),
  }), { asPrimary: true });

/** Creates a pristine disposable database and applies the committed initial PostgreSQL schema. */
export const provisionPostgres = () => Effect.gen(function*() {
  const platform = yield* Platform;
  const base = yield* DatabaseService.connection("COORDINATOR_TEST_POSTGRES_URL");
  const name = `coordinator_test_${(yield* platform.crypto.randomUUIDv4()).replaceAll("-", "")}`;
  const password = yield* platform.crypto.randomUUIDv4();
  yield* Effect.gen(function*() {
    const database = yield* DatabaseService;
    const version = yield* database.query(db => ({ execute: async () => (await sql<{ server_version_num: string }>`SHOW server_version_num`.execute(db)).rows }));
    if (!(Number(version[0]?.server_version_num) >= 180000 && Number(version[0]?.server_version_num) < 190000)) {
      return yield* Effect.fail(new Error("Disposable PostgreSQL 18 is required"));
    }
    yield* database.query(db => ({ execute: async () => (await sql`CREATE ROLE ${sql.id(name)} LOGIN PASSWORD ${sql.lit(password)}`.execute(db)).rows }));
    yield* database.query(db => ({ execute: async () => (await sql`CREATE DATABASE ${sql.id(name)} OWNER ${sql.id(name)}`.execute(db)).rows }));
  }).pipe(Effect.provide(postgresAdmin(base)), Effect.scoped);
  const url = new URL(Redacted.value(base));
  url.pathname = `/${name}`;
  url.username = name;
  url.password = password;
  const connection = Redacted.make(url.href);
  yield* migrateBrokerPostgres(connection);
  return connection;
});

/** Run after runtime owners close. */
export const dropPostgres = (connection: Redacted.Redacted<string>) => Effect.gen(function*() {
  const url = new URL(Redacted.value(connection));
  const name = url.pathname.slice(1);
  if (!/^coordinator_test_[a-f0-9]{32}$/.test(name)) return yield* Effect.fail(new Error("Refusing to drop a non-fixture PostgreSQL database"));
  const base = yield* DatabaseService.connection("COORDINATOR_TEST_POSTGRES_URL");
  yield* Effect.gen(function*() {
    const database = yield* DatabaseService;
    yield* database.query(db => ({ execute: async () => (await sql`DROP DATABASE ${sql.id(name)} WITH (FORCE)`.execute(db)).rows }));
    yield* database.query(db => ({ execute: async () => (await sql`DROP ROLE ${sql.id(name)}`.execute(db)).rows }));
  }).pipe(Effect.provide(postgresAdmin(base)), Effect.scoped);
});
