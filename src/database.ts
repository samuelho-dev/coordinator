import { Effect } from "effect";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

const brokerMigrations = Migrator.fromRecord({
  "0001_relational_baseline": Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    for (const statement of [
      "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS peers (id TEXT PRIMARY KEY, owner TEXT NOT NULL, sender TEXT NOT NULL, session TEXT NOT NULL, incarnation TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL, expires REAL NOT NULL)",
      "CREATE TABLE IF NOT EXISTS consents (owner TEXT NOT NULL, session TEXT NOT NULL, enabled INTEGER NOT NULL, participants TEXT NOT NULL, PRIMARY KEY(owner,session))",
      "CREATE TABLE IF NOT EXISTS participant_consents (owner TEXT NOT NULL, session TEXT NOT NULL, participant TEXT NOT NULL, enabled INTEGER NOT NULL, PRIMARY KEY(owner,session,participant))",
      "CREATE TABLE IF NOT EXISTS archive (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, session TEXT NOT NULL, entry TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL, gaps TEXT NOT NULL, created REAL NOT NULL, UNIQUE(owner,session,entry,digest))",
      "CREATE TABLE IF NOT EXISTS evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL, kind TEXT NOT NULL, sha TEXT NOT NULL, principal TEXT NOT NULL, data TEXT NOT NULL, observed REAL NOT NULL)"
    ]) yield* sql.unsafe(statement);
  })
});

const runnerMigrations = Migrator.fromRecord({
  "0001_relational_baseline": Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    for (const statement of [
      `CREATE TABLE IF NOT EXISTS tasks (taskId TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, work TEXT NOT NULL,
        worktree TEXT NOT NULL, state TEXT NOT NULL, occupied INTEGER NOT NULL, pid INTEGER,
        incarnation TEXT NOT NULL, address TEXT, promptId TEXT, reason TEXT, result TEXT, detached INTEGER NOT NULL DEFAULT 0)`,
      "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, taskId TEXT NOT NULL, payload TEXT NOT NULL)",
      "CREATE INDEX IF NOT EXISTS events_task ON events(taskId,seq)"
    ]) yield* sql.unsafe(statement);
    const columns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(tasks)");
    if (!columns.some((column) => column.name === "detached")) yield* sql.unsafe("ALTER TABLE tasks ADD COLUMN detached INTEGER NOT NULL DEFAULT 0");
    yield* sql.unsafe("DROP TABLE IF EXISTS owner");
  })
});

// Connection pragmas and runner privacy scrub/VACUUM stay outside migration transactions.
export const migrateBroker = (sql: (typeof SqliteClient.SqliteClient)["Service"]) =>
  Migrator.make({})({ loader: brokerMigrations, table: "coordinator_broker_migrations" }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql)
  );

export const migrateRunner = (sql: (typeof SqliteClient.SqliteClient)["Service"]) =>
  Migrator.make({})({ loader: runnerMigrations, table: "coordinator_runner_migrations" }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql)
  );
