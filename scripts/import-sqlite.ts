import { parseArgs } from "node:util";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { pathToFileURL } from "node:url";
import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { sql as expression } from "kysely";
import { NodeRuntime } from "@effect/platform-node";
import { Address, BackupError, BrokerConfig, JsonValue, LiveSession, RunnerConfig, RunnerEvent, RunnerResult, Sha, Task, Work } from "../src/contracts.ts";
import { NativeBinding, NativeRunId, NativeSubmissionId, NativeEventSequence } from "@samuelho/coordinator-native-control/contracts";
import { emptyBrokerDatabase, fenceBroker, hashFile, holdBrokerAccepted, holdRunnerAccepted, io, openBrokerGate, privateRecoveryPath, recoverOwned, recoveryError } from "../src/backup.ts";
import { Platform } from "../src/platform.ts";
import { Postgres } from "../src/postgres.ts";
import { makeDatabase } from "../src/kysely.ts";
import { assertRunnerSchema } from "../src/database.ts";
import * as Model from "../src/generated/database/types.ts";

const Cell = Schema.Union([Schema.String, Schema.Number, Schema.BigInt, Schema.Null]);
const Cells = Schema.Record(Schema.String, Cell);
const LegacyTask = Schema.Struct({ id: Schema.String, data: Schema.String });
// Legacy journals may contain transcripts. Decode only canonical metadata at this untrusted boundary.
const { reason: omittedReason, contentOmitted: omittedContent, ...legacyEventFields } = RunnerEvent.fields;
const LegacyEventMetadata = Schema.Struct(legacyEventFields);
export class LegacySqlite extends Context.Service<LegacySqlite>()("coordinator/offline/LegacySqlite", {
  make: (filename: string, readonly: boolean) => Effect.gen(function*() {
    const database = yield* Effect.acquireRelease(Effect.try({ try: () => new DatabaseSync(filename, { readOnly: readonly }), catch: () => recoveryError("Legacy SQLite open failed") }), (database) => Effect.sync(() => database.close()));
    const query = <S extends Schema.Top>(schema: S, statement: string, params: ReadonlyArray<SQLInputValue> = [], bigints = false) => Effect.try({
      try: () => { const query = database.prepare(statement); query.setReadBigInts(bigints); return query.all(...params); },
      catch: () => recoveryError("Legacy SQLite query failed"),
    }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(schema))));
    const execute = (statement: string, params: ReadonlyArray<SQLInputValue> = []) => Effect.try({
      try: () => database.prepare(statement).run(...params),
      catch: () => recoveryError("Legacy SQLite write failed"),
    });
    return { query, execute };
  }),
}) {
  static layer(filename: string, readonly = false) { return Layer.effect(this, this.make(filename, readonly)); }
}
const brokerTables: ReadonlyArray<"tasks" | "sessions" | "peers" | "consents" | "participant_consents" | "archive" | "evidence" | "effect_queue"> = ["tasks", "sessions", "peers", "consents", "participant_consents", "archive", "evidence", "effect_queue"];
export const noSidecars = (file: string) => Effect.gen(function*() {
  for (const suffix of ["-wal", "-shm", "-journal"]) yield* privateRecoveryPath(`${file}${suffix}`, "file", true);
});

const usage = "Usage: tsx scripts/import-sqlite.ts --source /private/ledger.sqlite --broker-config /private/broker.json [--runner-source /private/runner.sqlite --runner-config /private/runner.json] --artifact /private/new-artifact --source-sha <40-lowercase-hex> --offline-maintenance [--hold-accepted-work]. Stop every broker/runner/native child first. Source flock and SQLite exclusive checkpoint preserve committed WAL. Targets must be separate fresh empty exactly-migrated PostgreSQL databases. Accepted/running work is held without replay; private paired files are archived for reconciliation. Source data is never deleted.";
process.umask(0o077);
const cli = Effect.scoped(Effect.gen(function*() {
  const args = yield* Effect.try({ try: () => parseArgs({ options: {
    source: { type: "string" }, "broker-config": { type: "string" }, "runner-config": { type: "string" }, "runner-source": { type: "string" },
    artifact: { type: "string" }, "source-sha": { type: "string" }, "offline-maintenance": { type: "boolean" },
    "hold-accepted-work": { type: "boolean" }, help: { type: "boolean" },
  } }), catch: () => new BackupError({ message: usage }) });
  if (args.values.help) { console.log(usage); return; }
  const source = args.values.source, configPath = args.values["broker-config"], artifact = args.values.artifact, sha = args.values["source-sha"];
  if (!source || !configPath || !artifact || !sha || !args.values["offline-maintenance"]) return yield* new BackupError({ message: usage });
  const sourceSha = yield* Schema.decodeUnknownEffect(Sha)(sha);
  const platform = yield* Platform;
  yield* privateRecoveryPath(configPath, "file");
  const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configPath));
  let runner: RunnerConfig | undefined;
  const runnerPath = args.values["runner-config"];
  if (runnerPath) {
    yield* privateRecoveryPath(runnerPath, "file");
    runner = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(yield* platform.privateFile(runnerPath));
  }
  if (runner && [runner.childDirectory, ...Object.values(runner.repositories)].some((root) => [configPath, runnerPath].some((file) => file && (file === root || file.startsWith(`${root}/`))))) return yield* new BackupError({ message: "Operator configs must remain outside archived runner roots" });
  const metadata = yield* importLegacySqlite(source, config, artifact, sourceSha, runner, args.values["hold-accepted-work"] === true, args.values["runner-source"]);
  console.log(JSON.stringify({ operation: "offline-sqlite-import", sourcePreserved: true, sourceSha: metadata.sourceSha, artifact, metadataVersion: metadata.version,
    brokerRows: metadata.broker.rows, brokerRowsFencedHeld: metadata.broker.pendingAccepted, runnerRows: metadata.runner?.rows ?? null,
    maintenance: "Keep legacy source and paired runner stopped. Target gate opens only after exact row validation and complete private paired artifact creation. Reconcile occupied/accepted native uncertainty manually; never replay. On failure discard only the disposable target, never the source." }));
})).pipe(Effect.provide(Platform.layer));
// Frozen supported offline SQLite contracts, not a second live broker implementation.
export const legacyBrokerSql = (queueVersion: 1 | 2): ReadonlyArray<string> => [
  "CREATE TABLE tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
  "CREATE TABLE sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
  "CREATE TABLE peers (id TEXT PRIMARY KEY, owner TEXT NOT NULL, sender TEXT NOT NULL, session TEXT NOT NULL, incarnation TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL, expires REAL NOT NULL)",
  "CREATE TABLE consents (owner TEXT NOT NULL, session TEXT NOT NULL, enabled INTEGER NOT NULL, participants TEXT NOT NULL, PRIMARY KEY(owner,session))",
  "CREATE TABLE participant_consents (owner TEXT NOT NULL, session TEXT NOT NULL, participant TEXT NOT NULL, enabled INTEGER NOT NULL, PRIMARY KEY(owner,session,participant))",
  "CREATE TABLE archive (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, session TEXT NOT NULL, entry TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL, gaps TEXT NOT NULL, created REAL NOT NULL, UNIQUE(owner,session,entry,digest))",
  "CREATE TABLE evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL, kind TEXT NOT NULL, sha TEXT NOT NULL, principal TEXT NOT NULL, data TEXT NOT NULL, observed REAL NOT NULL)",
  ...["coordinator_broker_migrations", "effect_queue_migrations"].map((name) => `CREATE TABLE ${name} (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)`),
  queueVersion === 1 ?
    "CREATE TABLE effect_queue (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, queue_name TEXT NOT NULL, element TEXT NOT NULL, completed BOOLEAN NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_failure TEXT NULL, acquired_at DATETIME NULL, acquired_by TEXT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)" :
    "CREATE TABLE effect_queue (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, queue_name TEXT NOT NULL, element TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_failure TEXT NULL, visible_at DATETIME NOT NULL, acquired_at DATETIME NULL, acquired_by TEXT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)",
  "CREATE UNIQUE INDEX idx_effect_queue_id ON effect_queue (id, queue_name)",
  "CREATE INDEX idx_effect_queue_update ON effect_queue (sequence, acquired_by)",
  queueVersion === 1 ? "CREATE INDEX idx_effect_queue_take ON effect_queue (queue_name, completed, attempts, acquired_at)" :
    "CREATE INDEX idx_effect_queue_take ON effect_queue (queue_name, visible_at) WHERE state = 'pending'",
];

export const importLegacySqlite = (source: string, config: BrokerConfig, artifact: string, sourceSha: Sha, runner?: RunnerConfig, holdAcceptedWork = false, runnerSource?: string) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  if (runner) {
    if ([runner.childDirectory, ...Object.values(runner.repositories)].some((root) => source === root || source.startsWith(`${root}/`))) return yield* recoveryError("Legacy source must remain outside archived runner roots");
  }
  yield* privateRecoveryPath(source, "file");
  yield* privateRecoveryPath(`${source}.lock`, "file", !(yield* platform.fs.exists(`${source}.lock`)));
  const sourceLock = yield* platform.acquireJournalOwnership(`${source}.lock`);
  for (const suffix of ["-wal", "-shm", "-journal"]) {
    yield* privateRecoveryPath(`${source}${suffix}`, "file", !(yield* platform.fs.exists(`${source}${suffix}`)));
  }
  // Committed WAL is checkpointed under source flock before enforcing the standalone no-sidecars contract.
  // SQLite's own exclusive journal-mode transition fails if another uncooperative writer remains.
  yield* Effect.scoped(Effect.gen(function*() {
    const legacy = yield* LegacySqlite;
    yield* legacy.execute("PRAGMA synchronous=FULL");
    const checkpoint = yield* legacy.query(Schema.Struct({ busy: Schema.Number }), "PRAGMA wal_checkpoint(TRUNCATE)");
    if (checkpoint.some((row) => row.busy !== 0)) return yield* recoveryError("Legacy WAL checkpoint busy; stop every source writer offline");
    const mode = yield* legacy.query(Schema.Struct({ journal_mode: Schema.String }), "PRAGMA journal_mode=DELETE");
    if (mode[0]?.journal_mode !== "delete") return yield* recoveryError("Legacy standalone checkpoint failed");
  }).pipe(Effect.provide(LegacySqlite.layer(source)), io,
    Effect.mapError((error) => recoveryError(`Legacy checkpoint: ${error.message}`))));
  yield* noSidecars(source);
  const sourceDigest = yield* hashFile(source);
  const operation = Effect.scoped(Effect.gen(function*() {
    const { sql: target } = yield* Postgres;
    const db = makeDatabase(target);
    yield* emptyBrokerDatabase;
    const legacy = yield* LegacySqlite;
    const integrity = yield* legacy.query(Schema.Struct({ integrity_check: Schema.String }), "PRAGMA integrity_check");
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") return yield* recoveryError("Invalid legacy SQLite integrity");
    const Migration = Schema.Struct({ migration_id: Schema.Number, name: Schema.String });
    const brokerVersions = yield* legacy.query(Migration, "SELECT migration_id,name FROM coordinator_broker_migrations ORDER BY migration_id");
    const queueVersions = yield* legacy.query(Migration, "SELECT migration_id,name FROM effect_queue_migrations ORDER BY migration_id");
    if (JSON.stringify(brokerVersions) !== JSON.stringify([{ migration_id: 1, name: "relational_baseline" }]) ||
      (JSON.stringify(queueVersions) !== JSON.stringify([{ migration_id: 1, name: "create_table" }]) &&
        JSON.stringify(queueVersions) !== JSON.stringify([{ migration_id: 1, name: "create_table" }, { migration_id: 2, name: "upgrade_schema" }]))) return yield* recoveryError("Unsupported legacy broker/queue migration history");
    const version = queueVersions.length === 1 ? 1 : 2;
    const definitions = yield* legacy.query(Schema.Struct({ sql: Schema.String }), "SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name");
    const normalize = (value: string) => value.split(/('(?:''|[^'])*')/).map((part, index) => index % 2 ? part : part.replace(/IF NOT EXISTS/gi, "").replace(/["`\[\]\s]/g, "").toUpperCase()).join("");
    if (JSON.stringify(definitions.map((row) => normalize(row.sql)).sort()) !== JSON.stringify(legacyBrokerSql(version).map(normalize).sort())) return yield* recoveryError("Unsupported legacy SQLite schema; no best-effort import");
    const userVersion = yield* legacy.query(Schema.Struct({ user_version: Schema.Number }), "PRAGMA user_version");
    if (![0, 1].includes(userVersion[0]?.user_version ?? -1)) return yield* recoveryError("Unsupported legacy user_version");
    let pendingAccepted = 0, after: string | null = null;
    for (;;) {
      const page: ReadonlyArray<typeof LegacyTask.Type> = yield* legacy.query(LegacyTask, "SELECT id,data FROM tasks WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT 128", [after, after]);
      if (!page.length) break;
      for (const row of page) {
        const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data).pipe(
          Effect.mapError(() => recoveryError("Legacy task JSON decode failed")));
        if (task.taskId !== row.id || task.request.taskId !== row.id || task.fingerprint !== (yield* platform.fingerprint(task.request))) return yield* recoveryError("Legacy task identifier/fingerprint mismatch");
        if (task.state === "ACCEPTED" || task.state === "RUNNING") pendingAccepted++;
        if (task.runnerId && !["SUCCEEDED", "FAILED"].includes(task.state) && task.runnerId !== runner?.id) return yield* recoveryError("Legacy assigned uncertainty requires its paired runner");
        after = row.id;
      }
    }
    if (pendingAccepted && !holdAcceptedWork) return yield* recoveryError("Import requires --hold-accepted-work; accepted/running work is HELD without replay");
    if (!!runner !== !!runnerSource) return yield* recoveryError("Paired runner config and --runner-source are both required");
    const runnerState = runner && runnerSource ? yield* importLegacyRunner(runnerSource, runner, holdAcceptedWork) : undefined;
    if ((pendingAccepted + (runnerState?.pendingAccepted ?? 0)) && !holdAcceptedWork) return yield* recoveryError("Import requires --hold-accepted-work; accepted/running work is HELD without replay");
    yield* privateRecoveryPath(artifact, "directory", true);
    yield* fenceBroker;
    yield* target.withTransaction(Effect.gen(function*() {
      for (const table of brokerTables) {
        let cursor: string | null = null, imported = 0;
        for (;;) {
          const page: ReadonlyArray<typeof Cells.Type> = yield* legacy.query(Cells, `SELECT CAST(rowid AS TEXT) AS "_cursor",* FROM "${table}" WHERE (? IS NULL OR rowid > ?) ORDER BY rowid LIMIT 128`, [cursor, cursor], true);
          if (!page.length) break;
          for (const original of page) {
            const { _cursor, ...decoded } = original;
            const row: Record<string, typeof Cell.Type> = { ...decoded };
            if (typeof _cursor !== "string") return yield* recoveryError("Legacy cursor unavailable");
            cursor = _cursor;
            if (table === "sessions") {
              const session = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(row.data).pipe(
                Effect.mapError(() => recoveryError("Legacy session JSON decode failed")));
              if (session.sessionId !== row.id) return yield* recoveryError("Legacy session identifier mismatch");
            }
            if (table === "effect_queue" && version === 1) {
              if (row.completed !== 0n && row.completed !== 1n) return yield* recoveryError("Invalid legacy queue boolean");
              row.state = row.completed === 1n ? "completed" : "pending";
              row.visible_at = row.updated_at ?? null;
              delete row.completed;
            }
            if (table === "consents" || table === "participant_consents") {
              if (row.enabled !== 0n && row.enabled !== 1n) return yield* recoveryError("Invalid legacy consent boolean");
            }
            const input: Record<string, unknown> = {};
            const expected: Record<string, string | null> = {};
            for (const column of Object.keys(row)) {
              const value = row[column] ?? null;
              if (value !== null && !["string", "number", "bigint"].includes(typeof value)) return yield* recoveryError("Unsupported legacy SQLite cell type");
              const timestamp = table === "effect_queue" && ["visible_at", "acquired_at", "created_at", "updated_at"].includes(column);
              if (timestamp && value !== null && (typeof value !== "string" || !/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,6})?$/.test(value))) return yield* recoveryError("Unsupported native legacy timestamp encoding");
              if (table === "effect_queue" && column === "state" && !["pending", "processing", "completed", "failed"].includes(String(value))) return yield* recoveryError("Unsupported legacy queue state");
              if (table === "effect_queue" && column === "acquired_by" && value !== null && (typeof value !== "string" || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value))) return yield* recoveryError("Invalid legacy queue owner UUID");
              input[column] = timestamp && typeof value === "string" ? new Date(`${value.replace(" ", "T")}Z`) :
                column === "enabled" ? (value === 1n ? 1 : 0) :
                column === "attempts" && typeof value === "bigint" ? Number(value) : value;
              if (column === "attempts" && !Number.isSafeInteger(input[column])) return yield* recoveryError("Legacy attempts exceed safe integer range");
              expected[column] = value === null ? null : timestamp ? `${String(value).split(".")[0]}.${(String(value).split(".")[1] ?? "").padEnd(6, "0")}` : String(value);
            }
            // Decode every source row with generated insert schemas; returning checks include every stored field.
            const inserted = yield* Effect.gen(function*() {
              switch (table) {
                case "tasks": {
                  const value = yield* Schema.decodeUnknownEffect(Model.TasksInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy tasks insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "sessions": {
                  const value = yield* Schema.decodeUnknownEffect(Model.SessionsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy sessions insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "peers": {
                  const value = yield* Schema.decodeUnknownEffect(Model.PeersInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy peers insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "consents": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ConsentsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy consents insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "participant_consents": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ParticipantConsentsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy participant_consents insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "archive": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ArchiveInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy archive insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "evidence": {
                  const value = yield* Schema.decodeUnknownEffect(Model.EvidenceInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy evidence insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "effect_queue": {
                  const value = yield* Schema.decodeUnknownEffect(Model.EffectQueueInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy effect_queue insert decode failed")));
                  // Keep original PostgreSQL microseconds, not Date's millisecond-truncated validation value.
                  yield* db.execute(db.client.insertInto(table).values({ ...value,
                    acquired_at: expression<Date | null>`${row.acquired_at}::timestamp`,
                    created_at: expression<Date>`${row.created_at}::timestamp`,
                    updated_at: expression<Date>`${row.updated_at}::timestamp`,
                    visible_at: expression<Date>`${row.visible_at}::timestamp`,
                  }));
                  if (value.sequence === undefined) return yield* recoveryError("Legacy queue sequence unavailable");
                  return yield* db.execute(db.client.selectFrom(table).select(["sequence", "id", "queue_name", "element", "state", "attempts", "last_failure", "acquired_by"]).select((eb) => [
                    eb.fn<string | null>("to_char", [eb.ref("acquired_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("acquired_at"),
                    eb.fn<string>("to_char", [eb.ref("created_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("created_at"),
                    eb.fn<string>("to_char", [eb.ref("updated_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("updated_at"),
                    eb.fn<string>("to_char", [eb.ref("visible_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("visible_at"),
                  ]).where("sequence", "=", value.sequence));
                }
              }
            });
            const checkedRow = inserted[0];
            const actual = checkedRow === undefined ? {} : Object.fromEntries(Object.entries(checkedRow).map(([column, value]) =>
              [column, value === null ? null : typeof value === "boolean" ? (value ? "1" : "0") : String(value)]));
            if (inserted.length !== 1 || (yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(expected))) return yield* recoveryError("Legacy row changed during exact import");
            imported++;
          }
        }
        const counts = yield* db.execute(db.client.selectFrom(table).select((eb) => eb.fn.countAll<string>().as("count")));
        const sourceCounts = yield* legacy.query(Schema.Struct({ count: Schema.BigInt }), `SELECT count(*) AS count FROM "${table}"`, [], true);
        if (sourceCounts[0]?.count !== BigInt(imported)) return yield* recoveryError("Legacy source row count mismatch");
        if (BigInt(counts[0]?.count ?? "-1") !== BigInt(imported)) return yield* recoveryError("Legacy table row count mismatch");
      }
      const sequences: ReadonlyArray<readonly ["archive" | "evidence" | "effect_queue", "sequence" | "id"]> = [["archive", "sequence"], ["evidence", "id"], ["effect_queue", "sequence"]];
      for (const [table, column] of sequences) {
        const floors = yield* legacy.query(Schema.Struct({ seq: Schema.BigInt }), "SELECT seq FROM sqlite_sequence WHERE name=?", [table], true);
        const maximum = table === "evidence" ?
          yield* db.execute(db.client.selectFrom("evidence").select((eb) => eb.fn.max<Model.EvidenceId | null>("id").as("value"))) :
          yield* db.execute(db.client.selectFrom(table).select((eb) => eb.fn.max<Model.ArchiveId | Model.EffectQueueId | null>("sequence").as("value")));
        const sourceFloor = floors[0]?.seq ?? 0n, highest = BigInt(maximum[0]?.value ?? "0");
        const floor = sourceFloor > highest ? sourceFloor : highest;
        const sequence = yield* db.execute(db.client.selectNoFrom((eb) => eb.fn<string>("pg_get_serial_sequence", [eb.val(table), eb.val(column)]).as("name")));
        if (!sequence[0]?.name || floor < 0n) return yield* recoveryError("Legacy sequence identity/floor invalid");
        yield* db.execute(db.client.selectNoFrom(expression<bigint>`setval(${sequence[0].name}::regclass,${floor > 0n ? floor : 1n}::bigint,${floor > 0n})`.as("value")));
      }
    }).pipe(Effect.provideService(SqlClient.SafeIntegers, true)));
    // All native runner files, permissions, UID layout and no-live-child guards must finish before opening the target gate.
    const metadata = yield* recoverOwned("backup", config, artifact, sourceSha, runner).pipe(
      Effect.mapError((error) => recoveryError(`Legacy paired archive: ${error.message}`)));
    if (metadata.broker.pendingAccepted + (metadata.runner?.pendingAccepted ?? 0) > 0 && !holdAcceptedWork) return yield* recoveryError("Legacy paired acceptance changed; target remains fenced without acknowledgment");
    yield* holdBrokerAccepted;
    if ((yield* hashFile(source)) !== sourceDigest) return yield* recoveryError("Legacy source changed during import; target remains fenced");
    if (runner) yield* Effect.scoped(Effect.gen(function*() {
      const owner = yield* Postgres;
      yield* holdRunnerAccepted(runner, owner);
      yield* openBrokerGate;
    }).pipe(Effect.provide(Postgres.layer(runner.databaseUrlFile))));
    yield* openBrokerGate;
    return metadata;
  }).pipe(Effect.provide(LegacySqlite.layer(source, true)), io,
    Effect.mapError((error) => recoveryError(`Legacy import: ${error.message}`))));
  const lost = sourceLock.exitCode.pipe(Effect.andThen(Effect.fail(recoveryError("Legacy source ownership lost; target must remain fenced"))));
  return yield* operation.pipe(Effect.raceFirst(lost), Effect.raceFirst((yield* Postgres).failure));
})).pipe(Effect.provide(Postgres.layer(config.databaseUrlFile)), io);

const importLegacyRunner = (source: string, runner: RunnerConfig, holdAcceptedWork: boolean) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  yield* privateRecoveryPath(source, "file");
  if ([runner.childDirectory, ...Object.values(runner.repositories)].some((root) => source === root || source.startsWith(`${root}/`))) return yield* recoveryError("Legacy runner source must remain outside archived roots");
  const lockPath = platform.path.join(platform.path.dirname(source), "runner.lock");
  yield* privateRecoveryPath(lockPath, "file", !(yield* platform.fs.exists(lockPath)));
  const lock = yield* platform.acquireJournalOwnership(lockPath);
  for (const suffix of ["-wal", "-shm", "-journal"]) yield* privateRecoveryPath(`${source}${suffix}`, "file", !(yield* platform.fs.exists(`${source}${suffix}`)));
  yield* Effect.scoped(Effect.gen(function*() {
    const legacy = yield* LegacySqlite;
    yield* legacy.execute("PRAGMA synchronous=FULL");
    const checkpoint = yield* legacy.query(Schema.Struct({ busy: Schema.Number }), "PRAGMA wal_checkpoint(TRUNCATE)");
    if (checkpoint.some((row) => row.busy !== 0)) return yield* recoveryError("Legacy runner checkpoint busy; stop every writer");
    const mode = yield* legacy.query(Schema.Struct({ journal_mode: Schema.String }), "PRAGMA journal_mode=DELETE");
    if (mode[0]?.journal_mode !== "delete") return yield* recoveryError("Legacy runner checkpoint failed");
  }).pipe(Effect.provide(LegacySqlite.layer(source))));
  yield* noSidecars(source);
  const digest = yield* hashFile(source);
  const operation = Effect.gen(function*() {
    const legacy = yield* LegacySqlite;
    const pg = yield* Postgres;
    yield* assertRunnerSchema(pg.sql);
    yield* emptyBrokerDatabase;
    const db = makeDatabase(pg.sql);
    for (const table of ["runner_tasks", "runner_events"] as const) if ((yield* db.execute(db.client.selectFrom(table).selectAll().limit(1))).length) return yield* recoveryError("Fresh empty runner database required");
    const integrity = yield* legacy.query(Schema.Struct({ integrity_check: Schema.String }), "PRAGMA integrity_check");
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") return yield* recoveryError("Legacy runner integrity mismatch");
    const versions = yield* legacy.query(Schema.Struct({ migration_id: Schema.Int, name: Schema.String }), "SELECT migration_id,name FROM coordinator_runner_migrations ORDER BY migration_id");
    if (JSON.stringify(versions) !== JSON.stringify([{ migration_id: 1, name: "relational_baseline" }, { migration_id: 2, name: "native_mcp_binding" }])) return yield* recoveryError("Legacy runner migration history mismatch");
    const tables = yield* legacy.query(Schema.Struct({ name: Schema.String }), "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
    if (JSON.stringify(tables.map((row) => row.name)) !== JSON.stringify(["coordinator_runner_migrations", "events", "tasks"])) return yield* recoveryError("Unexpected legacy runner tables");
    const columns = yield* legacy.query(Schema.Struct({ name: Schema.String }), "PRAGMA table_info(tasks)");
    const expected = ["taskId", "fingerprint", "work", "worktree", "state", "occupied", "pid", "incarnation", "address", "promptId", "reason", "result", "detached", "nativeBinding", "nativeRunId", "nativeCursor"];
    if (JSON.stringify(columns.map((row) => row.name).sort()) !== JSON.stringify(expected.sort())) return yield* recoveryError("Legacy runner columns mismatch");
    const versionsOfJournal = yield* legacy.query(Schema.Struct({ user_version: Schema.Int }), "PRAGMA user_version");
    const journalVersion = versionsOfJournal[0]?.user_version;
    if (journalVersion === undefined || ![0, 1, 2].includes(journalVersion)) return yield* recoveryError("Unsupported legacy runner user_version");
    let pendingAccepted = 0;
    yield* fenceBroker;
    const state = yield* pg.sql.withTransaction(Effect.gen(function*() {
      let after = 0, rows = 0, events = 0;
      for (;;) {
        const page = yield* legacy.query(Cells, "SELECT rowid AS sequence,* FROM tasks WHERE rowid>? ORDER BY rowid LIMIT 128", [after]);
        if (!page.length) break;
        for (const row of page) {
          const original = yield* Schema.decodeUnknownEffect(Model.RunnerTasksInsert, { onExcessProperty: "error" })(row).pipe(Effect.mapError(() => recoveryError("Legacy runner row decode failed")));
          let result: RunnerResult = null;
          if (original.result != null) {
            if (journalVersion === 2) result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RunnerResult), { onExcessProperty: "error" })(original.result);
            else {
              const raw = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(JsonValue))(original.result);
              if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
                const fields = new Map<string, JsonValue>(Object.entries(raw));
                const launched = fields.get("launched"), hostSettled = fields.get("hostSettled"), evidenceVerified = fields.get("evidenceVerified"), exitCode = fields.get("exitCode"), signal = fields.get("signal");
                result = yield* Schema.decodeEffect(RunnerResult)({
                  ...(typeof launched === "boolean" ? { launched } : {}),
                  ...(typeof hostSettled === "boolean" ? { hostSettled } : {}),
                  ...(typeof evidenceVerified === "boolean" ? { evidenceVerified } : {}),
                  ...(typeof exitCode === "number" && Number.isSafeInteger(exitCode) ? { exitCode } : {}),
                  ...(typeof signal === "string" && /^SIG[A-Z0-9]{1,16}$/.test(signal) ? { signal } : {}),
                });
              }
            }
          }
          const value = journalVersion < 2 ? { ...original, reason: original.reason == null ? null : "historical diagnostic omitted",
            result: result === null ? null : Schema.encodeSync(Schema.fromJsonString(RunnerResult))(result) } : original;
          if (value.sequence === undefined || value.sequence <= after || value.sequence + runner.childUidBase > 4294967294) return yield* recoveryError("Legacy runner sequence invalid");
          const work = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Work), { onExcessProperty: "error" })(value.work);
          if (value.taskId !== work.taskId || value.fingerprint !== (yield* platform.fingerprint(work)) || value.worktree !== platform.path.join(runner.childDirectory, yield* platform.digest(value.taskId), "repo")) return yield* recoveryError("Legacy runner work identity mismatch");
          if (value.address != null) {
            const address = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Address), { onExcessProperty: "error" })(value.address);
            if (address.repo !== work.repo || address.ref !== work.ref || address.sha !== work.sha || address.incarnation !== value.incarnation) return yield* recoveryError("Legacy runner address identity mismatch");
          }
          if (value.nativeBinding != null) {
            const binding = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativeBinding), { onExcessProperty: "error" })(value.nativeBinding);
            if (binding.host !== work.launcher) return yield* recoveryError("Legacy runner native host mismatch");
          }
          if (value.promptId != null) yield* Schema.decodeUnknownEffect(NativeSubmissionId)(value.promptId);
          if (value.nativeRunId != null) {
            yield* Schema.decodeUnknownEffect(NativeRunId)(value.nativeRunId);
            if (value.nativeBinding == null || value.promptId == null) return yield* recoveryError("Legacy runner native run lacks owner/submission");
          }
          yield* Schema.decodeUnknownEffect(NativeEventSequence)(value.nativeCursor);
          if (value.pid !== null && value.pid !== undefined) yield* Effect.try({ try: () => {
            try { process.kill(value.pid!, 0); } catch (error) { if (typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH") return; throw error; }
            throw recoveryError("Legacy runner child remains live");
          }, catch: () => recoveryError("Legacy runner child liveness unsafe") });
          if (value.state === "ACCEPTED" && value.occupied === 1 && value.detached === 0) pendingAccepted++;
          const inserted = yield* db.execute(db.client.insertInto("runner_tasks").values(value).returningAll());
          if (inserted.length !== 1 || (yield* platform.fingerprint(inserted[0])) !== (yield* platform.fingerprint(value))) return yield* recoveryError("Legacy runner task changed during import");
          after = value.sequence; rows++;
        }
      }
      after = 0;
      for (;;) {
        const page = yield* legacy.query(Cells, "SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT 128", [after]);
        if (!page.length) break;
        for (const row of page) {
          const original = yield* Schema.decodeUnknownEffect(Model.RunnerEventsInsert, { onExcessProperty: "error" })(row);
          const metadata = journalVersion < 2 ?
            { ...(yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LegacyEventMetadata))(original.payload)), contentOmitted: true } :
            yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(original.payload);
          if ((metadata.type === "readback" && metadata.address === undefined) ||
            (metadata.type === "native_lifecycle" && (metadata.binding === undefined || metadata.runId === undefined || metadata.nativeCursor === undefined || metadata.kind === undefined))) return yield* recoveryError("Legacy runner event lacks identity");
          const task = (yield* db.execute(db.client.selectFrom("runner_tasks").select(["work", "incarnation"]).where("taskId", "=", original.taskId)))[0];
          if (!task) return yield* recoveryError("Legacy runner event task missing");
          const work = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Work), { onExcessProperty: "error" })(task.work);
          if (metadata.binding && metadata.binding.host !== work.launcher) return yield* recoveryError("Legacy runner event native host mismatch");
          if (metadata.address && (metadata.address.repo !== work.repo || metadata.address.ref !== work.ref || metadata.address.sha !== work.sha || metadata.address.incarnation !== task.incarnation)) return yield* recoveryError("Legacy runner event address identity mismatch");
          const value = journalVersion < 2 ? { ...original, payload: Schema.encodeUnknownSync(Schema.fromJsonString(RunnerEvent), { onExcessProperty: "error" })(metadata) } : original;
          if (value.seq === undefined || value.seq <= after) return yield* recoveryError("Legacy runner event sequence invalid");
          const inserted = yield* db.execute(db.client.insertInto("runner_events").values(value).returningAll());
          if (inserted.length !== 1 || (yield* platform.fingerprint(inserted[0])) !== (yield* platform.fingerprint(value))) return yield* recoveryError("Legacy runner event changed during import");
          after = value.seq; events++;
        }
      }
      const sourceCounts = yield* legacy.query(Schema.Struct({ tasks: Schema.Int, events: Schema.Int }), "SELECT (SELECT count(*) FROM tasks) AS tasks,(SELECT count(*) FROM events) AS events");
      if (sourceCounts[0]?.tasks !== rows || sourceCounts[0]?.events !== events) return yield* recoveryError("Legacy runner count mismatch");
      if (pendingAccepted && !holdAcceptedWork) return yield* recoveryError("Legacy runner accepted work requires HELD acknowledgement");
      const floors = yield* legacy.query(Schema.Struct({ seq: Schema.Int }), "SELECT seq FROM sqlite_sequence WHERE name='events'");
      const taskMaximum = yield* db.execute(db.client.selectFrom("runner_tasks").select((eb) => eb.fn.max<number>("sequence").as("maximum")));
      const eventFloor = Math.max(after, floors[0]?.seq ?? 0);
      for (const [name, floor] of [["runner_tasks_sequence_seq", taskMaximum[0]?.maximum ?? 0], ["runner_events_seq_seq", eventFloor]] as const) {
        yield* db.execute(db.client.selectNoFrom(expression<number>`setval(${name}::regclass,${Math.max(1, floor)},${floor > 0})`.as("value")));
      }
      if ((yield* hashFile(source)) !== digest) return yield* recoveryError("Legacy runner source changed; target remains offline");
      return { rows, pendingAccepted };
    }));
    return state;
  }).pipe(Effect.provide(LegacySqlite.layer(source, true)), Effect.provide(Postgres.layer(runner.databaseUrlFile)));
  return yield* operation.pipe(Effect.raceFirst(lock.exitCode.pipe(Effect.andThen(Effect.fail(recoveryError("Legacy runner ownership lost"))))));
})).pipe(io);

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) NodeRuntime.runMain(cli);
