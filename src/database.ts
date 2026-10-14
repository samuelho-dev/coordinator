import { Effect } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { ContractMarkerRow } from "@prisma/orm-postgres/family/verify";
import type { LedgerEntryRecord } from "@prisma/orm-postgres/contract/types";
import contract from "./generated/prisma/contract.json" with { type: "json" };
import { HttpError } from "./contracts.ts";
import { makeDatabase, type DatabaseClient } from "./kysely.ts";
import baseline from "../migrations/app/20261013T0303_broker_postgres_initial/migration.json" with { type: "json" };
import runnerMigration from "../migrations/app/20261014T0441_runner_postgres/migration.json" with { type: "json" };

export const brokerStorageHash = contract.storage.storageHash;

// Prisma owns this namespace. Project only its public native metadata types;
// rc.17 exports no snake-cased ledger row type, so map the selected columns.
type PrismaMetadata = {
  "prisma_contract.marker": Pick<ContractMarkerRow, "core_hash" | "profile_hash"> & Pick<LedgerEntryRecord, "space">;
  "prisma_contract.ledger": {
    space: LedgerEntryRecord["space"];
    migration_hash: LedgerEntryRecord["migrationHash"];
    destination_core_hash: LedgerEntryRecord["to"];
  };
};

// Recovery compares the same identity while its durable gate intentionally fences serving.
const schemaIdentity = (db: DatabaseClient) => Effect.gen(function*() {
  const metadata = db.client.withTables<PrismaMetadata>();
  const markers = yield* db.execute(metadata.selectFrom("prisma_contract.marker")
    .select(["space", "core_hash", "profile_hash"]).orderBy("space"));
  if (markers.length !== 1 || markers[0]?.space !== "app" ||
      markers[0].core_hash !== brokerStorageHash || markers[0].profile_hash !== contract.profileHash) {
    return yield* new HttpError({ status: 503, message: "Coordinator Prisma contract mismatch; apply exact committed migrations" });
  }
  const ledger = yield* db.execute(metadata.selectFrom("prisma_contract.ledger")
    .select(["migration_hash", "destination_core_hash"]).where("space", "=", "app"));
  const migrations = [baseline, runnerMigration];
  if (ledger.length !== migrations.length || migrations.some((migration) => !ledger.some((entry) =>
      entry.migration_hash === migration.migrationHash && entry.destination_core_hash === migration.to))) {
    return yield* new HttpError({ status: 503, message: "Coordinator immutable migration history mismatch" });
  }
  const versions = yield* db.execute(db.client.selectFrom("effect_queue_migrations").selectAll().orderBy("migration_id"));
  if (versions.length !== 2 || versions[0]?.migration_id !== 1 || versions[0].name !== "create_table" ||
      versions[1]?.migration_id !== 2 || versions[1].name !== "upgrade_schema") {
    return yield* new HttpError({ status: 503, message: "Broker native queue migration mismatch" });
  }
  return `${brokerStorageHash}:queue=1/create_table,2/upgrade_schema`;
});

export const brokerSchemaIdentity = (sql: SqlClient.SqlClient) => schemaIdentity(makeDatabase(sql));

export const assertBrokerSchema = (sql: SqlClient.SqlClient) => Effect.gen(function*() {
  const db = makeDatabase(sql);
  yield* schemaIdentity(db);
  const gate = yield* db.execute(db.client.selectFrom("recovery_gate").selectAll());
  if (gate.length > 1 || gate.some((row) => row.id !== 1 || row.ready !== true)) {
    return yield* new HttpError({ status: 503, message: "Broker recovery incomplete; serving remains fenced" });
  }
});

export const assertRunnerSchema = (sql: SqlClient.SqlClient) => Effect.gen(function*() {
  const db = makeDatabase(sql);
  yield* assertBrokerSchema(sql);
  yield* db.execute(db.client.selectFrom("runner_tasks").selectAll().limit(0));
  yield* db.execute(db.client.selectFrom("runner_events").selectAll().limit(0));
});
