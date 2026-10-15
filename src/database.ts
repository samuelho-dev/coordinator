import { Effect } from "effect";
import type { ContractMarkerRow } from "@prisma/orm-postgres/family/verify";
import type { LedgerEntryRecord } from "@prisma/orm-postgres/contract/types";
import contract from "./generated/prisma/contract.json" with { type: "json" };
import { HttpError } from "./contracts.ts";
import type { DatabaseClient } from "./database-service.ts";

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
    return yield* new HttpError({ status: 503, message: "Coordinator Prisma contract mismatch; apply exact committed migrations" });
  }
  yield* db.query((client) => client.selectFrom("dispatch_queue").selectAll().limit(0));
  return `${brokerStorageHash}:profile=${contract.profileHash}`;
});

export const assertBrokerSchema = (db: DatabaseClient) => Effect.gen(function*() {
  yield* brokerSchemaIdentity(db);
  const gate = yield* db.query((client) => client.selectFrom("recovery_gate").selectAll());
  if (gate.length > 1 || gate.some((row) => row.id !== 1 || row.ready !== true)) {
    return yield* new HttpError({ status: 503, message: "Broker recovery incomplete; serving remains fenced" });
  }
});

export const assertRunnerSchema = (db: DatabaseClient) => Effect.gen(function*() {
  yield* assertBrokerSchema(db);
  yield* db.query((client) => client.selectFrom("runner_tasks").selectAll().limit(0));
  yield* db.query((client) => client.selectFrom("runner_events").selectAll().limit(0));
});
