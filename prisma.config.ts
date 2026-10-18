import { definePrismaConfig } from "prisma/config";
import { defineConfig } from "@prisma/orm-postgres/config";
import { Effect, Redacted } from "effect";
import { DatabaseService } from "./src/database.ts";

const connection = process.env.COORDINATOR_DATABASE_URL === undefined ? undefined :
  Redacted.value(await Effect.runPromise(DatabaseService.connection()));

export default definePrismaConfig({
  orm: defineConfig({
    contract: "./prisma/contract.prisma",
    output: "./src/generated/prisma",
    db: { connection },
    migrations: { dir: "./migrations" }
  })
});
