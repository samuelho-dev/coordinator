import { definePrismaConfig } from "prisma/config";
import { defineConfig } from "@prisma/orm-postgres/config";
import { Effect, Redacted } from "effect";
import { Platform } from "./src/platform.ts";
import { readDatabaseUrl } from "./src/postgres.ts";

const urlFile = process.env["COORDINATOR_DATABASE_URL_FILE"];
const connection = urlFile ? Redacted.value(await Effect.runPromise(
  readDatabaseUrl(urlFile).pipe(
    Effect.provide(Platform.layer),
    Effect.catch(() => Effect.fail(new Error("Private PostgreSQL connection configuration is invalid")))
  )
)) : undefined;

export default definePrismaConfig({
  orm: defineConfig({
    contract: "./prisma/contract.prisma",
    output: "./src/generated/prisma",
    db: { connection },
    migrations: { dir: "./migrations" }
  })
});
