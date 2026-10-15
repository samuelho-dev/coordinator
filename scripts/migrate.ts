import { Effect, Redacted } from "effect";
import { ChildProcess } from "effect/process";
import { assertBrokerSchema, assertRunnerSchema, brokerStorageHash } from "../src/database.ts";
import { HttpError } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { awaitDatabaseLoss, DatabaseService } from "../src/database-service.ts";

export const migrateBrokerPostgres = (url: Redacted.Redacted<string>) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const root = yield* platform.path.fromFileUrl(new URL("../", import.meta.url));
  const prismaCli = yield* platform.path.fromFileUrl(new URL("./dist/prisma.js", import.meta.resolve("prisma/package.json")));
  const db = yield* DatabaseService;
  const temporary = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-prisma-" });
  // Suppress native diagnostics: neither a failed connection nor its DSN may reach logs.
  yield* platform.exec(ChildProcess.make(process.execPath, [prismaCli, "db", "migrate", "--to", brokerStorageHash,
    "--no-interactive", "--quiet", "--config", platform.path.join(root, "prisma.config.ts")], {
    cwd: root, env: { COORDINATOR_DATABASE_URL: Redacted.value(url), DO_NOT_TRACK: "1",
      HOME: temporary, XDG_CACHE_HOME: temporary, TMPDIR: temporary,
      ...(process.env.NODE_EXTRA_CA_CERTS ? { NODE_EXTRA_CA_CERTS: process.env.NODE_EXTRA_CA_CERTS } : {}) },
    extendEnv: false, stdin: "ignore", stdout: "ignore", stderr: "ignore"
  })).pipe(
    Effect.mapError(() => new HttpError({ status: 503, message: "Committed broker migration failed" })),
    Effect.andThen(assertBrokerSchema(db)),
    Effect.andThen(assertRunnerSchema(db)),
    Effect.raceFirst(awaitDatabaseLoss(db))
  );
}).pipe(Effect.provide(DatabaseService.layer(url))));

if (import.meta.main) {
  const program = Effect.gen(function*() {
    yield* migrateBrokerPostgres(yield* DatabaseService.connection());
  }).pipe(Effect.provide(Platform.layer));
  await Effect.runPromise(program.pipe(Effect.catch(() => Effect.sync(() => {
    console.error("Broker migration failed; database credentials and native diagnostics withheld");
    process.exitCode = 1;
  }))));
}
