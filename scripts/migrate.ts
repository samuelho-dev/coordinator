import { Config, Effect } from "effect";
import { ChildProcess } from "effect/process";
import { assertBrokerSchema, brokerStorageHash } from "../src/database.ts";
import { HttpError } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { BrokerPostgres } from "../src/postgres.ts";

export const migrateBrokerPostgres = (urlFile: string) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const root = yield* platform.path.fromFileUrl(new URL("../", import.meta.url));
  const prismaCli = yield* platform.path.fromFileUrl(new URL("./dist/prisma.js", import.meta.resolve("prisma/package.json")));
  const owner = yield* BrokerPostgres;
  const temporary = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-prisma-" });
  // Suppress native diagnostics: neither a failed connection nor its DSN may reach logs.
  yield* platform.exec(ChildProcess.make(process.execPath, [prismaCli, "db", "migrate", "--to", brokerStorageHash,
    "--no-interactive", "--quiet", "--config", platform.path.join(root, "prisma.config.ts")], {
    cwd: root, env: { COORDINATOR_DATABASE_URL_FILE: urlFile, DO_NOT_TRACK: "1",
      HOME: temporary, XDG_CACHE_HOME: temporary, TMPDIR: temporary,
      ...(process.env.NODE_EXTRA_CA_CERTS ? { NODE_EXTRA_CA_CERTS: process.env.NODE_EXTRA_CA_CERTS } : {}) },
    extendEnv: false, stdin: "ignore", stdout: "ignore", stderr: "ignore"
  })).pipe(
    Effect.mapError(() => new HttpError({ status: 503, message: "Committed broker migration failed" })),
    Effect.andThen(assertBrokerSchema(owner.sql)),
    Effect.raceFirst(owner.failure)
  );
}).pipe(Effect.provide(BrokerPostgres.layer(urlFile))));

if (import.meta.main) {
  const program = Effect.gen(function*() {
    const urlFile = yield* Config.String("COORDINATOR_DATABASE_URL_FILE");
    yield* migrateBrokerPostgres(urlFile);
  }).pipe(Effect.provide(Platform.layer));
  await Effect.runPromise(program.pipe(Effect.catch(() => Effect.sync(() => {
    console.error("Broker migration failed; database credentials and native diagnostics withheld");
    process.exitCode = 1;
  }))));
}
