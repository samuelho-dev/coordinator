import { parseArgs } from "node:util";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { BackupError, BrokerConfig, RunnerConfig, Sha } from "../src/contracts.ts";
import { importLegacySqlite, privateRecoveryPath } from "../src/backup.ts";
import { Platform } from "../src/platform.ts";

const usage = "Usage: tsx scripts/import-sqlite.ts --source /private/ledger.sqlite --broker-config /private/broker.json [--runner-config /private/runner.json] --artifact /private/new-artifact --source-sha <40-lowercase-hex> --offline-maintenance [--hold-accepted-work]. Stop every broker/runner/native child first. Source flock and SQLite exclusive checkpoint preserve committed WAL before standalone validation. The target must be a fresh empty exactly-migrated PostgreSQL database. Accepted/running work is held without replay; paired runner files are archived and remain offline for reconciliation. Source data is never deleted. No production rehearsal.";
process.umask(0o077);
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const args = yield* Effect.try({ try: () => parseArgs({ options: {
    source: { type: "string" }, "broker-config": { type: "string" }, "runner-config": { type: "string" },
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
  const metadata = yield* importLegacySqlite(source, config, artifact, sourceSha, runner, args.values["hold-accepted-work"] === true);
  console.log(JSON.stringify({ operation: "offline-sqlite-import", sourcePreserved: true, sourceSha: metadata.sourceSha, artifact, metadataVersion: metadata.version,
    brokerRows: metadata.broker.rows, brokerRowsFencedHeld: metadata.broker.pendingAccepted, runnerRows: metadata.runner?.rows ?? null,
    maintenance: "Keep legacy source and paired runner stopped. Target gate opens only after exact row validation and complete private paired artifact creation. Reconcile occupied/accepted native uncertainty manually; never replay. On failure discard only the disposable target, never the source." }));
})).pipe(Effect.provide(Platform.layer)));
