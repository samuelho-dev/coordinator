import { parseArgs } from "node:util";
import { Console, Effect, Formatter, Layer, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { BrokerConfig, RunnerConfig } from "../src/contracts.ts";
import { Evidence } from "../src/generated/database/types.ts";
import { BackupError, privateRecoveryPath, recover } from "../src/backup.ts";
import { Platform } from "../src/platform.ts";

// Offline only: this command never pauses services, dispatches work, or deletes existing state.
const usage = "Usage: tsx scripts/recover.ts backup|restore --broker-config /private/broker.json [--runner-config /private/runner.json] --artifact /private/new-artifact --source-sha <40-lowercase-hex> --offline-maintenance [--hold-accepted-work (restore only)]. Provide broker COORDINATOR_DATABASE_URL and, with --runner-config, separate COORDINATOR_RUNNER_DATABASE_URL through the environment; never command arguments or config files. Stop broker/runner/agent children offline. Restore into separate fresh empty PostgreSQL databases initialized with the committed initial schema; mount fresh runner roots at identical absolute paths/UID layout. Recovery version 3 uses PostgreSQL 18 custom data-only broker.pgdump and runner.pgdump. Independently reconstruct excluded credentials/model seed; encrypt private artifacts outside Git. Failed partial targets remain fenced; no replay or agent reattachment.";
process.umask(0o077);
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  const args = yield* Effect.try({ try: () => parseArgs({ allowPositionals: true, options: {
    "broker-config": { type: "string" }, "runner-config": { type: "string" }, artifact: { type: "string" },
    "source-sha": { type: "string" }, "offline-maintenance": { type: "boolean" }, "hold-accepted-work": { type: "boolean" }, help: { type: "boolean" },
  } }), catch: () => new BackupError({ message: "Invalid recovery arguments" }) });
  if (args.values.help) { yield* Console.log(usage); return; }
  const operation = args.positionals[0];
  const configPath = args.values["broker-config"], artifact = args.values.artifact, sha = args.values["source-sha"];
  if (args.positionals.length !== 1 || (operation !== "backup" && operation !== "restore") || !configPath || !artifact || !sha || !args.values["offline-maintenance"] || (operation === "backup" && args.values["hold-accepted-work"])) return yield* new BackupError({ message: usage });
  const sourceSha = yield* Schema.decodeUnknownEffect(Evidence.fields.sha)(sha);
  const platform = yield* Platform;
  yield* privateRecoveryPath(configPath, "file");
  const broker = yield* Schema.decodeEffect(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configPath));
  let runner: RunnerConfig | undefined;
  if (args.values["runner-config"]) {
    yield* privateRecoveryPath(args.values["runner-config"], "file");
    runner = yield* Schema.decodeEffect(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(yield* platform.privateFile(args.values["runner-config"]));
  }
  if (runner && [runner.childDirectory, ...Object.values(runner.repositories)].some((root) =>
    [configPath, args.values["runner-config"]].some((file) => file && (file === root || file.startsWith(`${root}/`))))) return yield* new BackupError({ message: "Operator config files must remain outside archived runner roots" });
  const metadata = yield* recover(operation, broker, artifact, sourceSha, runner, args.values["hold-accepted-work"] === true);
  yield* Console.log(Formatter.formatJson({ operation, artifact, kind: metadata.kind, sourceSha: metadata.sourceSha, brokerRows: metadata.broker.rows, runnerRows: metadata.runner?.rows ?? null, runnerEvents: metadata.runner?.eventRows ?? null,
    acceptedRowsFencedHeld: operation === "restore" ? metadata.broker.pendingAccepted + (metadata.runner?.pendingAccepted ?? 0) : 0,
    brokerRowsFencedHeld: operation === "restore" ? metadata.broker.pendingAccepted : 0, runnerRowsFencedHeld: operation === "restore" ? metadata.runner?.pendingAccepted ?? 0 : 0, excluded: metadata.excluded,
    maintenance: "Keep all services stopped until successful complete restore. On failure quarantine partial fresh volumes; do not start them. Reconstruct excluded credentials and model seed independently. Start broker, then runner with the same source/absolute paths/UID layout; reconcile HELD outcomes manually without replay. Unknown external side effects are not recoverable." }, { space: 2 }));
})).pipe(Effect.provide(Layer.mergeAll(Platform.layer))));
