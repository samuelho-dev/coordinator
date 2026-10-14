import { Config, Console, Context, Effect, Layer, Schema, Stdio } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { BrokerConfig, RunnerConfig, Commands, CommandError, type CommandInput } from "./contracts.ts";
import { Broker } from "./broker.ts";
import { Runner } from "./runner.ts";
import { Platform } from "./platform.ts";
import { Mcp } from "./http.ts";

const services = Layer.mergeAll(Platform.layer, Mcp.layer, NodeServices.layer);
NodeRuntime.runMain(Effect.scoped(Effect.gen(function*() {
  // Effect has no process umask API; credentials and adapter-created files must be private.
  process.umask(0o077);
  const args = yield* (yield* Stdio.Stdio).args;
  if (args.length !== 1 || (args[0] !== "broker" && args[0] !== "runner")) return yield* new CommandError({ message: "Expected broker or runner" });
  const configPath = yield* Config.String("COORDINATOR_CONFIG");
  const platform = yield* Platform;
  const runBroker = Effect.fnUntraced(function*({ configFile }: CommandInput<"program.broker">) {
    const contents = yield* platform.privateFile(configFile);
    const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(BrokerConfig)), { onExcessProperty: "error" })(contents);
    const context = yield* Layer.build(Broker.layer(config));
    const broker = Context.get(context, Broker);
    yield* Console.log(`coordinator broker listening on ${broker.config.host}:${broker.config.port}`);
    return yield* Effect.never;
  });
  const runRunner = Effect.fnUntraced(function*({ configFile }: CommandInput<"program.runner">) {
    const contents = yield* platform.privateFile(configFile);
    const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(RunnerConfig)), { onExcessProperty: "error" })(contents);
    const context = yield* Layer.build(Runner.layer(config));
    const runner = Context.get(context, Runner);
    yield* Console.log(`coordinator runner listening on ${runner.config.host}:${runner.config.port}`);
    // Fenced PostgreSQL serving must not dispose accepted native work.
    return yield* Effect.never;
  });
  const command = Commands[`program.${args[0]}`];
  return yield* command.executeDecoded({ runBroker, runRunner }, { configFile: configPath });
})).pipe(Effect.provide(services)));
