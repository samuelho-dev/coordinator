import { Config, Effect, Option, Schema } from "effect";
import { SessionTerminal } from "./inspection.ts";

const decode = Schema.decodeUnknownOption(SessionTerminal);

export const readTerminalContext = Effect.gen(function*() {
  const name = yield* Config.String("TERM_PROGRAM").pipe(Config.withDefault(""));
  if (!name) return undefined;
  const version = yield* Config.String("TERM_PROGRAM_VERSION").pipe(Config.withDefault(""));
  return Option.getOrUndefined(decode({ name, ...(version ? { version } : {}) }));
});
