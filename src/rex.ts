import { randomUUID } from "node:crypto";
import type { ProgramStatus } from "./inspection.ts";
import { Config, Effect } from "effect";

export const readRexContext = Effect.gen(function*() {
  const serverAddress = yield* Config.String("REX_SERVER").pipe(Config.withDefault(""));
  const sessionId = yield* Config.String("REX_SESSION").pipe(Config.withDefault(""));
  const blockId = yield* Config.String("REX_BLOCK").pipe(Config.withDefault(""));
  return serverAddress || sessionId || blockId ? {
    ...(serverAddress ? { serverAddress } : {}), ...(sessionId ? { sessionId } : {}), ...(blockId ? { blockId } : {}),
  } : undefined;
});

// ponytail: fixed labels avoid leaking native content and stay below all OSC text limits.
export const rexSequence = (id: string, status: ProgramStatus | undefined): string => {
  if (id.length > 128 || id.split("/").length > 8 || !/^[A-Za-z0-9_.+-]{1,32}(?:\/[A-Za-z0-9_.+-]{1,32})*$/u.test(id)) return "";
  return `\u001b]7501;id=${id}:state=${status?.state ?? "clear"}:app=coordinator:title=Q29vcmRpbmF0b3I=${status?.state === "blocked" ? `:kind=${status.kind}` : ""}\u001b\\`;
};

export const createRexReporter = (write: (sequence: string) => void) => {
  const id = `coordinator/${randomUUID().replaceAll("-", "")}`;
  let previous: string | undefined;
  return {
    update: (status: ProgramStatus | undefined, terminal: boolean) => {
      if (!terminal) return;
      const sequence = rexSequence(id, status);
      if (previous === sequence || (previous === undefined && status === undefined)) return;
      write(sequence);
      previous = sequence;
    },
    close: (terminal: boolean) => {
      if (terminal && previous !== undefined && !/:state=(?:done|error):/u.test(previous)) write(rexSequence(id, undefined));
      previous = undefined;
    },
  };
};
