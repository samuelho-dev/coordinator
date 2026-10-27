import { randomUUID } from "node:crypto";
import type { ProgramStatus } from "./inspection.ts";

// ponytail: fixed labels avoid leaking native content and stay below all OSC text limits.
export const programStatusSequence = (id: string, status: ProgramStatus | undefined): string => {
  if (id.length > 128 || id.split("/").length > 8 || !/^[A-Za-z0-9_.+-]{1,32}(?:\/[A-Za-z0-9_.+-]{1,32})*$/u.test(id)) return "";
  return `\u001b]7501;id=${id}:state=${status?.state ?? "clear"}:app=coordinator:title=Q29vcmRpbmF0b3I=${status?.state === "blocked" ? `:kind=${status.kind}` : ""}\u001b\\`;
};

export const createProgramStatusReporter = (write: (sequence: string) => void) => {
  const id = `coordinator/${randomUUID().replaceAll("-", "")}`;
  let previous: string | undefined;
  return {
    update: (status: ProgramStatus | undefined, terminal: boolean) => {
      if (!terminal) return;
      const sequence = programStatusSequence(id, status);
      if (previous === sequence || (previous === undefined && status === undefined)) return;
      write(sequence);
      previous = sequence;
    },
    close: (terminal: boolean) => {
      if (terminal && previous !== undefined && !/:state=(?:done|error):/u.test(previous)) write(programStatusSequence(id, undefined));
      previous = undefined;
    },
  };
};
