import { Deferred, Effect, Option, Scope, Stdio, Stream } from "effect";
import { Base64 } from "effect/encoding";
import { CommandError, type Context } from "./contracts.ts";

const prefix = "\x1b]7501;";
const reply = /\x1b\]7501;\?[^\x00-\x1f\x7f]*(?:\x07|\x1b\\)/g;
const messages = { permission: "Waiting for permission", question: "Waiting for an answer" };
const networkMessages = { retrying: "Coordinator reconnecting", paused: "Coordinator network paused" };
type State = "idle" | "working" | "done" | "error" | undefined;
type Kind = "permission" | "question";
type NetworkState = keyof typeof networkMessages | undefined;

export interface ProgramStatus {
  report: (state: State, kind?: Kind) => Effect.Effect<void, CommandError>;
  clear: () => Effect.Effect<void, CommandError>;
  network: (state: NetworkState) => Effect.Effect<void, CommandError>;
}

export const makeProgramStatus = (ctx: Context): Effect.Effect<ProgramStatus, never, Stdio.Stdio | Scope.Scope> => Effect.gen(function*() {
  const stdio = yield* Stdio.Stdio;
  let supported = false;
  let retired = false;
  let last: string | undefined;
  let taskState: State;
  let taskKind: Kind | undefined;
  let networkState: NetworkState;
  let flushing = false;
  const write = (fields: string) => Stream.make(`${prefix}id=coordinator:app=coordinator:${fields}\x1b\\`).pipe(
    Stream.run(stdio.stdout({ endOnDone: false })),
    Effect.catchCause(() => Effect.fail(new CommandError({ message: "Coordinator terminal status unavailable" }))),
  );
  const fields = () => {
    if (taskState === undefined && networkState === undefined) return undefined;
    const state = taskKind ? `state=blocked:kind=${taskKind}` : `state=${taskState ?? (networkState === "paused" ? "error" : "working")}`;
    const message = [taskKind ? messages[taskKind] : undefined, networkState ? networkMessages[networkState] : undefined].filter((value) => value !== undefined).join("; ");
    return message ? `${state}:msg=${Base64.encode(message)}` : state;
  };
  // One owner composes task and connectivity updates, including changes during a write.
  const flush = () => Effect.gen(function*() {
    if (retired || !supported || flushing) return;
    flushing = true;
    yield* Effect.gen(function*() {
      while (!retired) {
        const next = fields();
        if (next === last) return;
        yield* write(next ?? "state=clear");
        last = next;
      }
    }).pipe(Effect.ensuring(Effect.sync(() => { flushing = false; })));
  });
  const network = (state: NetworkState) => Effect.gen(function*() {
    if (retired || networkState === state) return;
    networkState = state;
    if (ctx.hasUI) {
      ctx.ui.setStatus?.("coordinator.network", state === "retrying" ? networkMessages.retrying : undefined);
      ctx.ui.setStatus?.("coordinator.paused", state === "paused" ? networkMessages.paused : undefined);
    }
    yield* flush();
  });
  const clear = () => Effect.gen(function*() {
    if (retired) return;
    taskState = undefined;
    taskKind = undefined;
    yield* network(undefined);
    yield* flush();
  });
  yield* Effect.addFinalizer(() => Effect.gen(function*() {
    // Connectivity is transient; completed task results still survive process exit.
    yield* network(undefined).pipe(Effect.catchCause(() => Effect.void));
    if (taskKind !== undefined || (taskState !== "done" && taskState !== "error")) {
      yield* clear().pipe(Effect.catchCause(() => Effect.void));
    }
    retired = true;
  }));
  const eligible = ctx.hasUI && ctx.mode === "tui" && ctx.agent?.kind !== "sub" && ctx.ui.onTerminalInput !== undefined && (yield* stdio.stdoutIsTerminal);
  if (eligible) {
    const received = yield* Deferred.make<void>();
    let observed = "";
    let listening = true;
    const unsubscribe = yield* Effect.acquireRelease(Effect.sync(() => {
      const stop = ctx.ui.onTerminalInput!((data) => {
      if (!listening || retired) return;
      // Native hosts normally frame complete OSCs. Pi can flush a torn OSC and
      // offers no reinjection API: observe fragments, never hold/drop their bytes.
      const combined = observed + data;
      reply.lastIndex = 0;
      for (const match of combined.matchAll(reply)) {
        if (Buffer.byteLength(match[0], "utf8") <= 4096) Deferred.doneUnsafe(received, Effect.void);
      }
      const start = combined.lastIndexOf("\x1b");
      const candidate = start < 0 ? "" : combined.slice(start);
      // Retain only a possible query reply, including a split introducer/ST.
      if (candidate === "\x1b" && observed.startsWith(`${prefix}?`) && combined.endsWith("\x1b")) {
        observed = combined.length <= 4096 ? combined : "";
      } else observed = (prefix.startsWith(candidate) || candidate.startsWith(`${prefix}?`)) && candidate.length <= 4096 ? candidate : "";
      const filtered = data.replace(reply, (sequence) => Buffer.byteLength(sequence, "utf8") <= 4096 ? "" : sequence);
      return filtered === data ? undefined : filtered.length === 0 ? { consume: true } : { data: filtered };
      });
      return () => {
        if (!listening) return;
        listening = false;
        observed = "";
        stop();
      };
    }), (unsubscribe) => Effect.sync(unsubscribe));
    yield* Effect.gen(function*() {
      const queried = yield* Stream.make(`${prefix}?\x1b\\`).pipe(
        Stream.run(stdio.stdout({ endOnDone: false })),
        Effect.map(() => true), Effect.catchCause(() => Effect.succeed(false)),
      );
      const answer = queried ? yield* Deferred.await(received).pipe(Effect.timeoutOption("100 millis")) : Option.none();
      supported = Option.isSome(answer);
      yield* flush().pipe(Effect.catchCause(() => Effect.void));
    }).pipe(
      Effect.ensuring(Effect.sync(unsubscribe)),
      Effect.forkScoped,
    );
  }
  return {
    clear,
    network,
    report: (state: State, kind?: Kind) => Effect.gen(function*() {
      if (retired) return;
      taskState = state;
      taskKind = state === undefined ? undefined : kind;
      yield* flush();
    }),
  };
});

