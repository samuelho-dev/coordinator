import { Effect } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import getExePath from "@effect/tsgo/lib/getExePath";

NodeRuntime.runMain(Effect.gen(function*() {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const code = yield* spawner.exitCode(ChildProcess.make(getExePath(), ["-p", "tsconfig.json"], {
    cwd: import.meta.dirname,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit"
  }));
  if (code !== 0) return yield* Effect.fail(new Error(`Native compilation exited with status ${code}`));
}).pipe(Effect.provide(NodeServices.layer)));
