import { Config, Effect, Layer, Stdio } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import getExePath from "@effect/tsgo/lib/getExePath";
import { Commands, CommandError, type CommandCapabilities } from "./contracts.ts";
import { Platform } from "./platform.ts";

NodeRuntime.runMain(Effect.gen(function*() {
  const args = yield* (yield* Stdio.Stdio).args;
  const mode = args[0];
  if (args.length !== 1 || (mode !== "check" && mode !== "test")) return yield* new CommandError({ message: "Expected check or test" });
  const platform = yield* Platform;
  const root = platform.path.resolve(platform.path.dirname(yield* platform.path.fromFileUrl(new URL(import.meta.url))), "..");
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const run = Effect.fnUntraced(function*(command: string, args: string[]) {
    const code = yield* spawner.exitCode(ChildProcess.make(command, args, { cwd: root, stdin: "inherit", stdout: "inherit", stderr: "inherit" }));
    if (code !== 0) return yield* new CommandError({ message: `Development command exited with status ${code}` });
  });
  const capabilities = {
    checkTypes: () => Effect.gen(function*() {
      yield* run("pnpm", ["db:generate"]);
      yield* run(process.execPath, ["--import", "tsx", "--input-type=module", "--eval", "import './src/generated/database/index.ts'"]);
      yield* run(getExePath(), ["--noEmit"]);
    }),
    runTests: () => Effect.gen(function*() {
      yield* Config.String("COORDINATOR_TEST_POSTGRES_URL_FILE");
      const files = (yield* platform.fs.readDirectory(platform.path.join(root, "test"))).filter((name) => name.endsWith(".test.ts")).sort();
      if (files.length === 0) return yield* new CommandError({ message: "No repository test files found" });
      yield* run(process.execPath, ["--import", "tsx", "--test", "--test-concurrency=1", ...files.map((name) => platform.path.join(root, "test", name))]);
    }),
  } satisfies CommandCapabilities<"development">;
  yield* Commands[`development.${mode}`].executeDecoded(capabilities, {});
}).pipe(Effect.provide(Layer.merge(Platform.layer, NodeServices.layer))));
