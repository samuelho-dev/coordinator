import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Data, Effect, FileSystem, Path } from "effect";
import { build } from "esbuild";

class BuildError extends Data.TaggedError("BuildError")<{ readonly cause: unknown }> {}

const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dist = yield* path.fromFileUrl(new URL("../dist", import.meta.url));
  yield* fs.remove(dist, { recursive: true, force: true });
  for (const harness of ["pi", "omp"]) {
    yield* Effect.tryPromise({
      try: () => build({
        entryPoints: ["src/plugin.ts"],
        bundle: true,
        // OMP's compatibility loader walks external runtime dependency graphs.
        packages: harness === "omp" ? "bundle" : "external",
        external: ["@oh-my-pi/*", "@earendil-works/*"],
        alias: harness === "omp" ? {
          "@earendil-works/pi-tui": "@oh-my-pi/pi-tui",
          "@earendil-works/pi-coding-agent": "@oh-my-pi/pi-coding-agent",
        } : undefined,
        platform: "node",
        format: "esm",
        target: "node22",
        define: { COORDINATOR_HARNESS: JSON.stringify(harness) },
        outfile: `dist/plugins/${harness}.js`,
        logLevel: "info",
      }),
      catch: (cause) => new BuildError({ cause }),
    });
  }
});

NodeRuntime.runMain(program.pipe(Effect.provide(NodeServices.layer)));
