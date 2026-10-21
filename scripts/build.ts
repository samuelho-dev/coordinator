import { rm } from "node:fs/promises";
import { build } from "esbuild";

await rm(new URL("../dist", import.meta.url), { recursive: true, force: true });

for (const harness of ["pi", "omp"]) {
  await build({
    entryPoints: ["src/plugin.ts"],
    bundle: true,
    // OMP's compatibility loader walks external runtime dependency graphs.
    packages: harness === "omp" ? "bundle" : "external",
    external: ["@oh-my-pi/*"],
    platform: "node",
    format: "esm",
    target: "node22",
    define: { COORDINATOR_HARNESS: JSON.stringify(harness) },
    outfile: `dist/plugins/${harness}.js`,
    logLevel: "info",
  });
}
