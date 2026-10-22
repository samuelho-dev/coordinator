import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Fiber, FileSystem, Layer, Path, Schema, Stdio, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Host } from "../src/platform.ts";

const guard = (condition: boolean, message: string) => condition ? Effect.void : Effect.die(message);
const Manifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  bin: Schema.Struct({ coordinator: Schema.String, "coordinator-install": Schema.String }),
  pi: Schema.Struct({ extensions: Schema.Array(Schema.String) }),
  omp: Schema.Struct({ extensions: Schema.Array(Schema.String) }),
  scripts: Schema.optional(Schema.Struct({
    preinstall: Schema.optional(Schema.String), install: Schema.optional(Schema.String), postinstall: Schema.optional(Schema.String),
  })),
});

const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const stdio = yield* Stdio.Stdio;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const [input, ...extra] = yield* stdio.args;
  if (input === undefined || extra.length !== 0) return yield* Effect.die("Usage: node scripts/package-smoke.ts /path/to/package.tgz (build first; host Pi/OMP required)");
  const tarball = yield* fs.realPath(path.resolve(input));
  const checkout = yield* fs.realPath(yield* path.fromFileUrl(new URL("../", import.meta.url)));
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator package smoke-" });
  yield* fs.chmod(root, 0o700);
  const privateRoot = yield* fs.realPath(root);
  yield* guard(privateRoot !== checkout && !privateRoot.startsWith(`${checkout}${path.sep}`), "Package installation must be outside the checkout");
  const home = path.join(root, "home");
  yield* fs.makeDirectory(home, { mode: 0o700 });
  const searchPath = yield* Config.String("PATH");
  const environment = { HOME: home, PATH: searchPath, npm_config_cache: path.join(root, "npm-cache") };
  const run = (command: string, args: ReadonlyArray<string>, env: Readonly<Record<string, string>> = environment) => Effect.gen(function*() {
    const child = yield* spawner.spawn(ChildProcess.make(command, args, {
      cwd: root, env, extendEnv: false, stdin: Stream.empty, forceKillAfter: "1 second",
    }));
    const output = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const errors = yield* child.stderr.pipe(Stream.decodeText(), Stream.runFold(() => "", (text, chunk) => text + chunk), Effect.forkScoped);
    const code = yield* child.exitCode;
    const stdout = yield* Fiber.join(output);
    const stderr = yield* Fiber.join(errors);
    yield* guard(code === 0, `${command} failed (${code}): ${stderr}\n${stdout}`);
    return stdout;
  });
  const entries = (yield* run("tar", ["-tzf", tarball])).trim().split("\n");
  const payload = /^(?:package\/(?:package\.json|README\.md|LICENSE|THIRD_PARTY_NOTICES\.md)|package\/dist\/(?:src\/[^/]+\.js|plugins\/(?:pi|omp)\.js|scripts\/install\.js))$/;
  for (const entry of entries) {
    yield* guard(payload.test(entry) || ["package/", "package/dist/", "package/dist/src/", "package/dist/plugins/", "package/dist/scripts/"].includes(entry), `Unexpected public tarball entry: ${entry}`);
  }
  for (const required of ["package.json", "README.md", "LICENSE", "THIRD_PARTY_NOTICES.md", "dist/src/main.js", "dist/src/inspection.js", "dist/scripts/install.js", "dist/plugins/pi.js", "dist/plugins/omp.js"]) {
    yield* guard(entries.includes(`package/${required}`), `Missing public tarball entry: ${required}`);
  }
  yield* fs.writeFileString(path.join(root, "package.json"), JSON.stringify({ private: true, type: "module" }));
  yield* run("npm", ["install", "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarball]);
  const packageRoot = path.join(root, "node_modules", "@samuelho", "coordinator");
  const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(yield* fs.readFileString(path.join(packageRoot, "package.json")));
  yield* guard(manifest.name === "@samuelho/coordinator", "Unexpected installed package identity");
  yield* guard(manifest.scripts?.preinstall === undefined && manifest.scripts?.install === undefined && manifest.scripts?.postinstall === undefined, "Published package must not define install lifecycle scripts");
  const { "coordinator-install": installerBin } = manifest.bin;
  yield* guard(manifest.bin.coordinator.replace(/^\.\//, "") === "dist/src/main.js" && installerBin.replace(/^\.\//, "") === "dist/scripts/install.js", "Public binaries must point to built entrypoints");
  yield* guard(JSON.stringify(manifest.pi.extensions) === JSON.stringify(["./dist/plugins/pi.js"]) && JSON.stringify(manifest.omp.extensions) === JSON.stringify(["./dist/plugins/omp.js"]), "Native package metadata must select the published extensions");
  const installed = (yield* run("npm", ["ls", "--all", "--omit=dev", "--parseable"])).trim().split("\n");
  for (const dependency of installed) {
    yield* guard(!/\/node_modules\/(?:@earendil-works\/|@oh-my-pi\/|@effect\/(?:tsgo|vitest)(?:\/|$)|(?:typescript|vitest|esbuild)(?:\/|$))/.test(dependency), `Development/native SDK dependency leaked into production installation: ${dependency}`);
  }
  for (const binary of ["coordinator", "coordinator-install"]) {
    const executable = path.join(root, "node_modules", ".bin", binary);
    yield* guard((yield* fs.realPath(executable)).startsWith(`${packageRoot}${path.sep}`), `${binary} must resolve inside installed package`);
    const version = (yield* run(executable, ["--version"])).trim();
    yield* guard(version.split(/\s+/).at(-1)?.replace(/^v/, "") === manifest.version, `${binary} version ${version} differs from package ${manifest.version}`);
  }
  const pi = yield* Config.String("COORDINATOR_TEST_PI").pipe(Config.withDefault(path.join(checkout, "node_modules", ".bin", "pi")));
  const omp = yield* Config.String("COORDINATOR_TEST_OMP").pipe(Config.withDefault(path.join(checkout, "node_modules", ".bin", "omp")));
  // Stage only the host-owned verifier: its relative runtime imports must resolve inside the installed package.
  const smoke = path.join(packageRoot, "dist", "scripts", "smoke.js");
  yield* fs.writeFileString(smoke, yield* fs.readFileString(path.join(checkout, "dist", "scripts", "smoke.js")), { flag: "wx", mode: 0o600 });
  const evidence = yield* run(host.executable, [smoke], {
    ...environment,
    COORDINATOR_SMOKE_NATIVE: "true",
    COORDINATOR_TEST_PI_SDK: import.meta.resolve("@earendil-works/pi-coding-agent"),
    COORDINATOR_TEST_PI: path.resolve(pi),
    COORDINATOR_TEST_OMP: path.resolve(omp),
  });
  yield* Console.log(evidence.trim());
  yield* Console.log(JSON.stringify({ scenario: "npm-package-acceptance", status: "ok", package: manifest.name, version: manifest.version,
    payloadEntries: entries.length, productionOnly: true, lifecycleScripts: false, isolatedInstall: true,
    installedBins: ["coordinator", "coordinator-install"], installedNativeEntrypoints: ["pi", "omp"], hostOwnedSdkReaders: true }));
});

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("5 minutes"), Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer))));
