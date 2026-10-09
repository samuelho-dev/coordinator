import { Config, Console, Effect, Layer, Option, Schema, Stdio } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { PluginConfig, RunnerConfig, InstallPlan, JsonValue, Commands, CommandError, HttpError, UserId, type CommandInput, type InstallCommand } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";

const command = Commands["program.install:host"];
const installHost = Effect.fnUntraced(function*(flags: CommandInput<"program.install:host">) {
  const platform = yield* Platform;
  const fs = platform.fs;
  const { dirname, isAbsolute, join, resolve } = platform.path;
  if (flags["--help"]) {
    yield* Console.log(command.description);
    for (const [name, field] of Object.entries(command.input.fields)) {
      yield* Console.log(`${name}${Schema.is(field)(true) ? "" : " VALUE"}: ${Schema.resolveAnnotations(field)?.description ?? ""}`);
    }
    return;
  }
  const dry = flags["--dry-run"] ?? false;
  const installerPrivateFile = Effect.fnUntraced(function*(path: string) {
    const info = yield* fs.stat(path);
    if (info.type !== "File" || (info.mode & 0o777) !== 0o600) return yield* new CommandError({ message: `Mode-0600 file required: ${path}` });
    return yield* platform.privateFile(path);
  });
  const root = resolve(dirname(yield* platform.path.fromFileUrl(new URL(import.meta.url))), "..");
  const loader = join(root, "node_modules/tsx/dist/loader.mjs");
  const pluginOnly = flags["--plugin-only"] ?? false, runnerOnly = flags["--runner-only"] ?? false;
  const host = pluginOnly ? flags["--host"] : undefined;
  const runtime = flags["--runtime"]!, binary = flags["--host-binary"]!;
  const pluginPath = pluginOnly ? flags["--plugin-config"] : undefined;
  const runnerPath = runnerOnly ? flags["--runner-config"] : undefined;
  const shellQuote = (value: string) => `'${value.replace(/'/g, "'\\''")}'`;
  if (flags["--remote"]) {
    const destination = flags["--remote"], remoteRoot = flags["--remote-root"]!;
    const remoteArgs = [runtime, "--import", join(remoteRoot, "node_modules/tsx/dist/loader.mjs"), join(remoteRoot, "scripts/install.ts"),
      "--runtime", runtime, "--host-binary", binary, ...(pluginPath ? ["--plugin-only", "--host", host!, "--plugin-config", pluginPath] : ["--runner-only", "--runner-config", runnerPath!]), ...(dry ? ["--dry-run"] : [])];
    const output = yield* platform.exec(ChildProcess.make("/usr/bin/ssh", ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", destination, remoteArgs.map(shellQuote).join(" ")]), 120000).pipe(
      Effect.mapError(() => new CommandError({ message: "Remote coordinator install unavailable; repository, dependencies and private configs must already exist at remote absolute paths. No local installation applied." })),
    );
    const plan = Schema.decodeUnknownSync(Schema.fromJsonString(InstallPlan), { onExcessProperty: "error" })(output);
    yield* Console.log(Schema.encodeUnknownSync(Schema.fromJsonString(InstallPlan, { space: 2 }), { onExcessProperty: "error" })(plan));
    return plan;
  }
  // Effect has no OS platform or current effective UID API; these enforce the native-account privilege boundary.
  if (runnerOnly && (process.platform !== "linux" || process.getuid?.() !== 0)) return yield* new CommandError({ message: "--runner-only requires root on Linux; macOS runner isolation is unavailable (plugin-only supported)" });
  if (pluginOnly && process.getuid?.() === 0) return yield* new CommandError({ message: "--plugin-only must run as the native nonroot user; root plugin registration is forbidden" });
  for (const path of [runtime, binary]) {
    const info = yield* fs.stat(path);
    if (info.type !== "File" || !(info.mode & 0o111)) return yield* new CommandError({ message: "Executable runtime/native launcher required" });
  }
  yield* fs.readFileString(loader);
  // Registration and service launches resolve the installed compiled private package.
  // Never substitute workspace TypeScript when a deployed release omitted its runtime files.
  const nativePackage = join(root, "node_modules/@samuelho/coordinator-native-control");
  for (const entry of ["contracts", "lifecycle", "client", "server", "interaction"]) {
    yield* fs.readFileString(join(nativePackage, "dist", `${entry}.js`)).pipe(
      Effect.mapError(() => new CommandError({ message: `Installed compiled native-control runtime missing: dist/${entry}.js; deploy the packed private package before registration` })),
    );
  }
  for (const entry of ["plugin", "native-plugin"]) {
    yield* fs.readFileString(join(root, "dist/plugins", `${entry}.js`)).pipe(
      Effect.mapError(() => new CommandError({ message: `Bundled public extension missing: dist/plugins/${entry}.js; build the release before registration` })),
    );
  }
  const sqliteProbe = "import{Context,Effect,Layer}from'effect';import*as SqliteClient from'@effect/sql-sqlite-node/SqliteClient';await Effect.runPromise(Effect.scoped(Effect.gen(function*(){const context=yield*Layer.build(SqliteClient.layer({filename:':memory:'}));yield*Context.get(context,SqliteClient.SqliteClient).unsafe('SELECT 1')})));";
  if (pluginOnly) {
    yield* platform.exec(ChildProcess.make(runtime, ["--import", loader, "--input-type=module", "-e", sqliteProbe], { cwd: root }));
    yield* platform.exec(ChildProcess.make(binary, ["--version"]));
  }
  const pluginContents = pluginPath ? yield* installerPrivateFile(pluginPath) : undefined;
  const runnerContents = runnerPath ? yield* installerPrivateFile(runnerPath) : undefined;
  const plugin = pluginContents !== undefined ? yield* Effect.try(() => Schema.decodeUnknownSync(Schema.fromJsonString(PluginConfig), { onExcessProperty: "error" })(pluginContents)) : undefined;
  const runner = runnerContents !== undefined ? yield* Effect.try(() => Schema.decodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(runnerContents)) : undefined;
  if (plugin && plugin.host !== host) return yield* new CommandError({ message: "Plugin host mismatch" });
  for (const config of [...(plugin ? [plugin] : []), ...(runner ? [runner] : [])]) {
    for (const path of [config.stateDirectory, config.tokenFile]) if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) return yield* new CommandError({ message: "Absolute secure configuration paths required" });
    yield* installerPrivateFile(config.tokenFile);
    if ((yield* fs.stat(dirname(config.tokenFile))).mode & 0o077) return yield* new CommandError({ message: "Private token directory required" });
  }
  const program = pluginOnly ? [binary] : [runtime, "--import", loader, join(root, "src/main.ts"), "runner"];
  const commands: InstallCommand[] = plugin ? [{ binary, args: host === "omp" ? ["plugin", "link", root] : ["install", root] }] : [];
  const configDirectory = plugin ? yield* Config.option(Config.String("XDG_CONFIG_HOME")) : Option.none<string>();
  const installedConfig = plugin ? join(Option.isSome(configDirectory) ? configDirectory.value : join(yield* Config.String("HOME"), ".config"), "coordinator", "plugin.json") : undefined;
  let service: string | undefined;
  let content = "";
  if (runner && runnerPath) {
    const owner = yield* fs.stat(runnerPath), tokenOwner = yield* fs.stat(runner.tokenFile);
    const uid = yield* Schema.decodeUnknownEffect(UserId, { onExcessProperty: "error" })(Option.getOrThrow(owner.uid)).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" }))), gid = Option.getOrThrow(owner.gid);
    if ((yield* fs.realPath(runnerPath)) !== runnerPath || (yield* fs.realPath(runner.tokenFile)) !== runner.tokenFile ||
      owner.type !== "File" || tokenOwner.type !== "File" || (owner.mode & 0o777) !== 0o600 || (tokenOwner.mode & 0o777) !== 0o600 ||
      uid === 0 || gid === 0 || uid !== Option.getOrThrow(tokenOwner.uid) || gid !== Option.getOrThrow(tokenOwner.gid)) return yield* new CommandError({ message: "Runner config/token must be mode-0600 regular files owned by the same nonroot UID/GID" });
    const account = (yield* fs.readFileString("/etc/passwd")).split("\n").map((line) => line.split(":")).find((entry) => Number(entry[2]) === uid);
    if (!account || Number(account[3]) !== gid || !["/usr/sbin/nologin", "/sbin/nologin", "/bin/false", "/usr/bin/false"].includes(account[6]!)) return yield* new CommandError({ message: "Runner owner must be a dedicated non-login service account, not a native/plugin user" });
    if ((yield* fs.readFileString("/etc/group")).split("\n").some((line) => line.split(":")[3]?.split(",").includes(account[0]!))) return yield* new CommandError({ message: "Dedicated service account must have no supplementary group memberships" });
    if (!Number.isSafeInteger(runner.childUidBase) || runner.childUidBase < 1000000 || runner.childUidBase > 0xffffffff - 1000000 || uid >= 1000000) return yield* new CommandError({ message: "Dedicated service UID and reserved child UID range required" });
    const securePath = Effect.fnUntraced(function*(path: string, privateDirectory = false) {
      if (!isAbsolute(path) || resolve(path) !== path || /[\r\n\u0000]/.test(path) || (yield* fs.realPath(path)) !== path) return yield* new CommandError({ message: "Canonical absolute runner paths required" });
      const info = yield* fs.stat(path);
      if (privateDirectory && (info.type !== "Directory" || Option.getOrThrow(info.uid) !== uid || Option.getOrThrow(info.gid) !== gid || (info.mode & 0o077))) return yield* new CommandError({ message: "Private runner directories must belong to the service owner" });
      for (let current = path; ; current = dirname(current)) {
        const entry = yield* fs.stat(current);
        if ((Option.getOrThrow(entry.uid) !== 0 && Option.getOrThrow(entry.uid) !== uid) || (entry.mode & 0o022)) return yield* new CommandError({ message: "Trusted installed release/runtime and runner paths must be root or service-owned, not writable by native accounts" });
        if (current === "/") break;
      }
    });
    for (const path of [runnerPath, runner.tokenFile, root]) yield* securePath(path);
    yield* securePath(yield* fs.realPath(runtime));
    yield* securePath(dirname(runtime));
    const visited = new Set<string>();
    const trustedRelease = (path: string): Effect.Effect<void, unknown> => Effect.gen(function*() {
      const canonical = yield* fs.realPath(path);
      if (visited.has(canonical)) return;
      visited.add(canonical);
      yield* securePath(canonical);
      if ((yield* fs.stat(canonical)).type === "Directory") for (const entry of yield* fs.readDirectory(canonical)) yield* trustedRelease(join(canonical, entry));
    });
    yield* trustedRelease(root);
    for (const path of [binary, ...Object.values(runner.launchers)]) {
      if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) return yield* new CommandError({ message: "Absolute native executable paths required" });
      const info = yield* fs.stat(path);
      if (info.type !== "File" || !(info.mode & 0o111)) return yield* new CommandError({ message: "Native executable required" });
    }
    for (const path of [dirname(runnerPath), dirname(runner.tokenFile), runner.stateDirectory, runner.modelDirectory, ...Object.values(runner.repositories)]) yield* securePath(path, true);
    if (!isAbsolute(runner.childDirectory) || resolve(runner.childDirectory) !== runner.childDirectory || /[\r\n\u0000]/.test(runner.childDirectory)) return yield* new CommandError({ message: "Canonical absolute child directory required" });
    yield* securePath(dirname(runner.childDirectory));
    if (!((yield* fs.stat("/usr/bin/flock")).mode & 0o111)) return yield* new CommandError({ message: "Executable flock required" });
    const caps = "-all,+chown,+dac_override,+kill,+setuid,+setgid,+setpcap";
    const moduleUrl = Effect.fnUntraced(function*(file: string) { return Schema.encodeUnknownSync(Schema.fromJsonString(JsonValue), { onExcessProperty: "error" })((yield* platform.path.toFileUrl(join(root, file))).href); });
    const isolationProbe = `${sqliteProbe}
import { Config, Effect as E, Layer as L, Schema } from "effect";
import { NodeServices } from "@effect/platform-node";
import { requireIsolation } from ${yield* moduleUrl("src/runner.ts")};
import { Platform } from ${yield* moduleUrl("src/platform.ts")};
import { RunnerConfig } from ${yield* moduleUrl("src/contracts.ts")};
await E.runPromise(E.gen(function*() {
  const p = yield* Platform;
  const text = yield* p.privateFile(yield* Config.String("COORDINATOR_CONFIG"));
  yield* requireIsolation(Schema.decodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(text));
}).pipe(E.provide(L.merge(Platform.layer, NodeServices.layer))));`;
    // Native version probes run only as a fresh child UID with every capability cleared.
    yield* Effect.gen(function*() {
      yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [`--reuid=${runner.childUidBase}`, `--regid=${runner.childUidBase}`, "--clear-groups", "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs", binary, "--version"], { env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", HOME: "/tmp" }, extendEnv: false }), 10000);
      yield* platform.exec(ChildProcess.make("/usr/bin/setpriv", [`--reuid=${uid}`, `--regid=${gid}`, "--clear-groups", `--bounding-set=${caps}`, `--inh-caps=${caps}`, `--ambient-caps=${caps}`, "--no-new-privs", runtime, "--import", loader, "--input-type=module", "-e", isolationProbe], { cwd: root, env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", COORDINATOR_CONFIG: runnerPath }, extendEnv: false }), 30000);
    }).pipe(Effect.mapError(() => new CommandError({ message: "Actual service UID/capability isolation validation failed; no system service installed" })));
    yield* securePath(runner.childDirectory);
    const childOwner = yield* fs.stat(runner.childDirectory);
    if (Option.getOrThrow(childOwner.uid) !== uid || Option.getOrThrow(childOwner.gid) !== gid) return yield* new CommandError({ message: "Child directory must belong to the service owner" });
    service = "/etc/systemd/system/coordinator-runner.service";
    const quote = (text: string) => `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/%/g, "%%")}"`;
    const capabilities = "CAP_CHOWN CAP_DAC_OVERRIDE CAP_KILL CAP_SETUID CAP_SETGID CAP_SETPCAP";
    content = `[Unit]\nDescription=Coordinator approved-work runner\nAfter=network-online.target\n[Service]\nType=simple\nUser=${uid}\nGroup=${gid}\nSupplementaryGroups=\nWorkingDirectory=${quote(root)}\nEnvironment=${quote(`COORDINATOR_CONFIG=${runnerPath}`)}\nExecStart=${program.map(quote).join(" ")}\nAmbientCapabilities=${capabilities}\nCapabilityBoundingSet=${capabilities}\nRestart=on-failure\nRestartSec=10\nUMask=0077\nNoNewPrivileges=true\n[Install]\nWantedBy=multi-user.target\n`;
    commands.push({ binary: "/usr/bin/systemctl", args: ["daemon-reload"] }, { binary: "/usr/bin/systemctl", args: ["enable", "--now", "coordinator-runner.service"] });
  }
  if (!dry) {
    if (service) {
      yield* fs.makeDirectory(dirname(service), { recursive: true, mode: 0o700 });
      yield* fs.writeFileString(service, content, { mode: 0o600 });
    }
    if (plugin && installedConfig) {
      yield* fs.makeDirectory(dirname(installedConfig), { recursive: true, mode: 0o700 });
      if ((yield* fs.stat(dirname(installedConfig))).mode & 0o077) return yield* new CommandError({ message: "Private plugin configuration directory required" });
      if (yield* fs.exists(installedConfig)) yield* installerPrivateFile(installedConfig);
      yield* fs.writeFileString(installedConfig, yield* Schema.encodeEffect(Schema.fromJsonString(PluginConfig))(plugin), { mode: 0o600 });
    }
    for (const command of commands) yield* platform.exec(ChildProcess.make(command.binary, command.args, { cwd: root })).pipe(
      Effect.mapError(() => new CommandError({ message: `Installer operation failed: ${command.binary}; subprocess output suppressed to protect secrets` })),
    );
  }
  // Print only paths and operation plans, never config contents, tokens, or host subprocess output.
  const plan: InstallPlan = { dryRun: dry, mode: pluginOnly ? "plugin-only" : "runner-only", service, pluginConfig: installedConfig, program, operations: commands, host, package: root };
  yield* Console.log(Schema.encodeUnknownSync(Schema.fromJsonString(InstallPlan, { space: 2 }), { onExcessProperty: "error" })(plan));
  return plan;
});

NodeRuntime.runMain(Effect.gen(function*() {
  const args = yield* (yield* Stdio.Stdio).args;
  const flags: Record<string, unknown> = {};
  for (let index = 0; index < args.length; index++) {
    const name = args[index]!;
    if (!Object.hasOwn(command.input.fields, name)) return yield* new CommandError({ message: `Unknown installer flag: ${name}` });
    if (Object.hasOwn(flags, name)) return yield* new CommandError({ message: `Duplicate installer flag: ${name}` });
    const field = command.input.fields[name as keyof typeof command.input.fields];
    if (Schema.is(field)(true)) flags[name] = true;
    else {
      const value = args[++index];
      if (value === undefined || value.startsWith("--")) return yield* new CommandError({ message: `Required value for ${name}` });
      flags[name] = value;
    }
  }
  return yield* command.executeDecoded({ installHost }, flags);
}).pipe(Effect.provide(Layer.merge(Platform.layer, NodeServices.layer))));
