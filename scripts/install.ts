#!/usr/bin/env node
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Layer, Logger, Option, Path, Schema } from "effect";
import { Command, Flag } from "effect/cli";
import { loadConnectorConfig, readConnectorToken } from "../src/native.ts";
import { checkExecutable, Host } from "../src/platform.ts";

class InstallError extends Schema.TaggedError<InstallError>()("InstallError", { message: Schema.String }) {}
const installationFailure = () => new InstallError({ message: "Installation failed; check owner, private paths, installed package and destination permissions. Existing native configuration was not changed." });
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const packageRoot = new URL("../../", import.meta.url);
const help = `Usage: coordinator-install --target pi|omp|claude|codex|hermes --destination /absolute/new-directory [--config /absolute/private/inspector.json] [--runtime /absolute/node] [--source] [--dry-run]

Run as the native session owner. --destination must be a NEW directory under an existing private owner directory; no existing native files are overwritten.
--config selects an existing private connector config; otherwise use COORDINATOR_CONNECTOR_CONFIG, the default private file, or COORDINATOR_API_URL/COORDINATOR_API_TOKEN. --runtime defaults to this Node executable.
--dry-run validates prerequisites and prints activation instructions without writing.
Printed native install commands pin the published package version. --source instead activates this exact local package (for intentional checkout/tarball development, never a managed native node_modules directory). Generated include artifacts require separate explicit activation; supervise the external coordinator connector separately. API tokens must be inherited from the native owner's environment/secret manager or read from the private config; they are never embedded in artifacts.
No settings/profile/statusLine replacement, exporter activation, remote setup, service management or historical data changes. Claude status-line metrics require optional manual composition; the installer does not configure a status line.`;

const install = (version: string) => Command.make("coordinator-install", {
  target: Flag.String("target").pipe(Flag.optional),
  destination: Flag.String("destination").pipe(Flag.optional),
  config: Flag.String("config").pipe(Flag.optional),
  runtime: Flag.String("runtime").pipe(Flag.optional),
  source: Flag.Boolean("source").pipe(Flag.withDefault(false)),
  dryRun: Flag.Boolean("dry-run").pipe(Flag.withDefault(false)),
}, (flags) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const values = {
    target: Option.getOrUndefined(flags.target),
    destination: Option.getOrUndefined(flags.destination),
    config: Option.getOrUndefined(flags.config),
    runtime: Option.getOrUndefined(flags.runtime),
  };
  const { dryRun } = flags;
  const target = values.target, destination = values.destination;
  if (!target || !["pi", "omp", "claude", "codex", "hermes"].includes(target) || !destination) return yield* new InstallError({ message: "--target and --destination are required. Run --help." });
  const ambientConfig = yield* Config.String("COORDINATOR_CONNECTOR_CONFIG").pipe(Config.option);
  const ambientUrl = yield* Config.String("COORDINATOR_API_URL").pipe(Config.option);
  const ambientToken = yield* Config.String("COORDINATOR_API_TOKEN").pipe(Config.option);
  const ambientXdg = yield* Config.String("XDG_CONFIG_HOME").pipe(Config.option);
  const configFile = values.config ?? Option.getOrUndefined(ambientConfig);
  const runtime = values.runtime ?? host.executable;
  for (const file of [destination, runtime, ...(configFile === undefined ? [] : [configFile])]) {
    if (!path.isAbsolute(file) || path.resolve(file) !== file || /[\r\n\u0000]/u.test(file)) return yield* new InstallError({ message: "Canonical absolute paths without control characters are required." });
  }
  if (host.uid === 0) return yield* new InstallError({ message: "Run the installer as the nonroot native session owner." });
  const parent = path.dirname(destination), owner = host.uid;
  const parentInfo = yield* fs.stat(parent).pipe(Effect.mapError(installationFailure));
  if (parentInfo.type !== "Directory" || owner === undefined || Option.getOrUndefined(parentInfo.uid) !== owner || (parentInfo.mode & 0o077) !== 0 || (yield* fs.realPath(parent).pipe(Effect.mapError(installationFailure))) !== parent) return yield* new InstallError({ message: "Destination parent must be a canonical mode-0700 directory owned by the current user." });
  if (yield* fs.exists(destination).pipe(Effect.mapError(installationFailure))) return yield* new InstallError({ message: "Destination already exists; choose a new directory. No existing configuration is overwritten." });
  const executable = yield* fs.stat(runtime).pipe(Effect.mapError(installationFailure));
  if (executable.type !== "File" || (yield* fs.realPath(runtime).pipe(Effect.mapError(installationFailure))) !== runtime) return yield* new InstallError({ message: "--runtime must name a regular Node executable." });
  yield* checkExecutable(runtime).pipe(Effect.mapError(installationFailure));
  const config = yield* loadConnectorConfig(values.config);
  if (!config) return yield* new InstallError({ message: "Configure COORDINATOR_API_URL or select a private connector config. Run --help." });
  yield* readConnectorToken(config);
  const root = yield* path.fromFileUrl(packageRoot);
  const { join, dirname } = path;
  const main = join(root, "dist/src/main.js");
  yield* fs.access(main, { readable: true }).pipe(Effect.mapError(installationFailure));
  const selectedEnvironment: ReadonlyArray<readonly [string, string]> = configFile === undefined ? [] : [["COORDINATOR_CONNECTOR_CONFIG", configFile]];
  const environment = selectedEnvironment.map(([key, value]) => `${key}=${quote(value)}`).join(" ");
  // MCP hosts differ: OMP inherits Bun.env; Claude/Hermes support ${VAR}; Codex uses env_vars.
  // Keep the file pointer literal, but never pin an ambient URL over future runtime overrides.
  const mcpEnvironment = configFile === undefined ? {} : { COORDINATOR_CONNECTOR_CONFIG: configFile };
  const referencedMcpEnvironment = {
    ...mcpEnvironment,
    ...(Option.isSome(ambientUrl) ? { COORDINATOR_API_URL: "${COORDINATOR_API_URL}" } : {}),
    ...(Option.isSome(ambientToken) ? { COORDINATOR_API_TOKEN: "${COORDINATOR_API_TOKEN}" } : {}),
    ...(Option.isSome(ambientXdg) ? { XDG_CONFIG_HOME: "${XDG_CONFIG_HOME}" } : {}),
  };
  const mcp = { command: runtime, args: [main, "mcp"], env: mcpEnvironment };
  const command = (mode: string) => `${environment} ${quote(runtime)} ${quote(main)} ${mode}`;
  const files: { path: string; content: string; executable?: boolean }[] = [];
  const json = (path: string, value: object) => files.push({ path, content: `${JSON.stringify(value, null, 2)}\n` });
  files.push({ path: "connector.sh", content: `#!/bin/sh\nexec env ${command("connector")}\n`, executable: true });
  files.push({ path: "mcp.sh", content: `#!/bin/sh\nexec env ${command("mcp")}\n`, executable: true });
  let activation = "";
  if (target === "pi" || target === "omp") {
    const bundle = join(root, `dist/plugins/${target}.js`);
    yield* fs.access(bundle, { readable: true }).pipe(Effect.mapError(installationFailure));
    // Published activation must not self-link a native package manager's node_modules tree.
    if (target === "omp") json("mcp.json", { mcpServers: { coordinator: mcp } });
    const activationCommand = flags.source
      ? `${target === "pi" ? "pi install" : "omp plugin link"} ${quote(root)}`
      : `${target === "pi" ? "pi install" : "omp plugin install"} ${quote(`${target === "pi" ? "npm:" : ""}@samuelho-dev/coordinator@${version}`)}`;
    activation = `${activationCommand}\n${target === "pi" ? "The extension provides native coordinator_session_list and coordinator_session_inspect tools; no Pi MCP configuration or adapter is required." : `Merge ONLY the coordinator entry from ${join(destination, "mcp.json")} into ~/.omp/agent/mcp.json; preserve all existing entries.`}\nLaunch the native harness with the same configuration environment${environment ? ` and ${environment}` : ""}.`;
  } else if (target === "claude" || target === "codex") {
    const events = ["SessionStart", "SessionEnd", "UserPromptSubmit", "PreToolUse", "PostToolUse", "PermissionRequest", "Stop", ...(target === "claude" ? ["PostToolUseFailure", "PostModelSwitch", "PreCompact", "StopFailure", "Elicitation", "ElicitationResult"] : ["Interrupt"])];
    const hooks = Object.fromEntries(events.map((event) => [event, [{ hooks: [{ type: "command", command: command(`hook ${target}`), timeout: 3 }] }]]));
    json("hooks/hooks.json", { hooks });
    if (target === "claude") {
      json(".claude-plugin/plugin.json", { name: "coordinator-inspector", version, description: "Read-only native session metadata inspector" });
      json(".mcp.json", { mcpServers: { coordinator: { ...mcp, env: referencedMcpEnvironment } } });
      activation = `claude --plugin-dir ${quote(destination)}\nApprove the plugin's read-only MCP tools and metadata hooks. Existing statusLine is untouched; optional manual composition can pass the same status-line stdin to ${command("claude-statusline")} without replacing your existing output.`;
    } else {
      files.push({ path: "mcp.toml", content: `[mcp_servers.coordinator]\ncommand = ${JSON.stringify(runtime)}\nargs = ${JSON.stringify([main, "mcp"])}\nenv_vars = ["COORDINATOR_API_TOKEN", "COORDINATOR_API_URL", "XDG_CONFIG_HOME"]\n${configFile === undefined ? "" : `\n[mcp_servers.coordinator.env]\nCOORDINATOR_CONNECTOR_CONFIG = ${JSON.stringify(configFile)}\n`}` });
      activation = `Merge ONLY generated hooks from ${join(destination, "hooks/hooks.json")} into your selected ~/.codex/hooks.json (or project .codex/hooks.json); preserve all existing hooks.\nMerge the coordinator MCP table from ${join(destination, "mcp.toml")} into your selected Codex config.toml; preserve all other tables.\nIn Codex's /hooks manager, review each Coordinator command and use that selected handler's Trust action; do not trust unrelated handlers. Loaded but untrusted hooks do not run.\nToken notifications are available only from an already attached native app-server source; this installer never opens another thread or app-server.`;
    }
  } else {
    // Hermes parses argv and execs directly: env replaces itself, preserving the native hook-host parent.
    const hooks = Object.fromEntries(["on_session_start", "on_session_end", "on_session_finalize", "post_api_request"].map((event) => [event, [{ command: `/usr/bin/env ${command("hook hermes")}`, timeout: 3 }]]));
    json("hooks.yaml", { hooks });
    const hermesMcp = { ...mcp, env: referencedMcpEnvironment };
    json("mcp.json", { mcpServers: { coordinator: hermesMcp } });
    json("mcp.yaml", { mcp_servers: { coordinator: hermesMcp } });
    activation = `Manually append ONLY generated hook entries from ${join(destination, "hooks.yaml")} to hooks in your selected Hermes profile's config.yaml; preserve all existing hooks.\nMerge ONLY mcp_servers.coordinator from ${join(destination, "mcp.yaml")} into the same profile's config.yaml; preserve all existing MCP servers and other settings. Review/approve the native commands before activation. No plugin installation or enable command is needed.\nThe equivalent stdio launcher is ${join(destination, "mcp.sh")}.\nNative shell-hook configuration/approval: https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks`;
  }
  if (!dryRun) {
    // New-directory creation is the overwrite fence: existing native settings are never opened for writing.
    yield* fs.makeDirectory(destination, { mode: 0o700 }).pipe(Effect.mapError(installationFailure));
    for (const file of files) {
      yield* fs.makeDirectory(dirname(join(destination, file.path)), { recursive: true, mode: 0o700 }).pipe(Effect.mapError(installationFailure));
      yield* fs.writeFileString(join(destination, file.path), file.content, { flag: "wx", mode: file.executable ? 0o700 : 0o600 }).pipe(Effect.mapError(installationFailure));
    }
  }
  yield* Console.log(`${dryRun ? "Validated (no writes)." : "Created native artifacts."}\n${activation}\nProvision COORDINATOR_API_TOKEN in the native owner's inherited environment/secret manager when credentials are not supplied by the selected private file. Tokens are not embedded in generated files or commands.\nFor external native metadata presence, supervise ${quote(join(destination, "connector.sh"))} as the same owner. No existing native configuration was modified.`);
})).pipe(Command.withDescription(help));

NodeRuntime.runMain(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const manifest = yield* path.fromFileUrl(new URL("package.json", packageRoot));
  const { version } = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ version: Schema.String })))(
    yield* fs.readFileString(manifest).pipe(Effect.mapError(installationFailure)),
  );
  yield* Command.run(install(version), { version, renderErrors: false });
}).pipe(
  Effect.catchTag("ShowHelp", () => Effect.fail(new InstallError({ message: "Invalid installer flags. Run --help; credentials are accepted only through environment or private config files." }))),
  Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer, Layer.succeed(Logger.LogToStderr, true))),
));
