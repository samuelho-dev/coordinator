import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Schema } from "effect";
import { access, lstat, mkdir, realpath, writeFile } from "node:fs/promises";
import { constants, existsSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadConnectorConfig, readConnectorToken } from "../src/native.ts";

class InstallError extends Schema.TaggedError<InstallError>()("InstallError", { message: Schema.String }) {}
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: () => new InstallError({ message: "Installation failed; check owner, private paths, release build and destination permissions. Existing native configuration was not changed." }) });
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const help = `Usage: pnpm install:host --target pi|omp|claude|codex|hermes --destination /absolute/new-directory --config /absolute/private/inspector.json [--runtime /absolute/node] [--dry-run]

Build first with pnpm build. Run as the native session owner. --destination must be a NEW directory under an existing private owner directory; no existing native files are overwritten.
--config is an existing private connector config (and private tokenFile), never a bearer argument. --runtime defaults to this Node executable.
--dry-run validates prerequisites and prints activation instructions without writing.
Generated native extension/plugin or include artifacts require explicit activation shown on success. Start the connector separately with COORDINATOR_CONNECTOR_CONFIG and coordinator connector.
No settings/profile/statusLine replacement, exporter activation, remote setup, service management or historical data changes. Claude status-line metrics require optional manual composition; the installer does not configure a status line.`;

const install = Effect.gen(function*() {
  const { values } = yield* Effect.try({
    try: () => parseArgs({ options: { help: { type: "boolean" }, target: { type: "string" }, destination: { type: "string" }, config: { type: "string" }, runtime: { type: "string" }, "dry-run": { type: "boolean" } }, strict: true, allowPositionals: false }),
    catch: () => new InstallError({ message: "Invalid installer flags. Run --help; credentials are accepted only through private config files." }),
  });
  const { "dry-run": dryRun } = values;
  if (values.help) return yield* Console.log(help);
  const target = values.target, destination = values.destination, configFile = values.config;
  if (!target || !["pi", "omp", "claude", "codex", "hermes"].includes(target) || !destination || !configFile) return yield* new InstallError({ message: "--target, --destination and --config are required. Run --help." });
  const runtime = values.runtime ?? process.execPath;
  for (const path of [destination, configFile, runtime]) {
    if (!isAbsolute(path) || resolve(path) !== path || /[\r\n\u0000]/u.test(path)) return yield* new InstallError({ message: "Canonical absolute paths without control characters are required." });
  }
  if (process.getuid?.() === 0) return yield* new InstallError({ message: "Run the installer as the nonroot native session owner." });
  const parent = dirname(destination), owner = process.getuid?.();
  const parentInfo = yield* io(() => lstat(parent));
  if (!parentInfo.isDirectory() || parentInfo.uid !== owner || (parentInfo.mode & 0o077) !== 0 || (yield* io(() => realpath(parent))) !== parent) return yield* new InstallError({ message: "Destination parent must be a canonical mode-0700 directory owned by the current user." });
  if (existsSync(destination)) return yield* new InstallError({ message: "Destination already exists; choose a new directory. No existing configuration is overwritten." });
  const executable = yield* io(() => lstat(runtime));
  if (!executable.isFile()) return yield* new InstallError({ message: "--runtime must name a regular Node executable." });
  yield* io(() => access(runtime, constants.X_OK));
  const config = yield* loadConnectorConfig(configFile);
  if (!config) return yield* new InstallError({ message: "An explicit valid private connector config is required." });
  yield* readConnectorToken(config);
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const main = join(root, "dist/src/main.js");
  yield* io(() => access(main, constants.R_OK));
  const env = { COORDINATOR_CONNECTOR_CONFIG: configFile };
  const mcp = { command: runtime, args: [main, "mcp"], env };
  const command = (mode: string) => `COORDINATOR_CONNECTOR_CONFIG=${quote(configFile)} ${quote(runtime)} ${quote(main)} ${mode}`;
  const files: { path: string; content: string; executable?: boolean }[] = [];
  const json = (path: string, value: object) => files.push({ path, content: `${JSON.stringify(value, null, 2)}\n` });
  files.push({ path: "connector.sh", content: `#!/bin/sh\nexec env COORDINATOR_CONNECTOR_CONFIG=${quote(configFile)} ${quote(runtime)} ${quote(main)} connector\n`, executable: true });
  files.push({ path: "mcp.sh", content: `#!/bin/sh\nexec env COORDINATOR_CONNECTOR_CONFIG=${quote(configFile)} ${quote(runtime)} ${quote(main)} mcp\n`, executable: true });
  let activation = "";
  if (target === "pi" || target === "omp") {
    const bundle = join(root, `dist/plugins/${target}.js`);
    yield* io(() => access(bundle, constants.R_OK));
    json("package.json", { name: "coordinator-inspector-extension", version: "1.0.0", private: true, type: "module", ...(target === "pi" ? { pi: { extensions: [bundle] } } : { omp: { extensions: [bundle] } }) });
    json("mcp.json", { mcpServers: { coordinator: mcp } });
    activation = `${target === "pi" ? "pi install" : "omp plugin link"} ${quote(destination)}\nMerge ONLY the coordinator entry from ${join(destination, "mcp.json")} into your selected native ${target === "pi" ? ".pi/agent/mcp.json or project .pi/mcp.json" : ".omp/mcp.json"}; preserve all existing entries.\nLaunch the native harness with COORDINATOR_CONNECTOR_CONFIG=${quote(configFile)}.`;
  } else if (target === "claude" || target === "codex") {
    const events = ["SessionStart", "SessionEnd", "UserPromptSubmit", "PreToolUse", "PostToolUse", "PermissionRequest", "Stop", ...(target === "claude" ? ["PostToolUseFailure", "PostModelSwitch", "PreCompact"] : ["Interrupt"])];
    const hooks = Object.fromEntries(events.map((event) => [event, [{ hooks: [{ type: "command", command: command(`hook ${target}`), timeout: 3 }] }]]));
    json("hooks/hooks.json", { hooks });
    if (target === "claude") {
      json(".claude-plugin/plugin.json", { name: "coordinator-inspector", version: "1.0.0", description: "Read-only native session metadata inspector" });
      json(".mcp.json", { mcpServers: { coordinator: mcp } });
      activation = `claude --plugin-dir ${quote(destination)}\nApprove the plugin's read-only MCP tools and metadata hooks. Existing statusLine is untouched; optional manual composition can pass the same status-line stdin to ${command("claude-statusline")} without replacing your existing output.`;
    } else {
      files.push({ path: "mcp.toml", content: `[mcp_servers.coordinator]\ncommand = ${JSON.stringify(runtime)}\nargs = ${JSON.stringify([main, "mcp"])}\n\n[mcp_servers.coordinator.env]\nCOORDINATOR_CONNECTOR_CONFIG = ${JSON.stringify(configFile)}\n` });
      activation = `Merge ONLY generated hooks from ${join(destination, "hooks/hooks.json")} into your selected ~/.codex/hooks.json (or project .codex/hooks.json); preserve all existing hooks.\nMerge the coordinator MCP table from ${join(destination, "mcp.toml")} into your selected Codex config.toml; preserve all other tables. Review/approve the new native hooks.\nToken notifications are available only from an already attached native app-server source; this installer never opens another thread or app-server.`;
    }
  } else {
    // Hermes parses argv and execs directly: env replaces itself, preserving the native hook-host parent.
    const hooks = Object.fromEntries(["on_session_start", "on_session_end", "on_session_finalize", "post_api_request"].map((event) => [event, [{ command: `/usr/bin/env ${command("hook hermes")}`, timeout: 3 }]]));
    json("hooks.yaml", { hooks });
    json("mcp.json", { mcpServers: { coordinator: mcp } });
    json("mcp.yaml", { mcp_servers: { coordinator: mcp } });
    activation = `Manually append ONLY generated hook entries from ${join(destination, "hooks.yaml")} to hooks in your selected Hermes profile's config.yaml; preserve all existing hooks.\nMerge ONLY mcp_servers.coordinator from ${join(destination, "mcp.yaml")} into the same profile's config.yaml; preserve all existing MCP servers and other settings. Review/approve the native commands before activation. No plugin installation or enable command is needed.\nThe equivalent stdio launcher is ${join(destination, "mcp.sh")}.\nNative shell-hook configuration/approval: https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks`;
  }
  if (!dryRun) {
    // New-directory creation is the overwrite fence: existing native settings are never opened for writing.
    yield* io(() => mkdir(destination, { mode: 0o700 }));
    for (const file of files) {
      yield* io(() => mkdir(dirname(join(destination, file.path)), { recursive: true, mode: 0o700 }));
      yield* io(() => writeFile(join(destination, file.path), file.content, { flag: "wx", mode: file.executable ? 0o700 : 0o600 }));
    }
  }
  yield* Console.log(`${dryRun ? "Validated (no writes)." : "Created native artifacts."}\n${activation}\nFor external native metadata presence, run ${quote(join(destination, "connector.sh"))} as the same owner. No existing native configuration was modified.`);
});

NodeRuntime.runMain(install.pipe(Effect.provide(NodeServices.layer)));
