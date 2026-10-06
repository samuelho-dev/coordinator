import { execFileSync } from "node:child_process";
import { accessSync, constants, mkdirSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { privateFile, type PluginConfig, type RunnerConfig } from "../src/protocol.js";

const args = process.argv.slice(2);
const dry = args.includes("--dry-run");
const option = (name: string): string => {
  const index = args.indexOf(name);
  const value = index >= 0 ? args[index + 1] : undefined;
  if (!value || value.startsWith("--")) throw new Error(`Required ${name}`);
  return value;
};
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const host = option("--host");
if (host !== "omp" && host !== "pi") throw new Error("--host must be omp or pi");
const runtime = option("--runtime");
const binary = option("--host-binary");
const pluginPath = option("--plugin-config");
const runnerPath = option("--runner-config");
for (const path of [runtime, binary, pluginPath, runnerPath]) {
  if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) throw new Error("Absolute single-line paths required");
}
if (args.includes("--remote")) {
  const destination = option("--remote");
  const remoteRoot = option("--remote-root");
  if (!/^[a-zA-Z0-9_.@-]+$/.test(destination) || destination.startsWith("-") || !isAbsolute(remoteRoot) || /[\r\n\u0000]/.test(remoteRoot)) throw new Error("Invalid remote destination/root");
  const shellQuote = (value: string) => `'${value.replace(/'/g, "'\\''")}'`;
  const remoteArgs = [runtime, "--import", join(remoteRoot, "node_modules/tsx/dist/loader.mjs"), join(remoteRoot, "scripts/install.ts"),
    "--host", host, "--runtime", runtime, "--host-binary", binary, "--plugin-config", pluginPath, "--runner-config", runnerPath, ...(dry ? ["--dry-run"] : [])];
  try {
    const output = execFileSync("/usr/bin/ssh", ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", destination, remoteArgs.map(shellQuote).join(" ")], { stdio: "pipe", encoding: "utf8" });
    console.log(JSON.stringify(JSON.parse(output), null, 2));
  }
  catch { throw new Error("Remote coordinator install unavailable; repository, dependencies and private configs must already exist at remote absolute paths. No local installation applied."); }
  process.exit(0);
}
accessSync(runtime, constants.X_OK); accessSync(binary, constants.X_OK);
const plugin = JSON.parse(privateFile(pluginPath)) as PluginConfig;
const runner = JSON.parse(privateFile(runnerPath)) as RunnerConfig;
if (plugin.host !== host) throw new Error("Plugin host mismatch");
for (const config of [plugin, runner]) {
  if (!isAbsolute(config.stateDirectory) || !isAbsolute(config.tokenFile)) throw new Error("Absolute secure configuration paths required");
  privateFile(config.tokenFile);
  if (statSync(dirname(config.tokenFile)).mode & 0o077) throw new Error("Private token directory required");
}
const loader = join(root, "node_modules/tsx/dist/loader.mjs");
accessSync(loader, constants.R_OK);
const program = [runtime, "--import", loader, join(root, "src/runner.ts")];
const commands: { binary: string; args: string[] }[] = [{ binary, args: host === "omp" ? ["plugin", "link", root] : ["install", root] }];
const home = homedir();
const launcher = join(home, ".local/bin", `coordinator-${host}`);
const shellQuote = (value: string) => `'${value.replace(/'/g, "'\\''")}'`;
const wrapper = `#!/bin/sh\nexport COORDINATOR_CONFIG=${shellQuote(pluginPath)}\nexec ${shellQuote(binary)} \"$@\"\n`;
const escapeXml = (text: string) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
let service: string;
let content: string;
if (process.platform === "darwin") {
  service = join(home, "Library/LaunchAgents/dev.coordinator.runner.plist");
  content = `<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict><key>Label</key><string>dev.coordinator.runner</string><key>ProgramArguments</key><array>${program.map((part) => `<string>${escapeXml(part)}</string>`).join("")}</array><key>EnvironmentVariables</key><dict><key>COORDINATOR_CONFIG</key><string>${escapeXml(runnerPath)}</string></dict><key>WorkingDirectory</key><string>${escapeXml(root)}</string><key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>10</integer><key>Umask</key><integer>63</integer></dict></plist>\n`;
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("User launchd requires UID");
  commands.push({ binary: "/bin/launchctl", args: ["bootstrap", `gui/${uid}`, service] });
} else if (process.platform === "linux") {
  service = join(home, ".config/systemd/user/coordinator-runner.service");
  const quote = (text: string) => `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/%/g, "%%")}"`;
  content = `[Unit]\nDescription=Coordinator approved-work runner\nAfter=network-online.target\n[Service]\nType=simple\nWorkingDirectory=${quote(root)}\nEnvironment=${quote(`COORDINATOR_CONFIG=${runnerPath}`)}\nExecStart=${program.map(quote).join(" ")}\nRestart=on-failure\nRestartSec=10\nUMask=0077\nNoNewPrivileges=true\n[Install]\nWantedBy=default.target\n`;
  commands.push({ binary: "/usr/bin/systemctl", args: ["--user", "daemon-reload"] }, { binary: "/usr/bin/systemctl", args: ["--user", "enable", "--now", "coordinator-runner.service"] });
} else throw new Error("Only macOS launchd and Linux systemd are supported");
if (!dry) {
  mkdirSync(dirname(service), { recursive: true, mode: 0o700 });
  writeFileSync(service, content, { mode: 0o600 });
  mkdirSync(dirname(launcher), { recursive: true, mode: 0o700 });
  writeFileSync(launcher, wrapper, { mode: 0o700 });
  for (const command of commands) {
    try { execFileSync(command.binary, command.args, { cwd: root, stdio: "pipe" }); }
    catch { throw new Error(`Installer operation failed: ${command.binary}; subprocess output suppressed to protect secrets`); }
  }
}
// Print only paths and operation plans, never config contents, tokens, or host subprocess output.
console.log(JSON.stringify({ dryRun: dry, service, launcher, program, operations: commands, pluginEnvironment: { COORDINATOR_CONFIG: pluginPath }, host, package: root }, null, 2));
