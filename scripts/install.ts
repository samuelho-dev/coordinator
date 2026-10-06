import { execFileSync } from "node:child_process";
import { accessSync, constants, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { PluginConfig, RunnerConfig } from "../src/protocol.js";

function privateFile(path: string): string {
  if ((statSync(path).mode & 0o777) !== 0o600) throw new Error(`Mode-0600 file required: ${path}`);
  return readFileSync(path, "utf8").trim();
}

const args = process.argv.slice(2);
if (args.includes("--help")) {
  console.log("pnpm install:host -- --runtime /absolute/node --host-binary /absolute/omp-or-pi (--plugin-only --host omp|pi --plugin-config /absolute/private/plugin.json | --runner-only --runner-config /absolute/private/runner.json) [--dry-run] [--remote SSH_ALIAS --remote-root /absolute/repository]");
  console.log("Run --plugin-only as the native user, then --runner-only as root from a trusted installed release on Linux. Runner config/token must belong to a dedicated non-login service account; release/runtime paths must be root/service-owned and not writable by native accounts. Six bounded supervisor capabilities and actual cap-zero child UID isolation are required. macOS supports plugin-only. Mode-0600 configs/credentials and installed dependencies required.");
  process.exit(0);
}
const dry = args.includes("--dry-run");
const option = (name: string): string => {
  const index = args.indexOf(name);
  const value = index >= 0 ? args[index + 1] : undefined;
  if (!value || value.startsWith("--")) throw new Error(`Required ${name}`);
  return value;
};
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pluginOnly = args.includes("--plugin-only");
const runnerOnly = args.includes("--runner-only");
if (pluginOnly === runnerOnly) throw new Error("Select --plugin-only as the native user, then run privileged --runner-only separately; combined/user-service installation is unsupported");
const host = pluginOnly ? option("--host") : undefined;
if (host !== undefined && host !== "omp" && host !== "pi") throw new Error("--host must be omp or pi");
const runtime = option("--runtime");
const binary = option("--host-binary");
const pluginPath = pluginOnly ? option("--plugin-config") : undefined;
const runnerPath = runnerOnly ? option("--runner-config") : undefined;
for (const path of [runtime, binary, ...(pluginPath ? [pluginPath] : []), ...(runnerPath ? [runnerPath] : [])]) {
  if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) throw new Error("Absolute single-line paths required");
}
if (args.includes("--remote")) {
  const destination = option("--remote");
  const remoteRoot = option("--remote-root");
  if (!/^[a-zA-Z0-9_.@-]+$/.test(destination) || destination.startsWith("-") || !isAbsolute(remoteRoot) || /[\r\n\u0000]/.test(remoteRoot)) throw new Error("Invalid remote destination/root");
  const shellQuote = (value: string) => `'${value.replace(/'/g, "'\\''")}'`;
  const remoteArgs = [runtime, "--import", join(remoteRoot, "node_modules/tsx/dist/loader.mjs"), join(remoteRoot, "scripts/install.ts"),
    "--runtime", runtime, "--host-binary", binary, ...(pluginPath ? ["--plugin-only", "--host", host!, "--plugin-config", pluginPath] : ["--runner-only", "--runner-config", runnerPath!]), ...(dry ? ["--dry-run"] : [])];
  try {
    const output = execFileSync("/usr/bin/ssh", ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", destination, remoteArgs.map(shellQuote).join(" ")], { stdio: "pipe", encoding: "utf8" });
    console.log(JSON.stringify(JSON.parse(output), null, 2));
  }
  catch { throw new Error("Remote coordinator install unavailable; repository, dependencies and private configs must already exist at remote absolute paths. No local installation applied."); }
  process.exit(0);
}
if (runnerOnly && (process.platform !== "linux" || process.getuid?.() !== 0)) throw new Error("--runner-only requires root on Linux; macOS runner isolation is unavailable (plugin-only supported)");
if (pluginOnly && process.getuid?.() === 0) throw new Error("--plugin-only must run as the native nonroot user; root plugin registration is forbidden");
accessSync(runtime, constants.X_OK); accessSync(binary, constants.X_OK);
if (pluginOnly) {
  execFileSync(runtime, ["-e", "const s=require('node:sqlite'); const d=new s.DatabaseSync(':memory:');d.close();if(typeof s.backup!=='function')process.exit(1)"], { stdio: "pipe" });
  execFileSync(binary, ["--version"], { stdio: "pipe" });
}
const plugin = pluginPath ? JSON.parse(privateFile(pluginPath)) as PluginConfig : undefined;
const runner = runnerPath ? JSON.parse(privateFile(runnerPath)) as RunnerConfig : undefined;
if (plugin && plugin.host !== host) throw new Error("Plugin host mismatch");
for (const config of [...(plugin ? [plugin] : []), ...(runner ? [runner] : [])]) {
  for (const path of [config.stateDirectory, config.tokenFile]) {
    if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) throw new Error("Absolute secure configuration paths required");
  }
  privateFile(config.tokenFile);
  if (statSync(dirname(config.tokenFile)).mode & 0o077) throw new Error("Private token directory required");
}
const loader = join(root, "node_modules/tsx/dist/loader.mjs");
accessSync(loader, constants.R_OK);
const program = [runtime, "--import", loader, join(root, "src/runner.ts")];
const commands: { binary: string; args: string[] }[] = plugin ? [{ binary, args: host === "omp" ? ["plugin", "link", root] : ["install", root] }] : [];
const launcher = plugin ? join(homedir(), ".local/bin", `coordinator-${host}`) : undefined;
const shellQuote = (value: string) => `'${value.replace(/'/g, "'\\''")}'`;
const wrapper = pluginPath ? `#!/bin/sh\nexport COORDINATOR_CONFIG=${shellQuote(pluginPath)}\nexec ${shellQuote(binary)} \"$@\"\n` : undefined;
let service: string | undefined;
let content = "";
if (runner && runnerPath) {
  const owner = lstatSync(runnerPath);
  const tokenOwner = lstatSync(runner.tokenFile);
  if (!owner.isFile() || !tokenOwner.isFile() || (owner.mode & 0o777) !== 0o600 || (tokenOwner.mode & 0o777) !== 0o600 ||
    owner.uid === 0 || owner.gid === 0 || owner.uid !== tokenOwner.uid || owner.gid !== tokenOwner.gid) throw new Error("Runner config/token must be mode-0600 regular files owned by the same nonroot UID/GID");
  const account = readFileSync("/etc/passwd", "utf8").split("\n").map((line) => line.split(":")).find((entry) => Number(entry[2]) === owner.uid);
  if (!account || Number(account[3]) !== owner.gid || !["/usr/sbin/nologin", "/sbin/nologin", "/bin/false", "/usr/bin/false"].includes(account[6]!)) throw new Error("Runner owner must be a dedicated non-login service account, not a native/plugin user");
  if (readFileSync("/etc/group", "utf8").split("\n").some((line) => line.split(":")[3]?.split(",").includes(account[0]!))) throw new Error("Dedicated service account must have no supplementary group memberships");
  if (!Number.isSafeInteger(runner.childUidBase) || runner.childUidBase < 1000000 || runner.childUidBase > 0xffffffff - 1000000 || owner.uid >= 1000000) throw new Error("Dedicated service UID and reserved child UID range required");
  const securePath = (path: string, privateDirectory = false): void => {
    if (!isAbsolute(path) || resolve(path) !== path || /[\r\n\u0000]/.test(path) || realpathSync(path) !== path) throw new Error("Canonical absolute runner paths required");
    const info = statSync(path);
    if (privateDirectory && (!info.isDirectory() || info.uid !== owner.uid || info.gid !== owner.gid || (info.mode & 0o077))) throw new Error("Private runner directories must belong to the service owner");
    for (let current = path; ; current = dirname(current)) {
      const entry = statSync(current);
      if ((entry.uid !== 0 && entry.uid !== owner.uid) || (entry.mode & 0o022)) throw new Error("Trusted installed release/runtime and runner paths must be root or service-owned, not writable by native accounts");
      if (current === "/") break;
    }
  };
  for (const path of [runnerPath, runner.tokenFile, root]) securePath(path);
  securePath(realpathSync(runtime));
  securePath(dirname(runtime));
  const visited = new Set<string>();
  const trustedRelease = (path: string): void => {
    const canonical = realpathSync(path);
    if (visited.has(canonical)) return;
    visited.add(canonical);
    securePath(canonical);
    if (statSync(canonical).isDirectory()) for (const entry of readdirSync(canonical)) trustedRelease(join(canonical, entry));
  };
  trustedRelease(root);
  for (const path of [binary, ...Object.values(runner.launchers)]) {
    if (!isAbsolute(path) || /[\r\n\u0000]/.test(path)) throw new Error("Absolute native executable paths required");
    accessSync(path, constants.X_OK);
  }
  for (const path of [dirname(runnerPath), dirname(runner.tokenFile), runner.stateDirectory, runner.modelDirectory, ...Object.values(runner.repositories)]) securePath(path, true);
  if (!isAbsolute(runner.childDirectory) || resolve(runner.childDirectory) !== runner.childDirectory || /[\r\n\u0000]/.test(runner.childDirectory)) throw new Error("Canonical absolute child directory required");
  securePath(dirname(runner.childDirectory));
  accessSync("/usr/bin/flock", constants.X_OK);
  // The service owner, not root, creates and validates the child root.
  const caps = "-all,+chown,+dac_override,+kill,+setuid,+setgid,+setpcap";
  try {
    // Native version probes run only as a fresh child UID with every capability cleared.
    execFileSync("/usr/bin/setpriv", [`--reuid=${runner.childUidBase}`, `--regid=${runner.childUidBase}`, "--clear-groups", "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs", binary, "--version"],
      { env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8", HOME: "/tmp" }, stdio: "pipe", timeout: 10000 });
    execFileSync("/usr/bin/setpriv", [`--reuid=${owner.uid}`, `--regid=${owner.gid}`, "--clear-groups", `--bounding-set=${caps}`, `--inh-caps=${caps}`, `--ambient-caps=${caps}`, "--no-new-privs",
      runtime, "--import", loader, "--input-type=module", "-e",
      `import{readFileSync}from'node:fs';import{DatabaseSync,backup}from'node:sqlite';import{requireIsolation}from${JSON.stringify(pathToFileURL(join(root, "src/runner.ts")).href)};const d=new DatabaseSync(':memory:');d.close();if(typeof backup!=='function')process.exit(1);requireIsolation(JSON.parse(readFileSync(process.argv[1],'utf8')))`,
      runnerPath], { cwd: root, env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8" }, stdio: "pipe", timeout: 30000 });
  } catch { throw new Error("Actual service UID/capability isolation validation failed; no system service installed"); }
  securePath(runner.childDirectory);
  if (statSync(runner.childDirectory).uid !== owner.uid || statSync(runner.childDirectory).gid !== owner.gid) throw new Error("Child directory must belong to the service owner");
  service = "/etc/systemd/system/coordinator-runner.service";
  const quote = (text: string) => `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/%/g, "%%")}"`;
  const capabilities = "CAP_CHOWN CAP_DAC_OVERRIDE CAP_KILL CAP_SETUID CAP_SETGID CAP_SETPCAP";
  content = `[Unit]\nDescription=Coordinator approved-work runner\nAfter=network-online.target\n[Service]\nType=simple\nUser=${owner.uid}\nGroup=${owner.gid}\nSupplementaryGroups=\nWorkingDirectory=${quote(root)}\nEnvironment=${quote(`COORDINATOR_CONFIG=${runnerPath}`)}\nExecStart=${program.map(quote).join(" ")}\nAmbientCapabilities=${capabilities}\nCapabilityBoundingSet=${capabilities}\nRestart=on-failure\nRestartSec=10\nUMask=0077\nNoNewPrivileges=true\n[Install]\nWantedBy=multi-user.target\n`;
  commands.push({ binary: "/usr/bin/systemctl", args: ["daemon-reload"] }, { binary: "/usr/bin/systemctl", args: ["enable", "--now", "coordinator-runner.service"] });
}
if (!dry) {
  if (service) {
    mkdirSync(dirname(service), { recursive: true, mode: 0o700 });
    writeFileSync(service, content, { mode: 0o600 });
  }
  if (launcher && wrapper) {
    mkdirSync(dirname(launcher), { recursive: true, mode: 0o700 });
    writeFileSync(launcher, wrapper, { mode: 0o700 });
  }
  for (const command of commands) {
    try { execFileSync(command.binary, command.args, { cwd: root, stdio: "pipe" }); }
    catch { throw new Error(`Installer operation failed: ${command.binary}; subprocess output suppressed to protect secrets`); }
  }
}
// Print only paths and operation plans, never config contents, tokens, or host subprocess output.
console.log(JSON.stringify({ dryRun: dry, mode: pluginOnly ? "plugin-only" : "runner-only", service, launcher, program, operations: commands, ...(pluginPath ? { pluginEnvironment: { COORDINATOR_CONFIG: pluginPath } } : {}), host, package: root }, null, 2));
