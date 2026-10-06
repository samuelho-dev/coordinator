import { chmodSync, chownSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { digest, type BrokerConfig, type RunnerConfig } from "../src/protocol.ts";

const repo = "github.com/samuelho-dev/coordinator";
const mode = process.argv[2];
const brokerUrl = process.env.COORDINATOR_BROKER_URL;
if (!brokerUrl?.startsWith("https://")) throw new Error("Private TLS broker URL required");
function credential(field: string): string {
  const value = process.env[field];
  if (!value || !/^[a-f0-9]{64}$/.test(value)) throw new Error(`Central enrollment field unavailable: ${field}`);
  return value;
}
function directory(path: string): void { mkdirSync(path, { recursive: true, mode: 0o700 }); chownSync(path, 1000, 1000); chmodSync(path, 0o700); }
function file(path: string, value: unknown): void { writeFileSync(path, typeof value === "string" ? value : JSON.stringify(value), { mode: 0o600 }); chownSync(path, 1000, 1000); chmodSync(path, 0o600); }
directory("/config");
if (mode === "broker") {
  directory("/data"); directory("/backups"); directory("/config/tokens");
  const runner = credential("HOMELAB_RUNNER_TOKEN");
  const control = credential("HOMELAB_RUNNER_CONTROL_TOKEN");
  const repositoryId = Number(process.env.COORDINATOR_REPOSITORY_ID);
  if (!Number.isSafeInteger(repositoryId) || repositoryId <= 0) throw new Error("Inspected enrollment GitHub repository ID required");
  const config: BrokerConfig = { database: "/data/ledger.sqlite", host: "0.0.0.0", port: 8787,
    backupDirectory: "/backups", archiveRetentionMs: 30 * 86400000, backupRetentionMs: 3 * 86400000,
    repositoryIds: { [repo]: repositoryId }, githubTokenEnv: "GITHUB_TOKEN", principals: [
      { id: "mac-session", owner: "samuelho-dev", roles: ["session", "coordinator"], repos: [repo], tokenHash: digest(credential("MAC_SESSION_TOKEN")) },
      { id: "homelab-session", owner: "samuelho-dev", roles: ["session", "coordinator"], repos: [repo], tokenHash: digest(credential("HOMELAB_SESSION_TOKEN")) },
      { id: "homelab-runner", owner: "samuelho-dev", roles: ["runner"], repos: [repo], tokenHash: digest(runner), controlTokenHash: digest(control), runnerUrl: process.env.COORDINATOR_RUNNER_URL },
      { id: "human-reviewer", owner: "samuelho-dev", roles: ["reviewer"], repos: [repo], tokenHash: digest(credential("REVIEWER_TOKEN")) },
      { id: "archive-writer", owner: "samuelho-dev", roles: ["archive-writer"], repos: [repo], tokenHash: digest(credential("ARCHIVE_WRITER_TOKEN")) },
    ] };
  if (!config.principals[2]?.runnerUrl?.startsWith("https://")) throw new Error("Private TLS runner URL required");
  const macUrl = process.env.COORDINATOR_MAC_RUNNER_URL;
  if (!macUrl?.startsWith("https://")) throw new Error("Private TLS Mac runner URL required");
  config.principals = [...config.principals, { id: "mac-runner", owner: "samuelho-dev", roles: ["runner"], repos: [repo], tokenHash: digest(credential("MAC_RUNNER_TOKEN")), controlTokenHash: digest(credential("MAC_RUNNER_CONTROL_TOKEN")), runnerUrl: macUrl }];
  file("/config/tokens/mac-runner", credential("MAC_RUNNER_CONTROL_TOKEN"));
  file("/config/tokens/homelab-runner", control); file("/config/broker.json", config);
  file("/config/health.header", `authorization: Bearer ${runner}`);
} else if (mode === "runner") {
  directory("/work"); directory("/work/repos"); directory("/work/runner"); directory("/work/home");
  const checkout = "/work/repos/coordinator";
  const sha = process.env.COORDINATOR_IMAGE_SHA;
  if (!sha || !/^[a-f0-9]{40}$/.test(sha)) throw new Error("Pinned full image source SHA required");
  const bundle = "/app/coordinator-source.bundle";
  if (execFileSync("git", ["bundle", "list-heads", bundle], { encoding: "utf8" }).trim() !== `${sha} HEAD`) throw new Error("Image source bundle does not match pinned SHA");
  const git = (args: string[], cwd?: string) => execFileSync("git", ["-c", `safe.directory=${checkout}`, ...args], { cwd, stdio: "ignore" });
  try {
    if (!existsSync(join(checkout, ".git"))) git(["clone", "--no-checkout", bundle, checkout]);
    git(["fetch", bundle, "HEAD"], checkout);
    git(["remote", "set-url", "origin", "https://github.com/samuelho-dev/coordinator.git"], checkout);
    git(["update-ref", "refs/heads/main", sha], checkout);
    git(["checkout", "--detach", sha], checkout);
    execFileSync("chown", ["-R", "1000:1000", checkout], { stdio: "ignore" });
  } catch { throw new Error("Private allowlisted repository initialization failed"); }
  directory("/work/home/.omp"); directory("/work/home/.omp/agent");
  const profile = "/work/home/.omp/agent/config.yml";
  if (!existsSync(profile)) file(profile, { modelRoles: { default: process.env.COORDINATOR_MODEL ?? "openai-codex/gpt-6.1-sol:xhigh" } });
  const runner = credential("HOMELAB_RUNNER_TOKEN");
  const controlTokenHash = credential("HOMELAB_RUNNER_CONTROL_HASH");
  file("/config/runner.token", runner); file("/config/health.header", `authorization: Bearer ${runner}`);
  const config: RunnerConfig = { id: "homelab-runner", brokerUrl, tokenFile: "/config/runner.token", host: "0.0.0.0", port: 8788,
    stateDirectory: "/work/runner", slots: 1, repositories: { [repo]: checkout }, launchers: { omp: "/usr/local/bin/omp" }, controlTokenHash };
  file("/config/runner.json", config);
} else throw new Error("Initializer mode must be broker or runner");
console.log(`coordinator ${mode} enrollment initialized`);
