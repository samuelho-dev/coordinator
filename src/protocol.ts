import { Schema } from "effect";
import { createHash, timingSafeEqual } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";

export const LIMIT = 64 * 1024;
export const Id = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_.:-]{1,160}$/));
export const Text = Schema.String.check(Schema.isMaxLength(16000));
export const Sha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
export const Repo = Schema.String.check(Schema.isPattern(/^[a-z0-9.-]+\/[a-z0-9_.-]+\/[a-z0-9_.-]+$/));
export const Address = Schema.Struct({
  messageId: Id, revision: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000000 })),
  workerId: Id, sessionId: Id, incarnation: Id, repo: Repo,
  ref: Text, sha: Sha, scope: Text, restrictions: Text,
  expectedEvidence: Schema.Array(Text).check(Schema.isMaxLength(32)), deadline: Schema.Number,
});
export type Address = typeof Address.Type;
export const Work = Schema.Struct({
  taskId: Id, repo: Repo, ref: Text, sha: Sha, launcher: Schema.Literals(["omp", "pi"]),
  prompt: Text, scope: Text, restrictions: Text,
  expectedEvidence: Schema.Array(Text).check(Schema.isMaxLength(32)), deadline: Schema.Number,
  workerId: Id, githubRepoId: Schema.Int, requiredLanes: Schema.Array(Text).check(Schema.isMaxLength(32)),
  bypassPeer: Schema.Boolean, approved: Schema.Literal(true),
});
export type Work = typeof Work.Type;
export type TaskState = "QUEUED" | "ASSIGNING" | "ACCEPTED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "HELD";
export interface Task {
  taskId: string; owner: string; fingerprint: string; request: Work; state: TaskState;
  runnerId?: string; peerSession?: string; peerIncarnation?: string;
  offerExpires?: number; offerDecision?: "pending" | "accepted" | "declined" | "expired" | "bypass";
  address?: Address; readback?: Address; confirmedRevision?: number;
  disposition?: "ready" | "unable" | "clarify" | "hold";
  reason?: string; nextCheck?: number; result?: unknown; createdAt: number; updatedAt: number;
  evidence?: unknown;
}
export interface Acceptance {
  taskId: string; accepted: boolean; state?: string; worktree?: string;
  address?: Address; reason?: string;
}
export interface Principal {
  id: string; owner: string; tokenHash: string; roles: string[]; repos: string[];
  runnerUrl?: string;
}
export interface BrokerConfig { database: string; host: string; port: number; principals: Principal[];
  backupDirectory: string; archiveRetentionMs: number; backupRetentionMs: number; githubTokenEnv?: string;
}
export interface RunnerConfig { id: string; brokerUrl: string; tokenFile: string; host: string; port: number;
  stateDirectory: string; slots: number; repositories: Record<string, string>; launchers: Record<string, string>;
}
export interface PluginConfig { brokerUrl: string; tokenFile: string; machineId: string;
  stateDirectory: string; selectedRemote?: string; host: "omp" | "pi";
}
export const RpcEnvelope = Schema.Struct({ method: Id, params: Schema.Unknown });
export function decode<S extends Schema.ConstraintDecoder<unknown>>(schema: S, value: unknown): S["Type"] {
  return Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(value);
}
export const digest = (value: string): string => createHash("sha256").update(value).digest("hex");
export function fingerprint(value: unknown): string {
  return digest(JSON.stringify(value, (_, item: unknown) => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
    return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
  }));
}
export function checkToken(token: string, hash: string): boolean {
  const actual = Buffer.from(digest(token)); const expected = Buffer.from(hash);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
export function privateFile(path: string): string {
  if ((statSync(path).mode & 0o077) !== 0) throw new Error(`Private file permissions required: ${path}`);
  return readFileSync(path, "utf8").trim();
}
export async function rpc<T = unknown>(url: string, token: string, method: string, params: unknown, timeout = 10000): Promise<T> {
  const body = JSON.stringify({ method, params });
  if (Buffer.byteLength(body) > LIMIT) throw new Error("Request too large");
  const endpoint = new URL("/rpc", url);
  if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) {
    throw new Error("TLS required except loopback");
  }
  const response = await fetch(endpoint, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body, signal: AbortSignal.timeout(timeout) });
  const bytes = await response.arrayBuffer();
  if (bytes.byteLength > LIMIT) throw new Error("Response too large");
  const result: unknown = JSON.parse(Buffer.from(bytes).toString("utf8"));
  if (!response.ok) throw new Error(`RPC ${response.status}: ${JSON.stringify(result)}`);
  return result as T;
}
export function canonicalRemote(remote: string): string {
  const scp = /^([^@\s]+@)?([^:\/\s]+):([^\s]+)$/.exec(remote);
  const url = scp ? new URL(`ssh://${scp[2]}/${scp[3]}`) : new URL(remote);
  if (!["https:", "ssh:", "git:"].includes(url.protocol) || url.search || url.hash) throw new Error("Unsupported remote");
  const parts = url.pathname.replace(/^\/+|\/+$/g, "").replace(/\.git$/, "").split("/");
  if (parts.length !== 2 || parts.some((part) => !part || part === "." || part === "..")) throw new Error("Noncanonical remote");
  return decode(Repo, `${url.hostname.toLowerCase()}/${parts.join("/").toLowerCase()}`);
}
export function inspectedRepo(cwd: string, selectedRemote?: string): string {
  const names = execFileSync("git", ["remote"], { cwd, encoding: "utf8" }).trim().split("\n").filter(Boolean);
  const name = selectedRemote ?? (names.length === 1 ? names[0] : undefined);
  if (!name || !names.includes(name)) throw new Error("Select one Git remote; ambiguous or absent remote");
  const urls = execFileSync("git", ["remote", "get-url", "--all", name], { cwd, encoding: "utf8" }).trim().split("\n");
  if (urls.length !== 1) throw new Error("Ambiguous selected remote URLs");
  return canonicalRemote(urls[0]!);
}
