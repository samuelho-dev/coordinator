import { Schema } from "effect";
import { decode, Id, Sha, Text, type Work } from "./protocol.ts";

const RepoIdentity = Schema.Struct({ id: Schema.Int, full_name: Text, private: Schema.Boolean });
const Job = Schema.Struct({ id: Schema.Int, name: Text, status: Text, conclusion: Schema.NullOr(Text), started_at: Schema.NullOr(Text), completed_at: Schema.NullOr(Text) });
const Run = Schema.Struct({ id: Schema.Int, workflow_id: Schema.Int, name: Schema.NullOr(Text), head_sha: Sha, head_branch: Schema.NullOr(Text), run_attempt: Schema.Int, status: Text, conclusion: Schema.NullOr(Text), html_url: Text, created_at: Text });
// GitHub adds fields; decode its selected fields rather than disabling strict product-boundary decoding.
function select(input: unknown, names: string[]): Record<string, unknown> {
  if (typeof input !== "object" || input === null || Array.isArray(input)) throw new Error("Malformed GitHub response");
  const record = input as Record<string, unknown>;
  return Object.fromEntries(names.map((name) => [name, record[name]]));
}
async function github(path: string, token?: string): Promise<unknown> {
  const response = await fetch(`https://api.github.com${path}`, {
    headers: { accept: "application/vnd.github+json", "x-github-api-version": "2022-11-28", ...(token ? { authorization: `Bearer ${token}` } : {}) },
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error(`GitHub unavailable (${response.status})`);
  return response.json();
}
function array(input: unknown, key: string): unknown[] {
  const value = select(input, [key])[key];
  if (!Array.isArray(value)) throw new Error("Malformed GitHub collection");
  return value;
}
export async function inspectGithubRepository(repo: string, token?: string): Promise<typeof RepoIdentity.Type> {
  const parts = repo.split("/");
  if (parts[0] !== "github.com" || parts.length !== 3) throw new Error("GitHub evidence requires a GitHub repository");
  const value = await github(`/repos/${parts[1]}/${parts[2]}`, token);
  return decode(RepoIdentity, select(value, ["id", "full_name", "private"]));
}
export interface CiEvidence {
  sha: string; repositoryId: number; ref: string; observedAt: string; provenance: string;
  verdict: "green" | "pending" | "failed" | "unknown"; reason?: string;
  lanes: { lane: string; runId?: number; attempt?: number; status?: string; conclusion?: string | null; jobs?: unknown[] }[];
}
export async function pollCi(work: Work, token?: string): Promise<CiEvidence> {
  const evidence: CiEvidence = { sha: work.sha, repositoryId: work.githubRepoId, ref: work.ref,
    observedAt: new Date().toISOString(), provenance: "GitHub Actions REST exact-SHA run-attempt/jobs", verdict: "unknown", lanes: [] };
  try {
    const identity = await inspectGithubRepository(work.repo, token);
    if (identity.id !== work.githubRepoId) throw new Error("Repository identity mismatch");
    const base = `/repos/${identity.full_name}`;
    const collection: typeof Run.Type[] = [];
    let truncated = false;
    for (let page = 1; page <= 10; page++) {
      const entries = array(await github(`${base}/actions/runs?head_sha=${work.sha}&per_page=100&page=${page}`, token), "workflow_runs");
      collection.push(...entries.map((entry) => decode(Run, select(entry, ["id", "workflow_id", "name", "head_sha", "head_branch", "run_attempt", "status", "conclusion", "html_url", "created_at"]))));
      if (entries.length < 100) break;
      if (page === 10) truncated = true;
    }
    if (truncated) throw new Error("Run history truncated; cannot prove latest relevant run");
    if (collection.some((run) => run.head_sha !== work.sha)) throw new Error("GitHub exact-SHA query mismatch");
    const relevant = collection.filter((run) => run.head_sha === work.sha && (run.head_branch === work.ref.replace(/^refs\/heads\//, "") || run.head_branch === work.ref));
    const latest = new Map<number, typeof Run.Type>();
    for (const run of relevant) {
      const prior = latest.get(run.workflow_id);
      if (!prior || run.id > prior.id || (run.id === prior.id && run.run_attempt > prior.run_attempt)) latest.set(run.workflow_id, run);
    }
    if (work.requiredLanes.length === 0) throw new Error("No explicit required CI lanes");
    const seen: { lane: string; run: typeof Run.Type; jobs: typeof Job.Type[]; green: boolean }[] = [];
    for (const run of latest.values()) {
      const attemptResponse = await github(`${base}/actions/runs/${run.id}/attempts/${run.run_attempt}`, token);
      const attempt = decode(Run, select(attemptResponse, ["id", "workflow_id", "name", "head_sha", "head_branch", "run_attempt", "status", "conclusion", "html_url", "created_at"]));
      if (attempt.head_sha !== work.sha || attempt.run_attempt !== run.run_attempt) throw new Error("Attempt SHA/number mismatch");
      const jobs: typeof Job.Type[] = [];
      for (let page = 1; page <= 10; page++) {
        const entries = array(await github(`${base}/actions/runs/${run.id}/attempts/${run.run_attempt}/jobs?per_page=100&page=${page}`, token), "jobs");
        jobs.push(...entries.map((entry) => decode(Job, select(entry, ["id", "name", "status", "conclusion", "started_at", "completed_at"]))));
        if (entries.length < 100) break;
        if (page === 10) throw new Error("Jobs truncated");
      }
      for (const job of jobs) seen.push({ lane: `${run.name}/${job.name}`, run: attempt, jobs: [job], green: attempt.status === "completed" && attempt.conclusion === "success" && job.status === "completed" && job.conclusion === "success" });
    }
    let verdict: CiEvidence["verdict"] = "green";
    for (const lane of work.requiredLanes) {
      const matches = seen.filter((entry) => entry.lane === lane);
      if (matches.length !== 1) { evidence.lanes.push({ lane }); verdict = "unknown"; continue; }
      const entry = matches[0]!;
      evidence.lanes.push({ lane, runId: entry.run.id, attempt: entry.run.run_attempt, status: entry.run.status, conclusion: entry.run.conclusion, jobs: entry.jobs });
      if (!entry.green && verdict !== "unknown") verdict = entry.run.status !== "completed" ? "pending" : "failed";
    }
    // Re-read each run after jobs: a rerun queued during polling invalidates old success.
    for (const run of latest.values()) {
      const current = select(await github(`${base}/actions/runs/${run.id}`, token), ["run_attempt", "status", "head_sha"]);
      if (current.run_attempt !== run.run_attempt || current.head_sha !== work.sha || current.status !== run.status) verdict = "pending";
    }
    evidence.verdict = verdict;
    return evidence;
  } catch (error) {
    evidence.verdict = "unknown";
    evidence.reason = error instanceof Error ? error.message : "GitHub unavailable";
    return evidence;
  }
}
export const LocalEvidence = Schema.Struct({ taskId: Id, sha: Sha, commands: Schema.Array(Text), passed: Schema.Boolean });
export const DeploymentEvidence = Schema.Struct({ taskId: Id, sha: Sha, environment: Text, url: Text, observedState: Text, provenance: Text });
export const ReviewEvidence = Schema.Struct({ taskId: Id, sha: Sha, decision: Schema.Literals(["approved", "changes-requested"]), note: Text });
