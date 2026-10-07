import { Clock, Effect } from "effect";
import { HttpClient } from "effect/http";
import { HttpError } from "./contracts.ts";
import { RepoIdentity, Job, Run, MergeRef, JsonValue, type Json, type Work, type CiEvidence, type CiSeen } from "./contracts.ts";
import { decode, decodeJson } from "./http.ts";
// GitHub adds fields; decode its selected fields rather than disabling strict product-boundary decoding.
function select(input: JsonValue | undefined, names: string[]): Json {
  if (typeof input !== "object" || input === null || Array.isArray(input)) throw new Error("Malformed GitHub response");
  return Object.fromEntries(names.filter((name) => Object.hasOwn(input, name)).map((name) => [name, input[name]!]));
}
function decodeRun(input: JsonValue): Run {
  const fields = select(input, ["id", "workflow_id", "name", "head_sha", "head_branch", "pull_requests", "run_attempt", "status", "conclusion", "html_url", "created_at"]);
  if (fields.pull_requests !== undefined) fields.pull_requests = array(input, "pull_requests").map((pr) => select(pr, ["number"]));
  return decode(Run, fields);
}
const github = Effect.fn("GitHub.request")(function*(path: string, token?: string) {
  return yield* Effect.scoped(Effect.gen(function*() {
    const client = HttpClient.withScope(yield* HttpClient.HttpClient);
    const response = yield* client.get(`https://api.github.com${path}`, {
      headers: { accept: "application/vnd.github+json", "x-github-api-version": "2022-11-28", ...(token ? { authorization: `Bearer ${token}` } : {}) },
    });
    if (response.status < 200 || response.status >= 300) return yield* new HttpError({ status: 502, message: `GitHub unavailable (${response.status})` });
    const text = yield* response.text;
    return yield* Effect.try(() => decodeJson(JsonValue, text));
  })).pipe(Effect.timeout("15 seconds"));
});
function array(input: JsonValue, key: string): JsonValue[] {
  const value = select(input, [key])[key];
  if (!Array.isArray(value)) throw new Error("Malformed GitHub collection");
  return value;
}
export const inspectGithubRepository = Effect.fn("GitHub.inspectRepository")(function*(repo: string, token?: string) {
  const parts = repo.split("/");
  if (parts[0] !== "github.com" || parts.length !== 3) return yield* new HttpError({ status: 502, message: "GitHub evidence requires a GitHub repository" });
  const value = yield* github(`/repos/${parts[1]}/${parts[2]}`, token);
  return (yield* Effect.try(() => decode(RepoIdentity, select(value, ["id", "full_name", "private"]))));
});
export const pollCi = Effect.fn("GitHub.pollCi")(function*(work: Work, token?: string) {
  const evidence: CiEvidence = { sha: work.sha, repositoryId: work.githubRepoId, ref: work.ref,
    observedAt: new Date(yield* Clock.currentTimeMillis).toISOString(), provenance: "GitHub Actions REST exact-SHA run-attempt/jobs", verdict: "unknown", lanes: [] };
  return yield* Effect.gen(function*() {
    const pull = /^refs\/pull\/([1-9]\d*)\/merge$/.exec(work.ref);
    if (pull && !token) return yield* new HttpError({ status: 502, message: "PR merge-ref evidence requires authenticated GitHub metadata" });
    const identity = yield* inspectGithubRepository(work.repo, token);
    if (identity.id !== work.githubRepoId) return yield* new HttpError({ status: 502, message: "Repository identity mismatch" });
    const base = `/repos/${identity.full_name}`;
    const mergeRefPath = pull ? `${base}/git/ref/pull/${pull[1]}/merge` : undefined;
    const checkMergeRef = Effect.fnUntraced(function*() {
      if (!mergeRefPath) return;
      const value = yield* github(mergeRefPath, token);
      const fields = (yield* Effect.try(() => select(value, ["ref", "object"])));
      fields.object = (yield* Effect.try(() => select(fields.object, ["type", "sha"])));
      const current = (yield* Effect.try(() => decode(MergeRef, fields)));
      if (current.ref !== work.ref || current.object.sha !== work.sha) return yield* new HttpError({ status: 502, message: "Current PR merge-ref SHA mismatch" });
    });
    yield* checkMergeRef();
    const matchesRef = (run: Run): boolean => pull
      ? run.pull_requests?.some((pr) => String(pr.number) === pull[1]) === true
      : run.head_branch === work.ref.replace(/^refs\/heads\//, "") || run.head_branch === work.ref;
    const collection: Run[] = [];
    let truncated = false;
    for (let page = 1; page <= 10; page++) {
      const response = yield* github(`${base}/actions/runs?head_sha=${work.sha}&per_page=100&page=${page}`, token);
      const entries = yield* Effect.try(() => array(response, "workflow_runs"));
      collection.push(...(yield* Effect.try(() => entries.map(decodeRun))));
      if (entries.length < 100) break;
      if (page === 10) truncated = true;
    }
    if (truncated) return yield* new HttpError({ status: 502, message: "Run history truncated; cannot prove latest relevant run" });
    if (collection.some((run) => run.head_sha !== work.sha)) return yield* new HttpError({ status: 502, message: "GitHub exact-SHA query mismatch" });
    const relevant = collection.filter((run) => run.head_sha === work.sha && matchesRef(run));
    const latest = new Map<number, Run>();
    for (const run of relevant) {
      const prior = latest.get(run.workflow_id);
      if (!prior || run.id > prior.id || (run.id === prior.id && run.run_attempt > prior.run_attempt)) latest.set(run.workflow_id, run);
    }
    if (work.requiredLanes.length === 0) return yield* new HttpError({ status: 502, message: "No explicit required CI lanes" });
    const seen: CiSeen[] = [];
    for (const run of latest.values()) {
      const attemptResponse = yield* github(`${base}/actions/runs/${run.id}/attempts/${run.run_attempt}`, token);
      const attempt = (yield* Effect.try(() => decodeRun(attemptResponse)));
      if (attempt.id !== run.id || attempt.workflow_id !== run.workflow_id || attempt.head_sha !== work.sha || attempt.run_attempt !== run.run_attempt || !matchesRef(attempt)) return yield* new HttpError({ status: 502, message: "Attempt identity/SHA/ref/number mismatch" });
      const jobs: Job[] = [];
      for (let page = 1; page <= 10; page++) {
        const response = yield* github(`${base}/actions/runs/${run.id}/attempts/${run.run_attempt}/jobs?per_page=100&page=${page}`, token);
        const entries = yield* Effect.try(() => array(response, "jobs"));
        jobs.push(...(yield* Effect.try(() => entries.map((entry) => decode(Job, select(entry, ["id", "name", "status", "conclusion", "started_at", "completed_at"]))))));
        if (entries.length < 100) break;
        if (page === 10) return yield* new HttpError({ status: 502, message: "Jobs truncated" });
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
      const currentResponse = yield* github(`${base}/actions/runs/${run.id}`, token);
      const current = (yield* Effect.try(() => select(currentResponse, ["run_attempt", "status", "head_sha"])));
      if (current.run_attempt !== run.run_attempt || current.head_sha !== work.sha || current.status !== run.status || (pull && !matchesRef(yield* Effect.try(() => decodeRun(currentResponse))))) verdict = "pending";
    }
    // PR synchronization during polling invalidates the previously tested merge commit.
    yield* checkMergeRef();
    evidence.verdict = verdict;
    return evidence;
  }).pipe(Effect.catch((error) => Effect.sync(() => {
    evidence.verdict = "unknown";
    evidence.reason = error instanceof Error ? error.message : "GitHub unavailable";
    return evidence;
  })));
});
