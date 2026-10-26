import { Clock, Effect, Fiber, FileSystem, Path, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { SessionGit, type SessionProject, type SessionWorkspace } from "./inspection.ts";

const command = (cwd: string, args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(ChildProcess.make("git", args, {
    cwd,
    // Do not inherit GIT_DIR, GIT_WORK_TREE, injected config, or credential helpers.
    env: { PATH: "/usr/bin:/bin", LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_OPTIONAL_LOCKS: "0", GIT_TERMINAL_PROMPT: "0" },
    extendEnv: false, stdin: "ignore", stderr: "ignore", forceKillAfter: "100 millis",
  }));
  let bytes = 0;
  const text = yield* child.stdout.pipe(
    Stream.mapEffect((chunk) => {
      bytes += chunk.length;
      return bytes > 16 * 1024 ? Effect.fail("Git output limit") : Effect.succeed(chunk);
    }),
    Stream.decodeText(),
    Stream.runFold(() => "", (all, chunk) => all + chunk),
  );
  return { code: yield* child.exitCode, text };
}));

// Exit 128 alone is not proof of a non-repository: broken/denied .git metadata
// must remain unavailable. Check only ancestor markers, never tree contents.
const outsideRepository = (cwd: string) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  let directory = yield* fs.realPath(cwd);
  const initial = yield* fs.stat(directory);
  if (initial.type !== "Directory") return false;
  const markerType = (name: string) => fs.stat(path.join(directory, name)).pipe(
    Effect.map((info) => info.type),
    Effect.catch((error) => error.reason._tag === "NotFound" ? Effect.succeed(undefined) : Effect.fail(error)),
  );
  while (true) {
    if (yield* fs.exists(path.join(directory, ".git"))) return false;
    const linkedMarker = yield* fs.readLink(path.join(directory, ".git")).pipe(
      Effect.map(() => true),
      Effect.catch((error) => error.reason._tag === "NotFound" ? Effect.succeed(false) : Effect.fail(error)),
    );
    if (linkedMarker) return false;
    // Bare repositories need a HEAD file and object/reference directories;
    // familiar filenames alone are not repository evidence.
    if ((yield* markerType("HEAD")) === "File" &&
      (yield* markerType("objects")) === "Directory" &&
      (yield* markerType("refs")) === "Directory") return false;
    const parent = path.dirname(directory);
    if (parent === directory) return true;
    // Git's sanitized environment uses its default same-device discovery.
    if ((yield* fs.stat(parent)).dev !== initial.dev) return true;
    directory = parent;
  }
});

const discoverGit = (cwd: string, observedAt: number) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const current = yield* fs.realPath(cwd);
  if ((yield* fs.stat(current)).type !== "Directory") return yield* Effect.fail("Invalid workspace directory");
  const root = yield* command(cwd, ["rev-parse", "--show-toplevel"]);
  if (root.code !== 0) {
    if (root.code === 128 && (yield* outsideRepository(cwd))) return { state: "not-repository", observedAt } satisfies SessionGit;
    return yield* Effect.fail("Git discovery failed");
  }
  // Strip only Git's terminating LF: whitespace/newlines can belong to a path.
  if (!root.text.endsWith("\n")) return yield* Effect.fail("Invalid Git root");
  const worktreeRoot = yield* fs.realPath(root.text.slice(0, -1));
  const common = yield* command(cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"]);
  if (common.code !== 0 || !common.text.endsWith("\n")) return yield* Effect.fail("Invalid Git common directory");
  const commonPath = common.text.slice(0, -1);
  if (!path.isAbsolute(commonPath)) return yield* Effect.fail("Relative Git common directory");
  const commonDir = yield* fs.realPath(commonPath);
  const symbolic = yield* command(cwd, ["symbolic-ref", "--quiet", "HEAD"]);
  if (symbolic.code !== 0 && symbolic.code !== 1) return yield* Effect.fail("Invalid Git branch");
  if (symbolic.code === 0 && (!symbolic.text.startsWith("refs/heads/") || !symbolic.text.endsWith("\n"))) return yield* Effect.fail("Invalid Git branch");
  const branch = symbolic.code === 1 ? null : symbolic.text.slice("refs/heads/".length, -1);
  const commit = yield* command(cwd, ["rev-parse", "--verify", "--quiet", "HEAD"]);
  if (commit.code !== 0 && !(commit.code === 1 && branch !== null && commit.text === "")) return yield* Effect.fail("Invalid Git HEAD");
  const head = commit.code === 1 ? null : commit.text.endsWith("\n") ? commit.text.slice(0, -1) : "";
  return yield* Schema.decodeUnknownEffect(SessionGit)({ state: "available", observedAt, worktreeRoot, commonDir, branch, head });
}).pipe(
  Effect.forkScoped,
  Effect.flatMap((fiber) => Fiber.join(fiber).pipe(
    Effect.timeout("1900 millis"),
    Effect.ensuring(Fiber.interrupt(fiber).pipe(Effect.forkScoped, Effect.asVoid)),
  )),
  Effect.catch(() => Effect.succeed<SessionGit>({ state: "unavailable", observedAt })),
);

const associateProject = (cwd: string, project: SessionProject) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.realPath(project.root);
  const current = yield* fs.realPath(cwd);
  if ((yield* fs.stat(root)).type !== "Directory" || (yield* fs.stat(current)).type !== "Directory") return undefined;
  const relative = path.relative(root, current);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
    ? { ...project, root }
    : undefined;
}).pipe(
  Effect.forkScoped,
  Effect.flatMap((fiber) => Fiber.join(fiber).pipe(
    Effect.timeout("1900 millis"),
    Effect.ensuring(Fiber.interrupt(fiber).pipe(Effect.forkScoped, Effect.asVoid)),
  )),
  Effect.catch(() => Effect.succeed(undefined)),
);

/** Local, bounded observation; the caller's scope owns interrupted discovery cleanup. */
export const readWorkspace = (cwd: string, previous?: SessionWorkspace, project?: SessionProject) => Effect.gen(function*() {
  const now = yield* Clock.currentTimeMillis;
  const cached = previous?.cwd === cwd ? previous.git : undefined;
  const age = cached === undefined ? -1 : now - cached.observedAt;
  // Concurrent observation deadlines retain Git when only project lookup stalls.
  // Cleanup may outlive this read, but never the caller's scope (including reaping).
  const [git, association] = yield* Effect.all([
    cached !== undefined && age >= 0 && age < 30_000 ? Effect.succeed(cached) : discoverGit(cwd, now),
    project === undefined ? Effect.succeed(undefined) : associateProject(cwd, project),
  ], { concurrency: "unbounded" });
  return { cwd, git, ...(association === undefined ? {} : { project: association }) } satisfies SessionWorkspace;
});
