import { NodeServices } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import { Clock, Deferred, Effect, Fiber, FileSystem, Path, PlatformError, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { TestClock } from "effect/testing";
import { readWorkspace } from "../src/workspace.ts";
import type { SessionGit, SessionWorkspace } from "../src/inspection.ts";

const git = (cwd: string, args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
  const child = yield* ChildProcess.make("git", args, {
    cwd, env: { PATH: "/usr/bin:/bin", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
    extendEnv: false, stdin: "ignore", stderr: "ignore",
  });
  const text = yield* child.stdout.pipe(Stream.decodeText(), Stream.runFold(() => "", (all, chunk) => all + chunk));
  assert.strictEqual(yield* child.exitCode, 0);
  return text.slice(0, -1);
}));

const repository = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-workspace-" });
  const root = path.join(temporary, "repo with spaces\nand newline");
  yield* fs.makeDirectory(root);
  yield* git(root, ["init", "--initial-branch=main"]);
  return { fs, path, root: yield* fs.realPath(root), temporary };
});

it.live("discovers subdirectories, linked worktrees and detached HEAD without inventing a project", () => Effect.scoped(Effect.gen(function*() {
  const { fs, path, root, temporary } = yield* repository;
  const unborn = yield* readWorkspace(root);
  assert.deepStrictEqual(unborn.git, { state: "available", observedAt: unborn.git?.observedAt, worktreeRoot: root, commonDir: path.join(root, ".git"), branch: "main", head: null });
  assert.strictEqual(unborn.project, undefined);
  yield* git(root, ["-c", "user.name=Workspace Test", "-c", "user.email=workspace@example.invalid", "commit", "--allow-empty", "-m", "initial"]);
  const head = yield* git(root, ["rev-parse", "HEAD"]);
  const subdirectory = path.join(root, "nested\n space");
  yield* fs.makeDirectory(subdirectory);
  const sub = yield* readWorkspace(subdirectory);
  assert.strictEqual(sub.cwd, subdirectory);
  assert.deepStrictEqual(sub.git, { state: "available", observedAt: sub.git?.observedAt, worktreeRoot: root, commonDir: path.join(root, ".git"), branch: "main", head });
  const linked = path.join(temporary, "linked\n worktree");
  yield* git(root, ["worktree", "add", "-b", "linked", linked]);
  const worktree = yield* readWorkspace(linked);
  assert.deepStrictEqual(worktree.git, { state: "available", observedAt: worktree.git?.observedAt, worktreeRoot: linked, commonDir: path.join(root, ".git"), branch: "linked", head });
  yield* git(linked, ["checkout", "--detach"]);
  const detached = yield* readWorkspace(linked);
  assert.strictEqual(detached.git?.state, "available");
  if (detached.git?.state !== "available") assert.fail("detached worktree missing");
  assert.strictEqual(detached.git.branch, null);
  assert.strictEqual(detached.git.head, head);
})).pipe(Effect.provide(NodeServices.layer)));

it.live("distinguishes non-repositories from missing, invalid, bare and broken repositories", () => Effect.scoped(Effect.gen(function*() {
  const { fs, path, root, temporary } = yield* repository;
  assert.strictEqual((yield* readWorkspace(temporary)).git?.state, "not-repository");
  const ordinary = path.join(temporary, "ordinary");
  yield* fs.makeDirectory(ordinary);
  yield* fs.writeFileString(path.join(ordinary, "HEAD"), "not a Git HEAD");
  yield* fs.writeFileString(path.join(ordinary, "objects"), "ordinary file");
  assert.strictEqual((yield* readWorkspace(ordinary)).git?.state, "not-repository");
  yield* fs.remove(path.join(ordinary, "objects"));
  yield* fs.makeDirectory(path.join(ordinary, "objects"));
  assert.strictEqual((yield* readWorkspace(ordinary)).git?.state, "not-repository");
  const denied = PlatformError.systemError({ _tag: "PermissionDenied", module: "FileSystem", method: "readLink", description: "private diagnostic" });
  const inaccessible = yield* readWorkspace(ordinary).pipe(Effect.provideService(FileSystem.FileSystem, {
    ...fs,
    readLink: (file) => file === path.join(ordinary, ".git") ? Effect.fail(denied) : fs.readLink(file),
  }));
  assert.deepStrictEqual(inaccessible.git, { state: "unavailable", observedAt: inaccessible.git?.observedAt });
  assert.strictEqual((yield* readWorkspace(path.join(temporary, "missing"))).git?.state, "unavailable");
  const file = path.join(temporary, "file");
  yield* fs.writeFileString(file, "not a directory");
  assert.strictEqual((yield* readWorkspace(file)).git?.state, "unavailable");
  const bare = path.join(temporary, "bare");
  yield* git(temporary, ["init", "--bare", bare]);
  assert.strictEqual((yield* readWorkspace(bare)).git?.state, "unavailable");
  const broken = path.join(temporary, "broken");
  yield* fs.makeDirectory(broken);
  yield* fs.symlink(path.join(temporary, "absent-git-dir"), path.join(broken, ".git"));
  assert.strictEqual((yield* readWorkspace(broken)).git?.state, "unavailable");
  yield* fs.writeFileString(path.join(root, ".git", "config"), "[invalid config\n");
  assert.strictEqual((yield* readWorkspace(root)).git?.state, "unavailable");
})).pipe(Effect.provide(NodeServices.layer)));

it.live("stops negative discovery at the filesystem boundary below an enclosing checkout", () => Effect.scoped(Effect.gen(function*() {
  const { fs, path, root } = yield* repository;
  const mounted = path.join(root, "mounted");
  const nested = path.join(mounted, "nested");
  yield* fs.makeDirectory(nested, { recursive: true });
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  // Simulate Git's boundary result without requiring a privileged mount.
  const negative = { ...spawner, spawn: (command: ChildProcess.Command) => spawner.spawn(command).pipe(
    Effect.map((child) => ({
      ...child,
      stdout: Stream.empty,
      exitCode: child.exitCode.pipe(Effect.as(ChildProcessSpawner.ExitCode(128))),
    })),
  ) };
  assert.strictEqual((yield* readWorkspace(nested).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, negative))).git?.state, "unavailable");
  let enclosingMarkers = 0;
  const workspace = yield* readWorkspace(nested).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, negative),
    Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      stat: (file) => fs.stat(file).pipe(Effect.map((info) => ({
        ...info, dev: file === mounted || file.startsWith(`${mounted}${path.sep}`) ? info.dev + 1 : info.dev,
      }))),
      exists: (file) => {
        if (file === path.join(root, ".git")) enclosingMarkers += 1;
        return fs.exists(file);
      },
    }),
  );
  assert.deepStrictEqual(workspace.git, { state: "not-repository", observedAt: workspace.git?.observedAt });
  assert.strictEqual(enclosingMarkers, 0);
})).pipe(Effect.provide(NodeServices.layer)));

it.live("preserves SHA-256 HEAD identities", () => Effect.scoped(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "coordinator-workspace-sha256-" });
  yield* git(root, ["init", "--initial-branch=main", "--object-format=sha256"]);
  yield* git(root, ["-c", "user.name=Workspace Test", "-c", "user.email=workspace@example.invalid", "commit", "--allow-empty", "-m", "initial"]);
  const head = yield* git(root, ["rev-parse", "HEAD"]);
  assert.strictEqual(head.length, 64);
  const workspace = yield* readWorkspace(root);
  if (workspace.git?.state !== "available") assert.fail("SHA-256 workspace missing");
  assert.strictEqual(workspace.git.head, head);
})).pipe(Effect.provide(NodeServices.layer)));

it.live("reuses every Git outcome for thirty seconds, refreshing expired, future and changed-cwd observations", () => Effect.scoped(Effect.gen(function*() {
  const { root, temporary } = yield* repository;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  let calls = 0;
  const counting = { ...spawner, spawn: (command: ChildProcess.Command) => Effect.suspend(() => {
    calls += 1;
    return spawner.spawn(command);
  }) };
  const read = (cwd: string, previous?: SessionWorkspace) => readWorkspace(cwd, previous).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, counting));
  const initial = yield* read(root);
  assert.strictEqual(calls, 4);
  assert.deepStrictEqual(yield* read(root, initial), initial);
  assert.strictEqual(calls, 4);
  const now = yield* Clock.currentTimeMillis;
  for (const state of ["not-repository", "unavailable"] as const) {
    const previous = { cwd: root, git: { state, observedAt: now } };
    assert.deepStrictEqual(yield* read(root, previous), previous);
  }
  assert.strictEqual(calls, 4);
  if (initial.git === undefined) assert.fail("Git observation missing");
  for (const observedAt of [now - 30_001, now + 60_000]) {
    yield* read(root, { ...initial, git: { ...initial.git, observedAt } });
  }
  assert.strictEqual(calls, 12);
  const changed = yield* read(temporary, initial);
  assert.strictEqual(changed.git?.state, "not-repository");
  assert.strictEqual(calls, 13);
})).pipe(Effect.provide(NodeServices.layer)));

it.live("associates only explicit canonical project boundaries, independently of cached Git", () => Effect.scoped(Effect.gen(function*() {
  const { fs, path, root, temporary } = yield* repository;
  const nested = path.join(root, "nested");
  yield* fs.makeDirectory(nested);
  const alias = path.join(temporary, "alias");
  yield* fs.symlink(root, alias);
  const outside = path.join(temporary, "outside");
  yield* fs.makeDirectory(outside);
  const escape = path.join(root, "escape");
  yield* fs.symlink(outside, escape);
  const project = { root: alias, source: "configured", id: "explicit-id", name: "Explicit project" } as const;
  const initial = yield* readWorkspace(nested);
  const associated = yield* readWorkspace(nested, initial, project);
  assert.deepStrictEqual(associated.project, { ...project, root });
  assert.strictEqual(associated.git, initial.git);
  assert.strictEqual((yield* readWorkspace(nested, associated)).project, undefined);
  assert.strictEqual((yield* readWorkspace(escape, associated, project)).project, undefined);
  const sibling = `${root}-sibling`;
  yield* fs.makeDirectory(sibling);
  assert.strictEqual((yield* readWorkspace(sibling, associated, project)).project, undefined);
  assert.strictEqual((yield* readWorkspace(nested, initial, { root, source: "native" })).project?.id, undefined);
  const throughAlias = path.join(alias, "nested");
  const linked = yield* readWorkspace(throughAlias, undefined, { root, source: "native" });
  assert.strictEqual(linked.cwd, throughAlias);
  assert.deepStrictEqual(linked.project, { root, source: "native" });
  assert.strictEqual((yield* readWorkspace(nested, initial, { root: path.join(temporary, "missing"), source: "native" })).project, undefined);
})).pipe(Effect.provide(NodeServices.layer)));

it.live("contains missing Git, timeout and excessive output while stripping inherited Git redirection", () => Effect.scoped(Effect.gen(function*() {
  const { root } = yield* repository;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const missing = PlatformError.systemError({ _tag: "NotFound", module: "ChildProcess", method: "spawn", description: "private diagnostic" });
  const failures = [Effect.fail(missing), Effect.never];
  for (const failure of failures) {
    const result = yield* readWorkspace(root).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
      ...spawner,
      spawn: (command) => {
        if (command._tag !== "StandardCommand") assert.fail("expected shell-free command");
        assert.strictEqual(command.command, "git");
        assert.strictEqual(command.options.cwd, root);
        assert.strictEqual(command.options.extendEnv, false);
        assert.strictEqual(command.options.stderr, "ignore");
        assert.strictEqual(command.options.env?.GIT_DIR, undefined);
        assert.strictEqual(command.options.env?.GIT_WORK_TREE, undefined);
        return failure;
      },
    }));
    assert.deepStrictEqual(result.git, { state: "unavailable", observedAt: result.git?.observedAt });
  }
  const result = yield* readWorkspace(root).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
    ...spawner,
    spawn: (command) => spawner.spawn(command).pipe(Effect.map((child) => ({
      ...child, stdout: Stream.make(new Uint8Array(16 * 1024 + 1)),
    }))),
  }));
  assert.strictEqual(result.git?.state, "unavailable");
  const cached: SessionGit = { state: "unavailable", observedAt: yield* Clock.currentTimeMillis };
  assert.strictEqual((yield* readWorkspace(root, { cwd: root, git: cached })).git, cached);
})).pipe(Effect.provide(NodeServices.layer)));

it.effect("bounds stalled project resolution while preserving cached or discovered Git and omitting the unverified project", () => Effect.scoped(Effect.gen(function*() {
  const { fs, path, root } = yield* repository;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const cached: SessionWorkspace = {
    cwd: root,
    git: { state: "available", observedAt: yield* Clock.currentTimeMillis, worktreeRoot: root, commonDir: path.join(root, ".git"), branch: "main", head: null },
  };
  for (const previous of [cached, undefined]) {
    const entered = yield* Deferred.make<void>();
    const gitReady = yield* Deferred.make<void>();
    if (previous !== undefined) yield* Deferred.succeed(gitReady, undefined);
    const stalled = { ...fs, realPath: (value: string) => value === "/project"
      ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
      : fs.realPath(value) };
    const pending = yield* readWorkspace(root, previous, { root: "/project", source: "configured" }).pipe(
      Effect.provideService(FileSystem.FileSystem, stalled),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
        ...spawner,
        spawn: (command) => spawner.spawn(command).pipe(Effect.map((child) => ({
          ...child,
          exitCode: child.exitCode.pipe(Effect.tap(() => command._tag === "StandardCommand" && command.args.includes("--verify")
            ? Deferred.succeed(gitReady, undefined)
            : Effect.void)),
        }))),
      }),
      Effect.forkChild,
    );
    yield* Deferred.await(entered);
    yield* Deferred.await(gitReady);
    yield* TestClock.adjust("2 seconds");
    const result = yield* Fiber.join(pending);
    assert.strictEqual(result.git?.state, "available");
    if (previous !== undefined) assert.strictEqual(result.git, previous.git);
    assert.strictEqual(result.cwd, root);
    assert.strictEqual(result.project, undefined);
  }
})).pipe(Effect.provide(NodeServices.layer)));

it.effect("returns within two seconds after acquiring a child while caller cancellation still waits for cleanup", () => Effect.scoped(Effect.gen(function*() {
  const { root } = yield* repository;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const acquired = yield* Deferred.make<void>();
  const cleanupStarted = yield* Deferred.make<void>();
  const releaseCleanup = yield* Deferred.make<void>();
  const ownerClosing = yield* Deferred.make<void>();
  const ownerClosed = yield* Deferred.make<void>();
  const observed = yield* Deferred.make<SessionWorkspace>();
  const startedAt = yield* Clock.currentTimeMillis;
  const owner = yield* Effect.scoped(Effect.gen(function*() {
    const workspace = yield* readWorkspace(root).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, {
      ...spawner,
      spawn: (command) => Effect.gen(function*() {
        if (command._tag !== "StandardCommand") assert.fail("expected shell-free command");
        // Acquire a real blocked child, not an unacquired Effect.never spawn.
        const child = yield* spawner.spawn(ChildProcess.make("sleep", ["60"], command.options));
        yield* Effect.addFinalizer(() => Effect.gen(function*() {
          yield* Deferred.succeed(cleanupStarted, undefined);
          yield* Deferred.await(releaseCleanup);
        }));
        yield* Deferred.succeed(acquired, undefined);
        return { ...child, stdout: Stream.fromEffect(Effect.never) };
      }),
    }));
    yield* Deferred.succeed(observed, workspace);
    yield* Effect.addFinalizer(() => Deferred.succeed(ownerClosing, undefined));
    return yield* Effect.never;
  })).pipe(
    Effect.onExit(() => Deferred.succeed(ownerClosed, undefined)),
    Effect.forkChild,
  );
  yield* Effect.gen(function*() {
    yield* Deferred.await(acquired);
    yield* TestClock.adjust("1900 millis");
    yield* Deferred.await(cleanupStarted);
    yield* TestClock.adjust("100 millis");
    assert.isTrue(yield* Deferred.isDone(observed), "observation must not await delayed child finalization");
    const workspace = yield* Deferred.await(observed);
    assert.deepStrictEqual(workspace.git, { state: "unavailable", observedAt: startedAt });
    assert.isTrue((yield* Clock.currentTimeMillis) - startedAt <= 2000);
    const cancellation = yield* Fiber.interrupt(owner).pipe(Effect.forkChild);
    yield* Deferred.await(ownerClosing);
    assert.isFalse(yield* Deferred.isDone(ownerClosed), "caller scope must retain ownership until child cleanup/reaping");
    yield* Deferred.succeed(releaseCleanup, undefined);
    yield* Fiber.join(cancellation);
    assert.isTrue(yield* Deferred.isDone(ownerClosed));
  }).pipe(Effect.ensuring(Deferred.succeed(releaseCleanup, undefined)));
})).pipe(Effect.provide(NodeServices.layer)));
