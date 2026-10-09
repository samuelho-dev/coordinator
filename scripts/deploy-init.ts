import { Config, Console, Effect, Layer, Option, Schema, Stdio } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { BrokerConfig, RunnerConfig, ModelProfile, Commands, CommandError, HttpError, Hash, ObjectBackupConfig, Repo, Sha, UserId } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { ownChildDirectory } from "../src/runner.ts";

NodeRuntime.runMain(Effect.gen(function*() {
  const platform = yield* Platform;
  const args = yield* (yield* Stdio.Stdio).args;
  const mode = args[0];
  if (args.length !== 1 || (mode !== "broker" && mode !== "runner")) return yield* new CommandError({ message: "Initializer mode must be broker or runner" });
  const repo = Repo.make("github.com/samuelho-dev/coordinator");
  const brokerUrl = yield* Config.String("COORDINATOR_BROKER_URL");
  if (!brokerUrl.startsWith("https://")) return yield* new CommandError({ message: "Private TLS broker URL required" });
  const credential = Effect.fnUntraced(function*(field: string) {
    const value = yield* Config.String(field);
    if (!/^[a-f0-9]{64}$/.test(value)) return yield* new CommandError({ message: `Central enrollment field unavailable: ${field}` });
    return value;
  });
  const directory = Effect.fnUntraced(function*(path: string) {
    yield* platform.fs.makeDirectory(path, { recursive: true, mode: 0o700 });
    yield* platform.fs.chown(path, 1000, 1000);
    yield* platform.fs.chmod(path, 0o700);
  });
  const file = Effect.fnUntraced(function*(path: string, value: string) {
    yield* platform.fs.writeFileString(path, value, { mode: 0o600 });
    yield* platform.fs.chown(path, 1000, 1000);
    yield* platform.fs.chmod(path, 0o600);
  });
  const initializeBrokerDeployment = Effect.fnUntraced(function*() {
    yield* directory("/config");
    for (const path of ["/data", "/backups", "/config/tokens"]) yield* directory(path);
    const runner = yield* credential("HOMELAB_RUNNER_TOKEN");
    const control = yield* credential("HOMELAB_RUNNER_CONTROL_TOKEN");
    const repositoryId = yield* Config.Number("COORDINATOR_REPOSITORY_ID");
    if (!Number.isSafeInteger(repositoryId) || repositoryId <= 0) return yield* new CommandError({ message: "Inspected enrollment GitHub repository ID required" });
    const runnerUrl = yield* Config.String("COORDINATOR_RUNNER_URL");
    const macUrl = yield* Config.String("COORDINATOR_MAC_RUNNER_URL");
    if (!runnerUrl.startsWith("https://") || !macUrl.startsWith("https://")) return yield* new CommandError({ message: "Private TLS runner URLs required" });
    const backupEndpoint = yield* Config.String("COORDINATOR_BACKUP_ENDPOINT").pipe(Config.option);
    const objectBackup = Option.isSome(backupEndpoint) ? {
      endpoint: backupEndpoint.value, region: yield* Schema.decodeUnknownEffect(ObjectBackupConfig.fields.region, { onExcessProperty: "error" })(yield* Config.String("COORDINATOR_BACKUP_REGION")).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" }))), bucket: yield* Config.String("COORDINATOR_BACKUP_BUCKET"),
      accessKeyFile: "/config/backup-access-key", secretKeyFile: "/config/backup-secret-key",
    } : undefined;
    if (objectBackup) {
      yield* file(objectBackup.accessKeyFile, yield* Config.String("COORDINATOR_BACKUP_ACCESS_KEY"));
      yield* file(objectBackup.secretKeyFile, yield* Config.String("COORDINATOR_BACKUP_SECRET_KEY"));
    }
    const config = yield* Schema.decodeUnknownEffect(BrokerConfig, { onExcessProperty: "error" })({
      database: "/data/ledger.sqlite", host: "0.0.0.0", port: 8787, backupDirectory: "/backups",
      archiveRetentionMs: 30 * 86400000, backupRetentionMs: 3 * 86400000,
      ...(objectBackup ? { objectBackup } : {}),
      repositoryIds: { [repo]: repositoryId }, githubTokenEnv: "GITHUB_TOKEN", principals: [
        { id: "mac-session", owner: "samuelho-dev", roles: ["session", "coordinator"], repos: [repo], tokenHash: yield* platform.digest(yield* credential("MAC_SESSION_TOKEN")) },
        { id: "homelab-session", owner: "samuelho-dev", roles: ["session", "coordinator"], repos: [repo], tokenHash: yield* platform.digest(yield* credential("HOMELAB_SESSION_TOKEN")) },
        { id: "homelab-runner", owner: "samuelho-dev", roles: ["runner"], repos: [repo], tokenHash: yield* platform.digest(runner), controlTokenHash: yield* platform.digest(control), runnerUrl },
        { id: "human-reviewer", owner: "samuelho-dev", roles: ["reviewer"], repos: [repo], tokenHash: yield* platform.digest(yield* credential("REVIEWER_TOKEN")) },
        { id: "archive-writer", owner: "samuelho-dev", roles: ["archive-writer"], repos: [repo], tokenHash: yield* platform.digest(yield* credential("ARCHIVE_WRITER_TOKEN")) },
        { id: "mac-runner", owner: "samuelho-dev", roles: ["runner"], repos: [repo], tokenHash: yield* platform.digest(yield* credential("MAC_RUNNER_TOKEN")), controlTokenHash: yield* platform.digest(yield* credential("MAC_RUNNER_CONTROL_TOKEN")), runnerUrl: macUrl },
      ],
    }).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
    yield* file("/config/tokens/mac-runner", yield* credential("MAC_RUNNER_CONTROL_TOKEN"));
    yield* file("/config/tokens/homelab-runner", control);
    yield* file("/config/broker.json", Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig), { onExcessProperty: "error" })(config));
    yield* file("/config/health.header", `authorization: Bearer ${runner}`);
  });
  const initializeRunnerDeployment = Effect.fnUntraced(function*() {
    yield* directory("/config");
    for (const path of ["/work", "/work/repos", "/work/runner", "/work/home", "/work/children"]) yield* directory(path);
    yield* platform.fs.chmod("/work", 0o711);
    yield* platform.fs.chmod("/work/children", 0o711);
    const checkout = "/work/repos/coordinator";
    const imageSha = yield* Config.String("COORDINATOR_IMAGE_SHA");
    if (!/^[a-f0-9]{40}$/.test(imageSha)) return yield* new CommandError({ message: "Pinned full image source SHA required" });
    const sha = Sha.make(imageSha);
    const bundle = "/app/coordinator-source.bundle";
    if ((yield* platform.exec(ChildProcess.make("git", ["bundle", "list-heads", bundle]))).trim() !== `${sha} HEAD`) return yield* new CommandError({ message: "Image source bundle does not match pinned SHA" });
    const git = (args: string[], cwd?: string) => platform.exec(ChildProcess.make("git", ["-c", `safe.directory=${checkout}`, ...args], { cwd }));
    yield* Effect.gen(function*() {
      if (!(yield* platform.fs.exists(platform.path.join(checkout, ".git")))) yield* git(["clone", "--no-checkout", bundle, checkout]);
      yield* git(["fetch", bundle, "HEAD"], checkout);
      yield* git(["remote", "set-url", "origin", "https://github.com/samuelho-dev/coordinator.git"], checkout);
      yield* git(["update-ref", "refs/heads/main", sha], checkout);
      yield* git(["checkout", "--detach", sha], checkout);
      yield* ownChildDirectory(checkout, UserId.make(1000));
    }).pipe(Effect.mapError(() => new CommandError({ message: "Private allowlisted repository initialization failed" })));
    yield* directory("/work/home/.omp");
    yield* directory("/work/home/.omp/agent");
    const profile = "/work/home/.omp/agent/config.yml";
    if (!(yield* platform.fs.exists(profile))) yield* file(profile, Schema.encodeUnknownSync(Schema.fromJsonString(ModelProfile), { onExcessProperty: "error" })({ modelRoles: { default: yield* Config.String("COORDINATOR_MODEL").pipe(Config.withDefault("openai-codex/gpt-6.1-sol:xhigh")) } }));
    const runner = yield* credential("HOMELAB_RUNNER_TOKEN");
    const controlTokenHash = yield* Schema.decodeUnknownEffect(Hash, { onExcessProperty: "error" })(yield* credential("HOMELAB_RUNNER_CONTROL_HASH")).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
    yield* file("/config/runner.token", runner);
    yield* file("/config/health.header", `authorization: Bearer ${runner}`);
    const config = yield* Schema.decodeUnknownEffect(RunnerConfig, { onExcessProperty: "error" })({
      id: "homelab-runner", brokerUrl, tokenFile: "/config/runner.token", host: "0.0.0.0", port: 8788,
      stateDirectory: "/work/runner", slots: 1, repositories: { [repo]: checkout }, launchers: { omp: "/usr/local/bin/omp" }, controlTokenHash,
      modelDirectory: "/work/home", childDirectory: "/work/children", childUidBase: UserId.make(1000000),
    }).pipe(Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })));
    yield* file("/config/runner.json", Schema.encodeUnknownSync(Schema.fromJsonString(RunnerConfig), { onExcessProperty: "error" })(config));
  });
  yield* Commands[`program.deploy-init.${mode}`].executeDecoded({ initializeBrokerDeployment, initializeRunnerDeployment }, {});
  yield* Console.log(`coordinator ${mode} enrollment initialized`);
}).pipe(Effect.provide(Layer.merge(Platform.layer, NodeServices.layer))));
