import { Console, Effect, Layer, Schema, Stdio } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ChildProcess } from "effect/process";
import { BrokerConfig, RunnerConfig, Commands, CommandError, ModelProfile, Sha, UserId } from "../src/contracts.ts";
import { Platform } from "../src/platform.ts";
import { ownChildDirectory } from "../src/runner.ts";

NodeRuntime.runMain(Effect.gen(function*() {
  const platform = yield* Platform;
  const args = yield* (yield* Stdio.Stdio).args;
  const mode = args[0];
  if (args.length !== 5 || (mode !== "broker" && mode !== "runner") || args[1] !== "--input" || args[3] !== "--config") {
    return yield* new CommandError({ message: "Usage: deploy-init.ts broker|runner --input DIRECTORY --config DIRECTORY" });
  }
  const input = platform.path.resolve(args[2]!);
  const output = platform.path.resolve(args[4]!);
  const fail = (message: string) => new CommandError({ message });
  const within = (root: string, path: string) => platform.path.isAbsolute(path) && platform.path.resolve(path) === path && path.startsWith(`${root}/`);
  const childTraversal = new Set<string>(["/work"]);
  const directory = Effect.fnUntraced(function*(path: string, mode = 0o700) {
    const root = [output, "/work", "/backups"].find((root) => path === root || within(root, path));
    if (!root || root === "/") return yield* fail("Directory outside deployment mounts");
    const ancestors = [path];
    for (let parent = path; parent !== root;) {
      parent = platform.path.dirname(parent);
      ancestors.push(parent);
    }
    for (const ancestor of ancestors.reverse()) {
      if (!(yield* platform.fs.exists(ancestor))) yield* platform.fs.makeDirectory(ancestor, { mode: 0o700 });
      if ((yield* platform.fs.realPath(ancestor)) !== ancestor || (yield* platform.fs.stat(ancestor)).type !== "Directory") return yield* fail("Deployment directory ancestors must be real directories, not symlinks");
      yield* platform.fs.chown(ancestor, 1000, 1000);
      yield* platform.fs.chmod(ancestor, childTraversal.has(ancestor) ? 0o711 : ancestor === path ? mode : 0o700);
    }
  });
  const file = Effect.fnUntraced(function*(path: string, value: string) {
    if (!within(output, path) && !within("/work", path)) return yield* fail("Secret destination outside deployment config/work mounts");
    yield* directory(platform.path.dirname(path));
    if (yield* platform.fs.exists(path)) {
      if ((yield* platform.fs.realPath(path)) !== path || (yield* platform.fs.stat(path)).type !== "File") return yield* fail("Secret destination must be a regular file");
    }
    yield* platform.fs.writeFileString(path, value, { mode: 0o600 });
    yield* platform.fs.chown(path, 1000, 1000);
    yield* platform.fs.chmod(path, 0o600);
  });
  const read = (key: string) => platform.fs.readFileString(platform.path.join(input, key));
  const token = Effect.fnUntraced(function*(key: string) {
    const value = (yield* read(key)).trim();
    if (!/^[a-f0-9]{64}$/.test(value)) return yield* fail(`Invalid deployment token: ${key}`);
    return value;
  });
  const decode = <S extends Schema.Top>(schema: S, value: string) => Schema.decodeUnknownEffect(Schema.fromJsonString(schema), { onExcessProperty: "error" })(value).pipe(Effect.mapError(() => fail("Invalid deployment input schema")));
  yield* directory(output);
  const health = yield* token("health.token");
  const initializeBrokerDeployment = Effect.fnUntraced(function*() {
    const config = yield* decode(BrokerConfig, yield* read("config.json"));
    if (output !== "/config" || config.databaseUrlFile !== "/config/database.url" || !within("/backups", `${config.backupDirectory}/sentinel`) || config.host !== "0.0.0.0") return yield* fail("Broker requires /config/database.url, /backups backupDirectory and host 0.0.0.0");
    yield* file(config.databaseUrlFile, yield* read("database.url"));
    yield* directory(config.backupDirectory);
    if (!(yield* Effect.forEach(config.principals, (principal) => platform.checkToken(health, principal.tokenHash))).some(Boolean)) return yield* fail("Health token is not an enrolled broker principal");
    for (const principal of config.principals.filter((p) => p.roles.includes("runner"))) {
      if (!principal.controlTokenHash || !principal.runnerUrl || !URL.canParse(principal.runnerUrl) || new URL(principal.runnerUrl).protocol !== "https:") return yield* fail("Runner principals require control hashes and private HTTPS endpoints");
      const control = yield* token(`control.${Buffer.from(principal.id).toString("base64url")}`);
      if (!(yield* platform.checkToken(control, principal.controlTokenHash))) return yield* fail("Runner control credential mismatch");
      yield* file(platform.path.join(output, "tokens", principal.id), control);
    }
    if (config.objectBackup) {
      if (!within(output, config.objectBackup.accessKeyFile) || !within(output, config.objectBackup.secretKeyFile)) return yield* fail("Backup credentials must use the config mount");
      yield* file(config.objectBackup.accessKeyFile, yield* read("backup.access-key"));
      yield* file(config.objectBackup.secretKeyFile, yield* read("backup.secret-key"));
    }
    yield* file(platform.path.join(output, "broker.json"), Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig))(config));
  });
  const initializeRunnerDeployment = Effect.fnUntraced(function*() {
    const config = yield* decode(RunnerConfig, yield* read("config.json"));
    if (!URL.canParse(config.brokerUrl) || new URL(config.brokerUrl).protocol !== "https:" || config.host !== "0.0.0.0" || !within(output, config.tokenFile)) return yield* fail("Runner requires private HTTPS broker, host 0.0.0.0 and config-mounted tokenFile");
    for (const path of [config.stateDirectory, config.modelDirectory, config.childDirectory, ...Object.values(config.repositories)]) {
      if (!within("/work", path)) return yield* fail("Runner data paths must be canonical descendants of /work");
    }
    const paths = [config.stateDirectory, config.modelDirectory, config.childDirectory, ...Object.values(config.repositories)];
    if (paths.some((a, i) => paths.some((b, j) => i !== j && (a === b || a.startsWith(`${b}/`))))) return yield* fail("Runner data directories must be distinct and non-overlapping");
    for (let parent = config.childDirectory; parent !== "/work"; parent = platform.path.dirname(parent)) childTraversal.add(parent);
    yield* directory("/work", 0o711);
    for (const path of paths) yield* directory(path, path === config.childDirectory ? 0o711 : 0o700);
    const credential = yield* token("runner.token");
    if (health !== credential) return yield* fail("Runner health.token must equal runner.token");
    yield* file(config.tokenFile, credential);
    const pins = yield* decode(Schema.Record(Schema.String, Sha), yield* read("repositories.json"));
    if (Object.keys(pins).length !== Object.keys(config.repositories).length) return yield* fail("Provide exactly one commit pin per configured repository");
    for (const [repo, checkout] of Object.entries(config.repositories)) {
      const sha = pins[repo];
      if (!sha || !/^github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repo)) return yield* fail("Explicit GitHub public repository commit pin required");
      const git = (argv: string[]) => platform.exec(ChildProcess.make("git", ["-c", `safe.directory=${checkout}`, ...argv], { cwd: checkout, env: { GIT_TERMINAL_PROMPT: "0" } }));
      if (!(yield* platform.fs.exists(platform.path.join(checkout, ".git")))) {
        if ((yield* platform.fs.readDirectory(checkout)).length !== 0) return yield* fail("Repository destination must be empty or a prepared checkout");
        yield* git(["init"]);
        yield* git(["remote", "add", "origin", `https://${repo}.git`]);
        yield* git(["fetch", "--depth=1", "origin", sha]);
        yield* git(["checkout", "--detach", sha]);
      }
      if ((yield* git(["rev-parse", "HEAD"])).trim() !== sha || (yield* git(["status", "--porcelain"])).trim() !== "") return yield* fail("Prepared repository must match its pinned commit and be clean; refusing to reset operator data");
      yield* ownChildDirectory(checkout, UserId.make(1000));
    }
    const seeds = { "omp.config.yml": ".omp/agent/config.yml", "omp.auth.json": ".omp/agent/auth.json", "omp.models.json": ".omp/agent/models.json", "pi.settings.json": ".pi/agent/settings.json", "pi.auth.json": ".pi/agent/auth.json", "pi.models.json": ".pi/agent/models.json" };
    for (const launcher of Object.keys(config.launchers)) {
      if (launcher !== "omp" && launcher !== "pi") return yield* fail("Deployment seed supports only installed omp/pi launchers");
      const executable = config.launchers[launcher]!;
      if (!platform.path.isAbsolute(executable) || !(yield* platform.fs.exists(executable)) || !((yield* platform.fs.stat(executable)).mode & 0o111)) return yield* fail("Configured native launcher must be installed and executable in the image");
      for (const key of launcher === "omp" ? ["omp.config.yml"] : ["pi.settings.json"]) {
        if (!(yield* platform.fs.exists(platform.path.join(input, key)))) return yield* fail(`Required model seed missing: ${key}`);
      }
    }
    if (Object.keys(config.launchers).length === 0) return yield* fail("At least one native launcher is required");
    for (const [key, destination] of Object.entries(seeds)) {
      if (yield* platform.fs.exists(platform.path.join(input, key))) {
        const value = yield* read(key);
        if (key === "omp.config.yml") yield* decode(ModelProfile, value);
        else yield* decode(Schema.Record(Schema.String, Schema.Unknown), value);
        yield* file(platform.path.join(config.modelDirectory, destination), value);
      } else {
        const path = platform.path.join(config.modelDirectory, destination);
        if (yield* platform.fs.exists(path)) {
          if ((yield* platform.fs.realPath(path)) !== path || (yield* platform.fs.stat(path)).type !== "File") return yield* fail("Previous model seed must be a regular file");
          yield* platform.fs.remove(path);
        }
      }
    }
    const checkSeed = Effect.fnUntraced(function*(directory: string): Effect.fn.Return<void, unknown> {
      for (const entry of yield* platform.fs.readDirectory(directory)) {
        const path = platform.path.join(directory, entry);
        const name = platform.path.relative(config.modelDirectory, path);
        if ((yield* platform.fs.realPath(path)) !== path) return yield* fail("Model seed must not contain symlinks");
        const info = yield* platform.fs.stat(path);
        if (info.type === "Directory" && Object.values(seeds).some((file) => file.startsWith(`${name}/`))) yield* checkSeed(path);
        else if (info.type !== "File" || !Object.values(seeds).includes(name)) return yield* fail("Model seed contains unrelated files; refusing to expose them to children");
      }
    });
    yield* checkSeed(config.modelDirectory);
    yield* file(platform.path.join(output, "runner.json"), Schema.encodeUnknownSync(Schema.fromJsonString(RunnerConfig))(config));
  });
  yield* Commands[`program.deploy-init.${mode}`].executeDecoded({ initializeBrokerDeployment, initializeRunnerDeployment }, {});
  yield* file(platform.path.join(output, "health.header"), `authorization: Bearer ${health}`);
  yield* Console.log(`coordinator ${mode} deployment initialized`);
}).pipe(Effect.provide(Layer.merge(Platform.layer, NodeServices.layer))));
