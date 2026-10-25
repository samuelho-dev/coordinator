import { constants } from "node:fs";
import { chmod, lstat, mkdir, open, realpath, rmdir, unlink } from "node:fs/promises";
import { createHash, randomBytes } from "node:crypto";
import { isIP } from "node:net";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { Config, Console, Effect, Option, Redacted, Schema } from "effect";
import { Command, Flag, Prompt } from "effect/cli";
import { ConnectorConfig, InspectionTools, InspectorConfig } from "./inspection.ts";
import { Mcp } from "./mcp.ts";
import { NativeError, readConnectorToken } from "./native.ts";
import { Host } from "./platform.ts";

const failure = (operation: string) => () => new NativeError({ operation });
const privateFailure = failure("Setup could not write private files; use a canonical owner-controlled path, mode-0700 directory and new filenames. Existing files are never overwritten");
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const canonical = (file: string) => {
  if (!isAbsolute(file) || resolve(file) !== file || /[\x00-\x1f\x7f-\x9f]/u.test(file)) throw new Error("Unsafe path");
};

// Exclusive creation is the overwrite fence; remove only files this transaction created.
const writePrivate = (directory: string, files: ReadonlyArray<readonly [string, string]>, uid: number | undefined) => Effect.tryPromise({
  try: async () => {
    canonical(directory);
    if (uid === undefined) throw new Error("Owner unavailable");
    const createdDirectories: string[] = [], createdFiles: string[] = [];
    const ensure = async (target: string, privateDirectory: boolean): Promise<void> => {
      try {
        const info = await lstat(target);
        if (!info.isDirectory() || info.isSymbolicLink() || await realpath(target) !== target || (privateDirectory && (info.uid !== uid || (info.mode & 0o777) !== 0o700))) throw new Error("Unsafe directory");
        // Ancestors must not be replaceable by another owner; sticky /tmp is allowed.
        if (!privateDirectory && (info.uid !== uid && info.uid !== 0 || (info.mode & 0o022) !== 0 && (info.mode & 0o1000) === 0)) throw new Error("Unsafe ancestor");
      } catch (error) {
        if (typeof error !== "object" || error === null || !("code" in error) || error.code !== "ENOENT") throw error;
        await ensure(dirname(target), false);
        await mkdir(target, { mode: 0o700 });
        createdDirectories.push(target);
        await chmod(target, 0o700);
      }
    };
    try {
      await ensure(directory, true);
      // realpath rejects symlinks anywhere in existing ancestry; validate its writable ancestors too.
      for (let parent = dirname(directory); ; parent = dirname(parent)) {
        await ensure(parent, false);
        if (parent === dirname(parent)) break;
      }
      for (const [file, content] of files) {
        canonical(file);
        if (dirname(file) !== directory) throw new Error("Unsafe destination");
        const handle = await open(file, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
        createdFiles.push(file);
        try { await handle.chmod(0o600); await handle.writeFile(content, "utf8"); } finally { await handle.close(); }
      }
    } catch (error) {
      for (const file of createdFiles.reverse()) await unlink(file).catch(() => undefined);
      for (const dir of createdDirectories.reverse()) await rmdir(dir).catch(() => undefined);
      throw error;
    }
  },
  catch: privateFailure,
});

const configHome = Effect.gen(function*() {
  const host = yield* Host;
  const xdg = yield* Config.String("XDG_CONFIG_HOME").pipe(Config.option);
  return join(Option.getOrUndefined(xdg) ?? join(host.homeDirectory, ".config"), "coordinator");
});

export const initializeServer = (options: { directory: string; owner: string; host: string; port: number }) => Effect.gen(function*() {
  if ((!isIP(options.host) && options.host !== "localhost") || !Number.isInteger(options.port) || options.port < 1 || options.port > 65535 || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$/u.test(options.owner)) {
    return yield* new NativeError({ operation: "Init requires an IP address or localhost, port 1–65535, and an owner identifier containing only letters, digits, dot, underscore or hyphen" });
  }
  const token = yield* Effect.try({ try: () => randomBytes(32).toString("base64url"), catch: failure("Secure credential generation unavailable") });
  const config = yield* Schema.decodeEffect(InspectorConfig)({ host: options.host, port: options.port, principals: [{ id: options.owner, owner: options.owner, tokenHash: createHash("sha256").update(token).digest("hex") }] }).pipe(Effect.mapError(failure("Invalid server configuration")));
  const configFile = join(options.directory, "server.json"), tokenFile = join(options.directory, "owner.token");
  yield* writePrivate(options.directory, [[tokenFile, `${token}\n`], [configFile, `${JSON.stringify(config, null, 2)}\n`]], (yield* Host).uid);
  return { configFile, tokenFile };
});

export const connectClient = (options: { endpoint: string; token: string; configFile: string; tokenFile?: string }) => Effect.gen(function*() {
  const targetToken = options.tokenFile ?? join(dirname(options.configFile), "inspector.token");
  const config = yield* Schema.decodeEffect(ConnectorConfig)({ inspectorUrl: options.endpoint, apiToken: options.token, tokenFile: targetToken }).pipe(Effect.mapError(failure("Invalid endpoint or credential; use HTTPS except for loopback, without URL credentials, query or fragment")));
  if (options.tokenFile !== undefined) {
    const stored = yield* readConnectorToken({ inspectorUrl: options.endpoint, tokenFile: options.tokenFile });
    if (stored !== options.token) return yield* new NativeError({ operation: "Credential file changed; retry connection setup" });
  }
  // This is an authenticated read only: it never resolves/creates a native machine identity.
  yield* (yield* Mcp).call(config.inspectorUrl, options.token, InspectionTools.list, { limit: 1 }).pipe(Effect.mapError(failure("Connection check failed; verify the endpoint, TLS certificate and owner credential. No client configuration was written")));
  const files: Array<readonly [string, string]> = [];
  if (options.tokenFile === undefined) files.push([targetToken, `${options.token}\n`]);
  files.push([options.configFile, `${JSON.stringify({ inspectorUrl: config.inspectorUrl, tokenFile: targetToken }, null, 2)}\n`]);
  yield* writePrivate(dirname(options.configFile), files, (yield* Host).uid);
  return { configFile: options.configFile, tokenFile: targetToken };
});

const init = Command.make("init", {
  directory: Flag.String("directory").pipe(Flag.optional),
  owner: Flag.String("owner").pipe(Flag.withDefault("owner")),
  host: Flag.String("listen-host").pipe(Flag.withDefault("127.0.0.1")),
  port: Flag.Int("port").pipe(Flag.withDefault(8787)),
}, (flags) => Effect.gen(function*() {
  const directory = Option.getOrUndefined(flags.directory) ?? join(yield* configHome, "server");
  const result = yield* initializeServer({ ...flags, directory });
  yield* Console.log(`Created private server configuration: ${result.configFile}\nOwner credential file: ${result.tokenFile}\nStart server:\nCOORDINATOR_CONFIG=${quote(result.configFile)} coordinator inspector\nKeep the credential private. For remote clients, expose this server through HTTPS.`);
})).pipe(Command.withDescription("Bootstrap a self-hosted server without printing its owner credential. Never overwrites existing files."));

const connect = Command.make("connect", {
  endpoint: Flag.String("endpoint").pipe(Flag.optional),
  tokenFile: Flag.String("token-file").pipe(Flag.optional),
  config: Flag.String("config").pipe(Flag.optional),
}, (flags) => Effect.gen(function*() {
  const selected = yield* Config.String("COORDINATOR_CONNECTOR_CONFIG").pipe(Config.option);
  const configFile = Option.getOrUndefined(flags.config) ?? Option.getOrUndefined(selected) ?? join(yield* configHome, "inspector.json");
  const endpoint = Option.getOrUndefined(flags.endpoint) ?? (yield* Prompt.String({ message: "Coordinator endpoint (HTTPS, or loopback HTTP)" }));
  const tokenFile = Option.getOrUndefined(flags.tokenFile);
  const environment = yield* Config.String("COORDINATOR_API_TOKEN").pipe(Config.option);
  const token = tokenFile !== undefined ? yield* readConnectorToken({ inspectorUrl: endpoint, tokenFile })
    : Option.isSome(environment) ? environment.value : Redacted.value(yield* Prompt.Password({ message: "Owner credential" }));
  const result = yield* connectClient({ endpoint, token, configFile, ...(tokenFile === undefined ? {} : { tokenFile }) });
  yield* Console.log(`Authenticated connection saved: ${result.configFile}\nCredential file: ${result.tokenFile}\nClient configuration contains only the endpoint and credential-file pointer. Existing native settings are unchanged.\nCOORDINATOR_API_URL / COORDINATOR_API_TOKEN environment overrides still take precedence at runtime.`);
})).pipe(Command.withDescription("Authenticate and save a private client configuration. Credentials: --token-file, COORDINATOR_API_TOKEN, or masked prompt; never argv. Existing files are not overwritten."));

export const runSetup = (args: ReadonlyArray<string>) => Command.runWith(Command.make("coordinator").pipe(Command.withSubcommands([init, connect])), { version: "", renderErrors: false })(args).pipe(
  Effect.catchTag("ShowHelp", () => Effect.fail(new NativeError({ operation: "Invalid setup flags. Run coordinator init --help or coordinator connect --help. Credentials are never accepted as arguments" }))),
);
