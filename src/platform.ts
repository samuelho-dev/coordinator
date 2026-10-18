import { timingSafeEqual } from "node:crypto";
import { Cause, Context, Crypto, Effect, FileSystem, Layer, Option, Path, Result, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { NodeServices } from "@effect/platform-node";
import { CommandError, Repo } from "./contracts.ts";
import { RunnerTask } from "./generated/database/types.ts";

const diagnosticError = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("HttpError"), status: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 })) }),
  Schema.Struct({ _tag: Schema.Literal("PlatformError"), reason: Schema.Struct({ _tag: Schema.Literals(["AlreadyExists", "BadResource", "Busy", "InvalidData", "NotFound", "PermissionDenied", "TimedOut", "UnexpectedEof", "Unknown", "WouldBlock", "WriteZero", "BadArgument"]) }) }),
  Schema.Struct({ _tag: Schema.Literals(["CommandError", "BackupError", "TimeoutError", "DatabaseError", "SchemaError"]) }),
]);
// Only typed failures contribute allowlisted metadata; defects never expose their payload.
export const redactedCause = <E>(cause: Cause.Cause<E>): string => cause.reasons.map((reason) => {
  if (Cause.isDieReason(reason)) return "unknown defect";
  if (Cause.isInterruptReason(reason)) return "interrupted";
  const decoded = Schema.decodeUnknownOption(diagnosticError)(reason.error);
  if (Option.isNone(decoded)) return "unknown failure";
  const error = decoded.value;
  if (error._tag === "HttpError") return `HTTP status=${error.status}`;
  if (error._tag === "PlatformError") return `platform ${error.reason._tag}`;
  switch (error._tag) {
    case "CommandError": return "command failure";
    case "BackupError": return "backup failure";
    case "TimeoutError": return "timeout";
    case "DatabaseError": return "database failure";
    case "SchemaError": return "invalid boundary data";
  }
}).join("; ") || "unknown cause";

const utf8 = new TextEncoder();
// WebCrypto requires ArrayBuffer-backed, exact-range views; never copy or expose the rest of a backing buffer.
const webCryptoBytes = (bytes: Uint8Array) => {
  if (!(bytes.buffer instanceof ArrayBuffer)) throw new Error("Unsupported cryptographic buffer");
  return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
};
export function canonicalRemote(remote: string): Repo {
  const scp = /^([^@:\s]+@)?([^:\/\s]+):([^\/\s][^\s]*)$/.exec(remote);
  const url = scp ? new URL(`ssh://${scp[2]}/${scp[3]}`) : new URL(remote);
  if (!["https:", "ssh:", "git:"].includes(url.protocol) || url.search || url.hash) throw new Error("Unsupported remote");
  const parts = url.pathname.replace(/^\/+|\/+$/g, "").replace(/\.git$/, "").split("/");
  if (parts.length !== 2 || parts.some((part) => !part || part === "." || part === "..")) throw new Error("Noncanonical remote");
  return Schema.decodeSync(Repo, { onExcessProperty: "error" })(`${url.hostname.toLowerCase()}/${parts.join("/").toLowerCase()}`);
}

export class Platform extends Context.Service<Platform>()("@samuelho/coordinator/Platform", {
  make: Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const digest = Effect.fnUntraced(function*(value: string) {
      return RunnerTask.fields.fingerprint.make(Hex.encode(yield* crypto.digest("SHA-256", utf8.encode(value))));
    });
    const encodeFingerprint = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown, {
      replacer: (_key, item: unknown) => {
        if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
        return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
      },
    }));
    const fingerprint = <A>(value: A) => Effect.gen(function*() {
      const text = yield* encodeFingerprint(value).pipe(Effect.mapError(() => new CommandError({ message: "Fingerprint input is not JSON serializable" })));
      return yield* digest(text);
    });
    // Effect has no HMAC API; signing uses standard WebCrypto.
    const subtle = yield* Effect.try({
      try: () => globalThis.crypto.subtle,
      catch: () => new CommandError({ message: "Cryptographic service unavailable" }),
    });
    const importHmac = (key: Uint8Array) => Effect.tryPromise({
      try: () => subtle.importKey("raw", webCryptoBytes(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]),
      catch: () => new CommandError({ message: "Cryptographic key import failed" }),
    });
    const checkToken = Effect.fnUntraced(function*(token: string, hash: typeof RunnerTask.fields.fingerprint.Type) {
      if (hash.length !== 64 || !/^[a-f0-9]{64}$/.test(hash)) return false;
      const expected = Hex.decode(hash);
      if (Result.isFailure(expected)) return false;
      const actual = yield* crypto.digest("SHA-256", utf8.encode(token));
      return timingSafeEqual(expected.success, actual);
    });
    const hmac = Effect.fnUntraced(function*(key: Uint8Array, value: string) {
      const imported = yield* importHmac(key);
      return new Uint8Array(yield* Effect.tryPromise({
        try: () => subtle.sign("HMAC", imported, utf8.encode(value)),
        catch: () => new CommandError({ message: "Cryptographic signing failed" }),
      }));
    });
    const privateFile = Effect.fnUntraced(function*(file: string) {
      if (((yield* fs.stat(file)).mode & 0o077) !== 0) return yield* new CommandError({ message: `Private file permissions required: ${file}` });
      return (yield* fs.readFileString(file)).trim();
    });
    const exec = Effect.fnUntraced(function*(command: ChildProcess.Command, timeoutMs = 30000, maxBytes = 65536) {
      const child = yield* spawner.spawn(command);
      yield* Effect.forkScoped(Stream.runDrain(child.stderr));
      let size = 0;
      const text = yield* child.stdout.pipe(
        Stream.mapEffect((chunk) => {
          size += chunk.length;
          return size > maxBytes ? Effect.fail(new CommandError({ message: "Subprocess output exceeds bound" })) : Effect.succeed(chunk);
        }),
        Stream.decodeText(),
        Stream.runFold(() => "", (text, chunk) => text + chunk),
      );
      if ((yield* child.exitCode) !== 0) return yield* new CommandError({ message: "Subprocess failed; diagnostic output suppressed" });
      return text;
    }, (effect, _command, timeoutMs = 30000) => Effect.scoped(effect).pipe(Effect.timeout(timeoutMs)));
    const inspectedRepo = Effect.fnUntraced(function*(cwd: string, selectedRemote?: string) {
      const env = { ...process.env };
      delete env.COORDINATOR_DATABASE_URL;
      delete env.COORDINATOR_RUNNER_DATABASE_URL;
      delete env.COORDINATOR_TEST_POSTGRES_URL;
      const git = (args: string[]) => exec(ChildProcess.make("git", args, { cwd, env, extendEnv: false, stdin: "ignore", forceKillAfter: "1 second" }));
      const names = (yield* git(["remote"])).trim().split("\n").filter(Boolean);
      const name = selectedRemote ?? (names.length === 1 ? names[0] : undefined);
      if (!name || !names.includes(name)) return yield* new CommandError({ message: "Select one Git remote; ambiguous or absent remote" });
      const urls = (yield* git(["remote", "get-url", "--all", name])).trim().split("\n");
      if (urls.length !== 1) return yield* new CommandError({ message: "Ambiguous selected remote URLs" });
      return yield* Effect.try({ try: () => canonicalRemote(urls[0]!), catch: () => new CommandError({ message: "Noncanonical remote" }) });
    });
    const acquireJournalOwnership = Effect.fnUntraced(function*(file: string) {
      if (!path.isAbsolute(file) || path.resolve(file) !== file ||
        (yield* fs.realPath(path.dirname(file))) !== path.dirname(file) ||
        ((yield* fs.stat(path.dirname(file))).mode & 0o077) !== 0) return yield* new CommandError({ message: "Canonical private ownership lock directory required" });
      yield* fs.writeFileString(file, "", { flag: "wx", mode: 0o600 }).pipe(Effect.catch((error) =>
        error.reason._tag === "AlreadyExists" ? Effect.void : Effect.fail(error)));
      const info = yield* fs.stat(file);
      if ((yield* fs.realPath(file)) !== file || info.type !== "File" || (info.mode & 0o077) !== 0) return yield* new CommandError({ message: "Canonical private ownership lock required" });
      // Effect has no native advisory-lock API; the fixed helper holds flock until its scoped stdin closes.
      const guard = yield* spawner.spawn(ChildProcess.make("/usr/bin/flock", ["--exclusive", "--nonblock", file, "/bin/sh", "-c", "printf 'locked\\n'; exec /usr/bin/cat >/dev/null"], {
        env: {}, extendEnv: false, stdin: Stream.never, forceKillAfter: "1 second",
      }));
      yield* Effect.forkScoped(Stream.runDrain(guard.stderr));
      const marker = yield* guard.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runHead, Effect.timeout("10 seconds"));
      if (Option.isNone(marker) || marker.value !== "locked") return yield* new CommandError({ message: "Journal already owned or kernel lock unavailable" });
      return guard;
    });
    return { fs, path, crypto, spawner, digest, hmac, fingerprint, checkToken, privateFile, inspectedRepo, canonicalRemote, exec, acquireJournalOwnership };
  }),
}) {
  static readonly layer = Layer.effect(this, this.make).pipe(Layer.provide(NodeServices.layer));
}
