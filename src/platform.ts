import { Cause, Context, Crypto, Effect, FileSystem, Layer, Option, Path, Result, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { NodeServices } from "@effect/platform-node";
import { CommandError, Hash, Repo } from "./contracts.ts";

const diagnosticError = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("HttpError"), status: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 })) }),
  Schema.Struct({ _tag: Schema.Literal("PlatformError"), reason: Schema.Struct({ _tag: Schema.Literals(["AlreadyExists", "BadResource", "Busy", "InvalidData", "NotFound", "PermissionDenied", "TimedOut", "UnexpectedEof", "Unknown", "WouldBlock", "WriteZero", "BadArgument"]) }) }),
  Schema.Struct({ _tag: Schema.Literals(["CommandError", "BackupError", "TimeoutError", "SqlError", "SchemaError"]) }),
]);
// Only typed failures contribute allowlisted metadata; defects never expose their payload.
export const redactedCause = (cause: Cause.Cause<unknown>): string => cause.reasons.map((reason) => {
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
    case "SqlError": return "database failure";
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
  return Schema.decodeUnknownSync(Repo, { onExcessProperty: "error" })(`${url.hostname.toLowerCase()}/${parts.join("/").toLowerCase()}`);
}

export class Platform extends Context.Service<Platform>()("@samuelho/coordinator/Platform", {
  make: Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const digest = Effect.fnUntraced(function*(value: string) {
      return Schema.decodeUnknownSync(Hash, { onExcessProperty: "error" })(Hex.encode(yield* crypto.digest("SHA-256", utf8.encode(value))));
    });
    const fingerprint = Effect.fnUntraced(function*(value: unknown) {
      return yield* digest(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown, {
        replacer: (_, item: unknown) => {
          if (typeof item !== "object" || item === null || Array.isArray(item)) return item;
          return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
        },
      }), { onExcessProperty: "error" })(value));
    });
    // Effect 4.0.1 has no HMAC API. Only MAC operations use captured standard WebCrypto.
    const subtle = yield* Effect.try({
      try: () => globalThis.crypto.subtle,
      catch: () => new CommandError({ message: "Cryptographic service unavailable" }),
    });
    const importHmac = (key: Uint8Array) => Effect.tryPromise({
      try: () => subtle.importKey("raw", webCryptoBytes(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]),
      catch: () => new CommandError({ message: "Cryptographic key import failed" }),
    });
    const comparisonKey = yield* importHmac(yield* crypto.randomBytes(32));
    const expectedMacs = new Map<Hash, Uint8Array<ArrayBuffer>>();
    const checkToken = Effect.fnUntraced(function*(token: string, hash: Hash) {
      if (hash.length !== 64 || !/^[a-f0-9]{64}$/.test(hash)) return false;
      let expectedMac = expectedMacs.get(hash);
      if (expectedMac === undefined) {
        const expected = Hex.decode(hash);
        if (Result.isFailure(expected)) return false;
        expectedMac = new Uint8Array(yield* Effect.tryPromise({
          try: () => subtle.sign("HMAC", comparisonKey, webCryptoBytes(expected.success)),
          catch: () => new CommandError({ message: "Credential verification failed" }),
        }));
        // Only trusted configured hashes belong here; bearer tokens and their digests are never cached.
        expectedMacs.set(hash, expectedMac);
      }
      const signature = expectedMac;
      const actual = yield* crypto.digest("SHA-256", utf8.encode(token));
      // Native MAC verification, not JavaScript equality, compares the unchanged stored SHA-256 hash.
      return yield* Effect.tryPromise({
        try: () => subtle.verify("HMAC", comparisonKey, signature, webCryptoBytes(actual)),
        catch: () => new CommandError({ message: "Credential verification failed" }),
      });
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
      const git = (args: string[]) => exec(ChildProcess.make("git", args, { cwd, stdin: "ignore", forceKillAfter: "1 second" }));
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
